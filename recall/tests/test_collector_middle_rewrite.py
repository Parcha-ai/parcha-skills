"""Strong snapshot resume checks at the collector and filesystem boundaries."""
import os
import unittest
import test_collector
from test_collector import claude_line, AckServer

class MiddleRewriteTest(unittest.TestCase):
    setUp = test_collector.CollectorTest.setUp
    tearDown = test_collector.CollectorTest.tearDown
    collector = test_collector.CollectorTest.collector

    def test_completed_touched_snapshot_keeps_acked_cursor_and_refreshes_witness(self):
        from unittest.mock import patch
        from collector import collector as module
        transcript = self.root / "session.jsonl"
        payload = "".join(claude_line(str(i)) for i in range(3)).encode()
        transcript.write_bytes(payload)
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertTrue(collector.scan()["scan_complete"])
        collector.flush()
        acknowledgements = list(AckServer.received)
        collector.max_scan_records = 1
        stat = transcript.stat()
        os.utime(transcript, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
        with patch.object(module, "snapshot_hashes", wraps=module.snapshot_hashes) as hashes:
            result = collector.scan()
            self.assertTrue(result["scan_complete"])
            row = collector.db.execute("SELECT * FROM files").fetchone()
            self.assertEqual(row["scanned_offset"], len(payload))
            self.assertEqual(row["committed_offset"], len(payload))
            self.assertEqual(row["status"], "ok")
            self.assertEqual(row["mtime_ns"], transcript.stat().st_mtime_ns)
            self.assertEqual(row["snapshot_stat"], module.json.dumps(module.snapshot_identity(transcript.stat())))
            self.assertEqual(hashes.call_count, 1)
            self.assertTrue(collector.scan()["scan_complete"])
            self.assertEqual(hashes.call_count, 1)
        collector.flush()
        self.assertEqual(AckServer.received, acknowledgements)
        self.assertEqual(transcript.read_bytes(), payload)

    def _completed_snapshot(self):
        transcript = self.root / "session.jsonl"
        payload = "".join(claude_line(str(i)) for i in range(3)).encode()
        transcript.write_bytes(payload)
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertTrue(collector.scan()["scan_complete"])
        collector.flush()
        return transcript, payload, collector

    def test_completed_identical_rewrite_and_replacement_do_not_replay(self):
        transcript, payload, collector = self._completed_snapshot()
        requests = AckServer.requests
        collector.max_scan_records = 1
        for replacement in (False, True):
            with self.subTest(replacement=replacement):
                if replacement:
                    other = self.root / "replacement.tmp"
                    other.write_bytes(payload)
                    other.replace(transcript)
                else:
                    transcript.write_bytes(payload)
                self.assertTrue(collector.scan()["scan_complete"])
                collector.flush()
                row = collector.db.execute("SELECT * FROM files").fetchone()
                self.assertEqual(row["committed_offset"], len(payload))
                self.assertEqual(row["scanned_offset"], len(payload))
                self.assertEqual(AckServer.requests, requests)
        self.assertEqual(transcript.read_bytes(), payload)

    def test_completed_changed_middle_is_delivered_despite_same_size(self):
        transcript, payload, collector = self._completed_snapshot()
        changed = payload.replace(b'"1"', b'"x"')
        self.assertEqual(len(changed), len(payload))
        transcript.write_bytes(changed)
        collector.max_scan_records = 1
        self.assertFalse(collector.scan()["scan_complete"])
        collector.flush()
        for _ in range(3):
            collector.scan()
            collector.flush()
        delivered = [e["content"]["message"]["content"] for b in AckServer.received for e in b["events"]]
        self.assertEqual(delivered, ["0", "1", "2", "x"])
        self.assertEqual(transcript.read_bytes(), changed)

    def test_completed_missing_weak_or_unsealed_proof_requires_scan(self):
        transcript, payload, collector = self._completed_snapshot()
        for field, value in (("snapshot_sha256", None), ("snapshot_sha256", "weak"), ("snapshot_stat", None)):
            with self.subTest(field=field, value=value):
                collector.max_scan_records = 100
                collector.scan()
                collector.flush()
                collector.db.execute(f"UPDATE files SET {field}=?", (value,))
                collector.db.commit()
                stat = transcript.stat()
                os.utime(transcript, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
                collector.max_scan_records = 1
                self.assertFalse(collector.scan()["scan_complete"])
                row = collector.db.execute("SELECT * FROM files").fetchone()
                self.assertEqual(row["scanned_offset"], len(claude_line("0").encode()))
                self.assertEqual(row["status"], "scanning-full")
        self.assertEqual(transcript.read_bytes(), payload)

    def test_completed_unacked_or_incomplete_cursor_cannot_be_advanced_by_hash(self):
        transcript, payload, collector = self._completed_snapshot()
        for scanned, committed in ((len(payload), 0), (0, 0)):
            with self.subTest(scanned=scanned, committed=committed):
                collector.max_scan_records = 100
                collector.scan()
                collector.flush()
                collector.db.execute("UPDATE files SET status='ok',scanned_offset=?,committed_offset=?", (scanned, committed))
                collector.db.commit()
                stat = transcript.stat()
                os.utime(transcript, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
                collector.max_scan_records = 1
                self.assertFalse(collector.scan()["scan_complete"])
                row = collector.db.execute("SELECT * FROM files").fetchone()
                self.assertEqual(row["scanned_offset"], len(claude_line("0").encode()))
                self.assertLess(row["committed_offset"], len(payload))
        self.assertEqual(transcript.read_bytes(), payload)

    def test_completed_append_delivers_tail_without_replaying_prefix(self):
        transcript, payload, collector = self._completed_snapshot()
        tail = claude_line("tail").encode()
        with transcript.open("ab") as output:
            output.write(tail)
        collector.max_scan_records = 1
        self.assertTrue(collector.scan()["scan_complete"])
        collector.flush()
        delivered = [e["content"]["message"]["content"] for b in AckServer.received for e in b["events"]]
        self.assertEqual(delivered, ["0", "1", "2", "tail"])
        row = collector.db.execute("SELECT * FROM files").fetchone()
        self.assertEqual(row["committed_offset"], len(payload + tail))
        self.assertEqual(transcript.read_bytes(), payload + tail)

    def test_completed_truncation_keeps_captured_history_without_phantom_offset(self):
        transcript, _, collector = self._completed_snapshot()
        short = claude_line("0").encode()
        transcript.write_bytes(short)
        self.assertTrue(collector.scan()["scan_complete"])
        collector.flush()
        row = collector.db.execute("SELECT * FROM files").fetchone()
        self.assertEqual(row["scanned_offset"], len(short))
        self.assertEqual(row["committed_offset"], len(short))
        self.assertEqual(collector.db.execute("SELECT COUNT(*) FROM active_records").fetchone()[0], 3)
        self.assertEqual([e["kind"] for b in AckServer.received for e in b["events"]], ["transcript_record"] * 3)

    def test_same_size_middle_rewrite_during_scan_revisits_changed_records(self) -> None:
        transcript = self.root / "session.jsonl"
        first, last = claude_line("a" * 5000), claude_line("z" * 5000)
        transcript.write_text(first + claude_line("old middle") + last)
        os.utime(transcript, ns=(1_000_000_000, 1_000_000_000))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 2
        collector.scan()
        collector.flush()
        transcript.write_text(first + claude_line("new middle") + last)
        os.utime(transcript, ns=(2_000_000_000, 2_000_000_000))
        for _ in range(3):
            collector.scan()
            collector.flush()
        delivered = [event["content"]["message"]["content"]
                     for batch in AckServer.received for event in batch["events"]]
        self.assertIn("new middle", delivered)


    def test_repeated_same_size_mtime_touches_do_not_starve_tail(self):
        transcript=self.root/'session.jsonl'
        transcript.write_text(''.join(claude_line('evidence '+str(i))for i in range(6)))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 1
        for i in range(6):
            os.utime(transcript,ns=((i+1)*1_000_000_000,(i+1)*1_000_000_000))
            collector.scan()
            collector.flush()
        delivered=[e['content']['message']['content']for b in AckServer.received for e in b['events']]
        self.assertEqual(delivered,['evidence '+str(i)for i in range(6)])

    def test_legacy_partial_snapshot_restarts_once_then_resumes(self):
        from unittest.mock import patch
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(''.join(claude_line(str(i)) for i in range(4)))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 1
        collector.scan()
        collector.flush()
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL')
        collector.db.commit()
        with patch.object(module, 'snapshot_hashes', wraps=module.snapshot_hashes) as hashes:
            collector.scan()
            collector.flush()
            first = collector.db.execute('SELECT scanned_offset FROM files').fetchone()[0]
            collector.scan()
            collector.flush()
            second = collector.db.execute('SELECT scanned_offset FROM files').fetchone()[0]
            self.assertEqual(hashes.call_count, 1)
            self.assertEqual(first, len(claude_line('0').encode()))
            self.assertGreater(second, first)

    def test_changed_content_during_scan_cannot_save_trusted_snapshot(self):
        from unittest.mock import patch
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('old') + claude_line('tail'))
        collector = self.collector()
        self.addCleanup(collector.close)
        original = collector.privacy.apply
        def mutate(content):
            transcript.write_text(claude_line('new') + claude_line('tail'))
            return original(content)
        with patch.object(collector.privacy, 'apply', side_effect=mutate):
            self.assertFalse(collector.scan()['scan_complete'])
        row = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertIsNotNone(row['snapshot_sha256'])
        self.assertIsNone(row['snapshot_stat'])
        self.assertTrue(row['status'].startswith('scanning-'))
        collector.close()
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.scan()
        collector.flush()
        delivered = [e['content']['message']['content'] for b in AckServer.received for e in b['events']]
        self.assertIn('new', delivered)

    def test_unchanged_stat_resume_does_not_rehash_snapshot(self):
        from unittest.mock import patch
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('first') + claude_line('second'))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 1
        with patch.object(module, 'snapshot_hashes', wraps=module.snapshot_hashes) as hashes:
            collector.scan()
            collector.flush()
            collector.scan()
            collector.flush()
            self.assertEqual(hashes.call_count, 1)

    def test_changed_stat_during_hash_is_refused(self):
        from unittest.mock import patch
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('first'))
        observed = transcript.stat()
        original = type(transcript).open
        def changing(path, *args, **kwargs):
            source = original(path, *args, **kwargs)
            if path != transcript or args != ('rb',):
                return source
            from unittest.mock import MagicMock
            wrapped = MagicMock(wraps=source)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = lambda *exc: source.close()
            def read(size):
                data = source.read(size)
                with original(transcript, 'w') as output:
                    output.write(claude_line('other'))
                return data
            wrapped.read.side_effect = read
            return wrapped
        with patch.object(type(transcript), 'open', changing):
            with self.assertRaises(module.CollectorRuntimeError):
                module.snapshot_hashes(transcript, observed, observed.st_size, None, float('inf'))

    def test_hash_reads_old_prefix_and_new_snapshot_once(self):
        import hashlib
        from unittest.mock import patch, MagicMock
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        payload = b'a' * (1024 * 1024 + 31) + b'b' * 53
        prefix_size = len(payload) - 53
        transcript.write_bytes(payload)
        original = type(transcript).open
        reads = []
        def counted(path, *args, **kwargs):
            source = original(path, *args, **kwargs)
            wrapped = MagicMock(wraps=source)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = lambda *exc: source.close()
            def read(size):
                data = source.read(size)
                reads.append(len(data))
                return data
            wrapped.read.side_effect = read
            return wrapped
        with patch.object(type(transcript), 'open', counted):
            full, prefix = module.snapshot_hashes(transcript, transcript.stat(), len(payload), prefix_size, float('inf'))
        self.assertEqual(sum(reads), len(payload))
        self.assertEqual(len(reads), 3)
        self.assertEqual(full, hashlib.sha256(payload).hexdigest())
        self.assertEqual(prefix, hashlib.sha256(payload[:prefix_size]).hexdigest())

    def test_touch_during_scan_preserves_completed_progress(self):
        from unittest.mock import patch
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('first') + claude_line('second'))
        collector = self.collector()
        self.addCleanup(collector.close)
        original = collector.privacy.apply
        def touch(content):
            os.utime(transcript, None)
            return original(content)
        with patch.object(collector.privacy, 'apply', side_effect=touch):
            self.assertFalse(collector.scan()['scan_complete'])
        collector.flush()
        row = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertEqual(row['scanned_offset'], transcript.stat().st_size)
        self.assertTrue(row['status'].startswith('scanning-'))
        self.assertIsNotNone(row['snapshot_sha256'])
        self.assertIsNone(row['snapshot_stat'])
        from collector import collector as module
        with patch.object(module, 'snapshot_hashes', wraps=module.snapshot_hashes) as hashes:
            self.assertTrue(collector.scan()['scan_complete'])
            self.assertEqual(hashes.call_count, 1)
        self.assertEqual(collector.db.execute('SELECT status FROM files').fetchone()[0], 'ok')
        self.assertEqual(sum(len(batch['events']) for batch in AckServer.received), 2)

    def test_expired_snapshot_budget_records_failure_without_reference(self):
        from unittest.mock import patch
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('first'))
        collector = self.collector()
        self.addCleanup(collector.close)
        original = module.snapshot_hashes
        def expired(path, stat, size, prefix_size, deadline):
            return original(path, stat, size, prefix_size, 0)
        with patch.object(module, 'snapshot_hashes', side_effect=expired):
            with self.assertRaisesRegex(module.CollectorRuntimeError, 'source_snapshot_deadline'):
                collector.scan()
        self.assertIsNone(collector.db.execute('SELECT * FROM files').fetchone())
        self.assertEqual(collector.db.execute("SELECT value FROM meta WHERE key='last_scan_complete'").fetchone()[0], '0')
        self.assertEqual(collector.pending_envelopes(), [])

    def _assert_preserved_mtime_replacement_revisited(self, *, completed):
        transcript = self.root / 'session.jsonl'
        first, last = claude_line('a' * 5000), claude_line('z' * 5000)
        transcript.write_text(first + claude_line('old middle') + last)
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 10 if completed else 2
        collector.scan()
        collector.flush()
        observed = transcript.stat()
        replacement = self.root / 'replacement.tmp'
        replacement.write_text(first + claude_line('new middle') + last)
        os.utime(replacement, ns=(observed.st_atime_ns, observed.st_mtime_ns))
        replacement.replace(transcript)
        self.assertEqual(transcript.stat().st_mtime_ns, observed.st_mtime_ns)
        self.assertEqual(transcript.stat().st_size, observed.st_size)
        for _ in range(3):
            collector.scan()
            collector.flush()
        delivered = [e['content']['message']['content'] for batch in AckServer.received for e in batch['events']]
        self.assertIn('new middle', delivered)

    def test_partial_preserved_mtime_replacement_revisits_middle(self):
        self._assert_preserved_mtime_replacement_revisited(completed=False)

    def test_completed_preserved_mtime_replacement_revisits_middle(self):
        self._assert_preserved_mtime_replacement_revisited(completed=True)

    def test_appends_during_every_snapshot_read_allow_bounded_progress(self):
        from unittest.mock import patch, MagicMock
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(''.join(claude_line('original ' + str(i)) for i in range(4)))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 1
        original_open = type(transcript).open
        original_hashes = module.snapshot_hashes
        appended = 0
        def growing_open(path, *args, **kwargs):
            source = original_open(path, *args, **kwargs)
            if path != transcript or args != ('rb',):
                return source
            wrapped = MagicMock(wraps=source)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = lambda *exc: source.close()
            def read(size):
                nonlocal appended
                data = source.read(size)
                appended += 1
                with original_open(transcript, 'a') as output:
                    output.write(claude_line('append ' + str(appended)))
                return data
            wrapped.read.side_effect = read
            return wrapped
        def growing_hashes(*args, **kwargs):
            with patch.object(type(transcript), 'open', growing_open):
                return original_hashes(*args, **kwargs)
        offsets = []
        with patch.object(module, 'snapshot_hashes', side_effect=growing_hashes):
            for _ in range(4):
                collector.scan()
                collector.flush()
                offsets.append(collector.db.execute('SELECT scanned_offset FROM files').fetchone()[0])
        delivered = [e['content']['message']['content'] for batch in AckServer.received for e in batch['events']]
        self.assertEqual(delivered, ['original ' + str(i) for i in range(4)])
        self.assertGreater(appended, 3)
        self.assertEqual(offsets, sorted(set(offsets)))

    def test_snapshot_final_read_must_fit_existing_deadline(self):
        from unittest.mock import patch, MagicMock
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('first'))
        original_open = type(transcript).open
        clock = [0.0]
        def slow_open(path, *args, **kwargs):
            source = original_open(path, *args, **kwargs)
            wrapped = MagicMock(wraps=source)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = lambda *exc: source.close()
            def read(size):
                data = source.read(size)
                clock[0] = 2.0
                return data
            wrapped.read.side_effect = read
            return wrapped
        with patch.object(type(transcript), 'open', slow_open), patch.object(module.time, 'monotonic', side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(module.CollectorRuntimeError, 'source_snapshot_deadline'):
                module.snapshot_hashes(transcript, transcript.stat(), transcript.stat().st_size, None, 1.0)

    def test_snapshot_deadline_advances_fair_cursor_to_quiet_file(self):
        from unittest.mock import patch
        from collector import collector as module
        active = self.root / 'a.jsonl'
        quiet = self.root / 'z.jsonl'
        active.write_text(claude_line('large' * 300000))
        quiet.write_text(claude_line('quiet evidence'))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 1
        collector._set_meta('scan_next_lane', 'fair')
        collector.db.commit()
        original = module.snapshot_hashes
        def budgeted(path, stat, size, prefix_size, deadline):
            return original(path, stat, size, prefix_size, 0 if path == active else deadline)
        with patch.object(module, 'snapshot_hashes', side_effect=budgeted):
            with self.assertRaisesRegex(module.CollectorRuntimeError, 'source_snapshot_deadline'):
                collector.scan()
            collector.scan()
            collector.flush()
        delivered = [e['content']['message']['content'] for batch in AckServer.received for e in batch['events']]
        self.assertEqual(delivered, ['quiet evidence'])
        self.assertIsNone(collector.db.execute('SELECT * FROM files WHERE path=?', (str(active),)).fetchone())

    def test_growth_confirmation_refuses_changed_observed_prefix(self):
        from unittest.mock import patch, MagicMock
        from collector import collector as module
        transcript = self.root / 'session.jsonl'
        transcript.write_text(claude_line('old middle'))
        observed = transcript.stat()
        original_open = type(transcript).open
        reads = []
        def rewriting_open(path, *args, **kwargs):
            source = original_open(path, *args, **kwargs)
            wrapped = MagicMock(wraps=source)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = lambda *exc: source.close()
            def read(size):
                data = source.read(size)
                reads.append(len(data))
                if len(reads) == 1:
                    with original_open(transcript, 'w') as output:
                        output.write(claude_line('new middle') + claude_line('appended'))
                return data
            wrapped.read.side_effect = read
            return wrapped
        with patch.object(type(transcript), 'open', rewriting_open):
            with self.assertRaisesRegex(module.CollectorRuntimeError, 'source_snapshot_changed'):
                module.snapshot_hashes(transcript, observed, observed.st_size, None, float('inf'))
        self.assertEqual(reads, [observed.st_size, observed.st_size])

    def test_time_bounded_appending_scan_keeps_unverified_prefix_progress(self):
        import time
        from unittest.mock import patch
        transcript = self.root / 'session.jsonl'
        transcript.write_text(''.join(claude_line('original ' + str(i)) for i in range(3)))
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_seconds = 0.1
        original = collector.privacy.apply
        appended = 0
        def slow_append(content):
            nonlocal appended
            appended += 1
            with transcript.open('a') as output:
                output.write(claude_line('append ' + str(appended)))
            time.sleep(0.11)
            return original(content)
        offsets = []
        with patch.object(collector.privacy, 'apply', side_effect=slow_append):
            for _ in range(3):
                result = collector.scan()
                self.assertFalse(result['scan_complete'])
                collector.flush()
                row = collector.db.execute('SELECT * FROM files').fetchone()
                offsets.append(row['scanned_offset'])
                self.assertIsNotNone(row['snapshot_sha256'])
                self.assertIsNone(row['snapshot_stat'])
        delivered = [e['content']['message']['content'] for batch in AckServer.received for e in batch['events']]
        self.assertEqual(delivered, ['original ' + str(i) for i in range(3)])
        self.assertEqual(offsets, sorted(set(offsets)))

    def test_unverified_checkpoint_rewrite_restarts_before_cursor_reuse(self):
        from unittest.mock import patch
        transcript = self.root / 'session.jsonl'
        first, last = claude_line('a' * 5000), claude_line('z' * 5000)
        transcript.write_text(first + claude_line('old middle') + last)
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 2
        original = collector.privacy.apply
        def touch(content):
            os.utime(transcript, None)
            return original(content)
        with patch.object(collector.privacy, 'apply', side_effect=touch):
            self.assertFalse(collector.scan()['scan_complete'])
        collector.flush()
        row = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertIsNone(row['snapshot_stat'])
        self.assertEqual(row['scanned_offset'], len((first + claude_line('old middle')).encode()))
        transcript.write_text(first + claude_line('new middle') + last)
        os.utime(transcript, ns=(row['mtime_ns'], row['mtime_ns']))
        collector.close()
        collector = self.collector()
        self.addCleanup(collector.close)
        collector.max_scan_records = 2
        collector.scan()
        collector.flush()
        delivered = [e['content']['message']['content'] for batch in AckServer.received for e in batch['events']]
        self.assertIn('new middle', delivered)
        self.assertNotIn('z' * 5000, delivered)
