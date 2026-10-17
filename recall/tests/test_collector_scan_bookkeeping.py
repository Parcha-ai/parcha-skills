"""Native scan progress does not need the retired disappearance membership set."""
import unittest

from privacy.policy import PrivacyPolicy
import test_collector_archive_dedup as fixtures


class ScanBookkeepingTests(unittest.TestCase):
    setUp = fixtures.ArchiveDedupTest.setUp
    collector = fixtures.ArchiveDedupTest.collector

    def trace(self, collector):
        statements = []
        collector.db.set_trace_callback(statements.append)
        return statements

    def assert_no_scratch_work(self, statements):
        redundant = [sql for sql in statements if
                     'SELECT native_id FROM scan_members' in sql or
                     'INSERT OR IGNORE INTO scan_members' in sql]
        self.assertEqual(redundant, [])

    def test_bounded_resume_append_and_ack_replay_without_membership_work(self):
        self.path.write_text(''.join(fixtures.line(f'record {i}') for i in range(5)))
        collector = self.collector(archive_workers=1, max_scan_records=2)
        statements = self.trace(collector)
        first = collector.scan()
        collector.flush()
        state = dict(collector.db.execute('SELECT * FROM files').fetchone())
        self.assertFalse(first['scan_complete'])
        self.assertGreater(state['committed_offset'], 0)
        self.assertLess(state['committed_offset'], self.path.stat().st_size)
        # An old version's durable scratch rows must not affect resumed capture.
        collector.db.execute('INSERT INTO scan_members VALUES (?,?)',
                             (str(self.path), 'legacy-unused-member'))
        collector.db.commit()
        collector.close()
        resumed = self.collector(archive_workers=1, max_scan_records=2)
        resumed.db.set_trace_callback(statements.append)
        for _ in range(3):
            resumed.scan()
            resumed.flush()
        self.assertEqual(len(self.writer.events), 5)
        self.assertEqual(len({e['native_id'] for e in self.writer.events}), 5)
        self.assertEqual(len(self.archive.calls), 5)
        self.assertEqual(resumed.db.execute('SELECT status FROM files').fetchone()[0], 'ok')
        original = [dict(r) for r in resumed.db.execute('SELECT * FROM active_records ORDER BY start_offset')]
        with self.path.open('a') as stream:
            stream.write(fixtures.line('appended'))
        resumed.scan()
        resumed.flush()
        self.assertEqual(len(self.writer.events), 6)
        self.assertEqual(original, [dict(r) for r in resumed.db.execute('SELECT * FROM active_records ORDER BY start_offset')][:5])
        # Force full ACK reconciliation rather than the unchanged-file shortcut.
        resumed.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
        resumed.db.commit()
        for _ in range(4):
            resumed.scan()
            resumed.flush()
        self.assertEqual(len(self.writer.events), 6)
        self.assertEqual(len(self.archive.calls), 6)
        self.assertEqual(resumed.db.execute('SELECT committed_offset FROM files').fetchone()[0], self.path.stat().st_size)
        self.assert_no_scratch_work(statements)

    def test_collapsed_and_private_rows_do_not_create_scratch_members(self):
        self.path.write_text(fixtures.line('same') * 5 +
                             fixtures.line('api_key=synthetic-secret-drop-canary-91') +
                             fixtures.line('safe neighbor'))
        collector = self.collector(archive_workers=1, privacy=PrivacyPolicy(mode='drop'))
        statements = self.trace(collector)
        result = collector.scan()
        collector.flush()
        self.assertEqual(result['records_collapsed'], 2)
        self.assertEqual(result['privacy']['actions']['drop'], 1)
        self.assertEqual(len(self.writer.events), 4)
        self.assertNotIn('synthetic-secret-drop-canary-91', str(self.writer.events))
        self.assertEqual(collector.db.execute('SELECT committed_offset FROM files').fetchone()[0], self.path.stat().st_size)
        self.assert_no_scratch_work(statements)

    def test_legacy_scratch_path_rebind_and_collision_are_preserved(self):
        collector = self.collector(archive_workers=1)
        old, new = str(self.path), str(self.root / 'renamed.jsonl')
        collector.db.execute('INSERT INTO scan_members VALUES (?,?)', (old, 'legacy'))
        self.assertTrue(collector._rebind_codex_path(old, new))
        self.assertEqual(tuple(collector.db.execute('SELECT * FROM scan_members').fetchone()), (new, 'legacy'))
        self.assertFalse(collector._rebind_codex_path(old, new))
