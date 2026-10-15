"""ACKed transcript rescans must not repeat immutable archive requests."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from collector.collector import Collector, CollectorRuntimeError, fingerprint


class Archive:
    def __init__(self):
        self.calls = []
        self.fail = False

    def put_raw(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise OSError('archive unavailable')
        digest = hashlib.sha256(kwargs['payload']).hexdigest()
        return dict(contract='recall.artifact-ref.v1', schema_version=1,
                    tenant_id=kwargs['tenant_id'], source_id=kwargs['source_id'],
                    artifact_id='art_' + digest[:32], storage_backend='s3',
                    object_key='objects/aa/' + digest, content_sha256=digest,
                    size_bytes=len(kwargs['payload']), media_type=kwargs['media_type'],
                    encryption='sse-s3', version_id='fixture', created_at=kwargs['created_at'])


class Writer:
    def __init__(self):
        self.events = []

    def ingest(self, events):
        self.events.extend(events)
        return dict(status='committed', inserted=len(events), duplicate_events=0,
                    receipts=[f"recall://{e['source_id']}/{e['native_id']}?rev=1" for e in events],
                    replay=False)


def line(text):
    return json.dumps(dict(type='user', timestamp='2026-10-14T00:00:00Z',
                           message=dict(content=text))) + '\n'


class ArchiveDedupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'sessions'
        self.root.mkdir()
        self.path = self.root / 'session.jsonl'
        self.archive = Archive()
        self.writer = Writer()

    def collector(self, **options):
        collector = Collector(root=self.root, harness='claude', source_id='claude:test',
                              tenant_id='tenant:test', spool_path=Path(self.tmp.name) / 'spool.db',
                              endpoint='http://127.0.0.1:1', token='fixture',
                              archive=self.archive, brain_writer=self.writer, **options)
        self.addCleanup(collector.close)
        return collector

    def touch(self):
        stat = self.path.stat()
        os.utime(self.path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 3_600_000_000_000))

    def test_healthy_unlink_retains_acknowledged_history_across_restart(self):
        self.path.write_text(line('retained first') + line('retained second'))
        collector = self.collector(archive_workers=1)
        collector.scan()
        collector.flush()
        original_events = list(self.writer.events)
        before_archive = len(self.archive.calls)
        self.path.unlink()
        result = collector.scan()
        self.assertTrue(result['scan_complete'])
        self.assertEqual(result['tombstones_queued'], 0)
        self.assertEqual(collector.pending_envelopes(), [])
        self.assertEqual(collector.doctor()['records'], 2)
        collector.close()
        resumed = self.collector(archive_workers=1)
        self.assertEqual(resumed.scan()['tombstones_queued'], 0)
        resumed.flush()
        self.assertEqual(self.writer.events, original_events)
        self.assertEqual(len(self.archive.calls), before_archive)
        self.path.write_text(line('retained first') + line('retained second'))
        self.assertEqual(resumed.scan()['records_queued'], 0)
        self.assertEqual(resumed.pending_envelopes(), [])

    def test_pending_legacy_marker_is_quarantined_without_ack_or_blocking_live_rows(self):
        self.path.write_text(line('original'))
        collector = self.collector(archive_workers=1, defer_scan_flush=True, batch_size=1)
        collector.scan()
        collector.flush()
        original = self.writer.events[0]
        marker = collector._envelope(self.path, original['native_id'], 'tombstone',
            {'target_native_id': original['native_id'], 'deletion_id': 'legacy-cleanup'},
            original['occurred_at'], 0, self.path.stat().st_size)
        collector._queue(self.path, marker, self.path.stat().st_size)
        collector.db.commit()
        marker_json = collector.db.execute("SELECT envelope_json FROM outbox WHERE state='pending'").fetchone()[0]
        with self.path.open('a') as stream:
            stream.write(line('new ordinary record'))
        collector.scan()
        before_archive = len(self.archive.calls)
        result = collector.flush()
        self.assertEqual(result['acked'], 1)
        self.assertEqual(result['policy_quarantined'], 1)
        self.assertEqual([event['kind'] for event in self.writer.events], ['transcript_record', 'transcript_record'])
        self.assertEqual(len(self.archive.calls), before_archive)
        row = collector.db.execute("SELECT * FROM outbox WHERE state='dead'").fetchone()
        self.assertEqual(row['envelope_json'], marker_json)
        self.assertIsNone(row['receipt'])
        self.assertIsNone(row['acked_at'])
        self.assertEqual(collector.doctor()['dead_letters'][0]['error_code'], 'filesystem_deletion_forbidden')
        collector.close()
        resumed = self.collector(archive_workers=1, defer_scan_flush=True)
        self.assertEqual(resumed.flush()['acked'], 0)
        self.assertEqual(resumed.doctor()['dead'], 1)
        self.assertEqual(resumed.pending_envelopes(), [])

    def test_nonobject_dead_envelope_still_uses_existing_payload_recovery(self):
        self.path.write_text(line('recoverable source record'))
        collector = self.collector(archive_workers=1, defer_scan_flush=True)
        collector.scan()
        for malformed in (None, [], {'kind': 'tombstone', 'provenance': None}):
            with self.subTest(malformed=malformed):
                collector.db.execute("UPDATE outbox SET state='dead',envelope_json=?", (json.dumps(malformed),))
                collector.db.commit()
                self.assertEqual(collector.recover_dead_payloads()['recovered'], 1)
                self.assertEqual(collector.pending_envelopes()[0]['kind'], 'transcript_record')

    def test_acked_touch_rescan_skips_archive_scalar_parallel_and_bulk(self):
        for workers, bulk in ((1, False), (2, False), (2, True)):
            with self.subTest(workers=workers, bulk=bulk):
                self.path.write_text(line(f'first-{workers}-{bulk}') + line('second'))
                collector = self.collector(archive_workers=workers, bulk_manifest_archive=bulk)
                collector.scan()
                collector.flush()
                before = len(self.archive.calls)
                rows = [tuple(r) for r in collector.db.execute('SELECT * FROM outbox')]
                # Exercise per-record ACK reuse, not the completed-file skip.
                self.path.write_text(self.path.read_text().replace('": ', '" :', 1))
                self.touch()
                summary = collector.scan()
                self.assertEqual(len(self.archive.calls), before)
                self.assertEqual(summary['records_queued'], 0)
                self.assertEqual(summary['tombstones_queued'], 0)
                self.assertEqual(rows, [tuple(r) for r in collector.db.execute('SELECT * FROM outbox')])
                self.assertEqual(collector.db.execute('SELECT scanned_offset FROM files').fetchone()[0], self.path.stat().st_size)
                collector.close()

    def test_same_size_middle_edit_is_not_hidden_by_sampled_fingerprint(self):
        self.path.write_text(line('h' * 5000) + line('middle-A') + line('t' * 5000))
        collector = self.collector(archive_workers=1)
        collector.scan()
        collector.flush()
        old_fingerprint = fingerprint(self.path, self.path.stat().st_size)
        old_ids = [e['native_id'] for e in self.writer.events]
        before = len(self.archive.calls)
        self.path.write_text(self.path.read_text().replace('middle-A', 'middle-B'))
        self.touch()
        self.assertEqual(fingerprint(self.path, self.path.stat().st_size), old_fingerprint)
        self.assertEqual(collector.scan()['records_queued'], 1)
        self.assertEqual(len(self.archive.calls), before + 1)
        collector.flush()
        self.assertEqual(self.writer.events[-1]['native_id'], old_ids[1])
        self.assertEqual(self.writer.events[-1]['content']['_recall_collector_generation'], 1)
        self.assertIn(b'middle-B', self.archive.calls[-1]['payload'])

    def test_pending_and_dead_rows_keep_archive_failure_and_retry_semantics(self):
        for state in ('pending', 'dead'):
            with self.subTest(state=state):
                self.path.write_text(line(state))
                collector = self.collector(archive_workers=1, defer_scan_flush=True)
                collector.scan()
                collector.db.execute('UPDATE outbox SET state=?', (state,))
                collector.db.commit()
                self.touch()
                self.archive.fail = True
                with self.assertRaises(CollectorRuntimeError):
                    collector.scan()
                self.archive.fail = False
                collector.scan()
                self.assertEqual(collector.db.execute('SELECT state FROM outbox ORDER BY id DESC LIMIT 1').fetchone()[0], state)
                collector.close()

    def test_bounded_truncation_restart_retains_captured_record(self):
        self.path.write_text(''.join(line(str(i)) for i in range(4)))
        collector = self.collector(archive_workers=1, max_scan_records=2)
        collector.scan()
        collector.scan()
        collector.flush()
        before = len(self.archive.calls)
        self.path.write_text(''.join(line(str(i)) for i in range(3)))
        self.touch()
        collector.scan()
        collector.close()
        resumed = self.collector(archive_workers=1, max_scan_records=2)
        summary = resumed.scan()
        self.assertEqual(summary['tombstones_queued'], 0)
        self.assertEqual(len(self.archive.calls), before)
        self.assertEqual(resumed.doctor()['records'], 4)
        self.assertEqual(resumed.pending_envelopes(), [])

    def test_reappearing_file_keeps_original_generation_and_archive(self):
        self.path.write_text(line('restore'))
        collector = self.collector(archive_workers=1)
        collector.scan()
        collector.flush()
        self.path.unlink()
        collector.scan()
        collector.flush()
        before = len(self.archive.calls)
        self.path.write_text(line('restore'))
        self.assertEqual(collector.scan()['restored_records_queued'], 0)
        self.assertEqual(len(self.archive.calls), before)
        collector.flush()
        self.assertEqual(self.writer.events[-1]['content']['_recall_collector_generation'], 0)

    def test_oversized_projection_keeps_existing_archive_path(self):
        self.path.write_text(line('large ' + 'x' * 6000))
        collector = self.collector(archive_workers=1)
        with mock.patch('collector.collector.MAX_BATCH_BYTES', 4000):
            collector.scan()
            collector.flush()
            self.assertEqual(self.writer.events[-1]['content']['content_fidelity'], 'head_tail')
            before = len(self.archive.calls)
            # Force a real rescan while preserving parsed content and offsets.
            self.path.write_text(self.path.read_text().replace('": ', '" :', 1))
            self.touch()
            self.assertEqual(collector.scan()['records_queued'], 0)
            self.assertEqual(len(self.archive.calls), before + 2)

    def test_missing_generation_or_ack_proof_does_not_skip_archive(self):
        for corruption in ('DELETE FROM record_generations', 'UPDATE outbox SET receipt=NULL'):
            with self.subTest(corruption=corruption):
                self.path.write_text(line('proof-' + corruption))
                collector = self.collector(archive_workers=1)
                collector.scan()
                collector.flush()
                collector.db.execute(corruption)
                collector.db.commit()
                before = len(self.archive.calls)
                # Same record/hash/offsets; only the whole-file digest changes.
                self.path.write_text(self.path.read_text().replace('": ', '" :', 1))
                self.touch()
                collector.scan()
                self.assertEqual(len(self.archive.calls), before + 1)
                collector.close()


if __name__ == '__main__':
    unittest.main()
