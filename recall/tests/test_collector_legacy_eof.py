"""Legacy EOF cursors must not hide records missing from the delivery ledger."""
import unittest
from unittest import mock

from collector import collector as module
import test_collector_archive_dedup as fixtures

line = fixtures.line


class LegacyEofTests(unittest.TestCase):
    setUp = fixtures.ArchiveDedupTest.setUp
    collector = fixtures.ArchiveDedupTest.collector

    def seed_legacy_eof(self, collector):
        stat = self.path.stat()
        collector.db.execute('INSERT INTO files(path,size,mtime_ns,fingerprint,scanned_offset,committed_offset,status,last_scan_id) VALUES(?,?,?,?,?,?,?,?)',
                             (str(self.path), stat.st_size, stat.st_mtime_ns,
                              module.fingerprint(self.path, stat.st_size), stat.st_size,
                              stat.st_size, 'ok', 'legacy'))
        collector.db.commit()

    def test_legacy_eof_reconciles_missing_record_once_preserving_existing_ack(self):
        self.path.write_text(line('already acknowledged') + line('missing historical record'))
        collector = self.collector(archive_workers=1)
        collector.scan()
        collector.flush()
        rows = list(collector.db.execute('SELECT * FROM active_records ORDER BY start_offset'))
        preserved = dict(rows[0])
        missing = rows[1]['native_id']
        # Reproduce a legacy completed cursor with only the first record's ledger.
        for table in ('active_records', 'record_generations', 'outbox'):
            collector.db.execute(f'DELETE FROM {table} WHERE native_id=?', (missing,))
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
        collector.db.commit()
        self.archive.calls.clear()
        self.writer.events.clear()

        result = collector.scan()
        self.assertEqual(result['records_queued'], 1)
        self.assertEqual(collector.flush()['acked'], 1)
        self.assertEqual([e['native_id'] for e in self.writer.events], [missing])
        self.assertEqual(len(self.archive.calls), 1)
        self.assertEqual(dict(collector.db.execute('SELECT * FROM active_records WHERE native_id=?',
                                                  (preserved['native_id'],)).fetchone()), preserved)
        state = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertIsNotNone(state['snapshot_sha256'])
        self.assertIsNotNone(state['snapshot_stat'])
        self.assertEqual(state['committed_offset'], self.path.stat().st_size)
        self.assertEqual(collector.scan()['records_queued'], 0)
        self.assertEqual(collector.flush()['acked'], 0)
        self.assertEqual(len(self.archive.calls), 1)

    def test_bounded_reconciliation_resumes_after_restart_without_duplicate_archive(self):
        self.path.write_text(''.join(line(f'historical record {i}') for i in range(3)))
        collector = self.collector(archive_workers=1, max_scan_records=1)
        self.seed_legacy_eof(collector)
        result = collector.scan()
        self.assertFalse(result['scan_complete'])
        self.assertEqual(result['records_queued'], 1)
        collector.flush()
        state = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertEqual(state['status'], 'scanning-full')
        self.assertLess(state['scanned_offset'], self.path.stat().st_size)
        collector.close()
        resumed = self.collector(archive_workers=1, max_scan_records=1)
        for _ in range(3):
            resumed.scan()
            resumed.flush()
        self.assertEqual(len(self.writer.events), 3)
        self.assertEqual(len({e['native_id'] for e in self.writer.events}), 3)
        self.assertEqual(len(self.archive.calls), 3)
        self.assertEqual(resumed.db.execute('SELECT status FROM files').fetchone()[0], 'ok')
        self.assertEqual(resumed.scan()['records_queued'], 0)

    def test_archive_failure_does_not_certify_legacy_eof(self):
        self.path.write_text(line('not archived yet'))
        collector = self.collector(archive_workers=1)
        self.seed_legacy_eof(collector)
        self.archive.fail = True
        with self.assertRaisesRegex(module.CollectorRuntimeError, 'archive_unavailable'):
            collector.scan()
        state = collector.db.execute('SELECT * FROM files').fetchone()
        self.assertIsNone(state['snapshot_sha256'])
        self.assertEqual(collector.pending_envelopes(), [])
        self.archive.fail = False
        self.assertEqual(collector.scan()['records_queued'], 1)
        self.assertEqual(collector.flush()['acked'], 1)

    def test_completed_ack_legacy_reconciliation_does_not_repeat_delivery(self):
        self.path.write_text(line('already captured'))
        collector = self.collector(archive_workers=1)
        collector.scan()
        collector.flush()
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
        collector.db.commit()
        self.archive.calls.clear()
        before = list(self.writer.events)
        self.assertEqual(collector.scan()['records_queued'], 0)
        self.assertEqual(collector.flush()['acked'], 0)
        self.assertEqual(self.archive.calls, [])
        self.assertEqual(self.writer.events, before)
        # The completed proof permits the existing unchanged fast path.
        with mock.patch.object(module, 'snapshot_hashes', side_effect=AssertionError('unexpected rescan')):
            self.assertEqual(collector.scan()['records_queued'], 0)


if __name__ == '__main__':
    unittest.main()
