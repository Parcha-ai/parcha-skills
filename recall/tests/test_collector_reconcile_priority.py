"""Fair reconciliation spends legacy-file slots on missing delivery first."""
import os
import unittest

from collector import collector as module
import test_collector_archive_dedup as fixtures


class ReconcilePriorityTests(unittest.TestCase):
    setUp = fixtures.ArchiveDedupTest.setUp
    collector = fixtures.ArchiveDedupTest.collector

    def legacy_file(self, collector, name, text):
        path = self.root / name
        path.write_text(fixtures.line(text))
        stat = path.stat()
        collector.db.execute(
            'INSERT INTO files(path,size,mtime_ns,fingerprint,scanned_offset,committed_offset,status,last_scan_id) VALUES(?,?,?,?,?,?,?,?)',
            (str(path), stat.st_size, stat.st_mtime_ns,
             module.fingerprint(path, stat.st_size), stat.st_size, stat.st_size, 'ok', 'legacy'))
        collector.db.commit()
        return path

    def test_missing_delivery_precedes_already_acknowledged_legacy_reproof(self):
        self.path = self.root / 'a-captured.jsonl'
        self.path.write_text(fixtures.line('already captured'))
        collector = self.collector(archive_workers=1, max_scan_records=1)
        collector.scan()
        collector.flush()
        before = list(self.writer.events)
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
        self.legacy_file(collector, 'z-missing.jsonl', 'missing historical record')
        collector.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('scan_next_lane','fair')")
        collector.db.execute("DELETE FROM meta WHERE key='scan_resume_after_path'")
        collector.db.commit()
        self.archive.calls.clear()

        result = collector.scan()
        self.assertEqual(result['records_queued'], 1)
        self.assertEqual(collector.flush()['acked'], 1)
        self.assertEqual([e['content']['message']['content'] for e in self.writer.events[len(before):]],
                         ['missing historical record'])
        self.assertEqual(len(self.archive.calls), 1)
        self.assertEqual(collector.db.execute('SELECT snapshot_sha256 FROM files WHERE path=?',
                                             (str(self.path),)).fetchone()[0], None)

    def test_partial_resume_keeps_its_slot_between_legacy_files(self):
        partial = self.root / 'b-partial.jsonl'
        partial.write_text(fixtures.line('partial first') + fixtures.line('partial second'))
        collector = self.collector(archive_workers=1, max_scan_records=1)
        collector.scan()
        collector.flush()
        self.assertEqual(collector.db.execute('SELECT status FROM files').fetchone()[0], 'scanning-new')
        # Establish an acknowledged legacy file before the partial's slot.
        captured = self.legacy_file(collector, 'a-captured.jsonl', 'already captured')
        collector.max_scan_records = 100
        collector.scan()
        collector.flush()
        # Recreate a genuine partial using a fresh native scan of an append.
        with partial.open('a') as stream:
            stream.write(fixtures.line('partial third') + fixtures.line('partial fourth'))
        collector.max_scan_records = 1
        collector.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('scan_next_lane','fresh')")
        collector.db.commit()
        collector.scan()
        collector.flush()
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL WHERE path=?', (str(captured),))
        self.legacy_file(collector, 'z-missing.jsonl', 'missing history')
        collector.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('scan_next_lane','fair')")
        collector.db.execute("DELETE FROM meta WHERE key='scan_resume_after_path'")
        collector.db.commit()
        before = len(self.writer.events)
        collector.max_scan_records = 2

        collector.scan()
        collector.flush()
        self.assertEqual([e['content']['message']['content'] for e in self.writer.events[before:]],
                         ['missing history', 'partial fourth'])
        self.assertIsNone(collector.db.execute('SELECT snapshot_sha256 FROM files WHERE path=?',
                                              (str(captured),)).fetchone()[0])

    def test_fresh_lane_and_restart_finish_finite_backlog_without_duplicate_acks(self):
        self.path = self.root / 'a-captured.jsonl'
        self.path.write_text(fixtures.line('captured'))
        collector = self.collector(archive_workers=1, max_scan_records=1)
        collector.scan()
        collector.flush()
        collector.db.execute('UPDATE files SET snapshot_sha256=NULL,snapshot_stat=NULL')
        self.legacy_file(collector, 'y-history.jsonl', 'history one')
        self.legacy_file(collector, 'z-history.jsonl', 'history two')
        live = self.root / 'b-live.jsonl'
        live.write_text(fixtures.line('fresh append'))
        os.utime(live, ns=(2_000_000_000_000_000_000, 2_000_000_000_000_000_000))
        collector.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('scan_next_lane','fresh')")
        collector.db.commit()

        collector.scan()
        collector.flush()
        self.assertEqual(self.writer.events[-1]['content']['message']['content'], 'fresh append')
        collector.close()
        resumed = self.collector(archive_workers=1, max_scan_records=1)
        for _ in range(12):
            result = resumed.scan()
            self.assertLessEqual(result['records_queued'], 1)
            resumed.flush()
            if result['scan_complete']:
                break
        self.assertTrue(result['scan_complete'])
        self.assertCountEqual([e['content']['message']['content'] for e in self.writer.events],
                              ['captured', 'fresh append', 'history one', 'history two'])
        self.assertEqual(len(self.archive.calls), 4)
        self.assertEqual(resumed.db.execute("SELECT count(*) FROM files WHERE status!='ok' OR snapshot_sha256 IS NULL").fetchone()[0], 0)

    def test_failed_priority_archive_preserves_gap_then_recovers_once(self):
        collector = self.collector(archive_workers=1, max_scan_records=1)
        missing = self.legacy_file(collector, 'missing.jsonl', 'must be archived')
        collector.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('scan_next_lane','fair')")
        collector.db.commit()
        self.archive.fail = True
        with self.assertRaisesRegex(module.CollectorRuntimeError, 'archive_unavailable'):
            collector.scan()
        self.assertEqual(self.writer.events, [])
        self.assertIsNone(collector.db.execute('SELECT snapshot_sha256 FROM files WHERE path=?',
                                              (str(missing),)).fetchone()[0])
        self.archive.fail = False
        collector.scan()
        self.assertEqual(collector.flush()['acked'], 1)
        self.assertEqual(collector.scan()['records_queued'], 0)
        self.assertEqual(len(self.writer.events), 1)


if __name__ == '__main__':
    unittest.main()
