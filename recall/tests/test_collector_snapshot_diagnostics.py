"""Snapshot deadline metadata explains work without exposing source contents."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from collector import cli, collector as module
import test_collector_archive_cycles
from test_collector_archive_dedup import line


class SnapshotDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'private-filename-marker.jsonl'
        self.path.write_bytes(b'private-body-marker')

    def test_expired_entry_reports_only_hash_and_work_metadata(self):
        with mock.patch.object(module.time, 'monotonic', return_value=12.0):
            with self.assertRaises(module.CollectorRuntimeError) as caught:
                module.snapshot_hashes(self.path, self.path.stat(), 19, None, 10.0)
        error = caught.exception
        self.assertEqual(type(error), module.CollectorRuntimeError)
        self.assertEqual(str(error), 'source_snapshot_deadline')
        self.assertEqual(error.snapshot_diagnostics, {
            'path_sha256': hashlib.sha256(os.fsencode(self.path)).hexdigest(),
            'observed_size_bytes': 19,
            'bytes_read': 0,
            'remaining_budget_ms_at_entry': -2000.0,
            'elapsed_ms': 0.0,
            'phase': 'initial',
        })
        rendered = json.dumps(error.snapshot_diagnostics)
        self.assertNotIn('private-', rendered)
        self.assertNotIn(str(self.path), rendered)

    def test_initial_read_counts_bytes_even_when_its_deadline_check_fails(self):
        self._assert_read_deadline(grow=False)

    def test_growth_confirmation_counts_both_reads_and_keeps_observed_size(self):
        self._assert_read_deadline(grow=True)

    def _assert_read_deadline(self, *, grow):
        original_open = type(self.path).open
        clock = [100.0]
        reads = []

        def slow_open(path, *args, **kwargs):
            stream = original_open(path, *args, **kwargs)
            wrapped = mock.MagicMock(wraps=stream)
            wrapped.__enter__.return_value = wrapped
            wrapped.__exit__.side_effect = stream.__exit__

            def read(length):
                value = stream.read(length)
                reads.append(len(value))
                if grow and len(reads) == 1:
                    with original_open(path, 'ab') as append:
                        append.write(b'x')
                    clock[0] = 100.25
                else:
                    clock[0] = 102.0
                return value

            wrapped.read.side_effect = read
            return wrapped

        with mock.patch.object(type(self.path), 'open', slow_open), \
                mock.patch.object(module.time, 'monotonic', side_effect=lambda: clock[0]):
            with self.assertRaises(module.CollectorRuntimeError) as caught:
                module.snapshot_hashes(self.path, self.path.stat(), 19, None, 101.0)
        error = caught.exception
        self.assertEqual(str(error), 'source_snapshot_deadline')
        self.assertEqual(reads, [19, 19] if grow else [19])
        self.assertEqual(error.snapshot_diagnostics, {
            'path_sha256': hashlib.sha256(os.fsencode(self.path)).hexdigest(),
            'observed_size_bytes': 19,
            'bytes_read': 38 if grow else 19,
            'remaining_budget_ms_at_entry': 1000.0,
            'elapsed_ms': 2000.0,
            'phase': 'growth_confirmation' if grow else 'initial',
        })

    def test_changed_snapshot_still_refuses_without_deadline_diagnostics(self):
        observed = self.path.stat()
        replacement = self.path.with_suffix('.replacement')
        replacement.write_bytes(b'other-private-body')
        replacement.replace(self.path)
        with self.assertRaises(module.CollectorRuntimeError) as caught:
            module.snapshot_hashes(self.path, observed, observed.st_size, None, float('inf'))
        self.assertEqual(str(caught.exception), 'source_snapshot_changed')
        self.assertIsNone(caught.exception.snapshot_diagnostics)


class SnapshotCycleDiagnosticsTests(unittest.TestCase):
    setUp = test_collector_archive_cycles.ArchiveCycleTests.setUp
    open_collector = test_collector_archive_cycles.ArchiveCycleTests.open_collector

    def test_cli_emits_deadline_metadata_only_without_advancing_unverified_file(self):
        self.path = self.root / 'private-filename-marker.jsonl'
        self.path.write_text(line('private-body-marker'))
        self.collector.defer_scan_flush = False
        original = module.snapshot_hashes

        def expired(path, stat, size, prefix_size, deadline):
            return original(path, stat, size, prefix_size, 0.0)

        with mock.patch.object(module, 'snapshot_hashes', side_effect=expired):
            failed = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(failed['scan']['error_code'], 'source_snapshot_deadline')
        self.assertFalse(failed['scan']['scan_complete'])
        self.assertEqual(failed['flush'], {'status': 'skipped', 'reason': 'scan_failed'})
        self.assertEqual(failed['scan']['snapshot_diagnostics']['path_sha256'],
                         hashlib.sha256(os.fsencode(self.path)).hexdigest())
        self.assertEqual(failed['scan']['snapshot_diagnostics']['observed_size_bytes'],
                         self.path.stat().st_size)
        self.assertEqual(failed['scan']['snapshot_diagnostics']['bytes_read'], 0)
        self.assertEqual(self.writer.report_health.call_args.args[0]['status'], 'degraded')
        self.assertEqual(self.writer.events, [])
        self.assertEqual(self.calls, 0)
        for table in ('files', 'outbox', 'active_records'):
            self.assertEqual(self.collector.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0], 0)
        rendered = json.dumps(failed)
        self.assertNotIn('private-', rendered)
        self.assertNotIn(str(self.path), rendered)
        recovered = cli._run_once(self.collector, self.writer, 'claude')
        self.assertNotIn('snapshot_diagnostics', recovered['scan'])
        self.assertIsNone(recovered['doctor']['last_error_code'])
        self.assertEqual(recovered['doctor']['acked'], 1)
        self.assertEqual(len(self.writer.events), 1)


if __name__ == '__main__':
    unittest.main()
