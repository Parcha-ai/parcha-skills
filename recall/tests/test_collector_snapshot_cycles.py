"""Snapshot refusal reports degraded health without restarting watch."""
import contextlib
import io
import json
import os
import unittest
from unittest import mock

from collector import cli, collector as module
import test_collector_archive_cycles
from test_collector_archive_dedup import line


class SnapshotCycleTests(unittest.TestCase):
    setUp = test_collector_archive_cycles.ArchiveCycleTests.setUp
    open_collector = test_collector_archive_cycles.ArchiveCycleTests.open_collector

    def test_deadline_preserves_ack_and_snapshot_then_recovers(self):
        self.path.write_text(line('captured'))
        cli._run_once(self.collector, self.writer, 'claude')
        before = dict(self.collector.db.execute('SELECT * FROM files').fetchone())
        with self.path.open('a') as stream:
            stream.write(line('new tail'))
        original = module.snapshot_hashes

        def expired(path, stat, size, prefix_size, deadline):
            return original(path, stat, size, prefix_size, 0)

        with mock.patch.object(module, 'snapshot_hashes', side_effect=expired):
            result = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(result['scan']['error_code'], 'source_snapshot_deadline')
        self.assertFalse(result['scan']['scan_complete'])
        self.assertEqual(result['flush']['status'], 'skipped')
        self.assertEqual(dict(self.collector.db.execute('SELECT * FROM files').fetchone()), before)
        self.assertEqual(len(self.writer.events), 1)
        report = self.writer.report_health.call_args.args[0]
        self.assertEqual(report['status'], 'degraded')
        self.assertEqual(report['last_error_code'], 'source_snapshot_deadline')
        recovered = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(recovered['flush']['acked'], 1)
        self.assertIsNone(recovered['doctor']['last_error_code'])
        self.assertEqual(len(self.writer.events), 2)
        self.assertEqual({event['kind'] for event in self.writer.events}, {'transcript_record'})

    def test_watch_waits_after_changed_snapshot_then_recovers_without_restart(self):
        self.path.write_text(line('original'))
        original = module.snapshot_hashes
        calls = []
        waits = []

        def changed(path, stat, size, prefix_size, deadline):
            calls.append(path)
            if len(calls) == 1:
                replacement = self.root / 'replacement.tmp'
                replacement.write_text(line('replacement'))
                replacement.replace(path)
            return original(path, stat, size, prefix_size, deadline)

        def sleep(seconds):
            waits.append(seconds)
            if len(waits) == 2:
                raise KeyboardInterrupt

        argv = ['collector', 'watch', '--harness', 'claude', '--root', str(self.root),
                '--source-id', 'claude:test', '--spool', str(self.spool),
                '--endpoint', self.endpoint, '--token-file', 'unused', '--interval', '30']
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch('sys.argv', argv), \
                mock.patch.object(cli, 'token_from_file', return_value='fixture'), \
                mock.patch.object(cli, 'Collector', return_value=self.collector), \
                mock.patch.object(module, 'snapshot_hashes', side_effect=changed), \
                mock.patch.object(cli.time, 'sleep', side_effect=sleep), \
                contextlib.redirect_stdout(output):
            cli.main()
        cycles = [json.loads(row) for row in output.getvalue().splitlines()]
        self.assertEqual(len(cycles), 2)
        self.assertEqual(cycles[0]['scan']['error_code'], 'source_snapshot_changed')
        self.assertEqual(cycles[0]['doctor']['acked'], 0)
        self.assertEqual(cycles[1]['flush']['acked'], 1)
        self.assertEqual(waits, [30, 30])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(self.writer.events), 1)
