"""Archive outages remain visible and recover at the ordinary watch interval."""
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from client.mac import CanonicalArchiveClient, MAX_CANONICAL_ARCHIVE_ATTEMPTS
from collector import cli
from collector.collector import Collector, CollectorRuntimeError
from test_collector_archive_dedup import Writer, line


class ArchiveCycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'sessions'
        self.root.mkdir()
        self.path = self.root / 'session.jsonl'
        self.spool = Path(self.temp.name) / 'spool.db'
        self.failed = False
        self.mismatch = False
        self.calls = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                fixture.calls += 1
                if fixture.failed:
                    status, result = 503, {'error': 'private provider detail'}
                else:
                    payload = base64.b64decode(body['payload_base64'])
                    digest = hashlib.sha256(payload).hexdigest()
                    status, result = 201, dict(
                        contract='recall.artifact-ref.v1', schema_version=1,
                        tenant_id=body['tenant_id'], source_id=body['source_id'],
                        artifact_id='art_' + digest[:32], storage_backend='s3',
                        object_key='objects/aa/' + digest,
                        content_sha256=('0' * 64 if fixture.mismatch else digest),
                        size_bytes=len(payload), media_type=body['media_type'],
                        encryption='sse-s3', version_id='fixture', created_at=body['created_at'])
                rendered = json.dumps(result).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(rendered)))
                self.end_headers()
                self.wfile.write(rendered)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'
        self.archive = CanonicalArchiveClient(endpoint=self.endpoint, token='fixture',
            source_id='claude:test', tenant_id='tenant:test', principal_id='owner')
        self.writer = Writer()
        self.writer.report_health = mock.Mock(return_value={'status': 'accepted'})
        self.collector = self.open_collector()

    def open_collector(self):
        collector = Collector(root=self.root, harness='claude', source_id='claude:test',
            tenant_id='tenant:test', spool_path=self.spool, endpoint=self.endpoint,
            token='fixture', archive=self.archive, brain_writer=self.writer,
            archive_workers=1, defer_scan_flush=True)
        self.addCleanup(collector.close)
        return collector

    def test_transport_outage_preserves_ack_cursor_and_reports_then_recovers(self):
        self.path.write_text(line('already acknowledged'))
        cli._run_once(self.collector, self.writer, 'claude')
        before = dict(self.collector.db.execute('SELECT * FROM files').fetchone())
        receipt = self.collector.db.execute('SELECT receipt FROM outbox').fetchone()[0]
        with self.path.open('a') as stream:
            stream.write(line('not yet archived'))
        self.failed = True
        self.writer.report_health.side_effect = OSError('private health detail')
        calls = self.calls
        with mock.patch('client.mac.time.sleep'):
            result = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(self.calls - calls, MAX_CANONICAL_ARCHIVE_ATTEMPTS)
        self.assertFalse(result['scan']['scan_complete'])
        self.assertEqual(result['scan']['error_code'], 'archive_unavailable')
        self.assertEqual(result['flush']['status'], 'skipped')
        self.assertEqual(result['health_report'], {'schema_version': 1, 'status': 'unavailable'})
        self.assertEqual(self.writer.report_health.call_args.args[0]['status'], 'degraded')
        self.assertNotIn('private', json.dumps(result))
        self.assertEqual(len(self.writer.events), 1)
        after = dict(self.collector.db.execute('SELECT * FROM files').fetchone())
        self.assertEqual((after['scanned_offset'], after['committed_offset']),
                         (before['scanned_offset'], before['committed_offset']))
        self.collector.close()
        self.collector = self.open_collector()
        self.assertEqual(self.collector.doctor()['last_error_code'], 'archive_unavailable')
        self.assertEqual(self.collector.db.execute('SELECT receipt FROM outbox').fetchone()[0], receipt)
        self.failed = False
        self.writer.report_health.side_effect = None
        result = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(result['flush']['acked'], 1)
        self.assertIsNone(result['doctor']['last_error_code'])
        self.assertEqual(len(self.writer.events), 2)
        self.assertEqual({e['kind'] for e in self.writer.events}, {'transcript_record'})
        self.assertEqual(self.collector.doctor()['acked'], 2)

    def test_watch_survives_outage_and_waits_normal_interval_before_recovery(self):
        self.path.write_text(line('queued after recovery'))
        self.failed = True
        waits = []

        def sleep(seconds):
            if seconds != 30:
                return  # Keep the unchanged client's bounded retries fast locally.
            waits.append(seconds)
            if len(waits) == 1:
                self.failed = False
            else:
                raise KeyboardInterrupt

        argv = ['collector', 'watch', '--harness', 'claude', '--root', str(self.root),
                '--source-id', 'claude:test', '--spool', str(self.spool),
                '--endpoint', self.endpoint, '--token-file', 'unused', '--interval', '30']
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch('sys.argv', argv), \
                mock.patch.object(cli, 'token_from_file', return_value='fixture'), \
                mock.patch.object(cli, 'Collector', return_value=self.collector), \
                mock.patch.object(cli.time, 'sleep', side_effect=sleep), \
                contextlib.redirect_stdout(output):
            cli.main()
        cycles = [json.loads(row) for row in output.getvalue().splitlines()]
        self.assertEqual(len(cycles), 2)
        self.assertEqual(cycles[0]['scan']['error_code'], 'archive_unavailable')
        self.assertEqual(cycles[1]['flush']['acked'], 1)
        self.assertEqual(waits, [30, 30])
        self.assertEqual(self.calls, MAX_CANONICAL_ARCHIVE_ATTEMPTS + 1)
        self.assertEqual(len(self.writer.events), 1)

    def test_pending_success_is_not_flushed_over_failed_scan_health(self):
        self.path.write_text(line('durable pending'))
        self.collector.scan()
        with self.path.open('a') as stream:
            stream.write(line('archive unavailable'))
        self.failed = True
        with mock.patch('client.mac.time.sleep'):
            result = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(result['doctor']['pending'], 1)
        self.assertEqual(result['doctor']['acked'], 0)
        self.assertEqual(result['doctor']['last_error_code'], 'archive_unavailable')
        self.assertEqual(self.writer.events, [])
        self.assertEqual(self.writer.report_health.call_args.args[0]['status'], 'degraded')
        self.failed = False
        result = cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(result['flush']['acked'], 2)
        self.assertEqual(len({e['native_id'] for e in self.writer.events}), 2)

    def test_direct_scan_and_integrity_errors_still_raise(self):
        self.path.write_text(line('must be archived exactly'))
        self.failed = True
        with mock.patch('client.mac.time.sleep'):
            with self.assertRaisesRegex(CollectorRuntimeError, '^archive_unavailable$'):
                self.collector.scan()
        self.failed = False
        self.mismatch = True
        with self.assertRaisesRegex(CollectorRuntimeError, '^archive_reference_mismatch$'):
            cli._run_once(self.collector, self.writer, 'claude')
        self.assertEqual(self.writer.events, [])
        self.assertEqual(self.collector.pending_envelopes(), [])
        self.writer.report_health.assert_not_called()
        with mock.patch.object(self.collector, 'scan', side_effect=ValueError('fixture bug')):
            with self.assertRaisesRegex(ValueError, 'fixture bug'):
                cli._run_once(self.collector, self.writer, 'claude')

    def test_one_shot_run_reports_failure_then_exits_nonzero(self):
        self.path.write_text(line('not archived'))
        self.failed = True
        argv = ['collector', 'run', '--harness', 'claude', '--root', str(self.root),
                '--source-id', 'claude:test', '--spool', str(self.spool),
                '--endpoint', self.endpoint, '--token-file', 'unused']
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch('sys.argv', argv), \
                mock.patch.object(cli, 'token_from_file', return_value='fixture'), \
                mock.patch.object(cli, 'Collector', return_value=self.collector), \
                mock.patch('client.mac.time.sleep'), \
                contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as stopped:
                cli.main()
        self.assertEqual(stopped.exception.code, 1)
        result = json.loads(output.getvalue())
        self.assertEqual(result['scan']['error_code'], 'archive_unavailable')
        self.assertFalse(result['scan']['scan_complete'])
        self.assertEqual(result['doctor']['last_error_code'], 'archive_unavailable')
        self.assertNotIn('private provider detail', output.getvalue())
        self.assertEqual(self.writer.events, [])
