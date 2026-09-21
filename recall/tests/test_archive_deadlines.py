from __future__ import annotations

import base64
import hashlib
import io
import threading
import time
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock

import boto3

from server.recall_server import archive
from server.recall_server.archive_runtime import build_evidence_archive_store


class Body(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.timeouts = []

    def set_socket_timeout(self, value):
        self.timeouts.append(value)


class ArchiveFixture:
    def setUp(self):
        self.payload = b'private evidence'
        self.client = mock.Mock()
        self.store = archive.S3ArchiveStore(
            bucket='evidence-test', endpoint_url='https://s3.us-west-2.amazonaws.com',
            namespace_key=b'k' * 32, client=mock.Mock(), deadline_client=self.client,
        )
        request = archive.ArchiveRequest('tenant:test', 'source:test', 'native:test',
                                         'application/json', self.payload)
        self.reference = self.store._reference(
            request, content_sha256=hashlib.sha256(self.payload).hexdigest(),
            size_bytes=len(self.payload), version_id='version-test',
        )
        self.body = Body(self.payload)
        self.response = {
            'Body': self.body, 'Metadata': archive._metadata(self.reference),
            'ContentLength': len(self.payload),
        }
        self.client.get_object.return_value = self.response

    def read(self, deadline=None, reference=None):
        return self.store.read_raw_bounded(
            (reference or self.reference).to_contract(
                tenant_id='tenant:test', source_id='source:test',
                created_at='2026-09-21T00:00:00Z'),
            deadline_at=time.monotonic() + 3 if deadline is None else deadline,
        )


class ArchiveDeadlineTest(ArchiveFixture, unittest.TestCase):
    def test_success_checks_exact_bytes_closes_body_and_passes_version(self):
        self.assertEqual(self.read(), self.payload)
        self.assertTrue(self.body.closed)
        self.assertTrue(self.body.timeouts)
        self.assertTrue(all(0 < t <= archive.S3_DEADLINE_READ_TIMEOUT for t in self.body.timeouts))
        self.client.get_object.assert_called_once_with(
            Bucket='evidence-test', Key=self.reference.object_key, VersionId='version-test')
        self.store.client.get_object.assert_not_called()

    def test_expired_or_insufficient_admission_budget_does_no_io(self):
        for budget in (-1, 0.001):
            with self.subTest(budget=budget), self.assertRaises(archive.ArchiveDeadlineExceeded):
                self.read(time.monotonic() + budget)
        self.client.get_object.assert_not_called()

    def test_invalid_reference_and_large_object_do_no_io(self):
        for reference in (
            replace(self.reference, object_key='invalid'),
            replace(self.reference, size_bytes=archive.DEFAULT_MAXIMUM_BYTES + 1),
        ):
            with self.subTest(reference=reference.size_bytes), self.assertRaises(archive.ArchiveError):
                self.read(reference=reference)
        self.client.get_object.assert_not_called()

    def test_version_mismatch_is_denied_before_io(self):
        self.store.compatibility_profile = 'aws-unversioned'
        with self.assertRaises(archive.ArchiveNotFound):
            self.read()
        self.client.get_object.assert_not_called()

    def test_unconfigured_reader_does_not_use_ordinary_client(self):
        self.store.deadline_client = None
        with self.assertRaisesRegex(archive.ArchiveError, 'not configured'):
            self.read()
        self.store.client.get_object.assert_not_called()

    def test_corrupt_metadata_and_hash_close_body(self):
        for kind in ('metadata', 'size', 'digest', 'tenant'):
            self.body = Body(self.payload if kind != 'digest' else b'x' * len(self.payload))
            response = {**self.response, 'Body': self.body, 'Metadata': dict(self.response['Metadata'])}
            if kind == 'metadata':
                response['Metadata']['artifact_id'] = 'bad'
            elif kind == 'size':
                response['ContentLength'] += 1
            elif kind == 'tenant':
                response['Metadata']['tenant_scope_sha256'] = '0' * 64
            self.client.get_object.return_value = response
            with self.subTest(kind=kind), self.assertRaises(archive.ArchiveError):
                self.read()
            self.assertTrue(self.body.closed)

    def test_deadline_crossed_in_get_closes_body_without_reading(self):
        now = [10.0]
        def get(**kwargs):
            now[0] = 12.0
            return self.response
        self.client.get_object.side_effect = get
        with mock.patch.object(archive.time, 'monotonic', side_effect=lambda: now[0]):
            with self.assertRaises(archive.ArchiveDeadlineExceeded):
                self.read(deadline=11.0)
        self.assertTrue(self.body.closed)
        self.assertEqual(self.body.timeouts, [])

    def test_slow_chunk_crossing_deadline_is_rejected_and_closed(self):
        now = [10.0]
        original = self.body.read
        def read(size):
            now[0] += 1.1
            return original(size)
        self.body.read = read
        with mock.patch.object(archive.time, 'monotonic', side_effect=lambda: now[0]):
            with self.assertRaises(archive.ArchiveDeadlineExceeded):
                self.read(deadline=11.0)
        self.assertTrue(self.body.closed)

    def test_stream_failure_is_sanitized_and_closed(self):
        self.body.read = mock.Mock(side_effect=RuntimeError('secret URL and content'))
        with self.assertRaisesRegex(archive.ArchiveError, '^archive provider request failed$') as caught:
            self.read()
        self.assertTrue(caught.exception.__suppress_context__)
        self.assertTrue(self.body.closed)


def environment():
    return {
        'RECALL_EVIDENCE_ARCHIVE_BACKEND': 's3',
        'RECALL_EVIDENCE_ARCHIVE_BUCKET': 'evidence-test',
        'RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL': 'https://s3.us-west-2.amazonaws.com',
        'RECALL_EVIDENCE_ARCHIVE_REGION': 'us-west-2',
        'RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID': 'synthetic',
        'RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY': 'synthetic',
        'RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY': base64.b64encode(b'k' * 32).decode(),
    }


class ArchiveDeadlineTransportTest(ArchiveFixture, unittest.TestCase):
    # Real SDK/HTTP fault injection; no live service, credentials, or detached I/O.
    def test_dedicated_client_configuration_is_opt_in(self):
        calls = []
        factory = lambda **kwargs: calls.append(kwargs) or mock.Mock()
        plain = build_evidence_archive_store(environment(), client_factory=factory)
        self.assertIsNone(plain.deadline_client)
        self.assertEqual(len(calls), 1)
        calls.clear()
        bounded = build_evidence_archive_store(environment(), client_factory=factory, deadline_reads=True)
        self.assertEqual(len(calls), 2)
        config = calls[1]['config']
        self.assertEqual(config.retries, {'total_max_attempts': 1})
        self.assertEqual(config.connect_timeout, archive.S3_DEADLINE_CONNECT_TIMEOUT)
        self.assertEqual(config.read_timeout, archive.S3_DEADLINE_READ_TIMEOUT)
        self.assertIsNot(bounded.client, bounded.deadline_client)

    def test_real_http_stalled_headers_body_503_and_slow_drip(self):
        for stage in ('headers', 'body', '503', 'drip', 'success'):
            attempts = []
            acquired_bodies = []
            release = threading.Event()
            owner = self
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args):
                    pass
                def do_GET(self):
                    attempts.append(self.path)
                    if stage == 'headers':
                        release.wait(2)
                        return
                    self.send_response(503 if stage == '503' else 200)
                    self.send_header('Content-Length', '0' if stage == '503' else str(len(owner.payload)))
                    for key, value in owner.response['Metadata'].items():
                        self.send_header('x-amz-meta-' + key, value)
                    self.end_headers()
                    self.wfile.flush()
                    if stage == 'body':
                        release.wait(2)
                    if stage == 'success':
                        self.wfile.write(owner.payload)
                        self.wfile.flush()
                    if stage == 'drip':
                        # Every write is inside the inactivity cap, but the GET
                        # plus completed read exceeds its absolute budget.
                        for piece in (owner.payload[:5], owner.payload[5:10], owner.payload[10:]):
                            if release.wait(0.3):
                                return
                            self.wfile.write(piece)
                            self.wfile.flush()
            server = HTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01})
            thread.start()
            clients = []
            def factory(**kwargs):
                kwargs['endpoint_url'] = f'http://127.0.0.1:{server.server_port}'
                client = boto3.client(**kwargs)
                clients.append(client)
                return client
            transport_store = build_evidence_archive_store(
                environment(), client_factory=factory, deadline_reads=True)
            self.store.deadline_client = transport_store.deadline_client
            client = clients[1]
            original_get = client.get_object
            def get(**kwargs):
                response = original_get(**kwargs)
                response['Body'].close = mock.Mock(wraps=response['Body'].close)
                acquired_bodies.append(response['Body'])
                return response
            try:
                started = time.monotonic()
                expected = archive.ArchiveDeadlineExceeded if stage == 'drip' else archive.ArchiveError
                with self.subTest(stage=stage), mock.patch.object(client, 'get_object', side_effect=get):
                    if stage == 'success':
                        self.assertEqual(self.read(), self.payload)
                    else:
                        with self.assertRaises(expected):
                            self.read(deadline=started + 0.8 if stage == 'drip' else started + 3)
                self.assertLess(time.monotonic() - started, 1.5)
                self.assertEqual(len(attempts), 1)
                for body in acquired_bodies:
                    body.close.assert_called_once_with()
                self.assertEqual(len(acquired_bodies), int(stage in ('body', 'drip', 'success')))
            finally:
                release.set()
                server.shutdown()
                thread.join(2)
                server.server_close()
                for client in clients:
                    client.close()
                self.assertFalse(thread.is_alive())


class ArchiveBulkDeadlineTest(ArchiveFixture, unittest.TestCase):
    def test_bulk_timeout_is_explicit_bounded_and_shared_with_sdk(self):
        calls = []
        factory = lambda **kwargs: calls.append(kwargs) or mock.Mock()
        store = build_evidence_archive_store(
            environment(), client_factory=factory, deadline_reads=True,
            deadline_read_timeout_seconds=5.0,
        )
        self.assertEqual(calls[1]['config'].read_timeout, 5.0)
        self.assertEqual(calls[1]['config'].connect_timeout, 0.25)
        self.assertEqual(calls[1]['config'].retries, {'total_max_attempts': 1})
        self.assertEqual(store.deadline_read_timeout_seconds, 5.0)
        self.assertEqual(archive.S3_DEADLINE_READ_TIMEOUT, 0.5)

    def test_invalid_timeout_does_not_construct_any_client(self):
        factory = mock.Mock()
        for value in (True, None, '5', 0, 0.49, 5.01, float('nan'), float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_evidence_archive_store(
                    environment(), client_factory=factory, deadline_reads=True,
                    deadline_read_timeout_seconds=value,
                )
        with self.assertRaises(ValueError):
            build_evidence_archive_store(
                environment(), client_factory=factory, deadline_read_timeout_seconds=5,
            )
        factory.assert_not_called()

    def test_bulk_budget_admission_and_body_absolute_deadline_are_preserved(self):
        self.store = archive.S3ArchiveStore(
            bucket='evidence-test', endpoint_url='https://s3.us-west-2.amazonaws.com',
            namespace_key=b'k' * 32, client=mock.Mock(), deadline_client=self.client,
            deadline_read_timeout_seconds=5,
        )
        with self.assertRaises(archive.ArchiveDeadlineExceeded):
            self.read(time.monotonic() + 5)
        self.client.get_object.assert_not_called()
        now = [10.0]
        def get(**kwargs):
            now[0] = 15.0
            return self.response
        self.client.get_object.side_effect = get
        original = self.body.read
        def read(size):
            now[0] = 16.1
            return original(size)
        self.body.read = read
        with mock.patch.object(archive.time, 'monotonic', side_effect=lambda: now[0]):
            with self.assertRaises(archive.ArchiveDeadlineExceeded):self.read(16.0)
        self.assertEqual(self.body.timeouts, [1.0])
        self.assertTrue(self.body.closed)
        self.client.get_object.assert_called_once()

    def test_real_sdk_delayed_header_and_body_default_fails_bulk_succeeds_once(self):
        for stage in ('headers', 'body'):
            for budget in (0.5, 5.0):
                attempts = []
                release = threading.Event()
                owner = self
                class Handler(BaseHTTPRequestHandler):
                    def log_message(self, *args):pass
                    def do_GET(self):
                        attempts.append(1)
                        try:
                            if stage == 'headers' and release.wait(0.75):return
                            self.send_response(200)
                            self.send_header('Content-Length', str(len(owner.payload)))
                            for key, value in owner.response['Metadata'].items():
                                self.send_header('x-amz-meta-' + key, value)
                            self.end_headers();self.wfile.flush()
                            if stage == 'body' and release.wait(0.75):return
                            self.wfile.write(owner.payload);self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):pass
                server = HTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01})
                thread.start();clients=[];acquired=[]
                def factory(**kwargs):
                    kwargs['endpoint_url'] = f'http://127.0.0.1:{server.server_port}'
                    client = boto3.client(**kwargs);clients.append(client);return client
                try:
                    store = build_evidence_archive_store(
                        environment(), client_factory=factory, deadline_reads=True,
                        deadline_read_timeout_seconds=budget,
                    )
                    original = clients[1].get_object
                    def get(**kwargs):
                        response = original(**kwargs)
                        response['Body'].close = mock.Mock(wraps=response['Body'].close)
                        acquired.append(response['Body']);return response
                    self.store=store
                    with self.subTest(stage=stage, budget=budget), mock.patch.object(clients[1], 'get_object', side_effect=get):
                        if budget==0.5:
                            with self.assertRaisesRegex(archive.ArchiveError, '^archive provider request failed$'):
                                self.read(time.monotonic()+8)
                        else:self.assertEqual(self.read(time.monotonic()+8),self.payload)
                    self.assertEqual(attempts,[1])
                    for body in acquired:body.close.assert_called_once_with()
                finally:
                    release.set();server.shutdown();thread.join(2);server.server_close()
                    for client in clients:client.close()
                    self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
