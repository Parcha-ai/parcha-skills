#!/usr/bin/env python3
"""Real HTTP + PostgreSQL proof for canonical archive binary transport."""
from __future__ import annotations

import gzip
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest import mock
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'server')]

from client.mac import CanonicalArchiveClient, CanonicalClientError
from recall_server.app import Handler
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import _identity_sha256
from recall_server.db import BrainStore


class BinaryArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()
        cls.tmp = tempfile.TemporaryDirectory()
        cls.archive = FilesystemArchiveStore(Path(cls.tmp.name)/'archive', namespace_key=b'b'*32)
        class TestHandler(Handler):
            store = cls.store
            archive_store = cls.archive
            fail_after_write = False
            first_reference = None
            captured = 0

            def send_json(self, status, payload, *args, **kwargs):
                if status == 201 and type(self).fail_after_write:
                    type(self).fail_after_write = False
                    type(self).first_reference = payload
                    return super().send_json(503, {"error": "synthetic retry"})
                return super().send_json(status, payload, *args, **kwargs)

            def do_POST(self):
                if self.path == '/capture':
                    type(self).captured += 1
                    self.send_json(400, {"error": "unexpected redirect"})
                else:
                    super().do_POST()
        cls.handler = TestHandler
        cls.env = mock.patch.dict(os.environ, {'RECALL_AUTH_REQUIRED': '1'})
        cls.env.start()
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), TestHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.endpoint = 'http://127.0.0.1:' + str(cls.server.server_port)
        # Larger than the observed 10,079,320-byte valid gzip. Synthetic only.
        cls.payload = gzip.compress(os.urandom(10_080_000), mtime=0)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.env.stop()
        cls.store.close()
        cls.tmp.cleanup()

    def setUp(self):
        self.tenant = 'tenant:binary:' + uuid.uuid4().hex
        self.source = 'codex:synthetic'
        self.principal = 'principal:synthetic'
        self.token = self.store.create_collector_token(self.tenant, self.source, ['write'],
            tenant_id=self.tenant, principal_id=self.principal)['token']
        self.client = CanonicalArchiveClient(endpoint=self.endpoint, token=self.token,
            tenant_id=self.tenant, source_id=self.source, principal_id=self.principal)
        self.metadata = dict(tenant_id=self.tenant, principal_id=self.principal,
            source_id=self.source, native_id='native:large', media_type='application/gzip',
            created_at='2026-09-24T00:00:00Z', content_sha256=hashlib.sha256(self.payload).hexdigest())

    def upload(self):
        return self.client.put_raw(**{key: value for key, value in self.metadata.items()
            if key not in {'principal_id', 'content_sha256'}}, payload=self.payload)

    def binary(self, *, metadata=None, payload=None, token=None):
        body = self.payload if payload is None else payload
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=15)
        try:
            connection.request('POST', '/v2/archive/objects', body=body, headers={
                'Authorization': 'Bearer ' + (token or self.token),
                'Content-Type': 'application/octet-stream',
                'X-Recall-Archive-Metadata': json.dumps(self.metadata if metadata is None else metadata),
            })
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_large_gzip_roundtrips_and_retry_returns_same_reference(self):
        first = self.upload()
        self.assertGreater(first['size_bytes'], 10_079_320)
        self.assertEqual(first['content_sha256'], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(self.archive.read_raw(first), self.payload)
        self.assertEqual(self.upload(), first)

    def test_binary_rejects_wrong_digest_and_source_without_archive_write(self):
        with mock.patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as put:
            status, _ = self.binary(metadata={**self.metadata, 'content_sha256': '0'*64})
            self.assertEqual(status, 400)
            status, _ = self.binary(metadata={**self.metadata, 'source_id': 'codex:other'}, payload=b'private')
            self.assertEqual(status, 403)
            put.assert_not_called()

    def test_forgotten_identity_remains_fenced(self):
        self.upload()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO forget_tombstones(
                tenant_id,source_id,target_identity_sha256,mode,reason,deleted_at,status,completed_at)
                VALUES(%s,%s,%s,'explicit_forget','owner_requested',now(),'deleted',now())''',
                (self.tenant, self.source, _identity_sha256(self.tenant, self.source, self.metadata['native_id'])))
        with self.assertRaisesRegex(CanonicalClientError, 'archive_identity_forgotten'):
            self.upload()

    def test_retry_after_committed_write_reuses_exact_reference(self):
        self.handler.fail_after_write = True
        with mock.patch('client.mac.time.sleep') as slept:
            reference = self.upload()
        self.assertEqual(reference, self.handler.first_reference)
        slept.assert_called_once()
        self.assertEqual(self.archive.read_raw(reference), self.payload)

    def test_binary_redirect_does_not_forward_bytes_or_credentials(self):
        destination = self.endpoint + '/capture'
        class Redirect(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(307)
                self.send_header('Location', destination)
                self.end_headers()

            def log_message(self, *_args):
                pass
        redirect = ThreadingHTTPServer(('127.0.0.1', 0), Redirect)
        thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        thread.start()
        try:
            self.client.endpoint = 'http://127.0.0.1:' + str(redirect.server_port)
            with mock.patch('client.mac.time.sleep') as slept:
                with self.assertRaisesRegex(CanonicalClientError, 'archive_unavailable'):
                    self.upload()
            slept.assert_not_called()
            self.assertEqual(self.handler.captured, 0)
        finally:
            redirect.shutdown()
            redirect.server_close()
            thread.join()

    def test_binary_requires_authenticated_exact_metadata(self):
        with mock.patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as put:
            for metadata in ([], {}, {**self.metadata, 'unexpected': True}):
                with self.subTest(metadata_type=type(metadata).__name__):
                    status, _ = self.binary(metadata=metadata, payload=b'private')
                    self.assertEqual(status, 400)
            status, _ = self.binary(payload=b'private', token='invalid')
            self.assertEqual(status, 401)
            put.assert_not_called()

    def test_binary_rejects_ambiguous_framing_and_truncated_payload(self):
        for extra in [('Content-Length', '1'), ('Transfer-Encoding', 'chunked'),
                      ('X-Recall-Archive-Metadata', '{}')]:
            with self.subTest(header=extra[0]):
                connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
                try:
                    connection.putrequest('POST', '/v2/archive/objects')
                    for key, value in [('Authorization', 'Bearer ' + self.token),
                            ('Content-Type', 'application/octet-stream'),
                            ('Content-Length', '1'),
                            ('X-Recall-Archive-Metadata', json.dumps(self.metadata)), extra]:
                        connection.putheader(key, value)
                    connection.endheaders(b'x')
                    response = connection.getresponse()
                    response.read()
                    self.assertEqual(response.status, 400)
                finally:
                    connection.close()
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        try:
            connection.request('POST', '/v2/archive/objects', body=b'x', headers={
                'Authorization': 'Bearer ' + self.token,
                'Content-Type': 'application/octet-stream', 'Content-Length': '2',
                'X-Recall-Archive-Metadata': json.dumps(self.metadata),
            })
            import socket
            connection.sock.shutdown(socket.SHUT_WR)
            response = connection.getresponse()
            response.read()
            self.assertEqual(response.status, 400)
        finally:
            connection.close()

    def test_binary_body_bound_comes_from_configured_store(self):
        with mock.patch.object(self.archive, 'maximum_bytes', 16):
            status, _ = self.binary(payload=b'x'*17)
        self.assertEqual(status, 413)


if __name__ == '__main__':
    unittest.main()
