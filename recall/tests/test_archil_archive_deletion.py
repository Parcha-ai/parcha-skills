from __future__ import annotations

import base64
import copy
import json
import unittest
from unittest import mock

from server.recall_server.archive import ArchiveCorruption, ArchiveError, ArchiveRequest
from server.recall_server.archive_runtime import build_archive_store, build_evidence_archive_store
from server.recall_server.archil_archive import ArchilDisk
from tests.test_archive_runtime import FakeR2


class FakeArchil:
    def __init__(self):
        self.calls = []
        self.lengths = {}
        self.failure = None

    def size(self, key):
        self.calls.append(('HEAD', key))
        return self.lengths.get(key)

    def delete(self, key):
        self.calls.append(('DELETE', key))
        if self.failure:
            raise ArchiveError('archive provider request failed')
        self.lengths.pop(key, None)


class ArchilArchiveDeletionTests(unittest.TestCase):
    def setUp(self):
        self.origin = FakeR2()
        self.disk = FakeArchil()
        self.env = {
            'RECALL_EVIDENCE_ARCHIVE_BACKEND': 's3-unversioned',
            'RECALL_EVIDENCE_ARCHIVE_BUCKET': 'recall-evidence-test',
            'RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL': 'https://s3.us-west-2.amazonaws.com',
            'RECALL_EVIDENCE_ARCHIVE_REGION': 'us-west-2',
            'RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID': 'synthetic',
            'RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY': 'synthetic',
            'RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY': base64.b64encode(b'k' * 32).decode(),
            'ARCHIL_API_KEY': 'key-synthetic',
            'RECALL_ARCHIL_DISK_ID': 'dsk-0123456789abcdef',
            'RECALL_ARCHIL_REGION': 'aws-us-west-2',
            'RECALL_EVIDENCE_DELETE_MODE': 'archil',
        }
        self.patch = mock.patch('server.recall_server.archil_archive.ArchilDisk', return_value=self.disk)
        self.constructor = self.patch.start()
        self.addCleanup(self.patch.stop)
        self.store = build_evidence_archive_store(self.env, client_factory=lambda **_: self.origin)
        self.reference = self.store.put(ArchiveRequest(
            tenant_id='tenant:synthetic', source_id='source:synthetic', native_id='native:one',
            media_type='text/plain', payload=b'evidence',
        ))
        self.contract = self.reference.to_contract(
            tenant_id='tenant:synthetic', source_id='source:synthetic', created_at='2026-10-21T00:00:00Z')
        self.disk.lengths[self.reference.object_key] = self.reference.size_bytes

    def test_all_delete_owners_use_disk_and_cleanup_requires_backing_confirmation(self):
        import time
        for owner in ('public', 'internal', 'bounded'):
            with self.subTest(owner=owner):
                self.disk.lengths[self.reference.object_key] = self.reference.size_bytes
                if owner == 'public':
                    removed = self.store.delete_raw(self.contract)
                elif owner == 'internal':
                    removed = self.store.delete_internal_raw(self.contract)
                else:
                    removed = self.store.delete_internal_raw_bounded(self.contract, deadline_at=time.monotonic() + 3)
                self.assertTrue(removed)
                self.assertIn((self.store.bucket, self.reference.object_key), self.origin.objects)
                self.assertEqual(self.disk.calls[-1], ('DELETE', self.reference.object_key))
                with self.assertRaises(ArchiveError):
                    self.store.confirm_raw_deletion(self.contract)
        self.origin.objects.clear()
        self.store.confirm_raw_deletion(self.contract)

    def test_new_upload_verification_does_not_wait_for_disk_discovery(self):
        self.disk.lengths.clear()
        self.store.verify_raw(self.contract)
        self.assertEqual(self.disk.calls, [])

    def test_object_not_yet_imported_is_not_acknowledged_as_deleted(self):
        self.disk.lengths.clear()
        with self.assertRaises(ArchiveError):
            self.store.delete_internal_raw(self.contract)
        self.assertFalse(any(operation == 'DELETE' for operation, _ in self.disk.calls))
        self.assertIn((self.store.bucket, self.reference.object_key), self.origin.objects)

    def test_failed_disk_delete_does_not_fall_back_to_origin(self):
        self.disk.failure = '409'
        with self.assertRaises(ArchiveError):
            self.store.delete_internal_raw(self.contract)
        self.assertIn((self.store.bucket, self.reference.object_key), self.origin.objects)
        self.assertIn(self.reference.object_key, self.disk.lengths)

    def test_wrong_scope_and_corrupt_origin_are_still_rejected(self):
        value = copy.deepcopy(self.contract)
        value['source_id'] = 'source:other'
        self.assertFalse(self.store.delete_internal_raw(value))
        self.assertFalse(any(operation == 'DELETE' for operation, _ in self.disk.calls))
        original = self.origin.objects[(self.store.bucket, self.reference.object_key)]
        original['Metadata'] = dict(original['Metadata'], **{'content-sha256': '0' * 64})
        with self.assertRaises(ArchiveCorruption):
            self.store.delete_internal_raw(self.contract)
        self.assertFalse(any(operation == 'DELETE' for operation, _ in self.disk.calls))

    def test_disk_only_object_is_not_acknowledged_as_already_missing(self):
        self.origin.objects.clear()
        with self.assertRaises(ArchiveError):
            self.store.delete_internal_raw(self.contract)
        self.assertIn(self.reference.object_key, self.disk.lengths)
        self.disk.lengths.clear()
        self.assertFalse(self.store.delete_internal_raw(self.contract))

    def test_partial_configuration_and_versioned_evidence_are_rejected(self):
        for name in ('ARCHIL_API_KEY', 'RECALL_ARCHIL_DISK_ID', 'RECALL_ARCHIL_REGION'):
            with self.subTest(name=name):
                env = {key: value for key, value in self.env.items() if key != name}
                with self.assertRaises(ValueError):
                    build_evidence_archive_store(env, client_factory=lambda **_: self.origin)
        with self.assertRaises(ValueError):
            build_evidence_archive_store(dict(self.env, RECALL_EVIDENCE_ARCHIVE_BACKEND='s3'),
                                         client_factory=lambda **_: self.origin)

    def test_rollout_hold_blocks_deletion_while_capture_still_works(self):
        env = {key: value for key, value in self.env.items() if key not in
               {'ARCHIL_API_KEY', 'RECALL_ARCHIL_DISK_ID', 'RECALL_ARCHIL_REGION'}}
        env['RECALL_EVIDENCE_DELETE_MODE'] = 'hold'
        held = build_evidence_archive_store(env, client_factory=lambda **_: self.origin)
        held.verify_raw(self.contract)
        with self.assertRaises(ArchiveError):
            held.delete_internal_raw(self.contract)
        self.assertIn((self.store.bucket, self.reference.object_key), self.origin.objects)

    def test_raw_archive_is_not_rerouted(self):
        raw = {key.replace('RECALL_EVIDENCE_', 'RECALL_'): value for key, value in self.env.items()}
        store = build_archive_store(raw, client_factory=lambda **_: self.origin)
        self.assertIs(store.client.client, self.origin)


class ArchilHTTPTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.connections = []
        self.responses = []
        self.key = 'objects/ab/' + 'c' * 64
        owner = self

        class Connection:
            def __init__(self, host, timeout):
                self.host = host
                self.timeout = timeout
                self.sock = mock.Mock()
                self.closed = False
                owner.connections.append(self)

            def connect(self):
                pass

            def request(self, method, path, headers):
                owner.requests.append((self.host, method, path, headers))

            def getresponse(self):
                response = owner.responses.pop(0)
                if isinstance(response, Exception):
                    raise response
                return response

            def close(self):
                self.closed = True

        self.transport = mock.patch('server.recall_server.archil_archive.http.client.HTTPSConnection', Connection)
        self.transport.start()
        self.addCleanup(self.transport.stop)
        self.mapping = {
            'success': True, 'data': {'id': 'dsk-0123456789abcdef', 'region': 'aws-us-west-2',
                'status': 'available', 'mounts': [{'type': 's3', 'path': '/', 'accessMode': 'rw',
                    'connectionStatus': 'connected', 'config': {'bucketName': 'recall-evidence-test',
                        'bucketPrefix': None, 'bucketEndpoint': 'aws'}}]}}

    def reply(self, status=200, *, body=b'', length='8'):
        result = mock.Mock(status=status)
        result.getheader.return_value = length
        result.read.side_effect = lambda limit: body[:limit]
        return result

    def disk(self, mapping=None):
        self.responses.append(self.reply(body=json.dumps(mapping or self.mapping).encode()))
        return ArchilDisk(api_key='key-private-canary', disk_id='dsk-0123456789abcdef',
                          region='aws-us-west-2', backing_bucket='recall-evidence-test', read_timeout=0.5)

    def test_verified_mapping_and_api_key_head_delete_transport(self):
        disk = self.disk()
        self.responses.extend([self.reply(), self.reply(204)])
        self.assertEqual(disk.size(self.key), 8)
        disk.delete(self.key)
        self.assertEqual([(host, method, path) for host, method, path, _ in self.requests], [
            ('control.green.us-west-2.aws.prod.archil.com', 'GET', '/api/disks/dsk-0123456789abcdef'),
            ('s3.green.us-west-2.aws.prod.archil.com', 'HEAD', '/dsk-0123456789abcdef/' + self.key),
            ('s3.green.us-west-2.aws.prod.archil.com', 'DELETE', '/dsk-0123456789abcdef/' + self.key)])
        self.assertTrue(all(headers == {'Authorization': 'key-private-canary'} for _, _, _, headers in self.requests))
        for connection in self.connections:
            self.assertTrue(connection.closed)
            self.assertEqual(connection.timeout, 0.25)
            connection.sock.settimeout.assert_called_once_with(0.5)

    def test_provider_failure_redirect_timeout_and_malformed_size_are_single_attempt(self):
        disk = self.disk()
        for response in [self.reply(409), self.reply(403), self.reply(500), self.reply(302),
                         TimeoutError('private-canary'), self.reply(length='invalid')]:
            before = len(self.requests)
            self.responses.append(response)
            with self.assertRaises(ArchiveError) as raised:
                disk.size(self.key)
            self.assertNotIn('private-canary', str(raised.exception))
            self.assertEqual(len(self.requests), before + 1)
            self.assertTrue(self.connections[-1].closed)

    def test_404_and_empty_objects_have_distinct_results(self):
        disk = self.disk()
        self.responses.extend([self.reply(404), self.reply(length='0'), self.reply(404)])
        self.assertIsNone(disk.size(self.key))
        self.assertEqual(disk.size(self.key), 0)
        disk.delete(self.key)

    def test_uncommitted_delete_is_not_a_success(self):
        disk = self.disk()
        self.responses.append(self.reply(202))
        with self.assertRaises(ArchiveError):
            disk.delete(self.key)

    def test_wrong_bucket_prefix_mount_and_region_fail_before_object_requests(self):
        mutations = [
            lambda d: d['data'].update(region='aws-us-east-1'),
            lambda d: d['data']['mounts'][0]['config'].update(bucketName='another-bucket'),
            lambda d: d['data']['mounts'][0]['config'].update(bucketPrefix='somewhere/'),
            lambda d: d['data']['mounts'][0].update(path='/elsewhere'),
            lambda d: d['data']['mounts'][0].update(accessMode='ro'),
        ]
        for mutate in mutations:
            mapping = copy.deepcopy(self.mapping)
            mutate(mapping)
            before = len(self.requests)
            with self.assertRaises(ValueError):
                self.disk(mapping)
            self.assertEqual(len(self.requests), before + 1)
            self.assertEqual(self.requests[-1][1], 'GET')


if __name__ == '__main__':
    unittest.main()
