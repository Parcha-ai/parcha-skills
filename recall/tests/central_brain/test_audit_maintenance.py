"""Audit work uses the existing no-retry archive client and caller deadline."""
import io
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.archive import ArchiveError, ArchiveNotFound, ArchiveRequest, S3ArchiveStore
from recall_server.audit_retirement import AuditRetirement
from recall_server.db import SearchDeadlineExceeded


class AuditArchiveBudgetTests(unittest.TestCase):
    def fixture(self, seconds=15):
        ordinary = Mock()
        bounded = Mock()
        bounded.head_object.side_effect = ArchiveNotFound('synthetic missing')
        bounded.put_object.return_value = {'VersionId': 'v1'}
        archive = S3ArchiveStore(client=ordinary, deadline_client=bounded,
            bucket='recall-fixture', endpoint_url='https://objects.example.test', namespace_key=b'k'*32)
        owner = AuditRetirement(None, archive, deadline_at=time.monotonic()+seconds)
        request = ArchiveRequest('tenant:test', 'source:test', 'audit:test', 'application/json', b'{}')
        return owner, request, ordinary, bounded

    def test_put_and_read_use_no_retry_client_without_mutating_archive(self):
        owner, request, ordinary, bounded = self.fixture()
        reference = owner.put(request)
        self.assertEqual(bounded.put_object.call_count, 1)
        stored = bounded.put_object.call_args.kwargs
        stream = io.BytesIO(request.payload)
        stream.set_socket_timeout = Mock()
        bounded.get_object.return_value = dict(Body=stream, ContentLength=2, Metadata=stored['Metadata'])
        self.assertEqual(owner.read(reference, tenant_id=request.tenant_id, source_id=request.source_id), b'{}')
        self.assertTrue(stream.closed)
        stream.set_socket_timeout.assert_called_once()
        self.assertEqual(ordinary.mock_calls, [])
        self.assertIs(owner.archive.client, ordinary)

    def test_expired_budget_makes_no_provider_request(self):
        owner, request, ordinary, bounded = self.fixture(-1)
        with self.assertRaises(SearchDeadlineExceeded):
            owner.put(request)
        self.assertEqual(ordinary.mock_calls, [])
        self.assertEqual(bounded.mock_calls, [])

    def test_uncertain_put_has_one_native_attempt(self):
        owner, request, ordinary, bounded = self.fixture()
        bounded.put_object.side_effect = TimeoutError('synthetic lost response')
        with self.assertRaises(ArchiveError):
            owner.put(request)
        self.assertEqual(bounded.put_object.call_count, 1)
        self.assertEqual(ordinary.mock_calls, [])


if __name__ == '__main__':
    unittest.main()
