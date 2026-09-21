"""One opt-in operation budget reaches SQL and the bounded archive transport."""
import hashlib
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))

from recall_server.archive import ArchiveDeadlineExceeded, ArchiveError, S3ArchiveStore
from recall_server.chunk_bodies import _VerifiedArchive
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import SearchDeadlineExceeded
from tests.central_brain.test_archived_chunk_reads import ReaderStore, Rows, TENANT, SOURCE


class VerifiedArchiveDeadlineTests(unittest.TestCase):
    def test_bounded_transport_receives_same_deadline(self):
        archive = mock.Mock()
        archive.read_raw_bounded.return_value = b'body'
        with mock.patch('recall_server.chunk_bodies.time.monotonic', return_value=100):
            self.assertEqual(_VerifiedArchive(archive, 101).read_raw({'size_bytes': 4}), b'body')
        archive.read_raw_bounded.assert_called_once_with({'size_bytes': 4}, deadline_at=101)
        archive.read_raw.assert_not_called()

    def test_transport_deadline_is_preserved_as_search_deadline(self):
        archive = mock.Mock()
        archive.read_raw_bounded.side_effect = ArchiveDeadlineExceeded('archive read deadline exceeded')
        with mock.patch('recall_server.chunk_bodies.time.monotonic', return_value=100):
            with self.assertRaises(SearchDeadlineExceeded):
                _VerifiedArchive(archive, 101).read_raw({'size_bytes': 4})
        archive.read_raw.assert_not_called()

    def test_non_network_archive_can_keep_ordinary_read(self):
        class LocalArchive:
            def read_raw(self, reference):
                return b'body'
        with mock.patch('recall_server.chunk_bodies.time.monotonic', return_value=100):
            self.assertEqual(_VerifiedArchive(LocalArchive(), 101).read_raw({'size_bytes': 4}), b'body')

    def test_real_s3_without_dedicated_client_never_uses_default_transport(self):
        archive = S3ArchiveStore(bucket='evidence-test', endpoint_url='https://s3.us-west-2.amazonaws.com',
                                 namespace_key=b'k' * 32, client=mock.Mock())
        reference = {
            'contract': 'recall.artifact-ref.v1', 'schema_version': 1,
            'tenant_id': 'tenant:test', 'source_id': 'source:test',
            'artifact_id': 'art_' + 'a' * 32, 'object_key': 'objects/aa/' + 'a' * 64,
            'storage_backend': 's3', 'content_sha256': hashlib.sha256(b'body').hexdigest(),
            'size_bytes': 4, 'media_type': 'application/json', 'encryption': 'sse-s3',
            'version_id': 'version', 'created_at': '2026-09-21T00:00:00Z',
        }
        with mock.patch('recall_server.chunk_bodies.time.monotonic', return_value=100):
            with self.assertRaisesRegex(ArchiveError, 'not configured'):
                _VerifiedArchive(archive, 101).read_raw(reference)
        archive.client.get_object.assert_not_called()


class ReaderOperationDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.store = ReaderStore()
        self.retrieval = BoundCanonicalRetrieval(
            self.store, tenant_id=TENANT, principal_id='principal:test',
            authorized_sources=(SOURCE,), chunk_body_archive=object())

    def call(self, method, **kwargs):
        if method == 'related':
            return self.retrieval.related()
        return getattr(self.retrieval, method)(self.store.target, **kwargs)

    def test_show_context_related_share_one_sql_and_hydration_budget(self):
        for method in ('show', 'session_context', 'related'):
            with self.subTest(method=method), ExitStack() as stack:
                stack.enter_context(mock.patch('recall_server.canonical_retrieval.time.monotonic', return_value=100))
                bounded = stack.enter_context(mock.patch.object(self.store, '_execute_bounded', wraps=self.store._execute_bounded))
                hydrate = stack.enter_context(mock.patch.object(self.retrieval, '_hydrate_chunk_rows'))
                if method == 'related':
                    stack.enter_context(mock.patch.object(self.store, 'execute', return_value=Rows([])))
                self.call(method)
                self.assertTrue(bounded.call_args_list)
                self.assertTrue(all(call.args[-1] == 101 for call in bounded.call_args_list))
                self.assertEqual(hydrate.call_args.kwargs['deadline_at'], 101)

    def test_context_keeps_investigator_budget(self):
        with mock.patch.object(self.store, '_execute_bounded', wraps=self.store._execute_bounded) as bounded:
            with mock.patch.object(self.retrieval, '_hydrate_chunk_rows') as hydrate:
                self.call('session_context', _deadline_at=777)
        self.assertTrue(all(call.args[-1] == 777 for call in bounded.call_args_list))
        self.assertEqual(hydrate.call_args.kwargs['deadline_at'], 777)

    def test_postgres_default_does_not_gain_a_new_deadline(self):
        self.retrieval.chunk_body_archive = None
        for method in ('show', 'session_context', 'related'):
            with self.subTest(method=method), ExitStack() as stack:
                bounded = stack.enter_context(mock.patch.object(self.store, '_execute_bounded', wraps=self.store._execute_bounded))
                if method == 'related':
                    stack.enter_context(mock.patch.object(self.store, 'execute', return_value=Rows([])))
                self.call(method)
                self.assertTrue(all(call.args[-1] is None for call in bounded.call_args_list))


class AppDeadlineConfigurationTests(unittest.TestCase):
    def test_dedicated_client_enabled_only_for_archive_chunk_reads(self):
        from recall_server import app
        for mode in ('postgres', 'archive'):
            with self.subTest(mode=mode), ExitStack() as stack:
                stack.enter_context(mock.patch.dict(app.os.environ, {
                    'RECALL_CANONICAL_V2_ENABLED': '1', 'RECALL_EVIDENCE_ENABLED': '1',
                    'RECALL_EVIDENCE_TENANT_ID': TENANT, 'RECALL_CHUNK_BODY_READS': mode,
                }, clear=True))
                for name in ('Handler', 'BrainStore', 'SemanticRuntime', 'build_rerank_runtime', 'OidcJwtVerifier',
                             'build_archive_store', 'EvidenceProjectionStore', 'CanonicalEvidenceProjector',
                             'build_deep_inspector', 'CanonicalPlane', 'LegacyIngestBridge'):
                    stack.enter_context(mock.patch.object(app, name))
                build = stack.enter_context(mock.patch.object(app, 'build_evidence_archive_store'))
                app.configure_runtime('synthetic')
                build.assert_called_once_with(deadline_reads=mode == 'archive')


if __name__ == '__main__':
    unittest.main()
