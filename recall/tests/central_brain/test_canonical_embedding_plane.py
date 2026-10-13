"""Retired PostgreSQL embeddings cannot turn a managed connector ACK into failure."""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / 'server')]

from recall_server.canonical_retrieval import CanonicalRetrieval  # noqa: E402
from recall_server.embedding_worker import run_canonical_embedding_worker  # noqa: E402
from recall_server.managed_worker import ManagedConnectorWorker  # noqa: E402


class RetiredStore:
    search_plane = 'turbopuffer'

    @property
    def semantic_runtime(self):
        raise AssertionError('retired embeddings must not inspect the query runtime')

    def connect(self):
        raise AssertionError('retired embeddings must not access PostgreSQL')


class CanonicalEmbeddingPlaneTests(unittest.TestCase):
    def test_turbopuffer_skips_runtime_and_database_for_direct_and_worker_calls(self):
        retrieval = CanonicalRetrieval(RetiredStore())
        expected = {'status': 'disabled', 'processed': 0, 'batches': 0}
        self.assertEqual(retrieval.embed_pending(), expected)
        self.assertEqual(run_canonical_embedding_worker(
            retrieval, tenant_id='tenant:test', batch_size=100,
            max_batches_per_cycle=3, interval_seconds=1, once=True,
        ), expected)

    def test_postgres_keeps_runtime_validation_and_embedding_lock(self):
        runtime = SimpleNamespace(dimensions=1536, fingerprint='synthetic')
        store = SimpleNamespace(search_plane='postgres', semantic_runtime=runtime)
        retrieval = CanonicalRetrieval(store)
        with self.assertRaisesRegex(ValueError, 'canonical embeddings require 512 dimensions'):
            retrieval.embed_pending()
        runtime.dimensions = 512
        connection = mock.Mock()
        connection.execute.return_value.fetchone.return_value = {'value': False}

        @contextmanager
        def connect():
            yield connection

        store.connect = mock.Mock(side_effect=connect)
        self.assertEqual(retrieval.embed_pending(), {'status': 'busy', 'processed': 0, 'batches': 0})
        store.connect.assert_called_once_with()
        self.assertIn('pg_try_advisory_lock(', connection.execute.call_args.args[0])
        self.assertIs(store.semantic_runtime, runtime)

    def test_managed_ack_commits_with_configured_runtime_on_turbopuffer(self):
        runtime = mock.Mock(dimensions=512)
        runtime.embed.side_effect = AssertionError('legacy provider must not run')
        store = SimpleNamespace(search_plane='turbopuffer', semantic_runtime=runtime,
            connect=mock.Mock(side_effect=AssertionError('legacy SQL must not run')))
        row = dict(id='installation:test', tenant_id='tenant:test', principal_id='principal:test',
                   source_id='source:test', privacy_mode='scrub')
        connector = mock.Mock()
        runner = mock.Mock()
        runner.checkpoints = None
        runner.run_once.return_value = dict(status='committed', acked=2, staged=2, has_more=True)
        with tempfile.TemporaryDirectory() as directory:
            worker = object.__new__(ManagedConnectorWorker)
            worker.store = store
            worker.archive = mock.Mock()
            worker.plane = mock.Mock()
            worker.retrieval = CanonicalRetrieval(store)
            worker.authority_root = Path(directory)
            worker.embedding_max_batches = 3
            worker.interval_seconds = 60
            worker.connector_factory = mock.Mock(return_value=(connector, Path(directory) / 'spool.db'))
            worker._claim = mock.Mock(return_value=row)
            worker._credentials = mock.Mock(return_value={})
            worker._finish = mock.Mock()
            worker._degrade_authority = mock.Mock()
            configured_url = 'https://synthetic.invalid/embeddings'
            with mock.patch.dict(os.environ, {'RECALL_EMBEDDING_URL': configured_url}), \
                 mock.patch('recall_server.managed_worker.ConnectorRunner', return_value=runner):
                result = worker.run_once()
                self.assertEqual(os.environ['RECALL_EMBEDDING_URL'], configured_url)
            self.assertEqual(result, dict(schema_version=1, status='committed', processed=1,
                committed=1, failed=0, acked=2, staged=2, embedded=0, has_more=True))
            worker._finish.assert_called_once_with('installation:test', success=True, retry_after_seconds=1)
            worker._degrade_authority.assert_not_called()
            runner.close.assert_called_once_with()
            connector.close.assert_called_once_with()
        runtime.embed.assert_not_called()
        store.connect.assert_not_called()
        self.assertIs(store.semantic_runtime, runtime)


if __name__ == '__main__':
    unittest.main()
