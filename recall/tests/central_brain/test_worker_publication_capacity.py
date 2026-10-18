"""Projection CLI reserves scan/search capacity without more logical slots."""
import contextlib
import io
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from recall_server import cli
from recall_server.worker_connections import WorkerWriteStore
from recall_server.db import BrainStore


class WorkerPublicationCapacityTests(unittest.TestCase):
    def test_cli_resolves_search_once_and_preserves_background_capacity(self):
        for concurrency in (4, 12, 30, 31, 32):
            for search in (None, object()):
                with self.subTest(concurrency=concurrency, search=search is not None):
                    def store(_dsn, **kwargs):
                        # Real constructor validates the deployment ceiling; no
                        # pool is opened because native owners below are doubles.
                        return BrainStore(_dsn, **kwargs)
                    with contextlib.ExitStack() as stack:
                        stack.enter_context(patch.object(sys, 'argv', [
                            'recall-server', '--dsn', 'postgresql://synthetic.invalid/db',
                            'projection-worker', '--tenant', 'tenant:synthetic', '--once',
                            '--passage-concurrency', str(concurrency), '--search-plane', 'auto']))
                        settings = stack.enter_context(patch.object(cli, '_search_plane_settings',
                                                                  return_value=search))
                        pool = stack.enter_context(patch.object(cli, 'BrainStore', side_effect=store))
                        logical = stack.enter_context(patch.object(cli, 'CanonicalLogicalEvidenceProjector'))
                        for name in ('CanonicalPassageProjector', 'CanonicalParquetScanProjector',
                                     'CanonicalBodyThinner', 'LogicalEvidenceProjectionStore',
                                     'build_evidence_archive_store', 'build_archive_store',
                                     'TurbopufferProjector', '_search_plane_client',
                                     'build_rerank_runtime'):
                            stack.enter_context(patch.object(cli, name))
                        stack.enter_context(patch.object(cli.SemanticRuntime, 'from_env', return_value=None))
                        run = stack.enter_context(patch.object(cli, 'run_projection_worker', return_value={}))
                        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                        cli.main()
                    settings.assert_called_once_with('auto')
                    expected_pool = min(32, concurrency + (2 if search is not None else 0))
                    self.assertEqual(pool.call_args.kwargs['pool_max_size'], expected_pool)
                    background = logical.call_args.args[0]
                    self.assertEqual(background._capacity,
                                     expected_pool - (4 if search is not None else 2))
                    self.assertEqual(run.call_args.kwargs['passage_concurrency'], concurrency)
                    self.assertEqual(run.call_args.kwargs['upload_concurrency'], 2)
                    self.assertEqual(run.call_args.kwargs['search_plane'] is not None, search is not None)

    def test_reservation_default_and_invalid_capacity(self):
        store = SimpleNamespace(pool_max_size=6, reserved_read_connections=0,
                                verified_singleton_receipts=False)
        self.assertEqual(WorkerWriteStore(store)._capacity, 4)
        self.assertEqual(WorkerWriteStore(store, publication_reserve=4)._capacity, 2)
        for invalid in (True, 0, 3, 6):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                WorkerWriteStore(store, publication_reserve=invalid)
        store.pool_max_size = 4
        with self.assertRaises(ValueError):
            WorkerWriteStore(store, publication_reserve=4)

    def test_warmup_accepts_general_pool_bound_but_admits_only_background_capacity(self):
        for pool_size, reserve, requested, admitted in ((4, 2, 4, 2), (6, 4, 6, 2),
                                                       (32, 4, 30, 28), (32, 4, 31, 28),
                                                       (32, 4, 32, 28)):
            with self.subTest(pool_size=pool_size, reserve=reserve, requested=requested):
                calls = []
                def warmup(count, *, timeout):
                    self.assertTrue(view._local.active)
                    self.assertEqual(view._available, 0)
                    self.assertGreaterEqual(timeout, 1)
                    self.assertLessEqual(timeout, 5)
                    calls.append(count)
                store = SimpleNamespace(pool_max_size=pool_size, reserved_read_connections=0,
                                        verified_singleton_receipts=False, prepare_pool=warmup)
                view = WorkerWriteStore(store, publication_reserve=reserve)
                view.prepare_pool(requested)
                self.assertEqual(calls, [admitted])
                self.assertEqual(view._available, admitted)
                self.assertFalse(view._local.active)
                with self.assertRaises(ValueError):
                    view.prepare_pool(pool_size + 1)
                self.assertEqual(calls, [admitted])
