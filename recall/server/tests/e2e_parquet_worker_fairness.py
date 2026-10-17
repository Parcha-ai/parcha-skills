"""Native publication progresses while one Parquet archive admission is paused."""
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from e2e_parquet_sparse_planning import SparsePlanning
from recall_server.db import BrainStore
from recall_server.parquet_scan import PARQUET_MEDIA_TYPE
from recall_server.projection_worker import run_projection_worker
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class ParquetFairness(SparsePlanning):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=4)
        cls.store.migrate()

    def test_native_search_advances_during_blocked_archive_without_invalidating_scan(self):
        self.ingest_parent('alpha', 'alpha/one', 'Synthetic dirty Parquet document')
        self.project()
        with self.store.connect() as c:
            target_receipt = c.execute(
                'SELECT receipts FROM canonical_passages WHERE tenant_id=%s '
                'ORDER BY first_occurred_at DESC,passage_id DESC LIMIT 1',
                (self.tenant,)).fetchone()['receipts'][0]
        blocked, release, published = (threading.Event() for _ in range(3))
        calls = []
        native_build, native_verify = self.scan._build, self.archive.verify_raw_bounded
        client = FakeTurbopuffer()
        writer = TurbopufferProjector(
            self.store, TurbopufferSettings(api_key='synthetic-local-only'), client=client,
            batch_rows=1, write_concurrency=1)

        def build(*args, **kwargs):
            calls.append('build')
            return native_build(*args, **kwargs)

        def verify(reference, *, deadline_at):
            if reference['media_type'] == PARQUET_MEDIA_TYPE and not release.is_set():
                blocked.set()
                if not release.wait(12):
                    raise AssertionError('fixture_archive_not_released')
            # The fixture pauses the transport outside its native request budget.
            # The real verifier still validates the exact object after release.
            import time
            return native_verify(reference, deadline_at=time.monotonic() + 1)

        def publish():
            result = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            if blocked.is_set() and any(
                target_receipt in row.get('receipts', [])
                for namespace in client.namespaces.values() for row in namespace.rows.values()
            ):
                published.set()
            return result

        def worker():
            return run_projection_worker(
                self.projector, self.passages, self.scan, tenant_id=self.tenant,
                logical_batch_size=4, passage_batch_size=4, embedding_batch_size=4,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=0.1, once=True, skip_embedding=True,
                parquet_every_cycles=1, search_plane=publish)

        with patch.object(self.scan, '_build', build), patch.object(
            self.archive, 'verify_raw_bounded', verify
        ), patch('recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS',
                 0.02, create=True), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(5), 'native archive admission did not block')
                self.assertFalse(any(
                    target_receipt in row.get('receipts', [])
                    for namespace in client.namespaces.values() for row in namespace.rows.values()
                ), 'fixture search work already published before archive pause')
                self.assertTrue(published.wait(3), 'publication waited behind archive admission')
                self.assertFalse(future.done())
                self.assertEqual(calls, ['build'])
            finally:
                release.set()
            result = future.result(timeout=10)
        self.assertEqual(result['parquet_stale'], 0)
        self.assertEqual(result['parquet_shards'], 1)
        self.assertEqual(calls, ['build'])
        with self.store.connect() as c:
            self.assertEqual(c.execute(
                'SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)



if __name__ == '__main__':
    suite = unittest.TestSuite(ParquetFairness(name) for name in ParquetFairness.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
