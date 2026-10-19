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

    def test_due_parquet_and_search_finish_while_native_logical_input_is_blocked(self):
        import psycopg
        from recall_server.worker_connections import WorkerWriteStore

        self.ingest_parent('alpha', 'alpha/one', 'Synthetic committed scan work')
        self.project()
        self.ingest_parent('held', 'held/one', 'Synthetic pending logical input')
        self.projector.store = WorkerWriteStore(self.store)
        blocked, release, scanned, published = (threading.Event() for _ in range(4))
        now = [0.0]
        scans = []
        native_iter = psycopg.ServerCursor.__iter__
        native_scan = self.scan.project_pending
        client = FakeTurbopuffer()
        writer = TurbopufferProjector(
            self.store, TurbopufferSettings(api_key='synthetic-local-only'), client=client,
            batch_rows=20, write_concurrency=1)

        def iterate(cursor):
            if cursor.name == 'logical_evidence_batch_stream':
                now[0] = 301.0
                blocked.set()
                if not release.wait(12):
                    raise AssertionError('fixture_logical_input_not_released')
            iterator = native_iter(cursor)
            while True:
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                yield row

        def scan(**kwargs):
            result = native_scan(**kwargs)
            scans.append((kwargs, result))
            if result['shards']:
                scanned.set()
            return result

        def publish():
            if not blocked.is_set():
                return dict(status='complete', rows=0, pending=1)
            result = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            if result['rows']:
                published.set()
            return result

        def worker():
            return run_projection_worker(
                self.projector, self.passages, self.scan, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=4, embedding_batch_size=4,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=0.1, once=True, skip_embedding=True,
                parquet_every_cycles=50, parquet_max_wait_seconds=300,
                clock=lambda: now[0], search_plane=publish)

        with patch.object(psycopg.ServerCursor, '__iter__', iterate), patch.object(
            self.scan, 'project_pending', scan
        ), patch('recall_server.logical_evidence_projection.PROJECTION_PROGRESS_INTERVAL_SECONDS',
                 0.02), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(5), 'native logical input did not block')
                self.assertTrue(scanned.wait(3), 'due Parquet waited behind logical input')
                self.assertTrue(published.wait(3), 'search waited behind logical input')
                self.assertFalse(future.done())
                self.assertEqual(len(scans), 1)
                self.assertEqual(scans[0][0]['batch_size'], 1)
                self.assertEqual(scans[0][0]['max_batches'], 1)
                self.assertEqual(scans[0][0]['compaction_budget'], 0)
                self.assertEqual(scans[0][1]['stale'], 0)
                with self.store.connect() as c:
                    self.assertEqual(c.execute(
                        'SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s',
                        (self.tenant,)).fetchone()['n'], 0)
            finally:
                release.set()
            result = future.result(timeout=10)
        self.assertEqual(len(scans), 1)
        self.assertEqual(result['parquet_shards'], scans[0][1]['shards'])
        self.assertEqual(result['documents'], 1)

    def test_native_search_advances_during_blocked_archive_without_invalidating_scan(self):
        self.ingest_parent('alpha', 'alpha/one', 'Synthetic dirty Parquet document')
        self.project()
        with self.store.connect() as c:
            target_receipt = c.execute(
                'SELECT receipts FROM canonical_passages WHERE tenant_id=%s '
                'ORDER BY first_occurred_at DESC,passage_id DESC LIMIT 1',
                (self.tenant,)).fetchone()['receipts'][0]
        blocked, release, published = (threading.Event() for _ in range(3))
        allow_publication = threading.Event()
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
            # The worker normally drains existing outbox work before Parquet.
            # Start this fixture's writer only after the paused-state assertion.
            if not allow_publication.is_set():
                return dict(status='complete', rows=0, pending=1)
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
                allow_publication.set()
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



class ParquetOverlapCapacity(ParquetFairness):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=6)
        cls.store.migrate()

    def test_search_progress_with_two_logical_checkouts_and_active_parquet_cursor(self):
        import psycopg
        from recall_server.worker_connections import WorkerWriteStore

        self.ingest_parent('alpha', 'alpha/one', 'Synthetic committed scan work')
        self.project()
        self.ingest_parent('held', 'held/one', 'Synthetic pending logical input')
        self.ingest_parent('held-two', 'held-two/one', 'Synthetic second pending input')
        self.projector.store = WorkerWriteStore(self.store, publication_reserve=4)
        self.assertEqual(self.projector.store._capacity, 2)
        blocked, release, scanned, published = (threading.Event() for _ in range(4))
        parquet_blocked, parquet_release = threading.Event(), threading.Event()
        logical_count = [0]
        count_lock = threading.Lock()
        now = [0.0]
        scans = []
        native_iter = psycopg.ServerCursor.__iter__
        native_fetch = psycopg.ServerCursor.fetchmany
        native_scan = self.scan.project_pending
        client = FakeTurbopuffer()
        writer = TurbopufferProjector(
            self.store, TurbopufferSettings(api_key='synthetic-local-only'), client=client,
            batch_rows=20, write_concurrency=1)

        def iterate(cursor):
            if cursor.name == 'logical_evidence_batch_stream':
                with count_lock:
                    logical_count[0] += 1
                    if logical_count[0] == 2:
                        now[0] = 301.0
                        blocked.set()
                if not release.wait(12):
                    raise AssertionError('fixture_logical_input_not_released')
            if cursor.name == 'parquet_passage_stream':
                parquet_blocked.set()
                if not parquet_release.wait(12):
                    raise AssertionError('fixture_parquet_input_not_released')
            iterator = native_iter(cursor)
            while True:
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                yield row

        def fetch(cursor, size=0):
            if cursor.name == 'parquet_passage_stream':
                parquet_blocked.set()
                if not parquet_release.wait(12):
                    raise AssertionError('fixture_parquet_input_not_released')
            return native_fetch(cursor, size)

        # The fixture writer is a separate external producer, not extra worker
        # capacity. It queues committed search work only after four of the six worker
        # connections are held by two logical owners and the native scan.
        writer.drain(tenant_id=self.tenant, max_months=1)

        def scan(**kwargs):
            result = native_scan(**kwargs)
            scans.append((kwargs, result))
            if result['shards']:
                scanned.set()
            return result

        def publish():
            if not blocked.is_set():
                return dict(status='complete', rows=0, pending=1)
            result = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            if parquet_blocked.is_set() and result['rows']:
                published.set()
            return result

        def worker():
            return run_projection_worker(
                self.projector, self.passages, self.scan, tenant_id=self.tenant,
                logical_batch_size=2, passage_batch_size=4, embedding_batch_size=4,
                max_batches_per_cycle=1, upload_concurrency=2, passage_concurrency=1,
                interval_seconds=0.1, once=True, skip_embedding=True,
                parquet_every_cycles=50, parquet_max_wait_seconds=300,
                clock=lambda: now[0], search_plane=publish)

        with patch.object(psycopg.ServerCursor, '__iter__', iterate), patch.object(
            self.scan, 'project_pending', scan
        ), patch.object(psycopg.ServerCursor, 'fetchmany', fetch), patch(
            'recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS', 0.02
        ), patch('recall_server.logical_evidence_projection.PROJECTION_PROGRESS_INTERVAL_SECONDS',
                 0.02), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(5), 'native logical input did not block')
                self.assertTrue(parquet_blocked.wait(3), 'native scan did not hold its cursor')
                self.assertEqual(logical_count[0], 2)
                from datetime import date
                from recall_server.search_outbox import enqueue_search_outbox
                with psycopg.connect(os.environ['RECALL_DATABASE_URL']) as producer:
                    enqueue_search_outbox(producer, tenant_id=self.tenant,
                                          source_id=self.source,
                                          months=[date(2026, 10, 1)], reason='backfill')
                self.assertTrue(published.wait(2),
                                'search cannot publish while two logical owners and scan hold their reserved connections')
                parquet_release.set()
                self.assertTrue(scanned.wait(3), 'native scan did not commit before logical release')
                self.assertEqual(scans[0][1]['stale'], 0)
                self.assertFalse(future.done())
            finally:
                parquet_release.set()
                release.set()
            result = future.result(timeout=10)
        self.assertEqual(len(scans), 1)
        self.assertEqual(result['parquet_shards'], scans[0][1]['shards'])
        self.assertEqual(result['documents'], 2)


if __name__ == '__main__':
    suite = unittest.TestSuite(cls(name) for cls in (ParquetFairness, ParquetOverlapCapacity)
                              for name in cls.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
