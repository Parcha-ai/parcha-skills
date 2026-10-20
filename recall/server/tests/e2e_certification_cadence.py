"""Optional certificates progress before a long native logical owner settles."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

import psycopg

from e2e_certification_selection_batch import CertificationSelectionTests
from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment
from recall_server.projection_worker import run_projection_worker


class CertificationCadenceTests(CertificationSelectionTests):
    def test_exhausted_slice_resumes_during_held_native_logical_owner(self):
        parents = self.ready_parents()
        self.source += ':held-logical'
        self.ingest('turn/held', 'Synthetic unrelated logical work')
        publisher = self.publisher()
        blocked, release, completed, search_after_cert = (threading.Event() for _ in range(4))
        now = [0.0]
        proof_calls, search_calls = [], []
        native_query = self.store._execute_bounded_pipeline
        native_iter = psycopg.ServerCursor.__iter__
        native_resume = remainder.resume

        def query(connection, statement, arguments, deadline):
            if statement == enrollment._OWNERS:
                proof_calls.append(deadline)
                if len(proof_calls) == 1:
                    return native_query(connection, 'SELECT pg_sleep(10)', (), deadline)
            return native_query(connection, statement, arguments, deadline)

        def iterate(cursor):
            if cursor.name == 'logical_evidence_batch_stream':
                now[0] = 31.0
                blocked.set()
                if not release.wait(15):
                    raise AssertionError('fixture_logical_owner_not_released')
            iterator = native_iter(cursor)
            while True:
                try:
                    yield next(iterator)
                except StopIteration:
                    return

        def resume(*args, **kwargs):
            value = native_resume(*args, **kwargs)
            if blocked.is_set() and value['certified_passages']:
                completed.set()
            return value

        def search():
            search_calls.append((now[0], completed.is_set()))
            if completed.is_set():
                search_after_cert.set()
            return dict(status='complete', months=0, rows=0, deleted=0,
                        failed=0, rate_limited=0, pending=0)

        def worker():
            return run_projection_worker(self.projector, publisher, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=100, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=2,
                interval_seconds=30, once=True, skip_embedding=True,
                clock=lambda: now[0], search_plane=search)

        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=query), patch.object(
            psycopg.ServerCursor, '__iter__', iterate
        ), patch.object(remainder, 'resume', side_effect=resume), patch(
            'recall_server.logical_evidence_projection.PROJECTION_PROGRESS_INTERVAL_SECONDS', .02
        ), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(worker)
            try:
                self.assertTrue(blocked.wait(7), 'native logical owner did not start')
                self.assertTrue(completed.wait(4), 'due certification waited for logical cycle completion')
                self.assertTrue(search_after_cert.wait(4), 'search did not resume after certificate work')
                self.assertFalse(future.done())
                self.assertTrue(any(at == 31.0 for at, _ in search_calls), 'search heartbeat starved')
            finally:
                release.set()
                result = future.result(timeout=12)
        self.assertGreater(result['membership_certified_passages'], 0)
        self.assertEqual(result['membership_failed_documents'], 1)
        for parent in parents:
            with self.store.connect() as connection:
                ids = tuple(row['passage_id'] for row in connection.execute(
                    'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s '
                    'AND source_id=%s AND logical_document_id=%s ORDER BY passage_id',
                    remainder.key(parent)).fetchall())
            value = enrollment.compare_parent_membership(self.store,
                **dict(self.scope, source_id=parent['source_id'], passage_ids=ids),
                deadline_at=time.monotonic()+3)
            self.assertEqual(value['certified_passages'], len(ids))
            self.assertEqual(value['disagreements'], 0)

    def test_due_certification_and_search_advance_before_native_parquet_build_releases(self):
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.parquet_scan import CanonicalParquetScanProjector

        self.ready_parents()
        publisher = self.publisher()
        scan = CanonicalParquetScanProjector(self.store, LogicalEvidenceProjectionStore(self.archive))
        blocked, release, completed, searched = (threading.Event() for _ in range(4))
        now = [0.0]
        native_build, native_resume, native_query = scan._build, remainder.resume, self.store._execute_bounded_pipeline
        expired = []

        def query(connection, statement, arguments, deadline):
            if statement == enrollment._OWNERS and not expired:
                expired.append(True)
                return native_query(connection, 'SELECT pg_sleep(10)', (), deadline)
            return native_query(connection, statement, arguments, deadline)

        def build(*args, **kwargs):
            now[0] = 31.0
            blocked.set()
            if not release.wait(12):
                raise AssertionError('fixture_parquet_build_not_released')
            return native_build(*args, **kwargs)

        def resume(*args, **kwargs):
            value = native_resume(*args, **kwargs)
            if blocked.is_set() and value['certified_passages']:
                completed.set()
            return value

        def search():
            if completed.is_set():
                searched.set()
            return dict(status='complete', months=0, rows=0, deleted=0,
                        failed=0, rate_limited=0, pending=0)

        class IdleLogical:
            def project_pending(self, **kwargs):
                return dict(status='complete', documents=0, records=0, batches=0,
                            pending=0, pruned=0, cleanup_failures=0)

        def worker():
            return run_projection_worker(IdleLogical(), publisher, scan, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=100, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=2,
                interval_seconds=30, once=True, skip_embedding=True,
                clock=lambda: now[0], search_plane=search)

        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=query), patch.object(
            scan, '_build', side_effect=build
        ), patch.object(remainder, 'resume', side_effect=resume), patch(
            'recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS', .02
        ), ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(worker)
            try:
                self.assertTrue(blocked.wait(7))
                self.assertTrue(completed.wait(4), 'certification waited behind Parquet')
                self.assertTrue(searched.wait(4), 'search did not resume after certification')
                self.assertFalse(future.done())
            finally:
                release.set()
                result = future.result(timeout=12)
        self.assertGreater(result['membership_certified_passages'], 0)
        self.assertEqual(result['membership_failed_documents'], 1)
        self.assertGreater(result['parquet_shards'], 0)
        self.assertEqual(result['parquet_stale'], 0)


if __name__ == '__main__':
    suite = unittest.TestSuite(CertificationCadenceTests(name)
        for name in CertificationCadenceTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
