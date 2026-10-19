"""Native logical progress after the coordinator services its due scan."""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

from e2e_parquet_worker_fairness import ParquetOverlapCapacity
from recall_server import logical_evidence_projection as logical_module
from recall_server import projection_worker as worker_module
from recall_server.projection_worker import run_projection_worker
from recall_server.worker_connections import WorkerWriteStore


class LogicalCadenceReadmission(ParquetOverlapCapacity):
    def test_due_scan_reopens_idle_slot_without_waiting_for_held_owner(self):
        self.exercise_readmission()

    def test_requested_scan_does_not_reopen_admission_before_settlement(self):
        self.exercise_readmission(hold_scan=True)

    def test_completed_scan_does_not_expand_original_finite_admission_count(self):
        self.exercise_readmission(budget=2)

    def exercise_readmission(self, *, hold_scan=False, budget=4):
        self.ingest_parent('alpha', 'alpha/two', 'Synthetic committed scan work')
        self.project()
        self.ingest_parent('held', 'held/one', 'Synthetic pending logical input')
        self.projector.store = WorkerWriteStore(self.store, publication_reserve=4)
        self.assertEqual(self.projector.store._capacity, 2)
        blocked, release, scanned, healthy_committed = (
            threading.Event() for _ in range(4)
        )
        scan_requested, scan_release, extra_committed = (
            threading.Event() for _ in range(3)
        )
        offset = [0.0]
        native_monotonic = time.monotonic
        started = native_monotonic()
        native_iter = psycopg.ServerCursor.__iter__
        native_scan = self.scan.project_pending
        native_commit = self.projector._commit_upload
        scan_results = []
        scan_threads, coordinator_threads = [], []

        def monotonic():
            return native_monotonic() + offset[0]

        def iterate(cursor):
            # Only the first actual input stream is held. Later work uses its
            # unchanged native SQL, archive preparation, authority and commit.
            if cursor.name == 'logical_evidence_batch_stream' and not blocked.is_set():
                offset[0] = 301.0
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
            scan_threads.append(threading.get_ident())
            scan_requested.set()
            if hold_scan and not scan_release.wait(10):
                raise AssertionError('fixture_scan_not_released')
            result = native_scan(**kwargs)
            scan_results.append((kwargs, result))
            if result['shards']:
                scanned.set()
            return result

        def commit(candidate, upload):
            status = native_commit(candidate, upload)
            if candidate.native_parent_id == 'healthy' and status == 'committed':
                healthy_committed.set()
            if candidate.native_parent_id == 'extra' and status == 'committed':
                extra_committed.set()
            return status

        def worker():
            coordinator_threads.append(threading.get_ident())
            return run_projection_worker(
                self.projector, self.passages, self.scan, tenant_id=self.tenant,
                logical_batch_size=budget, passage_batch_size=4, embedding_batch_size=4,
                max_batches_per_cycle=1, upload_concurrency=2, passage_concurrency=1,
                interval_seconds=0.1, once=True, skip_embedding=True,
                parquet_every_cycles=50, parquet_max_wait_seconds=300,
                clock=lambda: monotonic() - started,
            )

        with patch.object(psycopg.ServerCursor, '__iter__', iterate), patch.object(
            self.scan, 'project_pending', scan
        ), patch.object(self.projector, '_commit_upload', commit), patch.object(
            logical_module, 'time', SimpleNamespace(monotonic=monotonic)
        ), patch.object(
            worker_module, 'time', SimpleNamespace(monotonic=monotonic)
        ), patch.object(logical_module, 'PROJECTION_PROGRESS_INTERVAL_SECONDS', 0.02), \
                ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(5), 'native logical input did not block')
                if hold_scan:
                    self.assertTrue(scan_requested.wait(3))
                    self.ingest_parent('healthy', 'healthy/one', 'Synthetic fresh evidence')
                    self.assertFalse(healthy_committed.wait(.2),
                                     'requested scan reopened admission before settlement')
                    scan_release.set()
                self.assertTrue(scanned.wait(3), 'due native scan did not complete')
                self.assertEqual(scan_results[0][0]['batch_size'], 1)
                self.assertEqual(scan_results[0][0]['max_batches'], 1)
                self.assertEqual(scan_results[0][0]['compaction_budget'], 0)
                self.assertEqual(scan_results[0][1]['stale'], 0)
                if not hold_scan:
                    self.ingest_parent('healthy', 'healthy/one', 'Synthetic fresh evidence')
                progressed_while_held = healthy_committed.wait(3)
                self.assertFalse(future.done(), 'held owner unexpectedly completed')
                if budget == 2 and progressed_while_held:
                    self.ingest_parent('extra', 'extra/one', 'Synthetic next-cycle evidence')
                    self.assertFalse(extra_committed.wait(.2),
                                     'renewal expanded the finite admission count')
            finally:
                scan_release.set()
                release.set()
            result = future.result(timeout=10)
        self.assertTrue(progressed_while_held,
                        'healthy evidence waited behind held owner after due scan completed')
        self.assertEqual(result['documents'], 2)
        self.assertEqual(result['logical_failed'], 0)
        self.assertEqual(set(scan_threads), set(coordinator_threads))
        if budget == 2:
            self.assertFalse(extra_committed.is_set())


if __name__ == '__main__':
    suite = unittest.TestSuite(LogicalCadenceReadmission(name)
                              for name in LogicalCadenceReadmission.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())

