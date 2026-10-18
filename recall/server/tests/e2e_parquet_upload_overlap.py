"""One immutable upload overlaps the next bounded fragment, before native commit."""
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from unittest.mock import patch

from e2e_parquet_sparse_planning import SparsePlanning
from recall_server import parquet_scan


class UploadOverlap(SparsePlanning):
    def test_blocked_put_allows_next_passage_production_and_native_commit(self):
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        blocked, release, producer_advanced = (threading.Event() for _ in range(3))
        original_put = self.archive.put_raw
        original_row = self.scan._passage_row
        original_encode = parquet_scan._parquet_parts
        calls = []
        encoded_while_blocked = []
        active = 0
        peak = 0
        produced = 0
        lock = threading.Lock()

        def put(**kwargs):
            nonlocal active, peak
            with lock:
                calls.append(kwargs['native_id'])
                active += 1
                peak = max(peak, active)
                first = len(calls) == 1
            try:
                if first:
                    blocked.set()
                    if not release.wait(5):
                        raise RuntimeError('synthetic upload release missing')
                return original_put(**kwargs)
            finally:
                with lock:
                    active -= 1

        def row(*args):
            nonlocal produced
            result = original_row(*args)
            produced += 1
            if produced == 3 and blocked.wait(2) and not release.is_set():
                producer_advanced.set()
            return result

        def encode(*args, **kwargs):
            if blocked.is_set() and not release.is_set():
                encoded_while_blocked.append(True)
            return original_encode(*args, **kwargs)

        with patch.object(self.archive, 'put_raw', put), \
             patch.object(self.scan, '_passage_row', row), \
             patch.object(parquet_scan, '_parquet_parts', encode), \
             ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.scan._build, candidate, _rebuild=True)
            try:
                self.assertTrue(blocked.wait(3), 'actual native build never uploaded')
                self.assertTrue(producer_advanced.wait(1),
                                'producer waited behind the previous immutable PUT')
                self.assertFalse(future.done())
                self.assertEqual(peak, 1)
            finally:
                release.set()
            result = future.result(timeout=10)
        self.assertEqual(active, 0)
        self.assertEqual(peak, 1)
        self.assertEqual(encoded_while_blocked, [], 'next encoding bypassed upload backpressure')
        self.assertEqual(len(calls), len(set(calls)))
        self.assertEqual(len(self.rows(result)['passages']), 3)
        self.assertEqual(self.scan._commit(candidate, result), 'committed')
        self.assertEqual(self.scan._pending(tenant_id=self.tenant, limit=1), [])

    def test_payload_members_bounds_and_native_commit_equal_serial_upload(self):
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        emit = parquet_scan._StreamingUpload._emit

        def serial(owner, *args, **kwargs):
            emit(owner, *args, **kwargs)
            owner._join_upload()

        with patch.object(parquet_scan._StreamingUpload, '_emit', serial):
            oracle = self.scan._build(candidate, _rebuild=True)
        actual = self.scan._build(candidate, _rebuild=True)
        self.assertEqual(actual.references, oracle.references)
        self.assertEqual(actual.row_counts, oracle.row_counts)
        self.assertEqual(actual.members, oracle.members)
        self.assertEqual(actual.bounds, oracle.bounds)
        self.assertEqual(self.rows(actual), self.rows(oracle))
        self.assertEqual(self.scan._commit(candidate, actual), 'committed')

    def assert_cleanup_contains(self, refs):
        with self.store.connect() as c:
            actual = {row['artifact_id'] for row in c.execute(
                'SELECT artifact_id FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',
                (self.tenant,))}
        self.assertTrue({r['artifact_id'] for r in refs} <= actual)

    def test_failed_put_keeps_successful_cleanup_and_does_not_publish_or_retry(self):
        self.failed_put(RuntimeError)

    def test_upload_worker_stop_keeps_prior_successful_cleanup(self):
        self.failed_put(KeyboardInterrupt)

    def failed_put(self, error_type):
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        original = self.archive.put_raw
        refs, calls = [], []

        def put(**kwargs):
            calls.append(kwargs['native_id'])
            if len(calls) == 2:
                raise error_type('synthetic failed PUT')
            result = original(**kwargs)
            refs.append(result)
            return result

        with patch.object(self.archive, 'put_raw', put), \
             patch.object(self.scan, '_commit', wraps=self.scan._commit) as commit:
            with self.assertRaisesRegex(error_type, 'synthetic failed PUT'):
                self.scan._build(candidate, _rebuild=True)
            commit.assert_not_called()
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(refs), 1)
        self.assert_cleanup_contains(refs)
        self.assertEqual(self.candidate(), candidate)

    def test_producer_interrupt_joins_successful_upload_before_cleanup(self):
        self.producer_interrupt(cleanup_fails=False)

    def test_cleanup_failure_preserves_producer_stop_as_original_cause(self):
        self.producer_interrupt(cleanup_fails=True)

    def producer_interrupt(self, *, cleanup_fails):
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        blocked, release, interrupted = (threading.Event() for _ in range(3))
        original_put = self.archive.put_raw
        original_row = self.scan._passage_row
        refs, produced = [], []

        def put(**kwargs):
            blocked.set()
            if not release.wait(5):
                raise RuntimeError('synthetic upload release missing')
            result = original_put(**kwargs)
            refs.append(result)
            return result

        def row(*args):
            produced.append(True)
            if len(produced) == 3:
                self.assertTrue(blocked.wait(2))
                interrupted.set()
                raise KeyboardInterrupt('synthetic producer stop')
            return original_row(*args)

        with patch.object(self.archive, 'put_raw', put), \
             patch.object(self.scan, '_passage_row', row), \
             (patch.object(self.scan, '_schedule_cleanup',
                           side_effect=RuntimeError('synthetic cleanup failure'))
              if cleanup_fails else nullcontext()), \
             ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.scan._build, candidate, _rebuild=True)
            try:
                self.assertTrue(interrupted.wait(3))
                self.assertFalse(future.done(), 'abort abandoned the in-flight upload')
            finally:
                release.set()
            if cleanup_fails:
                with self.assertRaisesRegex(parquet_scan.ParquetScanError,
                                            'parquet_scan_cleanup_enqueue_failed') as failed:
                    future.result(timeout=5)
                self.assertIsInstance(failed.exception.__cause__, KeyboardInterrupt)
                self.assertEqual(str(failed.exception.__cause__), 'synthetic producer stop')
            else:
                with self.assertRaisesRegex(KeyboardInterrupt, 'synthetic producer stop'):
                    future.result(timeout=5)
        self.assertEqual(len(refs), 1)
        if not cleanup_fails:
            self.assert_cleanup_contains(refs)
        self.assertEqual(self.candidate(), candidate)

    def test_generation_change_during_upload_retains_native_stale_fence(self):
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        original = self.archive.put_raw
        changed = []

        def put(**kwargs):
            if not changed:
                changed.append(self.scan.seed_backfill(tenant_id=self.tenant))
            return original(**kwargs)

        with patch.object(self.archive, 'put_raw', put):
            result = self.scan._build(candidate, _rebuild=True)
        self.assertEqual(changed, [1])
        self.assertEqual(self.scan._commit(candidate, result), 'stale')
        self.assertGreater(self.candidate().generation, candidate.generation)


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        UploadOverlap(name) for name in UploadOverlap.__dict__ if name.startswith('test_')))
    raise SystemExit(not result.wasSuccessful())
