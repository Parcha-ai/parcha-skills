"""Worker error settlement keeps one scan and one publication coordinator."""
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from recall_server.archive import ArchiveError
from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages, _Scan


class ParquetWorkerFairness(unittest.TestCase):
    def exercise(self, *, scan_error=False, callback_error=False):
        blocked, release, heartbeat, settled = (threading.Event() for _ in range(4))
        calls, coordinator_threads = [], []

        class Scan(_Scan):
            def project_pending(self, **kwargs):
                calls.append('scan_started')
                blocked.set()
                try:
                    if not release.wait(5):
                        raise AssertionError('fixture_scan_not_released')
                    if scan_error:
                        raise ArchiveError('synthetic_archive_failure')
                    return super().project_pending(**kwargs)
                finally:
                    settled.set()

        def search():
            coordinator_threads.append(threading.get_ident())
            if blocked.is_set():
                heartbeat.set()
                if callback_error:
                    raise ValueError('synthetic_publication_failure')
            return {'status': 'complete', 'months': 0, 'rows': 0, 'deleted': 0,
                    'failed': 0, 'rate_limited': 0, 'pending': 0}

        def worker():
            return run_projection_worker(
                _Logical(calls, work=0), _Passages(calls, work=0), Scan(calls),
                tenant_id='synthetic', logical_batch_size=1, passage_batch_size=1,
                embedding_batch_size=1, max_batches_per_cycle=1, upload_concurrency=1,
                passage_concurrency=1, interval_seconds=0.1, once=True,
                skip_embedding=True, parquet_every_cycles=1, search_plane=search)

        with patch('recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS', 0.01), \
                ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(2))
                self.assertTrue(heartbeat.wait(2))
                self.assertFalse(settled.is_set())
                self.assertFalse(future.done())
                self.assertEqual(calls.count('scan_started'), 1)
            finally:
                release.set()
            if callback_error:
                with self.assertRaisesRegex(ValueError, 'synthetic_publication_failure'):
                    future.result(timeout=3)
            elif scan_error:
                with self.assertRaisesRegex(ArchiveError, 'synthetic_archive_failure'):
                    future.result(timeout=3)
            else:
                self.assertEqual(future.result(timeout=3)['parquet_shards'], 1)
        self.assertTrue(settled.is_set())
        self.assertEqual(calls.count('scan_started'), 1)
        self.assertEqual(len(set(coordinator_threads)), 1)
        self.assertEqual(calls.count('passages'), 1)

    def test_one_scan_completes_after_publication_on_original_coordinator(self):
        self.exercise()

    def test_scan_failure_settles_and_propagates_without_replay(self):
        with self.assertLogs('recall_server.projection_worker', level='ERROR'):
            self.exercise(scan_error=True)

    def test_callback_failure_joins_scan_and_preserves_both_errors(self):
        with self.assertLogs('recall_server.projection_worker', level='ERROR') as captured:
            self.exercise(scan_error=True, callback_error=True)
        self.assertTrue(any('parquet settlement failed' in row for row in captured.output))
