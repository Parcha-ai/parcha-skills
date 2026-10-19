"""Worker error settlement keeps one scan and one publication coordinator."""
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from recall_server.archive import ArchiveError
from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages, _Scan


class ParquetWorkerFairness(unittest.TestCase):
    def exercise(self, *, scan_error=False, callback_error=False, passage_error=None):
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

        class Passages(_Passages):
            def project_pending(self, **kwargs):
                coordinator_threads.append(threading.get_ident())
                self_outer.assertEqual(kwargs['batch_size'], 1)
                self_outer.assertEqual(kwargs['max_batches'], 1)
                self_outer.assertEqual(kwargs['concurrency'], 1)
                if blocked.is_set() and passage_error is not None:
                    heartbeat.set()
                    raise passage_error
                return super().project_pending(**kwargs)

        self_outer = self

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
                _Logical(calls, work=0), Passages(calls, work=0), Scan(calls),
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
            if passage_error is not None:
                with self.assertRaises(type(passage_error)) as caught:
                    future.result(timeout=3)
                self.assertIs(caught.exception, passage_error)
            elif callback_error:
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
        self.assertGreaterEqual(calls.count('passages'), 1 if passage_error else 2)

    def test_one_scan_completes_after_publication_on_original_coordinator(self):
        self.exercise()

    def test_scan_failure_settles_and_propagates_without_replay(self):
        with self.assertLogs('recall_server.projection_worker', level='ERROR'):
            self.exercise(scan_error=True)

    def test_callback_failure_joins_scan_and_preserves_both_errors(self):
        with self.assertLogs('recall_server.projection_worker', level='ERROR') as captured:
            self.exercise(scan_error=True, callback_error=True)
        self.assertTrue(any('parquet settlement failed' in row for row in captured.output))

    def test_passage_failure_or_shutdown_settles_active_scan_without_replay(self):
        for error in (RuntimeError('synthetic_passage_failure'), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                self.exercise(passage_error=error)


class QuietScanOutcomeTests(unittest.TestCase):
    def test_only_known_clean_scan_clears_quiet_window(self):
        outcomes = (
            ({'status': 'complete', 'failed': 0, 'stale': 0, 'contended': 0, 'requeued': 0}, True),
            ({'status': 'complete', 'failed': 0, 'stale': 1, 'contended': 0, 'requeued': 0}, False),
            ({'status': 'complete', 'failed': 0, 'stale': 0, 'contended': 1, 'requeued': 0}, False),
            ({'status': 'complete', 'failed': 0, 'stale': 0, 'contended': 0, 'requeued': 1}, False),
            ({'status': 'complete', 'failed': 1, 'stale': 0, 'contended': 0, 'requeued': 0}, False),
            ({'status': 'unknown'}, False),
            ({'status': 'complete'}, False),
            (RuntimeError('unknown scan outcome'), False),
        )
        for outcome, reopened in outcomes:
            with self.subTest(outcome=outcome):
                self.exercise(outcome, reopened)

    def exercise(self, outcome, reopened, *, callback_error=None):
        calls, during = [], []
        active = [False]
        scan_index = [-1]
        release = threading.Event()
        check = self

        class Passages(_Passages):
            def project_pending(self, **kwargs):
                if active[0]:
                    during.append(scan_index[0])
                    if scan_index[0] == 0 and callback_error is not None:
                        release.set()
                        raise callback_error
                return super().project_pending(**kwargs)

        class Scan(_Scan):
            def project_pending(self, **kwargs):
                scan_index[0] += 1
                release.clear()
                active[0] = True
                try:
                    check.assertTrue(release.wait(2))
                    if scan_index[0] == 1:
                        if isinstance(outcome, Exception):
                            raise outcome
                        return dict(super().project_pending(**kwargs), **outcome)
                    return dict(status='complete',failed=0,stale=1,contended=0,
                                requeued=0,shards=0,rows=0)
                finally:
                    active[0] = False

        def search():
            if active[0]:
                release.set()
            return dict(status='complete',months=0,rows=0,pending=0,deleted=0,failed=0,rate_limited=0)

        with patch('recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS',.001):
            run_projection_worker(_Logical(calls,work=0),Passages(calls,work=0),Scan(calls),
                tenant_id='synthetic',logical_batch_size=1,passage_batch_size=1,
                embedding_batch_size=1,max_batches_per_cycle=1,upload_concurrency=1,
                passage_concurrency=1,interval_seconds=.1,once=False,max_cycles=3,
                skip_embedding=True,parquet_every_cycles=1,search_plane=search,sleep=lambda _:None)
        self.assertEqual(during,[0,2] if reopened else [0])

    def test_unknown_passage_callback_retains_quiet_window(self):
        self.exercise({'status': 'unknown'}, False,
                      callback_error=RuntimeError('unknown passage commit outcome'))

    def test_restart_resets_only_ephemeral_quiet_state(self):
        # Each new worker conservatively discovers any new collision through
        # unchanged native CAS; it does not rewrite a durable queue on restart.
        for _ in range(2):
            self.exercise({'status': 'complete', 'failed': 0, 'stale': 0,
                           'contended': 0, 'requeued': 0}, True)
