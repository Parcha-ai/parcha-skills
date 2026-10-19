"""Deferred queue depth cannot escape the worker's decision boundaries."""
import threading
import unittest
from unittest.mock import patch

from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages, _Scan


class PassageCountBoundary(unittest.TestCase):
    def exercise_scan(self, error=None):
        calls, thinning = [], []
        settled, scanning, release = (threading.Event() for _ in range(3))
        outer = self

        class Passages(_Passages):
            counts = 0
            depth = 0

            def project_pending(self, **kwargs):
                outer.assertTrue(kwargs["defer_count"])
                result = super().project_pending(**kwargs)
                if scanning.is_set():
                    self.depth = 2
                    release.set()
                return dict(result, status="unknown", pending=None)

            def count_pending(self, **kwargs):
                self.counts += 1
                calls.append("count")
                if self.counts == 2:
                    outer.assertTrue(settled.is_set())
                    if error is not None:
                        raise error
                return dict(status="pending" if self.depth else "complete", pending=self.depth, count_ms=7)

        class Scan(_Scan):
            def project_pending(self, **kwargs):
                # The pre-scan decision used an observed zero, not unknown.
                outer.assertEqual(kwargs["compaction_budget"], 1)
                outer.assertEqual(passages.counts, 1)
                scanning.set()
                try:
                    if not release.wait(2):
                        raise AssertionError("fixture_callback_missing")
                    return dict(super().project_pending(**kwargs), failed=0, requeued=0)
                finally:
                    settled.set()

        passages = Passages(calls, work=0)

        def run():
            return run_projection_worker(
                _Logical(calls, work=0), passages, Scan(calls), tenant_id="synthetic",
                logical_batch_size=1, passage_batch_size=1, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=0.1, once=True, skip_embedding=True,
                search_plane=lambda: dict(status="complete", months=0, rows=0, pending=0, deleted=0, failed=0, rate_limited=0),
                body_thinner=lambda busy: thinning.append(busy) or dict(status="complete", documents=0, refused=0, document_bytes_removed=0, event_bytes_replaced=0),
            )

        with patch("recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS", 0.01):
            if error is None:
                result = run()
                self.assertEqual(result["passage_pending"], 2)
                self.assertEqual(result["status"], "pending")
                self.assertEqual(result["passage_count_ms"], 14)
                self.assertEqual(thinning, [True])
            else:
                with self.assertRaises(type(error)) as caught:
                    run()
                self.assertIs(caught.exception, error)
                self.assertEqual(thinning, [])
        self.assertTrue(settled.is_set())
        self.assertEqual(passages.counts, 2)

    def test_scan_callback_pending_depth_is_refreshed_before_thinning(self):
        self.exercise_scan()

    def test_failed_post_scan_count_never_reports_complete_or_thins(self):
        with self.assertLogs("recall_server.projection_worker", level="ERROR"):
            self.exercise_scan(ConnectionError("synthetic_count_failure"))

    def test_shutdown_at_post_scan_count_occurs_after_scan_settlement(self):
        self.exercise_scan(KeyboardInterrupt())
