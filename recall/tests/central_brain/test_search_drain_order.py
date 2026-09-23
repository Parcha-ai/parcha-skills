"""Ready search work must not wait for the next logical document batch."""

import threading
import unittest
from unittest.mock import patch

from recall_server import projection_worker as worker
from tests.central_brain import test_projection_worker as fixtures


class SearchDrainOrderTests(unittest.TestCase):
    def test_search_commits_before_a_giant_logical_batch_unblocks(self):
        calls, outcomes, failures, bounds = [], [], [], []
        entered, release, searched = (threading.Event() for _ in range(3))
        clock = fixtures._FakeClock()

        class Logical(fixtures._Logical):
            def project_pending(self, **kwargs):
                bounds.append(kwargs)
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("synthetic logical block")
                clock.advance(240)
                return super().project_pending(**kwargs)

        def search():
            calls.append("search")
            clock.advance(0.25)
            searched.set()
            return dict(
                status="pending",
                months=1,
                rows=7,
                deleted=2,
                failed=1,
                rate_limited=3,
                pending=5,
            )

        def run():
            try:
                outcomes.append(
                    worker.run_projection_worker(
                        Logical(calls, work=1),
                        fixtures._Passages(calls, work=1),
                        search_plane=search,
                        clock=clock,
                        **fixtures.SearchBeforeParquetTests().kwargs(),
                    )
                )
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(searched.is_set(), "search blocked behind logical upload")
            self.assertEqual(calls, ["embeddings", "passages", "search"])
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(failures)
        self.assertEqual(calls, ["embeddings", "passages", "search", "logical"])
        self.assertEqual(len(bounds), 1)
        self.assertEqual(bounds[0]["batch_size"], 5)
        self.assertEqual(bounds[0]["max_batches"], 2)
        self.assertEqual(bounds[0]["upload_concurrency"], 1)
        result = outcomes[0]
        for name, expected in dict(
            search_plane_elapsed_ms=250,
            logical_elapsed_ms=240000,
            cycle_elapsed_ms=240250,
            search_plane_rows=7,
            search_plane_failed=1,
            search_plane_rate_limited=3,
        ).items():
            self.assertEqual(result[name], expected)
        self.assertEqual(result["status"], "pending")

    def test_logical_failure_preserves_completed_search_timing(self):
        self.check_failure("logical")

    def test_search_failure_stops_before_logical_and_attributes_its_time(self):
        self.check_failure("search_plane")

    def check_failure(self, phase):
        calls = []
        clock = fixtures._FakeClock()
        failure = RuntimeError("synthetic failure")

        class Logical(fixtures._Logical):
            def project_pending(self, **kwargs):
                calls.append("logical")
                clock.advance(240)
                raise failure

        def search():
            calls.append("search")
            clock.advance(0.25)
            if phase == "search_plane":
                raise failure
            return dict(
                status="complete",
                months=1,
                rows=7,
                deleted=0,
                failed=0,
                rate_limited=0,
                pending=0,
            )

        with patch.object(worker, "record_cycle") as record:
            with self.assertLogs(worker.LOG, level="ERROR") as logged:
                with self.assertRaises(RuntimeError) as caught:
                    worker.run_projection_worker(
                        Logical(calls),
                        fixtures._Passages(calls),
                        search_plane=search,
                        clock=clock,
                        **fixtures.SearchBeforeParquetTests().kwargs(),
                    )
        self.assertIs(caught.exception, failure)
        record.assert_not_called()
        self.assertEqual(
            calls,
            ["embeddings", "passages", "search"]
            + (["logical"] if phase == "logical" else []),
        )
        message = logged.output[0]
        for expected in (
            f"failed_phase={phase}",
            "search_plane_elapsed_ms=250",
            "parquet_elapsed_ms=0",
            "thin_elapsed_ms=0",
        ):
            self.assertIn(expected, message)
        self.assertIn(
            "logical_elapsed_ms=" + ("240000" if phase == "logical" else "0"), message
        )
