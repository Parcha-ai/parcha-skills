from contextlib import ExitStack
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, call, patch

from psycopg.errors import AdminShutdown

from server.recall_server import managed_worker
from server.recall_server.managed_worker import ManagedConnectorWorker


class StopDaemon(BaseException):
    pass


class ManagedWorkerRecoveryTests(unittest.TestCase):
    def runtime(self, claims):
        stack = ExitStack()
        self.addCleanup(stack.close)
        worker = object.__new__(ManagedConnectorWorker)
        worker._claim = Mock(side_effect=claims)
        constructor = stack.enter_context(
            patch.object(managed_worker, "ManagedConnectorWorker", return_value=worker)
        )
        stack.enter_context(patch.object(managed_worker, "build_archive_store"))
        stack.enter_context(patch.object(managed_worker.SecretBox, "from_env"))
        stack.enter_context(
            patch.dict(os.environ, {
                "RECALL_MANAGED_PROJECTIONS_ENABLED": "0",
                "RECALL_CHUNK_BODY_READS": "postgres",
            })
        )
        sleep = stack.enter_context(patch.object(managed_worker.time, "sleep"))
        return worker, constructor, sleep

    def run_worker(self, *, once=False):
        return managed_worker.run_managed_worker(
            object(), state_root=Path("unused"), once=once, interval_seconds=60
        )

    def test_daemon_retries_failed_claim_then_completes_next_cycle(self):
        worker, constructor, sleep = self.runtime([
            AdminShutdown("private connection details"), None
        ])
        sleep.side_effect = [None, StopDaemon()]
        with self.assertLogs(managed_worker.LOG, level="ERROR") as logs:
            with self.assertRaises(StopDaemon):
                self.run_worker()
        self.assertEqual(worker._claim.call_count, 2)
        constructor.assert_called_once()
        self.assertEqual(sleep.call_args_list, [call(60), call(30)])
        self.assertIn("AdminShutdown", logs.output[0])
        self.assertNotIn("private connection details", "\n".join(logs.output))

    def test_once_preserves_database_failure_without_retry(self):
        failure = AdminShutdown("primary switched")
        worker, _, sleep = self.runtime([failure, None])
        with self.assertRaises(AdminShutdown) as caught:
            self.run_worker(once=True)
        self.assertIs(caught.exception, failure)
        self.assertEqual(worker._claim.call_count, 1)
        sleep.assert_not_called()

    def test_shutdown_signals_are_not_retried(self):
        for failure in (KeyboardInterrupt(), SystemExit(2)):
            with self.subTest(kind=type(failure).__name__):
                worker, _, sleep = self.runtime([failure])
                with self.assertRaises(type(failure)):
                    self.run_worker()
                self.assertEqual(worker._claim.call_count, 1)
                sleep.assert_not_called()

    def test_successful_once_result_is_unchanged(self):
        worker, _, sleep = self.runtime([None])
        result = self.run_worker(once=True)
        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["cycles"], 1)
        self.assertEqual(result["committed"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(worker._claim.call_count, 1)
        sleep.assert_not_called()

    def test_successful_coverage_is_logged_and_returned_without_source_content(self):
        worker, _, sleep = self.runtime([])
        coverage = {"known_channels": 5, "history_baselined_channels": 2,
                    "history_baseline_pending_channels": 3,
                    "historical_mutations_verified": False}
        worker.run_once = Mock(return_value={
            "status": "committed", "processed": 1, "committed": 1, "failed": 0,
            "installation_sha256": "a" * 64, "coverage": coverage,
        })
        with self.assertLogs(managed_worker.LOG, level="INFO") as logs:
            result = self.run_worker(once=True)
        self.assertEqual(result["coverage"], coverage)
        self.assertIn('"history_baseline_pending_channels": 3', logs.output[0])
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
