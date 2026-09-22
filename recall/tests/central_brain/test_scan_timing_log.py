from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

from recall_server import mcp


class ScanTimingLogTests(unittest.TestCase):
    def observe(self, result, name="recall_scan", elapsed=75.5):
        with patch.object(mcp, "_call_tool", return_value=result), patch.object(
            mcp.time, "monotonic", side_effect=[0, elapsed]
        ), self.assertLogs("recall.mcp", level="INFO") as logs:
            actual = mcp._call_tool_observed(None, {}, name, {})
        self.assertIs(actual, result)
        return [record.getMessage() for record in logs.records]

    def test_success_preserves_result_and_original_log(self):
        result = {"stdout": "PRIVATE", "timing": {
            "totalMs": 75000, "queueMs": 62000, "executeMs": 13000,
            "phases": {"stage_start_to_objects_readyMs": 3000,
                       "program_start_to_program_endMs": 1500}}}
        before = copy.deepcopy(result)
        logs = self.observe(result)
        self.assertEqual(result, before)
        self.assertEqual(logs[0], "mcp_tool tool=recall_scan outcome=ok elapsed_ms=75500.000 deadline_exceeded=unknown")
        self.assertEqual(len(logs), 2)
        self.assertTrue(logs[1].startswith("recall_scan_timing "))
        for field in ("elapsed_ms=75500.000", "archil_total_ms=75000.000", "queue_ms=62000.000", "unallocated_ms=500.000", "stage_start_to_objects_ready_ms=3000.000", "program_start_to_program_end_ms=1500.000"):
            self.assertIn(field, logs[1])
        self.assertNotIn("PRIVATE", logs[1])

    def test_other_tools_unchanged(self):
        self.assertEqual(len(self.observe({}, "recall_context")), 1)

    def test_absent_or_malformed_timing_is_unknown(self):
        for timing in (None, "PRIVATE", [], True, {}):
            with self.subTest(timing=type(timing).__name__):
                log = self.observe({"timing": timing})[1]
                self.assertIn("archil_total_ms=-1.000", log)
                self.assertIn("unallocated_ms=-1.000", log)
                self.assertNotIn("PRIVATE", log)

    def test_only_finite_bounded_numeric_allowlist(self):
        for invalid in (True, False, "PRIVATE", float("nan"), float("inf"), -0.1, 3600001, 10**1000, {}, []):
            with self.subTest(kind=type(invalid).__name__):
                log = self.observe({"timing": {"totalMs": invalid, "phases": {"wrapperMs": invalid, "PRIVATE_KEY": 123}}})[1]
                self.assertIn("archil_total_ms=-1.000", log)
                self.assertIn("wrapper_ms=-1.000", log)
                self.assertNotIn("PRIVATE", log)
        log = self.observe({"timing": {"queueMs": 0, "executeMs": 3600000}})[1]
        self.assertIn("queue_ms=0.000", log)
        self.assertIn("execute_ms=3600000.000", log)

    def test_inconsistent_total_does_not_claim_negative_or_zero_residual(self):
        self.assertIn("unallocated_ms=-1.000", self.observe({"timing": {"totalMs": 76000}})[1])

    def test_existing_failure_propagates_without_success_timing(self):
        original = RuntimeError("PRIVATE")
        with patch.object(mcp, "_call_tool", side_effect=original), self.assertLogs("recall.mcp", level="ERROR") as logs:
            with self.assertRaises(RuntimeError) as raised:
                mcp._call_tool_observed(None, {}, "recall_scan", {})
        self.assertIs(raised.exception, original)
        self.assertEqual(len(logs.records), 1)
        self.assertNotIn("recall_scan_timing", logs.output[0])
        self.assertNotIn("PRIVATE", logs.output[0])

    def test_new_log_failure_cannot_fail_success(self):
        result = {"timing": {}}
        def emit(message, *args):
            if message.startswith("recall_scan_timing"):
                raise RuntimeError("diagnostic sink failed")
        with patch.object(mcp, "_call_tool", return_value=result), patch.object(mcp.LOG, "info", side_effect=emit):
            self.assertIs(mcp._call_tool_observed(None, {}, "recall_scan", {}), result)

    def test_concurrent_measurements_do_not_share_state(self):
        with self.assertLogs("recall.mcp", level="INFO") as logs:
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda total: mcp._log_scan_timing({"timing": {"totalMs": total}}, total + 10), (100, 200)))
        rows = [record.getMessage() for record in logs.records]
        self.assertEqual(len(rows), 2)
        self.assertTrue(any("archil_total_ms=100.000" in row for row in rows))
        self.assertTrue(any("archil_total_ms=200.000" in row for row in rows))
        self.assertTrue(all("unallocated_ms=10.000" in row for row in rows))
