from __future__ import annotations

import json
import subprocess
import hashlib
import unittest
from unittest.mock import patch

from recall_server import deep_inspection as deep
from tests.central_brain import test_scan_staging_concurrency as fixture

PREFIX = "RECALL_STAGE_TIMING_V1\t"


def summary():
    # Four overlapping one-second lookups: sums are worker time, not wall time.
    value = dict(object_count=4, slow_1s_count=4, slow_5s_count=0,
                 task_max_us=1_000_000)
    for phase in ("lookup", "local", "bind", "remount"):
        value.update({f"{phase}_{metric}_us": 0 for metric in ("sum", "max")})
    value.update(lookup_sum_us=4_000_000, lookup_max_us=1_000_000)
    return value


def complete_markers():
    origin = 1_790_000_000_000_000
    return (
        "\n".join(
            f"RECALL_EXEC_TIMING_V1\t{phase}\t{origin + i * 2_000_000}"
            for i, phase in enumerate(deep.EXEC_TIMING_ORDER)
        )
        + "\n"
    )


class StagingTimingTests(unittest.TestCase):
    def test_private_summary_logs_same_execution_phases_without_response_change(self):
        original = complete_markers() + "ordinary stderr\n"
        before = deep._execution_timing(original, {})
        with self.assertLogs("recall.mcp", "INFO") as logs:
            after = deep._execution_timing(
                original + PREFIX + json.dumps(summary()) + "\n", {}
            )
        self.assertEqual(after, before)
        self.assertEqual(len(logs.records), 1)
        row = logs.records[0].getMessage()
        self.assertIn("stage_ms=2000.000", row)
        self.assertIn("program_ms=2000.000", row)
        self.assertIn("lookup_sum_us=4000000", row)
        self.assertIn("lookup_max_us=1000000", row)
        self.assertNotIn("union", row)
        self.assertEqual(len(summary()), 12)
        self.assertIn("valid=1", row)

    def test_invalid_duplicate_and_oversized_summary_unknown_only(self):
        cases = [
            "{PRIVATE",
            json.dumps(dict(summary(), PRIVATE="secret")),
            "0" * 4097,
            "[" * 1500 + "]" * 1500,
        ]
        for key, value in [
            ("object_count", 514),
            ("slow_1s_count", 5),
            ("task_max_us", 3_600_000_001),
            ("lookup_max_us", True),
            ("bind_sum_us", -1),
            ("slow_5s_count", 5),
            ("task_max_us", float("nan")),
            ("lookup_sum_us", 5_000_000),
            ("local_max_us", 1_000_001),
            ("slow_1s_count", 0),
            ("slow_5s_count", 1),
            ("covered_us", 1_000_000),
        ]:
            cases.append(json.dumps(dict(summary(), **{key: value})))
        cases.append(
            json.dumps(summary()).replace(
                '"object_count": 4', '"object_count": 3, "object_count": 4'
            )
        )
        cases.append(json.dumps(summary()) + "\n" + PREFIX + json.dumps(summary()))
        for raw in cases:
            with (
                self.subTest(raw=raw[:40]),
                self.assertLogs("recall.mcp", "INFO") as logs,
            ):
                stderr, timing, _ = deep._execution_timing(
                    "ordinary stderr\n" + PREFIX + raw + "\n", {}
                )
            self.assertEqual(stderr, "ordinary stderr\n")
            self.assertNotIn("staging", timing)
            self.assertIn("valid=0", logs.records[0].getMessage())
            self.assertNotIn("PRIVATE", logs.records[0].getMessage())
        # A vaguely similar ordinary stderr line is never swallowed.
        self.assertEqual(
            deep._execution_timing("RECALL_STAGE_TIMING_V1 ordinary\n", {})[0],
            "RECALL_STAGE_TIMING_V1 ordinary\n",
        )

    def test_absent_summary_and_logging_failure_preserve_result(self):
        before = deep._execution_timing(complete_markers(), {})
        with patch("logging.Logger.info", side_effect=RuntimeError("sink")):
            after = deep._execution_timing(
                complete_markers() + PREFIX + json.dumps(summary()) + "\n", {}
            )
        self.assertEqual(after, before)

    def stage(self, **kwargs):
        case = fixture.ScanStagingConcurrencyTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.stage(**kwargs)
        lines = [
            line
            for line in case.stderr.getvalue().splitlines()
            if line.startswith(PREFIX)
        ]
        self.assertEqual(len(lines), 1)
        return case, json.loads(lines[0][len(PREFIX) :])

    def test_injected_existing_boundary_delays_are_attributed(self):
        for phase in ("lookup", "local", "bind", "remount"):
            with self.subTest(phase=phase):
                case, value = self.stage(delay=0.04, timing_phase=phase)
                self.assertEqual(value["object_count"], len(case.items))
                self.assertGreaterEqual(value[f"{phase}_sum_us"], 9 * 35_000)
                self.assertGreaterEqual(value[f"{phase}_max_us"], 35_000)
                self.assertEqual(case.completed, 9)
                self.assertLessEqual(case.peak, 4)
                self.assertEqual(len(case.calls), 18)
                self.assertEqual(set(value), set(summary()))
                self.assertTrue(all(type(v) is int for v in value.values()))
                with self.assertLogs("recall.mcp", "INFO") as logs:
                    deep._execution_timing(case.stderr.getvalue(), {})
                self.assertIn("valid=1", logs.records[0].getMessage())

    def test_one_slow_object_differs_from_distributed_waits(self):
        _, one = self.stage(delay=1.01, single=True, timing_phase="lookup")
        _, many = self.stage(delay=1.01, timing_phase="lookup")
        self.assertEqual(one["slow_1s_count"], 1)
        self.assertEqual(many["slow_1s_count"], 9)
        self.assertEqual(one["slow_5s_count"], 0)
        self.assertLess(one["lookup_sum_us"], many["lookup_sum_us"] / 4)

    def test_original_failure_has_no_completed_summary_or_program(self):
        for failure in ("stat", "bind", "remount"):
            case = fixture.ScanStagingConcurrencyTests()
            case.setUp()
            self.addCleanup(case.doCleanups)
            expected = OSError if failure == "stat" else subprocess.CalledProcessError
            with self.subTest(failure=failure), self.assertRaises(expected) as raised:
                case.stage(fail=failure, delay=0.01)
            if failure != "stat":
                self.assertEqual(raised.exception.returncode, 1)
                self.assertEqual(raised.exception.cmd, ["mount"])
            self.assertEqual(case.active, 0)
            self.assertNotIn(PREFIX, case.stderr.getvalue())
            self.assertNotIn("objects_ready", case.stderr.getvalue())
            self.assertFalse((case.root / "tmp/recall-agent/duckdb-real").exists())

    def test_maximum_objects_remain_bounded_and_missing_data_is_counted(self):
        case = fixture.ScanStagingConcurrencyTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        for index in range(len(case.items), 513):
            body = f"object-{index}".encode()
            digest = hashlib.sha256(body).hexdigest()
            item = deep.AgentExecObject(f"objects/{digest[:2]}/{digest}", digest)
            path = case.root / "mnt/archil/evidence" / item.object_key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            case.items.append(item)
        case.tool = case.items[-1]
        (case.root / "mnt/archil/evidence" / case.items[0].object_key).unlink()
        case.stage(
            missing=True,
            tools={"linux-x86_64": case.tool, "linux-arm64": case.items[-2]},
        )
        rows = [
            line
            for line in case.stderr.getvalue().splitlines()
            if line.startswith(PREFIX)
        ]
        self.assertEqual(len(rows), 1)
        self.assertLess(len(rows[0]), 4096)
        value = json.loads(rows[0][len(PREFIX) :])
        self.assertEqual(value["object_count"], 513)
        self.assertEqual(set(value), set(summary()))
        self.assertIn("objects_unavailable\t1", case.stderr.getvalue())
        self.assertEqual(case.completed, 512)
        self.assertLessEqual(case.peak, 4)
        self.assertFalse(any(item.object_key in rows[0] for item in case.items))
        with self.assertLogs("recall.mcp", "INFO") as logs:
            deep._execution_timing(case.stderr.getvalue(), {})
        self.assertIn("valid=1", logs.records[0].getMessage())

    def test_document_execution_does_not_emit_scan_summary(self):
        case = fixture.ScanStagingConcurrencyTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        with patch("time.monotonic_ns", side_effect=AssertionError("non-scan timing")):
            case.stage(scan=False, delay=0.001)
        self.assertNotIn("stage_times", case.stage_namespace)
        self.assertNotIn(PREFIX, case.stderr.getvalue())
        self.assertEqual(case.peak, 1)


if __name__ == "__main__":
    unittest.main()
