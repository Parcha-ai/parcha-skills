# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Remote launch intent is durable before one POST; uncertain jobs never retry."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.retirement_jobs import (
    RenderJob,
    encode_result,
    decode_result,
    reconcile_enrollment,
    package_body,
)
from recall_server.retirement_campaign import digest


class JobTests(unittest.TestCase):
    def test_identical_plans_cannot_reconcile_an_older_attempt(self):
        config = dict(
            commit="commit",
            service_id="service",
            owner_modules={
                "recall_server." + name: "0" * 64
                for name in (
                    "retirement_runner",
                    "chunk_retirement",
                    "parent_chunk_proof",
                    "chunk_bodies",
                    "streaming_locators",
                    "locator_backfill_plan",
                )
            },
        )
        _, older_pin = package_body(config, dict(operation="enroll", plans=[]))
        _, current_pin = package_body(config, dict(operation="enroll", plans=[]))
        self.assertNotEqual(older_pin, current_pin)
        with self.assertRaisesRegex(ValueError, "wrong_pin"):
            decode_result(encode_result([], older_pin), current_pin)

    def test_reconciliation_uses_original_pin_config_and_recorded_job(self):
        config = dict(service_id="service")
        launch = dict(
            pin="original-pin", config_sha256=digest(config), owner_id="owner"
        )
        events = [
            dict(name="launch_intent", value=launch),
            dict(name="job", value=dict(id="job")),
        ]
        with patch.object(RenderJob, "reconcile", return_value=[]) as inspect:
            self.assertEqual(reconcile_enrollment(config, "job", events, Mock()), [])
            self.assertEqual(
                inspect.call_args.args[:3], ("job", "original-pin", "owner")
            )
            inspect.reset_mock()
            with self.assertRaisesRegex(ValueError, "reconciliation_job_changed"):
                reconcile_enrollment(config, "wrong", events, Mock())
            inspect.assert_not_called()
            with self.assertRaisesRegex(ValueError, "reconciliation_config_changed"):
                reconcile_enrollment(dict(service_id="other"), "job", events, Mock())
            inspect.assert_not_called()

    def test_reconcile_reads_exact_terminal_job_without_launch(self):
        outcome = [dict(status="enrolled", certainty="known")]
        calls = []

        def request(method, path, body=None):
            calls.append((method, path))
            self.assertEqual(method, "GET")
            if path.endswith("jobs/job"):
                return dict(id="job", createdAt="now", status="succeeded")
            self.assertTrue(path.startswith("/logs?"))
            return dict(
                hasMore=False,
                logs=[
                    dict(message=__import__("json").dumps(row))
                    for row in encode_result(outcome, "pin")
                ],
            )

        transport = RenderJob(dict(service_id="service"), request=request)
        self.assertEqual(transport.reconcile("job", "pin", "owner", Mock()), outcome)
        self.assertEqual(len(calls), 2)
        with self.assertRaisesRegex(ValueError, "wrong_pin"):
            transport.reconcile("job", "other", "owner", Mock())

    def test_reconcile_refuses_nonterminal_or_failed_job(self):
        for status in ("running", "failed", "canceled"):
            request = Mock(return_value=dict(id="job", createdAt="now", status=status))
            transport = RenderJob(dict(service_id="service"), request=request)
            with self.assertRaisesRegex(
                ValueError, "job_failed_requires_reconciliation"
            ):
                transport.reconcile("job", "pin", "owner", Mock())
            self.assertEqual(request.call_count, 1)

    def test_slow_preflight_cannot_reach_post(self):
        elapsed = [0]
        calls = []

        def request(method, path, body=None):
            calls.append(method)
            elapsed[0] += 45
            if method == "POST":
                raise AssertionError("expired preflight launched a job")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.endswith("jobs?limit=100"):
                return []
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {},
            clock=lambda: elapsed[0],
        )
        with self.assertRaisesRegex(ValueError, "preflight_deadline"):
            transport.run("pass", "pin", Mock())
        self.assertNotIn("POST", calls)

    def test_result_chunks_detect_missing_or_changed_output(self):
        result = {"rows": ["test" * 1000]}
        rows = encode_result(result, "pin")
        self.assertEqual(decode_result(rows, "pin"), result)
        with self.assertRaises(ValueError):
            decode_result(rows[:-1], "pin")
        with self.assertRaises(ValueError):
            decode_result(rows, "other")

    def test_post_unknown_records_intent_once(self):
        events = []

        def request(method, path, body=None):
            if method == "POST":
                self.assertEqual(events[-1][0], "launch_intent")
                raise TimeoutError()
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.endswith("jobs?limit=100"):
                return []
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {"ok": True},
        )
        with self.assertRaises(TimeoutError):
            transport.run(
                "pass", "pin", lambda name, value: events.append((name, value))
            )
        self.assertEqual(sum(name == "launch_intent" for name, _ in events), 1)

    def test_missing_terminal_result_leaves_unknown_and_logs_only_after_terminal(self):
        terminal = False
        calls = []

        def request(method, path, body=None):
            nonlocal terminal
            calls.append((method, path))
            if method == "POST":
                return dict(id="job", createdAt="now", status="pending")
            if path.endswith("deploys?limit=1"):
                return [{"deploy": {"status": "live", "commit": {"id": "commit"}}}]
            if path.endswith("jobs?limit=100"):
                return []
            if path.endswith("jobs/job"):
                terminal = True
                return dict(id="job", status="succeeded")
            if path.startswith("/logs?"):
                self.assertTrue(terminal)
                return dict(hasMore=False, logs=[])
            return {"autoDeploy": "no", "ownerId": "owner"}

        transport = RenderJob(
            dict(service_id="service", commit="commit"),
            request=request,
            guard=lambda: {},
        )
        with self.assertRaises(ValueError):
            transport.run("pass", "pin", Mock())
        self.assertEqual(sum(method == "POST" for method, _ in calls), 1)


if __name__ == "__main__":
    unittest.main()
