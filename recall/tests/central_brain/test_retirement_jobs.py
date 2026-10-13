# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Remote launch intent is durable before one POST; uncertain jobs never retry."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.retirement_jobs import RenderJob, encode_result, decode_result


class JobTests(unittest.TestCase):
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
