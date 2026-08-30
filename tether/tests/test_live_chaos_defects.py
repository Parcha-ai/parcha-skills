"""Defects found by running a live multi-agent conversation (chaos test).

Each of these was reproduced against the deployed system on 2026-08-30 while
three agents argued in one Slack thread. None was caught by any existing
test, because each needs a real gateway, a real Slack workspace, or both.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load(name: str, path: pathlib.Path):
    for stale in [k for k in sys.modules if k.startswith(name)]:
        sys.modules.pop(stale, None)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SlackErrorDetailTest(unittest.TestCase):
    """Logging only the exception type blindfolds every diagnosis.

    thread_not_found, not_in_channel, ratelimited and missing_scope all
    surface as a bare "SlackApiError" with completely different remedies.
    """

    @classmethod
    def setUpClass(cls):
        previous = list(sys.path)
        try:
            sys.path.insert(0, str(RUNTIME))
            cls.plugin = load("tether_plugin_chaos", RUNTIME / "plugin" / "__init__.py")
        finally:
            sys.path[:] = previous

    def detail(self, exc):
        return self.plugin._slack_error_detail(exc)

    def test_slack_error_code_is_surfaced(self):
        exc = RuntimeError("boom")
        exc.response = types.SimpleNamespace(data={"ok": False, "error": "thread_not_found"})
        self.assertEqual(self.detail(exc), ": thread_not_found")

    def test_missing_scope_reports_what_is_needed(self):
        exc = RuntimeError("boom")
        exc.data = {"ok": False, "error": "missing_scope", "needed": "channels:history"}
        self.assertEqual(
            self.detail(exc), ": missing_scope (needed channels:history)"
        )

    def test_plain_exception_falls_back_to_its_message(self):
        self.assertEqual(self.detail(ValueError("no token")), ": no token")

    def test_bare_exception_adds_nothing(self):
        class Bare(Exception):
            def __str__(self):
                return "Bare"

        self.assertEqual(self.detail(Bare()), "")


class PollFailureIsolationTest(unittest.TestCase):
    """One unreachable thread must not strand every bridge's queued turns.

    The poll loop drains queued work after polling; raising here skipped the
    drain entirely, so a single bad thread silently stopped recovery for
    every conversation on the machine.
    """

    def test_total_poll_failure_no_longer_raises(self):
        source = (RUNTIME / "plugin" / "__init__.py").read_text()
        self.assertNotIn('raise RuntimeError("every Slack thread poll failed")', source)
        self.assertIn("queue drain continues", source)


class ChannelMembershipVisibilityTest(unittest.TestCase):
    """A bot outside its channel never sees mentions and reports nothing.

    Slack drops the event before any agent logic runs, so the agent looks
    hung with no signal anywhere. Doctor must be able to say so.
    """

    def test_broker_status_reports_membership(self):
        source = (RUNTIME / "bridge_runtime.py").read_text()
        self.assertIn("default_channel_membership", source)
        self.assertIn('"conversations.info": "/api/conversations.info"', source)

    def test_doctor_renders_a_failure_for_a_non_member_bot(self):
        source = (ROOT / "bin" / "tether.js").read_text()
        self.assertIn('status.default_channel_membership === "not_member"', source)
        self.assertIn("mentions there are dropped", source)


if __name__ == "__main__":
    unittest.main()
