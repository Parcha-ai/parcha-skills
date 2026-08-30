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


class VanishedThreadTest(unittest.TestCase):
    """A reply into a thread Slack no longer has must fail, not fork.

    Slack does not reject an unknown thread_ts on chat.postMessage — it
    silently promotes the message to a new top-level root. The sender
    believes it replied in-thread; the operator sees stray messages in the
    channel. This is the "my updates landed as new messages instead of in
    the thread" failure reported in production on 2026-08-28.
    """

    def test_fork_detection_runs_after_delivery_without_a_preflight_call(self):
        source = (RUNTIME / "bridge_runtime.py").read_text()
        # Detected from the response we already have: no extra Slack round
        # trip on the happy path (a preflight probe cost one call per reply
        # and added a fresh failure mode to every delivery).
        self.assertIn("def _detect_thread_fork", source)
        self.assertIn('"thread fork detection"', source)
        self.assertNotIn("_require_live_thread", source)
        # It is best-effort post-delivery work, never a delivery blocker.
        detect_at = source.index('"thread fork detection"')
        complete_at = source.index("self.store.complete_message(")
        self.assertLess(complete_at, detect_at)

    def test_missing_thread_flips_the_binding_out_of_verified(self):
        source = (RUNTIME / "bridge_runtime.py").read_text()
        self.assertIn("def mark_bridge_thread_missing", source)
        self.assertIn("binding_state='rebind_required'", source)
        self.assertIn("binding_error_code='thread_not_found'", source)

    def test_only_a_definitively_missing_thread_marks_a_rebind(self):
        """A transient Slack failure must not condemn a healthy binding."""
        source = (RUNTIME / "bridge_runtime.py").read_text()
        guard = source[source.index("def _detect_thread_fork"):]
        guard = guard[: guard.index("def _deliver_staged_message")]
        self.assertIn('{"thread_not_found", "message_not_found"}', guard)
        # Any other error, and any unexpected exception, leaves it alone.
        self.assertIn("return", guard)
        self.assertIn("except Exception:", guard)


class SilentAuthorizationGateTest(unittest.TestCase):
    """Four independent gates can each silently drop an inbound mention.

    Reproduced live on 2026-08-30: making one agent answer another required
    clearing all four, and each rejection was invisible from the outside —
    the agent simply looked unresponsive.

      1. Slack channel membership     (Slack drops it; nothing logs)
      2. Hermes SLACK_TRUSTED_BOT_IDS (adapter "early reject", warn only)
      3. Tether router peer trust     (cancelled: untrusted_peer_bot)
      4. The installed plugin build   (old build ignores 2 and 3 entirely)

    Each is defensible alone. Together, with no single place that reports
    "your message was dropped and here is which gate did it", they make a
    silent agent indistinguishable from a broken one.
    """

    def test_router_rejection_carries_a_diagnosable_reason(self):
        source = (RUNTIME / "routing.py").read_text()
        # The reason string must name the gate, so an operator reading the
        # ingress ledger learns which allowlist to edit.
        self.assertIn('"untrusted_peer_bot"', source)
        self.assertIn("ambient_peer_bot_channels", source)

    def test_peer_trust_is_opt_in_and_documented_where_it_is_read(self):
        source = (RUNTIME / "plugin" / "__init__.py").read_text()
        block = source[source.index("def _allowed_peer_bot_users"):]
        block = block[: block.index("def _resolve_slack_adapter")]
        # Empty default is deliberate; the docstring must say so and say
        # what it costs, because the failure is otherwise invisible.
        self.assertIn("TETHER_ALLOWED_BOT_USERS", block)
        self.assertIn("invisible", block)
