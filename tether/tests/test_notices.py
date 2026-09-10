"""Gateway housekeeping notices are dropped at the Slack adapter; real messages pass."""

from __future__ import annotations

import asyncio
import sys
import types
import unittest
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.plugin_next import notices  # noqa: E402


class NoticeMatchTests(unittest.TestCase):
    def test_hermes_notices_match_whole_message_only(self):
        for text in [
            ":hourglass_flowing_sand: Gateway is shutting down and is not accepting another turn right now.",
            "⏳ Gateway restarting — queued for the next turn after it comes back.",
            ":warning: Gateway shutting down — Your current task will be interrupted.",
            "⚠️ Gateway restarting — Your current task will be interrupted. Send any message after restart and I'll try to resume where you left off.",
            ":zap: Interrupting current task (iteration 3/60). I'll respond to your message shortly.\n\n:bulb: First-time tip: ...",
            ":arrow_right_hook: Redirected current run (9 min elapsed, iteration 3/60). I'll adjust using your correction.",
            ":information_source: Context compression deferred — summary still streaming. Continuing without compression this turn.",
            "Operation interrupted: retrying API call after error (retry 1/3).",
            "[System: Empty message content sanitised to satisfy protocol]",
        ]:
            self.assertTrue(notices.is_gateway_notice(text), text)
        for text in [
            "<@U1> the gateway is shutting down at 5pm, heads up",
            "Redirected current run is what Hermes says; here is my fragment: {}",
            "NO_REPLY",
            "",
        ]:
            self.assertFalse(notices.is_gateway_notice(text), text)


class InstallTests(unittest.TestCase):
    def test_install_wraps_send_once_and_drops_only_notices(self):
        sent = []

        @dataclass
        class SendResult:
            success: bool
            message_id: str | None = None

        class SlackAdapter:
            async def send(self, chat_id, content, reply_to=None, metadata=None):
                sent.append((chat_id, content, reply_to))
                return SendResult(True, "1.1")

        module = types.SimpleNamespace(SlackAdapter=SlackAdapter, SendResult=SendResult)
        modules = {"hermes_plugins.slack_platform.adapter": module, "other.thing": types.SimpleNamespace()}
        self.assertEqual(notices.install(modules), 1)
        self.assertEqual(notices.install(modules), 0, "idempotent")
        adapter = SlackAdapter()
        dropped = asyncio.run(adapter.send("C1", "⏳ Gateway is shutting down and is not accepting another turn right now."))
        self.assertTrue(dropped.success)
        self.assertIsNone(dropped.message_id)
        real = asyncio.run(adapter.send("C1", "<@U1> done, PR #12", reply_to="100.1"))
        self.assertEqual(real.message_id, "1.1")
        self.assertEqual(sent, [("C1", "<@U1> done, PR #12", "100.1")])


if __name__ == "__main__":
    unittest.main()
