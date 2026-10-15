"""Slack polling must preserve edits that restore earlier message text."""

from pathlib import Path
import tempfile
import unittest

from connectors.sdk import ConnectorPage, ConnectorRunner
from connectors.slack_source import normalize_slack_message
from test_connector_sdk import FakeBrain, SyntheticConnector


class SlackEditRevisions(unittest.TestCase):
    def test_history_and_reply_edits_match_events_under_original_identity(self):
        for thread_ts in (None, "1784332799.000100"):
            with self.subTest(thread_ts=thread_ts):
                message = {
                    "ts": "1784332800.000100", "user": "U111", "text": "Edited decision",
                    "edited": {"ts": "1784332802.000100", "user": "U111"},
                }
                if thread_ts:
                    message["thread_ts"] = thread_ts
                api = normalize_slack_message(
                    workspace_id="T123", channel_id="C123", value=message,
                )
                event = normalize_slack_message(
                    workspace_id="T123", channel_id="C123",
                    value={"subtype": "message_changed", "message": message},
                    provenance_surface="events",
                )
                self.assertEqual(api.content, event.content)
                self.assertEqual(api.content["edited_at"], "2026-07-18T00:00:02.000100Z")
                self.assertEqual(api.native_id, "slack:T123:C123:1784332800.000100")
                self.assertEqual(api.native_id, event.native_id)
                self.assertEqual(api.native_parent_id, event.native_parent_id)
                self.assertEqual(api.occurred_at, "2026-07-18T00:00:00.000100Z")

    def test_polling_restores_earlier_text_after_an_edit_and_restart(self):
        def record(text, edited=None):
            message = {
                "ts": "1784332800.000100", "user": "U111", "text": text,
                "thread_ts": "1784332799.000100",
            }
            if edited:
                message["edited"] = {"ts": edited, "user": "U111"}
            return normalize_slack_message(
                workspace_id="T123", channel_id="C123", value=message,
            )

        original = record("Original decision")
        changed = record("Revised decision", "1784332801.000100")
        restored = record("Original decision", "1784332802.000100")
        connector = SyntheticConnector({
            None: ConnectorPage((original,), "original", True),
            "original": ConnectorPage((changed,), "changed", True),
            "changed": ConnectorPage((restored,), "restored", True),
            "restored": ConnectorPage((restored,), "replayed", False),
        })
        brain = FakeBrain()
        with tempfile.TemporaryDirectory() as directory:
            spool = Path(directory) / "spool.db"
            runner = ConnectorRunner(connector=connector, brain=brain, spool_path=spool)
            try:
                runner.run_once()
                runner.run_once()
            finally:
                runner.close()
            runner = ConnectorRunner(connector=connector, brain=brain, spool_path=spool)
            try:
                runner.run_once()
                runner.run_once()
            finally:
                runner.close()

        events = list(brain.events.values())
        self.assertEqual([event["content"]["text"] for event in events], [
            "Original decision", "Revised decision", "Original decision",
        ])
        self.assertEqual(brain.calls, 3, "Replaying the same edit remains idempotent")
        self.assertEqual({event["native_id"] for event in events}, {original.native_id})
        self.assertEqual(events[-1]["content"]["edited_at"], "2026-07-18T00:00:02.000100Z")


if __name__ == "__main__":
    unittest.main()
