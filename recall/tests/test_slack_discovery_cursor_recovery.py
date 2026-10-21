"""Expired discovery positions must not replay completed channel prefixes forever."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from connectors.sdk import ConnectorRunner, ConnectorRunError
from connectors.slack_workspace import SlackWorkspaceConnector
from test_connector_sdk import FakeBrain
from test_slack_source_plugin import Rail, slack_response


class SlackDiscoveryCursorRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "spool.db"
        self.rail = Rail()
        self.rail.public_history = True
        self.brain = FakeBrain()
        self.runners = []
        self.addCleanup(lambda: [r.close() for r in self.runners])
        clock = patch("connectors.slack_workspace._now", return_value="2026-07-20T00:00:00Z")
        clock.start()
        self.addCleanup(clock.stop)

    def runner(self, channels=()):
        connector = SlackWorkspaceConnector(
            rail=self.rail, source_id="synthetic:slack:discovery", workspace_id="T111",
            channel_ids=channels,
        )
        runner = ConnectorRunner(connector=connector, brain=self.brain, spool_path=self.path)
        self.runners.append(runner)
        return runner

    def listing(self, channels, cursor=""):
        self.rail.add("channels.list", {"ok": True,
            "channels": [{"id": c, "is_archived": True} for c in channels],
            "response_metadata": {"next_cursor": cursor}})

    def completed_prefix(self, count=2):
        runner = self.runner()
        channels = [f"C{i:03d}" for i in range(count)]
        self.rail.add("users.list", {"ok": True, "members": []})
        self.listing(channels, "expired-discovery")
        self.rail.add("messages.history", *(slack_response([]) for _ in channels))
        for _ in range(count + 2):
            runner.run_once()
        self.assertEqual(json.loads(runner._cursor())["phase"], "discover")
        self.assertEqual(runner.doctor()["coverage"]["completed_discovery_cycles"], 0)
        return runner, channels

    def restart_listing(self, channels, cursor="fresh-next"):
        self.rail.add("channels.list", {"ok": False, "error": "invalid_cursor"})
        self.listing(channels, cursor)

    def test_expired_cursor_skips_completed_first50_and_reaches_later_channels(self):
        runner, channels = self.completed_prefix(50)
        before = json.loads(runner._cursor())
        self.restart_listing(channels)
        runner.run_once()
        after = json.loads(runner._cursor())
        self.assertEqual((after["cycle"], after["upper"], after["coverage"]),
                         (before["cycle"], before["upper"], before["coverage"]))
        self.assertEqual(after["channels"], [])
        self.assertEqual(after["discovery_page"], "fresh-next")
        self.assertEqual(runner.doctor()["coverage"]["completed_discovery_cycles"], 0)
        runner = self.runner()
        self.listing(["CLAST"])
        self.rail.add("messages.history", slack_response([]))
        runner.run_once()
        runner.run_once()
        coverage = runner.doctor()["coverage"]
        self.assertEqual(coverage["completed_discovery_cycles"], 1)
        self.assertEqual(coverage["last_complete_discovery_channels"], 51)
        self.assertEqual(coverage["history_baselined_channels"], 51)
        histories = [kw["query"]["channel"] for op, kw in self.rail.calls if op == "messages.history"]
        self.assertEqual(histories, channels + ["CLAST"])
        listings = [kw["query"] for op, kw in self.rail.calls if op == "channels.list"]
        self.assertEqual(listings[-3]["cursor"], "expired-discovery")
        self.assertNotIn("cursor", listings[-2])
        self.assertEqual(listings[-1]["cursor"], "fresh-next")
        self.assertFalse(any(op == "channels.join" for op, _ in self.rail.calls))

    def test_recovered_next_channel_waits_for_ack_then_restart_flushes(self):
        runner, channels = self.completed_prefix()
        self.restart_listing(channels + ["CLAST"], "")
        runner.run_once()
        before = runner._cursor()
        self.assertEqual(json.loads(before)["channels"], ["CLAST:1"])
        self.rail.add("messages.history", slack_response([
            {"ts": "1784332800.000100", "user": "U111", "text": "new channel"},
        ]))
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            runner.run_once()
        self.assertEqual(runner._cursor(), before)
        self.assertEqual(runner.doctor()["coverage"]["completed_discovery_cycles"], 0)
        runner = self.runner()
        calls = len(self.rail.calls)
        runner.run_once()
        self.assertEqual(len(self.rail.calls), calls)
        self.assertEqual(runner.doctor()["coverage"]["last_complete_discovery_channels"], 3)
        self.assertEqual(len(self.brain.events), 1)

    def test_only_exact_current_completed_rows_can_be_skipped(self):
        for change in ("scanned_cycle=-1", "last_seen_cycle=-1", "capture_version=0",
                       "history_through=NULL", "history_through='not-a-timestamp'",
                       "history_through='2026-07-19T00:00:00Z'", "authority='member'"):
            with self.subTest(change=change):
                # Each independently owned spool starts from a valid completed prefix.
                self.path = Path(self.tmp.name) / (str(len(self.runners)) + ".db")
                runner, channels = self.completed_prefix(1)
                runner.db.execute("UPDATE slack_channel_coverage SET " + change)
                runner.db.commit()
                self.restart_listing(channels)
                runner.run_once()
                self.assertEqual(json.loads(runner._cursor())["channels"], ["C000:1"])

    def test_changed_selection_cannot_reuse_old_completion_proof(self):
        runner, channels = self.completed_prefix(1)
        runner = self.runner(channels=("C000",))
        self.restart_listing(channels)
        runner.run_once()
        self.assertEqual(json.loads(runner._cursor())["channels"], ["C000:1"])

    def test_repeated_expiry_keeps_completed_prefix_then_provider_eof_finishes(self):
        runner, channels = self.completed_prefix()
        for continuation in ("fresh-next", "fresh-next-again", ""):
            self.restart_listing(channels, continuation)
            runner.run_once()
            coverage = runner.doctor()["coverage"]
            self.assertEqual(coverage["completed_discovery_cycles"], int(not continuation))
        self.assertEqual(coverage["last_complete_discovery_channels"], 2)
        self.assertEqual(sum(op == "messages.history" for op, _ in self.rail.calls), 2)

    def test_invalid_cursor_without_a_continuation_does_not_retry(self):
        runner = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        runner.run_once()
        before = runner._cursor()
        self.rail.add("channels.list", {"ok": False, "error": "invalid_cursor"})
        with self.assertRaises(ConnectorRunError):
            runner.run_once()
        self.assertEqual(runner._cursor(), before)
        self.assertEqual(sum(op == "channels.list" for op, _ in self.rail.calls), 1)

    def test_other_errors_and_failed_cursorless_retry_preserve_checkpoint(self):
        for errors in (("missing_scope",), ("invalid_cursor", "invalid_cursor")):
            with self.subTest(errors=errors):
                self.path = Path(self.tmp.name) / (str(len(self.runners)) + ".db")
                runner, _ = self.completed_prefix(1)
                before = runner._cursor()
                calls = len(self.rail.calls)
                self.rail.add("channels.list", *({"ok": False, "error": e} for e in errors))
                with self.assertRaises(ConnectorRunError):
                    runner.run_once()
                self.assertEqual(runner._cursor(), before)
                self.assertEqual(len(self.rail.calls) - calls, len(errors))
                self.assertEqual(runner.doctor()["coverage"]["completed_discovery_cycles"], 0)


if __name__ == "__main__":
    unittest.main()
