"""Expired live positions must recover through the native ACK checkpoint."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from connectors.sdk import ConnectorContractError, ConnectorRunner, ConnectorRunError
from connectors.slack_source import SLACK_MESSAGE_CAPTURE_VERSION
from connectors.slack_workspace import SlackWorkspaceConnector, _cursor, _initial_state
from test_connector_sdk import FakeBrain
from test_slack_historical_reconciliation import HistoricalRail


LOWER = "2026-07-18T00:00:00.000001Z"
UPPER = "2026-07-18T00:00:00.000010Z"


class SlackLiveTimePaginationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "spool.db"
        self.rail = HistoricalRail()
        self.rail.page_size = 2
        self.brain = FakeBrain()
        self.runners = []
        self.addCleanup(lambda: [r.close() for r in self.runners])
        clock = patch("connectors.slack_workspace._now", return_value=UPPER)
        clock.start()
        self.addCleanup(clock.stop)

    def runner(self, *, reconciliation=False):
        connector = SlackWorkspaceConnector(
            rail=self.rail, source_id="synthetic:slack:live-time", workspace_id="T111",
            page_size=self.rail.page_size,
        )
        if reconciliation:
            connector.reconciliation_epoch = "a" * 64
        runner = ConnectorRunner(connector=connector, brain=self.brain, spool_path=self.path)
        self.runners.append(runner)
        return runner

    def seed(self, runner):
        state = {**_initial_state(public_history=True, upper=UPPER, cycle=7),
                 "phase": "history", "channels": ["C111:1"], "channel_lower": LOWER}
        runner._set_meta("committed_cursor", json.dumps(_cursor(state)))
        runner.db.execute("""INSERT INTO slack_channel_coverage
            (workspace_id,authority,channel_id,last_seen_cycle,history_through,scanned_cycle,capture_version)
            VALUES ('T111','public','C111',7,?,6,?)""", (LOWER, SLACK_MESSAGE_CAPTURE_VERSION))
        runner.db.commit()

    def messages(self, *, threads):
        rows = [{"ts": f"1784332800.{i:06d}", "user": "U111", "text": f"message {i}"}
                for i in range(12)]
        if threads:
            rows[2]["reply_count"] = 7
            for i in (3, 4, 5, 6, 7, 8, 10):
                rows[i]["thread_ts"] = rows[2]["ts"]
        self.rail.channel_messages = {"C111": rows}
        return rows

    def drain(self, runner, *, reconciliation=False, limit=80):
        for _ in range(limit):
            runner.run_once()
            through = runner.db.execute("SELECT history_through FROM slack_channel_coverage").fetchone()[0]
            if through == UPPER:
                return runner
            runner = self.runner(reconciliation=reconciliation)
        self.fail("fixed live window did not complete within bounded native turns")

    def test_expired_history_preserves_both_inclusive_endpoints_after_restart(self):
        self.messages(threads=False)
        runner = self.runner()
        self.seed(runner)
        self.rail.expire_cursors = True
        runner = self.drain(runner)
        self.assertEqual({e["native_id"].rsplit(":", 1)[-1] for e in self.brain.events.values()},
                         {f"1784332800.{i:06d}" for i in range(1, 11)})
        self.assertEqual(runner.doctor()["pending"], 0)
        self.assertEqual(sum(bool(kw.get("query", {}).get("cursor"))
                             for _, kw in self.rail.calls), 1)

    def test_expired_reply_and_long_thread_do_not_repeat_channel_forever(self):
        self.messages(threads=True)
        runner = self.runner()
        self.seed(runner)
        self.rail.expire_cursors = True
        runner = self.drain(runner)
        self.assertEqual({e["native_id"].rsplit(":", 1)[-1] for e in self.brain.events.values()},
                         {f"1784332800.{i:06d}" for i in range(1, 11)})
        calls = [kw["query"] for op, kw in self.rail.calls if op == "messages.history"]
        self.assertLessEqual(sum(q["latest"] == "1784332800.000010" for q in calls), 2)
        self.assertEqual(sum(bool(kw.get("query", {}).get("cursor"))
                             for _, kw in self.rail.calls), 1)

    def test_valid_provider_cursors_remain_functional(self):
        self.messages(threads=True)
        runner = self.runner()
        self.seed(runner)
        self.drain(runner)
        self.assertEqual(len(self.brain.events), 10)
        self.assertTrue(any(kw.get("query", {}).get("cursor") for _, kw in self.rail.calls))

    def test_lost_recovery_ack_keeps_legacy_position_then_flushes_without_provider(self):
        self.messages(threads=False)
        runner = self.runner(reconciliation=True)
        self.seed(runner)
        runner.run_once()
        before = runner._cursor()
        self.assertEqual(json.loads(runner.connector._reconciliation.state(before)["live_cursor"])["v"], 4)
        self.rail.expire_cursors = True
        # Replay contains already-ACKed records. Change one body so this
        # recovery page also requires an actual remote ACK rather than empty dedup.
        self.rail.channel_messages["C111"][9]["text"] = "edited during expired cursor"
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            runner.run_once()
        self.assertEqual(runner._cursor(), before)
        self.assertEqual(runner.db.execute("SELECT history_through FROM slack_channel_coverage").fetchone()[0], LOWER)
        calls = len(self.rail.calls)
        runner = self.runner(reconciliation=True)
        runner.run_once()
        self.assertEqual(len(self.rail.calls), calls)
        self.assertEqual(json.loads(runner.connector._reconciliation.state(runner._cursor())["live_cursor"])["v"], 5)
        self.assertEqual(runner.doctor()["coverage"]["committed_live_cursor"]["status"], "observed")
        self.drain(runner, reconciliation=True)
        self.assertEqual(len({e["native_id"] for e in self.brain.events.values()}), 10)

    def test_one_record_pages_preserve_microsecond_neighbors(self):
        self.rail.page_size = 1
        self.messages(threads=True)
        runner = self.runner()
        self.seed(runner)
        self.rail.expire_cursors = True
        self.drain(runner)
        self.assertEqual({e["native_id"].rsplit(":", 1)[-1] for e in self.brain.events.values()},
                         {f"1784332800.{i:06d}" for i in range(1, 11)})

    def test_recovered_live_pages_allow_another_historical_turn(self):
        self.rail.page_size = 1
        self.messages(threads=True)
        runner = self.runner(reconciliation=True)
        self.seed(runner)
        self.rail.expire_cursors = True
        runner = self.drain(runner, reconciliation=True)
        state = json.loads(runner.db.execute("SELECT state_json FROM slack_reconciliation_state").fetchone()[0])
        self.assertGreaterEqual(state["historical_pages"], 1)
        self.assertEqual(state["historical_failures"], 0)
        self.assertEqual(runner.doctor()["coverage"]["committed_live_cursor"]["status"], "observed")

    def test_live_timestamp_proof_preserves_strict_historical_default(self):
        from connectors.slack_reconciliation import live_time_boundary, live_time_page, time_page
        query = {"oldest": "1784332800.000001", "latest": "1784332800.000010"}
        terminal = {"has_more": False}
        endpoints = [{"ts": query["oldest"]}, {"ts": query["latest"]}]
        self.assertIsNone(live_time_page(terminal, endpoints, query, descending=True))
        with self.assertRaises(ConnectorContractError):
            time_page(terminal, endpoints, query, descending=True)
        self.assertEqual(live_time_boundary("9999999999999999.999998", descending=False),
                         "9999999999999999.999999")
        self.assertEqual(live_time_boundary("1784332800", descending=True), "1784332799.999999")
        self.assertEqual(live_time_boundary("1784332800.1", descending=False), "1784332800.100001")
        for malformed in ("1.0000001", "NaN", "1e6", "-1", 123):
            with self.subTest(malformed=malformed), self.assertRaises(ConnectorContractError):
                live_time_boundary(malformed, descending=False)

    def test_repeated_boundary_cannot_claim_progress_or_hide_continuation(self):
        from connectors.slack_reconciliation import live_time_page
        query = {"oldest": "1784332800.000005", "latest": "1784332800.000010",
                 "ts": "1784332800.000002"}
        terminal = {"has_more": False, "response_metadata": {"next_cursor": ""}}
        root = {"ts": query["ts"]}
        self.assertIsNone(live_time_page(terminal, [root], query, descending=False))
        for response, messages in (
            ({"has_more": True}, [root]),
            ({"has_more": False, "response_metadata": {"next_cursor": "opaque"}}, [root]),
            (terminal, [{"ts": "1784332800.000004"}]),
            (terminal, [{"ts": "1784332800.000011"}]),
            ({"has_more": True}, []),
            ({"has_more": True}, [{"ts": query["latest"]}]),
        ):
            with self.subTest(response=response, messages=messages), self.assertRaises(ConnectorContractError):
                live_time_page(response, messages, query, descending=False)
        # Repeated identical in-page records share one native ID; their boundary
        # remains exact and excludes that ID on the following request.
        self.assertEqual(live_time_page({"has_more": True},
                         [{"ts": "1784332800.000006"}] * 2, query, descending=False),
                         "1784332800.000006")

    def test_only_expired_opaque_continuations_enter_recovery(self):
        for error, first in (("invalid_cursor", True), ("missing_scope", False)):
            with self.subTest(error=error):
                self.path = Path(self.tmp.name) / (error + ".db")
                self.messages(threads=False)
                runner = self.runner()
                self.seed(runner)
                if not first:
                    runner.run_once()
                before = runner._cursor()
                original = self.rail.request
                calls = []
                def refuse(operation, **kwargs):
                    calls.append(operation)
                    return {"ok": False, "error": error}
                self.rail.request = refuse
                try:
                    with self.assertRaises(ConnectorRunError):
                        runner.run_once()
                finally:
                    self.rail.request = original
                self.assertEqual(calls, ["messages.history"])
                self.assertEqual(runner._cursor(), before)

    def test_legacy_and_new_positions_cannot_be_silently_confused(self):
        from connectors.slack_workspace import _state
        state = {**_initial_state(public_history=True, upper=UPPER), "v": 5,
                 "phase": "history", "channels": ["C111:1"], "page": "opaque-provider-position"}
        with self.assertRaises(ConnectorContractError):
            _state(_cursor(state), (), True)
        state["v"] = 4
        self.assertEqual(_state(_cursor(state), (), True)["page"], "opaque-provider-position")

    def test_authority_epoch_change_discards_recovered_positions(self):
        self.messages(threads=False)
        runner = self.runner(reconciliation=True)
        self.seed(runner)
        runner.run_once()
        self.rail.expire_cursors = True
        runner.run_once()
        saved = runner.connector._reconciliation.state(runner._cursor())
        self.assertEqual(json.loads(saved["live_cursor"])["v"], 5)
        runner.connector._reconciliation.epoch = "b" * 64
        fresh = runner.connector._reconciliation.state(runner._cursor())
        self.assertEqual(json.loads(fresh["live_cursor"])["phase"], "users")
        self.assertEqual(json.loads(fresh["live_cursor"])["cycle"], 8)
        self.assertEqual(fresh["live_pages"], 0)
        self.assertEqual(runner.db.execute("SELECT history_through FROM slack_channel_coverage").fetchone()[0], LOWER)
