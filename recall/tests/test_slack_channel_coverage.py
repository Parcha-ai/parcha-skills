from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock
from uuid import UUID

from connectors.sdk import ConnectorContractError, ConnectorPage, ConnectorRunner, ConnectorRunError
from connectors.slack_source import SLACK_MESSAGE_CAPTURE_VERSION, normalize_slack_message
from connectors.slack_workspace import SlackWorkspaceConnector
from test_connector_sdk import FakeBrain
from test_slack_source_plugin import Rail, slack_response
from server.recall_server import managed_worker


class SlackChannelCoverageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "spool.db"
        self.brain = FakeBrain()
        self.rail = Rail()
        self.rail.public_history = True
        self.runners = []
        self.addCleanup(lambda: [r.close() for r in self.runners])

    def runner(self, *, channels=()):
        runner = ConnectorRunner(
            connector=SlackWorkspaceConnector(
                rail=self.rail, source_id="synthetic:slack:coverage", workspace_id="T123",
                channel_ids=channels,
            ), brain=self.brain, spool_path=self.path,
        )
        self.runners.append(runner)
        return runner

    def discovery(self, channels, *, cursor=""):
        self.rail.add("channels.list", {
            "ok": True, "channels": [{"id": c} for c in channels],
            "response_metadata": {"next_cursor": cursor},
        })
        self.rail.add("channels.join", *({"ok": True} for _ in channels))

    def cycle(self, runner, channels):
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(channels)
        self.rail.add("messages.history", *(slack_response([]) for _ in channels))
        for _ in range(len(channels) + 2):
            result = runner.run_once()
        self.assertFalse(result["has_more"])

    def histories(self):
        return [kw["query"] for op, kw in self.rail.calls if op == "messages.history"]

    def test_new_channel_gets_epoch_existing_channel_retains_own_watermark_after_restart(self):
        r = self.runner()
        with patch("connectors.slack_workspace._now", return_value="2026-09-23T00:00:00Z"):
            self.cycle(r, ["COLD"])
        r = self.runner()
        self.cycle(r, ["CNEW", "COLD"])
        newest = {q["channel"]: q for q in self.histories()[-2:]}
        self.assertNotIn("oldest", newest["CNEW"])
        self.assertEqual(newest["COLD"]["oldest"], "1790121600.000000")
        coverage = r.doctor()["coverage"]
        self.assertEqual(coverage["known_channels"], 2)
        self.assertEqual(coverage["history_baselined_channels"], 2)
        self.assertEqual(coverage["completed_discovery_cycles"], 2)
        self.assertFalse(coverage["historical_mutations_verified"])
        self.assertNotIn("CNEW", json.dumps(coverage))

    def test_restored_channel_rebaselines_after_absent_discovery_cycle(self):
        r = self.runner()
        self.cycle(r, ["CALWAYS", "CRETURN"])
        self.cycle(r, ["CALWAYS"])
        self.cycle(r, ["CALWAYS", "CRETURN"])
        self.assertNotIn("oldest", self.histories()[-1])
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 2)

    def test_empty_discovery_is_recorded_and_returning_channel_rebaselines(self):
        r = self.runner()
        self.cycle(r, ["CRETURN"])
        self.cycle(r, [])
        self.assertEqual(r.doctor()["coverage"]["last_complete_discovery_channels"], 0)
        self.cycle(r, ["CRETURN"])
        self.assertNotIn("oldest", self.histories()[-1])

    def test_inventory_survives_multiple_discovery_batches_and_restart(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"], cursor="opaque-next")
        self.discovery(["CTWO"])
        self.rail.add("messages.history", slack_response([]), slack_response([]))
        for _ in range(3):
            r.run_once()
        r = self.runner()
        self.assertEqual(r.doctor()["coverage"]["known_channels"], 1)
        self.assertEqual(r.doctor()["coverage"]["completed_discovery_cycles"], 0)
        r.run_once()
        r.run_once()
        self.assertEqual(r.doctor()["coverage"]["known_channels"], 2)
        self.assertEqual(r.doctor()["coverage"]["completed_discovery_cycles"], 1)

    def test_provider_reports_more_without_cursor_cannot_certify_complete_history(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"])
        self.rail.add("messages.history", {"ok": True, "messages": [], "has_more": True})
        r.run_once()
        r.run_once()
        before = r._cursor()
        with self.assertRaises(ConnectorContractError):
            r.run_once()
        self.assertEqual(r._cursor(), before)
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)

    def test_lost_ack_does_not_certify_channel_then_restart_flush_commits_atomically(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"])
        self.rail.add("messages.history", slack_response([{
            "ts": "1784332800.000100", "user": "U111", "text": "ACK boundary",
        }]))
        r.run_once()
        r.run_once()
        before = r._cursor()
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            r.run_once()
        self.assertEqual(r._cursor(), before)
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        r = self.runner()
        calls = len(self.rail.calls)
        r.run_once()
        self.assertEqual(len(self.rail.calls), calls)
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)
        self.assertEqual(r.doctor()["pending_pages"], 0)

    def test_legacy_inflight_page_resumes_without_falsely_certifying_earlier_history(self):
        r = self.runner()
        old = {"v": 3, "coverage": "public", "phase": "history", "page": "opaque-history",
               "discovery_page": None, "channels": ["COLD:0"], "configured_index": 0,
               "channel_index": 0, "threads": [], "thread_index": 0, "thread_page": None,
               "watermark": "2026-09-22T00:00:00Z", "upper": "2026-09-23T00:00:00Z",
               "cycle": 7, "found": True}
        r._set_meta("committed_cursor", json.dumps(json.dumps(old)))
        r.db.commit()
        self.rail.add("messages.history", slack_response([]))
        r.run_once()
        self.assertEqual(self.histories()[0]["cursor"], "opaque-history")
        self.assertEqual(json.loads(r._cursor())["cycle"], 8)
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        self.cycle(r, ["COLD"])
        self.assertNotIn("oldest", self.histories()[-1])
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)

    def test_checkpoint_failure_rolls_back_coverage_cursor_and_pending_page_ack(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"])
        self.rail.add("messages.history", slack_response([{
            "ts": "1784332800.000100", "user": "U111", "text": "Atomic checkpoint",
        }]))
        r.run_once()
        r.run_once()
        before = r._cursor()
        commit = r.connector.commit_checkpoint

        def interrupted(previous, current):
            commit(previous, current)
            raise RuntimeError("interrupt after ledger update")

        with patch.object(r.connector, "commit_checkpoint", side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, "interrupt"):
                r.run_once()
        self.assertEqual(r._cursor(), before)
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        self.assertEqual(r.doctor()["pending_pages"], 1)
        r = self.runner()
        r.run_once()
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)
        self.assertEqual(r.doctor()["pending_pages"], 0)

    def test_legacy_pending_first_history_page_cannot_certify_global_watermark_as_epoch(self):
        r = self.runner()
        before = {"v": 3, "coverage": "public", "phase": "history", "page": None,
                  "discovery_page": None, "channels": ["COLD:0"], "configured_index": 0,
                  "channel_index": 0, "threads": [], "thread_index": 0, "thread_page": None,
                  "watermark": "2026-09-22T00:00:00Z", "upper": "2026-09-23T00:00:00Z",
                  "cycle": 7, "found": True}
        after = {**before, "phase": "users", "channels": [], "cycle": 8, "found": False,
                 "watermark": before["upper"], "upper": "2026-09-24T00:00:00Z"}
        cursor = json.dumps(before)
        r._set_meta("committed_cursor", json.dumps(cursor))
        r.db.commit()
        record = normalize_slack_message(workspace_id="T123", channel_id="COLD", value={
            "ts": "1790100000.000100", "user": "U111", "text": "Old pending page",
        })
        r._stage(ConnectorPage(records=(record,), next_cursor=json.dumps(after), has_more=False), cursor)
        r = self.runner()
        r.run_once()
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        self.assertEqual(r.doctor()["pending_pages"], 0)

    def test_partial_legacy_epoch_scan_replays_under_current_capture_version(self):
        for phase, page in (("history", "old-page"), ("threads", None)):
            with self.subTest(phase=phase):
                self.path = self.path.with_name(f"legacy-{phase}.db")
                r = self.runner()
                before = {"v": 3, "coverage": "public", "phase": phase, "page": page,
                          "discovery_page": None, "channels": ["COLD:0"], "configured_index": 0,
                          "channel_index": 0, "threads": ["1784332800.000100"] if phase == "threads" else [],
                          "thread_index": 0, "thread_page": None,
                          "watermark": "1970-01-01T00:00:00Z", "upper": "2026-09-23T00:00:00Z",
                          "cycle": 0, "found": True}
                r._set_meta("committed_cursor", json.dumps(json.dumps(before)))
                r.db.commit()
                self.rail.add("messages.replies" if phase == "threads" else "messages.history", slack_response([]))
                r.run_once()
                self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
                self.cycle(r, ["COLD"])
                self.assertNotIn("oldest", self.histories()[-1])
                self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)

    def test_new_capture_version_invalidates_old_baseline_without_resetting_spool(self):
        r = self.runner()
        self.cycle(r, ["CONE"])
        with patch("connectors.slack_workspace.SLACK_MESSAGE_CAPTURE_VERSION", SLACK_MESSAGE_CAPTURE_VERSION + 1):
            r = self.runner()
            self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
            self.assertEqual(r.doctor()["coverage"]["history_baseline_pending_channels"], 1)
            self.assertEqual(r.doctor()["coverage"]["capture_version"], SLACK_MESSAGE_CAPTURE_VERSION + 1)
            self.cycle(r, ["CONE"])
            self.assertNotIn("oldest", self.histories()[-1])
            self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)

    def test_pending_old_capture_first_page_acks_before_full_replay(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"])
        self.rail.add("messages.history", slack_response([{
            "ts": "1784332800.000100", "user": "U111", "text": "Old capture",
        }]))
        r.run_once()
        r.run_once()
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            r.run_once()
        calls = len(self.rail.calls)
        with patch("connectors.slack_workspace.SLACK_MESSAGE_CAPTURE_VERSION", SLACK_MESSAGE_CAPTURE_VERSION + 1):
            r = self.runner()
            r.run_once()
            self.assertEqual(len(self.rail.calls), calls)
            self.assertEqual(r.doctor()["pending_pages"], 0)
            self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
            self.cycle(r, ["CONE"])
            self.assertNotIn("oldest", self.histories()[-1])
            self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)

    def test_pending_v3_first_middle_last_pages_ack_then_recover_same_native_message(self):
        for position, prior_page, next_page in (("first", None, "middle"),
                                                ("middle", "middle", "last"),
                                                ("last", "last", None)):
            with self.subTest(position=position):
                self.path = self.path.with_name(f"pending-v3-{position}.db")
                self.brain = FakeBrain()
                r = self.runner()
                before = {"v": 3, "coverage": "public", "phase": "history", "page": prior_page,
                          "discovery_page": None, "channels": ["COLD:0"], "configured_index": 0,
                          "channel_index": 0, "threads": [], "thread_index": 0, "thread_page": None,
                          "watermark": "1970-01-01T00:00:00Z", "upper": "2026-09-23T00:00:00Z",
                          "cycle": 0, "found": True}
                after = {**before, "page": next_page}
                if next_page is None:
                    after.update(phase="users", channels=[], cycle=1, found=False,
                                 watermark=before["upper"], upper="2026-09-24T00:00:00Z")
                raw = {"ts": "1784332800.000100", "user": "U111", "text": ""}
                record = normalize_slack_message(workspace_id="T123", channel_id="COLD", value=raw)
                cursor = json.dumps(before)
                r._set_meta("committed_cursor", json.dumps(cursor))
                r.db.commit()
                r._stage(ConnectorPage(records=(record,), next_cursor=json.dumps(after),
                                       has_more=next_page is not None), cursor)
                calls = len(self.rail.calls)
                r = self.runner()
                r.run_once()
                self.assertEqual(len(self.rail.calls), calls)
                if next_page is not None:
                    self.rail.add("messages.history", slack_response([]))
                    r.run_once()
                self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
                self.rail.add("users.list", {"ok": True, "members": []})
                self.discovery(["COLD"])
                self.rail.add("messages.history", slack_response([{**raw, "text": "Recovered visible content"}]))
                for _ in range(3):
                    r.run_once()
                self.assertNotIn("oldest", self.histories()[-1])
                self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)
                self.assertEqual(len(self.brain.events), 2)
                self.assertEqual(len({key[1] for key in self.brain.events}), 1)

    def test_thread_replies_must_be_acked_before_channel_history_is_baselined(self):
        r = self.runner()
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"])
        root = {"ts": "1784332800.000100", "user": "U111", "text": "Root", "reply_count": 1}
        reply = {"ts": "1784332801.000100", "thread_ts": root["ts"], "user": "U111", "text": "Reply"}
        self.rail.add("messages.history", slack_response([root]))
        self.rail.add("messages.replies", slack_response([root], cursor="thread-next"), slack_response([reply]))
        for _ in range(4):
            r.run_once()
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        self.brain.fail_after_commit = True
        with self.assertRaises(ConnectorRunError):
            r.run_once()
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 0)
        r = self.runner()
        r.run_once()
        self.assertEqual(r.doctor()["coverage"]["history_baselined_channels"], 1)
        self.assertEqual(len(self.brain.events), 2)

    def test_incremental_history_does_not_claim_historical_mutation_reconciliation(self):
        r = self.runner()
        self.cycle(r, ["CONE"])
        # A reply/edit/delete to an older root need not appear in this newer
        # history window. No Events subscription is assumed by coverage.
        self.cycle(r, ["CONE"])
        self.assertIn("oldest", self.histories()[-1])
        self.assertFalse(any(op == "messages.replies" for op, _ in self.rail.calls))
        self.assertFalse(r.doctor()["coverage"]["historical_mutations_verified"])

    def test_completed_inventory_snapshot_does_not_shrink_during_next_discovery(self):
        r = self.runner()
        self.cycle(r, ["CONE", "CTWO"])
        self.rail.add("users.list", {"ok": True, "members": []})
        self.discovery(["CONE"], cursor="next")
        r.run_once()
        r.run_once()
        status = r.doctor()["coverage"]
        self.assertEqual(status["last_complete_discovery_channels"], 2)
        self.assertEqual(status["latest_discovery_channels"], 1)
        self.assertEqual(status["latest_discovery_scanned_channels"], 0)
        self.assertTrue(status["discovery_in_progress"])

    def test_configured_scope_change_does_not_claim_previous_inventory_as_current(self):
        def configured_cycle(runner, count):
            self.rail.add("users.list", {"ok": True, "members": []})
            self.rail.add("messages.history", *(slack_response([]) for _ in range(count)))
            for _ in range(count + 1):
                runner.run_once()

        r = self.runner(channels=("CONE", "CTWO"))
        configured_cycle(r, 2)
        r = self.runner(channels=("CONE",))
        status = r.doctor()["coverage"]
        self.assertIsNone(status["last_complete_discovery_channels"])
        self.assertFalse(status["discovery_matches_current_selection"])
        self.assertEqual(status["known_channels"], 1)
        configured_cycle(r, 1)
        self.assertEqual(r.doctor()["coverage"]["last_complete_discovery_channels"], 1)
        r = self.runner(channels=("CONE", "CTHREE"))
        self.assertIsNone(r.doctor()["coverage"]["last_complete_discovery_channels"])
        configured_cycle(r, 2)
        self.assertNotIn("oldest", self.histories()[-1])
        status = r.doctor()["coverage"]
        self.assertTrue(status["discovery_matches_current_selection"])
        self.assertEqual(status["known_channels"], 2)
        self.assertEqual(status["history_baselined_channels"], 2)

    def test_managed_worker_real_runner_uuid_and_failed_diagnostic_preserve_ack_success(self):
        for diagnostic_fails in (False, True):
            with self.subTest(diagnostic_fails=diagnostic_fails):
                worker = object.__new__(managed_worker.ManagedConnectorWorker)
                row = {"id": UUID("12345678-1234-5678-1234-567812345678"),
                       "tenant_id": "tenant:test", "principal_id": "owner",
                       "source_id": "synthetic:slack:coverage", "privacy_mode": "off"}
                worker._claim = Mock(return_value=row)
                worker._credentials = Mock(return_value={})
                worker._finish = Mock()
                worker.authority_root = Path(self.tmp.name)
                worker.plane = Mock()
                worker.store = Mock()
                worker.archive = Mock()
                worker.retrieval = Mock()
                worker.retrieval.embed_pending.return_value = {"processed": 0}
                worker.embedding_max_batches = 1
                worker.interval_seconds = 60
                self.rail.add("users.list", {"ok": True, "members": []})
                worker.connector_factory = lambda *_: (
                    SlackWorkspaceConnector(rail=self.rail, source_id=row["source_id"], workspace_id="T123"),
                    self.path.with_name(f"worker-{diagnostic_fails}.db"),
                )
                with patch.object(managed_worker, "CanonicalArchiveGateway", return_value=Mock()):
                    if diagnostic_fails:
                        with patch.object(ConnectorRunner, "doctor", side_effect=RuntimeError("private")):
                            with self.assertLogs(managed_worker.LOG, level="WARNING") as logs:
                                result = worker.run_once()
                        self.assertNotIn("private", str(logs.output))
                        self.assertEqual(result["coverage_error_code"], "coverage_unavailable")
                    else:
                        result = worker.run_once()
                        self.assertIn("known_channels", result["coverage"])
                        self.assertEqual(len(result["installation_sha256"]), 64)
                self.assertEqual(result["status"], "committed")
                worker._finish.assert_called_once_with(row["id"], success=True, retry_after_seconds=1)


if __name__ == "__main__":
    unittest.main()
