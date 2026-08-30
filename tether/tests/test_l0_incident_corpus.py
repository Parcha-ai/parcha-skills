"""Executable, sanitized reproducers for the production incident corpus.

These tests deliberately freeze baseline defects as observations.  They are not
the desired contract: ``evals/incident-corpus.json`` marks each observation as
``baseline_defect`` and names the L1 acceptance test that must invert it.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

from test_bridge import load_runtime


TEAM = "T12345678"
CHANNEL = "C12345678"
THREAD = "1785000000.000001"


class AwaitingAckIncidentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.bridge = self.store.bind(
            self.store.create(
                {
                    "source_kind": "headless_run",
                    "source": {"run_id": "incident-session", "cwd": "/tmp/project"},
                    "owner_user_id": "U12345678",
                    "team_id": TEAM,
                    "channel_id": CHANNEL,
                    "idempotency_key": "incident-binding",
                }
            ).bridge_id,
            THREAD,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def awaiting_attempt(self) -> str:
        event_id = "1785000001.000001"
        self.assertTrue(self.store.enqueue_event(event_id, self.bridge.bridge_id, "first"))
        batch = self.store.claim_event_batch(self.bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            self.bridge.bridge_id,
            [item["event_id"] for item in batch],
            self.bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_id],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            )
        )
        return attempt_id

    def test_generic_post_succeeds_but_does_not_close_active_attempt(self) -> None:
        """Baseline defect: generic egress bypasses attempt completion."""
        attempt_id = self.awaiting_attempt()
        broker = self.runtime.Broker(
            "test-token", self.store, verified_workspace_team_id=TEAM
        )
        with mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(
            self.runtime, "slack_post", return_value="1785000002.000001"
        ):
            result = broker.handle(
                {
                    "op": "thread_reply",
                    "team_id": TEAM,
                    "channel_id": CHANNEL,
                    "thread_ts": THREAD,
                    "text": "generic response",
                    "idempotency_key": "generic-response",
                }
            )

        self.assertEqual(result["message_ts"], "1785000002.000001")
        self.assertEqual(
            self.store.attempt_state(attempt_id, self.bridge.bridge_id),
            "awaiting_ack",
        )

    def test_health_and_unresolved_disagree_about_awaiting_ack_blocker(self) -> None:
        """Baseline defect: doctor sees a blocker its recovery API omits."""
        attempt_id = self.awaiting_attempt()
        self.assertTrue(
            self.store.enqueue_event(
                "1785000003.000001", self.bridge.bridge_id, "later follow-up"
            )
        )

        health = self.store.delivery_health()
        unresolved = self.store.unresolved_operations(TEAM)

        self.assertEqual(health["blocked_bridge_count"], 1)
        self.assertNotIn(
            attempt_id,
            [item["id"] for item in unresolved if item["kind"] == "attempt"],
        )

    def test_real_cli_and_broker_expose_the_same_operator_mismatch(self) -> None:
        """Black-box baseline: real CLI status fails while unresolved is empty."""
        self.awaiting_attempt()
        self.assertTrue(
            self.store.enqueue_event(
                "1785000004.000001", self.bridge.bridge_id, "queued behind blocker"
            )
        )
        socket_path = self.home / "bridge.sock"
        lock_fd = self.runtime._acquire_broker_lock(self.home / "bridges.db")
        server = self.runtime.start_broker(
            "synthetic-token",
            socket_path,
            store=self.store,
            lock_fd=lock_fd,
        )
        server.broker._workspace_team_id = TEAM
        cli = pathlib.Path(__file__).resolve().parents[1] / "bin/tether.js"
        env = {
            **os.environ,
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
            "XDG_DATA_HOME": str(self.home / ".local/share"),
            "TETHER_BROKER_SOCKET": str(socket_path),
        }
        try:
            status = subprocess.run(
                ["node", str(cli), "status", "--json"],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
                check=False,
            )
            unresolved = subprocess.run(
                ["node", str(cli), "unresolved", "--team", TEAM, "--json"],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=10,
                check=False,
            )
        finally:
            server.shutdown()
            server.server_close()
            socket_path.unlink(missing_ok=True)

        self.assertEqual(status.returncode, 1)
        self.assertEqual(json.loads(status.stdout)["blocked_bridge_count"], 1)
        self.assertEqual(unresolved.returncode, 0)
        self.assertEqual(json.loads(unresolved.stdout)["operations"], [])


if __name__ == "__main__":
    unittest.main()
