from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import unittest

from test_bridge_lifecycle import load_runtime


TEAM = "T12345678"
CHANNEL = "C12345678"


def process_identity() -> str:
    payload = {
        "agent": "codex",
        "boot": "00000000-0000-4000-8000-000000000001",
        "exe": "1:2",
        "exe_path": hashlib.sha256(b"/opt/codex").hexdigest()[:16],
        "pane": "7",
        "pid": 207,
        "session": "work",
        "start": "20007",
        "tty": "34823",
    }
    return "linux-proc-v2:" + json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )


class OperatorRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        identity = process_identity()
        bridge = self.store.create(
            {
                "source_kind": "codex_session",
                "source": {
                    "session_id": "codex-operator-recovery",
                    "cwd": "/tmp/project",
                    "zellij_session": "work",
                    "zellij_pane_id": "7",
                    "pane_agent": "codex",
                    "pane_command_hash": hashlib.sha256(
                        identity.encode()
                    ).hexdigest(),
                    "process_identity": identity,
                },
                "owner_user_id": "U12345678",
                "team_id": TEAM,
                "channel_id": CHANNEL,
                "idempotency_key": "operator-recovery",
            }
        )
        self.bridge = self.store.bind(bridge.bridge_id, "1785000000.000001")

    def tearDown(self):
        self.temp.cleanup()

    def uncertain_hermes_ingress(self, suffix: str = "1") -> str:
        event_id = f"slack:{TEAM}:{CHANNEL}:1785000001.00000{suffix}"
        claim = self.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=self.bridge.bridge_id,
            binding_generation=self.bridge.binding_generation,
            payload={
                "text": "continue",
                "message_ts": f"1785000001.00000{suffix}",
            },
        )
        self.assertTrue(
            self.store.mark_thread_ingress_dispatched(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        )
        self.assertTrue(
            self.store.mark_thread_ingress_uncertain(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
                error_code="synthetic_crash",
            )
        )
        return event_id

    def uncertain_attempt(self) -> str:
        event_id = f"slack:{TEAM}:{CHANNEL}:1785000002.000001"
        claim = self.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            route_action="native",
            writer_id="native",
            bridge_id=self.bridge.bridge_id,
            binding_generation=self.bridge.binding_generation,
            payload={"text": "run it"},
        )
        self.assertTrue(
            self.store.transfer_thread_ingress(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                "run it",
            )
        )
        batch = self.store.claim_event_batch(self.bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            self.bridge.bridge_id,
            [event_id],
            self.bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [item["event_id"] for item in batch],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_uncertain(
                attempt_id,
                self.bridge.bridge_id,
            )
        )
        return attempt_id

    def awaiting_ack_attempt(self) -> str:
        event_id = f"slack:{TEAM}:{CHANNEL}:1785000002.000002"
        claim = self.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            route_action="native",
            writer_id="native",
            bridge_id=self.bridge.bridge_id,
            binding_generation=self.bridge.binding_generation,
            payload={"text": "run it"},
        )
        self.assertTrue(
            self.store.transfer_thread_ingress(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                "run it",
            )
        )
        batch = self.store.claim_event_batch(self.bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            self.bridge.bridge_id,
            [event_id],
            self.bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [item["event_id"] for item in batch],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
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

    def failed_message_reconciliation(self) -> tuple[str, str]:
        idempotency_key = "operator-recovery-message"
        reserved = self.store.reserve_message(
            idempotency_key,
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            "operator recovery",
        )
        client_msg_id = reserved["client_msg_id"]
        reconciliation_key = self.store.reconciliation_key(
            "message",
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            client_msg_id,
        )
        self.store.ensure_reconciliation(
            reconciliation_key=reconciliation_key,
            team_id=TEAM,
            method="conversations.replies",
            channel_id=CHANNEL,
            thread_ts=self.bridge.thread_ts,
            target_kind="message",
            target_id=client_msg_id,
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliations
                SET state='failed',error='slack_reconciliation_page_limit'
                WHERE reconciliation_key=?
                """,
                (reconciliation_key,),
            )
            database.execute(
                """
                UPDATE slack_messages SET state='uncertain'
                WHERE idempotency_key=?
                """,
                (idempotency_key,),
            )
        return reconciliation_key, idempotency_key

    def test_uncertain_hermes_ingress_requires_explicit_retry(self):
        event_id = self.uncertain_hermes_ingress()
        unresolved = self.store.unresolved_operations(TEAM)
        self.assertEqual(
            [(item["kind"], item["id"]) for item in unresolved],
            [("ingress", event_id)],
        )

        result = self.store.resolve_uncertain_operation(
            TEAM,
            "ingress",
            event_id,
            "retry",
        )
        self.assertEqual(result["state"], "pending")
        future = (
            self.runtime.datetime.datetime.now(
                self.runtime.datetime.timezone.utc
            )
            + self.runtime.datetime.timedelta(minutes=10)
        )
        self.assertEqual(
            [item["event_id"] for item in self.store.recoverable_hermes_ingress(now=future)],
            [event_id],
        )

    def test_complete_or_abandon_uncertain_ingress_unblocks_rebind(self):
        event_id = self.uncertain_hermes_ingress("2")
        result = self.store.resolve_uncertain_operation(
            TEAM,
            "ingress",
            event_id,
            "abandon",
        )
        self.assertEqual(result["state"], "cancelled")
        rebound = self.store.rebind(
            self.bridge.bridge_id,
            "codex_session",
            dict(self.bridge.source),
            expected_generation=self.bridge.binding_generation,
        )
        self.assertEqual(
            rebound.binding_generation,
            self.bridge.binding_generation + 1,
        )

    def test_uncertain_native_attempt_can_be_explicitly_requeued(self):
        attempt_id = self.uncertain_attempt()
        unresolved = self.store.unresolved_operations(TEAM)
        self.assertEqual(
            [(item["kind"], item["id"]) for item in unresolved],
            [("attempt", attempt_id)],
        )
        result = self.store.resolve_uncertain_operation(
            TEAM,
            "attempt",
            attempt_id,
            "retry",
        )
        self.assertEqual(result["state"], "requeued")
        claimed = self.store.claim_event_batch(self.bridge.bridge_id)
        self.assertEqual(len(claimed), 1)

    def test_failed_terminal_interrupt_becomes_operator_resolvable(self):
        attempt_id = self.awaiting_ack_attempt()
        self.assertTrue(
            self.store.mark_attempt_interrupt_unverified(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            )
        )
        unresolved = self.store.unresolved_operations(TEAM)
        self.assertEqual(
            [(item["kind"], item["id"], item["error_code"]) for item in unresolved],
            [("attempt", attempt_id, "terminal_interrupt_unverified")],
        )

    def test_failed_reconciliation_is_durable_and_can_be_retried(self):
        reconciliation_key, _idempotency_key = (
            self.failed_message_reconciliation()
        )
        unresolved = self.store.unresolved_operations(TEAM)
        self.assertEqual(
            [
                (item["kind"], item["id"], item["operation"])
                for item in unresolved
            ],
            [("reconciliation", reconciliation_key, "message")],
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliations
                SET updated_at=datetime('now','-60 days')
                WHERE reconciliation_key=?
                """,
                (reconciliation_key,),
            )
        self.store.prune(retention_days=30)
        self.assertEqual(
            self.store.unresolved_operations(TEAM)[0]["id"],
            reconciliation_key,
        )

        resolved = self.store.resolve_uncertain_operation(
            TEAM,
            "reconciliation",
            reconciliation_key,
            "retry",
        )
        self.assertEqual(resolved["state"], "pending")
        self.assertEqual(
            self.store.pending_reconciliation_keys(),
            [reconciliation_key],
        )

    def test_exhausted_pre_routing_ingress_requires_operator_retry(self):
        event_id = f"slack:{TEAM}:{CHANNEL}:1785000003.000001"
        self.assertEqual(
            self.store.reserve_routing_ingress(
                event_id,
                TEAM,
                CHANNEL,
                "1785000003.000001",
                {
                    "text": "first contact",
                    "message_ts": "1785000003.000001",
                    "user": "U12345678",
                },
            ),
            "routing",
        )
        for attempt in range(12):
            state = self.store.defer_routing_ingress(
                event_id,
                "actor_identity_unresolved",
            )
            self.assertEqual(
                state,
                "uncertain" if attempt == 11 else "routing",
            )
        self.assertEqual(
            [
                (item["kind"], item["id"], item["operation"])
                for item in self.store.unresolved_operations(TEAM)
            ],
            [("ingress", event_id, "unresolved")],
        )
        resolution = self.store.resolve_uncertain_operation(
            TEAM,
            "ingress",
            event_id,
            "retry",
        )
        self.assertEqual(resolution["state"], "routing")

    def test_corrupt_ingress_is_quarantined_for_operator_resolution(self):
        routing_event = f"slack:{TEAM}:{CHANNEL}:1785000003.000010"
        self.assertEqual(
            self.store.reserve_routing_ingress(
                routing_event,
                TEAM,
                CHANNEL,
                self.bridge.thread_ts,
                {
                    "text": "route this",
                    "message_ts": "1785000003.000010",
                    "user": "U12345678",
                },
            ),
            "routing",
        )
        hermes_event = f"slack:{TEAM}:{CHANNEL}:1785000003.000011"
        claim = self.store.claim_thread_ingress(
            hermes_event,
            TEAM,
            CHANNEL,
            self.bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=self.bridge.bridge_id,
            binding_generation=self.bridge.binding_generation,
            payload={
                "text": "continue",
                "message_ts": "1785000003.000011",
            },
        )
        self.assertTrue(
            self.store.release_thread_ingress(
                hermes_event,
                claim["lease_id"],
                "synthetic_retry",
            )
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE thread_ingress
                SET payload_json='[',updated_at=datetime('now','-1 hour')
                WHERE event_id IN (?,?)
                """,
                (routing_event, hermes_event),
            )

        self.assertEqual(self.store.recoverable_routing_ingress(), [])
        self.assertEqual(self.store.recoverable_hermes_ingress(), [])
        unresolved = {
            item["id"]: item["error_code"]
            for item in self.store.unresolved_operations(TEAM)
        }
        self.assertEqual(
            unresolved,
            {
                routing_event: "stored_ingress_invalid",
                hermes_event: "stored_ingress_invalid",
            },
        )

    def test_abandon_failed_reconciliation_stops_outbox_recovery(self):
        reconciliation_key, idempotency_key = (
            self.failed_message_reconciliation()
        )
        resolved = self.store.resolve_uncertain_operation(
            TEAM,
            "reconciliation",
            reconciliation_key,
            "abandon",
        )
        self.assertEqual(resolved["state"], "abandoned")
        self.assertNotIn(
            idempotency_key,
            self.store.pending_message_keys(),
        )
        self.assertEqual(self.store.unresolved_operations(TEAM), [])
        with self.assertRaisesRegex(
            ValueError,
            "requires verified Slack evidence",
        ):
            self.store.resolve_uncertain_operation(
                TEAM,
                "reconciliation",
                reconciliation_key,
                "complete",
            )

    def test_broker_resolution_is_disabled_without_operator_isolation(self):
        event_id = self.uncertain_hermes_ingress("3")
        woken: list[str] = []
        broker = self.runtime.Broker(
            "xox" + "b-test-token",
            store=self.store,
            attempt_closed=woken.append,
            verified_workspace_team_id=TEAM,
        )
        listed = broker.handle({"op": "unresolved", "team_id": TEAM})
        self.assertEqual(listed["operations"][0]["id"], event_id)
        with self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "OS-isolated operator authority",
        ):
            broker.handle(
                {
                    "op": "resolve",
                    "team_id": TEAM,
                    "kind": "ingress",
                    "id": event_id,
                    "action": "retry",
                }
            )
        self.assertEqual(woken, [])
        with self.assertRaises(self.runtime.NativeContinuationError):
            broker.handle({"op": "unresolved", "team_id": "T87654321"})


if __name__ == "__main__":
    unittest.main()
