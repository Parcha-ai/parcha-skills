import hashlib
import importlib.util
import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta

from test_bridge import load_runtime


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "runtime" / "domain_schema.py"
CONTROL_PATH = ROOT / "runtime" / "domain_control.py"


def load_modules():
    schema_spec = importlib.util.spec_from_file_location("domain_schema", SCHEMA_PATH)
    schema = importlib.util.module_from_spec(schema_spec)
    sys.modules["domain_schema"] = schema
    schema_spec.loader.exec_module(schema)
    control_name = "tether_domain_control_test"
    control_spec = importlib.util.spec_from_file_location(control_name, CONTROL_PATH)
    control = importlib.util.module_from_spec(control_spec)
    sys.modules[control_name] = control
    control_spec.loader.exec_module(control)
    return schema, control


class DomainControlTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.schema, self.control = load_modules()
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )

    def tearDown(self):
        self.temp.cleanup()

    def resolve_endpoint(self, row):
        raw = json.loads(str(row["source_json"]))
        source, binding = self.runtime._canonical_source(
            str(row["source_kind"]),
            raw,
            allow_legacy=True,
        )
        endpoint_key = self.runtime.endpoint_identity_key(binding)
        return self.schema.LegacyEndpointRef(
            endpoint_key=endpoint_key,
            candidate_endpoint_key=endpoint_key,
            endpoint_kind=binding.endpoint_kind,
            source_kind=str(row["source_kind"]),
            source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
            ref_version=binding.version,
            ready=True,
        )

    def migrate_august_14_fixture(self):
        bridge = self.store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": "august-14-fixture", "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": "august-14-root",
            }
        )
        bridge = self.store.bind(bridge.bridge_id, "1786690136.400269")
        self.assertTrue(self.store.enqueue_event("event-open", bridge.bridge_id, "first"))
        claimed = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            ["event-open"],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [row["event_id"] for row in claimed],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        for index in range(7):
            self.assertTrue(
                self.store.enqueue_event(
                    f"event-queued-{index}",
                    bridge.bridge_id,
                    f"queued {index}",
                )
            )
        connection = sqlite3.connect(self.home / "bridges.db")
        try:
            self.schema.migrate_legacy_v17(
                connection,
                self.descriptor,
                self.resolve_endpoint,
            )
        finally:
            connection.close()
        return attempt_id

    def request(self, condition, *, action="complete", receipt="auth-receipt-1"):
        return self.control.OperatorResolutionRequest(
            condition_id=condition.condition_id,
            expected_revision=condition.revision,
            action=action,
            authority_receipt_id=receipt,
            operator_principal_hash=hashlib.sha256(b"operator").hexdigest(),
            evidence_ref="authority://incident-review/august-14",
            evidence_sha256=hashlib.sha256(b"verified evidence").hexdigest(),
        )

    def test_august_14_blocker_is_visible_and_never_advertises_retry(self):
        attempt_id = self.migrate_august_14_fixture()
        connection = sqlite3.connect(self.home / "bridges.db")
        try:
            snapshot = self.control.blocking_snapshot(
                connection,
                now=datetime.now(tz=UTC) + timedelta(hours=1),
            )
        finally:
            connection.close()

        self.assertEqual(snapshot.summary["condition_count"], 1)
        self.assertEqual(snapshot.summary["readiness_blocker_count"], 1)
        self.assertEqual(snapshot.summary["operator_resolvable_count"], 0)
        self.assertEqual(snapshot.summary["blocked_turn_count"], 8)
        self.assertFalse(snapshot.summary["ready"])
        condition = snapshot.conditions[0]
        self.assertEqual(condition.attempt_id, attempt_id)
        self.assertEqual(condition.reason_code, "native_execution_uncertain")
        self.assertEqual(condition.allowed_actions, ())
        self.assertNotIn("retry", condition.allowed_actions)
        self.assertEqual(
            condition.next_action_code,
            "enable_isolated_operator_authority",
        )
        self.assertGreater(condition.age_seconds, 0)
        second_connection = sqlite3.connect(self.home / "bridges.db")
        try:
            second_snapshot = self.control.blocking_snapshot(
                second_connection,
                now=datetime.now(tz=UTC) + timedelta(hours=2),
            )
        finally:
            second_connection.close()
        self.assertEqual(snapshot.snapshot_revision, second_snapshot.snapshot_revision)

    def test_authority_resolution_is_fenced_atomic_and_idempotent(self):
        attempt_id = self.migrate_august_14_fixture()
        connection = sqlite3.connect(self.home / "bridges.db")
        connection.row_factory = sqlite3.Row
        capabilities = self.control.ControlCapabilities(operator_resolution=True)
        snapshot = self.control.blocking_snapshot(
            connection,
            capabilities=capabilities,
        )
        condition = snapshot.conditions[0]
        request = self.request(condition)
        seen = []

        result = self.control.resolve_condition(
            connection,
            request,
            verify_authority=lambda observed, blocker: seen.append(
                (observed.authority_receipt_id, blocker.condition_id)
            )
            or True,
            capabilities=capabilities,
        )
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.attempt_id, attempt_id)
        self.assertEqual(
            seen,
            [
                (request.authority_receipt_id, condition.condition_id),
                (request.authority_receipt_id, condition.condition_id),
            ],
        )
        self.assertTrue(self.control.blocking_snapshot(connection).summary["ready"])
        attempt = connection.execute(
            "SELECT state FROM native_attempts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["state"], "operator_completed")
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM queued_turns WHERE state='completed'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM queued_turns WHERE state='ready'"
            ).fetchone()[0],
            7,
        )
        self.assertEqual(
            connection.execute(
                "SELECT COUNT(*) FROM endpoint_leases WHERE released_at IS NULL"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(self.schema.invariant_violations(connection), [])
        resolution_time = connection.execute(
            "SELECT resolved_at FROM operator_resolutions WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()[0]
        self.assertEqual(
            resolution_time,
            connection.execute(
                "SELECT terminal_at FROM native_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0],
        )
        self.assertGreaterEqual(resolution_time, "2026-08-18 00:00:00")

        replay_seen = []
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "operator authority did not approve",
        ):
            self.control.resolve_condition(
                connection,
                request,
                verify_authority=lambda _request, _condition: False,
                capabilities=capabilities,
            )
        endpoint_id = connection.execute(
            "SELECT endpoint_id FROM native_attempts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()[0]
        connection.execute(
            "UPDATE endpoints SET incarnation=incarnation+1 WHERE endpoint_id=?",
            (endpoint_id,),
        )
        connection.commit()
        self.assertEqual(self.schema.invariant_violations(connection), [])
        replay = self.control.resolve_condition(
            connection,
            request,
            verify_authority=lambda _request, blocker: replay_seen.append(
                blocker.condition_id
            )
            or True,
            capabilities=capabilities,
        )
        self.assertEqual(replay.status, "already_applied")
        self.assertEqual(replay_seen, [condition.condition_id])

        wrong_target = self.control.OperatorResolutionRequest(
            **{
                **request.__dict__,
                "condition_id": "blk_wrong_target",
            }
        )
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "different resolution",
        ):
            self.control.resolve_condition(
                connection,
                wrong_target,
                verify_authority=lambda _request, _condition: True,
                capabilities=capabilities,
            )
        connection.close()

    def test_resolution_rejects_denied_stale_and_retry_requests_without_mutation(self):
        attempt_id = self.migrate_august_14_fixture()
        connection = sqlite3.connect(self.home / "bridges.db")
        capabilities = self.control.ControlCapabilities(operator_resolution=True)
        condition = self.control.blocking_snapshot(
            connection,
            capabilities=capabilities,
        ).conditions[0]

        denied = self.request(condition)
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "attested isolated authority channel",
        ):
            self.control.resolve_condition(
                connection,
                denied,
                verify_authority=lambda _request, _condition: True,
            )
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "operator authority did not approve",
        ):
            self.control.resolve_condition(
                connection,
                denied,
                verify_authority=lambda _request, _condition: False,
                capabilities=capabilities,
            )

        validations = iter((True, False))
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "expired or was revoked before commit",
        ):
            self.control.resolve_condition(
                connection,
                denied,
                verify_authority=lambda _request, _condition: next(validations),
                capabilities=capabilities,
            )

        stale = self.control.OperatorResolutionRequest(
            **{
                **self.request(condition, receipt="auth-receipt-2").__dict__,
                "expected_revision": "0" * 64,
            }
        )
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "fetch a new snapshot",
        ):
            self.control.resolve_condition(
                connection,
                stale,
                verify_authority=lambda _request, _condition: True,
                capabilities=capabilities,
            )

        retry = self.request(condition, action="retry", receipt="auth-receipt-3")
        with self.assertRaisesRegex(
            self.control.DomainControlError,
            "only complete or abandon",
        ):
            self.control.resolve_condition(
                connection,
                retry,
                verify_authority=lambda _request, _condition: True,
                capabilities=capabilities,
            )

        self.assertEqual(
            connection.execute(
                "SELECT state FROM native_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0],
            "uncertain",
        )
        self.assertEqual(
            connection.execute("SELECT COUNT(*) FROM operator_resolutions").fetchone()[0],
            0,
        )
        connection.close()


if __name__ == "__main__":
    unittest.main()
