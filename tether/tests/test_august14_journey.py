"""End-to-end August 14 incident regression on the schema-18 domain.

The witnessed incident: a turn was accepted, the completion proof was lost,
and the system sat silently wedged with siblings queued behind it. On the new
domain the same journey must (1) surface immediately as one typed blocker
with age and blocked-turn count, (2) never advertise retry, and (3) resolve
only through the capability-gated operator path, which frees the endpoint for
the queued siblings.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class SimulatedKill(BaseException):
    pass


def load_modules():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in (
            "native_driver",
            "domain_runtime",
            "domain_control",
            "domain_schema",
            "security",
        ):
            sys.modules.pop(name, None)
        return (
            importlib.import_module("domain_runtime"),
            importlib.import_module("native_driver"),
            importlib.import_module("domain_control"),
        )
    finally:
        sys.path[:] = previous


class August14JourneyTest(unittest.TestCase):
    def setUp(self):
        self.runtime_module, self.driver_module, self.control = load_modules()
        self.schema = self.runtime_module.domain_schema
        self.temp = tempfile.TemporaryDirectory(prefix="tether-aug14-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        self.db_path = base / "domain.db"
        connection = sqlite3.connect(self.db_path)
        try:
            self.schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={self.schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        self.runtime = self.runtime_module.DomainRuntime(self.db_path)
        self.driver = self.driver_module.NativeDriver(
            self.runtime,
            work_root=base / "driver",
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_accepted_without_terminal_is_visible_bounded_and_resolvable(self):
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        endpoint = self.runtime.register_endpoint(
            endpoint_key="aug14-session",
            endpoint_kind="detached_native",
            source_kind="claude_session",
            source_json='{"session_id":"aug14"}',
            ref_version=1,
            descriptor=descriptor,
        )
        binding = self.runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"],
            team_id="T12345678",
            channel_id="C1",
            thread_ts="100.1",
            owner_user_id="U12345678",
            idempotency_key="aug14-binding",
        )
        for index in range(8):
            self.runtime.admit_turn(
                binding_id=binding["binding_id"],
                event_key=f"evt-{index}",
                ordered_at=f"10{index}.0",
                payload_inline=f"mensaje {index}",
            )
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=1)

        def kill(mark: str) -> None:
            if mark == "after_spawn":
                raise SimulatedKill(mark)

        with self.assertRaises(SimulatedKill):
            self.driver.launch(
                attempt,
                fault_inject=kill,
                command=["/bin/sh", "-c", "printf 'perdido'"],
                cwd=pathlib.Path(self.temp.name),
                env={"PATH": "/usr/bin:/bin"},
            )
        recovery = self.driver.recover(attempt)
        self.assertEqual(recovery["classification"], "uncertain")

        # 1. The wedge is visible immediately, with age and blocked work.
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            snapshot = self.control.blocking_snapshot(
                connection,
                now=datetime.now(tz=UTC) + timedelta(minutes=5),
            )
        finally:
            connection.close()
        self.assertFalse(snapshot.summary["ready"])
        condition = next(
            item
            for item in snapshot.conditions
            if item.attempt_id == attempt["attempt_id"]
        )
        self.assertEqual(condition.reason_code, "spawn_proof_lost")
        self.assertGreater(condition.age_seconds, 0)
        self.assertEqual(condition.blocked_turn_count, 8)

        # 2. No retry is ever advertised, and without the isolated authority
        # capability nothing can move the attempt.
        self.assertEqual(condition.allowed_actions, ())
        self.assertEqual(
            condition.next_action_code,
            "enable_isolated_operator_authority",
        )
        self.assertIsNone(self.runtime.schedule_next(endpoint["endpoint_id"]))

        # 3. Capability-gated operator resolution frees the endpoint.
        capabilities = self.control.ControlCapabilities(operator_resolution=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            gated = self.control.blocking_snapshot(
                connection,
                capabilities=capabilities,
            )
            gated_condition = next(
                item
                for item in gated.conditions
                if item.attempt_id == attempt["attempt_id"]
            )
            self.assertIn("abandon", gated_condition.allowed_actions)
            request = self.control.OperatorResolutionRequest(
                condition_id=gated_condition.condition_id,
                expected_revision=gated_condition.revision,
                action="abandon",
                authority_receipt_id="auth-receipt-aug14",
                operator_principal_hash=hashlib.sha256(b"operator").hexdigest(),
                evidence_ref="authority://incident-review/august-14",
                evidence_sha256=hashlib.sha256(b"verified evidence").hexdigest(),
            )
            result = self.control.resolve_condition(
                connection,
                request,
                verify_authority=lambda observed, blocker: True,
                capabilities=capabilities,
            )
        finally:
            connection.close()
        self.assertEqual(result.status, "applied")

        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "operator_abandoned")
        self.assertFalse(status["lease_open"])

        # The queued siblings drain on a fresh fence.
        rescheduled = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.assertIsNotNone(rescheduled)
        self.assertEqual(rescheduled["lease_fence"], attempt["lease_fence"] + 1)

        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
