from __future__ import annotations

import importlib
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load_modules():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("domain_runtime", "domain_control", "domain_schema"):
            sys.modules.pop(name, None)
        module = importlib.import_module("domain_runtime")
        control = importlib.import_module("domain_control")
        return module, control
    finally:
        sys.path[:] = previous


class DomainRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.module, self.control = load_modules()
        self.schema = self.module.domain_schema
        self.temp = tempfile.TemporaryDirectory(prefix="tether-domain-runtime-")
        self.db_path = pathlib.Path(self.temp.name) / "domain.db"
        connection = sqlite3.connect(self.db_path)
        try:
            self.schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={self.schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        self.runtime = self.module.DomainRuntime(self.db_path)
        self.descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )

    def tearDown(self):
        self.temp.cleanup()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def assert_invariants_hold(self):
        connection = self.connect()
        try:
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def endpoint(self, key="native-session", kind="detached_native"):
        return self.runtime.register_endpoint(
            endpoint_key=key,
            endpoint_kind=kind,
            source_kind="claude_session",
            source_json='{"session_id":"work"}',
            ref_version=1,
            descriptor=self.descriptor,
        )

    def binding(self, endpoint_id, *, channel="C1", thread="100.1", key=None):
        return self.runtime.bind_thread(
            endpoint_id=endpoint_id,
            team_id="T12345678",
            channel_id=channel,
            thread_ts=thread,
            owner_user_id="U12345678",
            idempotency_key=key or f"idem-{channel}-{thread}",
        )

    def admit(self, binding_id, event_key, ordered_at, text="hola"):
        return self.runtime.admit_turn(
            binding_id=binding_id,
            event_key=event_key,
            ordered_at=ordered_at,
            payload_inline=text,
        )

    def receipt(self, attempt, *, sequence, state, operation="submit", **overrides):
        arguments = dict(
            attempt_id=attempt["attempt_id"],
            receipt_id=f"rcp-{attempt['attempt_id']}-{sequence}",
            lease_fence=attempt["lease_fence"],
            sequence=sequence,
            driver_incarnation="drv-1",
            operation=operation,
            request_id=attempt["driver_request_id"],
            watch_cursor=f"cursor-{sequence}",
            state=state,
            observed_at="2026-08-19 00:00:00",
        )
        arguments.update(overrides)
        return self.runtime.record_driver_receipt(**arguments)

    def test_full_turn_lifecycle_reaches_driver_owned_terminal(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        self.admit(binding["binding_id"], "evt-2", "100.2")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.assertEqual(attempt["event_keys"], ["evt-1", "evt-2"])
        self.assertEqual(attempt["lease_fence"], 1)
        self.assertEqual(attempt["driver_kind"], "detached_native")
        self.runtime.mark_submitting(attempt["attempt_id"])
        self.receipt(attempt, sequence=1, state="accepted")
        self.receipt(attempt, sequence=2, state="running")
        result = self.receipt(
            attempt,
            sequence=3,
            state="completed_with_response",
            response_ref="blob://sha256/aa",
            response_sha256="a" * 64,
            response_bytes=12,
        )
        self.assertEqual(result["state"], "completed_with_response")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertFalse(status["lease_open"])
        self.assertEqual(status["binding_generation"], binding["generation"])
        connection = self.connect()
        try:
            states = {
                row["event_key"]: row["state"]
                for row in connection.execute("SELECT event_key,state FROM queued_turns")
            }
        finally:
            connection.close()
        self.assertEqual(states, {"evt-1": "completed", "evt-2": "completed"})
        self.assert_invariants_hold()

    def test_admission_is_idempotent_by_event_key(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        first = self.admit(binding["binding_id"], "evt-1", "100.1")
        replay = self.admit(binding["binding_id"], "evt-1", "999.9", text="otro")
        self.assertEqual(first["ordered_at"], replay["ordered_at"])
        connection = self.connect()
        try:
            count = connection.execute("SELECT COUNT(*) FROM queued_turns").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(count, 1)

    def test_one_open_lease_per_endpoint_runtime_and_index(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        self.admit(binding["binding_id"], "evt-2", "100.2")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=1)
        self.assertIsNotNone(attempt)
        self.assertIsNone(self.runtime.schedule_next(endpoint["endpoint_id"]))
        connection = self.connect()
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO endpoint_leases(
                      attempt_id,endpoint_id,endpoint_incarnation,fence,
                      acquired_at,expires_at
                    ) VALUES('att_rogue',?,1,99,'2026-08-19 00:00:00',
                             '2026-08-19 01:00:00')
                    """,
                    (endpoint["endpoint_id"],),
                )
        finally:
            connection.close()

    def test_sibling_bindings_are_scheduled_oldest_ready_first(self):
        endpoint = self.endpoint()
        chatty = self.binding(endpoint["endpoint_id"], channel="C1", thread="100.1")
        patient = self.binding(endpoint["endpoint_id"], channel="C2", thread="200.1")
        self.admit(patient["binding_id"], "evt-patient", "150.0")
        self.admit(chatty["binding_id"], "evt-chatty-1", "100.0")
        first = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.assertEqual(first["binding_id"], chatty["binding_id"])
        # The chatty binding keeps producing newer turns while the first
        # attempt runs; the patient binding must not starve.
        self.admit(chatty["binding_id"], "evt-chatty-2", "300.0")
        self.receipt(first, sequence=1, state="no_reply")
        second = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.assertEqual(second["binding_id"], patient["binding_id"])
        self.assertEqual(second["event_keys"], ["evt-patient"])
        self.assertEqual(second["lease_fence"], 2)
        self.assert_invariants_hold()

    def test_one_attempt_never_claims_across_bindings(self):
        endpoint = self.endpoint()
        one = self.binding(endpoint["endpoint_id"], channel="C1", thread="100.1")
        two = self.binding(endpoint["endpoint_id"], channel="C2", thread="200.1")
        self.admit(one["binding_id"], "evt-1", "100.0")
        self.admit(two["binding_id"], "evt-2", "100.5")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=8)
        self.assertEqual(attempt["event_keys"], ["evt-1"])
        self.assertEqual(attempt["binding_id"], one["binding_id"])

    def test_stale_fence_and_replay_and_sequence_gap_are_dead(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        self.admit(binding["binding_id"], "evt-2", "100.2")
        first = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=1)
        self.receipt(first, sequence=1, state="no_reply")
        second = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=1)
        self.assertEqual(second["lease_fence"], 2)

        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(second, sequence=1, state="accepted", lease_fence=1)
        self.assertEqual(caught.exception.code, "stale_lease_fence")

        accepted = self.receipt(second, sequence=1, state="accepted")
        self.assertEqual(accepted["state"], "accepted")
        replay = self.receipt(second, sequence=1, state="accepted")
        self.assertTrue(replay["replay"])

        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(second, sequence=5, state="running")
        self.assertEqual(caught.exception.code, "receipt_sequence_gap")

        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(second, sequence=2, state="running", request_id="req_forged")
        self.assertEqual(caught.exception.code, "receipt_request_mismatch")
        self.assert_invariants_hold()

    def test_uncertain_holds_the_lease_and_surfaces_the_august_14_shape(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        for index in range(8):
            self.admit(binding["binding_id"], f"evt-{index}", f"10{index}.0")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"], max_turns=1)
        self.runtime.mark_submitting(attempt["attempt_id"])
        self.receipt(attempt, sequence=1, state="accepted")
        result = self.receipt(
            attempt,
            sequence=2,
            state="uncertain",
            error_code="native_execution_uncertain",
        )
        self.assertEqual(result["state"], "uncertain")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertTrue(status["lease_open"])
        self.assertIsNone(self.runtime.schedule_next(endpoint["endpoint_id"]))
        connection = self.connect()
        try:
            snapshot = self.control.blocking_snapshot(connection)
        finally:
            connection.close()
        conditions = [
            condition
            for condition in snapshot.conditions
            if condition.reason_code == "native_execution_uncertain"
        ]
        self.assertEqual(len(conditions), 1)
        self.assertEqual(conditions[0].attempt_id, attempt["attempt_id"])
        # The claimed turn stays 'ready' until terminalization, so the whole
        # queue counts as blocked behind the uncertain attempt.
        self.assertEqual(conditions[0].blocked_turn_count, 8)
        self.assertNotIn("retry", conditions[0].allowed_actions)
        self.assertEqual(conditions[0].allowed_actions, ())
        self.assert_invariants_hold()

    def test_failed_before_start_requeues_without_replay_risk(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        first = self.runtime.schedule_next(endpoint["endpoint_id"])
        result = self.receipt(first, sequence=1, state="not_started")
        self.assertEqual(result["state"], "failed_before_start")
        connection = self.connect()
        try:
            turn_state = connection.execute(
                "SELECT state FROM queued_turns WHERE event_key='evt-1'"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(turn_state, "ready")
        second = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.assertEqual(second["event_keys"], ["evt-1"])
        self.assertEqual(second["lease_fence"], 2)
        self.assert_invariants_hold()

    def test_cancellation_is_idempotent_and_driver_confirmed(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.runtime.mark_submitting(attempt["attempt_id"])
        self.receipt(attempt, sequence=1, state="accepted")
        self.runtime.request_cancel(attempt["attempt_id"], "cancel-1")
        self.runtime.request_cancel(attempt["attempt_id"], "cancel-1")
        with self.assertRaises(self.module.DomainRuntimeError):
            self.runtime.request_cancel(attempt["attempt_id"], "cancel-2")
        result = self.receipt(
            attempt,
            sequence=2,
            state="cancelled",
            operation="cancel",
            request_id="cancel-1",
        )
        self.assertEqual(result["state"], "cancelled")
        connection = self.connect()
        try:
            turn_state = connection.execute(
                "SELECT state FROM queued_turns WHERE event_key='evt-1'"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(turn_state, "cancelled")
        self.assert_invariants_hold()

    def test_terminal_attempts_reject_further_receipts(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.receipt(attempt, sequence=1, state="no_reply")
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(attempt, sequence=2, state="failed")
        self.assertEqual(caught.exception.code, "attempt_terminal")

    def test_herdr_and_zellij_endpoints_are_never_auto_scheduled(self):
        for kind, key in (("herdr_agent", "herdr-ep"), ("zellij_pane", "zellij-ep")):
            endpoint = self.endpoint(key=key, kind=kind)
            binding = self.binding(
                endpoint["endpoint_id"],
                channel=f"C-{key}",
                thread="100.1",
            )
            self.admit(binding["binding_id"], f"evt-{key}", "100.1")
            self.assertIsNone(
                self.runtime.schedule_next(endpoint["endpoint_id"]),
                kind,
            )

    def test_exact_no_reply_token_is_the_only_silence(self):
        self.assertTrue(self.module.is_no_reply("NO_REPLY"))
        self.assertTrue(self.module.is_no_reply("  NO_REPLY \n"))
        for not_silence in ("", "no reply", "NO_REPLY.", "NO_REPLY x", "NOREPLY"):
            self.assertFalse(self.module.is_no_reply(not_silence), repr(not_silence))

    def test_completed_response_requires_durable_evidence(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(attempt, sequence=1, state="completed_with_response")
        self.assertEqual(caught.exception.code, "response_evidence_missing")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "prepared")
        self.assertTrue(status["lease_open"])

    def test_unrelated_operations_never_close_an_open_attempt(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.receipt(attempt, sequence=1, state="accepted")
        # Generic activity — new admissions, new bindings, status reads — is
        # not a completion path.
        self.admit(binding["binding_id"], "evt-2", "100.2")
        self.binding(endpoint["endpoint_id"], channel="C9", thread="900.1")
        self.runtime.attempt_status(attempt["attempt_id"])
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "accepted")
        self.assertTrue(status["lease_open"])

    def test_mark_uncertain_requires_possible_execution(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.runtime.mark_uncertain(attempt["attempt_id"], "lost")
        self.assertEqual(caught.exception.code, "attempt_not_submitted")
        self.runtime.mark_submitting(attempt["attempt_id"])
        self.runtime.mark_uncertain(attempt["attempt_id"], "spawn_proof_lost")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "uncertain")
        self.assertTrue(status["lease_open"])
        self.assert_invariants_hold()

    def test_binding_idempotency_conflicts_fail_closed(self):
        endpoint = self.endpoint()
        self.binding(endpoint["endpoint_id"], key="idem-1")
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.runtime.bind_thread(
                endpoint_id=endpoint["endpoint_id"],
                team_id="T12345678",
                channel_id="C-other",
                thread_ts="777.7",
                owner_user_id="U12345678",
                idempotency_key="idem-1",
            )
        self.assertEqual(caught.exception.code, "idempotency_conflict")

    def test_pending_root_binding_claims_thread_once(self):
        endpoint = self.endpoint()
        binding = self.runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"],
            team_id="T12345678",
            channel_id="C1",
            thread_ts=None,
            owner_user_id="U12345678",
            idempotency_key="root-1",
        )
        self.assertEqual(binding["state"], "pending_root")
        active = self.runtime.activate_binding(binding["binding_id"], "111.1")
        self.assertEqual(active["state"], "active")
        replay = self.runtime.activate_binding(binding["binding_id"], "111.1")
        self.assertEqual(replay["thread_ts"], "111.1")
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.runtime.activate_binding(binding["binding_id"], "222.2")
        self.assertEqual(caught.exception.code, "thread_claim_conflict")


if __name__ == "__main__":
    unittest.main()


class ReceiptCollisionTest(DomainRuntimeTest):
    """A different observation reusing a receipt id is never swallowed."""

    def test_racing_driver_failure_is_a_conflict_not_a_silent_replay(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.receipt(attempt, sequence=1, state="accepted")

        # A second driver holding a stale sequence builds the same
        # deterministic receipt id and reports a terminal failure.
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(attempt, sequence=1, state="failed")
        self.assertEqual(caught.exception.code, "receipt_identity_conflict")

        # The failure is surfaced, not lost: the attempt is untouched and
        # still visibly open rather than quietly wedged.
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "accepted")
        self.assertTrue(status["lease_open"])

    def test_identical_observation_is_still_absorbed_as_a_replay(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        first = self.receipt(attempt, sequence=1, state="accepted")
        again = self.receipt(attempt, sequence=1, state="accepted")
        self.assertFalse(first["replay"])
        self.assertTrue(again["replay"])

    def test_differing_cursor_or_operation_also_conflicts(self):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "evt-1", "100.1")
        attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
        self.receipt(attempt, sequence=1, state="accepted")
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(
                attempt, sequence=1, state="accepted", watch_cursor="other"
            )
        self.assertEqual(caught.exception.code, "receipt_identity_conflict")


class ProjectionConsistencyTest(DomainRuntimeTest):
    """Every terminal and recovery transition must satisfy the schema's own
    invariants. Each test here corresponds to a defect found by driving the
    public API adversarially; before the fixes, all four left
    ``invariant_violations`` non-empty and the corrupted rows immutable.
    """

    def live_attempt(self, *, event_key="e1"):
        endpoint = self.endpoint()
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], event_key, "2026-08-30 00:00:00")
        return self.runtime.schedule_next(endpoint["endpoint_id"])

    def assert_clean(self):
        connection = self.connect()
        try:
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def turn_state(self, event_key="e1"):
        connection = self.connect()
        try:
            return connection.execute(
                "SELECT state FROM queued_turns WHERE event_key=?", (event_key,)
            ).fetchone()[0]
        finally:
            connection.close()

    def test_failed_attempt_cancels_its_turns(self):
        # A failed attempt delivered nothing. Recording its turns 'completed'
        # told the operator the message was answered when the driver crashed,
        # and the terminal-monotonic trigger made that permanent.
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="accepted")
        self.receipt(attempt, sequence=2, state="failed", error_code="exit_7")
        self.assertEqual(self.turn_state(), "cancelled")
        self.assert_clean()

    def test_completed_attempt_still_completes_its_turns(self):
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="accepted")
        self.receipt(
            attempt,
            sequence=2,
            state="completed_with_response",
            response_ref="blob:sha256:aa",
            response_sha256="a" * 64,
            response_bytes=3,
        )
        self.assertEqual(self.turn_state(), "completed")
        self.assert_clean()

    def test_mark_uncertain_projects_a_matching_receipt(self):
        # mark_uncertain wrote state without a receipt, so the newest receipt
        # still said 'accepted' while the attempt said 'uncertain'.
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="accepted")
        self.runtime.mark_uncertain(attempt["attempt_id"], "exit_status_unobserved")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "uncertain")
        connection = self.connect()
        try:
            row = connection.execute(
                "SELECT state,driver_incarnation FROM driver_receipts "
                "WHERE attempt_id=? ORDER BY sequence DESC LIMIT 1",
                (attempt["attempt_id"],),
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[0], "uncertain")
        # Tagged so an operator can tell a recovered uncertainty from a
        # witnessed one.
        self.assertEqual(row[1], "recovery")
        self.assert_clean()

    def test_mark_uncertain_after_running_receipt_stays_consistent(self):
        # The recovery projection has to hold from 'running' too, not just
        # 'accepted'; both are receipt states the invariant treats as live.
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="running")
        self.runtime.mark_uncertain(attempt["attempt_id"], "spawn_proof_lost")
        self.assertEqual(
            self.runtime.attempt_status(attempt["attempt_id"])["state"], "uncertain"
        )
        self.assert_clean()

    def test_running_receipt_before_accepted_reaches_accepted(self):
        # 'running' is a public receipt state and the projection invariant
        # treats it exactly like 'accepted'; stopping at 'submitting' left the
        # newest receipt disagreeing with the attempt.
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="running")
        self.assertEqual(
            self.runtime.attempt_status(attempt["attempt_id"])["state"], "accepted"
        )
        self.assert_clean()

    def test_running_receipt_after_accepted_is_still_accepted(self):
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="accepted")
        self.receipt(attempt, sequence=2, state="running")
        self.assertEqual(
            self.runtime.attempt_status(attempt["attempt_id"])["state"], "accepted"
        )
        self.assert_clean()

    def test_uncertain_attempt_refuses_a_liveness_receipt(self):
        # An uncertain attempt is frozen until reconciliation or an operator
        # resolves it; a bare liveness ping is not that proof.
        attempt = self.live_attempt()
        self.receipt(attempt, sequence=1, state="accepted")
        self.runtime.mark_uncertain(attempt["attempt_id"], "chaos")
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.receipt(attempt, sequence=3, state="running")
        self.assertEqual(caught.exception.code, "attempt_uncertain")
        self.assertEqual(
            self.runtime.attempt_status(attempt["attempt_id"])["state"], "uncertain"
        )
        self.assert_clean()


class EndpointRepointTest(DomainRuntimeTest):
    """Re-registering an endpoint used to silently discard the new coordinates,
    so a restarted agent kept the dead session's target and attempts were
    driven at a process that no longer existed. No invariant caught it.
    """

    def register(self, source_json, ref_version):
        return self.runtime.register_endpoint(
            endpoint_key="ep-repoint",
            endpoint_kind="detached_native",
            source_kind="claude_session",
            source_json=source_json,
            ref_version=ref_version,
            descriptor=self.descriptor,
        )

    def endpoint_row(self):
        connection = self.connect()
        try:
            return connection.execute(
                "SELECT source_json,ref_version,incarnation FROM endpoints "
                "WHERE endpoint_key='ep-repoint'"
            ).fetchone()
        finally:
            connection.close()

    def test_identical_registration_is_idempotent(self):
        self.register('{"session_id":"A"}', 1)
        self.register('{"session_id":"A"}', 1)
        row = self.endpoint_row()
        self.assertEqual(row[2], 1, "an unchanged re-registration must not bump")

    def test_new_coordinates_repoint_and_invalidate_bindings(self):
        endpoint = self.register('{"session_id":"A","pid":111}', 1)
        binding = self.binding(endpoint["endpoint_id"])
        self.register('{"session_id":"B","pid":222}', 2)
        row = self.endpoint_row()
        self.assertEqual(row[0], '{"session_id":"B","pid":222}')
        self.assertEqual(row[1], 2)
        self.assertEqual(row[2], 2, "repointing must bump the incarnation")
        connection = self.connect()
        try:
            state, code = connection.execute(
                "SELECT state,error_code FROM thread_bindings WHERE binding_id=?",
                (binding["binding_id"],),
            ).fetchone()
            self.assertEqual(state, "rebind_required")
            self.assertEqual(code, "endpoint_incarnation_changed")
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def test_repoint_refused_while_a_lease_is_open(self):
        endpoint = self.register('{"session_id":"A","pid":111}', 1)
        binding = self.binding(endpoint["endpoint_id"])
        self.admit(binding["binding_id"], "e1", "2026-08-30 00:00:00")
        self.assertIsNotNone(self.runtime.schedule_next(endpoint["endpoint_id"]))
        with self.assertRaises(self.module.DomainRuntimeError) as caught:
            self.register('{"session_id":"B","pid":222}', 2)
        self.assertEqual(caught.exception.code, "endpoint_busy")
        # The live attempt keeps its target; nothing was yanked mid-flight.
        self.assertEqual(self.endpoint_row()[0], '{"session_id":"A","pid":111}')
