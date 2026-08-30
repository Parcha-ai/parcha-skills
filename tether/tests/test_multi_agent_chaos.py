"""The schema-18 domain under the load pattern that broke production.

The deployed system failed in exactly one shape: several participants in
one Slack thread, turns arriving while an attempt is open, a driver that
stops answering, and an operator who has to work out what is stuck. These
tests drive that shape against the new domain runtime — not to assert a
fix exists, but to check the invariants hold when the traffic is messy.
"""

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


def load():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("domain_runtime", "domain_control", "domain_schema"):
            sys.modules.pop(name, None)
        return (
            importlib.import_module("domain_runtime"),
            importlib.import_module("domain_control"),
        )
    finally:
        sys.path[:] = previous


class MultiAgentChaosTest(unittest.TestCase):
    def setUp(self):
        self.runtime_module, self.control = load()
        self.schema = self.runtime_module.domain_schema
        self.temp = tempfile.TemporaryDirectory(prefix="tether-chaos-")
        self.db = pathlib.Path(self.temp.name) / "domain.db"
        connection = sqlite3.connect(self.db)
        try:
            self.schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={self.schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        self.runtime = self.runtime_module.DomainRuntime(self.db)
        self.descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T0516FFQDRU",
            persona_id="primary",
            authorized_owner_ids=("UHUMAN01", "UAGENT01", "UAGENT02"),
            policy_generation=1,
        )
        self.endpoint = self.runtime.register_endpoint(
            endpoint_key="debate-session",
            endpoint_kind="detached_native",
            source_kind="claude_session",
            source_json='{"session_id":"debate"}',
            ref_version=1,
            descriptor=self.descriptor,
        )["endpoint_id"]

    def tearDown(self):
        self.temp.cleanup()

    def bind(self, name, thread):
        return self.runtime.bind_thread(
            endpoint_id=self.endpoint,
            team_id="T0516FFQDRU",
            channel_id="C095VU95XQR",
            thread_ts=thread,
            owner_user_id="UHUMAN01",
            idempotency_key=f"debate-{name}",
        )["binding_id"]

    def say(self, binding, key, at, who="UHUMAN01"):
        return self.runtime.admit_turn(
            binding_id=binding,
            event_key=key,
            ordered_at=at,
            payload_inline=f"{who} says {key}",
        )

    def receipt(self, attempt, seq, state, **kw):
        args = dict(
            attempt_id=attempt["attempt_id"],
            receipt_id=f"r-{attempt['attempt_id']}-{seq}",
            lease_fence=attempt["lease_fence"],
            sequence=seq,
            driver_incarnation="drv",
            operation="submit",
            request_id=attempt["driver_request_id"],
            watch_cursor=f"c{seq}",
            state=state,
            observed_at="2026-08-30 03:00:00",
        )
        args.update(kw)
        return self.runtime.record_driver_receipt(**args)

    def invariants(self):
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        try:
            return self.schema.invariant_violations(connection)
        finally:
            connection.close()

    def test_three_threads_interleaved_never_cross_or_starve(self):
        """The debate shape: three threads on one endpoint, all talking."""
        threads = {
            name: self.bind(name, ts)
            for name, ts in (("a", "100.1"), ("b", "200.1"), ("c", "300.1"))
        }
        # Everyone speaks, oldest-first ordering deliberately interleaved.
        self.say(threads["b"], "b1", "100.0")
        self.say(threads["a"], "a1", "101.0")
        self.say(threads["c"], "c1", "102.0")
        self.say(threads["b"], "b2", "103.0")

        served = []
        for _ in range(4):
            attempt = self.runtime.schedule_next(self.endpoint, max_turns=1)
            if attempt is None:
                break
            served.append((attempt["binding_id"], attempt["event_keys"]))
            self.receipt(attempt, 1, "no_reply")

        # Every thread got served, oldest-ready first, and no attempt ever
        # claimed turns belonging to two different threads.
        self.assertEqual(len(served), 4)
        self.assertEqual(served[0][0], threads["b"])
        self.assertEqual(served[1][0], threads["a"])
        self.assertEqual(served[2][0], threads["c"])
        self.assertEqual({len(keys) for _b, keys in served}, {1})
        self.assertEqual(self.invariants(), [])

    def test_a_silent_driver_blocks_only_until_an_operator_resolves_it(self):
        """The production failure: one turn goes unanswered forever."""
        a = self.bind("a", "100.1")
        b = self.bind("b", "200.1")
        self.say(a, "a1", "100.0")
        for index in range(5):
            self.say(b, f"b{index}", f"20{index}.0")

        stuck = self.runtime.schedule_next(self.endpoint, max_turns=1)
        self.runtime.mark_submitting(stuck["attempt_id"])
        self.receipt(stuck, 1, "accepted")
        self.receipt(stuck, 2, "uncertain", error_code="driver_went_silent")

        # The sibling thread is blocked — deliberately, since the endpoint
        # holds one lease — and the blocker is visible with its cost.
        self.assertIsNone(self.runtime.schedule_next(self.endpoint))
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        try:
            snapshot = self.control.blocking_snapshot(connection)
        finally:
            connection.close()
        blocker = next(
            c for c in snapshot.conditions
            if c.attempt_id == stuck["attempt_id"]
        )
        self.assertEqual(blocker.blocked_turn_count, 6)
        self.assertEqual(blocker.allowed_actions, ())

        # Resolution frees the endpoint and the backlog drains in order.
        capabilities = self.control.ControlCapabilities(operator_resolution=True)
        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        try:
            gated = self.control.blocking_snapshot(
                connection, capabilities=capabilities
            )
            condition = next(
                c for c in gated.conditions
                if c.attempt_id == stuck["attempt_id"]
            )
            import hashlib
            self.control.resolve_condition(
                connection,
                self.control.OperatorResolutionRequest(
                    condition_id=condition.condition_id,
                    expected_revision=condition.revision,
                    action="abandon",
                    authority_receipt_id="auth-chaos",
                    operator_principal_hash=hashlib.sha256(b"op").hexdigest(),
                    evidence_ref="authority://chaos",
                    evidence_sha256=hashlib.sha256(b"ev").hexdigest(),
                ),
                verify_authority=lambda observed, blocker: True,
                capabilities=capabilities,
            )
        finally:
            connection.close()

        drained = self.runtime.schedule_next(self.endpoint, max_turns=8)
        self.assertIsNotNone(drained)
        self.assertEqual(drained["binding_id"], b)
        self.assertEqual(self.invariants(), [])

    def test_turns_arriving_mid_attempt_queue_and_never_double_execute(self):
        """Messages sent while an agent is working must wait, not race."""
        a = self.bind("a", "100.1")
        self.say(a, "a1", "100.0")
        attempt = self.runtime.schedule_next(self.endpoint, max_turns=1)
        self.runtime.mark_submitting(attempt["attempt_id"])
        self.receipt(attempt, 1, "accepted")

        # Three more arrive while the first is in flight.
        for index in range(3):
            self.say(a, f"late{index}", f"10{index + 1}.0")
        self.assertIsNone(self.runtime.schedule_next(self.endpoint))

        self.receipt(attempt, 2, "no_reply")
        follow = self.runtime.schedule_next(self.endpoint, max_turns=8)
        self.assertEqual(
            follow["event_keys"], ["late0", "late1", "late2"]
        )
        # The completed turn is never re-run.
        self.assertNotIn("a1", follow["event_keys"])
        self.assertEqual(self.invariants(), [])

    def test_peer_agent_turns_are_ordinary_turns_once_admitted(self):
        """Agent-authored turns get no special path — same lease, same order."""
        a = self.bind("a", "100.1")
        self.say(a, "human", "100.0", who="UHUMAN01")
        self.say(a, "agent", "101.0", who="UAGENT02")

        attempt = self.runtime.schedule_next(self.endpoint, max_turns=8)
        self.assertEqual(attempt["event_keys"], ["human", "agent"])
        self.receipt(
            attempt, 1, "completed_with_response",
            response_ref="blob:sha256:" + "a" * 64,
            response_sha256="a" * 64,
            response_bytes=7,
        )
        self.assertIsNone(self.runtime.schedule_next(self.endpoint))
        self.assertEqual(self.invariants(), [])
