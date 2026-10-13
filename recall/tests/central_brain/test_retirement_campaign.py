# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""A durable finite enrollment pass must survive process loss without replay."""

import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path[:0] = [str(Path(__file__).resolve().parents[2] / "server")]
from recall_server.retirement_campaign import (
    Campaign,
    ReconciliationRequired,
    seal_inventory,
)


def plan(source, parent):
    value = dict(
        contract="recall.retirement-cohort.v1",
        scope=dict(tenant_id="tenant", principal_id="principal", source_ids=[source]),
        parents=[
            dict(
                source_id=source,
                native_parent_id=parent,
                catalog_created_at="2026-10-13",
                manifest={"revision": 1},
            )
        ],
        after=None,
        more=False,
        next_cursor=[source, parent],
    )
    value["proof_sha256"] = hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return value


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.inventory = seal_inventory(
            [plan("a", "1"), plan("a", "2"), plan("b", "1")]
        )

    def campaign(self):
        return Campaign(self.path, self.inventory, self.inventory["sha256"])

    def test_intent_durable_before_owner_and_crash_refuses_replay(self):
        def crash(selected, emit):
            self.assertTrue(list(self.path.glob("*/intent.json")))
            raise KeyboardInterrupt()

        with self.campaign() as run:
            with self.assertRaises(KeyboardInterrupt):
                run.step(crash)
        with self.campaign() as run:
            owner = Mock()
            with self.assertRaises(ReconciliationRequired):
                run.step(owner)
            owner.assert_not_called()

    def test_new_campaign_directory_entry_is_durable_before_use(self):
        synced = []
        fsync = os.fsync

        def record(descriptor):
            synced.append(os.fstat(descriptor).st_ino)
            fsync(descriptor)

        with patch("os.fsync", side_effect=record):
            with Campaign(self.path / "new", self.inventory, self.inventory["sha256"]):
                self.assertIn(self.path.stat().st_ino, synced)

    def test_sources_alternate_and_deferred_parent_does_not_starve_tail(self):
        visited = []

        def owner(selected, emit):
            row = selected["parents"][0]
            visited.append((row["source_id"], row["native_parent_id"]))
            return dict(status="metadata_timeout", certainty="known")

        for _ in range(3):
            with self.campaign() as run:
                run.step(owner)
        self.assertEqual(visited, [("a", "1"), ("b", "1"), ("a", "2")])
        with self.campaign() as run:
            report = run.report()
            self.assertTrue(report["inventory_pass_exhausted"])
            self.assertIsNone(report["retirement_complete"])
            self.assertEqual(report["deferred_parents"], 3)

    def test_unattempted_is_retained_and_other_source_gets_turn(self):
        with self.campaign() as run:
            run.step(lambda p, emit: dict(status="budget", certainty="unattempted"))
            result = run.step(
                lambda p, emit: dict(status="enrolled", certainty="known")
            )
            self.assertEqual(result["plan"]["parents"][0]["source_id"], "b")
            self.assertEqual(run.report()["remaining_parents"], 2)

    def test_unknown_outcome_blocks_next_invocation(self):
        with self.campaign() as run:
            run.step(
                lambda p, emit: dict(status="enrollment_unknown", certainty="unknown")
            )
        with self.campaign() as run:
            with self.assertRaises(ReconciliationRequired):
                run.step(Mock())

    def test_reconcile_lost_result_appends_without_rewriting_intent(self):
        def lost(selected, emit):
            emit("launch_intent", dict(pin="pin"))
            emit("job", dict(id="job"))
            raise TimeoutError()

        with self.campaign() as run:
            with self.assertRaises(TimeoutError):
                run.step(lost)
        directory = self.path / "00000000"
        original = (directory / "intent.json").read_bytes()

        def inspect(events, emit):
            self.assertEqual([e["name"] for e in events], ["launch_intent", "job"])
            emit("reconciled_job", dict(id="job"))
            return [dict(status="enrolled", certainty="known")]

        with self.campaign() as run:
            run.reconcile(inspect)
            self.assertFalse(run.report()["reconciliation_required"])
            self.assertEqual(run.report()["considered_parents"], 1)
        self.assertEqual((directory / "intent.json").read_bytes(), original)
        self.assertFalse((directory / "result.json").exists())
        self.assertTrue((directory / "reconciliation.json").exists())
        with self.campaign() as run:
            self.assertEqual(run.report()["considered_parents"], 1)

    def test_observed_state_preserves_unknown_original_and_replays_admission(self):
        with self.campaign() as run:
            with self.assertRaises(TimeoutError):
                run.step(Mock(side_effect=TimeoutError()))
            original = (self.path / "00000000/intent.json").read_bytes()

            def inspect(plans, events, emit):
                return dict(
                    original_job=dict(id="job", pin="pin", body_sha256="sha"),
                    observation_job=dict(id="read-job", pin="read-pin"),
                    parents=[
                        dict(
                            plan_sha256=plans[0]["proof_sha256"],
                            status="observed_enabled",
                            certainty="known",
                        )
                    ],
                )

            run.reconcile(inspect, observed=True)
            self.assertFalse(run.report()["reconciliation_required"])
            self.assertEqual(run.report()["original_outcomes_unknown"], 1)
        directory = self.path / "00000000"
        self.assertEqual((directory / "intent.json").read_bytes(), original)
        self.assertFalse((directory / "result.json").exists())
        self.assertFalse((directory / "reconciliation.json").exists())
        saved = json.loads((directory / "observed-state.json").read_bytes())
        self.assertEqual(saved["original_outcome"], "unknown")
        with self.campaign() as run:
            self.assertEqual(run.report()["considered_parents"], 1)
            self.assertEqual(run.report()["original_outcomes_unknown"], 1)
            self.assertEqual(run.report()["deferred_parents"], 0)

    def test_observed_state_requires_every_frozen_plan_in_order(self):
        for parents in (
            [],
            [dict(plan_sha256="wrong", status="observed_enabled", certainty="known")],
        ):
            with self.subTest(parents=parents), tempfile.TemporaryDirectory() as tmp:
                with Campaign(
                    Path(tmp), self.inventory, self.inventory["sha256"]
                ) as run:
                    with self.assertRaises(TimeoutError):
                        run.step(Mock(side_effect=TimeoutError()))
                    with self.assertRaisesRegex(
                        ValueError, "observed_parent_membership"
                    ):
                        run.reconcile(
                            lambda *args: dict(parents=parents), observed=True
                        )
                    self.assertTrue(run.report()["reconciliation_required"])
                    self.assertFalse(
                        (Path(tmp) / "00000000/observed-state.json").exists()
                    )

    def test_transport_reconciliation_cannot_resolve_unknown_owner_commit(self):
        with self.campaign() as run:
            run.step(
                lambda p, emit: dict(status="enrollment_unknown", certainty="unknown")
            )
        with self.campaign() as run:
            inspect = Mock()
            with self.assertRaisesRegex(ValueError, "owner_outcome_unknown"):
                run.reconcile(inspect)
            inspect.assert_not_called()

    def test_reconciled_logs_with_unknown_owner_leave_campaign_blocked(self):
        with self.campaign() as run:
            with self.assertRaises(TimeoutError):
                run.step(Mock(side_effect=TimeoutError()))
            with self.assertRaisesRegex(ValueError, "owner_outcome_unknown"):
                run.reconcile(
                    lambda events, emit: [
                        dict(status="enrollment_unknown", certainty="unknown")
                    ]
                )
            self.assertTrue(run.report()["reconciliation_required"])
        self.assertFalse((self.path / "00000000" / "reconciliation.json").exists())

    def test_mutation_event_fsynced_and_secret_exception_not_stored(self):
        def owner(p, emit):
            emit("parent", dict(index=0, status="enrollment_inflight"))
            raise OSError("private SQL contents")

        with self.campaign() as run:
            with self.assertRaises(OSError):
                run.step(owner)
        evidence = "".join(p.read_text() for p in self.path.rglob("*.json"))
        self.assertIn("enrollment_inflight", evidence)
        self.assertNotIn("private SQL", evidence)

    def test_inventory_change_or_tampered_journal_refused(self):
        with self.campaign() as run:
            run.step(lambda p, emit: dict(status="disabled", certainty="known"))
        result = next(self.path.glob("*/result.json"))
        result.write_text("{}")
        with self.assertRaises(ValueError):
            with self.campaign():
                pass

    def test_duplicate_or_unsigned_plan_refused(self):
        with self.assertRaises(ValueError):
            seal_inventory([plan("a", "1"), plan("a", "1")])
        bad = plan("a", "1")
        bad["parents"][0]["catalog_created_at"] = "recreated"
        with self.assertRaises(ValueError):
            seal_inventory([bad])

    def test_second_operator_cannot_enter_locked_campaign(self):
        with self.campaign():
            with self.assertRaises(BlockingIOError):
                with self.campaign():
                    pass

    def test_known_results_require_recognized_terminal_states(self):
        with self.campaign() as run:
            with self.assertRaises(ValueError):
                run.step(
                    lambda p, emit: dict(status="invented_success", certainty="known")
                )
        with self.campaign() as run:
            with self.assertRaises(ReconciliationRequired):
                run.step(Mock())

    def test_quantum_commits_known_prefix_and_leaves_unattempted_tail(self):
        with self.campaign() as run:
            first = run.quantum(
                lambda plans, emit: [
                    dict(status="metadata_timeout", certainty="known")
                ],
                limit=25,
            )
            self.assertEqual(len(first["outcomes"]), 1)
            self.assertEqual(run.cursors, {"a": 1, "b": 0})
        with self.campaign() as run:
            chosen = []
            run.quantum(lambda plans, emit: chosen.extend(plans) or [], limit=25)
            self.assertEqual(chosen[0]["parents"][0]["source_id"], "b")
            self.assertEqual(run.report()["remaining_parents"], 2)

    def test_quantum_unknown_retains_exact_remaining_plans(self):
        with self.campaign() as run:
            run.quantum(
                lambda plans, emit: [
                    dict(status="enrolled", certainty="known"),
                    dict(status="enrollment_unknown", certainty="unknown"),
                ],
                limit=25,
            )
        with self.campaign() as run:
            self.assertEqual(run.cursors["a"], 1)
            self.assertTrue(run.report()["reconciliation_required"])
            self.assertEqual(len(run.pending["plans"]), 2)
            with self.assertRaises(ReconciliationRequired):
                run.quantum(Mock())


if __name__ == "__main__":
    unittest.main()
