"""Claim diagnostics preserve exception and context-manager behavior."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path[:0] = [
    str(Path(__file__).resolve().parents[2]),
    str(Path(__file__).resolve().parents[2] / "server"),
]
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import ChunkRetirementError
from recall_server.db import SearchDeadlineExceeded
from psycopg_pool import PoolTimeout


PRIVATE = "private-parent private-source private-dsn"


class PrivateError(RuntimeError):
    pass


class ClaimStore:
    def __init__(self, failures=None, suppress=None, candidate=True):
        self.failures = failures or {}
        self.suppress = suppress
        self.candidate = candidate
        self.calls = []
        self.exits = []

    def step(self, phase):
        self.calls.append(phase)
        if phase in self.failures:
            raise self.failures[phase]

    def connect(self):
        self.step("connect")
        return self.Context(self, "connection")

    class Context:
        def __init__(self, store, name):
            self.store, self.name = store, name

        def __enter__(self):
            self.store.step(self.name + "_enter")
            return self

        def __exit__(self, kind, error, traceback):
            self.store.exits.append((self.name, error))
            self.store.step(self.name + "_exit")
            return self.store.suppress == self.name

        def transaction(self):
            self.store.step("transaction_construct")
            return self.store.Context(self.store, "transaction")

    def _execute_bounded(self, connection, sql, args, deadline):
        phase = (
            "candidate_select" if sql.lstrip().startswith("SELECT") else "epoch_update"
        )
        self.step(phase)
        if phase == "epoch_update":
            value = {"scope_epoch": 4}
        else:
            value = (
                dict(
                    source_id="source",
                    native_parent_id=PRIVATE,
                    scope_epoch=3,
                    manifest={},
                )
                if self.candidate
                else None
            )
        return Mock(fetchone=lambda: self.fetch(phase, value))

    def fetch(self, phase, value):
        self.step(phase + "_fetch")
        return value


class ClaimDiagnosticTests(unittest.TestCase):
    PHASES = (
        "connect",
        "connection_enter",
        "transaction_enter",
        "authorize",
        "candidate_select",
        "epoch_update",
        "transaction_exit",
        "connection_exit",
    )

    def scope(self):
        return runner.RetirementScope("tenant", "principal", ("source",))

    def candidate(self):
        return dict(
            source_id="source",
            native_parent_id="completed-parent",
            scope_epoch=2,
            manifest=dict(
                tenant_id="tenant",
                source_id="source",
                native_parent_id="completed-parent",
                manifest_artifact_id="artifact",
                revision=1,
            ),
        )

    def after_work(self, store):
        claim = runner._claim
        calls = []

        def next_claim(*args, **kwargs):
            calls.append(kwargs)
            return self.candidate() if len(calls) == 1 else claim(*args, **kwargs)

        complete = dict(
            complete=True,
            batches=2,
            cleared_documents=3,
            cleared_chunks=4,
            cleared_utf8_bytes=5,
            eligible_documents=3,
            excluded={"unlocated": 1},
        )
        with (
            patch.object(runner, "_claim", side_effect=next_claim),
            patch.object(
                runner, "_authorize", side_effect=lambda *a: store.step("authorize")
            ),
            patch.object(
                runner, "retire_parent_chunks", return_value=complete
            ) as retire,
        ):
            result = runner.run_retirement(
                store, Mock(), scope=self.scope(), apply=True
            )
        self.assertEqual(len(calls), 2)
        self.assertEqual(retire.call_count, 1)
        self.assertEqual(
            set(result),
            {
                "status",
                "attempted_parents",
                "completed_parent_proofs",
                "failed_parents",
                "partial_parents",
                "batches",
                "cleared_documents",
                "cleared_chunks",
                "cleared_utf8_bytes",
                "eligible_documents",
                "commit_outcome_unknown",
                "errors",
                "excluded",
                "archive_gets",
                "archive_bytes",
                "body_eligibility",
                "physical_reclaimed_bytes",
            },
        )
        self.assertEqual(result["status"], "claim_refused")
        self.assertEqual(
            (
                result["attempted_parents"],
                result["completed_parent_proofs"],
                result["failed_parents"],
            ),
            (1, 1, 0),
        )
        self.assertEqual(
            (
                result["batches"],
                result["cleared_documents"],
                result["cleared_chunks"],
                result["cleared_utf8_bytes"],
            ),
            (2, 3, 4, 5),
        )
        self.assertEqual(result["excluded"], {"unlocated": 1})
        self.assertFalse(result["commit_outcome_unknown"])
        self.assertEqual(result["errors"]["retirement_claim_unavailable"], 1)
        self.assertNotIn(PRIVATE, str(result))
        for key, value in result["errors"].items():
            self.assertRegex(key, r"^[a-z_]{1,80}$")
            self.assertEqual(value, 1)
        return result

    def test_every_phase_stops_after_work_without_losing_counters(self):
        for phase in self.PHASES:
            with self.subTest(phase=phase):
                result = self.after_work(ClaimStore({phase: PrivateError(PRIVATE)}))
                self.assertEqual(
                    set(result["errors"]),
                    {"retirement_claim_unavailable", f"retirement_claim_{phase}_other"},
                )

    def test_first_claim_preserves_exact_exception_object(self):
        for phase in self.PHASES:
            with self.subTest(phase=phase):
                error = PrivateError(PRIVATE)
                store = ClaimStore({phase: error})
                with (
                    patch.object(
                        runner,
                        "_authorize",
                        side_effect=lambda *a: store.step("authorize"),
                    ),
                    patch.object(runner, "retire_parent_chunks") as retire,
                    self.assertRaises(PrivateError) as caught,
                ):
                    runner.run_retirement(store, Mock(), scope=self.scope(), apply=True)
                self.assertIs(caught.exception, error)
                retire.assert_not_called()

    def test_factory_and_fetch_failures_have_operation_phase(self):
        for failure, phase in (
            ("transaction_construct", "transaction_enter"),
            ("candidate_select_fetch", "candidate_select"),
            ("epoch_update_fetch", "epoch_update"),
        ):
            with self.subTest(failure=failure):
                result = self.after_work(ClaimStore({failure: PrivateError(PRIVATE)}))
                self.assertIn(f"retirement_claim_{phase}_other", result["errors"])

    def test_exceptional_rollback_or_connection_cleanup_identifies_last_exit(self):
        for final in ("transaction_exit", "connection_exit"):
            with self.subTest(final=final):
                body = PrivateError(PRIVATE)
                cleanup = TimeoutError(PRIVATE)
                failures = {"authorize": body, final: cleanup}
                store = ClaimStore(failures)
                result = self.after_work(store)
                self.assertIn(f"retirement_claim_{final}_timeout", result["errors"])
                self.assertIs(store.exits[0][1], body)
                if final == "transaction_exit":
                    self.assertIs(store.exits[1][1], cleanup)

    def test_connection_exit_can_replace_transaction_exit_without_claiming_rollback(
        self,
    ):
        store = ClaimStore(
            {
                "transaction_exit": PrivateError(PRIVATE),
                "connection_exit": TimeoutError(PRIVATE),
            }
        )
        result = self.after_work(store)
        self.assertIn("retirement_claim_connection_exit_timeout", result["errors"])
        self.assertNotIn("retirement_claim_transaction_exit_other", result["errors"])

    def test_success_no_candidate_and_context_suppression_are_preserved(self):
        for suppress in ("transaction", "connection"):
            with self.subTest(suppress=suppress):
                store = ClaimStore(
                    {"authorize": PrivateError(PRIVATE)}, suppress=suppress
                )
                with patch.object(
                    runner, "_authorize", side_effect=lambda *a: store.step("authorize")
                ):
                    report = runner.run_retirement(
                        store, Mock(), scope=self.scope(), apply=True
                    )
                self.assertEqual(report["status"], "no_ready_parents")
                self.assertEqual(report["errors"], {})
        store = ClaimStore(candidate=False)
        with patch.object(
            runner, "_authorize", side_effect=lambda *a: store.step("authorize")
        ):
            result = runner._claim(
                store,
                scope=self.scope(),
                limits=runner.RunnerLimits(),
                deadline_at=10,
                attempted=[],
            )
        self.assertIsNone(result)
        self.assertEqual(store.calls[-2:], ["transaction_exit", "connection_exit"])
        store = ClaimStore()
        with patch.object(
            runner, "_authorize", side_effect=lambda *a: store.step("authorize")
        ):
            result = runner._claim(
                store,
                scope=self.scope(),
                limits=runner.RunnerLimits(),
                deadline_at=10,
                attempted=[],
            )
        self.assertEqual(result["scope_epoch"], 4)

    def test_category_is_allowlisted_and_never_renders_error(self):
        error = PrivateError(PRIVATE)
        error.sqlstate = "55P03"
        result = self.after_work(ClaimStore({"candidate_select": error}))
        self.assertIn(
            "retirement_claim_candidate_select_sql_lock_not_available", result["errors"]
        )
        error.sqlstate = "private-state"
        result = self.after_work(ClaimStore({"candidate_select": error}))
        self.assertIn("retirement_claim_candidate_select_other", result["errors"])
        self.assertNotIn("private-state", str(result))

    def test_local_deadline_and_pool_timeout_categories_are_closed(self):
        for error, category in (
            (SearchDeadlineExceeded(PRIVATE), "search_deadline"),
            (PoolTimeout(PRIVATE), "pool_timeout"),
        ):
            with self.subTest(category=category):
                result = self.after_work(ClaimStore({"connect": error}))
                self.assertIn(f"retirement_claim_connect_{category}", result["errors"])

    def test_diagnostic_attributes_cannot_replace_failure_or_expose_text(self):
        class HostileError(Exception):
            @property
            def sqlstate(self):
                raise ValueError(PRIVATE)

            def __str__(self):
                raise AssertionError("must not render exception")

        result = self.after_work(ClaimStore({"epoch_update": HostileError()}))
        self.assertIn("retirement_claim_epoch_update_other", result["errors"])
        self.assertIsNone(runner._claim_error_label({"phase": PRIVATE}, HostileError()))

    def test_no_candidate_exit_failure_is_not_reported_as_no_work(self):
        for phase in ("transaction_exit", "connection_exit"):
            with self.subTest(phase=phase):
                result = self.after_work(
                    ClaimStore({phase: PrivateError(PRIVATE)}, candidate=False)
                )
                self.assertIn(f"retirement_claim_{phase}_other", result["errors"])

    def test_domain_primary_reason_and_legacy_patched_claim_are_preserved(self):
        candidate = self.candidate()
        for error in (
            ChunkRetirementError("retirement_owner_required"),
            PrivateError(PRIVATE),
        ):
            with (
                self.subTest(error=type(error).__name__),
                patch.object(runner, "_claim", side_effect=[candidate, error]),
                patch.object(
                    runner, "retire_parent_chunks", return_value={"complete": True}
                ),
            ):
                result = runner.run_retirement(
                    Mock(), Mock(), scope=self.scope(), apply=True
                )
            reason = (
                error.error_code
                if isinstance(error, ChunkRetirementError)
                else "retirement_claim_unavailable"
            )
            self.assertEqual(result["errors"], {reason: 1})
        store = ClaimStore(
            {"authorize": ChunkRetirementError("retirement_owner_required")}
        )
        with (
            patch.object(
                runner, "_authorize", side_effect=lambda *a: store.step("authorize")
            ),
            patch.object(
                runner, "retire_parent_chunks", return_value={"complete": True}
            ),
        ):
            claim = runner._claim
            with patch.object(
                runner,
                "_claim",
                side_effect=lambda *a, **kw: (
                    candidate if not kw["attempted"] else claim(*a, **kw)
                ),
            ):
                result = runner.run_retirement(
                    store, Mock(), scope=self.scope(), apply=True
                )
        self.assertEqual(
            result["errors"],
            {
                "retirement_owner_required": 1,
                "retirement_claim_authorize_chunk_retirement": 1,
            },
        )


if __name__ == "__main__":
    unittest.main()
