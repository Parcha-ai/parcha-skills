"""Explicit enrollment and bounded enabled-only retirement execution."""

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
from recall_server.parent_chunk_proof import parent_retirement_plan


class RunnerTests(unittest.TestCase):
    def scope(self):
        return runner.RetirementScope("tenant", "principal", ("source",))

    def test_scope_and_budget_refuse_wildcards_and_unbounded_work(self):
        for sources in ((), ("*",), ("source", "source"), ["source"], ({},)):
            with self.assertRaises(ChunkRetirementError):
                runner.RetirementScope("tenant", "principal", sources)
        for values in (
            {"max_parents": 0},
            {"max_archive_bytes": True},
            {"cooldown_seconds": -1},
        ):
            with self.assertRaises(ChunkRetirementError):
                runner.RunnerLimits(**values)

    def test_shared_plan_ignores_non_authority_columns(self):
        manifest = dict(
            tenant_id="tenant",
            source_id="source",
            native_parent_id="parent",
            manifest_artifact_id="artifact",
            revision=1,
        )
        first = parent_retirement_plan(("tenant", "source", "parent"), manifest)
        self.assertEqual(
            first,
            parent_retirement_plan(
                ("tenant", "source", "parent"), dict(manifest, updated_at="later")
            ),
        )
        self.assertNotEqual(
            first,
            parent_retirement_plan(
                ("tenant", "source", "parent"), dict(manifest, revision=2)
            ),
        )

    def test_dry_run_never_claims_or_reads_archive(self):
        store, archive = Mock(), Mock()
        with (
            patch.object(
                runner, "inspect_enabled", return_value={"eligible_parents": 2}
            ),
            patch.object(runner, "_claim") as claim,
        ):
            result = runner.run_retirement(store, archive, scope=self.scope())
        self.assertEqual(result["status"], "dry_run")
        claim.assert_not_called()
        archive.read_raw.assert_not_called()

    def candidate(self, name="parent", epoch=2):
        return dict(
            source_id="source",
            native_parent_id=name,
            scope_epoch=epoch,
            manifest=dict(
                tenant_id="tenant",
                source_id="source",
                native_parent_id=name,
                manifest_artifact_id=name,
                revision=1,
            ),
        )

    def test_runner_single_proof_and_claim_epoch(self):
        result = dict(
            complete=True,
            status="applied",
            batches=1,
            cleared_documents=1,
            cleared_chunks=1,
            cleared_utf8_bytes=10,
            excluded={},
        )
        with (
            patch.object(runner, "_claim", side_effect=[self.candidate(), None]),
            patch.object(runner, "retire_parent_chunks", return_value=result) as retire,
        ):
            report = runner.run_retirement(
                Mock(), Mock(), scope=self.scope(), apply=True
            )
        self.assertEqual(report["cleared_utf8_bytes"], 10)
        self.assertEqual(retire.call_count, 1)
        self.assertTrue(retire.call_args.kwargs["apply"])
        self.assertEqual(retire.call_args.kwargs["required_scope_epoch"], 2)
        self.assertEqual(retire.call_args.kwargs["owner_principal_id"], "principal")

    def test_stop_before_claim_preserves_all_work(self):
        with patch.object(runner, "_claim") as claim:
            report = runner.run_retirement(
                Mock(), Mock(), scope=self.scope(), apply=True, should_stop=lambda: True
            )
        claim.assert_not_called()
        self.assertEqual(report["status"], "stopped")

    def test_failure_rotates_but_unknown_commit_stops(self):
        bad = ChunkRetirementError("parent_retirement_archive_proof_required")
        bad.committed = {}
        good = dict(
            complete=True,
            batches=1,
            cleared_documents=1,
            cleared_chunks=1,
            cleared_utf8_bytes=9,
            excluded={},
        )
        with (
            patch.object(
                runner,
                "_claim",
                side_effect=[self.candidate("bad"), self.candidate("good"), None],
            ),
            patch.object(runner, "retire_parent_chunks", side_effect=[bad, good]),
        ):
            result = runner.run_retirement(
                Mock(), Mock(), scope=self.scope(), apply=True
            )
        self.assertEqual(result["failed_parents"], 1)
        self.assertEqual(result["cleared_utf8_bytes"], 9)
        bad = ChunkRetirementError("parent_retirement_unavailable")
        bad.committed = {"cleared_utf8_bytes": 2}
        with (
            patch.object(runner, "_claim", return_value=self.candidate()) as claim,
            patch.object(runner, "retire_parent_chunks", side_effect=bad),
        ):
            result = runner.run_retirement(
                Mock(), Mock(), scope=self.scope(), apply=True
            )
        self.assertEqual(claim.call_count, 1)
        self.assertTrue(result["commit_outcome_unknown"])

    def test_archive_failed_reads_charge_budget(self):
        archive = Mock()
        archive.read_raw.side_effect = ValueError("transport")
        meter = runner.RunArchive(archive, max_bytes=10)
        with self.assertRaises(ValueError):
            meter.read_raw({"size_bytes": 6})
        with self.assertRaises(ChunkRetirementError):
            meter.read_raw({"size_bytes": 5})
        self.assertEqual((meter.gets, meter.bytes), (1, 6))
        self.assertEqual(archive.read_raw.call_count, 1)

    def test_expired_already_empty_prefix_stops_before_another_commit(self):
        import os
        from contextlib import contextmanager
        from unittest.mock import MagicMock
        from recall_server import chunk_retirement as retirement

        clock = [100.0]
        store = MagicMock()
        store._execute_bounded.return_value.fetchone.side_effect = [
            {"version": 69},
            {
                "enabled": True,
                "scope_epoch": 2,
                "last_record_ordinal": 10,
                "manifest_artifact_id": "artifact",
            },
        ]

        def prefixes():
            yield {"body_record_ordinal": 0, "pg_body_bytes": 0}
            clock[0] = 111.0
            yield {"body_record_ordinal": 1, "pg_body_bytes": 0}

        spool = Mock()
        spool.verified.side_effect = prefixes

        @contextmanager
        def proof(*args, **kwargs):
            yield {
                "spool": spool,
                "manifest": {"manifest_artifact_id": "artifact"},
                "plan": {"proof_sha256": "proof"},
            }

        with (
            patch.dict(os.environ, RECALL_CHUNK_BODY_READS="archive"),
            patch("recall_server.parent_chunk_proof.prove_parent_chunks", proof),
            patch.object(retirement.time, "monotonic", side_effect=lambda: clock[0]),
            patch.object(retirement, "_retire_parent_batch") as commit,
        ):
            with self.assertRaises(ChunkRetirementError) as failed:
                retirement.retire_parent_chunks(
                    store,
                    Mock(),
                    tenant_id="tenant",
                    source_id="source",
                    native_parent_id="parent",
                    apply=True,
                    reviewed_plan={"proof_sha256": "proof"},
                    deadline_at=110.0,
                )
        self.assertEqual(
            failed.exception.error_code, "parent_retirement_deadline_exceeded"
        )
        self.assertEqual(failed.exception.committed["batches"], 0)
        commit.assert_not_called()


class RunnerCliTests(unittest.TestCase):
    def module(self):
        import importlib.util

        path = Path(__file__).resolve().parents[2] / "scripts/run_chunk_retirement.py"
        spec = importlib.util.spec_from_file_location("runner_cli", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_default_run_does_not_build_archive_or_enable(self):
        import os
        from contextlib import redirect_stdout
        from io import StringIO

        module = self.module()
        with (
            patch.dict(os.environ, RECALL_DATABASE_URL="postgresql://synthetic"),
            patch.object(module, "BrainStore"),
            patch.object(module, "build_evidence_archive_store") as archive,
            patch.object(module, "enroll_cohort") as enroll,
            patch.object(
                module, "run_retirement", return_value={"status": "dry_run"}
            ) as run,
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(
                module.main(
                    [
                        "run",
                        "--tenant-id",
                        "tenant",
                        "--principal-id",
                        "principal",
                        "--source-id",
                        "source",
                    ]
                ),
                0,
            )
        self.assertFalse(run.call_args.kwargs["apply"])
        archive.assert_not_called()
        enroll.assert_not_called()

    def test_enrollment_output_never_exposes_identity_and_plan_is_private(self):
        import os
        import tempfile
        from contextlib import redirect_stdout
        from io import StringIO

        module = self.module()
        output = StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            with (
                patch.dict(os.environ, RECALL_DATABASE_URL="postgresql://synthetic"),
                patch.object(module, "BrainStore"),
                patch.object(
                    module,
                    "plan_cohort",
                    return_value={
                        "parents": [{"native_parent_id": "private-name"}],
                        "more": False,
                        "proof_sha256": "f" * 64,
                    },
                ),
                redirect_stdout(output),
            ):
                self.assertEqual(
                    module.main(
                        [
                            "enroll",
                            "--tenant-id",
                            "tenant",
                            "--principal-id",
                            "principal",
                            "--source-id",
                            "source",
                            "--plan-file",
                            str(path),
                        ]
                    ),
                    0,
                )
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("private-name", output.getvalue())

    def test_later_claim_failure_exits_nonzero_with_committed_counts(self):
        import os
        from contextlib import redirect_stdout
        from io import StringIO

        module = self.module()
        output = StringIO()
        result = {
            "status": "claim_refused",
            "failed_parents": 0,
            "errors": {"retirement_owner_required": 1},
            "cleared_documents": 3,
            "commit_outcome_unknown": False,
        }
        with (
            patch.dict(os.environ, RECALL_DATABASE_URL="postgresql://synthetic"),
            patch.object(module, "BrainStore"),
            patch.object(module, "build_evidence_archive_store"),
            patch.object(module, "run_retirement", return_value=result),
            redirect_stdout(output),
        ):
            self.assertEqual(
                module.main(
                    [
                        "run",
                        "--apply",
                        "--tenant-id",
                        "tenant",
                        "--principal-id",
                        "principal",
                        "--source-id",
                        "source",
                    ]
                ),
                1,
            )
        self.assertIn('"cleared_documents": 3', output.getvalue())

    def test_timeout_refuses_before_database(self):
        from contextlib import redirect_stderr
        from io import StringIO

        module = self.module()
        with patch.object(module, "BrainStore") as store, redirect_stderr(StringIO()):
            self.assertEqual(
                module.main(
                    [
                        "run",
                        "--tenant-id",
                        "tenant",
                        "--principal-id",
                        "principal",
                        "--source-id",
                        "source",
                        "--timeout-seconds",
                        "nan",
                    ]
                ),
                1,
            )
        store.assert_not_called()


if __name__ == "__main__":
    unittest.main()
