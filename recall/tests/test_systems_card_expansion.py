"""Pinned private truth expansions preserve the original systems-card ruler."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from evals.expanded_truth import canonical_sha256
from evals.retrieval import EvaluationInputError
from evals.systems_card import accuracy, runner
from evals.systems_card.probes import timed
from evals.systems_card.truth import load_truth_expansion
from tests.test_agentic_truth import truth_cases
from tests.test_expanded_truth import added_case, families
from tests.test_systems_card import FakeBrain, default_tools, make_context


class CardExpansionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.base = truth_cases()
        self.additions = [added_case(1000 + i) for i in range(28)]
        self.mapping = families(self.base + self.additions)
        self.manifest = {
            "schema_version": "recall.systems-card.truth-expansion.v1",
            "base_canonical_sha256": canonical_sha256(self.base),
        }
        for name, rows in (("base", self.base), ("additions", self.additions), ("families", self.mapping)):
            self.artifact(name, rows)
        self.save_manifest()

    def artifact(self, name, rows):
        payload = "".join(json.dumps(row) + "\n" for row in rows).encode()
        path = self.root / f"{name}.jsonl"
        path.write_bytes(payload)
        path.chmod(0o600)
        self.manifest[name] = {"path": path.name, "sha256": hashlib.sha256(payload).hexdigest()}

    def save_manifest(self):
        self.path = self.root / "expansion.json"
        self.path.write_text(json.dumps(self.manifest))
        self.path.chmod(0o600)

    def brain(self, *, fail_id=None, miss_original=False):
        by_question = {c["question"]: c for c in self.base + self.additions}
        original_ids = {c["id"] for c in self.base}

        def search(arguments):
            case = by_question[arguments["query"]]
            if case["id"] == fail_id:
                return None
            gold = [] if miss_original and case["id"] in original_ids else case["gold_boundaries"]
            return {"results": [
                {"source_id": g["source_id"], "logical_document_id": g["logical_document_id"], "revision": 1, "matching_ranges": []}
                for g in gold
            ], "diagnostics": {}}

        tools = default_tools()
        tools["recall_search"] = search
        return FakeBrain(tools)

    def context(self, brain=None, **options):
        return make_context(
            brain or self.brain(), truth_expansion_path=str(self.path),
            truth_split="validation", accuracy_pointer_checks=0, **options,
        )

    def test_bad_pin_fails_before_any_card_network_or_profile_access(self):
        for field in ("base", "additions", "families", "base_canonical_sha256"):
            with self.subTest(field=field):
                saved = json.loads(json.dumps(self.manifest))
                if field == "base_canonical_sha256":
                    self.manifest[field] = "0" * 64
                else:
                    self.manifest[field]["sha256"] = "0" * 64
                self.save_manifest()
                args = runner.parser().parse_args(["run", "--output-dir", str(self.root / "out"), "--truth-expansion", str(self.path)])
                with mock.patch.object(runner, "load_profile") as profile:
                    with self.assertRaises(EvaluationInputError):
                        runner.run_card(args)
                profile.assert_not_called()
                self.manifest = saved

    def test_family_overlap_and_unapproved_rows_fail_before_probe_network(self):
        for invalid in ("family", "approval"):
            with self.subTest(invalid=invalid):
                if invalid == "family":
                    rows = json.loads(json.dumps(self.mapping))
                    rows[-1]["family_id"] = rows[0]["family_id"]
                    self.artifact("families", rows)
                else:
                    self.artifact("families", self.mapping)
                    rows = json.loads(json.dumps(self.additions))
                    rows[0]["owner_review"]["status"] = "pending"
                    self.artifact("additions", rows)
                self.save_manifest()
                brain = self.brain()
                result = timed(accuracy.TruthBoundaryProbe(), self.context(brain))
                self.assertEqual(result.status, "failed")
                self.assertEqual(brain.calls, [])

    def test_expanded_coverage_retains_negatives_errors_and_private_rows(self):
        brain = self.brain(fail_id=self.additions[0]["id"])
        context = self.context(brain)
        context.private_dir = str(self.root / "private-output")
        result = accuracy.TruthBoundaryProbe().run(context)
        self.assertEqual(len(brain.calls), 43)
        self.assertEqual(result.samples, 43)
        self.assertEqual(result.metrics["answerable_queries"], 40)
        self.assertEqual(result.metrics["insufficient_queries"], 3)
        self.assertEqual(result.metrics["original.answerable_queries"], 12)
        self.assertEqual(result.metrics["original.insufficient_queries"], 3)
        self.assertAlmostEqual(result.metrics["backend_error_rate"], round(1 / 43, 4))
        self.assertEqual(result.metrics["original.backend_error_rate"], 0)
        saved = list(Path(context.private_dir).glob("*.jsonl"))
        rows = list(map(json.loads, saved[0].read_text().splitlines()))
        self.assertEqual(len(rows), 43)
        self.assertEqual(sum(bool(r["backend_error"]) for r in rows), 1)
        self.assertEqual(saved[0].stat().st_mode & 0o777, 0o600)
        serialized = json.dumps(result.as_dict())
        self.assertNotIn(str(self.path), serialized)
        for case in self.base + self.additions:
            self.assertNotIn(case["question"], serialized)

    def test_original_panel_regression_fails_even_when_expansion_passes(self):
        result = accuracy.TruthBoundaryProbe().run(self.context(self.brain(miss_original=True)))
        gates = {g.metric: g for g in result.gates}
        self.assertTrue(gates["boundary_mrr"].passed)
        self.assertTrue(gates["boundary_recall@20"].passed)
        self.assertFalse(gates["original.boundary_mrr"].passed)
        self.assertFalse(gates["original.boundary_recall@20"].passed)
        self.assertEqual(result.status, "degraded")
        card = runner.build_card([result], base_url="https://brain.invalid/mcp", started_at=0, repo_root=self.root, options={})
        history = runner.history_row(card)
        self.assertEqual(history["accuracy.truth_boundary.original.boundary_mrr"], 0)

    def test_valid_cli_uses_preflight_snapshot_and_retains_both_panels(self):
        args = runner.parser().parse_args([
            "run", "--output-dir", str(self.root / "out"),
            "--truth-expansion", str(self.path), "--dimensions", "accuracy",
        ])
        client = self.context().client

        def profile(**kwargs):
            # Profile loading occurs only after preflight. Subsequent local edits
            # must not change this run's already validated questions or pins.
            (self.root / "additions.jsonl").write_text("invalid after preflight")
            return "https://brain.invalid/mcp", "synthetic-token"

        with mock.patch.object(runner, "load_profile", side_effect=profile), mock.patch.object(runner, "McpClient", return_value=client):
            card = runner.run_card(args)
        probe = card["dimensions"]["accuracy"]["probes"][0]
        self.assertEqual(probe["samples"], 43)
        self.assertEqual(probe["metrics"]["original.cases"], 15)
        self.assertEqual(len(probe["gates"]), 10)
        history = json.loads((self.root / "out" / "history.jsonl").read_text())
        self.assertEqual(history["accuracy.truth_boundary.original.boundary_mrr"], 1)

    def test_expansion_paths_cannot_escape_or_traverse_symlinks(self):
        for path in ("../base.jsonl", "link.jsonl"):
            with self.subTest(path=path):
                if path == "link.jsonl":
                    (self.root / path).symlink_to(self.root / "base.jsonl")
                self.manifest["base"]["path"] = path
                self.save_manifest()
                with self.assertRaises(EvaluationInputError):
                    load_truth_expansion(self.path)

    def test_legacy_behavior_matches_independent_original_panel(self):
        with mock.patch.object(accuracy.time, "monotonic", return_value=10):
            legacy = accuracy.TruthBoundaryProbe().run(make_context(self.brain(), truth_path=str(self.root / "base.jsonl"), truth_split="validation", accuracy_pointer_checks=0))
            expanded = accuracy.TruthBoundaryProbe().run(self.context())
        for key, value in legacy.metrics.items():
            if key in expanded.metrics and isinstance(value, (int, float)) and key not in ("cases", "candidate_depth", "receipt_resolution_checks") and not key.startswith("stratum."):
                self.assertEqual(expanded.metrics[f"original.{key}"], value)
        self.assertEqual(len(legacy.gates), 5)
        original_gates = [g for g in expanded.gates if g.metric.startswith("original.")]
        for old, new in zip(legacy.gates, original_gates, strict=True):
            self.assertEqual((old.metric, old.op, old.threshold, old.observed, old.passed), (new.metric.removeprefix("original."), new.op, new.threshold, new.observed, new.passed))

    def test_manifest_closed_private_schema_and_validation_only(self):
        for mutation in ("unknown", "absolute", "unsafe_mode", "missing", "schema"):
            with self.subTest(mutation=mutation):
                saved = json.loads(json.dumps(self.manifest))
                if mutation == "unknown":
                    self.manifest["surprise"] = "private input marker"
                if mutation == "absolute":
                    self.manifest["base"]["path"] = str(self.root / "base.jsonl")
                if mutation == "unsafe_mode":
                    (self.root / "base.jsonl").chmod(0o644)
                if mutation == "missing":
                    self.manifest["base"]["path"] = "private-input-marker.jsonl"
                if mutation == "schema":
                    self.manifest["schema_version"] = "unrecognized"
                self.save_manifest()
                brain = self.brain()
                with self.assertRaises(EvaluationInputError) as caught:
                    accuracy.TruthBoundaryProbe().run(self.context(brain))
                self.assertNotIn("private-input-marker", str(caught.exception))
                self.assertEqual(brain.calls, [])
                self.manifest = saved
                (self.root / "base.jsonl").chmod(0o600)
        self.save_manifest()
        args = runner.parser().parse_args(["run", "--output-dir", str(self.root / "out"), "--truth-expansion", str(self.path), "--truth-split", "test"])
        with mock.patch.object(runner, "load_profile") as profile:
            with self.assertRaises(EvaluationInputError):
                runner.run_card(args)
        profile.assert_not_called()


if __name__ == "__main__":
    unittest.main()
