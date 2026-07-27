from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from evals.agentic_truth import (
    EvaluationInputError,
    build_owner_review_packet,
    score_boundary_candidates,
    validate_truth_set,
)


STRATA = (
    "exact-document",
    "bounded-timeline",
    "source-specific",
    "cross-source",
    "insufficient",
)


def private_directory(root: Path) -> Path:
    directory = root / "private"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    return directory


def private_write(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
    )
    path.chmod(0o600)


def truth_cases() -> list[dict]:
    cases: list[dict] = []
    for stratum_ordinal, stratum in enumerate(STRATA):
        for ordinal in range(12):
            split = (
                "optimize"
                if ordinal < 5
                else "validation"
                if ordinal < 8
                else "test"
            )
            case_ordinal = stratum_ordinal * 12 + ordinal
            answerable = stratum != "insufficient"
            boundary_count = 2 if stratum == "cross-source" else 1
            boundaries = []
            for boundary_ordinal in range(boundary_count if answerable else 0):
                identity = case_ordinal * 2 + boundary_ordinal
                source = f"synthetic:source:{boundary_ordinal}"
                receipt = (
                    f"recall://{source}/record-{identity}"
                    "?rev=1#item=0"
                )
                boundaries.append(
                    {
                        "logical_document_id": f"ldoc_{identity:032x}",
                        "source_id": source,
                        "revision": 1,
                        "receipts": [receipt],
                        "first_occurred_at": "2026-07-01T00:00:00Z",
                        "last_occurred_at": "2026-07-01T00:05:00Z",
                    }
                )
            facts = (
                [
                    {
                        "id": f"fact_{case_ordinal:032x}",
                        "description": (
                            f"Synthetic required fact for case {case_ordinal}."
                        ),
                        "receipts": [boundaries[0]["receipts"][0]],
                    }
                ]
                if answerable
                else []
            )
            cases.append(
                {
                    "id": f"case_{case_ordinal:032x}",
                    "split": split,
                    "stratum": stratum,
                    "question": (
                        f"What happened in synthetic boundary {case_ordinal}?"
                    ),
                    "answerability": (
                        "answerable" if answerable else "insufficient"
                    ),
                    "gold_boundaries": boundaries,
                    "gold_facts": facts,
                    "owner_review": {"status": "approved", "revision": 1},
                }
            )
    return cases


class AgenticTruthSetTest(unittest.TestCase):
    def test_validates_frozen_stratified_truth_without_rendering_content(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = private_directory(Path(temporary))
            truth = directory / "truth.jsonl"
            private_write(truth, truth_cases())

            receipt = validate_truth_set(truth)

        self.assertEqual(receipt["case_count"], 60)
        self.assertEqual(receipt["split_counts"], {
            "optimize": 25,
            "test": 20,
            "validation": 15,
        })
        self.assertEqual(receipt["stratum_counts"], {
            stratum: 12 for stratum in sorted(STRATA)
        })
        self.assertEqual(receipt["answerable_cases"], 48)
        self.assertEqual(receipt["insufficient_cases"], 12)
        self.assertEqual(receipt["owner_approved_cases"], 60)
        rendered = json.dumps(receipt)
        self.assertNotIn("Synthetic required fact", rendered)
        self.assertNotIn("record-", rendered)

    def test_rejects_boundaries_shared_across_splits(self) -> None:
        rows = truth_cases()
        optimize = next(
            row for row in rows
            if row["split"] == "optimize" and row["gold_boundaries"]
        )
        test = next(
            row for row in rows
            if row["split"] == "test" and row["gold_boundaries"]
        )
        optimize["gold_boundaries"][0]["logical_document_id"] = (
            test["gold_boundaries"][0]["logical_document_id"]
        )
        optimize["gold_boundaries"][0]["source_id"] = (
            test["gold_boundaries"][0]["source_id"]
        )
        optimize["gold_boundaries"][0]["revision"] = (
            test["gold_boundaries"][0]["revision"]
        )
        with tempfile.TemporaryDirectory() as temporary:
            directory = private_directory(Path(temporary))
            truth = directory / "truth.jsonl"
            private_write(truth, rows)

            with self.assertRaisesRegex(
                EvaluationInputError,
                "crosses evaluation splits",
            ):
                validate_truth_set(truth)

    def test_rejects_private_truth_stored_inside_repository(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            directory = private_directory(repo)
            truth = directory / "truth.jsonl"
            private_write(truth, truth_cases())

            with self.assertRaisesRegex(
                EvaluationInputError,
                "outside Git",
            ):
                validate_truth_set(truth, repo_root=repo)

    def test_rejects_unapproved_or_ungrounded_gold(self) -> None:
        rows = truth_cases()
        rows[0]["owner_review"]["status"] = "pending"
        with tempfile.TemporaryDirectory() as temporary:
            directory = private_directory(Path(temporary))
            truth = directory / "truth.jsonl"
            private_write(truth, rows)
            with self.assertRaisesRegex(EvaluationInputError, "owner-approved"):
                validate_truth_set(truth)

        rows = truth_cases()
        rows[0]["gold_facts"][0]["receipts"] = [
            "recall://synthetic:source:0/not-in-boundary?rev=1#item=0"
        ]
        with tempfile.TemporaryDirectory() as temporary:
            directory = private_directory(Path(temporary))
            truth = directory / "truth.jsonl"
            private_write(truth, rows)
            with self.assertRaisesRegex(EvaluationInputError, "gold receipt"):
                validate_truth_set(truth)

    def test_builds_private_static_owner_review_packet_from_pending_truth(
        self,
    ) -> None:
        rows = truth_cases()
        for row in rows:
            row["owner_review"]["status"] = "pending"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = private_directory(root)
            truth = directory / "truth.jsonl"
            packet = directory / "truth-review.html"
            repo = root / "repo"
            repo.mkdir()
            private_write(truth, rows)

            receipt = build_owner_review_packet(
                truth,
                packet,
                repo_root=repo,
            )
            rendered = packet.read_text()

            self.assertEqual(receipt["case_count"], 60)
            self.assertEqual(receipt["owner_approved_cases"], 0)
            self.assertEqual(receipt["owner_pending_cases"], 60)
            self.assertEqual(packet.stat().st_mode & 0o777, 0o600)
            self.assertEqual(rendered.count("<article>"), 60)
            self.assertIn(
                "What happened in synthetic boundary 0?",
                rendered,
            )
            self.assertNotIn("<script", rendered)

    def test_scores_boundary_recall_mrr_pointer_and_authorization(self) -> None:
        cases = truth_cases()
        results = []
        for case in cases:
            candidates = [
                {
                    "logical_document_id": boundary["logical_document_id"],
                    "source_id": boundary["source_id"],
                    "revision": boundary["revision"],
                    "pointer_valid": True,
                    "authorized": True,
                }
                for boundary in case["gold_boundaries"]
            ]
            results.append(
                {
                    "id": case["id"],
                    "candidates": candidates,
                    "latency_ms": 25.0,
                    "backend_error": "",
                }
            )

        report = score_boundary_candidates(cases, results)

        self.assertEqual(report["aggregate"]["boundary_recall@20"], 1.0)
        self.assertEqual(report["aggregate"]["boundary_mrr"], 1.0)
        self.assertEqual(report["aggregate"]["negative_false_hit_rate"], 0.0)
        self.assertEqual(report["aggregate"]["pointer_integrity"], 1.0)
        self.assertEqual(report["aggregate"]["authorization_violation_rate"], 0.0)
        self.assertEqual(report["aggregate"]["backend_error_rate"], 0.0)
        self.assertNotIn("cases", report)
        self.assertNotIn("question", json.dumps(report))


if __name__ == "__main__":
    unittest.main()
