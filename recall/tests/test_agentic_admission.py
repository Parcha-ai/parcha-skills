from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from evals.agentic_admission import (
    score_admission,
    write_candidate_cards,
    write_control_selections,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
STRATA = (
    "exact-document",
    "bounded-timeline",
    "source-specific",
    "cross-source",
    "insufficient",
)
INTENTS = (
    "project-status",
    "decision-rationale",
    "change-history",
    "incident-root-cause",
    "ownership-next-step",
)


def private_write(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    )
    path.chmod(0o600)


def truth_cases() -> list[dict]:
    cases = []
    for stratum_index, stratum in enumerate(STRATA):
        for ordinal in range(12):
            split = (
                "optimize"
                if ordinal < 5
                else "validation"
                if ordinal < 8
                else "test"
            )
            index = stratum_index * 12 + ordinal
            answerable = stratum != "insufficient"
            boundary_count = 2 if stratum == "cross-source" else 1
            boundaries = []
            for boundary_index in range(
                boundary_count if answerable else 0
            ):
                identity = index * 2 + boundary_index
                source = f"synthetic:source:{boundary_index}"
                receipt = (
                    f"recall://{source}/record-{identity}?rev=1#item=0"
                )
                boundaries.append({
                    "logical_document_id": f"ldoc_{identity:032x}",
                    "source_id": source,
                    "revision": 1,
                    "receipts": [receipt],
                    "first_occurred_at": "2026-07-01T00:00:00Z",
                    "last_occurred_at": "2026-07-01T00:05:00Z",
                })
            cases.append({
                "id": f"case_{index:032x}",
                "split": split,
                "stratum": stratum,
                "intent": INTENTS[index % len(INTENTS)],
                "question": f"What happened in synthetic boundary {index}?",
                "answerability": (
                    "answerable" if answerable else "insufficient"
                ),
                "gold_boundaries": boundaries,
                "gold_facts": (
                    [{
                        "id": f"fact_{index:032x}",
                        "description": f"Synthetic fact for case {index}.",
                        "receipts": [boundaries[0]["receipts"][0]],
                    }]
                    if answerable
                    else []
                ),
                "owner_review": {"status": "approved", "revision": 1},
            })
    return cases


def candidate(
    source_id: str,
    document_id: str,
    *,
    rank: int = 1,
) -> dict:
    return {
        "logical_document_id": document_id,
        "source_id": source_id,
        "revision": 1,
        "pointer_valid": True,
        "authorized": True,
    }


def card(
    source_id: str,
    document_id: str,
    *,
    rank: int = 1,
) -> dict:
    return {
        "logical_document_id": document_id,
        "source_id": source_id,
        "source_family": "coding_history",
        "first_occurred_at": "2026-07-01T00:00:00Z",
        "last_occurred_at": "2026-07-01T00:05:00Z",
        "snippets": ["Synthetic bounded pointer."],
        "provenance": [{
            "query_ordinal": 0,
            "arm": "dense",
            "rank": rank,
        }],
        "pointer_valid": True,
        "authorized": True,
    }


class AgenticAdmissionEvaluationTest(unittest.TestCase):
    def test_card_freeze_and_control_are_private_and_truth_blind(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "private"
            private.mkdir(mode=0o700)
            questions = private / "questions.jsonl"
            matrix = private / "matrix.jsonl"
            cards = private / "cards.jsonl"
            control = private / "control.jsonl"
            case_id = "case_" + "1" * 32
            marker = "PRIVATE SYNTHETIC QUESTION"
            source = "codex:linux:synthetic"
            document = "ldoc_" + "2" * 32
            private_write(
                questions,
                [{"id": case_id, "question": marker}],
            )
            arm_candidates = [candidate(source, document)]
            private_write(matrix, [{
                "id": case_id,
                "queries": [{
                    "ordinal": 0,
                    "arms": {"dense": arm_candidates},
                    "statuses": {"dense": "ok"},
                    "latency_ms": 1.0,
                }],
                "bundle_rankings": {"dense": arm_candidates},
            }])

            def describe(identities):
                return {
                    identity: {
                        "source_id": identity[0],
                        "logical_document_id": identity[1],
                        "source_family": "coding_history",
                        "first_occurred_at": "2026-07-01T00:00:00Z",
                        "last_occurred_at": "2026-07-01T00:05:00Z",
                        "snippets": ["Synthetic bounded pointer."],
                        "pointer_valid": True,
                        "authorized": True,
                    }
                    for identity in identities
                }

            report = write_candidate_cards(
                questions,
                matrix,
                cards,
                describe=describe,
                repo_root=REPO_ROOT,
                run_id="synthetic-card-freeze",
                expected_cases=1,
            )
            control_report = write_control_selections(
                cards,
                control,
                repo_root=REPO_ROOT,
                run_id="synthetic-control",
                expected_cases=1,
            )

            self.assertEqual(report["truth_derived_field_count"], 0)
            self.assertEqual(report["full_document_field_count"], 0)
            self.assertEqual(report["candidate_count"], 1)
            self.assertEqual(control_report["selected_count"], 1)
            self.assertEqual(os.stat(cards).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(control).st_mode & 0o777, 0o600)
            rendered = cards.read_text()
            self.assertIn(marker, rendered)
            self.assertNotIn('"gold"', rendered)
            self.assertNotIn('"truth"', rendered)
            self.assertNotIn('"full_document"', rendered)

    def test_unknown_descriptor_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "private"
            private.mkdir(mode=0o700)
            questions = private / "questions.jsonl"
            matrix = private / "matrix.jsonl"
            case_id = "case_" + "1" * 32
            source = "codex:linux:synthetic"
            document = "ldoc_" + "2" * 32
            private_write(
                questions,
                [{"id": case_id, "question": "Question"}],
            )
            arm_candidates = [candidate(source, document)]
            private_write(matrix, [{
                "id": case_id,
                "queries": [{
                    "ordinal": 0,
                    "arms": {"dense": arm_candidates},
                    "statuses": {"dense": "ok"},
                    "latency_ms": 1.0,
                }],
                "bundle_rankings": {"dense": arm_candidates},
            }])

            with self.assertRaisesRegex(
                ValueError,
                "descriptor is invalid",
            ):
                write_candidate_cards(
                    questions,
                    matrix,
                    private / "cards.jsonl",
                    describe=lambda _identities: {
                        (source, document): {
                            **{
                                key: value
                                for key, value in card(
                                    source,
                                    document,
                                ).items()
                                if key != "provenance"
                            },
                            "gold": True,
                        }
                    },
                    repo_root=REPO_ROOT,
                    run_id="synthetic-invalid-card",
                    expected_cases=1,
                )

    def test_offline_score_is_replayable_and_duplicates_cannot_inflate(
        self,
    ) -> None:
        cases = truth_cases()
        optimize = [case for case in cases if case["split"] == "optimize"]
        card_rows = []
        selections = []
        for case in optimize:
            cards = [
                card(
                    boundary["source_id"],
                    boundary["logical_document_id"],
                    rank=index + 1,
                )
                for index, boundary in enumerate(case["gold_boundaries"])
            ]
            if not cards:
                cards = [
                    card(
                        "synthetic:source:0",
                        "ldoc_" + f"{10_000 + len(card_rows):032x}",
                    )
                ]
            card_rows.append({
                "id": case["id"],
                "question": case["question"],
                "scope": {
                    "source_families": [],
                    "since": None,
                    "until": None,
                },
                "cards": cards,
            })
            selected = (
                [
                    {
                        "source_id": boundary["source_id"],
                        "logical_document_id": boundary[
                            "logical_document_id"
                        ],
                    }
                    for boundary in case["gold_boundaries"]
                ]
                if case["answerability"] == "answerable"
                else []
            )
            selections.append({
                "id": case["id"],
                "status": "ok",
                "selected": selected,
                "latency_ms": 2.0,
                "stages": [{
                    "stage": "final",
                    "input_count": len(cards),
                    "selected_count": len(selected),
                }],
                "error": None,
            })

        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "private"
            private.mkdir(mode=0o700)
            truth = private / "truth.jsonl"
            cards_path = private / "cards.jsonl"
            selected = private / "selected.jsonl"
            score1 = private / "score1.json"
            score2 = private / "score2.json"
            private_write(truth, cases)
            private_write(cards_path, card_rows)
            private_write(selected, selections)
            first = score_admission(
                truth,
                cards_path,
                selected,
                score1,
                repo_root=REPO_ROOT,
                run_id="synthetic-score-1",
                split="optimize",
            )
            second = score_admission(
                truth,
                cards_path,
                selected,
                score2,
                repo_root=REPO_ROOT,
                run_id="synthetic-score-2",
                split="optimize",
            )

            self.assertEqual(
                first["analysis_sha256"],
                second["analysis_sha256"],
            )
            self.assertEqual(first["aggregate"]["natural_recall@8"], 1.0)
            self.assertEqual(
                first["aggregate"]["pool_conditioned_recall@8"],
                1.0,
            )
            self.assertEqual(first["aggregate"]["selected_precision"], 1.0)

            duplicated = [dict(row) for row in selections]
            duplicated[0] = {
                **duplicated[0],
                "selected": [
                    duplicated[0]["selected"][0],
                    duplicated[0]["selected"][0],
                ],
                "stages": [{
                    "stage": "final",
                    "input_count": 2,
                    "selected_count": 2,
                }],
            }
            duplicate_path = private / "duplicates.jsonl"
            private_write(duplicate_path, duplicated)
            with self.assertRaisesRegex(
                ValueError,
                "selected identity is invalid",
            ):
                score_admission(
                    truth,
                    cards_path,
                    duplicate_path,
                    private / "duplicate-score.json",
                    repo_root=REPO_ROOT,
                    run_id="synthetic-duplicate",
                    split="optimize",
                )

    def test_missing_selection_case_fails_closed(self) -> None:
        cases = truth_cases()
        optimize = [case for case in cases if case["split"] == "optimize"]
        card_rows = []
        for index, case in enumerate(optimize):
            card_rows.append({
                "id": case["id"],
                "question": case["question"],
                "scope": {
                    "source_families": [],
                    "since": None,
                    "until": None,
                },
                "cards": [
                    card(
                        "synthetic:source:0",
                        "ldoc_" + f"{20_000 + index:032x}",
                    )
                ],
            })
        selections = [{
            "id": row["id"],
            "status": "ok",
            "selected": [],
            "latency_ms": 1.0,
            "stages": [],
            "error": None,
        } for row in card_rows[:-1]]

        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "private"
            private.mkdir(mode=0o700)
            truth = private / "truth.jsonl"
            cards_path = private / "cards.jsonl"
            selected = private / "selected.jsonl"
            private_write(truth, cases)
            private_write(cards_path, card_rows)
            private_write(selected, selections)
            with self.assertRaisesRegex(
                ValueError,
                "case count is invalid",
            ):
                score_admission(
                    truth,
                    cards_path,
                    selected,
                    private / "score.json",
                    repo_root=REPO_ROOT,
                    run_id="synthetic-missing-case",
                    split="optimize",
                )


if __name__ == "__main__":
    unittest.main()
