from __future__ import annotations

import copy
import unittest

from evals.agentic_truth import (
    EvaluationInputError,
    _validate_cases,
    score_boundary_candidates,
)
from evals.expanded_truth import (
    SCHEMA_VERSION,
    canonical_sha256,
    score_expanded_boundary_candidates,
    validate_expanded_truth_set,
)
from test_agentic_truth import truth_cases


def added_case(ordinal: int = 1000) -> dict:
    case = copy.deepcopy(truth_cases()[0])
    case.update(
        id=f"case_{ordinal:032x}",
        split="validation",
        question=f"What happened in new synthetic boundary {ordinal}?",
    )
    boundary = case["gold_boundaries"][0]
    boundary["logical_document_id"] = f"ldoc_{ordinal:032x}"
    boundary["receipts"] = [
        f"recall://{boundary['source_id']}/record-{ordinal}?rev=1#item=0"
    ]
    case["gold_facts"][0].update(
        id=f"fact_{ordinal:032x}", receipts=boundary["receipts"][:]
    )
    return case


def families(cases: list[dict]) -> list[dict]:
    return [
        {
            "source_id": boundary["source_id"],
            "logical_document_id": boundary["logical_document_id"],
            "family_id": f"native-{ordinal}",
        }
        for ordinal, boundary in enumerate(
            boundary for case in cases for boundary in case["gold_boundaries"]
        )
    ]


def results_for(cases: list[dict]) -> list[dict]:
    return [
        {
            "id": case["id"],
            "latency_ms": ordinal + 0.5,
            "backend_error": "synthetic timeout" if ordinal == 2 else "",
            "candidates": [
                {
                    "source_id": boundary["source_id"],
                    "logical_document_id": boundary["logical_document_id"],
                    "revision": 2,
                    "pointer_valid": ordinal != 1,
                    "authorized": ordinal != 3,
                }
                for boundary in case["gold_boundaries"]
            ],
        }
        for ordinal, case in enumerate(cases)
    ]


class ExpandedTruthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.base = truth_cases()
        self.additions = [added_case()]
        self.base_pin = canonical_sha256(self.base)
        self.mapping = families(self.base + self.additions)

    def validate(self) -> dict:
        return validate_expanded_truth_set(
            self.base, self.additions, self.mapping, expected_base_sha256=self.base_pin
        )

    def test_legacy_exact_sixty_and_balanced_contracts_remain(self) -> None:
        with self.assertRaisesRegex(EvaluationInputError, "exactly 60"):
            _validate_cases(self.base + self.additions)
        self.base[0]["split"] = "validation"
        with self.assertRaisesRegex(EvaluationInputError, "split counts"):
            self.validate()

    def test_rejects_shape_valid_change_to_pinned_base(self) -> None:
        self.base[0]["question"] = "A different but schema-valid synthetic question?"
        with self.assertRaisesRegex(EvaluationInputError, "base digest mismatch"):
            self.validate()

    def test_accepts_expansion_and_preserves_base_and_negative_cases(self) -> None:
        before = copy.deepcopy((self.base, self.additions, self.mapping))
        report = self.validate()
        self.assertEqual(report["schema_version"], SCHEMA_VERSION)
        self.assertEqual(report["case_count"], 61)
        self.assertEqual(report["added_case_count"], 1)
        self.assertEqual(report["insufficient_cases"], 12)
        self.assertEqual(
            report["split_counts"], {"optimize": 25, "validation": 16, "test": 20}
        )
        self.assertEqual((self.base, self.additions, self.mapping), before)
        self.assertNotIn("new synthetic", str(report))
        self.assertNotIn("native-", str(report))

    def test_rejects_pending_test_split_and_insufficient_additions(self) -> None:
        for change, message in [
            ({"owner_review": {"status": "pending", "revision": 1}}, "owner-approved"),
            ({"split": "test"}, "validation-only"),
            (
                {
                    "answerability": "insufficient",
                    "stratum": "insufficient",
                    "gold_boundaries": [],
                    "gold_facts": [],
                },
                "answerable",
            ),
        ]:
            with self.subTest(change=change):
                self.additions = [added_case() | change]
                self.mapping = families(self.base + self.additions)
                with self.assertRaisesRegex(EvaluationInputError, message):
                    self.validate()

    def test_rejects_duplicate_case_question_and_boundary(self) -> None:
        for kind in ("case", "question", "boundary"):
            with self.subTest(kind=kind):
                self.additions = [added_case(), added_case(1001)]
                first, second = self.additions
                if kind == "case":
                    second["id"] = first["id"]
                elif kind == "question":
                    second["question"] = first["question"].upper()
                else:
                    second["gold_boundaries"] = copy.deepcopy(first["gold_boundaries"])
                    second["gold_facts"][0]["receipts"] = first["gold_facts"][0][
                        "receipts"
                    ][:]
                self.mapping = families(self.base + self.additions)
                with self.assertRaises(EvaluationInputError):
                    self.validate()

    def test_rejects_family_overlap_with_every_frozen_split(self) -> None:
        for split in ("optimize", "validation", "test"):
            with self.subTest(split=split):
                self.mapping = families(self.base + self.additions)
                boundary = next(case for case in self.base if case["split"] == split)[
                    "gold_boundaries"
                ][0]
                native = next(
                    row
                    for row in self.mapping
                    if row["logical_document_id"] == boundary["logical_document_id"]
                )
                self.mapping[-1]["family_id"] = native["family_id"]
                with self.assertRaisesRegex(EvaluationInputError, "frozen family"):
                    self.validate()

    def test_rejects_family_overlap_between_additions(self) -> None:
        self.additions.append(added_case(1001))
        self.mapping = families(self.base + self.additions)
        self.mapping[-1]["family_id"] = self.mapping[-2]["family_id"]
        with self.assertRaisesRegex(EvaluationInputError, "added cases"):
            self.validate()

    def test_allows_multiple_own_boundaries_in_one_native_family(self) -> None:
        other = added_case(1001)["gold_boundaries"][0]
        self.additions[0]["gold_boundaries"].append(other)
        self.mapping = families(self.base + self.additions)
        self.mapping[-1]["family_id"] = self.mapping[-2]["family_id"]
        self.assertEqual(self.validate()["added_case_count"], 1)

    def test_mapping_must_be_complete_unique_and_exact(self) -> None:
        good = copy.deepcopy(self.mapping)
        for mapping in (
            good[:-1],
            good + [good[0]],
            good
            + [
                {
                    "source_id": "extra",
                    "logical_document_id": "ldoc_" + "f" * 32,
                    "family_id": "extra",
                }
            ],
            good[:-1] + [good[-1] | {"family_id": " "}],
            good[:-1] + [good[-1] | {"extra": True}],
        ):
            with self.subTest(mapping=mapping[-1]):
                self.mapping = mapping
                with self.assertRaisesRegex(EvaluationInputError, "family mapping"):
                    self.validate()

    def test_identical_base_results_have_identical_metrics(self) -> None:
        mapping = families(self.base)
        results = results_for(self.base)
        # Include a negative false hit so parity exercises that metric too.
        results[-1]["candidates"] = copy.deepcopy(results[0]["candidates"])
        for split in (None, "validation", "test", "optimize"):
            with self.subTest(split=split):
                selected = (
                    results
                    if split is None
                    else [
                        row
                        for case, row in zip(self.base, results)
                        if case["split"] == split
                    ]
                )
                old = score_boundary_candidates(self.base, selected, split=split)
                new = score_expanded_boundary_candidates(
                    self.base,
                    [],
                    mapping,
                    selected,
                    expected_base_sha256=self.base_pin,
                    split=split,
                )
                self.assertEqual(new["schema_version"], SCHEMA_VERSION)
                new["schema_version"] = old["schema_version"]
                self.assertEqual(new, old)

    def test_expanded_validation_scoring_retains_frozen_negatives(self) -> None:
        cases = self.base + self.additions
        results = [
            row
            for case, row in zip(cases, results_for(cases))
            if case["split"] == "validation"
        ]
        report = score_expanded_boundary_candidates(
            self.base,
            self.additions,
            self.mapping,
            results,
            expected_base_sha256=self.base_pin,
            split="validation",
        )
        self.assertEqual(report["aggregate"]["queries"], 16)
        self.assertEqual(report["aggregate"]["insufficient_queries"], 3)
        self.assertEqual(report["evaluated_split"], "validation")
        with self.assertRaisesRegex(EvaluationInputError, "split is invalid"):
            score_expanded_boundary_candidates(
                self.base,
                self.additions,
                self.mapping,
                results,
                expected_base_sha256=self.base_pin,
                split="other",
            )

    def test_added_results_use_strict_candidate_contract(self) -> None:
        results = results_for(self.base + self.additions)
        candidate = results[-1]["candidates"][0]
        for candidates in (
            [candidate, candidate],
            [candidate | {"pointer_valid": 1}],
            [candidate | {"revision": True}],
            [candidate | {"extra": "ignored?"}],
        ):
            with self.subTest(candidates=candidates):
                results[-1]["candidates"] = candidates
                with self.assertRaises(EvaluationInputError):
                    score_expanded_boundary_candidates(
                        self.base,
                        self.additions,
                        self.mapping,
                        results,
                        expected_base_sha256=self.base_pin,
                    )

    def test_requires_all_results_including_negatives_and_checks_diagnostics(
        self,
    ) -> None:
        cases = self.base + self.additions
        results = results_for(cases)
        for broken in (
            results[:-1],
            [
                row
                for case, row in zip(cases, results)
                if case["answerability"] == "answerable"
            ],
            results[:-1] + [results[0]],
            results[:-1] + [results[-1] | {"latency_ms": float("nan")}],
            results[:-1] + [results[-1] | {"backend_error": 5}],
        ):
            with self.subTest(length=len(broken)):
                with self.assertRaises(EvaluationInputError):
                    score_expanded_boundary_candidates(
                        self.base,
                        self.additions,
                        self.mapping,
                        broken,
                        expected_base_sha256=self.base_pin,
                    )
        report = score_expanded_boundary_candidates(
            self.base,
            self.additions,
            self.mapping,
            results,
            expected_base_sha256=self.base_pin,
        )
        self.assertEqual(report["aggregate"]["queries"], 61)
        self.assertEqual(report["aggregate"]["insufficient_queries"], 12)


if __name__ == "__main__":
    unittest.main()
