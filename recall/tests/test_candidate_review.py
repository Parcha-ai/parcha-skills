"""Candidate calibration cannot silently turn missing evidence into negatives."""
import copy
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from evals import candidate_review
from evals.candidate_review import evidence_digest, score_reviewed_candidates
from evals.retrieval import EvaluationInputError


def fixture():
    evidence = [
        {"id": f"candidate-{i}", "case_id": "question-1", "question": "Was it shipped?",
         "source_id": "codex:test", "logical_document_id": f"ldoc_{i:030x}",
         "text": "It was shipped." if i == 0 else "It was only proposed.",
         "context": {"source_time": "2026-09-17T00:00:00Z"},
         "receipts": [f"recall://codex:test/event-{i}?rev=1#item=0"],
         "families": [f"family-{i}"], "complete": True}
        for i in range(3)
    ]
    reviews = [
        {"id": e["id"], "evidence_sha256": evidence_digest(e),
         "reviewer": "delegated reviewer", "label": label, "rationale": "Exact supplied evidence reviewed.",
         "witnesses": [{"start": 0, "end": len(e["text"]), "quote": e["text"], "receipt": e["receipts"][0]}]}
        for e, label in zip(evidence, ["answers_query", "does_not_answer", "insufficient_evidence"])
    ]
    predictions = [{"id": e["id"], "evidence_sha256": evidence_digest(e), "probability": p, "error": None}
                   for e, p in zip(evidence, [.9, .2, .8])]
    return evidence, reviews, predictions


class CandidateReviewTest(unittest.TestCase):
    def score(self, evidence, reviews, predictions, **kwargs):
        return score_reviewed_candidates(evidence, reviews, predictions,
                                         expected_evidence_sha256=evidence_digest(evidence),
                                         protected_families=kwargs.get("protected_families", []))

    def test_known_arithmetic_and_unknowns_are_separate(self):
        report = self.score(*fixture())
        self.assertEqual(report["coverage"]["candidates"], 3)
        self.assertEqual(report["coverage"]["ambiguous_reviews"], 1)
        self.assertEqual(report["coverage"]["scored_predictions"], 2)
        self.assertEqual(report["diagnostic_at_0_5"], {"true_positive": 1, "true_negative": 1, "false_positive": 0, "false_negative": 0})
        self.assertAlmostEqual(report["brier_available"], .025)
        self.assertNotIn("Was it shipped", str(report))
        self.assertNotIn("It was shipped", str(report))
        self.assertFalse(report["production_threshold_earned"])

    def test_frozen_evidence_pin_is_required(self):
        e, r, p = fixture()
        with self.assertRaises(EvaluationInputError):
            score_reviewed_candidates(e, r, p, expected_evidence_sha256="0" * 64, protected_families=[])

    def test_question_identity_and_source_edits_invalidate_review(self):
        for field in ("question", "source_id", "text", "families"):
            e, r, p = fixture()
            e[0][field] = ["changed"] if field == "families" else "changed"
            with self.subTest(field=field), self.assertRaises(EvaluationInputError):
                self.score(e, r, p)

    def test_foreign_receipt_and_wrong_quote_offsets_fail(self):
        for field, value in (("receipt", "recall://codex:test/foreign?rev=1#item=0"), ("quote", "Made up"), ("start", -1), ("end", True)):
            e, r, p = fixture()
            r[0]["witnesses"][0][field] = value
            with self.subTest(field=field), self.assertRaises(EvaluationInputError):
                self.score(e, r, p)

    def test_review_requires_reviewer_and_positive_quote(self):
        for field, value in (("reviewer", ""), ("witnesses", []), ("label", "auto-approved")):
            e, r, p = fixture()
            r[0][field] = value
            with self.subTest(field=field), self.assertRaises(EvaluationInputError):
                self.score(e, r, p)

    def test_duplicate_or_unknown_rows_fail_instead_of_changing_denominators(self):
        for index in range(3):
            rows = list(fixture())
            rows[index].append(copy.deepcopy(rows[index][0]))
            with self.subTest(index=index), self.assertRaises(EvaluationInputError):
                self.score(*rows)
        e, r, p = fixture()
        p[0]["id"] = "not-in-pool"
        with self.assertRaises(EvaluationInputError):
            self.score(e, r, p)

    def test_missing_review_and_failed_or_missing_predictions_remain_counted(self):
        e, r, p = fixture()
        r.pop()
        p[0].update(probability=None, error="timeout")
        p.pop(1)
        report = self.score(e, r, p)
        self.assertEqual(report["coverage"]["candidates"], 3)
        self.assertEqual(report["coverage"]["unreviewed_candidates"], 1)
        self.assertEqual(report["coverage"]["prediction_errors"], 1)
        self.assertEqual(report["coverage"]["missing_predictions"], 1)
        self.assertEqual(report["coverage"]["scored_predictions"], 0)
        self.assertIsNone(report["brier_available"])

    def test_protected_unresolved_and_incomplete_evidence_are_withheld(self):
        e, r, p = fixture()
        e[1]["families"] = []
        e[2]["complete"] = False
        for row, review, prediction in zip(e, r, p):
            review["evidence_sha256"] = prediction["evidence_sha256"] = evidence_digest(row)
        report = self.score(e, r, p, protected_families=["family-0"])
        self.assertEqual(report["coverage"]["protected_candidates"], 1)
        self.assertEqual(report["coverage"]["unresolved_family_candidates"], 1)
        self.assertEqual(report["coverage"]["incomplete_evidence_candidates"], 1)
        self.assertEqual(report["coverage"]["eligible_candidates"], 0)
        self.assertIsNone(report["brier_available"])

    def test_invalid_probability_and_error_shapes_fail(self):
        for value in (True, float("nan"), float("inf"), -0.1, 1.1, "0.9", None, 10 ** 1000):
            e, r, p = fixture()
            p[0]["probability"] = value
            with self.subTest(value=value), self.assertRaises(EvaluationInputError):
                self.score(e, r, p)
        e, r, p = fixture()
        p[0]["error"] = "timeout"
        with self.assertRaises(EvaluationInputError):
            self.score(e, r, p)

    def test_prediction_cannot_be_reused_for_changed_evidence(self):
        e, r, p = fixture()
        p[0]["evidence_sha256"] = "0" * 64
        with self.assertRaises(EvaluationInputError):
            self.score(e, r, p)

    def test_changed_temporal_context_invalidates_review(self):
        e, r, p = fixture()
        e[0]["context"]["source_time"] = "2026-01-01T00:00:00Z"
        with self.assertRaises(EvaluationInputError):
            self.score(e, r, p)

    def test_negative_label_is_only_about_supplied_evidence(self):
        report = self.score(*fixture())
        self.assertEqual(report["label_scope"], "supplied_selected_passages")
        self.assertFalse(report["whole_document_relevance_claimed"])

    def test_private_cli_emits_only_report_and_rejects_unsafe_input(self):
        e, r, p = fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = ["candidate_review", "--expected-evidence-sha256", evidence_digest(e)]
            for name, rows in (("evidence", e), ("reviews", r), ("predictions", p), ("protected-families", [])):
                path = root / (name + ".jsonl")
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                path.chmod(0o600)
                args.extend(["--" + name, str(path)])
            output = io.StringIO()
            with mock.patch("sys.argv", args), contextlib.redirect_stdout(output):
                candidate_review.main()
            self.assertEqual(json.loads(output.getvalue())["coverage"]["candidates"], 3)
            self.assertNotIn(str(root), output.getvalue())
            (root / "evidence.jsonl").chmod(0o644)
            output, errors = io.StringIO(), io.StringIO()
            with mock.patch("sys.argv", args), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                with self.assertRaises(SystemExit) as stopped:
                    candidate_review.main()
            self.assertEqual(stopped.exception.code, 2)
            self.assertEqual(output.getvalue(), "")
            self.assertNotIn(str(root), errors.getvalue())
            self.assertNotIn("It was shipped", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
