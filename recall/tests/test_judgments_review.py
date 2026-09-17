from __future__ import annotations

import copy
import hashlib
import json
import math
import stat
import tempfile
import unittest
from pathlib import Path

from evals.judgments_review import prepare_review
from evals.retrieval import EvaluationInputError
from tests.test_agentic_truth import private_directory, private_write, truth_cases


def candidate(case: dict) -> dict:
    boundary = case["gold_boundaries"][0]
    return {
        **{key: boundary[key] for key in ("source_id", "logical_document_id", "revision")},
        "authorized": True,
        "pointer_valid": True,
    }


def draft_case(ordinal: int = 1000) -> dict:
    case = copy.deepcopy(truth_cases()[5])
    case["id"] = f"case_{ordinal:032x}"
    case["question"] = f"What caused synthetic incident {ordinal}?"
    case["owner_review"] = {"status": "pending", "revision": 1}
    boundary = case["gold_boundaries"][0]
    boundary["logical_document_id"] = f"ldoc_{ordinal:032x}"
    boundary["receipts"] = [f"recall://synthetic:source:0/record-{ordinal}?rev=1#item=0"]
    case["gold_facts"][0]["receipts"] = boundary["receipts"]
    return case


class JudgmentsReviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.private = private_directory(self.root)
        self.truth = self.private / "truth.jsonl"
        self.output = self.private / "review.html"
        self.cases = truth_cases()
        self.validation = self.cases[5]
        self.test = self.cases[8]
        private_write(self.truth, self.cases)

    def write(self, name: str, rows: list[dict]) -> Path:
        path = self.private / name
        private_write(path, rows)
        return path

    def prepare(self, results: list[Path] | None = None, **kwargs) -> dict:
        return prepare_review(
            self.truth, results or [], self.output, repo_root=self.repo, **kwargs,
        )

    def family_map(self, *extra: dict) -> Path:
        boundaries = {
            (b["source_id"], b["logical_document_id"]): b
            for case in [*self.cases, *extra] for b in case["gold_boundaries"]
        }
        return self.write("complete-families.jsonl", [
            {"source_id": key[0], "logical_document_id": key[1], "family_id": key[1]}
            for key in boundaries
        ])

    def test_pools_known_gold_and_candidates_without_changing_truth_or_exposing_test(self) -> None:
        before = hashlib.sha256(self.truth.read_bytes()).hexdigest()
        retrieved = candidate(draft_case())
        probes = self.write("probes.jsonl", [
            {"id": self.validation["id"], "candidates": [retrieved, candidate(self.test)]},
            {"id": self.test["id"], "candidates": [candidate(self.test)]},
        ])
        receipt = self.prepare([probes])
        rendered = self.output.read_text()
        self.assertIn(self.validation["question"], rendered)
        self.assertIn(self.validation["gold_facts"][0]["description"], rendered)
        self.assertIn(retrieved["logical_document_id"], rendered)
        self.assertNotIn(self.test["question"], rendered)
        self.assertNotIn(self.test["gold_boundaries"][0]["logical_document_id"], rendered)
        self.assertEqual(receipt["withheld_candidates"], 1)
        self.assertEqual(receipt["approved_answerable_questions"], 12)
        self.assertEqual(receipt["approved_question_gap"], 28)
        self.assertEqual(hashlib.sha256(self.truth.read_bytes()).hexdigest(), before)
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o600)
        self.assertNotIn(self.validation["question"], json.dumps(receipt))
        self.assertNotIn(retrieved["source_id"], json.dumps(receipt))

    def test_many_candidates_do_not_inflate_independent_question_coverage(self) -> None:
        probes = self.write("probes.jsonl", [{
            "id": self.validation["id"],
            "candidates": [candidate(draft_case(i)) for i in range(1000, 1050)],
        }])
        receipt = self.prepare([probes], families_path=self.family_map())
        self.assertEqual(receipt["unique_answerable_questions"], 12)
        self.assertEqual(receipt["answerable_family_clusters"], 12)
        self.assertGreaterEqual(receipt["candidate_count"], 50)

    def test_union_retains_both_probe_origins_and_omits_unauthorized_candidates(self) -> None:
        returned = candidate(draft_case())
        unauthorized = candidate(draft_case(1001))
        unauthorized["authorized"] = False
        first = self.write("first.jsonl", [{"id": self.validation["id"], "candidates": [returned, unauthorized]}])
        second = self.write("second.jsonl", [{"id": self.validation["id"], "candidates": [returned]}])
        receipt = self.prepare([first, second])
        rendered = self.output.read_text()
        self.assertIn("saved-probe-1:rank-1", rendered)
        self.assertIn("saved-probe-2:rank-1", rendered)
        self.assertNotIn(unauthorized["logical_document_id"], rendered)
        self.assertEqual(receipt["withheld_candidates"], 1)
        self.assertEqual(receipt["candidate_count"], 16)  # 15 gold boundaries plus one unioned candidate

    def test_drafts_remain_pending_and_duplicates_and_shared_families_are_visible(self) -> None:
        first = draft_case()
        duplicate = draft_case(1001)
        duplicate["question"] = first["question"].upper()
        related = draft_case(1002)
        related["gold_boundaries"] = copy.deepcopy(first["gold_boundaries"])
        related["gold_facts"] = copy.deepcopy(first["gold_facts"])
        questions = self.write("drafts.jsonl", [first, duplicate, related])
        receipt = self.prepare(questions_path=questions, families_path=self.family_map(first, duplicate, related))
        self.assertEqual(receipt["approved_answerable_questions"], 12)
        self.assertEqual(receipt["proposed_answerable_questions"], 3)
        self.assertEqual(receipt["unique_answerable_questions"], 14)
        self.assertEqual(receipt["answerable_family_clusters"], 13)
        self.assertEqual(receipt["duplicate_question_groups"], 1)
        self.assertEqual(receipt["shared_family_groups"], 1)
        self.assertIn("Proposed question — awaiting owner review", self.output.read_text())

    def test_drafts_cannot_promote_themselves_or_reuse_protected_questions(self) -> None:
        draft = draft_case()
        draft["owner_review"]["status"] = "approved"
        questions = self.write("drafts.jsonl", [draft])
        with self.assertRaisesRegex(EvaluationInputError, "pending"):
            self.prepare(questions_path=questions)
        draft["owner_review"]["status"] = "pending"
        draft["question"] = self.test["question"]
        private_write(questions, [draft])
        receipt = self.prepare(questions_path=questions)
        self.assertEqual(receipt["withheld_questions"], 1)
        self.assertNotIn(self.test["question"], self.output.read_text())

    def test_native_family_links_protect_heldout_and_group_related_questions(self) -> None:
        draft = draft_case()
        rows = []
        for case in (draft, self.test):
            rows.append({
                **{key: case["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id")},
                "family_id": "synthetic-native-parent",
            })
        families = self.write("families.jsonl", rows)
        questions = self.write("drafts.jsonl", [draft])
        receipt = self.prepare(questions_path=questions, families_path=families)
        self.assertEqual(receipt["withheld_questions"], 1)
        self.assertNotIn(draft["question"], self.output.read_text())
        self.assertFalse(receipt["native_family_audit_complete"])

    def test_withheld_question_labels_are_also_withheld(self) -> None:
        draft = draft_case()
        draft["question"] = self.test["question"]
        questions = self.write("drafts.jsonl", [draft])
        labels = self.write("labels.jsonl", [{
            "id": draft["id"],
            **{key: draft["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [{"receipt": draft["gold_boundaries"][0]["receipts"][0], "text": "Held-out evidence must not render."}],
            "proposal": None,
        }])
        receipt = self.prepare(questions_path=questions, labels_path=labels)
        self.assertEqual(receipt["withheld_questions"], 1)
        self.assertNotIn("Held-out evidence must not render.", self.output.read_text())

    def test_supplied_evidence_and_model_labels_are_proposals_not_approvals(self) -> None:
        row = {
            "id": self.validation["id"],
            **{key: self.validation["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [{"receipt": self.validation["gold_boundaries"][0]["receipts"][0], "text": "A <script>bad()</script> source excerpt."}],
            "proposal": {"relevance_probability": 0.9, "model": "synthetic-judge"},
        }
        labels = self.write("labels.jsonl", [row])
        receipt = self.prepare(labels_path=labels, families_path=self.family_map())
        rendered = self.output.read_text()
        self.assertEqual(receipt["candidate_label_proposals"], 1)
        self.assertEqual(receipt["candidates_with_source_evidence"], 1)
        self.assertIn("Proposed relevance", rendered)
        self.assertIn("&lt;script&gt;bad()&lt;/script&gt;", rendered)
        self.assertNotIn("<script>bad()", rendered)
        self.assertIn("No approvals are recorded by this packet", rendered)

    def test_rejects_bad_label_probabilities_and_unpooled_labels(self) -> None:
        row = {
            "id": self.validation["id"],
            **{key: self.validation["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [], "proposal": {"relevance_probability": math.nan, "model": "synthetic"},
        }
        labels = self.write("labels.jsonl", [row])
        with self.assertRaises(EvaluationInputError):
            self.prepare(labels_path=labels)
        row["proposal"]["relevance_probability"] = 0.9
        row["logical_document_id"] = "ldoc_" + "f" * 32
        private_write(labels, [row])
        with self.assertRaisesRegex(EvaluationInputError, "pooled candidate"):
            self.prepare(labels_path=labels)
        self.assertFalse(self.output.exists())

    def test_all_private_inputs_and_output_must_stay_outside_repo(self) -> None:
        inside = private_directory(self.repo)
        bad_truth = inside / "truth.jsonl"
        private_write(bad_truth, self.cases)
        with self.assertRaisesRegex(EvaluationInputError, "outside Git"):
            prepare_review(bad_truth, [], self.output, repo_root=self.repo)
        for option in ("questions_path", "labels_path", "families_path"):
            path = inside / (option + ".jsonl")
            private_write(path, [])
            with self.subTest(option=option), self.assertRaisesRegex(EvaluationInputError, "outside Git"):
                self.prepare(**{option: path})
        with self.assertRaisesRegex(EvaluationInputError, "outside Git"):
            prepare_review(self.truth, [], inside / "packet.html", repo_root=self.repo)
        self.assertFalse(self.output.exists())

    def test_symlinks_and_public_files_are_rejected(self) -> None:
        link = self.private / "linked-truth.jsonl"
        link.symlink_to(self.truth)
        with self.assertRaisesRegex(EvaluationInputError, "symlinks"):
            prepare_review(link, [], self.output, repo_root=self.repo)
        self.truth.chmod(0o644)
        with self.assertRaisesRegex(EvaluationInputError, "0600"):
            self.prepare()

    def test_protected_receipts_cannot_be_laundered_into_validation_labels(self) -> None:
        base = self.test["gold_boundaries"][0]["receipts"][0].split("?", 1)[0]
        families = self.family_map()
        for suffix in ("?rev=1#item=0", "?rev=99#item=9", "#item=7"):
            with self.subTest(suffix=suffix):
                labels = self.write("attack-labels.jsonl", [{
                    "id": self.validation["id"],
                    **{key: self.validation["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
                    "evidence": [{"receipt": base + suffix, "text": "HELDOUT_MARKER"}],
                    "proposal": None,
                }])
                with self.assertRaisesRegex(EvaluationInputError, "protected receipt"):
                    self.prepare(labels_path=labels, families_path=families)
                self.assertFalse(self.output.exists())

    def test_protected_receipts_cannot_be_laundered_into_proposed_gold(self) -> None:
        base = self.test["gold_boundaries"][0]["receipts"][0].split("?", 1)[0]
        for suffix in ("?rev=1#item=0", "?rev=99#item=9", "#item=7"):
            with self.subTest(suffix=suffix):
                draft = draft_case()
                draft["gold_boundaries"][0]["receipts"] = [base + suffix]
                draft["gold_facts"][0]["receipts"] = [base + suffix]
                questions = self.write("attack-questions.jsonl", [draft])
                receipt = self.prepare(questions_path=questions, families_path=self.family_map(draft))
                self.assertEqual(receipt["withheld_questions"], 1)
                self.assertNotIn(draft["question"], self.output.read_text())
                self.assertNotIn(base, self.output.read_text())
                self.output.unlink()

    def test_known_receipt_cannot_be_assigned_to_a_different_document(self) -> None:
        labels = self.write("wrong-document-labels.jsonl", [{
            "id": self.validation["id"],
            **{key: self.validation["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [{"receipt": self.cases[6]["gold_boundaries"][0]["receipts"][0], "text": "Wrong document."}],
            "proposal": None,
        }])
        with self.assertRaisesRegex(EvaluationInputError, "frozen boundary"):
            self.prepare(labels_path=labels, families_path=self.family_map())

    def test_family_audit_includes_protected_references_and_all_pooled_candidates(self) -> None:
        rows = [
            {"source_id": b["source_id"], "logical_document_id": b["logical_document_id"], "family_id": b["logical_document_id"]}
            for case in self.cases if case["split"] == "validation" for b in case["gold_boundaries"]
        ]
        families = self.write("partial-families.jsonl", rows)
        probes = self.write("unmapped-pool.jsonl", [{"id": self.validation["id"], "candidates": [candidate(draft_case())]}])
        receipt = self.prepare([probes], families_path=families)
        self.assertFalse(receipt["native_family_audit_complete"])
        self.assertGreater(receipt["unresolved_reference_families"], 0)
        self.assertEqual(receipt["unresolved_candidate_families"], 1)
        self.assertEqual(receipt["answerable_family_clusters"], 0)
        self.output.unlink()
        receipt = self.prepare([probes], families_path=self.family_map())
        self.assertFalse(receipt["native_family_audit_complete"])
        self.assertEqual(receipt["unresolved_reference_families"], 0)
        self.assertEqual(receipt["unresolved_candidate_families"], 1)
        self.assertEqual(receipt["answerable_family_clusters"], 12)

    def test_unknown_family_proposals_and_evidence_are_not_ready_for_review(self) -> None:
        draft = draft_case()
        questions = self.write("unmapped-questions.jsonl", [draft])
        families = self.family_map()
        receipt = self.prepare(questions_path=questions, families_path=families)
        self.assertEqual(receipt["withheld_questions"], 1)
        self.assertNotIn(draft["question"], self.output.read_text())
        self.output.unlink()
        probes = self.write("unmapped-pool.jsonl", [{"id": self.validation["id"], "candidates": [candidate(draft)]}])
        labels = self.write("unmapped-labels.jsonl", [{
            "id": self.validation["id"],
            **{key: draft["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [{"receipt": draft["gold_boundaries"][0]["receipts"][0], "text": "Unvetted family."}],
            "proposal": None,
        }])
        with self.assertRaisesRegex(EvaluationInputError, "resolved.*families"):
            self.prepare([probes], labels_path=labels, families_path=families)

    def test_novel_receipt_association_is_marked_unverified(self) -> None:
        labels = self.write("novel-labels.jsonl", [{
            "id": self.validation["id"],
            **{key: self.validation["gold_boundaries"][0][key] for key in ("source_id", "logical_document_id", "revision")},
            "evidence": [{"receipt": "recall://synthetic:source:0/new-record?rev=1#item=0", "text": "Supplied excerpt."}],
            "proposal": None,
        }])
        receipt = self.prepare(labels_path=labels, families_path=self.family_map())
        self.assertEqual(receipt["unverified_excerpt_associations"], 1)
        self.assertIn("supplied-unverified", self.output.read_text())


if __name__ == "__main__":
    unittest.main()
