"""Offline rerank blend replay: exact order reproduction, grid, private paths."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from evals import rerank_blend_replay as replay
from evals.retrieval import EvaluationInputError
from tests.test_fusion_tuning import candidate, case, private_directory, private_write


def evidence(fused: float, rerank: float | None = None) -> dict:
    value = {"fused": fused}
    if rerank is not None:
        value["rerank"] = rerank
    return value


class OrderTests(unittest.TestCase):
    def _rows(self):
        return [
            replay.Row(("s", "a"), 1, 0.9, 0.2),
            replay.Row(("s", "b"), 2, 0.1, 0.9),
            replay.Row(("s", "c"), 3, 0.5, 0.5),
            replay.Row(("s", "d"), 4, 0.05, None),
        ]

    def test_pure_rerank_and_recorded_order(self) -> None:
        rows = self._rows()
        self.assertEqual([i[1] for i in replay.order(rows, 1.0)], ["b", "c", "a", "d"])
        self.assertEqual([i[1] for i in replay.order(rows, None)], ["a", "b", "c", "d"])

    def test_blend_matches_apply_rerank_scores(self) -> None:
        # Same fixture as RerankBlendTests in the server suite: blend 0.5 keeps a ahead.
        rows = self._rows()
        self.assertEqual([i[1] for i in replay.order(rows, 0.5)], ["a", "b", "c", "d"])
        self.assertEqual([i[1] for i in replay.order(rows, 0.0)], ["a", "c", "b", "d"])


class ReplayTests(unittest.TestCase):
    def test_grid_reports_best_blend_and_rejects_in_repo_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private = private_directory(root)
            repo = root / "repo"
            repo.mkdir()
            truth = private / "truth.jsonl"
            private_write(truth, [
                case("case_1", "optimize", ["gold"]),
                case("case_2", "validation", ["gold"]),
                case("case_3", "validation", [], answerable=False),
            ])
            results = private / "rows.jsonl"
            # gold is fused last but reranked first: blend 1.0 puts it at rank 1.
            row = {
                "candidates": [candidate("top"), candidate("mid"), candidate("gold")],
                "rerank_evidence": [evidence(0.9, 0.3), evidence(0.5, 0.4), evidence(0.1, 0.95)],
            }
            private_write(results, [
                {"id": "case_1", **row}, {"id": "case_2", **row}, {"id": "case_3", "candidates": [], "rerank_evidence": []},
            ])
            output = private / "report.json"
            report = replay.replay(truth, [results], output, repo_root=repo, run_id="t", step=0.5)
            self.assertEqual(report["best_blend"], 1.0)
            self.assertEqual(report["recorded"]["report"]["mrr"], round(1 / 3, 4))
            best = next(r for r in report["table"] if r["blend"] == 1.0)
            self.assertEqual(best["report"]["mrr"], 1.0)
            self.assertEqual(best["report"]["negative_false_hit_rate"], 0.0)
            saved = json.loads(output.read_text())
            self.assertNotIn("secret question", output.read_text())
            self.assertEqual(saved["schema"], "recall.rerank-blend-replay.v1")
            self.assertEqual(oct(output.stat().st_mode & 0o777), "0o600")
            with self.assertRaises(EvaluationInputError):
                replay.replay(truth, [results], repo / "report.json", repo_root=repo, run_id="t", step=0.5)

    def test_rows_without_evidence_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private = private_directory(root)
            repo = root / "repo"
            repo.mkdir()
            truth = private / "truth.jsonl"
            private_write(truth, [case("case_1", "optimize", ["gold"])])
            results = private / "rows.jsonl"
            private_write(results, [{"id": "case_1", "candidates": [candidate("gold")], "arm_scores": [{}]}])
            with self.assertRaises(EvaluationInputError):
                replay.replay(truth, [results], private / "r.json", repo_root=repo, run_id="t", step=0.5)


if __name__ == "__main__":
    unittest.main()
