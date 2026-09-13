"""Offline fusion tuner: private paths, content-free reports, alpha search."""

from __future__ import annotations

import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from evals import fusion_tuning
from evals.retrieval import EvaluationInputError


def private_directory(root: Path, name: str = "private") -> Path:
    directory = root / name
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    return directory


def private_write(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
    path.chmod(0o600)


def ldoc(name: str) -> str:
    return "ldoc_" + name.ljust(32, "0")


def boundary(name: str) -> dict:
    return {
        "logical_document_id": ldoc(name),
        "source_id": "synthetic:source:1",
        "revision": 1,
        "receipts": [f"recall://synthetic:source:1/{name}?rev=1#item=0"],
        "first_occurred_at": "2026-08-01T00:00:00Z",
        "last_occurred_at": "2026-08-01T00:10:00Z",
    }


def case(case_id: str, split: str, gold: list[str], *, answerable: bool = True) -> dict:
    return {
        "id": case_id,
        "split": split,
        "stratum": "exact-document",
        "intent": "project-status",
        "question": f"secret question text for {case_id}",
        "answerability": "answerable" if answerable else "insufficient",
        "gold_boundaries": [boundary(name) for name in gold],
        "gold_facts": [],
        "owner_review": {"status": "approved", "revision": 1},
    }


def candidate(name: str) -> dict:
    return {
        "logical_document_id": ldoc(name),
        "source_id": "synthetic:source:1",
        "revision": 1,
        "pointer_valid": True,
        "authorized": True,
    }


def arms(dense: float | None = None, lexical: float | None = None, sparse: float | None = None) -> dict:
    value = {}
    for arm, score in (("dense", dense), ("passage-lexical", lexical), ("sparse-exact", sparse)):
        if score is not None:
            value[arm] = {"score": score, "rank": 1, "normalized": score}
    return value


def synthetic_truth() -> list[dict]:
    rows = []
    for index in range(4):
        rows.append(case(f"opt-{index}", "optimize", [f"gold{index}"]))
    rows.append(case("opt-neg", "optimize", [], answerable=False))
    for index in range(2):
        rows.append(case(f"val-{index}", "validation", [f"gold{index}"]))
    return rows


def synthetic_results() -> list[dict]:
    """Recorded order favours dense; the gold document wins only on the lexical arm."""

    rows = []
    for case_id, gold in (("opt-0", "gold0"), ("opt-1", "gold1"), ("opt-2", "gold2"), ("opt-3", "gold3"),
                          ("val-0", "gold0"), ("val-1", "gold1")):
        rows.append({
            "id": case_id,
            "candidates": [candidate("noise-a"), candidate("noise-b"), candidate(gold)],
            "arm_scores": [arms(dense=1.0, lexical=0.1), arms(dense=0.9, lexical=0.0), arms(dense=0.2, lexical=1.0)],
            "latency_ms": 12.0,
            "backend_error": "",
        })
    rows.append({"id": "opt-neg", "candidates": [], "arm_scores": [], "latency_ms": 5.0, "backend_error": ""})
    return rows


class FusionTuningTests(unittest.TestCase):
    def _repo(self, root: Path) -> Path:
        repo = root / "repo"
        repo.mkdir()
        subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                        "commit", "-q", "--allow-empty", "-m", "init"], check=True)
        return repo

    def test_tunes_alphas_on_optimize_and_reports_validation_without_content(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            private = private_directory(root)
            truth = private / "truth.jsonl"
            results = private / "systems-card-boundaries-optimize-1.jsonl"
            private_write(truth, synthetic_truth())
            private_write(results, synthetic_results())
            output = private / "fusion-tuning.json"

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                report = fusion_tuning.tune(
                    truth, [results], output, repo_root=repo, run_id="tune-1", step=0.25,
                )

            self.assertEqual(report["tune_split"], "optimize")
            self.assertEqual(report["report_split"], "validation")
            self.assertEqual(report["split_counts"], {"optimize": 5, "validation": 2})
            self.assertEqual(report["results_coverage"], {"optimize": 5, "validation": 2})
            self.assertEqual(report["grid_size"], 15)
            # Recorded order puts gold third (MRR 1/3); a lexical-heavy alpha lifts it to first.
            self.assertAlmostEqual(report["tune"]["recorded"]["mrr"], round(1 / 3, 4), places=4)
            self.assertEqual(report["tune"]["best"]["mrr"], 1.0)
            self.assertEqual(report["tune"]["best"]["recall@20"], 1.0)
            self.assertEqual(report["report"]["best"]["mrr"], 1.0)
            self.assertEqual(report["tune"]["recorded"]["negative_false_hit_rate"], 0.0)
            self.assertGreaterEqual(report["best_alphas"]["passage-lexical"], report["best_alphas"]["dense"])
            self.assertAlmostEqual(sum(report["best_alphas"].values()), 1.0)
            self.assertTrue(report["env"]["RECALL_SEARCH_FUSION_ALPHAS"].startswith("dense:"))
            self.assertEqual(report["pins"]["truth_sha256"], __import__("hashlib").sha256(truth.read_bytes()).hexdigest())

            self.assertEqual(oct(output.stat().st_mode & 0o777), "0o600")
            written = output.read_text()
            self.assertEqual(json.loads(written), report)
            for token in ("secret question", "recall://", "ldoc_", "opt-0", "val-1", "noise-a", "gold0"):
                self.assertNotIn(token, written)
            self.assertEqual(
                set(report),
                {"schema_version", "run_id", "k", "grid_step", "grid_size", "tune_split", "report_split",
                 "split_counts", "results_coverage", "candidates_without_arm_scores", "best_alphas",
                 "baseline_alphas", "tune", "report", "pins", "env"},
            )
            self.assertEqual(sorted(os.listdir(private)), sorted([truth.name, results.name, output.name]))

    def test_recall_floor_keeps_recorded_ordering_when_nothing_improves(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            private = private_directory(root)
            truth = private / "truth.jsonl"
            results = private / "results.jsonl"
            private_write(truth, synthetic_truth())
            rows = synthetic_results()
            for row in rows:  # gold already first everywhere: no alpha can beat the recorded order
                row["candidates"].reverse()
                row["arm_scores"].reverse()
            private_write(results, rows)
            report = fusion_tuning.tune(
                truth, [results], private / "out.json", repo_root=repo, run_id="tune-2", step=0.5,
                baseline={"dense": 0.15, "passage-lexical": 0.30, "sparse-exact": 0.55},
            )
            self.assertEqual(report["tune"]["recorded"]["mrr"], 1.0)
            self.assertEqual(report["tune"]["best"]["mrr"], 1.0)
            self.assertEqual(report["baseline_alphas"]["sparse-exact"], 0.55)

    def test_rejects_in_repo_paths_and_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            inside = private_directory(repo, "private")
            outside = private_directory(root)
            truth_in = inside / "truth.jsonl"
            results_in = inside / "results.jsonl"
            truth_out = outside / "truth.jsonl"
            results_out = outside / "results.jsonl"
            for path in (truth_in, truth_out):
                private_write(path, synthetic_truth())
            for path in (results_in, results_out):
                private_write(path, synthetic_results())

            with self.assertRaisesRegex(EvaluationInputError, "outside Git"):
                fusion_tuning.tune(truth_in, [results_out], outside / "a.json", repo_root=repo, run_id="r")
            with self.assertRaisesRegex(EvaluationInputError, "outside Git"):
                fusion_tuning.tune(truth_out, [results_in], outside / "b.json", repo_root=repo, run_id="r")
            with self.assertRaisesRegex(EvaluationInputError, "outside Git"):
                fusion_tuning.tune(truth_out, [results_out], inside / "c.json", repo_root=repo, run_id="r")
            self.assertEqual(sorted(os.listdir(inside)), ["results.jsonl", "truth.jsonl"])
            self.assertEqual(sorted(os.listdir(outside)), ["results.jsonl", "truth.jsonl"])
            self.assertEqual(
                sorted(p.name for p in repo.iterdir() if p.name != ".git"), ["private"],
            )
            # Existing outputs are never overwritten either.
            existing = outside / "exists.json"
            existing.write_text("{}")
            existing.chmod(0o600)
            with self.assertRaisesRegex(EvaluationInputError, "must be new"):
                fusion_tuning.tune(truth_out, [results_out], existing, repo_root=repo, run_id="r")
            self.assertEqual(existing.read_text(), "{}")

    def test_rejects_results_without_arm_scores(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._repo(root)
            private = private_directory(root)
            truth = private / "truth.jsonl"
            results = private / "results.jsonl"
            private_write(truth, synthetic_truth())
            rows = synthetic_results()
            for row in rows:
                row.pop("arm_scores")
            private_write(results, rows)
            with self.assertRaisesRegex(EvaluationInputError, "per-arm scores"):
                fusion_tuning.tune(truth, [results], private / "out.json", repo_root=repo, run_id="r")
            self.assertFalse((private / "out.json").exists())

    def test_alpha_parsing_and_grid(self) -> None:
        self.assertEqual(
            fusion_tuning.parse_alphas("dense:0.5,lexical:0.3,sparse:0.2"),
            {"dense": 0.5, "passage-lexical": 0.3, "sparse-exact": 0.2},
        )
        with self.assertRaises(EvaluationInputError):
            fusion_tuning.parse_alphas("dense:0.5,lexical:0.3,sparse:0.3")
        grid = fusion_tuning.simplex_grid(0.05)
        self.assertEqual(len(grid), 231)
        self.assertTrue(all(abs(sum(point.values()) - 1.0) < 1e-9 for point in grid))
        with self.assertRaises(EvaluationInputError):
            fusion_tuning.simplex_grid(0.3)


if __name__ == "__main__":
    unittest.main()
