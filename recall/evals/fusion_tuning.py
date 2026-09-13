"""Offline convex-fusion alpha tuner (H2-b).

Replays saved `recall_search` candidates under different arm weights without
touching the server. Inputs are the owner-private truth set and one or more
per-case candidate files written by the systems-card accuracy probe
(``systems-card-boundaries-<split>-<stamp>.jsonl``); each candidate there
carries the server's ``arm_scores`` (per arm: raw best score, arm rank, and
the normalised value the fused score used). Fusion is re-run offline as
``Σ alpha_arm × normalized`` for every alpha on a simplex grid, the best alpha
is chosen on the tuning split (MRR, subject to recall@k not dropping below the
recorded ordering), and the held-out split is reported for that alpha.

Only documents that reached the saved candidate list can be re-ranked; a
document the live fusion left outside the top ``candidate_depth`` cannot be
recovered here, so the tuner is conservative about recall. Deeper probe
captures (``CANDIDATE_LIMIT``) widen what it can see.

Everything printed or written is content-free: alphas, aggregate metrics per
split, grid size, and input digests. No question text, receipts, document ids,
or per-case rows leave the private inputs. Every path must live outside the
repository and follow the private-holdout rules (owner-only parent, mode 0600).

    PYTHONPATH=recall:recall/server python -m evals.fusion_tuning \\
        --truth ~/.recall/eval/agentic-truth.jsonl \\
        --results ~/.recall/systems-card/systems-card-boundaries-optimize-*.jsonl \\
                  ~/.recall/systems-card/systems-card-boundaries-validation-*.jsonl \\
        --output ~/.recall/systems-card/fusion-tuning-<stamp>.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Sequence

from .agentic_truth import _outside_repository
from .boundary_identity import stable_boundary_identity
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError
from .runner import git_dirty, git_sha

SCHEMA_VERSION = "recall.fusion-tuning.v1"
ARM_NAMES = ("dense", "passage-lexical", "sparse-exact")
ARM_ALIASES = {
    "dense": "dense",
    "lexical": "passage-lexical",
    "passage-lexical": "passage-lexical",
    "sparse": "sparse-exact",
    "sparse-exact": "sparse-exact",
}
DEFAULT_ALPHAS = {"dense": 0.15, "passage-lexical": 0.30, "sparse-exact": 0.55}
DEFAULT_STEP = 0.05
DEFAULT_K = 20
MAX_RESULT_FILES = 16
MAX_CANDIDATES_PER_CASE = 100


class Case:
    __slots__ = ("split", "gold", "answerable")

    def __init__(self, split: str, gold: frozenset[tuple[str, str]], answerable: bool) -> None:
        self.split = split
        self.gold = gold
        self.answerable = answerable


class Candidate:
    __slots__ = ("identity", "position", "arms")

    def __init__(self, identity: tuple[str, str], position: int, arms: dict[str, float]) -> None:
        self.identity = identity
        self.position = position
        self.arms = arms


def parse_alphas(text: str | None) -> dict[str, float]:
    if text is None or not text.strip():
        return dict(DEFAULT_ALPHAS)
    alphas: dict[str, float] = {}
    for item in text.split(","):
        name, separator, value = item.strip().partition(":")
        arm = ARM_ALIASES.get(name.strip().lower())
        if not separator or arm is None or arm in alphas:
            raise EvaluationInputError("alphas must be dense:x,lexical:y,sparse:z")
        try:
            weight = float(value)
        except ValueError as error:
            raise EvaluationInputError("alphas must be dense:x,lexical:y,sparse:z") from error
        if not math.isfinite(weight) or weight < 0:
            raise EvaluationInputError("alphas must be non-negative and finite")
        alphas[arm] = weight
    if set(alphas) != set(ARM_NAMES) or abs(sum(alphas.values()) - 1.0) > 1e-6:
        raise EvaluationInputError("alphas must name every arm once and sum to 1")
    return alphas


def simplex_grid(step: float) -> list[dict[str, float]]:
    if not (0.01 <= step <= 0.5):
        raise EvaluationInputError("grid step must be between 0.01 and 0.5")
    units = round(1.0 / step)
    if abs(units * step - 1.0) > 1e-9:
        raise EvaluationInputError("grid step must divide 1 evenly")
    grid: list[dict[str, float]] = []
    for dense in range(units + 1):
        for lexical in range(units + 1 - dense):
            sparse = units - dense - lexical
            grid.append({
                "dense": round(dense / units, 6),
                "passage-lexical": round(lexical / units, 6),
                "sparse-exact": round(sparse / units, 6),
            })
    return grid


def load_cases(cases: list[dict[str, Any]]) -> dict[str, Case]:
    loaded: dict[str, Case] = {}
    for case in cases:
        case_id = case.get("id")
        split = case.get("split")
        gold = case.get("gold_boundaries")
        if (
            not isinstance(case_id, str) or not case_id or case_id in loaded
            or not isinstance(split, str) or not split
            or not isinstance(gold, list)
        ):
            raise EvaluationInputError("truth case schema is invalid")
        try:
            identities = frozenset(stable_boundary_identity(boundary) for boundary in gold)
        except (ValueError, AttributeError) as error:
            raise EvaluationInputError("truth gold boundaries are invalid") from error
        loaded[case_id] = Case(split, identities, case.get("answerability", "answerable") == "answerable")
    if not loaded:
        raise EvaluationInputError("truth set has no cases")
    return loaded


def load_results(rows: Iterable[dict[str, Any]], results: dict[str, list[Candidate]]) -> int:
    """Merge one saved candidate file into ``results``; later files win per case."""

    without_arm_scores = 0
    for row in rows:
        case_id = row.get("id")
        candidates = row.get("candidates")
        arm_scores = row.get("arm_scores")
        if not isinstance(case_id, str) or not isinstance(candidates, list):
            raise EvaluationInputError("results row schema is invalid")
        if len(candidates) > MAX_CANDIDATES_PER_CASE:
            raise EvaluationInputError("results row has too many candidates")
        if not isinstance(arm_scores, list) or len(arm_scores) != len(candidates):
            raise EvaluationInputError(
                "results rows carry no per-arm scores; capture them with a server that "
                "reports diagnostics.fusion (accuracy probe rows include arm_scores)"
            )
        loaded: list[Candidate] = []
        seen: set[tuple[str, str]] = set()
        for position, (candidate, arms) in enumerate(zip(candidates, arm_scores, strict=True), start=1):
            try:
                identity = stable_boundary_identity(candidate)
            except (ValueError, AttributeError) as error:
                raise EvaluationInputError("results candidate identity is invalid") from error
            if identity in seen:
                raise EvaluationInputError("results candidates contain duplicates")
            seen.add(identity)
            if arms is None:
                without_arm_scores += 1
                arms = {}
            if not isinstance(arms, dict):
                raise EvaluationInputError("results arm_scores entry is invalid")
            normalized: dict[str, float] = {}
            for arm, entry in arms.items():
                if arm not in ARM_NAMES or not isinstance(entry, dict):
                    raise EvaluationInputError("results arm_scores entry is invalid")
                value = entry.get("normalized")
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise EvaluationInputError("results arm_scores entry is invalid")
                normalized[arm] = float(value)
            loaded.append(Candidate(identity, position, normalized))
        results[case_id] = loaded
    return without_arm_scores


def rerank(candidates: Sequence[Candidate], alphas: dict[str, float] | None) -> list[tuple[str, str]]:
    """Order candidates by convex fused score; ``None`` keeps the recorded order."""

    if alphas is None:
        return [candidate.identity for candidate in candidates]
    scored = sorted(
        candidates,
        key=lambda candidate: (
            -sum(alphas[arm] * value for arm, value in candidate.arms.items()),
            candidate.position,
        ),
    )
    return [candidate.identity for candidate in scored]


def score_split(
    cases: dict[str, Case],
    results: dict[str, list[Candidate]],
    *,
    split: str,
    alphas: dict[str, float] | None,
    k: int,
) -> dict[str, float | int]:
    recalls: list[float] = []
    reciprocal_ranks: list[float] = []
    hits_at_1 = 0
    negatives = 0
    negative_hits = 0
    missing = 0
    for case_id, case in cases.items():
        if case.split != split:
            continue
        candidates = results.get(case_id)
        if candidates is None:
            missing += 1
            candidates = []
        ranked = rerank(candidates, alphas)
        if not case.answerable or not case.gold:
            negatives += 1
            negative_hits += int(bool(ranked))
            continue
        top = ranked[:k]
        matched = sum(1 for identity in top if identity in case.gold)
        recalls.append(matched / len(case.gold))
        first = next((ordinal for ordinal, identity in enumerate(top, 1) if identity in case.gold), None)
        reciprocal_ranks.append(0.0 if first is None else 1.0 / first)
        hits_at_1 += int(first == 1)
    positives = len(recalls)
    return {
        "cases": positives + negatives,
        "answerable_cases": positives,
        "missing_results": missing,
        f"recall@{k}": statistics.fmean(recalls) if recalls else 0.0,
        "mrr": statistics.fmean(reciprocal_ranks) if reciprocal_ranks else 0.0,
        "hit@1": (hits_at_1 / positives) if positives else 0.0,
        "negative_false_hit_rate": (negative_hits / negatives) if negatives else None,
    }


def choose_alphas(
    cases: dict[str, Case],
    results: dict[str, list[Candidate]],
    *,
    split: str,
    grid: list[dict[str, float]],
    k: int,
    baseline: dict[str, float] | None,
) -> tuple[dict[str, float], dict[str, float | int], dict[str, float | int]]:
    """Maximise MRR with recall@k not below the baseline ordering's recall."""

    recorded = score_split(cases, results, split=split, alphas=baseline, k=k)
    floor = float(recorded[f"recall@{k}"])
    best: tuple[tuple[float, float, float], dict[str, float], dict[str, float | int]] | None = None
    for alphas in grid:
        metrics = score_split(cases, results, split=split, alphas=alphas, k=k)
        recall = float(metrics[f"recall@{k}"])
        if recall + 1e-12 < floor:
            continue
        distance = -sum((alphas[arm] - DEFAULT_ALPHAS[arm]) ** 2 for arm in ARM_NAMES)
        key = (float(metrics["mrr"]), recall, distance)
        if best is None or key > best[0]:
            best = (key, alphas, metrics)
    if best is None:  # every grid point lost recall; keep the recorded ordering's alphas
        return dict(DEFAULT_ALPHAS if baseline is None else baseline), recorded, recorded
    return best[1], best[2], recorded


def _round(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (round(value, 4) if isinstance(value, float) else value)
        for key, value in metrics.items()
    }


def tune(
    truth_path: Path,
    result_paths: Sequence[Path],
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    step: float = DEFAULT_STEP,
    k: int = DEFAULT_K,
    tune_split: str | None = None,
    report_split: str | None = None,
    baseline: dict[str, float] | None = None,
) -> dict[str, Any]:
    if not isinstance(run_id, str) or not run_id or len(run_id) > 160:
        raise EvaluationInputError("run id is invalid")
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= MAX_CANDIDATES_PER_CASE:
        raise EvaluationInputError("k must be between 1 and 100")
    if not 1 <= len(result_paths) <= MAX_RESULT_FILES:
        raise EvaluationInputError("provide 1 to 16 results files")
    repo = Path(repo_root).resolve(strict=True)
    truth = _private_path(truth_path, exists=True)
    _outside_repository(truth, repo)
    results_resolved = [_private_path(path, exists=True) for path in result_paths]
    for path in results_resolved:
        _outside_repository(path, repo)
    output = _private_path(output_path, exists=False)
    resolved_output = output.resolve(strict=False)
    if resolved_output == repo or repo in resolved_output.parents:
        raise EvaluationInputError("private agentic evaluation files must stay outside Git")

    truth_rows, truth_payload = _load_jsonl(truth)
    cases = load_cases(truth_rows)
    results: dict[str, list[Candidate]] = {}
    result_digests: list[str] = []
    without_arm_scores = 0
    for path in results_resolved:
        rows, payload = _load_jsonl(path)
        result_digests.append(hashlib.sha256(payload).hexdigest())
        without_arm_scores += load_results(rows, results)
    unknown = set(results) - set(cases)
    if unknown:
        raise EvaluationInputError("results reference cases missing from the truth set")

    split_counts = Counter(case.split for case in cases.values())
    splits = sorted(split_counts)
    if tune_split is None:
        tune_split = "optimize" if "optimize" in split_counts else splits[0]
    if report_split is None:
        report_split = (
            "validation" if "validation" in split_counts and "validation" != tune_split
            else next((name for name in splits if name != tune_split), tune_split)
        )
    if tune_split not in split_counts or report_split not in split_counts:
        raise EvaluationInputError("requested split is not present in the truth set")

    grid = simplex_grid(step)
    best_alphas, best_tune_metrics, recorded_tune_metrics = choose_alphas(
        cases, results, split=tune_split, grid=grid, k=k, baseline=baseline,
    )
    covered = {
        split: sum(1 for case_id, case in cases.items() if case.split == split and case_id in results)
        for split in splits
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "k": k,
        "grid_step": step,
        "grid_size": len(grid),
        "tune_split": tune_split,
        "report_split": report_split,
        "split_counts": dict(sorted(split_counts.items())),
        "results_coverage": covered,
        "candidates_without_arm_scores": without_arm_scores,
        "best_alphas": best_alphas,
        "baseline_alphas": baseline,
        "tune": {
            "recorded": _round(recorded_tune_metrics),
            "best": _round(best_tune_metrics),
        },
        "report": {
            "recorded": _round(score_split(cases, results, split=report_split, alphas=baseline, k=k)),
            "best": _round(score_split(cases, results, split=report_split, alphas=best_alphas, k=k)),
        },
        "pins": {
            "truth_sha256": hashlib.sha256(truth_payload).hexdigest(),
            "results_sha256": result_digests,
            "scorer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "git_sha": git_sha(repo),
            "git_dirty": git_dirty(repo),
        },
        "env": {
            "RECALL_SEARCH_FUSION": "convex",
            "RECALL_SEARCH_FUSION_ALPHAS": ",".join(
                f"{short}:{best_alphas[arm]:g}"
                for short, arm in (("dense", "dense"), ("lexical", "passage-lexical"), ("sparse", "sparse-exact"))
            ),
        },
    }
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(payload)
    return report


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-fusion-tuning", description=__doc__.split("\n\n")[0])
    value.add_argument("--truth", required=True, help="owner-private agentic truth set (JSONL, mode 0600)")
    value.add_argument("--results", required=True, nargs="+", help="saved accuracy-probe candidate files with arm_scores")
    value.add_argument("--output", required=True, help="new content-free JSON report, outside the repository")
    value.add_argument("--repo-root", default=str(default_repo_root()))
    value.add_argument("--run-id", default=time.strftime("fusion-tuning-%Y%m%dT%H%M%SZ", time.gmtime()))
    value.add_argument("--step", type=float, default=DEFAULT_STEP, help="simplex grid step (default 0.05)")
    value.add_argument("--k", type=int, default=DEFAULT_K, help="recall depth (default 20)")
    value.add_argument("--tune-split", help="split to search on (default: optimize, else the first split)")
    value.add_argument("--report-split", help="held-out split to report (default: validation)")
    value.add_argument(
        "--baseline-alphas",
        help="alphas the results were captured with; default keeps the recorded ordering as baseline",
    )
    return value


def main() -> None:
    args = parser().parse_args()
    try:
        report = tune(
            Path(args.truth),
            [Path(path) for path in args.results],
            Path(args.output),
            repo_root=Path(args.repo_root),
            run_id=args.run_id,
            step=args.step,
            k=args.k,
            tune_split=args.tune_split,
            report_split=args.report_split,
            baseline=parse_alphas(args.baseline_alphas) if args.baseline_alphas else None,
        )
    except EvaluationInputError as error:
        print(f"fusion tuning rejected: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    summary = {
        "best_alphas": report["best_alphas"],
        "tune": report["tune"],
        "report": report["report"],
        "env": report["env"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
