"""Replay the reranker blend offline from saved accuracy-probe rows.

``apply_rerank_scores`` orders reranked documents by
``blend × minmax(rerank) + (1 − blend) × minmax(fused)`` over the reranked
set, with unreranked documents keeping the fused order behind them. The
accuracy probe saves every candidate's ``fused``/``rerank``/``blended``
scores (``rerank_evidence``), so any blend value can be replayed exactly
without a deploy. Inputs are the owner-private truth set and one or more
saved candidate files; the report is content-free and must be written
outside the repository (same rules as ``fusion_tuning``).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Any

from .fusion_tuning import (
    MAX_CANDIDATES_PER_CASE,
    MAX_RESULT_FILES,
    Case,
    EvaluationInputError,
    load_cases,
    stable_boundary_identity,
)
from .private_holdout import _load_jsonl, _private_path

DEFAULT_STEP = 0.05
DEFAULT_K = 20


class Row:
    __slots__ = ("identity", "position", "fused", "rerank")

    def __init__(self, identity: tuple[str, str], position: int, fused: float, rerank: float | None) -> None:
        self.identity = identity
        self.position = position
        self.fused = fused
        self.rerank = rerank


def load_results(rows: list[dict[str, Any]], results: dict[str, list[Row]]) -> int:
    """Merge one saved candidate file; later files win per case. Returns rows lacking evidence."""

    without = 0
    for row in rows:
        case_id = row.get("id")
        candidates = row.get("candidates")
        evidence = row.get("rerank_evidence")
        if not isinstance(case_id, str) or not isinstance(candidates, list):
            raise EvaluationInputError("results row schema is invalid")
        if len(candidates) > MAX_CANDIDATES_PER_CASE:
            raise EvaluationInputError("results row has too many candidates")
        if not isinstance(evidence, list) or len(evidence) != len(candidates):
            raise EvaluationInputError(
                "results rows carry no rerank evidence; capture them with an accuracy probe "
                "that saves rerank_evidence"
            )
        loaded: list[Row] = []
        seen: set[tuple[str, str]] = set()
        for position, (candidate, entry) in enumerate(zip(candidates, evidence, strict=True), start=1):
            try:
                identity = stable_boundary_identity(candidate)
            except (ValueError, AttributeError) as error:
                raise EvaluationInputError("results candidate identity is invalid") from error
            if identity in seen:
                raise EvaluationInputError("results candidates contain duplicates")
            seen.add(identity)
            if entry is None:
                without += 1
                loaded.append(Row(identity, position, 0.0, None))
                continue
            if not isinstance(entry, dict):
                raise EvaluationInputError("results rerank_evidence entry is invalid")
            fused = entry.get("fused")
            rerank = entry.get("rerank")
            for value in (fused, rerank):
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                ):
                    raise EvaluationInputError("results rerank_evidence entry is invalid")
            loaded.append(Row(identity, position, float(fused or 0.0), None if rerank is None else float(rerank)))
        results[case_id] = loaded
    return without


def _min_max(values: list[float]) -> list[float]:
    low, high = min(values), max(values)
    if high - low <= 1e-12:
        return [1.0 for _ in values]
    return [(value - low) / (high - low) for value in values]


def order(rows: list[Row], blend: float | None) -> list[tuple[str, str]]:
    """Document order under ``blend``; ``None`` keeps the recorded order."""

    if blend is None:
        return [row.identity for row in rows]
    reranked = [row for row in rows if row.rerank is not None]
    rest = [row for row in rows if row.rerank is None]
    if not reranked:
        return [row.identity for row in rows]
    if blend >= 1.0:
        keyed = sorted(reranked, key=lambda row: (-row.rerank, row.position))
    else:
        rerank_norm = _min_max([row.rerank for row in reranked])
        fused_norm = _min_max([row.fused for row in reranked])
        scored = [
            (blend * rerank_norm[index] + (1.0 - blend) * fused_norm[index], row)
            for index, row in enumerate(reranked)
        ]
        keyed = [row for _score, row in sorted(scored, key=lambda item: (-item[0], item[1].position))]
    return [row.identity for row in keyed] + [row.identity for row in rest]


def score_split(cases: dict[str, Case], results: dict[str, list[Row]], *, split: str, blend: float | None, k: int) -> dict[str, Any]:
    recalls: list[float] = []
    reciprocal: list[float] = []
    hits_at_1 = hits_at_5 = 0
    negatives = negative_hits = missing = 0
    for case_id, case in cases.items():
        if case.split != split:
            continue
        rows = results.get(case_id)
        if rows is None:
            missing += 1
            rows = []
        ranked = order(rows, blend)
        if not case.answerable or not case.gold:
            negatives += 1
            negative_hits += int(bool(ranked))
            continue
        top = ranked[:k]
        recalls.append(sum(1 for identity in top if identity in case.gold) / len(case.gold))
        first = next((ordinal for ordinal, identity in enumerate(top, 1) if identity in case.gold), None)
        reciprocal.append(0.0 if first is None else 1.0 / first)
        hits_at_1 += int(first == 1)
        hits_at_5 += int(first is not None and first <= 5)
    positives = len(recalls)
    mean = lambda values: (sum(values) / len(values)) if values else 0.0  # noqa: E731
    return {
        "cases": positives + negatives,
        "answerable_cases": positives,
        "missing_results": missing,
        f"recall@{k}": mean(recalls),
        "mrr": mean(reciprocal),
        "hit@1": (hits_at_1 / positives) if positives else 0.0,
        "hit@5": (hits_at_5 / positives) if positives else 0.0,
        "negative_false_hit_rate": (negative_hits / negatives) if negatives else None,
    }


def _round(metrics: dict[str, Any]) -> dict[str, Any]:
    return {key: (round(value, 4) if isinstance(value, float) else value) for key, value in metrics.items()}


def replay(
    truth_path: Path,
    result_paths: list[Path],
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    step: float = DEFAULT_STEP,
    k: int = DEFAULT_K,
    tune_split: str | None = None,
    report_split: str | None = None,
) -> dict[str, Any]:
    if not result_paths or len(result_paths) > MAX_RESULT_FILES:
        raise EvaluationInputError("provide between 1 and 16 result files")
    if not (0.01 <= step <= 0.5) or abs(round(1.0 / step) * step - 1.0) > 1e-9:
        raise EvaluationInputError("grid step must divide 1 evenly")
    if not 1 <= k <= 100:
        raise EvaluationInputError("k must be between 1 and 100")
    truth = _private_path(truth_path, exists=True)
    resolved = [_private_path(path, exists=True) for path in result_paths]
    output = _private_path(output_path, exists=False)
    root = repo_root.resolve()
    for path in (truth, *resolved, output):
        if path == root or root in path.parents:
            raise EvaluationInputError("private inputs and reports must live outside the repository")
    truth_rows, _payload = _load_jsonl(truth)
    cases = load_cases(truth_rows)
    results: dict[str, list[Row]] = {}
    without = 0
    for path in resolved:
        rows, _payload = _load_jsonl(path)
        without += load_results(rows, results)
    splits = sorted({case.split for case in cases.values()})
    tune_on = tune_split or ("optimize" if "optimize" in splits else splits[0])
    report_on = report_split or ("validation" if "validation" in splits else tune_on)
    if tune_on not in splits or report_on not in splits:
        raise EvaluationInputError("requested split is not in the truth set")
    units = round(1.0 / step)
    grid = [round(index / units, 6) for index in range(units + 1)]
    table = []
    best: tuple[float, float, float] | None = None
    for blend in grid:
        tuned = score_split(cases, results, split=tune_on, blend=blend, k=k)
        held = score_split(cases, results, split=report_on, blend=blend, k=k)
        table.append({"blend": blend, "tune": _round(tuned), "report": _round(held)})
        key = (tuned["mrr"], tuned[f"recall@{k}"], -blend)
        if best is None or key > best:
            best = key
            best_blend = blend
    recorded_tune = score_split(cases, results, split=tune_on, blend=None, k=k)
    recorded_report = score_split(cases, results, split=report_on, blend=None, k=k)
    report = {
        "schema": "recall.rerank-blend-replay.v1",
        "run_id": run_id,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "k": k,
        "step": step,
        "tune_split": tune_on,
        "report_split": report_on,
        "rows_without_evidence": without,
        "recorded": {"tune": _round(recorded_tune), "report": _round(recorded_report)},
        "best_blend": best_blend,
        "table": table,
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
    value = argparse.ArgumentParser(prog="recall-rerank-blend-replay", description=__doc__.split("\n\n")[0])
    value.add_argument("--truth", required=True)
    value.add_argument("--results", required=True, nargs="+")
    value.add_argument("--output", required=True)
    value.add_argument("--repo-root", default=str(default_repo_root()))
    value.add_argument("--run-id", default=time.strftime("rerank-blend-%Y%m%dT%H%M%SZ", time.gmtime()))
    value.add_argument("--step", type=float, default=DEFAULT_STEP)
    value.add_argument("--k", type=int, default=DEFAULT_K)
    value.add_argument("--tune-split")
    value.add_argument("--report-split")
    return value


def main() -> None:
    args = parser().parse_args()
    try:
        report = replay(
            Path(args.truth), [Path(path) for path in args.results], Path(args.output),
            repo_root=Path(args.repo_root), run_id=args.run_id, step=args.step, k=args.k,
            tune_split=args.tune_split, report_split=args.report_split,
        )
    except EvaluationInputError as error:
        print(f"rerank blend replay rejected: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    print(json.dumps({
        "best_blend": report["best_blend"], "recorded": report["recorded"],
        "best": next(row for row in report["table"] if row["blend"] == report["best_blend"]),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
