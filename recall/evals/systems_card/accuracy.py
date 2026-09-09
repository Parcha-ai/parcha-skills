"""Accuracy: replay the owner-approved truth set through the public MCP.

The caller is the retrieval agent, so accuracy is measured on what a client
agent sees from `recall_search`: ranked logical-document boundaries. Scoring
reuses `evals.agentic_truth` (boundary recall, MRR, false hits, integrity).
Question text, receipts, and per-case rows stay in the owner-private
directory; only aggregates reach the card.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from ..agentic_truth import score_boundary_candidates
from ..private_holdout import _load_jsonl, _private_path
from .model import Gate, ProbeResult
from .probes import ProbeContext

CANDIDATE_LIMIT = 20  # recall_search maximum; boundary_recall@50 therefore equals @20 over MCP


def _revision(hit: dict[str, Any]) -> int:
    value = hit.get("revision")
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 1 else 1


def candidates_from_search(result: dict[str, Any]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, Any]] = []
    for hit in result.get("results", []):
        ldoc = hit.get("logical_document_id")
        source = hit.get("source_id")
        if not isinstance(ldoc, str) or not isinstance(source, str) or not source:
            continue
        identity = (source, ldoc)
        if identity in seen:
            continue
        seen.add(identity)
        candidates.append({
            "logical_document_id": ldoc,
            "source_id": source,
            "revision": _revision(hit),
            # Search results are server-verified receipts inside the caller's
            # authorization; violations are measured by the authorization probe.
            "pointer_valid": True,
            "authorized": True,
        })
        if len(candidates) >= 100:
            break
    return candidates


def first_receipt(result: dict[str, Any]) -> str | None:
    for hit in result.get("results", []):
        for rng in hit.get("matching_ranges", []):
            for receipt in rng.get("receipts", []):
                if isinstance(receipt, str) and receipt.startswith("recall://"):
                    return receipt
    return None


class TruthBoundaryProbe:
    name = "accuracy.truth_boundary"
    dimension = "accuracy"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        truth = context.options.get("truth_path")
        if not truth:
            result.status = "skipped"
            result.notes.append("no --truth path given; accuracy needs the owner-private truth set")
            return result
        split = context.options.get("truth_split", "validation")
        truth_path = _private_path(Path(truth), exists=True)
        cases, payload = _load_jsonl(truth_path)
        selected = [case for case in cases if split in (None, "all") or case.get("split") == split]
        if not selected:
            result.status = "failed"
            result.notes.append("truth split selected no cases")
            return result
        pointer_checks = int(context.options.get("accuracy_pointer_checks", 5))

        rows: list[dict[str, Any]] = []
        resolution_ok = 0
        resolution_checked = 0
        for case in selected:
            started = time.monotonic()
            outcome = context.client.call_tool(
                "recall_search", {"query": case["question"], "limit": CANDIDATE_LIMIT},
            )
            latency = (time.monotonic() - started) * 1000.0
            if outcome.ok and outcome.result:
                candidates = candidates_from_search(outcome.result)
                error = ""
                receipt = first_receipt(outcome.result)
                if receipt and resolution_checked < pointer_checks:
                    resolution_checked += 1
                    check = context.client.call_tool("recall_session_context", {"target": receipt, "before": 0, "after": 0})
                    resolution_ok += 1 if check.ok else 0
            else:
                candidates = []
                error = (outcome.error or "unknown_error")[:160]
            rows.append({
                "id": case["id"],
                "candidates": candidates,
                "latency_ms": round(latency, 3),
                "backend_error": error,
            })

        report = score_boundary_candidates(cases, rows, split=None if split == "all" else split)
        aggregate = report["aggregate"]
        result.samples = len(rows)
        metrics: dict[str, Any] = {
            "split": split,
            "cases": len(rows),
            "candidate_depth": CANDIDATE_LIMIT,
            "truth_sha256": hashlib.sha256(payload).hexdigest(),
            **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in aggregate.items()},
            "receipt_resolution_rate": (resolution_ok / resolution_checked) if resolution_checked else None,
            "receipt_resolution_checks": resolution_checked,
        }
        for stratum, values in report.get("strata", {}).items():
            for key in ("boundary_recall@20", "boundary_mrr", "negative_false_hit_rate", "case_hit_rate@50"):
                if key in values and values[key] is not None:
                    metrics[f"stratum.{stratum}.{key}"] = round(values[key], 4)
        result.metrics = metrics
        result.notes.append("negative_false_hit_rate is reported, not gated: recall_search is a hint engine and abstention belongs to the calling agent")
        result.gates = [
            Gate("boundary_recall@20", ">=", 0.6).evaluate(aggregate.get("boundary_recall@20")),
            Gate("boundary_mrr", ">=", 0.3).evaluate(aggregate.get("boundary_mrr")),
            Gate("authorization_violation_rate", "==", 0.0).evaluate(aggregate.get("authorization_violation_rate")),
            Gate("backend_error_rate", "<=", 0.02).evaluate(aggregate.get("backend_error_rate")),
            Gate("latency_p95_ms", "<=", 5000.0).evaluate(aggregate.get("latency_p95_ms")),
        ]
        if aggregate.get("backend_error_rate", 0) >= 0.5:
            result.status = "failed"
        elif any(g.passed is False for g in result.gates):
            result.status = "degraded"

        private_dir = context.private_dir
        if private_dir:
            out_dir = Path(private_dir).expanduser()
            out_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            results_path = out_dir / f"systems-card-boundaries-{split}-{stamp}.jsonl"
            fd = os.open(results_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as handle:
                for row in rows:
                    handle.write(json.dumps(row, sort_keys=True) + "\n")
            result.notes.append(f"per-case rankings saved privately ({results_path.name})")
        return result


class SyntheticSuiteProbe:
    """Surface the most recent frozen synthetic retrieval report, if provided."""

    name = "accuracy.synthetic_suite"
    dimension = "accuracy"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        path = context.options.get("synthetic_report")
        if not path:
            result.status = "skipped"
            result.notes.append("no --synthetic-report given; CI runs e2e_retrieval_eval.py on every PR")
            return result
        report = json.loads(Path(path).expanduser().read_text())
        aggregate = report.get("aggregate", {})
        result.metrics = {
            "schema_version": report.get("schema_version"),
            "run_id": report.get("run_id"),
            **{k: v for k, v in aggregate.items() if isinstance(v, (int, float, str)) or v is None},
        }
        result.samples = int(aggregate.get("queries", 0) or 0)
        return result
