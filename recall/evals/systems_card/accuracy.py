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
from ..expanded_truth import score_expanded_boundary_candidates
from ..private_holdout import _load_jsonl, _private_path
from ..retrieval import EvaluationInputError
from .model import Gate, ProbeResult
from .probes import ProbeContext
from .truth import load_truth_expansion

CANDIDATE_LIMIT = 50  # recall_search maximum (H2-d); boundary_recall@50 is measured at real depth


def _revision(hit: dict[str, Any]) -> int:
    value = hit.get("revision")
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 1 else 1


def arm_scores_from_search(result: dict[str, Any]) -> list[dict[str, Any] | None]:
    """Per-candidate arm evidence, aligned with ``candidates_from_search``.

    Each entry is the server's content-free ``arm_scores`` map (raw best
    score, arm rank, normalised value per arm) or ``None`` when the server
    did not expose it. ``evals.fusion_tuning`` replays fusion offline from
    these values; nothing here carries question text or receipts.
    """

    seen: set[tuple[str, str]] = set()
    scores: list[dict[str, Any] | None] = []
    for hit in result.get("results", []):
        ldoc = hit.get("logical_document_id")
        source = hit.get("source_id")
        if not isinstance(ldoc, str) or not isinstance(source, str) or not source:
            continue
        identity = (source, ldoc)
        if identity in seen:
            continue
        seen.add(identity)
        arms = hit.get("arm_scores")
        scores.append(
            {
                str(arm): {
                    key: value for key, value in entry.items()
                    if key in ("score", "rank", "normalized")
                    and isinstance(value, (int, float)) and not isinstance(value, bool)
                }
                for arm, entry in arms.items()
                if isinstance(entry, dict)
            }
            if isinstance(arms, dict)
            else None
        )
        if len(scores) >= 100:
            break
    return scores


def rerank_evidence_from_search(result: dict[str, Any]) -> list[dict[str, Any] | None]:
    """Per-candidate fused/rerank/blended scores, aligned with the candidates.

    Content-free floats: ``fused`` (the convex fusion score the server calls
    ``rank``), ``rerank`` (the cross-encoder score, absent when the row was
    not reranked) and ``blended``. Lets the blend be replayed offline.
    """

    seen: set[tuple[str, str]] = set()
    evidence: list[dict[str, Any] | None] = []
    for hit in result.get("results", []):
        ldoc = hit.get("logical_document_id")
        source = hit.get("source_id")
        if not isinstance(ldoc, str) or not isinstance(source, str) or not source:
            continue
        identity = (source, ldoc)
        if identity in seen:
            continue
        seen.add(identity)
        entry = {
            key: float(hit[name])
            for key, name in (("fused", "rank"), ("rerank", "rerank_score"), ("blended", "blended_score"))
            if isinstance(hit.get(name), (int, float)) and not isinstance(hit.get(name), bool)
        }
        evidence.append(entry or None)
        if len(evidence) >= 100:
            break
    return evidence


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


def _accuracy_gates(aggregate: dict[str, Any], prefix: str = "") -> list[Gate]:
    # Preserve the existing card thresholds for both independently scored panels.
    return [
        Gate(prefix + metric, op, threshold).evaluate(aggregate.get(metric))
        for metric, op, threshold in (
            ("boundary_recall@20", ">=", 0.66),
            ("boundary_mrr", ">=", 0.5),
            ("authorization_violation_rate", "==", 0.0),
            ("backend_error_rate", "<=", 0.02),
            ("latency_p95_ms", "<=", 5000.0),
        )
    ]


class TruthBoundaryProbe:
    name = "accuracy.truth_boundary"
    dimension = "accuracy"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        truth = context.options.get("truth_path")
        split = context.options.get("truth_split", "validation")
        expansion = context.options.get("_truth_expansion")
        expansion_path = context.options.get("truth_expansion_path")
        if expansion_path:
            if truth:
                raise EvaluationInputError("truth sources are mutually exclusive")
            expansion = expansion or load_truth_expansion(expansion_path, split=split)
        if not truth and expansion is None:
            result.status = "skipped"
            result.notes.append("no --truth path given; accuracy needs the owner-private truth set")
            return result
        if expansion is None:
            truth_path = _private_path(Path(truth), exists=True)
            cases, payload = _load_jsonl(truth_path)
            truth_sha256 = hashlib.sha256(payload).hexdigest()
        else:
            if split != "validation":
                raise EvaluationInputError("truth expansion is validation-only")
            cases = expansion.base + expansion.additions
            truth_sha256 = expansion.pins["base_sha256"]
        selected = [case for case in cases if split in (None, "all") or case.get("split") == split]
        if not selected:
            result.status = "failed"
            result.notes.append("truth split selected no cases")
            return result
        pointer_checks = int(context.options.get("accuracy_pointer_checks", 5))

        rows: list[dict[str, Any]] = []
        arm_scores: dict[str, list[dict[str, Any] | None]] = {}
        rerank_evidence: dict[str, list[dict[str, Any] | None]] = {}
        fusion_diagnostics: dict[str, Any] | None = None
        rerank_model: str | None = None
        search_plane: str | None = None
        rerank_statuses: dict[str, int] = {}
        resolution_ok = 0
        resolution_checked = 0
        for case in selected:
            started = time.monotonic()
            outcome = context.client.call_tool(
                # Compact snippets: the probe scores ids; full snippets at
                # limit 50 are ~400 KB per call and dominate its latency.
                "recall_search", {"query": case["question"], "limit": CANDIDATE_LIMIT, "snippet_chars": 256},
            )
            latency = (time.monotonic() - started) * 1000.0
            if outcome.ok and outcome.result:
                candidates = candidates_from_search(outcome.result)
                arm_scores[case["id"]] = arm_scores_from_search(outcome.result)
                rerank_evidence[case["id"]] = rerank_evidence_from_search(outcome.result)
                fusion = outcome.result.get("diagnostics", {}).get("fusion")
                if fusion_diagnostics is None and isinstance(fusion, dict):
                    fusion_diagnostics = {
                        "mode": fusion.get("mode"),
                        "alphas": fusion.get("alphas"),
                    }
                diagnostics = outcome.result.get("diagnostics", {})
                status = diagnostics.get("rerank_status")
                if isinstance(status, str):
                    rerank_statuses[status] = rerank_statuses.get(status, 0) + 1
                if rerank_model is None and isinstance(diagnostics.get("rerank_model"), str):
                    rerank_model = diagnostics["rerank_model"]
                if search_plane is None and isinstance(diagnostics.get("search_plane"), str):
                    search_plane = diagnostics["search_plane"]
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

        original_aggregate = None
        if expansion is None:
            report = score_boundary_candidates(cases, rows, split=None if split == "all" else split)
        else:
            report = score_expanded_boundary_candidates(
                expansion.base, expansion.additions, expansion.families, rows,
                expected_base_sha256=expansion.base_canonical_sha256, split="validation",
            )
            original_ids = {case["id"] for case in expansion.base if case["split"] == "validation"}
            original_report = score_boundary_candidates(
                expansion.base, [row for row in rows if row["id"] in original_ids], split="validation",
            )
            original_aggregate = original_report["aggregate"]
        aggregate = report["aggregate"]
        result.samples = len(rows)
        metrics: dict[str, Any] = {
            "split": split,
            "cases": len(rows),
            "candidate_depth": CANDIDATE_LIMIT,
            "truth_sha256": truth_sha256,
            **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in aggregate.items()},
            "receipt_resolution_rate": (resolution_ok / resolution_checked) if resolution_checked else None,
            "receipt_resolution_checks": resolution_checked,
        }
        if original_aggregate is not None:
            metrics.update({
                f"original.{key}": round(value, 4) if isinstance(value, float) else value
                for key, value in original_aggregate.items()
            })
            metrics["original.cases"] = original_aggregate["queries"]
            metrics.update({f"truth_expansion.{key}": value for key, value in expansion.pins.items()})
        if fusion_diagnostics is not None:
            metrics["fusion.mode"] = fusion_diagnostics["mode"]
            metrics["fusion.alphas"] = fusion_diagnostics["alphas"]
        if rerank_model is not None:
            metrics["rerank.model"] = rerank_model
        if search_plane is not None:
            metrics["search.plane"] = search_plane
        for status, count in sorted(rerank_statuses.items()):
            metrics[f"rerank.status.{status}"] = count
        for stratum, values in report.get("strata", {}).items():
            for key in (
                "boundary_recall@5", "boundary_recall@10", "boundary_recall@20",
                "boundary_recall@50", "boundary_mrr", "negative_false_hit_rate",
                "case_hit_rate@50",
            ):
                if key in values and values[key] is not None:
                    metrics[f"stratum.{stratum}.{key}"] = round(values[key], 4)
        result.metrics = metrics
        result.notes.append("negative_false_hit_rate is reported, not gated: recall_search is a hint engine and abstention belongs to the calling agent")
        result.notes.append("boundary_recall@50 is reported at candidate_depth 50, not gated; boundary_recall@20 stays the gate")
        result.gates = _accuracy_gates(aggregate)
        if original_aggregate is not None:
            result.gates.extend(_accuracy_gates(original_aggregate, "original."))
            result.notes.append("original.* independently scores the unchanged original validation panel from the same search calls; both panels must pass the existing gates")
        if aggregate.get("backend_error_rate", 0) >= 0.5 or (original_aggregate is not None and original_aggregate.get("backend_error_rate", 0) >= 0.5):
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
                    # Rows keep the scorer's exact result schema plus the
                    # per-arm evidence the offline fusion tuner replays.
                    saved = {
                        **row,
                        "arm_scores": arm_scores.get(row["id"], []),
                        "rerank_evidence": rerank_evidence.get(row["id"], []),
                    }
                    handle.write(json.dumps(saved, sort_keys=True) + "\n")
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
