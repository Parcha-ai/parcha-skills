"""Offline calibration against source-reviewed selected-passage labels.

This is separate from retrieval truth: absent evidence does not establish whole-
document irrelevance. Callers own source-lineage and quote-to-receipt attestation.
The scorer checks exact supplied bytes, reviewer witnesses and family exclusions;
it never approves labels, changes gold, dispatches models or chooses a threshold.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from .expanded_truth import canonical_sha256
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError, receipt_source

SCHEMA = "recall.candidate-review.v1"
LABELS = {"answers_query", "does_not_answer", "insufficient_evidence"}
EVIDENCE_FIELDS = {"id", "case_id", "question", "source_id", "logical_document_id", "text", "context", "receipts", "families", "complete"}
REVIEW_FIELDS = {"id", "evidence_sha256", "reviewer", "label", "rationale", "witnesses"}
PREDICTION_FIELDS = {"id", "evidence_sha256", "probability", "error"}


def evidence_digest(value: Any) -> str:
    """Pin the full ordered input, including query, provenance and completeness."""
    try:
        return canonical_sha256(value)
    except (ValueError, TypeError):
        raise EvaluationInputError("candidate evidence cannot be hashed") from None


def _require(condition: bool) -> None:
    if not condition:
        raise EvaluationInputError("candidate review contract is invalid")


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _strings(value: Any) -> bool:
    return isinstance(value, list) and all(_text(x) for x in value) and len(set(value)) == len(value)


def _index(rows: list[dict], fields: set[str]) -> dict[str, dict]:
    _require(isinstance(rows, list))
    out = {}
    for row in rows:
        _require(isinstance(row, dict) and set(row) == fields and _text(row.get("id")))
        _require(row["id"] not in out)
        out[row["id"]] = row
    return out


def score_reviewed_candidates(
    evidence: list[dict], reviews: list[dict], predictions: list[dict], *,
    expected_evidence_sha256: str, protected_families: list[str],
) -> dict[str, Any]:
    """Score one fixed prediction pass; retain every pool slot in coverage.

    Reviews and predictions must pin their individual evidence row. Missing rows
    are counted; unknown IDs, stale pins and duplicate observations fail closed.
    An ambiguous label is excluded from binary math, never converted to negative.
    The fixed 0.5 split is a diagnostic, not a recommended deployment threshold.
    """
    _require(_strings(protected_families))
    pin = evidence_digest(evidence)
    _require(pin == expected_evidence_sha256)
    pool = _index(evidence, EVIDENCE_FIELDS)
    _require(bool(pool))
    by_review = _index(reviews, REVIEW_FIELDS)
    by_prediction = _index(predictions, PREDICTION_FIELDS)
    _require(set(by_review) <= set(pool) and set(by_prediction) <= set(pool))
    queries, boundaries = {}, set()
    for row in evidence:
        _require(all(_text(row[k]) for k in ("case_id", "question", "source_id", "logical_document_id", "text")))
        _require(_strings(row["receipts"]) and bool(row["receipts"]) and _strings(row["families"]))
        _require(isinstance(row["context"], dict))
        _require(type(row["complete"]) is bool)
        _require(all(receipt_source(r) == row["source_id"] for r in row["receipts"]))
        _require(queries.setdefault(row["case_id"], row["question"]) == row["question"])
        boundary = (row["case_id"], row["source_id"], row["logical_document_id"])
        _require(boundary not in boundaries)
        boundaries.add(boundary)
    for row in reviews:
        source = pool[row["id"]]
        _require(row["evidence_sha256"] == evidence_digest(source))
        _require(_text(row["reviewer"]) and _text(row["rationale"]) and _text(row["label"]) and row["label"] in LABELS)
        _require(isinstance(row["witnesses"], list))
        _require(row["label"] != "answers_query" or bool(row["witnesses"]))
        for quote in row["witnesses"]:
            _require(isinstance(quote, dict) and set(quote) == {"start", "end", "quote", "receipt"})
            start, end = quote["start"], quote["end"]
            _require(type(start) is int and type(end) is int and 0 <= start < end <= len(source["text"]))
            _require(source["text"][start:end] == quote["quote"] and quote["receipt"] in source["receipts"])
    for row in predictions:
        _require(row["evidence_sha256"] == evidence_digest(pool[row["id"]]))
        p, error = row["probability"], row["error"]
        _require((error is None and type(p) in (int, float) and 0 <= p <= 1 and math.isfinite(p))
                 or (_text(error) and p is None))

    coverage = dict.fromkeys([
        "candidates", "eligible_candidates", "protected_candidates", "unresolved_family_candidates",
        "incomplete_evidence_candidates", "reviewed_candidates", "unreviewed_candidates", "ambiguous_reviews",
        "binary_eligible_reviews", "missing_predictions", "prediction_errors", "scored_predictions",
        "positive_labels_scored", "negative_labels_scored",
    ], 0)
    confusion = dict.fromkeys(["true_positive", "true_negative", "false_positive", "false_negative"], 0)
    squared_errors = []
    for key, source in pool.items():
        protected = bool(set(source["families"]) & set(protected_families))
        unresolved, incomplete = not source["families"], not source["complete"]
        eligible = not (protected or unresolved or incomplete)
        review, prediction = by_review.get(key), by_prediction.get(key)
        coverage["candidates"] += 1
        coverage["protected_candidates"] += protected
        coverage["unresolved_family_candidates"] += unresolved
        coverage["incomplete_evidence_candidates"] += incomplete
        coverage["eligible_candidates"] += eligible
        coverage["reviewed_candidates"] += review is not None
        coverage["unreviewed_candidates"] += review is None
        coverage["missing_predictions"] += prediction is None
        coverage["prediction_errors"] += prediction is not None and prediction["error"] is not None
        if review is None:
            continue
        unknown = review["label"] == "insufficient_evidence"
        coverage["ambiguous_reviews"] += unknown
        if not eligible or unknown:
            continue
        coverage["binary_eligible_reviews"] += 1
        if prediction is None or prediction["error"] is not None:
            continue
        positive = review["label"] == "answers_query"
        probability = prediction["probability"]
        predicted = probability >= .5
        coverage["scored_predictions"] += 1
        coverage["positive_labels_scored" if positive else "negative_labels_scored"] += 1
        confusion[("true_" if predicted == positive else "false_") + ("positive" if predicted else "negative")] += 1
        squared_errors.append((probability - int(positive)) ** 2)
    return {
        "schema_version": SCHEMA, "evidence_sha256": pin, "cases": len(queries),
        "reviews_sha256": evidence_digest(reviews), "predictions_sha256": evidence_digest(predictions),
        "protected_families_sha256": evidence_digest(sorted(protected_families)),
        "coverage": coverage, "diagnostic_at_0_5": confusion,
        "brier_available": sum(squared_errors) / len(squared_errors) if squared_errors else None,
        "label_scope": "supplied_selected_passages", "whole_document_relevance_claimed": False,
        "production_threshold_earned": False,
        "limitations": [
            "Reviewer identity, lineage and quote-to-receipt attribution are caller attestations, not independently authenticated here.",
            "Coverage counts overlap; withheld rows remain in the pool denominator, not in calibration arithmetic.",
            "Selected and related candidates are correlated; this report is not population calibration or retrieval improvement.",
            "Missing answers in selected evidence do not prove the complete document cannot answer.",
        ],
    }


def _load(path: str) -> list[dict]:
    private = _private_path(Path(path), exists=True)
    if any((parent / ".git").exists() for parent in private.parents):
        raise EvaluationInputError("candidate inputs must stay outside git")
    rows, _ = _load_jsonl(private)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("evidence", "reviews", "predictions", "protected-families", "expected-evidence-sha256"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    try:
        families = _load(args.protected_families)
        _require(all(isinstance(r, dict) and set(r) == {"family_id"} for r in families))
        report = score_reviewed_candidates(
            _load(args.evidence), _load(args.reviews), _load(args.predictions),
            expected_evidence_sha256=args.expected_evidence_sha256,
            protected_families=[r["family_id"] for r in families],
        )
    except (OSError, ValueError, TypeError, KeyError):
        parser.exit(2, "candidate review failed validation; no score emitted\n")
    print(json.dumps(report, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
