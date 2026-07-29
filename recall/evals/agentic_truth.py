"""Private truth and boundary scoring for agentic Recall retrieval.

Questions, gold facts, evidence identities, and per-case rankings stay in
owner-only files outside Git. This module emits aggregate metrics and content
hashes only.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError, receipt_source
from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.agentic-retrieval-truth.v2"
SPLIT_COUNTS = {
    "optimize": 25,
    "validation": 15,
    "test": 20,
}
STRATUM_SPLIT_COUNTS = {
    "optimize": 5,
    "validation": 3,
    "test": 4,
}
STRATA = (
    "exact-document",
    "bounded-timeline",
    "source-specific",
    "cross-source",
    "insufficient",
)
INTENTS = (
    "project-status",
    "decision-rationale",
    "change-history",
    "incident-root-cause",
    "ownership-next-step",
)
CASE_FIELDS = {
    "id",
    "split",
    "stratum",
    "intent",
    "question",
    "answerability",
    "gold_boundaries",
    "gold_facts",
    "owner_review",
}
BOUNDARY_FIELDS = {
    "logical_document_id",
    "source_id",
    "revision",
    "receipts",
    "first_occurred_at",
    "last_occurred_at",
}
FACT_FIELDS = {"id", "description", "receipts"}
REVIEW_FIELDS = {"status", "revision"}
RESULT_FIELDS = {"id", "candidates", "latency_ms", "backend_error"}
CANDIDATE_FIELDS = {
    "logical_document_id",
    "source_id",
    "revision",
    "pointer_valid",
    "authorized",
}
CASE_ID_RE = re.compile(r"^case_[0-9a-f]{32}$")
FACT_ID_RE = re.compile(r"^fact_[0-9a-f]{32}$")
DOCUMENT_ID_RE = re.compile(r"^ldoc_[0-9a-f]{32}$")
ABSOLUTE_PATH_RE = re.compile(r"(?:^|\s)(?:/(?:home|tmp|Users|var|opt|etc)/|[A-Za-z]:\\)")
QUESTION_MAX_CHARS = 240
FACT_MAX_CHARS = 600
MAX_FACTS_PER_CASE = 5


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EvaluationInputError("gold boundary timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise EvaluationInputError("gold boundary timestamp is invalid") from error
    if parsed.utcoffset() is None:
        raise EvaluationInputError("gold boundary timestamp is invalid")
    return parsed


def _boundary_identity(value: dict[str, Any]) -> tuple[str, str]:
    """Return migration-invariant discovery identity.

    A projection rebuild may advance a logical document's revision without
    changing which source-level document was discovered. Revision is therefore
    scored as version freshness after a stable boundary match, not folded into
    retrieval identity.
    """

    return (
        value["source_id"],
        value["logical_document_id"],
    )


def _validate_boundary(value: Any) -> tuple[str, str]:
    if not isinstance(value, dict) or set(value) != BOUNDARY_FIELDS:
        raise EvaluationInputError("gold boundary schema is invalid")
    source_id = value["source_id"]
    revision = value["revision"]
    if (
        not isinstance(source_id, str)
        or not source_id
        or len(source_id) > 255
        or not isinstance(value["logical_document_id"], str)
        or DOCUMENT_ID_RE.fullmatch(value["logical_document_id"]) is None
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
    ):
        raise EvaluationInputError("gold boundary identity is invalid")
    receipts = value["receipts"]
    if (
        not isinstance(receipts, list)
        or not receipts
        or len(receipts) != len(set(receipts))
        or any(
            not isinstance(receipt, str) or receipt_source(receipt) != source_id
            for receipt in receipts
        )
    ):
        raise EvaluationInputError("gold boundary receipt is invalid")
    if _timestamp(value["first_occurred_at"]) > _timestamp(
        value["last_occurred_at"]
    ):
        raise EvaluationInputError("gold boundary time order is invalid")
    return _boundary_identity(value)


def _validate_cases(
    cases: list[dict[str, Any]],
    *,
    require_owner_approval: bool = True,
) -> dict[str, Any]:
    if not isinstance(cases, list) or len(cases) != 60:
        raise EvaluationInputError("agentic truth set must contain exactly 60 cases")
    ids: set[str] = set()
    questions: set[str] = set()
    boundary_splits: dict[tuple[str, str], str] = {}
    split_counts: Counter[str] = Counter()
    stratum_counts: Counter[str] = Counter()
    intent_counts: Counter[str] = Counter()
    matrix: Counter[tuple[str, str]] = Counter()
    answerable_cases = 0
    insufficient_cases = 0
    approved_cases = 0
    boundary_count = 0
    fact_count = 0
    receipt_count = 0
    sources: set[str] = set()

    for case in cases:
        if not isinstance(case, dict) or set(case) != CASE_FIELDS:
            raise EvaluationInputError("agentic truth case schema is invalid")
        case_id = case["id"]
        question = case["question"]
        split = case["split"]
        stratum = case["stratum"]
        intent = case["intent"]
        if (
            not isinstance(case_id, str)
            or CASE_ID_RE.fullmatch(case_id) is None
            or case_id in ids
        ):
            raise EvaluationInputError("agentic truth case identity is invalid")
        ids.add(case_id)
        if (
            not isinstance(question, str)
            or not question
            or question != question.strip()
            or any(character in question for character in "\r\n")
            or len(question) > QUESTION_MAX_CHARS
            or ABSOLUTE_PATH_RE.search(question) is not None
        ):
            raise EvaluationInputError("agentic truth question is invalid")
        question_digest = hashlib.sha256(
            question.strip().casefold().encode()
        ).hexdigest()
        if question_digest in questions:
            raise EvaluationInputError("agentic truth questions must be unique")
        questions.add(question_digest)
        if split not in SPLIT_COUNTS or stratum not in STRATA or intent not in INTENTS:
            raise EvaluationInputError(
                "agentic truth split, stratum, or intent is invalid"
            )
        split_counts[split] += 1
        stratum_counts[stratum] += 1
        intent_counts[intent] += 1
        matrix[(stratum, split)] += 1

        review = case["owner_review"]
        if (
            not isinstance(review, dict)
            or set(review) != REVIEW_FIELDS
            or review["status"] not in {"pending", "approved", "rejected"}
            or isinstance(review["revision"], bool)
            or not isinstance(review["revision"], int)
            or review["revision"] < 1
        ):
            raise EvaluationInputError("agentic truth owner review is invalid")
        if require_owner_approval and review["status"] != "approved":
            raise EvaluationInputError(
                "all agentic truth cases must be owner-approved"
            )
        approved_cases += int(review["status"] == "approved")

        boundaries = case["gold_boundaries"]
        facts = case["gold_facts"]
        if not isinstance(boundaries, list) or not isinstance(facts, list):
            raise EvaluationInputError("agentic truth gold evidence is invalid")
        if case["answerability"] == "answerable":
            if (
                not boundaries
                or not 1 <= len(facts) <= MAX_FACTS_PER_CASE
                or stratum == "insufficient"
            ):
                raise EvaluationInputError("answerable case requires gold evidence")
            answerable_cases += 1
        elif case["answerability"] == "insufficient":
            if boundaries or facts or stratum != "insufficient":
                raise EvaluationInputError(
                    "insufficient case cannot contain gold evidence"
                )
            insufficient_cases += 1
        else:
            raise EvaluationInputError("agentic truth answerability is invalid")

        boundary_receipts: set[str] = set()
        boundary_sources: set[str] = set()
        for boundary in boundaries:
            identity = _validate_boundary(boundary)
            prior_split = boundary_splits.get(identity)
            if prior_split is not None and prior_split != split:
                raise EvaluationInputError(
                    "gold boundary crosses evaluation splits"
                )
            boundary_splits[identity] = split
            boundary_receipts.update(boundary["receipts"])
            boundary_sources.add(boundary["source_id"])
            sources.add(boundary["source_id"])
            receipt_count += len(boundary["receipts"])
            boundary_count += 1
        if stratum == "cross-source" and len(boundary_sources) < 2:
            raise EvaluationInputError(
                "cross-source case requires two source boundaries"
            )

        fact_ids: set[str] = set()
        for fact in facts:
            if (
                not isinstance(fact, dict)
                or set(fact) != FACT_FIELDS
                or not isinstance(fact["id"], str)
                or FACT_ID_RE.fullmatch(fact["id"]) is None
                or fact["id"] in fact_ids
                or not isinstance(fact["description"], str)
                or not fact["description"]
                or fact["description"] != fact["description"].strip()
                or any(character in fact["description"] for character in "\r\n")
                or len(fact["description"]) > FACT_MAX_CHARS
                or fact["description"].startswith(("{", "[", "```"))
                or not isinstance(fact["receipts"], list)
                or not fact["receipts"]
                or any(
                    not isinstance(receipt, str)
                    or receipt not in boundary_receipts
                    for receipt in fact["receipts"]
                )
            ):
                raise EvaluationInputError(
                    "gold receipt must resolve inside a gold boundary"
                )
            fact_ids.add(fact["id"])
            fact_count += 1

    if dict(split_counts) != SPLIT_COUNTS:
        raise EvaluationInputError("agentic truth split counts are invalid")
    if dict(stratum_counts) != {stratum: 12 for stratum in STRATA}:
        raise EvaluationInputError("agentic truth stratum counts are invalid")
    if dict(intent_counts) != {intent: 12 for intent in INTENTS}:
        raise EvaluationInputError("agentic truth intent counts are invalid")
    if any(
        matrix[(stratum, split)] != expected
        for stratum in STRATA
        for split, expected in STRATUM_SPLIT_COUNTS.items()
    ):
        raise EvaluationInputError("agentic truth split is not stratified")
    return {
        "case_count": len(cases),
        "split_counts": dict(sorted(split_counts.items())),
        "stratum_counts": dict(sorted(stratum_counts.items())),
        "intent_counts": dict(sorted(intent_counts.items())),
        "answerable_cases": answerable_cases,
        "insufficient_cases": insufficient_cases,
        "owner_approved_cases": approved_cases,
        "gold_boundary_count": boundary_count,
        "gold_fact_count": fact_count,
        "gold_receipt_count": receipt_count,
        "source_count": len(sources),
    }


def _outside_repository(path: Path, repo_root: Path) -> None:
    repo = Path(repo_root).resolve(strict=True)
    candidate = Path(path).resolve(strict=True)
    if candidate == repo or repo in candidate.parents:
        raise EvaluationInputError(
            "private agentic evaluation files must stay outside Git"
        )


def validate_truth_set(
    path: Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    source = _private_path(path, exists=True)
    if repo_root is not None:
        _outside_repository(source, repo_root)
    cases, payload = _load_jsonl(source)
    receipt = _validate_cases(cases)
    return {
        "schema_version": SCHEMA_VERSION,
        **receipt,
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
    }


def build_owner_review_packet(
    source_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
) -> dict[str, Any]:
    """Render a private, static packet without publishing truth-set bodies."""

    source = _private_path(source_path, exists=True)
    output = _private_path(output_path, exists=False)
    _outside_repository(source, repo_root)
    repo = Path(repo_root).resolve(strict=True)
    resolved_output = output.resolve(strict=False)
    if resolved_output == repo or repo in resolved_output.parents:
        raise EvaluationInputError(
            "private agentic evaluation files must stay outside Git"
        )
    cases, payload = _load_jsonl(source)
    receipt = _validate_cases(cases, require_owner_approval=False)
    def render_case(ordinal: int, case: dict[str, Any]) -> str:
        if case["gold_facts"]:
            facts = "".join(
                f"<li>{html.escape(fact['description'])}</li>"
                for fact in case["gold_facts"]
            )
            expected = f"<ol>{facts}</ol>"
        else:
            expected = (
                "<p><b>Expected behavior:</b> say there is not enough "
                "evidence to answer confidently.</p>"
            )
        boundaries = "".join(
            "<li>"
            f"{html.escape(boundary['source_id'])} · "
            f"{html.escape(boundary['first_occurred_at'])} → "
            f"{html.escape(boundary['last_occurred_at'])}"
            "</li>"
            for boundary in case["gold_boundaries"]
        )
        source_summary = (
            f"<ul>{boundaries}</ul>"
            if boundaries
            else "<p>No gold source by design.</p>"
        )
        technical = {
            "id": case["id"],
            "owner_review": case["owner_review"],
            "gold_boundaries": case["gold_boundaries"],
            "gold_fact_receipts": [
                {"id": fact["id"], "receipts": fact["receipts"]}
                for fact in case["gold_facts"]
            ],
        }
        return (
            "<article>"
            f"<p class=\"eyebrow\">Case {ordinal} · "
            f"{html.escape(case['intent'])} · "
            f"{html.escape(case['stratum'])} · "
            f"{html.escape(case['split'])}</p>"
            f"<h2>{html.escape(case['question'])}</h2>"
            "<h3>Expected answer</h3>"
            f"{expected}"
            "<h3>Where the evidence lives</h3>"
            f"{source_summary}"
            "<label class=\"review-check\"><input type=\"checkbox\"> "
            "I reviewed this card (local checklist only)</label>"
            "<details><summary>Technical evidence IDs and receipts</summary>"
            f"<pre>{html.escape(json.dumps(technical, indent=2, sort_keys=True))}</pre>"
            "</details>"
            "</article>"
        )

    articles = "\n".join(
        render_case(ordinal, case)
        for ordinal, case in enumerate(cases, 1)
    )
    rendered = f"""<!doctype html>
<meta charset="utf-8">
<title>Private Recall truth-set owner review</title>
<style>
body {{ font: 16px/1.55 system-ui; margin: 2rem auto; max-width: 960px;
       padding: 0 1rem; color: #17202a; }}
article {{ border: 1px solid #d5d8dc; border-radius: 12px; padding: 1.25rem;
           margin: 1.25rem 0; }}
article h2 {{ margin-top: .25rem; }}
article h3 {{ margin-bottom: .25rem; }}
pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #f4f6f7;
       padding: 1rem; }}
.warning {{ color: #922b21; font-weight: 700; }}
.explainer {{ background: #eef6ff; border: 1px solid #b8d8f8;
              border-radius: 12px; padding: 1rem 1.25rem; }}
.action {{ background: #eef9f0; border-left: 5px solid #238636;
           padding: .8rem 1rem; }}
.eyebrow {{ color: #576574; font-size: .88rem; margin: 0;
            text-transform: uppercase; letter-spacing: .03em; }}
.review-check {{ display: block; background: #fff8dc; padding: .7rem;
                 margin: 1rem 0; }}
details {{ margin-top: 1rem; }}
summary {{ cursor: pointer; font-weight: 700; }}
code {{ background: #f4f6f7; padding: .1rem .3rem; }}
</style>
<h1>Private Recall truth-set owner review</h1>
<p class="warning">Private source-derived evaluation data. Do not publish or
serve this file outside the owner's trusted device.</p>
<section class="explainer">
<h2>What is this?</h2>
<p>This is a 60-question exam for the company brain. Each card shows a short
question an employee might ask, the useful answer Recall should recover, and
the source/time boundary that proves it.</p>
<h2>What do I need from you?</h2>
<ol>
<li>Would a real employee ask this question?</li>
<li>Is the expected answer correct, concise, and useful?</li>
<li>Does the source/time look like the right evidence?</li>
</ol>
<p class="action">Use each checkbox as a local reading aid. To record approval,
reply in chat with <code>approve all 60</code>, or list corrections such as
<code>Case 7: expected answer should say …</code>. This page is read-only.</p>
<p>You can ignore hashes, IDs, and receipts unless something looks wrong. They
are collapsed under each card for machine verification.</p>
</section>
{articles}
"""
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as target:
        target.write(rendered)
    return {
        "schema_version": SCHEMA_VERSION,
        "case_count": receipt["case_count"],
        "owner_approved_cases": receipt["owner_approved_cases"],
        "owner_pending_cases": sum(
            case["owner_review"]["status"] == "pending" for case in cases
        ),
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (
        ordered[upper] - ordered[lower]
    ) * (position - lower)


def _aggregate(rows: list[dict[str, Any]]) -> dict[str, int | float]:
    positives = [row for row in rows if row["answerable"]]
    negatives = [row for row in rows if not row["answerable"]]
    candidates = sum(row["candidate_count"] for row in rows)
    valid_pointers = sum(row["valid_pointer_count"] for row in rows)
    authorization_violations = sum(
        row["authorization_violation_count"] for row in rows
    )
    matched_boundaries = sum(row["matched_boundary_count"] for row in rows)
    fresh_revisions = sum(row["fresh_revision_count"] for row in rows)
    exact_revisions = sum(row["exact_revision_count"] for row in rows)
    return {
        "queries": len(rows),
        "answerable_queries": len(positives),
        "insufficient_queries": len(negatives),
        "boundary_recall@20": statistics.fmean(
            row["boundary_recall@20"] for row in positives
        )
        if positives
        else 0.0,
        "boundary_mrr": statistics.fmean(
            row["reciprocal_rank"] for row in positives
        )
        if positives
        else 0.0,
        "negative_false_hit_rate": statistics.fmean(
            float(row["candidate_count"] > 0) for row in negatives
        )
        if negatives
        else 0.0,
        "pointer_integrity": valid_pointers / candidates if candidates else 1.0,
        "authorization_violation_rate": (
            authorization_violations / candidates if candidates else 0.0
        ),
        "revision_freshness_on_match": (
            fresh_revisions / matched_boundaries
            if matched_boundaries
            else 0.0
        ),
        "revision_exact_on_match": (
            exact_revisions / matched_boundaries
            if matched_boundaries
            else 0.0
        ),
        "backend_error_rate": statistics.fmean(
            float(bool(row["backend_error"])) for row in rows
        ),
        "latency_p50_ms": _percentile(
            [row["latency_ms"] for row in rows],
            0.50,
        ),
        "latency_p95_ms": _percentile(
            [row["latency_ms"] for row in rows],
            0.95,
        ),
    }


def score_boundary_candidates(
    cases: list[dict[str, Any]],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    _validate_cases(cases)
    case_by_id = {case["id"]: case for case in cases}
    if (
        not isinstance(results, list)
        or len(results) != len(cases)
        or any(
            not isinstance(result, dict) or set(result) != RESULT_FIELDS
            for result in results
        )
    ):
        raise EvaluationInputError("boundary result schema is invalid")
    result_by_id = {result["id"]: result for result in results}
    if len(result_by_id) != len(results) or set(result_by_id) != set(case_by_id):
        raise EvaluationInputError("boundary results must cover every case once")

    rows: list[dict[str, Any]] = []
    for case_id, case in case_by_id.items():
        result = result_by_id[case_id]
        latency = result["latency_ms"]
        if (
            isinstance(latency, bool)
            or not isinstance(latency, (int, float))
            or not math.isfinite(latency)
            or latency < 0
            or not isinstance(result["backend_error"], str)
            or len(result["backend_error"]) > 160
        ):
            raise EvaluationInputError("boundary result diagnostics are invalid")
        candidates = result["candidates"]
        if not isinstance(candidates, list) or len(candidates) > 100:
            raise EvaluationInputError("boundary candidates are invalid")
        identities: list[tuple[str, str]] = []
        revisions: list[int] = []
        valid_pointer_count = 0
        authorization_violation_count = 0
        for candidate in candidates:
            if (
                not isinstance(candidate, dict)
                or set(candidate) != CANDIDATE_FIELDS
                or not isinstance(candidate["source_id"], str)
                or not candidate["source_id"]
                or not isinstance(candidate["logical_document_id"], str)
                or DOCUMENT_ID_RE.fullmatch(
                    candidate["logical_document_id"]
                ) is None
                or isinstance(candidate["revision"], bool)
                or not isinstance(candidate["revision"], int)
                or candidate["revision"] < 1
                or not isinstance(candidate["pointer_valid"], bool)
                or not isinstance(candidate["authorized"], bool)
            ):
                raise EvaluationInputError("boundary candidate is invalid")
            identities.append(_boundary_identity(candidate))
            revisions.append(candidate["revision"])
            valid_pointer_count += int(candidate["pointer_valid"])
            authorization_violation_count += int(not candidate["authorized"])
        if len(identities) != len(set(identities)):
            raise EvaluationInputError("boundary candidates contain duplicates")

        gold = {
            _boundary_identity(boundary): boundary["revision"]
            for boundary in case["gold_boundaries"]
        }
        ranked = identities[:20]
        ranked_revisions = revisions[:20]
        relevant = [identity in gold for identity in ranked]
        matched = [
            (identity, revision)
            for identity, revision in zip(
                ranked,
                ranked_revisions,
                strict=True,
            )
            if identity in gold
        ]
        first = next(
            (ordinal for ordinal, value in enumerate(relevant, 1) if value),
            None,
        )
        rows.append(
            {
                "id": case_id,
                "split": case["split"],
                "stratum": case["stratum"],
                "answerable": case["answerability"] == "answerable",
                "candidate_count": len(identities),
                "valid_pointer_count": valid_pointer_count,
                "authorization_violation_count": authorization_violation_count,
                "matched_boundary_count": len(matched),
                "fresh_revision_count": sum(
                    revision >= gold[identity]
                    for identity, revision in matched
                ),
                "exact_revision_count": sum(
                    revision == gold[identity]
                    for identity, revision in matched
                ),
                "boundary_recall@20": (
                    len(set(ranked).intersection(gold)) / len(gold)
                    if gold
                    else 0.0
                ),
                "reciprocal_rank": 0.0 if first is None else 1.0 / first,
                "latency_ms": float(latency),
                "backend_error": result["backend_error"],
            }
        )

    by_stratum: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_split: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_stratum[row["stratum"]].append(row)
        by_split[row["split"]].append(row)
    return {
        "schema_version": SCHEMA_VERSION,
        "aggregate": _aggregate(rows),
        "strata": {
            key: _aggregate(value)
            for key, value in sorted(by_stratum.items())
        },
        "splits": {
            key: _aggregate(value)
            for key, value in sorted(by_split.items())
        },
    }


def score_boundary_files(
    truth_path: Path,
    result_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
) -> dict[str, Any]:
    if not isinstance(run_id, str) or not run_id or len(run_id) > 160:
        raise EvaluationInputError("boundary run id is invalid")
    truth = _private_path(truth_path, exists=True)
    results = _private_path(result_path, exists=True)
    output = _private_path(output_path, exists=False)
    _outside_repository(truth, repo_root)
    _outside_repository(results, repo_root)
    repo = Path(repo_root).resolve(strict=True)
    resolved_output = output.resolve(strict=False)
    if resolved_output == repo or repo in resolved_output.parents:
        raise EvaluationInputError(
            "private agentic evaluation files must stay outside Git"
        )
    cases, truth_payload = _load_jsonl(truth)
    result_rows, result_payload = _load_jsonl(results)
    report = score_boundary_candidates(cases, result_rows)
    report["run_id"] = run_id
    report["pins"] = {
        "truth_sha256": hashlib.sha256(truth_payload).hexdigest(),
        "results_sha256": hashlib.sha256(result_payload).hexdigest(),
        "scorer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "git_sha": git_sha(repo),
        "git_dirty": git_dirty(repo),
    }
    payload = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(payload)
    return report


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-agentic-truth")
    commands = value.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--input", required=True)
    validate.add_argument("--repo-root", required=True)
    review = commands.add_parser("review")
    review.add_argument("--input", required=True)
    review.add_argument("--output", required=True)
    review.add_argument("--repo-root", required=True)
    score = commands.add_parser("score")
    score.add_argument("--truth", required=True)
    score.add_argument("--results", required=True)
    score.add_argument("--output", required=True)
    score.add_argument("--repo-root", required=True)
    score.add_argument("--run-id", required=True)
    return value


def main() -> None:
    args = parser().parse_args()
    try:
        if args.command == "validate":
            result = validate_truth_set(
                Path(args.input),
                repo_root=Path(args.repo_root),
            )
        elif args.command == "review":
            result = build_owner_review_packet(
                Path(args.input),
                Path(args.output),
                repo_root=Path(args.repo_root),
            )
        else:
            result = score_boundary_files(
                Path(args.truth),
                Path(args.results),
                Path(args.output),
                repo_root=Path(args.repo_root),
                run_id=args.run_id,
            )
    except EvaluationInputError as error:
        raise SystemExit(f"agentic truth rejected: {error}") from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
