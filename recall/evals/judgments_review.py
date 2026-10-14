"""Prepare an offline W5 review packet; never approve labels or change the scorer.

Inputs are the frozen 60-case truth set and saved systems-card probe JSONL files.
Optional question proposals use the truth case schema with pending owner review.
Optional label rows contain id, source_id, logical_document_id, revision, evidence
([{receipt, text}]), and proposal (null or {relevance_probability, model}). Optional
native-family rows contain source_id, logical_document_id, and family_id.

All files are private and outside Git. Only validation questions are rendered;
questions and candidates from optimize/test families are withheld. Family counts
are a conservative clustering aid, not proof that questions are independent.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from .agentic_truth import (
    CASE_FIELDS, CASE_ID_RE, DOCUMENT_ID_RE, FACT_ID_RE, INTENTS, STRATA,
    _validate_boundary, _validate_cases,
)
from .boundary_identity import boundary_revision, stable_boundary_identity
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError, receipt_identity, receipt_source


SCHEMA_VERSION = "recall.judgments-review.v1"
TARGET_ANSWERABLE = 40


def _private_file(path: Path, repo: Path, *, exists: bool = True) -> Path:
    resolved = _private_path(path, exists=exists)
    if resolved == repo or repo in resolved.parents:
        raise EvaluationInputError("private judgment review files must stay outside Git")
    return resolved


def _question_key(question: str) -> str:
    return hashlib.sha256(" ".join(question.casefold().split()).encode()).hexdigest()


def _candidate_key(row: dict[str, Any]) -> tuple[str, str, int]:
    try:
        source, document = stable_boundary_identity(row)
        revision = boundary_revision(row)
    except (ValueError, AttributeError) as error:
        raise EvaluationInputError("review candidate identity is invalid") from error
    if len(source) > 255 or DOCUMENT_ID_RE.fullmatch(document) is None:
        raise EvaluationInputError("review candidate identity is invalid")
    return source, document, revision


def _validate_draft(case: dict[str, Any]) -> None:
    if set(case) != CASE_FIELDS:
        raise EvaluationInputError("question proposal must use the truth case schema")
    if (
        case.get("owner_review") != {"status": "pending", "revision": 1}
        or isinstance(case["owner_review"]["revision"], bool)
    ):
        raise EvaluationInputError("new question proposals must remain pending at revision 1")
    if (
        not isinstance(case["id"], str) or CASE_ID_RE.fullmatch(case["id"]) is None
        or case["split"] != "validation" or case["stratum"] not in STRATA
        or case["intent"] not in INTENTS or not isinstance(case["question"], str)
        or not case["question"].strip() or len(case["question"]) > 240
        or any(c in case["question"] for c in "\r\n")
        or not isinstance(case["gold_boundaries"], list)
        or not isinstance(case["gold_facts"], list)
    ):
        raise EvaluationInputError("question proposal schema is invalid")
    boundaries, facts = case["gold_boundaries"], case["gold_facts"]
    receipts = set()
    for boundary in boundaries:
        _validate_boundary(boundary)
        receipts.update(boundary["receipts"])
    if case["answerability"] == "answerable":
        if not boundaries or not 1 <= len(facts) <= 5 or case["stratum"] == "insufficient":
            raise EvaluationInputError("answerable proposal requires proposed gold evidence")
        if case["stratum"] == "cross-source" and len({b["source_id"] for b in boundaries}) < 2:
            raise EvaluationInputError("cross-source proposal requires two sources")
        for fact in facts:
            if (
                not isinstance(fact, dict) or set(fact) != {"id", "description", "receipts"}
                or not isinstance(fact["id"], str) or FACT_ID_RE.fullmatch(fact["id"]) is None
                or not isinstance(fact["description"], str) or not fact["description"].strip()
                or len(fact["description"]) > 600 or not isinstance(fact["receipts"], list)
                or not fact["receipts"] or any(not isinstance(r, str) or r not in receipts for r in fact["receipts"])
            ):
                raise EvaluationInputError("proposed fact must reference its proposed boundary")
    elif case["answerability"] != "insufficient" or boundaries or facts or case["stratum"] != "insufficient":
        raise EvaluationInputError("question proposal answerability is invalid")


def _coverage(cases: list[dict], family_keys: dict[str, set], verified_cases: set[str]) -> tuple[dict, dict]:
    questions: dict[str, list[str]] = defaultdict(list)
    families: dict[tuple, list[str]] = defaultdict(list)
    answerable = {c["id"] for c in cases if c["answerability"] == "answerable"}
    for case in cases:
        questions[_question_key(case["question"])].append(case["id"])
        for family in family_keys[case["id"]]:
            families[family].append(case["id"])
    parents = {case_id: case_id for case_id in answerable}

    def root(case_id: str) -> str:
        while parents[case_id] != case_id:
            parents[case_id] = parents[parents[case_id]]
            case_id = parents[case_id]
        return case_id

    for group in [*questions.values(), *families.values()]:
        members = [case_id for case_id in group if case_id in answerable]
        for case_id in members[1:]:
            parents[root(case_id)] = root(members[0])
    approved = sum(c["id"] in answerable and c["owner_review"]["status"] == "approved" for c in cases)
    clusters = len({root(case_id) for case_id in answerable & verified_cases})
    duplicate_groups = [members for members in questions.values() if len(members) > 1]
    shared_groups = [members for members in families.values() if len(members) > 1]
    return {
        "validation_questions": len(cases),
        "approved_answerable_questions": approved,
        "proposed_answerable_questions": len(answerable) - approved,
        "unique_answerable_questions": len({_question_key(c["question"]) for c in cases if c["id"] in answerable}),
        "answerable_family_clusters": clusters,
        "answerable_questions_with_unresolved_families": len(answerable - verified_cases),
        "duplicate_question_groups": len(duplicate_groups),
        "shared_family_groups": len(shared_groups),
        "approved_question_gap": max(0, TARGET_ANSWERABLE - approved),
        "additional_question_gap_after_proposals": max(0, TARGET_ANSWERABLE - clusters),
    }, {"duplicate_questions": duplicate_groups, "shared_families": shared_groups}


def prepare_review(
    truth_path: Path, result_paths: list[Path], output_path: Path, *, repo_root: Path,
    questions_path: Path | None = None, labels_path: Path | None = None,
    families_path: Path | None = None,
) -> dict[str, Any]:
    """Union candidate pools with known gold and render a private read-only packet."""
    repo = Path(repo_root).resolve(strict=True)
    output = _private_file(output_path, repo, exists=False)
    digests = []

    def load(path: Path) -> list[dict]:
        rows, payload = _load_jsonl(_private_file(path, repo))
        digests.append(hashlib.sha256(payload).hexdigest())
        return rows

    cases = load(truth_path)
    _validate_cases(cases)
    frozen_receipt_documents = {}
    protected_receipts = set()
    for case in cases:
        for boundary in case["gold_boundaries"]:
            identity = stable_boundary_identity(boundary)
            for receipt in boundary["receipts"]:
                normalized = receipt_identity(receipt)
                prior = frozen_receipt_documents.setdefault(normalized, identity)
                if prior != identity:
                    raise EvaluationInputError("frozen receipt boundary association is ambiguous")
                if case["split"] != "validation":
                    protected_receipts.add(normalized)
    drafts = load(questions_path) if questions_path is not None else []
    for case in drafts:
        _validate_draft(case)
    all_cases = {c["id"]: c for c in [*cases, *drafts]}
    if len(all_cases) != len(cases) + len(drafts):
        raise EvaluationInputError("question proposal IDs must be unique and new")
    native_families = {}
    for row in load(families_path) if families_path is not None else []:
        if set(row) != {"source_id", "logical_document_id", "family_id"}:
            raise EvaluationInputError("native family map schema is invalid")
        source, document, _ = _candidate_key({**row, "revision": 1})
        if not isinstance(row["family_id"], str) or not row["family_id"] or len(row["family_id"]) > 255:
            raise EvaluationInputError("native family identity is invalid")
        if (source, document) in native_families:
            raise EvaluationInputError("native family map contains duplicate identities")
        native_families[(source, document)] = row["family_id"]

    def family(row: dict) -> tuple:
        identity = stable_boundary_identity(row)
        return ("native", native_families[identity]) if identity in native_families else ("document", *identity)

    family_keys = {c["id"]: {family(b) for b in c["gold_boundaries"]} for c in all_cases.values()}
    protected = {family(b) for c in cases if c["split"] != "validation" for b in c["gold_boundaries"]}
    protected_boundaries = {stable_boundary_identity(b) for c in cases if c["split"] != "validation" for b in c["gold_boundaries"]}
    protected_families_resolved = protected_boundaries <= native_families.keys()
    verified_cases = {
        c["id"] for c in all_cases.values()
        if protected_families_resolved
        and all(stable_boundary_identity(b) in native_families for b in c["gold_boundaries"])
    }
    protected_questions = {_question_key(c["question"]) for c in cases if c["split"] != "validation"}
    requested = [c for c in all_cases.values() if c["split"] == "validation"]
    selected = [
        c for c in requested
        if not (family_keys[c["id"]] & protected)
        and _question_key(c["question"]) not in protected_questions
        and not any(receipt_identity(r) in protected_receipts for b in c["gold_boundaries"] for r in b["receipts"])
        and (c["owner_review"]["status"] == "approved" or c["id"] in verified_cases)
    ]
    for case in selected:
        for boundary in case["gold_boundaries"]:
            for receipt in boundary["receipts"]:
                known = frozen_receipt_documents.get(receipt_identity(receipt))
                if known is not None and known != stable_boundary_identity(boundary):
                    raise EvaluationInputError("proposal receipt does not match its frozen boundary")
    summary, groups = _coverage(selected, family_keys, verified_cases)
    pools: dict[str, dict[tuple, dict]] = {c["id"]: {} for c in selected}

    def add(case_id: str, row: dict, origin: str) -> None:
        key = _candidate_key(row)
        pooled = pools[case_id].setdefault(key, {
            "source_id": key[0], "logical_document_id": key[1], "revision": key[2],
            "origins": [], "evidence": [], "proposal": None,
            "native_family_verified": key[:2] in native_families and protected_families_resolved,
        })
        if origin not in pooled["origins"]:
            pooled["origins"].append(origin)

    for case in selected:
        for boundary in case["gold_boundaries"]:
            add(case["id"], boundary, "known-gold" if case["owner_review"]["status"] == "approved" else "proposed-gold")
    if len(result_paths) > 16:
        raise EvaluationInputError("review supports at most 16 saved candidate files")
    withheld_candidates = 0
    for ordinal, path in enumerate(result_paths, 1):
        for row in load(path):
            if row.get("id") not in all_cases or not isinstance(row.get("candidates"), list):
                raise EvaluationInputError("saved probe must reference known cases and candidates")
            if row["id"] not in pools:
                continue
            if len(row["candidates"]) > 100:
                raise EvaluationInputError("saved probe exceeds candidate bound")
            for rank, candidate in enumerate(row["candidates"], 1):
                _candidate_key(candidate)
                if candidate.get("authorized") is not True or candidate.get("pointer_valid") is not True or family(candidate) in protected:
                    withheld_candidates += 1
                    continue
                add(row["id"], candidate, f"saved-probe-{ordinal}:rank-{rank}")
    labeled = set()
    for row in load(labels_path) if labels_path is not None else []:
        if set(row) != {"id", "source_id", "logical_document_id", "revision", "evidence", "proposal"}:
            raise EvaluationInputError("candidate label proposal schema is invalid")
        if row["id"] in all_cases and row["id"] not in pools:
            continue
        key = _candidate_key(row)
        if row["id"] not in pools or key not in pools[row["id"]]:
            raise EvaluationInputError("label proposal must reference a pooled candidate")
        if (row["id"], key) in labeled:
            raise EvaluationInputError("candidate label proposals must be unique")
        labeled.add((row["id"], key))
        evidence = row["evidence"]
        if not isinstance(evidence, list) or len(evidence) > 10:
            raise EvaluationInputError("candidate source evidence is invalid")
        annotated_evidence = []
        for excerpt in evidence:
            if (
                not isinstance(excerpt, dict) or set(excerpt) != {"receipt", "text"}
                or not isinstance(excerpt["receipt"], str) or receipt_source(excerpt["receipt"]) != key[0]
                or not isinstance(excerpt["text"], str) or not excerpt["text"].strip()
                or len(excerpt["text"]) > 20000
            ):
                raise EvaluationInputError("source evidence requires a matching receipt and bounded text")
            normalized = receipt_identity(excerpt["receipt"])
            if normalized in protected_receipts:
                raise EvaluationInputError("source evidence cannot contain a protected receipt")
            known = frozen_receipt_documents.get(normalized)
            if known is not None and known != key[:2]:
                raise EvaluationInputError("source receipt does not match its frozen boundary")
            if not pools[row["id"]][key]["native_family_verified"]:
                raise EvaluationInputError("source evidence requires resolved reference and candidate families")
            annotated_evidence.append({
                **excerpt,
                "receipt_association": "frozen-truth" if known else "supplied-unverified",
            })
        proposal = row["proposal"]
        if proposal is not None:
            if not isinstance(proposal, dict) or set(proposal) != {"relevance_probability", "model"}:
                raise EvaluationInputError("candidate probability proposal schema is invalid")
            probability = proposal["relevance_probability"]
            if (
                isinstance(probability, bool) or not isinstance(probability, (float, int))
                or not math.isfinite(probability) or not 0 <= probability <= 1
                or not isinstance(proposal["model"], str) or not proposal["model"].strip()
            ):
                raise EvaluationInputError("candidate probability proposal is invalid")
        pools[row["id"]][key].update(evidence=annotated_evidence, proposal=proposal)
    candidates = [row for pool in pools.values() for row in pool.values()]
    reference_boundaries = {stable_boundary_identity(b) for c in all_cases.values() for b in c["gold_boundaries"]}
    candidate_boundaries = {stable_boundary_identity(c) for c in candidates}
    unresolved_references = reference_boundaries - native_families.keys()
    unresolved_candidates = candidate_boundaries - native_families.keys()
    summary = {
        "schema_version": SCHEMA_VERSION, **summary,
        "withheld_questions": len(requested) - len(selected),
        "withheld_candidates": withheld_candidates,
        "candidate_count": len(candidates),
        "candidate_label_proposals": sum(c["proposal"] is not None for c in candidates),
        "candidates_with_source_evidence": sum(bool(c["evidence"]) for c in candidates),
        "candidates_missing_source_evidence": sum(not c["evidence"] for c in candidates),
        "unverified_excerpt_associations": sum(e["receipt_association"] == "supplied-unverified" for c in candidates for e in c["evidence"]),
        "unresolved_reference_families": len(unresolved_references),
        "unresolved_candidate_families": len(unresolved_candidates),
        "native_family_audit_complete": not unresolved_references and not unresolved_candidates,
        "input_sha256": digests,
    }
    rendered = _render(selected, pools, summary, groups)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as target:
        target.write(rendered)
    return summary


def _render(cases: list[dict], pools: dict, summary: dict, groups: dict) -> str:
    escape = html.escape
    pending_sections, reference_sections = [], []
    for case in sorted(cases, key=lambda value: value["owner_review"]["status"] == "approved"):
        # Put the work awaiting review before the frozen reference panel.
        proposed = case["owner_review"]["status"] != "approved"
        candidates = []
        for row in pools[case["id"]].values():
            excerpts = "".join(
                f"<p>Supplied excerpt; verify its contents at the receipt. Association: {escape(e['receipt_association'])}.</p>"
                f"<blockquote>{escape(e['text'])}</blockquote><code>{escape(e['receipt'])}</code>"
                for e in row["evidence"]
            )
            if not excerpts:
                excerpts = "<p>Source excerpts are missing. Open and verify the source before approving a label.</p>"
            proposal = row["proposal"]
            label = f"Proposed relevance: {proposal['relevance_probability']:.3f} ({escape(proposal['model'])}); awaiting owner review." if proposal else "No model label supplied."
            candidates.append(
                "<details><summary>" + escape(", ".join(row["origins"])) + "</summary>"
                f"<p>{label}</p>{excerpts}<pre>{escape(json.dumps(row, ensure_ascii=False, indent=2))}</pre></details>"
            )
        facts = "".join(f"<li>{escape(f['description'])}</li>" for f in case["gold_facts"])
        (pending_sections if proposed else reference_sections).append(
            "<article><p>" + ("Proposed question — awaiting owner review" if proposed else "Frozen owner-approved question") + "</p>"
            f"<h2>{escape(case['question'])}</h2><p>{escape(case['intent'])} · {escape(case['stratum'])}</p>"
            f"<h3>{'Proposed answer' if proposed else 'Frozen expected answer'}</h3>"
            + (f"<ul>{facts}</ul>" if facts else "<p>Insufficient evidence is expected.</p>")
            + "<h3>Pooled candidates and source evidence</h3>" + "".join(candidates)
            + f"<details><summary>Question identity and evidence</summary><pre>{escape(json.dumps(case, ensure_ascii=False, indent=2))}</pre></details></article>"
        )
    return """<!doctype html><meta charset="utf-8"><title>Private W5 judgment review</title>
<style>body{font:16px/1.5 system-ui;max-width:1000px;margin:2rem auto;padding:0 1rem}
article{border:1px solid #aaa;padding:1rem;margin:1rem 0}pre{white-space:pre-wrap;
overflow-wrap:anywhere;background:#f5f5f5;padding:.8rem}details{margin:.8rem 0}
blockquote{white-space:pre-wrap;border-left:3px solid #aaa;padding-left:1rem}</style>
<h1>Private W5 judgment review</h1><p>Keep this source-derived packet on the owner's trusted device.</p>
<p>No approvals are recorded by this packet. Proposed questions and model labels remain pending.
Frozen expected answers are shown for comparison; the original truth set is unchanged.</p>
<p>Review whether each question is useful, whether the shown source supports the answer, and whether
each candidate is relevant. Correct errors before recording approval separately. Missing source
excerpts and unknown native session relationships are unfinished review work.</p>
<p>Candidate counts do not increase question coverage. Family clusters group known shared documents,
provided native families, and exact normalized question duplicates; they do not prove independence.</p>
""" + f"<pre>{escape(json.dumps(summary, indent=2))}</pre>" + "".join(pending_sections) + (
        "<details><summary>Frozen approved reference panel</summary>" + "".join(reference_sections) + "</details>"
    ) + (
        "<details><summary>Duplicate questions and shared session families</summary>"
        f"<pre>{escape(json.dumps(groups, indent=2))}</pre></details>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--truth", required=True)
    parser.add_argument("--results", nargs="*", default=[])
    parser.add_argument("--output", required=True)
    parser.add_argument("--questions")
    parser.add_argument("--labels")
    parser.add_argument("--families")
    args = parser.parse_args()
    try:
        receipt = prepare_review(
            Path(args.truth), [Path(p) for p in args.results], Path(args.output),
            repo_root=Path(__file__).resolve().parents[2],
            questions_path=Path(args.questions) if args.questions else None,
            labels_path=Path(args.labels) if args.labels else None,
            families_path=Path(args.families) if args.families else None,
        )
    except (EvaluationInputError, OSError, ValueError, TypeError) as error:
        parser.exit(2, f"review preparation failed ({type(error).__name__}); no approval was recorded\n")
    print(json.dumps(receipt, sort_keys=True))


if __name__ == "__main__":
    main()
