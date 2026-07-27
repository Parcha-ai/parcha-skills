"""Produce private logical-boundary rankings from an authorized Recall runtime.

Questions and per-case candidates remain in owner-only files outside Git. The
only printable result is an aggregate, content-free execution receipt. Owner
approval and gold scoring intentionally remain separate in ``agentic_truth``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .agentic_truth import (
    CANDIDATE_FIELDS,
    CASE_ID_RE,
    DOCUMENT_ID_RE,
    _outside_repository,
)
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError
from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.agentic-boundary-rankings.v1"
AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")
MAX_CASES = 500
MAX_CANDIDATES = 20


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


def _questions(
    path: Path,
    *,
    repo_root: Path,
    expected_cases: int,
) -> tuple[list[dict[str, str]], bytes]:
    source = _private_path(path, exists=True)
    _outside_repository(source, repo_root)
    rows, payload = _load_jsonl(source)
    if (
        isinstance(expected_cases, bool)
        or not isinstance(expected_cases, int)
        or not 1 <= expected_cases <= MAX_CASES
        or len(rows) != expected_cases
    ):
        raise EvaluationInputError("private ranking case count is invalid")
    cases: list[dict[str, str]] = []
    ids: set[str] = set()
    question_digests: set[str] = set()
    for row in rows:
        case_id = row.get("id")
        question = row.get("question")
        if (
            not isinstance(case_id, str)
            or CASE_ID_RE.fullmatch(case_id) is None
            or case_id in ids
            or not isinstance(question, str)
            or not question.strip()
            or len(question.encode()) > 32_768
        ):
            raise EvaluationInputError("private ranking question is invalid")
        question_digest = hashlib.sha256(
            question.strip().casefold().encode()
        ).hexdigest()
        if question_digest in question_digests:
            raise EvaluationInputError("private ranking questions must be unique")
        ids.add(case_id)
        question_digests.add(question_digest)
        cases.append({"id": case_id, "question": question})
    return cases, payload


def resolve_logical_boundaries(
    results: list[dict[str, Any]],
    *,
    tenant_id: str,
    authorized_sources: tuple[str, ...],
    lookup: Callable[
        [tuple[tuple[str, str], ...]],
        dict[tuple[str, str], dict[str, Any]],
    ],
) -> list[dict[str, Any]]:
    """Collapse ranked event hits to current logical-document boundaries."""

    if not isinstance(results, list):
        raise EvaluationInputError("Recall search response is invalid")
    keys: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for result in results:
        if not isinstance(result, dict):
            raise EvaluationInputError("Recall search result is invalid")
        source_id = result.get("source_id")
        native_id = result.get("native_id")
        native_parent_id = result.get("native_parent_id")
        if (
            not isinstance(source_id, str)
            or AUTHORITY_RE.fullmatch(source_id) is None
            or not isinstance(native_id, str)
            or not native_id
            or (
                native_parent_id is not None
                and (not isinstance(native_parent_id, str) or not native_parent_id)
            )
        ):
            raise EvaluationInputError("Recall search result boundary is invalid")
        key = (source_id, native_parent_id or native_id)
        if key not in seen:
            seen.add(key)
            keys.append(key)
        if len(keys) == MAX_CANDIDATES:
            break
    catalog = lookup(tuple(keys))
    candidates: list[dict[str, Any]] = []
    authorized = set(authorized_sources)
    for key in keys:
        source_id, native_parent_id = key
        expected_id = _logical_document_id(
            tenant_id,
            source_id,
            native_parent_id,
        )
        row = catalog.get(key)
        pointer_valid = bool(
            row is not None
            and row.get("logical_document_id") == expected_id
            and isinstance(row.get("revision"), int)
            and not isinstance(row.get("revision"), bool)
            and row["revision"] >= 1
        )
        candidates.append(
            {
                "logical_document_id": expected_id,
                "source_id": source_id,
                "revision": row["revision"] if pointer_valid else 1,
                "pointer_valid": pointer_valid,
                "authorized": source_id in authorized,
            }
        )
    return candidates


def _logical_document_id(
    tenant_id: str,
    source_id: str,
    native_parent_id: str,
) -> str:
    # Import lazily so the content-free runner remains unit-testable without
    # loading the service runtime or its storage clients.
    from recall_server.logical_evidence import logical_document_id

    return logical_document_id(tenant_id, source_id, native_parent_id)


def _validated_candidates(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > MAX_CANDIDATES:
        raise EvaluationInputError("boundary candidates are invalid")
    for candidate in value:
        if (
            not isinstance(candidate, dict)
            or set(candidate) != CANDIDATE_FIELDS
            or not isinstance(candidate["logical_document_id"], str)
            or DOCUMENT_ID_RE.fullmatch(candidate["logical_document_id"]) is None
            or not isinstance(candidate["source_id"], str)
            or AUTHORITY_RE.fullmatch(candidate["source_id"]) is None
            or isinstance(candidate["revision"], bool)
            or not isinstance(candidate["revision"], int)
            or candidate["revision"] < 1
            or not isinstance(candidate["pointer_valid"], bool)
            or not isinstance(candidate["authorized"], bool)
        ):
            raise EvaluationInputError("boundary candidate is invalid")
    return value


def rank_private_questions(
    input_path: Path,
    output_path: Path,
    *,
    search: Callable[[str], dict[str, Any]],
    resolve: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    repo_root: Path,
    run_id: str,
    workers: int = 4,
    expected_cases: int = 60,
) -> dict[str, Any]:
    """Run private questions and persist only closed-schema boundary rankings."""

    if not isinstance(run_id, str) or not run_id or len(run_id) > 160:
        raise EvaluationInputError("private ranking run id is invalid")
    if (
        isinstance(workers, bool)
        or not isinstance(workers, int)
        or not 1 <= workers <= 8
    ):
        raise EvaluationInputError("private ranking worker count is invalid")
    cases, input_payload = _questions(
        input_path,
        repo_root=repo_root,
        expected_cases=expected_cases,
    )
    output = _private_path(output_path, exists=False)
    resolved_repo = Path(repo_root).resolve(strict=True)
    resolved_output = output.resolve(strict=False)
    if resolved_output == resolved_repo or resolved_repo in resolved_output.parents:
        raise EvaluationInputError(
            "private agentic evaluation files must stay outside Git"
        )

    def run(case: dict[str, str]) -> dict[str, Any]:
        started = time.monotonic()
        try:
            response = search(case["question"])
            results = response.get("results") if isinstance(response, dict) else None
            candidates = _validated_candidates(resolve(results))
            error = ""
        except Exception as failure:
            candidates = []
            error = type(failure).__name__[:160]
        return {
            "id": case["id"],
            "candidates": candidates,
            "latency_ms": round((time.monotonic() - started) * 1000, 3),
            "backend_error": error,
        }

    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="recall-boundary-rank",
    ) as executor:
        rows = list(executor.map(run, cases))
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    ).encode()
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(payload)

    candidates = sum(len(row["candidates"]) for row in rows)
    valid = sum(
        int(candidate["pointer_valid"])
        for row in rows
        for candidate in row["candidates"]
    )
    unauthorized = sum(
        int(not candidate["authorized"])
        for row in rows
        for candidate in row["candidates"]
    )
    latencies = [float(row["latency_ms"]) for row in rows]
    errors = sum(bool(row["backend_error"]) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "case_count": len(rows),
        "candidate_count": candidates,
        "backend_error_count": errors,
        "backend_error_rate": errors / len(rows),
        "pointer_integrity": valid / candidates if candidates else 1.0,
        "authorization_violation_rate": (
            unauthorized / candidates if candidates else 0.0
        ),
        "latency_p50_ms": _percentile(latencies, 0.50),
        "latency_p95_ms": _percentile(latencies, 0.95),
        "pins": {
            "input_sha256": hashlib.sha256(input_payload).hexdigest(),
            "rankings_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(Path(repo_root)),
            "git_dirty": git_dirty(Path(repo_root)),
            "python": platform.python_version(),
        },
    }


def _live_rankings(
    *,
    dsn: str,
    tenant_id: str,
    source_ids: tuple[str, ...],
    input_path: Path,
    output_path: Path,
    repo_root: Path,
    run_id: str,
    workers: int,
) -> dict[str, Any]:
    from recall_server.canonical_retrieval import BoundCanonicalRetrieval
    from recall_server.db import BrainStore
    from recall_server.semantic import SemanticRuntime

    if (
        AUTHORITY_RE.fullmatch(tenant_id) is None
        or not source_ids
        or len(source_ids) != len(set(source_ids))
        or any(AUTHORITY_RE.fullmatch(value) is None for value in source_ids)
    ):
        raise EvaluationInputError("private ranking authority is invalid")
    store = BrainStore(
        dsn,
        semantic_runtime=SemanticRuntime.from_env(),
        pool_max_size=max(4, workers * 2),
    )
    retrieval = BoundCanonicalRetrieval(
        store,
        tenant_id=tenant_id,
        principal_id="private-eval",
        authorized_sources=source_ids,
    )

    def search(question: str) -> dict[str, Any]:
        return retrieval.search(question, limit=MAX_CANDIDATES)

    def lookup(
        keys: tuple[tuple[str, str], ...],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        if not keys:
            return {}
        with store.connect() as connection:
            rows = connection.execute(
                """WITH requested(source_id,native_parent_id) AS (
                       SELECT * FROM unnest(%s::text[],%s::text[])
                   )
                   SELECT evidence.source_id,evidence.native_parent_id,
                          evidence.logical_document_id,evidence.revision
                     FROM requested
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=%s
                      AND evidence.source_id=requested.source_id
                      AND evidence.native_parent_id=requested.native_parent_id
                    WHERE evidence.source_id=ANY(%s)""",
                (
                    [key[0] for key in keys],
                    [key[1] for key in keys],
                    tenant_id,
                    list(source_ids),
                ),
            ).fetchall()
        return {
            (row["source_id"], row["native_parent_id"]): row
            for row in rows
        }

    def resolve(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return resolve_logical_boundaries(
            results,
            tenant_id=tenant_id,
            authorized_sources=source_ids,
            lookup=lookup,
        )

    try:
        return rank_private_questions(
            input_path,
            output_path,
            search=search,
            resolve=resolve,
            repo_root=repo_root,
            run_id=run_id,
            workers=workers,
        )
    finally:
        store.close()


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-agentic-rankings")
    value.add_argument("--input", required=True)
    value.add_argument("--output", required=True)
    value.add_argument("--repo-root", required=True)
    value.add_argument("--run-id", required=True)
    value.add_argument("--tenant", required=True)
    value.add_argument("--source", action="append", required=True)
    value.add_argument("--dsn-env", default="RECALL_DATABASE_URL")
    value.add_argument("--workers", type=int, default=4)
    return value


def main() -> None:
    args = parser().parse_args()
    dsn = os.environ.get(args.dsn_env, "")
    if not dsn:
        raise SystemExit("private ranking database credential is unavailable")
    try:
        report = _live_rankings(
            dsn=dsn,
            tenant_id=args.tenant,
            source_ids=tuple(args.source),
            input_path=Path(args.input),
            output_path=Path(args.output),
            repo_root=Path(args.repo_root),
            run_id=args.run_id,
            workers=args.workers,
        )
    except EvaluationInputError as error:
        raise SystemExit(f"agentic rankings rejected: {error}") from None
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
