"""Private multi-representation candidate matrix without question leakage."""

from __future__ import annotations

import hashlib
import json
import platform
import time
from pathlib import Path
from typing import Any, Callable

from .agentic_candidate_matrix import (
    _matrix_candidate_count,
    _percentile,
    _private_output,
    _validate_matrix,
    _write_private,
)
from .agentic_rankings import (
    MAX_CANDIDATES,
    _query_bundles,
    _questions,
    _validated_candidates,
)
from .private_holdout import _load_jsonl, _private_path
from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.agentic-representation-matrix.v1"


def _bundle_rankings(
    query_rows: list[dict[str, Any]],
    arm_names: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    def bundle(arm: str) -> list[dict[str, Any]]:
        candidates: dict[tuple[str, str], dict[str, Any]] = {}
        scores: dict[tuple[str, str], float] = {}
        for query_row in query_rows:
            for rank, candidate in enumerate(
                query_row["arms"][arm],
                start=1,
            ):
                key = (
                    candidate["source_id"],
                    candidate["logical_document_id"],
                )
                candidates.setdefault(key, candidate)
                scores[key] = scores.get(key, 0.0) + 1.0 / (60 + rank)
        ranked = sorted(
            candidates,
            key=lambda key: (
                scores[key],
                key[0],
                key[1],
            ),
            reverse=True,
        )
        return [candidates[key] for key in ranked[:MAX_CANDIDATES]]

    return {
        arm: _validated_candidates(
            bundle(arm),
            max_candidates=MAX_CANDIDATES,
        )
        for arm in arm_names
    }


def _query_row(
    query: str,
    *,
    ordinal: int,
    arm_names: tuple[str, ...],
    search: Callable[
        [str],
        dict[str, tuple[list[dict[str, Any]], str]],
    ],
    resolve: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        response = search(query)
        if set(response) != set(arm_names):
            raise ValueError(
                "representation search returned incomplete arms"
            )
        arms = {
            arm: _validated_candidates(
                resolve(response[arm][0]),
                max_candidates=MAX_CANDIDATES,
            )
            for arm in arm_names
        }
        statuses = {
            arm: response[arm][1]
            for arm in arm_names
        }
        if any(
            not isinstance(status, str) or not status
            for status in statuses.values()
        ):
            raise ValueError(
                "representation search returned invalid status"
            )
    except Exception as failure:
        arms = {arm: [] for arm in arm_names}
        statuses = {
            arm: type(failure).__name__[:160]
            for arm in arm_names
        }
    return {
        "ordinal": ordinal,
        "arms": arms,
        "statuses": statuses,
        "latency_ms": round(
            (time.monotonic() - started) * 1000,
            3,
        ),
    }


def write_representation_matrix(
    input_path: Path,
    query_bundle_path: Path,
    output_path: Path,
    *,
    arm_names: tuple[str, ...],
    search: Callable[
        [str],
        dict[str, tuple[list[dict[str, Any]], str]],
    ],
    resolve: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    repo_root: Path,
    run_id: str,
    expected_cases: int,
) -> dict[str, Any]:
    """Write exact case × planned-query × representation coverage."""

    if (
        not arm_names
        or len(set(arm_names)) != len(arm_names)
        or not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
    ):
        raise ValueError("representation matrix execution is invalid")
    cases, input_payload = _questions(
        input_path,
        repo_root=repo_root,
        expected_cases=expected_cases,
    )
    bundles, bundle_payload = _query_bundles(
        query_bundle_path,
        repo_root=repo_root,
        case_ids={case["id"] for case in cases},
    )
    output = _private_output(output_path, repo_root=repo_root)
    rows = []
    for case in cases:
        queries = (case["question"], *bundles[case["id"]])
        query_rows = [
            _query_row(
                query,
                ordinal=ordinal,
                arm_names=arm_names,
                search=search,
                resolve=resolve,
            )
            for ordinal, query in enumerate(queries)
        ]

        rows.append({
            "id": case["id"],
            "queries": query_rows,
            "bundle_rankings": _bundle_rankings(
                query_rows,
                arm_names,
            ),
        })
    _validate_matrix(rows, arms=arm_names)
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    ).encode()
    _write_private(output, payload)
    query_rows = [
        query
        for row in rows
        for query in row["queries"]
    ]
    candidates = [
        candidate
        for row in rows
        for query in row["queries"]
        for values in query["arms"].values()
        for candidate in values
    ] + [
        candidate
        for row in rows
        for values in row["bundle_rankings"].values()
        for candidate in values
    ]
    status_errors = {
        arm: sum(
            query["statuses"][arm] != "ok"
            for query in query_rows
        )
        for arm in arm_names
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "case_count": len(rows),
        "query_count": len(query_rows),
        "candidate_depth": MAX_CANDIDATES,
        "candidate_count": _matrix_candidate_count(rows),
        "arms": list(arm_names),
        "status_error_counts": status_errors,
        "backend_error_count": sum(status_errors.values()),
        "pointer_integrity": (
            sum(candidate["pointer_valid"] for candidate in candidates)
            / len(candidates)
            if candidates
            else 1.0
        ),
        "authorization_violation_rate": (
            sum(not candidate["authorized"] for candidate in candidates)
            / len(candidates)
            if candidates
            else 0.0
        ),
        "query_latency_p50_ms": _percentile(
            [row["latency_ms"] for row in query_rows],
            0.50,
        ),
        "query_latency_p95_ms": _percentile(
            [row["latency_ms"] for row in query_rows],
            0.95,
        ),
        "pins": {
            "input_sha256": hashlib.sha256(input_payload).hexdigest(),
            "query_bundle_sha256": hashlib.sha256(bundle_payload).hexdigest(),
            "matrix_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }


def repair_representation_matrix(
    input_path: Path,
    query_bundle_path: Path,
    matrix_path: Path,
    output_path: Path,
    *,
    arm_names: tuple[str, ...],
    search: Callable[
        [str],
        dict[str, tuple[list[dict[str, Any]], str]],
    ],
    resolve: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    repo_root: Path,
    run_id: str,
    expected_cases: int,
) -> dict[str, Any]:
    """Retry only failed query cells, then rebuild grounded bundle rankings."""

    cases, input_payload = _questions(
        input_path,
        repo_root=repo_root,
        expected_cases=expected_cases,
    )
    bundles, bundle_payload = _query_bundles(
        query_bundle_path,
        repo_root=repo_root,
        case_ids={case["id"] for case in cases},
    )
    source = _private_path(matrix_path, exists=True)
    matrix_rows, matrix_payload = _load_jsonl(source)
    _validate_matrix(matrix_rows, arms=arm_names)
    if (
        set(row["id"] for row in matrix_rows)
        != {case["id"] for case in cases}
        or not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
    ):
        raise ValueError("representation matrix repair is invalid")
    output = _private_output(output_path, repo_root=repo_root)
    by_id = {row["id"]: row for row in matrix_rows}
    repaired_queries = 0
    started = time.monotonic()
    for case in cases:
        row = by_id[case["id"]]
        queries = (case["question"], *bundles[case["id"]])
        if len(queries) != len(row["queries"]):
            raise ValueError("representation matrix repair is invalid")
        for ordinal, query in enumerate(queries):
            current = row["queries"][ordinal]
            if all(
                status == "ok"
                for status in current["statuses"].values()
            ):
                continue
            row["queries"][ordinal] = _query_row(
                query,
                ordinal=ordinal,
                arm_names=arm_names,
                search=search,
                resolve=resolve,
            )
            repaired_queries += 1
        row["bundle_rankings"] = _bundle_rankings(
            row["queries"],
            arm_names,
        )
    rows = [by_id[case["id"]] for case in cases]
    _validate_matrix(rows, arms=arm_names)
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    ).encode()
    _write_private(output, payload)
    query_rows = [query for row in rows for query in row["queries"]]
    status_errors = {
        arm: sum(
            query["statuses"][arm] != "ok"
            for query in query_rows
        )
        for arm in arm_names
    }
    candidates = [
        candidate
        for row in rows
        for query in row["queries"]
        for values in query["arms"].values()
        for candidate in values
    ] + [
        candidate
        for row in rows
        for values in row["bundle_rankings"].values()
        for candidate in values
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "case_count": len(rows),
        "query_count": len(query_rows),
        "repaired_query_count": repaired_queries,
        "status_error_counts": status_errors,
        "backend_error_count": sum(status_errors.values()),
        "pointer_integrity": (
            sum(candidate["pointer_valid"] for candidate in candidates)
            / len(candidates)
            if candidates
            else 1.0
        ),
        "authorization_violation_rate": (
            sum(not candidate["authorized"] for candidate in candidates)
            / len(candidates)
            if candidates
            else 0.0
        ),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "pins": {
            "input_sha256": hashlib.sha256(input_payload).hexdigest(),
            "query_bundle_sha256": hashlib.sha256(
                bundle_payload
            ).hexdigest(),
            "prior_matrix_sha256": hashlib.sha256(
                matrix_payload
            ).hexdigest(),
            "matrix_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }
