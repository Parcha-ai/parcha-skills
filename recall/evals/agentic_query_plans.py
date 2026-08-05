"""Write owner-private candidate-query bundles without evaluation truth."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import time
from pathlib import Path
from typing import Callable

from recall_server.candidate_planning import (
    CandidateQueryPlan,
    CandidateScope,
)

from .agentic_rankings import _questions
from .private_holdout import _private_path
from .retrieval import EvaluationInputError
from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.agentic-query-bundle.v1"


def write_query_bundle(
    input_path: Path,
    output_path: Path,
    *,
    plan: Callable[[str, CandidateScope], CandidateQueryPlan],
    scope: CandidateScope,
    repo_root: Path,
    expected_cases: int,
) -> dict:
    """Plan every question and write only the private compatible bundle."""

    cases, input_payload = _questions(
        input_path,
        repo_root=repo_root,
        expected_cases=expected_cases,
    )
    output = _private_path(output_path, exists=False)
    repository = repo_root.resolve(strict=True)
    resolved = output.resolve(strict=False)
    if resolved == repository or repository in resolved.parents:
        raise EvaluationInputError(
            "private query bundle must stay outside Git"
        )
    started = time.monotonic()
    rows = []
    failures = []
    for case in cases:
        try:
            value = plan(case["question"], scope)
            if not isinstance(value, CandidateQueryPlan):
                raise EvaluationInputError(
                    "candidate planner returned an invalid plan"
                )
            if value.scope != scope:
                raise EvaluationInputError(
                    "candidate planner widened explicit scope"
                )
            rows.append(
                {"id": case["id"], "queries": list(value.queries)}
            )
        except Exception as error:
            failures.append(
                {"id": case["id"], "error": type(error).__name__[:80]}
            )
    if failures:
        raise EvaluationInputError(
            f"candidate query planning failed for {len(failures)} cases"
        )
    payload = (
        json.dumps(
            {"private_rows": rows},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    descriptor = os.open(
        output,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
    counts = sorted(len(row["queries"]) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "case_count": len(rows),
        "planner_failure_count": 0,
        "query_count": sum(counts),
        "min_queries_per_case": counts[0],
        "max_queries_per_case": counts[-1],
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "pins": {
            "input_sha256": hashlib.sha256(input_payload).hexdigest(),
            "bundle_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }
