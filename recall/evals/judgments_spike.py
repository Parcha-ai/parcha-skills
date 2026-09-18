"""W0: replay frozen private TypeSafe requests without changing retrieval or gold.

Each JSONL row carries ``id``, ``phase``, ``state``, and ``questions``. Optional
``checks`` compare named Choice answers with independently supplied
``regex`` or ``gold`` expectations. Missing checks remain unmeasured. Keep source
evidence, request fixtures, and reports outside the repository, mode 0600.

This measures individual requests, not the complete search dependency graph. A
successful smoke test or percentile never marks a production wave accepted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError

PHASES = {"smoke", "query", "documents_20", "documents_50"}
INPUT_USD_PER_MILLION = 0.042
MAX_REQUESTS = 1000


def _bytes(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode())
    except (TypeError, ValueError) as error:
        raise EvaluationInputError("request must contain finite JSON values") from error


def _preflight(
    rows: list[dict[str, Any]], repeats: int, max_input_tokens: int
) -> list[int]:
    from recall_server.judgments import JudgmentUnavailable, prepare_judgment_request

    if (
        isinstance(repeats, bool)
        or not isinstance(repeats, int)
        or not 1 <= repeats <= 10
    ):
        raise EvaluationInputError("repeats must be between 1 and 10")
    if not rows or len(rows) * repeats > MAX_REQUESTS:
        raise EvaluationInputError("provide between 1 and 1000 total request attempts")
    if (
        isinstance(max_input_tokens, bool)
        or not isinstance(max_input_tokens, int)
        or max_input_tokens < 1
    ):
        raise EvaluationInputError("input token reservation must be positive")
    reservations = []
    for row in rows:
        if set(row) - {"id", "phase", "state", "questions", "checks"}:
            raise EvaluationInputError("request fixture contains unknown fields")
        if not isinstance(row.get("id"), str) or not 1 <= len(row["id"]) <= 160:
            raise EvaluationInputError("request fixture needs a bounded opaque id")
        if row.get("phase") not in PHASES:
            raise EvaluationInputError("request fixture phase is unsupported")
        state, questions = row.get("state"), row.get("questions")
        try:
            prepare_judgment_request(state, questions)
        except JudgmentUnavailable as error:
            raise EvaluationInputError(
                "request fixture does not satisfy the judgment contract"
            ) from error
        state_bytes = _bytes(state)
        sizes = [_bytes(question) for question in questions.values()]
        # UTF-8 bytes are deliberately conservative for these text fixtures. The
        # model's limits are tokens, not characters. Leave room for serialization.
        if state_bytes + max(sizes) > 30_000 or state_bytes + sum(sizes) > 60_000:
            raise EvaluationInputError(
                "request fixture exceeds the conservative context bound"
            )
        # Reserve the state for EACH independent question; never assume shared
        # prefix accounting or a successful response from a dispatched request.
        reservations.append(len(questions) * state_bytes + sum(sizes) + 1024)
        checks = row.get("checks", {})
        if not isinstance(checks, dict) or set(checks) - {"regex", "gold"}:
            raise EvaluationInputError("request fixture checks are invalid")
        for expected in checks.values():
            if (
                not isinstance(expected, dict)
                or not expected
                or set(expected) - set(questions)
            ):
                raise EvaluationInputError(
                    "request fixture checks must name supplied questions"
                )
            for name, value in expected.items():
                question = questions[name]
                if (
                    question["type"] != "choice"
                    or not isinstance(value, str)
                    or value not in question["criteria"]
                ):
                    raise EvaluationInputError(
                        "agreement checks require a supplied Choice option"
                    )
    if sum(reservations) * repeats > max_input_tokens:
        raise EvaluationInputError(
            "run exceeds the input token reservation; reduce fixtures or raise the explicit cap"
        )
    return reservations


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    return round(sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)], 3)


def _matched(answers: dict[str, Any], expected: dict[str, Any]) -> bool:
    return all(answers[name].choice == value for name, value in expected.items())


def run(
    fixture_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    client: Any,
    repeats: int = 1,
    max_input_tokens: int = 1_000_000,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    # Import lazily so private input preparation needs no server installation.
    from recall_server.judgments import JEV_MODEL, JudgmentUnavailable

    source = _private_path(fixture_path, exists=True)
    output = _private_path(output_path, exists=False)
    root = repo_root.resolve()
    if any(path == root or root in path.parents for path in (source, output)):
        raise EvaluationInputError(
            "private fixtures and reports must live outside the repository"
        )
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 30:
        raise EvaluationInputError(
            "measurement timeout must be positive and at most 30 seconds"
        )
    rows, payload = _load_jsonl(source)
    reservations = _preflight(rows, repeats, max_input_tokens)
    observations = []
    accounted_tokens = 0
    for repeat in range(repeats):
        for row, reserved in zip(rows, reservations, strict=True):
            started = time.monotonic()
            checks = {}
            dispatched = accounted_tokens + reserved <= max_input_tokens
            if not dispatched:
                metadata = {}
                error_code = "spend_cap_reached"
            else:
                try:
                    result = client.judge(
                        state=row["state"],
                        questions=row["questions"],
                        deadline=started + timeout_seconds,
                    )
                    metadata = result.metadata
                    error_code = None
                    checks = {
                        kind: _matched(result.answers, values)
                        for kind, values in row.get("checks", {}).items()
                    }
                except JudgmentUnavailable as error:
                    metadata = error.metadata
                    error_code = error.code
                tokens = metadata.get("input_tokens")
                accounted_tokens += reserved if tokens is None else tokens
            # Neither ids, state, rubric text, answer labels nor upstream bodies
            # enter this receipt. Exact request identity is a digest of the fixture.
            observations.append(
                {
                    "phase": row["phase"],
                    "repeat": repeat,
                    "dispatched": dispatched,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    "error": error_code,
                    "status_code": metadata.get("status_code"),
                    "input_tokens": metadata.get("input_tokens"),
                    "output_tokens": metadata.get("output_tokens"),
                    "contract_hash": metadata.get("contract_hash"),
                    "checks": checks,
                }
            )
    groups = {}
    for phase in sorted({row["phase"] for row in observations}):
        observed = [row for row in observations if row["phase"] == phase]
        successes = [row for row in observed if row["error"] is None]
        latencies = [row["elapsed_ms"] for row in successes]
        groups[phase] = {
            "attempts": sum(row["dispatched"] for row in observed),
            "successes": len(successes),
            "skipped_for_spend_cap": sum(not row["dispatched"] for row in observed),
            "errors": dict(
                Counter(
                    row["error"]
                    for row in observed
                    if row["error"] and row["dispatched"]
                )
            ),
            "http_errors": dict(
                Counter(
                    str(row["status_code"])
                    for row in observed
                    if row["status_code"] is not None
                )
            ),
            "request_success_p50_ms": _percentile(latencies, 0.5),
            "request_success_p95_ms": _percentile(latencies, 0.95),
            "first_pass_success_p95_ms": _percentile(
                [r["elapsed_ms"] for r in successes if r["repeat"] == 0], 0.95
            ),
            "repeat_success_p95_ms": _percentile(
                [r["elapsed_ms"] for r in successes if r["repeat"] > 0], 0.95
            ),
            "checks": {
                kind: {
                    "compared": sum(kind in row["checks"] for row in successes),
                    "matched": sum(row["checks"].get(kind, False) for row in successes),
                }
                for kind in ("regex", "gold")
            },
        }
    unknown = sum(
        row["dispatched"] and row["input_tokens"] is None for row in observations
    )
    observed_tokens = sum(row["input_tokens"] or 0 for row in observations)
    input_cost = observed_tokens * INPUT_USD_PER_MILLION / 1_000_000
    report = {
        "schema": "recall.judgments-spike.v1",
        "model": JEV_MODEL,
        "fixture_sha256": hashlib.sha256(payload).hexdigest(),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "fixture_rows": len(rows),
        "repeats": repeats,
        "groups": groups,
        "transport_status": "failed"
        if any(row["error"] for row in observations)
        else "passed",
        "measurement_scope": "individual_request",
        "stage_budget_evaluated": False,
        "production_ready": False,
        "limitations": [
            "Individual request timing; total search latency and parallel document-batch latency remain unmeasured.",
            "First pass does not prove a cold provider cache; failures are excluded from success percentiles and counted separately.",
            "Missing gold comparisons are unmeasured; regex agreement is not correctness.",
            "Local byte reservations and provider token estimates are not authoritative billing; reconcile provider usage.",
        ],
        "usage": {
            "observed_input_tokens": observed_tokens,
            "observed_output_tokens": sum(
                row["output_tokens"] or 0 for row in observations
            ),
            "unknown_requests": unknown,
            "reserved_input_tokens": sum(reservations) * repeats,
            "accounted_input_tokens": accounted_tokens,
            "max_input_tokens": max_input_tokens,
            "input_usd_per_million": INPUT_USD_PER_MILLION,
            "observed_input_cost_usd": round(input_cost, 9),
            "input_cost_usd": None if unknown else round(input_cost, 9),
        },
        "observations": observations,
    }
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as target:
        os.fchmod(target.fileno(), 0o600)
        json.dump(report, target, indent=2, sort_keys=True, allow_nan=False)
        target.write("\n")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--endpoint", required=True, help="Explicit broker /typesafe/v1/systemone URL"
    )
    parser.add_argument(
        "--approved-endpoint",
        help="Exact remote broker endpoint approved by runtime configuration",
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--max-input-tokens", type=int, default=1_000_000)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)
    from recall_server.judgments import JudgmentClient, JudgmentUnavailable

    try:
        client = JudgmentClient(
            endpoint=args.endpoint,
            broker_key=os.environ.get("LITELLM_API_KEY", ""),
            approved_endpoint=args.approved_endpoint,
            timeout_seconds=args.timeout_seconds,
        )
        report = run(
            args.fixtures,
            args.output,
            repo_root=Path(__file__).resolve().parents[2],
            client=client,
            repeats=args.repeats,
            max_input_tokens=args.max_input_tokens,
            timeout_seconds=args.timeout_seconds,
        )
    except (EvaluationInputError, JudgmentUnavailable, ValueError):
        print("judgments spike input or broker configuration rejected", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "schema",
                    "fixture_sha256",
                    "transport_status",
                    "measurement_scope",
                    "stage_budget_evaluated",
                    "production_ready",
                    "limitations",
                    "groups",
                    "usage",
                )
            }
        )
    )
    return 0 if report["transport_status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
