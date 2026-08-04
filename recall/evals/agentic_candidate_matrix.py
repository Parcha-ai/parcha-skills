"""Private candidate-matrix generation and aggregate-only attribution.

Live retrieval never reads truth. It writes only logical-document boundary
identities to an owner-private matrix. Offline scoring is a separate command
that reads the approved truth and emits content-free aggregate attribution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .agentic_rankings import (
    AUTHORITY_RE,
    CASE_ID_RE,
    MAX_CANDIDATES,
    _query_bundles,
    _questions,
    _retrieval_error,
    _select_arm,
    _validated_candidates,
    resolve_passage_boundaries,
)
from .agentic_truth import SPLIT_COUNTS, _outside_repository, _validate_cases
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError
from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.agentic-candidate-matrix.v1"
SCORE_SCHEMA_VERSION = "recall.agentic-candidate-attribution.v1"
BASE_ARMS = ("dense", "passage-lexical", "sparse-exact")
ARMS = (*BASE_ARMS, "fused")
CLASSIFICATIONS = (
    "available_in_fused_50",
    "available_in_union_100_but_dropped",
    "absent_from_union_100",
)


class _EvaluationDeadlineStore:
    """Expose a longer read-only deadline without changing production config."""

    def __init__(self, store: Any, *, search_deadline_ms: int) -> None:
        self._store = store
        self.search_deadline_ms = search_deadline_ms
        self.semantic_runtime = store.semantic_runtime

    def connect(self) -> Any:
        return self._store.connect()

    def _execute_bounded(
        self,
        connection: Any,
        sql: str,
        values: list[Any] | tuple[Any, ...],
        deadline_at: float | None,
    ) -> Any:
        return self._store._execute_bounded(
            connection,
            sql,
            values,
            deadline_at,
        )


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = int(index)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _private_output(path: Path, *, repo_root: Path) -> Path:
    output = _private_path(path, exists=False)
    repository = Path(repo_root).resolve(strict=True)
    resolved = output.resolve(strict=False)
    if resolved == repository or repository in resolved.parents:
        raise EvaluationInputError(
            "private candidate matrix files must stay outside Git"
        )
    return output


def _write_private(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(payload)


def _matrix_candidate_count(rows: list[dict[str, Any]]) -> int:
    return sum(
        len(candidates)
        for row in rows
        for query in row["queries"]
        for candidates in query["arms"].values()
    ) + sum(
        len(candidates)
        for row in rows
        for candidates in row["bundle_rankings"].values()
    )


def write_candidate_matrix(
    input_path: Path,
    query_bundle_path: Path,
    output_path: Path,
    *,
    search: Callable[[str], dict[str, Any]],
    resolve: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    fuse: Callable[
        [tuple[list[dict[str, Any]], ...], int],
        list[dict[str, Any]],
    ],
    repo_root: Path,
    run_id: str,
    expected_cases: int,
    workers: int = 1,
    candidate_depth: int = MAX_CANDIDATES,
) -> dict[str, Any]:
    """Write a complete private case × query × arm candidate matrix."""

    if (
        not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
        or isinstance(workers, bool)
        or not isinstance(workers, int)
        or not 1 <= workers <= 8
        or candidate_depth != MAX_CANDIDATES
    ):
        raise EvaluationInputError("candidate matrix execution bound is invalid")
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

    def run(case: dict[str, str]) -> dict[str, Any]:
        queries = (case["question"], *bundles[case["id"]])
        query_rows = []
        raw_responses = []
        for ordinal, query in enumerate(queries):
            started = time.monotonic()
            try:
                response = search(query)
                arms = {}
                statuses = {}
                for arm in ARMS:
                    selected = _select_arm(response, arm)
                    arms[arm] = _validated_candidates(
                        resolve(selected["results"]),
                        max_candidates=candidate_depth,
                    )
                    statuses[arm] = _retrieval_error(selected) or "ok"
                raw_responses.append(response)
            except Exception as failure:
                arms = {arm: [] for arm in ARMS}
                statuses = {
                    arm: type(failure).__name__[:160]
                    for arm in ARMS
                }
                raw_responses.append(None)
            query_rows.append({
                "ordinal": ordinal,
                "arms": arms,
                "statuses": statuses,
                "latency_ms": round(
                    (time.monotonic() - started) * 1000,
                    3,
                ),
            })
        bundle_rankings = {}
        for arm in ARMS:
            raw_rankings = tuple(
                (
                    response["results"]
                    if arm == "fused"
                    else response["arms"][arm]
                )
                for response in raw_responses
                if response is not None
            )
            bundle_rankings[arm] = _validated_candidates(
                resolve(fuse(raw_rankings, candidate_depth)),
                max_candidates=candidate_depth,
            )
        return {
            "id": case["id"],
            "queries": query_rows,
            "bundle_rankings": bundle_rankings,
        }

    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="recall-candidate-matrix",
    ) as executor:
        rows = list(executor.map(run, cases))
    _validate_matrix(rows)
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
        for arm_candidates in query["arms"].values()
        for candidate in arm_candidates
    ] + [
        candidate
        for row in rows
        for arm_candidates in row["bundle_rankings"].values()
        for candidate in arm_candidates
    ]
    status_counts = {
        arm: sum(
            query["statuses"][arm] != "ok"
            for query in query_rows
        )
        for arm in ARMS
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "case_count": len(rows),
        "query_count": len(query_rows),
        "candidate_depth": candidate_depth,
        "candidate_count": _matrix_candidate_count(rows),
        "status_error_counts": status_counts,
        "backend_error_count": sum(status_counts.values()),
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
            "git_sha": git_sha(Path(repo_root)),
            "git_dirty": git_dirty(Path(repo_root)),
            "python": platform.python_version(),
        },
    }


def _validate_candidate_list(value: Any) -> list[dict[str, Any]]:
    candidates = _validated_candidates(value, max_candidates=MAX_CANDIDATES)
    identities = [
        (candidate["source_id"], candidate["logical_document_id"])
        for candidate in candidates
    ]
    if len(identities) != len(set(identities)):
        raise EvaluationInputError(
            "candidate matrix ranking contains duplicate documents"
        )
    return candidates


def _validate_matrix(
    rows: list[dict[str, Any]],
    *,
    arms: tuple[str, ...] = ARMS,
) -> None:
    if (
        not arms
        or len(set(arms)) != len(arms)
        or any(
            not isinstance(arm, str)
            or not arm
            or len(arm) > 80
            for arm in arms
        )
    ):
        raise EvaluationInputError("candidate matrix arms are invalid")
    ids: set[str] = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != {"id", "queries", "bundle_rankings"}
            or not isinstance(row["id"], str)
            or CASE_ID_RE.fullmatch(row["id"]) is None
            or row["id"] in ids
            or not isinstance(row["queries"], list)
            or not row["queries"]
            or len(row["queries"]) > 8
            or not isinstance(row["bundle_rankings"], dict)
            or set(row["bundle_rankings"]) != set(arms)
        ):
            raise EvaluationInputError("candidate matrix row is invalid")
        ids.add(row["id"])
        for arm in arms:
            _validate_candidate_list(row["bundle_rankings"][arm])
        for ordinal, query in enumerate(row["queries"]):
            if (
                not isinstance(query, dict)
                or set(query) != {
                    "ordinal",
                    "arms",
                    "statuses",
                    "latency_ms",
                }
                or query["ordinal"] != ordinal
                or not isinstance(query["arms"], dict)
                or set(query["arms"]) != set(arms)
                or not isinstance(query["statuses"], dict)
                or set(query["statuses"]) != set(arms)
                or any(
                    not isinstance(status, str) or not status
                    for status in query["statuses"].values()
                )
                or isinstance(query["latency_ms"], bool)
                or not isinstance(query["latency_ms"], (int, float))
                or query["latency_ms"] < 0
            ):
                raise EvaluationInputError(
                    "candidate matrix query row is invalid"
                )
            for arm in arms:
                _validate_candidate_list(query["arms"][arm])
        for arm in arms:
            available = {
                (candidate["source_id"], candidate["logical_document_id"])
                for query in row["queries"]
                for candidate in query["arms"][arm]
            }
            bundled = {
                (candidate["source_id"], candidate["logical_document_id"])
                for candidate in row["bundle_rankings"][arm]
            }
            if not bundled.issubset(available):
                raise EvaluationInputError(
                    "candidate matrix bundle ranking is not grounded"
                )


def _recall(
    ranking: list[dict[str, Any]] | set[tuple[str, str]],
    gold: set[tuple[str, str]],
    *,
    depth: int | None = None,
) -> float:
    if not gold:
        return 0.0
    if isinstance(ranking, set):
        identities = ranking
    else:
        selected = ranking if depth is None else ranking[:depth]
        identities = {
            (candidate["source_id"], candidate["logical_document_id"])
            for candidate in selected
        }
    return len(identities.intersection(gold)) / len(gold)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def score_candidate_matrix(
    truth_path: Path,
    matrix_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    split: str,
    availability_arms: tuple[str, ...] = BASE_ARMS,
    fused_arm: str = "fused",
) -> dict[str, Any]:
    """Attribute retrieval availability separately from bundle fusion."""

    if (
        not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
        or split not in SPLIT_COUNTS
    ):
        raise EvaluationInputError("candidate attribution run is invalid")
    truth = _private_path(truth_path, exists=True)
    matrix = _private_path(matrix_path, exists=True)
    output = _private_output(output_path, repo_root=repo_root)
    _outside_repository(truth, repo_root)
    _outside_repository(matrix, repo_root)
    cases, truth_payload = _load_jsonl(truth)
    matrix_rows, matrix_payload = _load_jsonl(matrix)
    _validate_cases(cases)
    if not matrix_rows:
        raise EvaluationInputError("candidate matrix is empty")
    all_arms = tuple(matrix_rows[0]["bundle_rankings"])
    if (
        not availability_arms
        or not set(availability_arms) <= set(all_arms)
        or fused_arm not in all_arms
    ):
        raise EvaluationInputError(
            "candidate attribution arms are unavailable"
        )
    _validate_matrix(matrix_rows, arms=all_arms)
    selected_cases = [case for case in cases if case["split"] == split]
    case_by_id = {case["id"]: case for case in selected_cases}
    matrix_by_id = {row["id"]: row for row in matrix_rows}
    if set(matrix_by_id) != set(case_by_id):
        raise EvaluationInputError(
            "candidate matrix does not exactly cover the selected split"
        )

    started = time.monotonic()
    rows = []
    for case_id, case in case_by_id.items():
        matrix_row = matrix_by_id[case_id]
        gold = {
            (boundary["source_id"], boundary["logical_document_id"])
            for boundary in case["gold_boundaries"]
        }
        answerable = case["answerability"] == "answerable"
        arm_recall = {
            arm: {
                depth: _recall(
                    matrix_row["bundle_rankings"][arm],
                    gold,
                    depth=depth,
                )
                for depth in (20, 50, 100)
            }
            for arm in all_arms
        }
        available = {
            depth: {
                (
                    candidate["source_id"],
                    candidate["logical_document_id"],
                )
                for arm in availability_arms
                for candidate in matrix_row["bundle_rankings"][arm][
                    :depth
                ]
            }
            for depth in (20, 50, 100)
        }
        fused_50 = {
            (candidate["source_id"], candidate["logical_document_id"])
            for candidate in matrix_row["bundle_rankings"][fused_arm][:50]
        }
        case_classifications = {key: 0 for key in CLASSIFICATIONS}
        for identity in gold:
            key = (
                "available_in_fused_50"
                if identity in fused_50
                else "available_in_union_100_but_dropped"
                if identity in available[100]
                else "absent_from_union_100"
            )
            case_classifications[key] += 1
        errors = sum(
            status != "ok"
            for query in matrix_row["queries"]
            for status in query["statuses"].values()
        )
        candidates = [
            candidate
            for query in matrix_row["queries"]
            for arm_candidates in query["arms"].values()
            for candidate in arm_candidates
        ] + [
            candidate
            for arm_candidates in matrix_row["bundle_rankings"].values()
            for candidate in arm_candidates
        ]
        rows.append({
            "stratum": case["stratum"],
            "answerable": answerable,
            "gold_documents": len(gold),
            "arm_recall": arm_recall,
            "availability_recall": {
                depth: _recall(available[depth], gold)
                for depth in (20, 50, 100)
            },
            "classifications": case_classifications,
            "backend_error_count": errors,
            "candidate_count": len(candidates),
            "valid_pointer_count": sum(
                candidate["pointer_valid"] for candidate in candidates
            ),
            "authorization_violation_count": sum(
                not candidate["authorized"] for candidate in candidates
            ),
        })

    def aggregate(values: list[dict[str, Any]]) -> dict[str, Any]:
        answerable = [row for row in values if row["answerable"]]
        candidate_count = sum(row["candidate_count"] for row in values)
        gold_documents = sum(row["gold_documents"] for row in answerable)
        classified = {
            key: sum(row["classifications"][key] for row in answerable)
            for key in CLASSIFICATIONS
        }
        return {
            "cases": len(values),
            "answerable_cases": len(answerable),
            "gold_documents": gold_documents,
            "arm_recall": {
                arm: {
                    f"recall@{depth}": _mean([
                        row["arm_recall"][arm][depth]
                        for row in answerable
                    ])
                    for depth in (20, 50, 100)
                }
                for arm in all_arms
            },
            **{
                f"availability_union_recall@{depth}": _mean([
                    row["availability_recall"][depth]
                    for row in answerable
                ])
                for depth in (20, 50, 100)
            },
            "classification_counts": classified,
            "classified_gold_documents": sum(classified.values()),
            "backend_error_count": sum(
                row["backend_error_count"] for row in values
            ),
            "pointer_integrity": (
                sum(row["valid_pointer_count"] for row in values)
                / candidate_count
                if candidate_count
                else 1.0
            ),
            "authorization_violation_rate": (
                sum(
                    row["authorization_violation_count"]
                    for row in values
                )
                / candidate_count
                if candidate_count
                else 0.0
            ),
        }

    aggregate_report = aggregate(rows)
    by_stratum = {
        stratum: aggregate([
            row for row in rows if row["stratum"] == stratum
        ])
        for stratum in sorted({row["stratum"] for row in rows})
    }
    union_recall = aggregate_report["availability_union_recall@50"]
    fused_recall = aggregate_report["arm_recall"][fused_arm]["recall@50"]
    verdict = (
        "retrieval-availability"
        if union_recall < 0.95
        else "fusion-ranking"
        if fused_recall < 0.95
        else "candidate-generation-complete"
    )
    core = {
        "evaluated_split": split,
        "availability_arms": list(availability_arms),
        "fused_arm": fused_arm,
        "aggregate": aggregate_report,
        "strata": by_stratum,
        "verdict": verdict,
    }
    analysis_payload = json.dumps(
        core,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    report = {
        "schema_version": SCORE_SCHEMA_VERSION,
        "run_id": run_id,
        **core,
        "analysis_sha256": hashlib.sha256(analysis_payload).hexdigest(),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "pins": {
            "truth_sha256": hashlib.sha256(truth_payload).hexdigest(),
            "matrix_sha256": hashlib.sha256(matrix_payload).hexdigest(),
            "scorer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "git_sha": git_sha(Path(repo_root)),
            "git_dirty": git_dirty(Path(repo_root)),
        },
    }
    _write_private(
        output,
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode(),
    )
    return report


def _live(args: argparse.Namespace) -> dict[str, Any]:
    from recall_server.canonical_retrieval import _informative_query_terms
    from recall_server.db import BrainStore
    from recall_server.passage_projection import PassagePolicy
    from recall_server.passage_retrieval import (
        PassageHintRetrieval,
        fuse_document_rankings,
    )
    from recall_server.semantic import SemanticRuntime

    dsn = os.environ.get(args.dsn_env, "")
    if (
        not dsn
        or AUTHORITY_RE.fullmatch(args.tenant) is None
        or not args.source
        or len(args.source) != len(set(args.source))
        or any(AUTHORITY_RE.fullmatch(value) is None for value in args.source)
        or isinstance(args.search_deadline_ms, bool)
        or not 10 <= args.search_deadline_ms <= 30_000
    ):
        raise EvaluationInputError(
            "candidate matrix runtime authority is invalid"
        )
    store = BrainStore(
        dsn,
        semantic_runtime=SemanticRuntime.from_env(),
        pool_max_size=max(4, args.workers * 2),
        search_deadline_ms=min(args.search_deadline_ms, 5_000),
    )
    evaluation_store = _EvaluationDeadlineStore(
        store,
        search_deadline_ms=args.search_deadline_ms,
    )
    policy = PassagePolicy(
        target_tokens=args.target_tokens,
        overlap_tokens=args.overlap_tokens,
    )
    retrieval = PassageHintRetrieval(
        evaluation_store,
        tenant_id=args.tenant,
        sources=list(args.source),
        policy_fingerprint=policy.fingerprint,
    )

    def search(query: str) -> dict[str, Any]:
        return retrieval.search(
            query,
            lexical_query=" ".join(_informative_query_terms(query)),
            since=None,
            until=None,
            limit=MAX_CANDIDATES,
            include_arms=True,
        )

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
                    args.tenant,
                    list(args.source),
                ),
            ).fetchall()
        return {
            (row["source_id"], row["native_parent_id"]): row
            for row in rows
        }

    def resolve(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return resolve_passage_boundaries(
            results,
            tenant_id=args.tenant,
            authorized_sources=tuple(args.source),
            lookup=lookup,
            max_candidates=MAX_CANDIDATES,
        )

    def fuse(
        rankings: tuple[list[dict[str, Any]], ...],
        limit: int,
    ) -> list[dict[str, Any]]:
        return fuse_document_rankings(rankings, limit=limit)

    try:
        report = write_candidate_matrix(
            Path(args.input),
            Path(args.query_bundle),
            Path(args.output),
            search=search,
            resolve=resolve,
            fuse=fuse,
            repo_root=Path(args.repo_root),
            run_id=args.run_id,
            expected_cases=args.expected_cases,
            workers=args.workers,
        )
        return {
            **report,
            "passage_policy": {
                "target_tokens": args.target_tokens,
                "overlap_tokens": args.overlap_tokens,
                "fingerprint": policy.fingerprint,
            },
            "search_deadline_ms": args.search_deadline_ms,
        }
    finally:
        store.close()


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-agentic-candidate-matrix")
    commands = value.add_subparsers(dest="command", required=True)
    live = commands.add_parser("live")
    live.add_argument("--input", required=True)
    live.add_argument("--query-bundle", required=True)
    live.add_argument("--output", required=True)
    live.add_argument("--repo-root", required=True)
    live.add_argument("--run-id", required=True)
    live.add_argument("--tenant", required=True)
    live.add_argument("--source", action="append", required=True)
    live.add_argument("--dsn-env", default="RECALL_DATABASE_URL")
    live.add_argument("--expected-cases", type=int, required=True)
    live.add_argument("--workers", type=int, default=1)
    live.add_argument("--search-deadline-ms", type=int, default=30_000)
    live.add_argument("--target-tokens", type=int, default=512)
    live.add_argument("--overlap-tokens", type=int, default=64)
    score = commands.add_parser("score")
    score.add_argument("--truth", required=True)
    score.add_argument("--matrix", required=True)
    score.add_argument("--output", required=True)
    score.add_argument("--repo-root", required=True)
    score.add_argument("--run-id", required=True)
    score.add_argument("--split", choices=tuple(SPLIT_COUNTS), required=True)
    score.add_argument("--availability-arm", action="append")
    score.add_argument("--fused-arm", default="fused")
    return value


def main() -> None:
    args = parser().parse_args()
    try:
        if args.command == "live":
            report = _live(args)
        else:
            report = score_candidate_matrix(
                Path(args.truth),
                Path(args.matrix),
                Path(args.output),
                repo_root=Path(args.repo_root),
                run_id=args.run_id,
                split=args.split,
                availability_arms=tuple(
                    args.availability_arm or BASE_ARMS
                ),
                fused_arm=args.fused_arm,
            )
    except EvaluationInputError as error:
        raise SystemExit(
            f"candidate matrix rejected: {error}"
        ) from None
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
