"""Private candidate-card freezing, admission execution, and offline scoring."""

from __future__ import annotations

import hashlib
import json
import platform
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .agentic_candidate_matrix import (
    _percentile,
    _private_output,
    _validate_matrix,
    _write_private,
)
from .agentic_rankings import _questions
from .agentic_truth import SPLIT_COUNTS, _outside_repository, _validate_cases
from .private_holdout import _load_jsonl, _private_path
from .retrieval import EvaluationInputError
from .runner import git_dirty, git_sha
from recall_server.candidate_admission import (
    CARD_FIELDS,
    FINAL_DOCUMENTS,
    AdmissionError,
    AdmissionScope,
    admit_candidate_documents,
    candidate_in_scope,
    validate_candidate_card,
)


CARD_SCHEMA_VERSION = "recall.agentic-admission-cards.v1"
SELECTION_SCHEMA_VERSION = "recall.agentic-admission-selections.v1"
SCORE_SCHEMA_VERSION = "recall.agentic-admission-score.v1"
CARD_ROW_FIELDS = {"id", "question", "scope", "cards"}
SCOPE_FIELDS = {"source_families", "since", "until"}
SELECTION_ROW_FIELDS = {
    "id",
    "status",
    "selected",
    "latency_ms",
    "stages",
    "error",
}
SELECTED_FIELDS = {"source_id", "logical_document_id"}
STAGE_FIELDS = {"stage", "input_count", "selected_count"}
DESCRIPTOR_FIELDS = {
    "source_id",
    "logical_document_id",
    "source_family",
    "first_occurred_at",
    "last_occurred_at",
    "snippets",
    "pointer_valid",
    "authorized",
}


def _private_json(path: Path, value: dict[str, Any]) -> None:
    output = _private_path(path, exists=False)
    payload = (
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    _write_private(output, payload)


def _scope(value: Any) -> AdmissionScope:
    if not isinstance(value, dict) or set(value) != SCOPE_FIELDS:
        raise EvaluationInputError("admission scope schema is invalid")
    families = value["source_families"]
    if not isinstance(families, list):
        raise EvaluationInputError("admission scope schema is invalid")
    try:
        return AdmissionScope(
            source_families=tuple(families),
            since=value["since"],
            until=value["until"],
        )
    except ValueError as error:
        raise EvaluationInputError("admission scope is invalid") from error


def _validate_card_rows(
    rows: list[dict[str, Any]],
    *,
    expected_cases: int,
) -> None:
    if len(rows) != expected_cases:
        raise EvaluationInputError("admission card case count is invalid")
    ids: set[str] = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != CARD_ROW_FIELDS
            or not isinstance(row["id"], str)
            or row["id"] in ids
            or not isinstance(row["question"], str)
            or not row["question"].strip()
            or not isinstance(row["cards"], list)
            or not row["cards"]
            or len(row["cards"]) > 512
        ):
            raise EvaluationInputError("admission card row is invalid")
        ids.add(row["id"])
        _scope(row["scope"])
        identities = []
        for card in row["cards"]:
            try:
                validate_candidate_card(card)
            except ValueError as error:
                raise EvaluationInputError(
                    "admission candidate card is invalid"
                ) from error
            identities.append(
                (card["source_id"], card["logical_document_id"])
            )
        if len(identities) != len(set(identities)):
            raise EvaluationInputError(
                "admission candidate cards are duplicated"
            )


def _validate_selection_rows(
    rows: list[dict[str, Any]],
    *,
    cards_by_id: dict[str, dict[str, Any]],
) -> None:
    if len(rows) != len(cards_by_id):
        raise EvaluationInputError("admission selection case count is invalid")
    ids: set[str] = set()
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != SELECTION_ROW_FIELDS
            or not isinstance(row["id"], str)
            or row["id"] not in cards_by_id
            or row["id"] in ids
            or row["status"] not in {"ok", "error"}
            or isinstance(row["latency_ms"], bool)
            or not isinstance(row["latency_ms"], (int, float))
            or row["latency_ms"] < 0
            or not isinstance(row["selected"], list)
            or len(row["selected"]) > FINAL_DOCUMENTS
            or not isinstance(row["stages"], list)
            or (
                row["status"] == "ok"
                and row["error"] is not None
            )
            or (
                row["status"] == "error"
                and (
                    not isinstance(row["error"], str)
                    or not row["error"]
                    or len(row["error"]) > 160
                    or row["selected"]
                )
            )
        ):
            raise EvaluationInputError("admission selection row is invalid")
        ids.add(row["id"])
        pool = {
            (card["source_id"], card["logical_document_id"])
            for card in cards_by_id[row["id"]]["cards"]
        }
        scope = _scope(cards_by_id[row["id"]]["scope"])
        scoped_pool = {
            (card["source_id"], card["logical_document_id"])
            for card in cards_by_id[row["id"]]["cards"]
            if candidate_in_scope(card, scope)
        }
        selected = []
        for item in row["selected"]:
            if (
                not isinstance(item, dict)
                or set(item) != SELECTED_FIELDS
            ):
                raise EvaluationInputError(
                    "admission selected identity is invalid"
                )
            identity = (item["source_id"], item["logical_document_id"])
            if (
                identity not in pool
                or identity not in scoped_pool
                or identity in selected
            ):
                raise EvaluationInputError(
                    "admission selected identity is invalid"
                )
            selected.append(identity)
        for stage in row["stages"]:
            if (
                not isinstance(stage, dict)
                or set(stage) != STAGE_FIELDS
                or stage["stage"] not in {
                    "control",
                    "map",
                    "reduce",
                    "final",
                }
                or any(
                    isinstance(stage[field], bool)
                    or not isinstance(stage[field], int)
                    or stage[field] < 0
                    for field in ("input_count", "selected_count")
                )
                or stage["selected_count"] > stage["input_count"]
            ):
                raise EvaluationInputError(
                    "admission stage attribution is invalid"
                )


def write_candidate_cards(
    input_path: Path,
    matrix_path: Path,
    output_path: Path,
    *,
    describe: Callable[
        [tuple[tuple[str, str], ...]],
        dict[tuple[str, str], dict[str, Any]],
    ],
    repo_root: Path,
    run_id: str,
    expected_cases: int,
    candidate_depth: int = 50,
    scopes: dict[str, AdmissionScope] | None = None,
) -> dict[str, Any]:
    """Freeze truth-blind candidate cards from one pinned matrix."""

    if (
        not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
        or candidate_depth != 50
    ):
        raise EvaluationInputError("admission card run is invalid")
    cases, input_payload = _questions(
        input_path,
        repo_root=repo_root,
        expected_cases=expected_cases,
    )
    matrix = _private_path(matrix_path, exists=True)
    _outside_repository(matrix, repo_root)
    matrix_rows, matrix_payload = _load_jsonl(matrix)
    if not matrix_rows:
        raise EvaluationInputError("admission matrix is empty")
    arms = tuple(matrix_rows[0]["bundle_rankings"])
    _validate_matrix(matrix_rows, arms=arms)
    matrix_by_id = {row["id"]: row for row in matrix_rows}
    if set(matrix_by_id) != {case["id"] for case in cases}:
        raise EvaluationInputError(
            "admission matrix does not cover the selected cases"
        )
    scope_by_id = scopes or {}
    if set(scope_by_id) - set(matrix_by_id):
        raise EvaluationInputError("admission scope case is unknown")

    memberships: dict[
        str,
        dict[tuple[str, str], dict[str, Any]],
    ] = {}
    identities: set[tuple[str, str]] = set()
    for case in cases:
        membership: dict[tuple[str, str], dict[str, Any]] = {}
        for query in matrix_by_id[case["id"]]["queries"]:
            for arm in arms:
                for rank, candidate in enumerate(
                    query["arms"][arm][:candidate_depth],
                    start=1,
                ):
                    key = (
                        candidate["source_id"],
                        candidate["logical_document_id"],
                    )
                    value = membership.setdefault(
                        key,
                        {
                            "pointer_valid": candidate["pointer_valid"],
                            "authorized": candidate["authorized"],
                            "provenance": [],
                        },
                    )
                    if (
                        value["pointer_valid"] != candidate["pointer_valid"]
                        or value["authorized"] != candidate["authorized"]
                    ):
                        raise EvaluationInputError(
                            "admission candidate authority changed"
                        )
                    value["provenance"].append({
                        "query_ordinal": query["ordinal"],
                        "arm": arm,
                        "rank": rank,
                    })
                    identities.add(key)
        memberships[case["id"]] = membership
    descriptors = describe(tuple(sorted(identities)))
    if set(descriptors) != identities:
        raise EvaluationInputError(
            "admission candidate descriptors are incomplete"
        )
    for key, descriptor in descriptors.items():
        if (
            not isinstance(descriptor, dict)
            or set(descriptor) != DESCRIPTOR_FIELDS
            or (
                descriptor["source_id"],
                descriptor["logical_document_id"],
            )
            != key
        ):
            raise EvaluationInputError(
                "admission candidate descriptor is invalid"
            )

    rows = []
    for case in cases:
        cards = []
        for key, value in memberships[case["id"]].items():
            descriptor = descriptors[key]
            card = {
                **descriptor,
                "pointer_valid": (
                    descriptor["pointer_valid"]
                    and value["pointer_valid"]
                ),
                "authorized": (
                    descriptor["authorized"]
                    and value["authorized"]
                ),
                "provenance": sorted(
                    value["provenance"],
                    key=lambda item: (
                        item["query_ordinal"],
                        item["arm"],
                        item["rank"],
                    ),
                ),
            }
            if set(card) != CARD_FIELDS:
                raise EvaluationInputError(
                    "admission candidate card leaked an unknown field"
                )
            cards.append(validate_candidate_card(card))
        cards.sort(
            key=lambda card: hashlib.sha256(
                (
                    case["id"]
                    + "\0"
                    + card["source_id"]
                    + "\0"
                    + card["logical_document_id"]
                ).encode()
            ).hexdigest()
        )
        scope = scope_by_id.get(case["id"], AdmissionScope())
        rows.append({
            "id": case["id"],
            "question": case["question"],
            "scope": scope.as_json(),
            "cards": cards,
        })
    _validate_card_rows(rows, expected_cases=expected_cases)
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    ).encode()
    output = _private_output(output_path, repo_root=repo_root)
    _write_private(output, payload)
    cards = [card for row in rows for card in row["cards"]]
    counts = [len(row["cards"]) for row in rows]
    return {
        "schema_version": CARD_SCHEMA_VERSION,
        "run_id": run_id,
        "case_count": len(rows),
        "candidate_count": len(cards),
        "candidate_count_min": min(counts),
        "candidate_count_max": max(counts),
        "candidate_count_mean": sum(counts) / len(counts),
        "pointer_integrity": (
            sum(card["pointer_valid"] for card in cards) / len(cards)
        ),
        "authorization_violation_count": sum(
            not card["authorized"] for card in cards
        ),
        "scope_widening_count": 0,
        "truth_derived_field_count": 0,
        "full_document_field_count": 0,
        "backend_error_count": 0,
        "arms": list(arms),
        "candidate_depth": candidate_depth,
        "pins": {
            "input_sha256": hashlib.sha256(input_payload).hexdigest(),
            "matrix_sha256": hashlib.sha256(matrix_payload).hexdigest(),
            "card_bundle_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }


def _control_selection(row: dict[str, Any]) -> dict[str, Any]:
    started = time.monotonic()
    ranked = sorted(
        [
            card
            for card in row["cards"]
            if candidate_in_scope(card, _scope(row["scope"]))
        ],
        key=lambda card: (
            sum(
                1.0 / (60 + item["rank"])
                for item in card["provenance"]
            ),
            card["last_occurred_at"],
            card["logical_document_id"],
        ),
        reverse=True,
    )[:FINAL_DOCUMENTS]
    return {
        "id": row["id"],
        "status": "ok",
        "selected": [
            {
                "source_id": card["source_id"],
                "logical_document_id": card["logical_document_id"],
            }
            for card in ranked
        ],
        "latency_ms": round((time.monotonic() - started) * 1000, 3),
        "stages": [{
            "stage": "control",
            "input_count": len(row["cards"]),
            "selected_count": len(ranked),
        }],
        "error": None,
    }


def write_control_selections(
    card_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    expected_cases: int,
) -> dict[str, Any]:
    """Write a deterministic RRF top-eight control over frozen cards."""

    return _write_selections(
        card_path,
        output_path,
        repo_root=repo_root,
        run_id=run_id,
        expected_cases=expected_cases,
        select=_control_selection,
        workers=1,
        kind="control",
    )


def write_agentic_selections(
    card_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    expected_cases: int,
    generate: Callable[[tuple[dict[str, str], ...]], str],
    instruction: str | None = None,
    workers: int = 2,
    map_workers: int = 4,
) -> dict[str, Any]:
    """Run the bounded Gemma map-reduce selector over every frozen case."""

    def select(row: dict[str, Any]) -> dict[str, Any]:
        try:
            result = admit_candidate_documents(
                row["question"],
                scope=_scope(row["scope"]),
                cards=row["cards"],
                generate=generate,
                instruction=instruction,
                map_workers=map_workers,
            )
            return {
                "id": row["id"],
                "status": "ok",
                "selected": result["selected"],
                "latency_ms": result["latency_ms"],
                "stages": result["stages"],
                "error": None,
            }
        except Exception as error:
            return {
                "id": row["id"],
                "status": "error",
                "selected": [],
                "latency_ms": 0.0,
                "stages": [],
                "error": (
                    str(error)[:160]
                    if isinstance(error, AdmissionError)
                    else type(error).__name__[:160]
                ),
            }

    return _write_selections(
        card_path,
        output_path,
        repo_root=repo_root,
        run_id=run_id,
        expected_cases=expected_cases,
        select=select,
        workers=workers,
        kind="agentic-map-reduce",
    )


def _write_selections(
    card_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    expected_cases: int,
    select: Callable[[dict[str, Any]], dict[str, Any]],
    workers: int,
    kind: str,
) -> dict[str, Any]:
    if (
        not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
        or isinstance(workers, bool)
        or not isinstance(workers, int)
        or not 1 <= workers <= 8
    ):
        raise EvaluationInputError("admission selection run is invalid")
    source = _private_path(card_path, exists=True)
    _outside_repository(source, repo_root)
    rows, card_payload = _load_jsonl(source)
    _validate_card_rows(rows, expected_cases=expected_cases)
    started = time.monotonic()
    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="recall-admission-case",
    ) as executor:
        selections = list(executor.map(select, rows))
    cards_by_id = {row["id"]: row for row in rows}
    _validate_selection_rows(selections, cards_by_id=cards_by_id)
    payload = "".join(
        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
        for row in selections
    ).encode()
    output = _private_output(output_path, repo_root=repo_root)
    _write_private(output, payload)
    latencies = [
        row["latency_ms"]
        for row in selections
        if row["status"] == "ok"
    ]
    return {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "run_id": run_id,
        "kind": kind,
        "case_count": len(selections),
        "error_count": sum(row["status"] == "error" for row in selections),
        "selected_count": sum(
            len(row["selected"]) for row in selections
        ),
        "selected_set_min": min(
            len(row["selected"]) for row in selections
        ),
        "selected_set_max": max(
            len(row["selected"]) for row in selections
        ),
        "latency_p50_ms": _percentile(latencies, 0.50),
        "latency_p95_ms": _percentile(latencies, 0.95),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "pins": {
            "card_bundle_sha256": hashlib.sha256(
                card_payload
            ).hexdigest(),
            "selection_sha256": hashlib.sha256(payload).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }


def score_admission(
    truth_path: Path,
    card_path: Path,
    selection_path: Path,
    output_path: Path,
    *,
    repo_root: Path,
    run_id: str,
    split: str,
) -> dict[str, Any]:
    """Score admission separately from upstream candidate availability."""

    if (
        not isinstance(run_id, str)
        or not run_id
        or len(run_id) > 160
        or split not in SPLIT_COUNTS
    ):
        raise EvaluationInputError("admission score run is invalid")
    truth = _private_path(truth_path, exists=True)
    cards = _private_path(card_path, exists=True)
    selections = _private_path(selection_path, exists=True)
    for path in (truth, cards, selections):
        _outside_repository(path, repo_root)
    truth_rows, truth_payload = _load_jsonl(truth)
    card_rows, card_payload = _load_jsonl(cards)
    selection_rows, selection_payload = _load_jsonl(selections)
    _validate_cases(truth_rows)
    expected_cases = SPLIT_COUNTS[split]
    _validate_card_rows(card_rows, expected_cases=expected_cases)
    cards_by_id = {row["id"]: row for row in card_rows}
    _validate_selection_rows(selection_rows, cards_by_id=cards_by_id)
    selected_truth = [
        row for row in truth_rows if row["split"] == split
    ]
    truth_by_id = {row["id"]: row for row in selected_truth}
    if set(truth_by_id) != set(cards_by_id):
        raise EvaluationInputError(
            "admission artifacts do not cover the selected split"
        )
    selection_by_id = {row["id"]: row for row in selection_rows}
    started = time.monotonic()
    scored = []
    for case_id, case in truth_by_id.items():
        card_row = cards_by_id[case_id]
        selection = selection_by_id[case_id]
        gold = {
            (boundary["source_id"], boundary["logical_document_id"])
            for boundary in case["gold_boundaries"]
        }
        pool = {
            (card["source_id"], card["logical_document_id"])
            for card in card_row["cards"]
        }
        selected = {
            (item["source_id"], item["logical_document_id"])
            for item in selection["selected"]
        }
        scored.append({
            "stratum": case["stratum"],
            "answerable": case["answerability"] == "answerable",
            "gold_count": len(gold),
            "available_gold_count": len(gold & pool),
            "selected_gold_count": len(gold & selected),
            "pool_count": len(pool),
            "selected_count": len(selected),
            "error_count": int(selection["status"] == "error"),
            "latency_ms": selection["latency_ms"],
            "valid_pointer_count": sum(
                card["pointer_valid"] for card in card_row["cards"]
            ),
            "authorization_violation_count": sum(
                not card["authorized"] for card in card_row["cards"]
            ),
            "card_count": len(card_row["cards"]),
        })

    def aggregate(values: list[dict[str, Any]]) -> dict[str, Any]:
        answerable = [row for row in values if row["answerable"]]
        gold = sum(row["gold_count"] for row in answerable)
        available_gold = sum(
            row["available_gold_count"] for row in answerable
        )
        selected_gold = sum(
            row["selected_gold_count"] for row in answerable
        )
        pool = sum(row["pool_count"] for row in answerable)
        selected = sum(row["selected_count"] for row in answerable)
        card_count = sum(row["card_count"] for row in values)
        latencies = [
            row["latency_ms"]
            for row in values
            if not row["error_count"]
        ]
        input_precision = available_gold / pool if pool else 0.0
        selected_precision = (
            selected_gold / selected if selected else 0.0
        )
        sizes = [row["selected_count"] for row in values]
        return {
            "cases": len(values),
            "answerable_cases": len(answerable),
            "gold_documents": gold,
            "available_gold_documents": available_gold,
            "selected_gold_documents": selected_gold,
            "natural_recall@8": selected_gold / gold if gold else 0.0,
            "pool_conditioned_recall@8": (
                selected_gold / available_gold
                if available_gold
                else 0.0
            ),
            "input_pool_precision": input_precision,
            "selected_precision": selected_precision,
            "precision_ratio": (
                selected_precision / input_precision
                if input_precision
                else 0.0
            ),
            "precision_absolute_improvement": (
                selected_precision - input_precision
            ),
            "selected_set_min": min(sizes) if sizes else 0,
            "selected_set_max": max(sizes) if sizes else 0,
            "selected_set_mean": (
                sum(sizes) / len(sizes) if sizes else 0.0
            ),
            "backend_model_error_count": sum(
                row["error_count"] for row in values
            ),
            "pointer_integrity": (
                sum(row["valid_pointer_count"] for row in values)
                / card_count
                if card_count
                else 1.0
            ),
            "authorization_violation_count": sum(
                row["authorization_violation_count"]
                for row in values
            ),
            "scope_widening_count": 0,
            "latency_p50_ms": _percentile(latencies, 0.50),
            "latency_p95_ms": _percentile(latencies, 0.95),
        }

    overall = aggregate(scored)
    strata = {
        stratum: aggregate([
            row for row in scored if row["stratum"] == stratum
        ])
        for stratum in sorted({row["stratum"] for row in scored})
    }
    core = {
        "evaluated_split": split,
        "aggregate": overall,
        "strata": strata,
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
            "card_bundle_sha256": hashlib.sha256(card_payload).hexdigest(),
            "selection_sha256": hashlib.sha256(
                selection_payload
            ).hexdigest(),
            "scorer_sha256": hashlib.sha256(
                Path(__file__).read_bytes()
            ).hexdigest(),
            "git_sha": git_sha(repo_root),
            "git_dirty": git_dirty(repo_root),
            "python": platform.python_version(),
        },
    }
    output = _private_output(output_path, repo_root=repo_root)
    _private_json(output, report)
    return report
