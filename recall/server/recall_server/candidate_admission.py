"""Bounded agentic admission from authorized hint cards to document IDs."""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable


DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+=-]{0,511}\Z")
CARD_FIELDS = {
    "logical_document_id",
    "source_id",
    "source_family",
    "first_occurred_at",
    "last_occurred_at",
    "snippets",
    "provenance",
    "pointer_valid",
    "authorized",
}
PROVENANCE_FIELDS = {"query_ordinal", "arm", "rank"}
MODEL_SELECTION_FIELDS = {"id", "reason", "needs"}
MODEL_RESPONSE_FIELDS = {"selected"}
MAX_CARDS = 512
SHARD_SIZE = 32
MAP_SURVIVORS = 6
REDUCE_FAN_IN = 48
INTERMEDIATE_SURVIVORS = 16
FINAL_DOCUMENTS = 8
MAX_SNIPPETS = 2
MAX_SNIPPET_CHARS = 360

ADMISSION_PROMPT = """\
You select a small working set of documents for a later evidence agent.
Do not answer the question. Candidate hints are fallible pointers, not facts.
False negatives are expensive: preserve complementary documents that could
contain evidence for different parts of the question. Prefer candidates whose
bounded snippets, source family, time range, and independent retrieval hits
fit a material evidence need. Candidate IDs are short host-owned aliases; do
not select by ID shape and do not invent an ID. Return strict JSON only:
{"selected":[{"id":"d001","reason":"brief relevance reason",
"needs":["brief evidence need"]}]}
"""


class AdmissionError(ValueError):
    """Candidate admission violated a structural or authority boundary."""


@dataclass(frozen=True)
class AdmissionScope:
    """Explicit source and time scope that model selection cannot widen."""

    source_families: tuple[str, ...] = ()
    since: str | None = None
    until: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_families, tuple)
            or len(self.source_families) > 16
            or len(set(self.source_families)) != len(self.source_families)
            or any(
                not isinstance(value, str)
                or IDENTITY_RE.fullmatch(value) is None
                for value in self.source_families
            )
        ):
            raise AdmissionError("admission source scope is invalid")
        since = _timestamp(self.since) if self.since is not None else None
        until = _timestamp(self.until) if self.until is not None else None
        if since is not None and until is not None and since > until:
            raise AdmissionError("admission time scope is invalid")

    def as_json(self) -> dict[str, Any]:
        return {
            "source_families": list(self.source_families),
            "since": self.since,
            "until": self.until,
        }


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise AdmissionError("candidate timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise AdmissionError("candidate timestamp is invalid") from error
    if parsed.utcoffset() is None:
        raise AdmissionError("candidate timestamp is invalid")
    return parsed


def validate_candidate_card(value: Any) -> dict[str, Any]:
    """Validate one truth-blind, authorized document hint card."""

    if not isinstance(value, dict) or set(value) != CARD_FIELDS:
        raise AdmissionError("candidate card schema is invalid")
    document_id = value["logical_document_id"]
    source_id = value["source_id"]
    source_family = value["source_family"]
    if (
        not isinstance(document_id, str)
        or DOCUMENT_ID_RE.fullmatch(document_id) is None
        or not isinstance(source_id, str)
        or IDENTITY_RE.fullmatch(source_id) is None
        or not isinstance(source_family, str)
        or IDENTITY_RE.fullmatch(source_family) is None
        or not isinstance(value["pointer_valid"], bool)
        or not isinstance(value["authorized"], bool)
    ):
        raise AdmissionError("candidate card identity is invalid")
    first = _timestamp(value["first_occurred_at"])
    last = _timestamp(value["last_occurred_at"])
    if first > last:
        raise AdmissionError("candidate card time range is invalid")
    snippets = value["snippets"]
    if (
        not isinstance(snippets, list)
        or len(snippets) > MAX_SNIPPETS
        or any(
            not isinstance(snippet, str)
            or not snippet.strip()
            or len(snippet) > MAX_SNIPPET_CHARS
            for snippet in snippets
        )
    ):
        raise AdmissionError("candidate card snippets are invalid")
    provenance = value["provenance"]
    if not isinstance(provenance, list) or not provenance:
        raise AdmissionError("candidate card provenance is invalid")
    seen: set[tuple[int, str, int]] = set()
    for item in provenance:
        if (
            not isinstance(item, dict)
            or set(item) != PROVENANCE_FIELDS
            or isinstance(item["query_ordinal"], bool)
            or not isinstance(item["query_ordinal"], int)
            or not 0 <= item["query_ordinal"] <= 7
            or not isinstance(item["arm"], str)
            or not item["arm"]
            or len(item["arm"]) > 80
            or isinstance(item["rank"], bool)
            or not isinstance(item["rank"], int)
            or not 1 <= item["rank"] <= 50
        ):
            raise AdmissionError("candidate card provenance is invalid")
        identity = (item["query_ordinal"], item["arm"], item["rank"])
        if identity in seen:
            raise AdmissionError("candidate card provenance is duplicated")
        seen.add(identity)
    return value


def _in_scope(card: dict[str, Any], scope: AdmissionScope) -> bool:
    if (
        scope.source_families
        and card["source_family"] not in scope.source_families
    ):
        return False
    first = _timestamp(card["first_occurred_at"])
    last = _timestamp(card["last_occurred_at"])
    if scope.since is not None and last < _timestamp(scope.since):
        return False
    return not (
        scope.until is not None and first > _timestamp(scope.until)
    )


def candidate_in_scope(
    card: dict[str, Any],
    scope: AdmissionScope,
) -> bool:
    """Return whether one valid card is inside the explicit request scope."""

    return _in_scope(validate_candidate_card(card), scope)


def _model_card(card: dict[str, Any], alias: str) -> dict[str, Any]:
    hits = sorted(
        card["provenance"],
        key=lambda item: (
            item["rank"],
            item["query_ordinal"],
            item["arm"],
        ),
    )[:12]
    return {
        "id": alias,
        "source_family": card["source_family"],
        "first_occurred_at": card["first_occurred_at"],
        "last_occurred_at": card["last_occurred_at"],
        "snippets": card["snippets"],
        "retrieval_hits": hits,
    }


def _messages(
    *,
    question: str,
    scope: AdmissionScope,
    cards: list[dict[str, Any]],
    aliases: dict[str, str],
    stage: str,
    limit: int,
    instruction: str | None,
) -> tuple[dict[str, str], ...]:
    system = ADMISSION_PROMPT
    if instruction is not None:
        if (
            not isinstance(instruction, str)
            or not instruction.strip()
            or len(instruction) > 12_000
        ):
            raise AdmissionError("admission instruction is invalid")
        system += "\nAdditional selection guidance:\n" + instruction.strip()
    payload = {
        "stage": stage,
        "selection_limit": limit,
        "question": question,
        "scope": scope.as_json(),
        "candidates": [
            _model_card(
                card,
                aliases[card["logical_document_id"]],
            )
            for card in cards
        ],
    }
    return (
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )


def _parse_model_selection(
    text: str,
    *,
    allowed: set[str],
    limit: int,
) -> tuple[str, ...]:
    if not isinstance(text, str) or not text.strip():
        raise AdmissionError("admission model returned no content")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise AdmissionError("admission model output is not JSON") from error
    if (
        not isinstance(value, dict)
        or set(value) != MODEL_RESPONSE_FIELDS
    ):
        raise AdmissionError("admission model output schema is invalid")
    rows = value["selected"]
    if not isinstance(rows, list):
        raise AdmissionError("admission model selected value is not a list")
    if len(rows) > limit:
        raise AdmissionError("admission model selected too many documents")
    selected: list[str] = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != MODEL_SELECTION_FIELDS
        ):
            raise AdmissionError(
                "admission model selection schema is invalid"
            )
        if not isinstance(row["id"], str) or row["id"] not in allowed:
            raise AdmissionError("admission model selected an unknown document")
        if row["id"] in selected:
            raise AdmissionError("admission model selected a duplicate document")
        if (
            not isinstance(row["reason"], str)
            or not row["reason"].strip()
            or len(row["reason"]) > 240
        ):
            raise AdmissionError("admission model reason is invalid")
        if (
            not isinstance(row["needs"], list)
            or len(row["needs"]) > 4
            or any(
                not isinstance(need, str)
                or not need.strip()
                or len(need) > 120
                for need in row["needs"]
            )
        ):
            raise AdmissionError("admission model evidence needs are invalid")
        selected.append(row["id"])
    return tuple(selected)


def _chunks(
    values: list[dict[str, Any]],
    size: int,
) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _select_stage(
    cards: list[dict[str, Any]],
    *,
    question: str,
    scope: AdmissionScope,
    stage: str,
    limit: int,
    instruction: str | None,
    generate: Callable[[tuple[dict[str, str], ...]], str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    aliases = {
        card["logical_document_id"]: f"d{index:03d}"
        for index, card in enumerate(cards, start=1)
    }
    documents_by_alias = {
        alias: document_id
        for document_id, alias in aliases.items()
    }
    selected = _parse_model_selection(
        generate(
            _messages(
                question=question,
                scope=scope,
                cards=cards,
                aliases=aliases,
                stage=stage,
                limit=limit,
                instruction=instruction,
            )
        ),
        allowed=set(documents_by_alias),
        limit=limit,
    )
    by_id = {
        card["logical_document_id"]: card
        for card in cards
    }
    return (
        [
            by_id[documents_by_alias[alias]]
            for alias in selected
        ],
        {
            "stage": stage,
            "input_count": len(cards),
            "selected_count": len(selected),
        },
    )


def admit_candidate_documents(
    question: str,
    *,
    scope: AdmissionScope,
    cards: list[dict[str, Any]],
    generate: Callable[[tuple[dict[str, str], ...]], str],
    instruction: str | None = None,
    map_workers: int = 4,
) -> dict[str, Any]:
    """Use bounded map-reduce judgment to retain at most eight documents."""

    if (
        not isinstance(question, str)
        or not question.strip()
        or len(question.encode()) > 32_768
        or not isinstance(cards, list)
        or not 0 <= len(cards) <= MAX_CARDS
        or isinstance(map_workers, bool)
        or not isinstance(map_workers, int)
        or not 1 <= map_workers <= 8
    ):
        raise AdmissionError("candidate admission input is invalid")
    validated = [validate_candidate_card(card) for card in cards]
    identities = [
        (card["source_id"], card["logical_document_id"])
        for card in validated
    ]
    if len(identities) != len(set(identities)):
        raise AdmissionError("candidate admission cards are duplicated")
    if any(
        not card["pointer_valid"] or not card["authorized"]
        for card in validated
    ):
        raise AdmissionError(
            "unauthorized or invalid cards cannot reach admission"
        )
    scoped = [card for card in validated if _in_scope(card, scope)]
    started = time.monotonic()
    if not scoped:
        return {
            "selected": [],
            "stages": [],
            "latency_ms": round((time.monotonic() - started) * 1000, 3),
        }
    shards = list(_chunks(scoped, SHARD_SIZE))

    def run_map(
        indexed: tuple[int, list[dict[str, Any]]],
    ) -> tuple[int, list[dict[str, Any]], dict[str, Any]]:
        index, shard = indexed
        selected, trace = _select_stage(
            shard,
            question=question,
            scope=scope,
            stage="map",
            limit=MAP_SURVIVORS,
            instruction=instruction,
            generate=generate,
        )
        return index, selected, trace

    with ThreadPoolExecutor(
        max_workers=min(map_workers, len(shards)),
        thread_name_prefix="recall-admission-map",
    ) as executor:
        mapped = list(executor.map(run_map, enumerate(shards)))
    mapped.sort(key=lambda item: item[0])
    traces = [item[2] for item in mapped]
    survivors = [
        card
        for _, selected, _ in mapped
        for card in selected
    ]
    while len(survivors) > REDUCE_FAN_IN:
        reduced: list[dict[str, Any]] = []
        for shard in _chunks(survivors, REDUCE_FAN_IN):
            selected, trace = _select_stage(
                shard,
                question=question,
                scope=scope,
                stage="reduce",
                limit=INTERMEDIATE_SURVIVORS,
                instruction=instruction,
                generate=generate,
            )
            reduced.extend(selected)
            traces.append(trace)
        survivors = reduced
    if survivors:
        survivors, trace = _select_stage(
            survivors,
            question=question,
            scope=scope,
            stage="final",
            limit=FINAL_DOCUMENTS,
            instruction=instruction,
            generate=generate,
        )
        traces.append(trace)
    return {
        "selected": [
            {
                "source_id": card["source_id"],
                "logical_document_id": card["logical_document_id"],
            }
            for card in survivors
        ],
        "stages": traces,
        "latency_ms": round((time.monotonic() - started) * 1000, 3),
    }
