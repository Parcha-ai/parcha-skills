"""Bounded agent-authored queries for high-recall document construction.

The model owns semantic decomposition. The host owns scope, authorization,
shape, budgets, and deduplication. This module deliberately knows nothing
about evaluation truth, storage, retrieval arms, or source-specific aliases.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any


MAX_QUESTION_BYTES = 32_768
MAX_QUERY_CHARS = 512
MAX_PLANNER_INSTRUCTION_CHARS = 12_000
MIN_PLANNED_QUERIES = 2
MAX_PLANNED_QUERIES = 5
SOURCE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}\Z")
PLANNER_CONTRACT = "recall.candidate-query-plan.v1"
PLANNER_PROMPT = """You plan high-recall searches over a private work-history archive.
Given one natural question and its host-enforced scope, return materially different,
standalone searches likely to point at the full documents containing the answer.

Use simple agent judgment. Preserve every exact project, repository, branch, path,
service, artifact, person, UUID, and technical identifier from the question. Cover
distinct evidence needs, the requested action/decision/outcome, and useful implementation
vocabulary or aliases. Queries may be natural language; do not reduce everything to
keywords. Do not answer the question, invent facts, invent source or time constraints,
or repeat paraphrases that search for the same thing.

Return JSON only with exactly one key and between two and five queries:
{"queries":["...","..."]}"""


class CandidatePlanningError(ValueError):
    """The planner input or model output violated the closed contract."""


def _timestamp(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise CandidatePlanningError("candidate scope timestamp is invalid")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise CandidatePlanningError(
            "candidate scope timestamp is invalid"
        ) from None
    if parsed.tzinfo is None:
        raise CandidatePlanningError("candidate scope timestamp is invalid")
    return value.strip()


@dataclass(frozen=True)
class CandidateScope:
    """Caller-owned constraints that model output cannot widen."""

    source_ids: tuple[str, ...] = ()
    since: str | None = None
    until: str | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_ids, tuple)
            or len(self.source_ids) > 64
            or len(set(self.source_ids)) != len(self.source_ids)
            or any(
                not isinstance(value, str)
                or SOURCE_RE.fullmatch(value) is None
                for value in self.source_ids
            )
        ):
            raise CandidatePlanningError(
                "candidate scope sources are invalid"
            )
        since = _timestamp(self.since)
        until = _timestamp(self.until)
        if since is not None and until is not None:
            start = datetime.fromisoformat(since.replace("Z", "+00:00"))
            end = datetime.fromisoformat(until.replace("Z", "+00:00"))
            if start > end:
                raise CandidatePlanningError(
                    "candidate scope interval is invalid"
                )

    def model_view(self) -> dict[str, Any]:
        return {
            "source_ids": list(self.source_ids),
            "since": self.since,
            "until": self.until,
            "enforcement": "host-owned; preserve exactly; do not infer",
        }


@dataclass(frozen=True)
class CandidateQueryPlan:
    """Validated semantic searches carrying the unchanged caller scope."""

    queries: tuple[str, ...]
    scope: CandidateScope
    contract: str = PLANNER_CONTRACT


def _model_json(value: str) -> dict[str, Any]:
    if not isinstance(value, str) or len(value.encode()) > 64_000:
        raise CandidatePlanningError("candidate planner response is invalid")
    text = value.strip()
    if text.startswith("```") and text.endswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1]).strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise CandidatePlanningError(
            "candidate planner response is invalid"
        ) from None
    if not isinstance(parsed, dict) or set(parsed) != {"queries"}:
        raise CandidatePlanningError("candidate planner response is invalid")
    return parsed


def plan_candidate_queries(
    question: str,
    *,
    scope: CandidateScope,
    generate: Callable[[tuple[dict[str, str], ...]], str],
    instruction: str = PLANNER_PROMPT,
) -> CandidateQueryPlan:
    """Ask one model for semantics, then enforce a closed host contract."""

    if (
        not isinstance(question, str)
        or not question.strip()
        or len(question.encode()) > MAX_QUESTION_BYTES
        or not isinstance(scope, CandidateScope)
        or not callable(generate)
        or not isinstance(instruction, str)
        or not instruction.strip()
        or len(instruction) > MAX_PLANNER_INSTRUCTION_CHARS
    ):
        raise CandidatePlanningError("candidate planner input is invalid")
    messages = (
        {"role": "system", "content": instruction.strip()},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "question": question.strip(),
                    "scope": scope.model_view(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )
    parsed = _model_json(generate(messages))
    raw_queries = parsed["queries"]
    if (
        not isinstance(raw_queries, list)
        or not MIN_PLANNED_QUERIES
        <= len(raw_queries)
        <= MAX_PLANNED_QUERIES
    ):
        raise CandidatePlanningError("candidate planner queries are invalid")
    queries: list[str] = []
    seen = {question.strip().casefold()}
    for value in raw_queries:
        if not isinstance(value, str):
            raise CandidatePlanningError(
                "candidate planner queries are invalid"
            )
        query = " ".join(value.split())
        normalized = query.casefold()
        if (
            not query
            or len(query) > MAX_QUERY_CHARS
            or normalized in seen
        ):
            continue
        seen.add(normalized)
        queries.append(query)
    if not MIN_PLANNED_QUERIES <= len(queries) <= MAX_PLANNED_QUERIES:
        raise CandidatePlanningError("candidate planner queries are invalid")
    return CandidateQueryPlan(queries=tuple(queries), scope=scope)
