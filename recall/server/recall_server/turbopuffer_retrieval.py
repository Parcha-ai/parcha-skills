"""Passage arms on the turbopuffer search plane (H3-c').

``TurbopufferHintRetrieval`` is ``PassageHintRetrieval`` with its three arms
answered by one turbopuffer namespace per tenant instead of Postgres: the
dense arm is an ANN query embedded natively (``["Embed", query]``, no
provider call from this process), the lexical arm is BM25 over the verbatim
passage text (BM25's OR scoring is the min-should-match the Postgres arm
emulated), the sparse arm is BM25 over the identifier tokens with a
``ContainsAllTokens`` filter per identifier. Every arm returns the row shape
the fusion, nomination, reranker, temporal/source hints and clause passes
already consume, so nothing above the arms changes.
"""
from __future__ import annotations

import time
from typing import Any

from .passage_retrieval import (
    DENSE_NEAREST_LIMIT,
    PassageHintRetrieval,
    identifier_tokens,
)
from .turbopuffer_plane import EMBED_TEXT_ATTRIBUTE, TEXT_ATTRIBUTE, TurbopufferSettings

ROW_ATTRIBUTES = (
    "source_id", "logical_document_id", "policy_fingerprint", "native_parent_id",
    "revision", "ordinal", "first_occurred_at", "last_occurred_at",
    "doc_first_occurred_at", "doc_last_occurred_at", "manifest_object_key",
    "manifest_content_sha256", "text_sha256", "receipts", "spans", "header", TEXT_ATTRIBUTE,
)
LEXICAL_LIMIT = 400
SPARSE_LIMIT = 400
MAX_SPARSE_TOKENS = 8


def _value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        pass
    try:
        return getattr(row, key)
    except AttributeError:
        extra = getattr(row, "__pydantic_extra__", None) or {}
        return extra.get(key, default)


def scope_filters(
    *,
    sources: list[str],
    policy_fingerprint: str,
    since: str | None,
    until: str | None,
    actor_ids: list[str] | None,
    actor_relations: list[str] | None,
) -> list[Any]:
    """The arm scope as turbopuffer filter clauses (AND-ed by the caller)."""

    clauses: list[Any] = [
        ("source_id", "In", list(sources)),
        ("policy_fingerprint", "Eq", policy_fingerprint),
    ]
    if since is not None:
        clauses.append(("last_occurred_at", "Gte", _iso(since)))
    if until is not None:
        clauses.append(("first_occurred_at", "Lte", _iso(until)))
    if actor_ids is not None:
        if actor_relations:
            keys = [f"{relation}:{actor}" for relation in actor_relations for actor in actor_ids]
            clauses.append(("actor_keys", "ContainsAny", keys))
        else:
            clauses.append(("actor_ids", "ContainsAny", list(actor_ids)))
    return clauses


def _iso(value: Any) -> str:
    text = str(value).strip().replace(" ", "T", 1)
    if text.endswith("+00"):
        text += ":00"
    if len(text) == 10:
        text += "T00:00:00+00:00"
    return text


def arm_row(row: Any, score: float) -> dict[str, Any] | None:
    """Map one turbopuffer document to the arm row shape; ``None`` if malformed."""

    passage_id = _value(row, "id")
    text = _value(row, TEXT_ATTRIBUTE)
    if not isinstance(passage_id, str) or not isinstance(text, str):
        return None
    import json

    spans_raw = _value(row, "spans")
    try:
        spans = json.loads(spans_raw) if isinstance(spans_raw, str) else (spans_raw or [])
    except json.JSONDecodeError:
        spans = []
    return {
        "source_id": _value(row, "source_id"),
        "logical_document_id": _value(row, "logical_document_id"),
        "revision": int(_value(row, "revision") or 1),
        "native_parent_id": _value(row, "native_parent_id"),
        "first_occurred_at": _value(row, "doc_first_occurred_at"),
        "last_occurred_at": _value(row, "doc_last_occurred_at"),
        "manifest_object_key": _value(row, "manifest_object_key"),
        "manifest_content_sha256": _value(row, "manifest_content_sha256"),
        "passage_id": passage_id,
        "passage_ordinal": int(_value(row, "ordinal") or 0),
        "spans": spans,
        "receipts": list(_value(row, "receipts") or ()),
        "text_redacted": text,
        "header_redacted": _value(row, "header") or None,
        "text_sha256": _value(row, "text_sha256"),
        "passage_first_occurred_at": _value(row, "first_occurred_at"),
        "passage_last_occurred_at": _value(row, "last_occurred_at"),
        "score": float(score),
    }


def dedupe_texts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first (best) row per distinct passage text, as the dense SQL did."""

    seen: set[str] = set()
    kept = []
    for row in rows:
        key = row.get("text_sha256") or row["passage_id"]
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)
    return kept


class TurbopufferHintRetrieval(PassageHintRetrieval):
    """PassageHintRetrieval whose arms read one turbopuffer namespace."""

    plane = "turbopuffer"

    def __init__(self, store: Any, *, settings: TurbopufferSettings | None = None, client: Any = None, **kwargs: Any) -> None:
        super().__init__(store, **kwargs)
        self.settings = settings or getattr(store, "turbopuffer", None)
        if self.settings is None:
            raise ValueError("turbopuffer settings are required for the turbopuffer plane")
        self.client = client if client is not None else getattr(store, "turbopuffer_client", None)
        if self.client is None:
            raise ValueError("turbopuffer client is required for the turbopuffer plane")
        self.namespace = self.client.namespace(self.settings.namespace(self.tenant_id))

    # The query is embedded by turbopuffer: no provider round trip here, and
    # the clause / window passes reuse the text.
    def _embed_query(self, query: str) -> Any:
        return query

    def _timeout(self, deadline_at: float) -> float | None:
        remaining = deadline_at - time.monotonic()
        if remaining <= 0.05:
            return None
        return min(float(self.settings.query_timeout_seconds), remaining)

    def _query(self, *, rank_by: Any, filters: list[Any], limit: int, deadline_at: float) -> tuple[list[Any], str]:
        timeout = self._timeout(deadline_at)
        if timeout is None:
            return [], "deadline-exceeded"
        try:
            response = self.namespace.query(
                rank_by=rank_by,
                filters=("And", filters) if len(filters) > 1 else filters[0],
                limit=limit,
                include_attributes=list(ROW_ATTRIBUTES),
                timeout=timeout,
            )
        except Exception as error:  # noqa: BLE001 - the arm reports a status, never raises
            name = type(error).__name__
            if "Timeout" in name:
                return [], "deadline-exceeded"
            if "RateLimit" in name:
                return [], "pool-exhausted"
            return [], "unavailable"
        rows = getattr(response, "rows", None)
        if rows is None and isinstance(response, dict):
            rows = response.get("rows")
        return list(rows or ()), "ok"

    def _scope(self, since: str | None, until: str | None, actor_ids: list[str] | None, actor_relations: list[str] | None) -> list[Any]:
        return scope_filters(
            sources=self.sources, policy_fingerprint=self.policy_fingerprint,
            since=since, until=until, actor_ids=actor_ids, actor_relations=actor_relations,
        )

    def _dense_candidates(
        self,
        query: str,
        *,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
        vector: Any = None,
    ) -> tuple[list[dict[str, Any]], str, str, int | None]:
        text = vector if isinstance(vector, str) and vector else query
        raw, status = self._query(
            rank_by=(EMBED_TEXT_ATTRIBUTE, "ANN", ["Embed", text]),
            filters=self._scope(since, until, actor_ids, actor_relations),
            limit=DENSE_NEAREST_LIMIT,
            deadline_at=deadline_at,
        )
        if status != "ok":
            return [], status, "unavailable", None
        rows = []
        for item in raw:
            distance = _value(item, "$dist")
            try:
                score = 1.0 - float(distance)
            except (TypeError, ValueError):
                score = 0.0
            mapped = arm_row(item, score)
            if mapped is not None:
                rows.append(mapped)
        rows.sort(key=lambda row: -row["score"])
        return dedupe_texts(rows), "ok", "turbopuffer-ann", None

    def _lexical_candidates(
        self,
        lexical_query: str,
        *,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> tuple[list[dict[str, Any]], str]:
        terms = [term for term in lexical_query.split() if term]
        self.lexical_plan = {"terms": len(terms), "plane": self.plane}
        if not terms:
            return [], "ok"
        raw, status = self._query(
            rank_by=(TEXT_ATTRIBUTE, "BM25", " ".join(terms)[:8000]),
            filters=self._scope(since, until, actor_ids, actor_relations),
            limit=min(LEXICAL_LIMIT, max(candidate_limit, 1) * 2),
            deadline_at=deadline_at,
        )
        if status != "ok":
            return [], status
        return self._scored(raw), "ok"

    def _sparse_candidates(
        self,
        lexical_query: str,
        *,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
        original_query: str | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        if self.actor_scope:
            return [], "skipped-actor-scope"
        tokens = identifier_tokens(lexical_query, *(() if original_query is None else (original_query,)))[:MAX_SPARSE_TOKENS]
        if not tokens:
            return [], "skipped-prose-query"
        filters = self._scope(since, until, actor_ids, actor_relations)
        exact = [(TEXT_ATTRIBUTE, "ContainsAllTokens", token) for token in tokens]
        filters.append(("Or", exact) if len(exact) > 1 else exact[0])
        raw, status = self._query(
            rank_by=(TEXT_ATTRIBUTE, "BM25", " ".join(tokens)),
            filters=filters,
            limit=min(SPARSE_LIMIT, max(candidate_limit, 1) * 2),
            deadline_at=deadline_at,
        )
        if status != "ok":
            return [], status
        return self._scored(raw), "ok"

    @staticmethod
    def _scored(raw: list[Any]) -> list[dict[str, Any]]:
        rows = []
        for item in raw:
            # The service reports the BM25 score under ``$dist`` (higher is
            # better, unlike the ANN distance); older shapes used ``$score``.
            value = _value(item, "$score")
            if value is None:
                value = _value(item, "$dist")
            try:
                score = float(value or 0.0)
            except (TypeError, ValueError):
                score = 0.0
            mapped = arm_row(item, score)
            if mapped is not None:
                rows.append(mapped)
        rows.sort(key=lambda row: -row["score"])
        return rows
