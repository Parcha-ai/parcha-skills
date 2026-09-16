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

import os
import time
from typing import Any

from .db import bounded_search_text
from .passage_retrieval import (
    DENSE_NEAREST_LIMIT,
    PassageHintRetrieval,
    identifier_tokens,
)
from .turbopuffer_plane import EMBED_TEXT_ATTRIBUTE, TEXT_ATTRIBUTE, TurbopufferSettings

# The arms read the catalog attributes only (a few hundred bytes a row);
# the bodies (text, spans, receipts, header: kilobytes a row) are fetched
# once, by id, for the ranges that survived the collapse. Live: a dense
# pass moved ~6 MB of bodies for 400 rows across regions and a dated
# question ~15 MB over its arms; the collapsed head needs ~150 of them.
CATALOG_ATTRIBUTES = (
    "source_id", "logical_document_id", "policy_fingerprint", "native_parent_id",
    "revision", "ordinal", "first_occurred_at", "last_occurred_at",
    "doc_first_occurred_at", "doc_last_occurred_at", "manifest_object_key",
    "manifest_content_sha256", "text_sha256",
)
BODY_ATTRIBUTES = ("receipts", "spans", "header", TEXT_ATTRIBUTE)
ROW_ATTRIBUTES = CATALOG_ATTRIBUTES + BODY_ATTRIBUTES
HYDRATE_BATCH_IDS = 200
LEXICAL_LIMIT = 400
SPARSE_LIMIT = 400
MAX_SPARSE_TOKENS = 8


def _value(row: Any, key: str, default: Any = None) -> Any:
    """Attribute ``key`` of a result row, or ``default``.

    The SDK's ``Row`` is a pydantic model whose ``__getitem__`` delegates to
    ``getattr`` and raises ``AttributeError`` (not ``KeyError``) for a key
    the service did not return; the first cutover failed on exactly that
    (BM25 rows carry ``$dist`` and no ``$score``).
    """

    if type(row) is dict:
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, TypeError, IndexError, AttributeError):
        pass
    extra = getattr(row, "__pydantic_extra__", None) or {}
    if key in extra:
        return extra[key]
    try:
        return getattr(row, key)
    except AttributeError:
        return default


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
    if not isinstance(passage_id, str):
        return None
    if text is None:
        text = ""  # catalog-only row: the body arrives with hydration
    elif not isinstance(text, str):
        return None
    spans = parse_spans(_value(row, "spans"))
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


def parse_spans(raw: Any) -> list[Any]:
    """Spans travel as a JSON string attribute."""

    import json

    if isinstance(raw, str):
        try:
            return list(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            return []
    return list(raw or [])


def dedupe_texts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the first (best) row per distinct passage text, as the dense SQL did.

    Catalog-only rows carry the text hash, which identifies the text as well.
    """

    seen: set[str] = set()
    kept = []
    for row in rows:
        key = row.get("text_sha256") or row["passage_id"]
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)
    return kept


DEFAULT_WINDOW_BUDGET_MS = 1500


def window_budget_ms_from_env() -> int:
    """``RECALL_TPUF_WINDOW_BUDGET_MS``: the temporal window pass budget on this plane."""

    raw = os.environ.get("RECALL_TPUF_WINDOW_BUDGET_MS", "").strip()
    try:
        value = int(raw) if raw else DEFAULT_WINDOW_BUDGET_MS
    except ValueError:
        return DEFAULT_WINDOW_BUDGET_MS
    return value if 100 <= value <= 20_000 else DEFAULT_WINDOW_BUDGET_MS


def query_client(client: Any) -> Any:
    """The client the arms query with: no SDK retries inside a search deadline.

    The SDK retries a timed-out attempt four times with backoff, so an arm
    given a 150 ms budget spent ~4 s failing (live: every dated question
    paid it, the window pass never returned a row). The arms own their
    deadlines; a retry is the projector's business.
    """

    with_options = getattr(client, "with_options", None)
    if not callable(with_options):
        return client
    try:
        return with_options(max_retries=0)
    except Exception:  # noqa: BLE001 - a client without the option queries as it is
        return client


class TurbopufferHintRetrieval(PassageHintRetrieval):
    """PassageHintRetrieval whose arms read one turbopuffer namespace."""

    plane = "turbopuffer"
    # A filtered ANN pass over the namespace (one day of passages) costs
    # hundreds of milliseconds from the service region, not the 150 ms an
    # indexed Postgres scan gets; and the query text is embedded by the
    # plane, so the pass runs beside the arms instead of after them.
    temporal_window_budget_ms = DEFAULT_WINDOW_BUDGET_MS
    window_pass_concurrent = True

    def __init__(self, store: Any, *, settings: TurbopufferSettings | None = None, client: Any = None, **kwargs: Any) -> None:
        super().__init__(store, **kwargs)
        self.settings = settings or getattr(store, "turbopuffer", None)
        if self.settings is None:
            raise ValueError("turbopuffer settings are required for the turbopuffer plane")
        self.client = client if client is not None else getattr(store, "turbopuffer_client", None)
        if self.client is None:
            raise ValueError("turbopuffer client is required for the turbopuffer plane")
        self.namespace = query_client(self.client).namespace(self.settings.namespace(self.tenant_id))
        self.temporal_window_budget_ms = window_budget_ms_from_env()

    # The query is embedded by turbopuffer: no provider round trip here, and
    # the clause / window passes reuse the text.
    def _embed_query(self, query: str) -> Any:
        return query

    def _timeout(self, deadline_at: float) -> float | None:
        remaining = deadline_at - time.monotonic()
        if remaining <= 0.05:
            return None
        return min(float(self.settings.query_timeout_seconds), remaining)

    def _query(
        self,
        *,
        rank_by: Any,
        filters: list[Any],
        limit: int,
        deadline_at: float,
        include_attributes: tuple[str, ...] = CATALOG_ATTRIBUTES,
    ) -> tuple[list[Any], str]:
        timeout = self._timeout(deadline_at)
        if timeout is None:
            return [], "deadline-exceeded"
        try:
            response = self.namespace.query(
                rank_by=rank_by,
                filters=("And", filters) if len(filters) > 1 else filters[0],
                limit=limit,
                include_attributes=list(include_attributes),
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

    def _hydrate_ranges(
        self,
        results: list[dict[str, Any]],
        legs: tuple[tuple[str, float, list[dict[str, Any]]], ...],
        *,
        deadline_at: float,
    ) -> dict[str, Any]:
        """Fetch the bodies of the collapsed head's ranges in one pass.

        The arms returned catalog rows; the ranges that survived the
        collapse (and the leg rows behind them, which the reranker reads)
        get text, spans, receipts, and header here: one ``id In`` query per
        ``HYDRATE_BATCH_IDS`` ids, ordered by id so the plane reads them in
        one sweep. A shortfall leaves the affected ranges bodiless
        (``hydrate_status`` says so) rather than dropping the result.
        """

        wanted: dict[str, list[dict[str, Any]]] = {}
        for row in results:
            for item in row.get("matching_ranges") or ():
                passage_id = item.get("passage_id")
                if isinstance(passage_id, str) and not item.get("text"):
                    wanted.setdefault(passage_id, []).append(item)
        if not wanted:
            return {"hydrate_status": "ok", "hydrated_passages": 0}
        leg_rows: dict[str, list[dict[str, Any]]] = {}
        for _name, _weight, rows in legs:
            for row in rows:
                passage_id = row.get("passage_id")
                if passage_id in wanted:
                    leg_rows.setdefault(passage_id, []).append(row)
        ids = sorted(wanted)
        hydrated = 0
        status = "ok"
        for start in range(0, len(ids), HYDRATE_BATCH_IDS):
            batch = ids[start:start + HYDRATE_BATCH_IDS]
            raw, batch_status = self._query(
                rank_by=("id", "asc"),
                filters=[("id", "In", batch)],
                limit=len(batch),
                deadline_at=deadline_at,
                include_attributes=BODY_ATTRIBUTES,
            )
            if batch_status != "ok":
                status = batch_status
                continue
            for item in raw:
                passage_id = _value(item, "id")
                if passage_id not in wanted:
                    continue
                text = _value(item, TEXT_ATTRIBUTE)
                if not isinstance(text, str):
                    continue
                spans = parse_spans(_value(item, "spans"))
                receipts = list(_value(item, "receipts") or ())
                header = _value(item, "header") or None
                bounded, clipped = bounded_search_text(text)
                for hint in wanted[passage_id]:
                    hint.update({"text": bounded, "text_clipped": clipped, "spans": spans, "receipts": receipts})
                for row in leg_rows.get(passage_id, ()):
                    row.update({
                        "text_redacted": text, "spans": spans, "receipts": receipts, "header_redacted": header,
                    })
                hydrated += 1
        if hydrated < len(ids) and status == "ok":
            status = "partial"
        return {"hydrate_status": status, "hydrated_passages": hydrated, "hydrate_wanted": len(ids)}

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
