"""Hybrid hints collapsed to authorized logical-document boundaries."""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from psycopg import sql

from .actor_attribution import ACTOR_ID_RE, ACTOR_RELATIONS
from .db import SearchDeadlineExceeded, bounded_search_text
from .passage_representations import FINGERPRINT_RE, VECTOR_COLUMNS


MAX_BUNDLE_SEARCH_WORKERS = 4
MAX_EXACT_DENSE_SCOPE_PASSAGES = 20_000
# Forgotten passages are rare: rank first, then drop the few whose chunks are
# gone from an oversampled top-K instead of probing canonical_chunks per hit.
LIVENESS_OVERSAMPLE = 2
# The sparse-exact arm scans canonical_chunks (every record, tool output
# included) through one global GIN index. It exists to match identifiers
# exactly; for prose it scores millions of chunks by rank and runs to the
# deadline while dense and passage-lexical already cover the same words.
# Run it only when the query carries identifier-shaped tokens, and never
# let it hold the rest of the search past its own share of the budget.
IDENTIFIER_TOKEN_RE = re.compile(
    r"(?:"
    r"[A-Za-z0-9_-]*\d[A-Za-z0-9_-]*"          # anything with a digit: ids, versions, ports
    r"|[A-Za-z][A-Za-z0-9]*(?:[_./:#-][A-Za-z0-9]+)+"  # snake_case, dotted, paths, k8s names
    r"|[A-Z][a-z]+(?:[A-Z][a-z0-9]+)+"           # CamelCase symbols
    r"|[A-Z]+_[A-Z0-9_]+"                       # ALL_CAPS constants with a separator
    r")"
)
# Acronyms such as MCP, API, or SQL are ordinary vocabulary here, not exact
# identifiers; they stay with the passage-lexical arm.
SPARSE_ARM_BUDGET_FRACTION = 0.5
# Ranking every full-text match by ts_rank_cd is proportional to the size of
# the match set. Common words match most of the corpus. Each text arm first
# tries the full ranking under a short share of the budget, then falls back
# to ranking only the most recent matches, which the index can stop early.
RANKED_PHASE_BUDGET_FRACTION = 0.7
RECENT_WINDOW_DAYS = 30
# Ranking every full-text match reads each passage's TOASTed search_vector:
# ~3 random disk reads per match on the managed instance. The fallback orders
# matches by recency (inline columns only), then ranks just that pool.
RECENT_POOL_MULTIPLIER = 4
# HNSW cost grows with the requested neighbour count; 200 neighbours cost
# ~1.7 s cold on the managed instance versus 3+ s for 400 and far more for
# the temporal ×50 oversample. Documents are ranked after the scan anyway.
DENSE_NEAREST_LIMIT = 400


def _recent_window_since(now: float | None = None) -> str:
    moment = time.time() if now is None else now
    return time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(moment - RECENT_WINDOW_DAYS * 86400)
    )


def _phase_deadline(deadline_at: float, fraction: float) -> float:
    now = time.monotonic()
    return min(deadline_at, now + max(0.0, deadline_at - now) * fraction)


def sparse_arm_applies(lexical_query: str) -> bool:
    """True when the query has at least one token that looks like an identifier."""

    for token in lexical_query.split():
        stripped = token.strip("\"'`()[]{},;")
        if len(stripped) < 3:
            continue
        if IDENTIFIER_TOKEN_RE.fullmatch(stripped):
            return True
    return False
# The exact-vs-ANN decision only needs an approximate scope size; reuse it for
# a short window so the count query does not run before every search.
SCOPE_COUNT_TTL_SECONDS = 60.0
SCOPE_COUNT_CACHE_ENTRIES = 256
_SCOPE_COUNT_CACHE: dict[tuple, tuple[float, int]] = {}
_SCOPE_COUNT_LOCK = threading.Lock()


def _scope_count_cache_get(key: tuple, *, now: float | None = None) -> int | None:
    moment = time.monotonic() if now is None else now
    with _SCOPE_COUNT_LOCK:
        entry = _SCOPE_COUNT_CACHE.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at < moment:
            _SCOPE_COUNT_CACHE.pop(key, None)
            return None
        return value


def _scope_count_cache_put(key: tuple, value: int, *, now: float | None = None) -> None:
    moment = time.monotonic() if now is None else now
    with _SCOPE_COUNT_LOCK:
        if len(_SCOPE_COUNT_CACHE) >= SCOPE_COUNT_CACHE_ENTRIES:
            oldest = min(_SCOPE_COUNT_CACHE, key=lambda k: _SCOPE_COUNT_CACHE[k][0])
            _SCOPE_COUNT_CACHE.pop(oldest, None)
        _SCOPE_COUNT_CACHE[key] = (moment + SCOPE_COUNT_TTL_SECONDS, value)


def reset_scope_count_cache() -> None:
    with _SCOPE_COUNT_LOCK:
        _SCOPE_COUNT_CACHE.clear()


def collapse_document_candidates(
    legs: tuple[tuple[str, float, list[dict[str, Any]]], ...],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Fuse mechanical hints while keeping their strongest exact ranges."""

    documents: dict[str, dict[str, Any]] = {}
    for leg_name, weight, rows in legs:
        seen_documents: set[str] = set()
        for rank, row in enumerate(rows, start=1):
            document_id = row["logical_document_id"]
            value = documents.setdefault(
                document_id,
                {
                    "source_id": row["source_id"],
                    "logical_document_id": document_id,
                    "revision": row["revision"],
                    "native_parent_id": row["native_parent_id"],
                    "first_occurred_at": str(row["first_occurred_at"]),
                    "last_occurred_at": str(row["last_occurred_at"]),
                    "manifest_object_key": row["manifest_object_key"],
                    "manifest_content_sha256": row[
                        "manifest_content_sha256"
                    ],
                    "_score": 0.0,
                    "_reasons": set(),
                    "_ranges": {},
                },
            )
            if document_id not in seen_documents:
                value["_score"] += weight / (60 + rank)
                seen_documents.add(document_id)
            value["_reasons"].add(leg_name)
            range_key = (
                row.get("passage_id")
                or row.get("receipt")
                or f"{leg_name}:{rank}"
            )
            prior = value["_ranges"].get(range_key)
            if prior is None or float(row["score"]) > float(prior["score"]):
                text, clipped = bounded_search_text(row["text_redacted"])
                hint = {
                    "kind": leg_name,
                    "score": round(float(row["score"]), 8),
                    "text": text,
                    "text_clipped": clipped,
                    "receipts": list(row.get("receipts") or (
                        [row["receipt"]] if row.get("receipt") else []
                    )),
                }
                if row.get("passage_id"):
                    hint.update({
                        "passage_id": row["passage_id"],
                        "passage_ordinal": int(row["passage_ordinal"]),
                        "spans": row["spans"],
                    })
                value["_ranges"][range_key] = hint
    ranked = sorted(
        documents.values(),
        key=lambda value: (
            value["_score"],
            value["last_occurred_at"],
            value["logical_document_id"],
        ),
        reverse=True,
    )[:limit]
    results = []
    for value in ranked:
        ordered_ranges = sorted(
            value.pop("_ranges").items(),
            key=lambda pair: (pair[1]["score"], pair[1]["kind"]),
            reverse=True,
        )
        selected_range_keys: set[str] = set()
        ranges = []
        for kind in ("dense", "passage-lexical", "sparse-exact"):
            selected = next(
                (
                    (key, item)
                    for key, item in ordered_ranges
                    if item["kind"] == kind
                ),
                None,
            )
            if selected is not None:
                key, item = selected
                selected_range_keys.add(key)
                ranges.append(item)
        for key, item in ordered_ranges:
            if len(ranges) >= 3:
                break
            if key not in selected_range_keys:
                selected_range_keys.add(key)
                ranges.append(item)
        reasons = sorted(value.pop("_reasons"))
        score = value.pop("_score")
        results.append({
            **value,
            "rank": round(score, 8),
            "reasons": reasons,
            "matching_ranges": ranges,
        })
    return results


def fuse_document_rankings(
    rankings: tuple[list[dict[str, Any]], ...],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Fuse equally weighted query rankings at logical-document boundaries."""

    documents: dict[str, dict[str, Any]] = {}
    for ranking in rankings:
        for rank, row in enumerate(ranking, start=1):
            document_id = row["logical_document_id"]
            value = documents.setdefault(
                document_id,
                {
                    **row,
                    "_query_score": 0.0,
                    "_reasons": set(),
                    "_ranges": {},
                },
            )
            value["_query_score"] += 1.0 / (60 + rank)
            value["_reasons"].update(row.get("reasons") or ())
            for item in row.get("matching_ranges") or ():
                key = (
                    item.get("passage_id")
                    or tuple(item.get("receipts") or ())
                    or (item.get("kind"), item.get("text"))
                )
                prior = value["_ranges"].get(key)
                if prior is None or float(item["score"]) > float(prior["score"]):
                    value["_ranges"][key] = item
    ranked = sorted(
        documents.values(),
        key=lambda value: (
            value["_query_score"],
            value["last_occurred_at"],
            value["logical_document_id"],
        ),
        reverse=True,
    )
    ranked = ranked[:limit]
    results = []
    for value in ranked:
        ranges = sorted(
            value.pop("_ranges").values(),
            key=lambda item: (item["score"], item["kind"]),
            reverse=True,
        )[:3]
        reasons = sorted(value.pop("_reasons"))
        score = value.pop("_query_score")
        value.pop("rank", None)
        results.append({
            **value,
            "rank": round(score, 8),
            "reasons": reasons,
            "matching_ranges": ranges,
        })
    return results


class PassageHintRetrieval:
    """Read-only retrieval over one selected lossless passage policy."""

    def __init__(
        self,
        store: Any,
        *,
        tenant_id: str,
        sources: list[str],
        policy_fingerprint: str,
        actor_ids: tuple[str, ...] | None = None,
        actor_relations: tuple[str, ...] | None = None,
        actor_scope: bool = False,
    ) -> None:
        if actor_ids is not None and (
            not isinstance(actor_ids, tuple)
            or len(actor_ids) > 64
            or tuple(sorted(set(actor_ids))) != actor_ids
            or any(not ACTOR_ID_RE.fullmatch(value) for value in actor_ids)
        ):
            raise ValueError("invalid actor hint scope")
        if actor_relations is not None and (
            not isinstance(actor_relations, tuple)
            or tuple(sorted(set(actor_relations))) != actor_relations
            or not set(actor_relations) <= ACTOR_RELATIONS
            or actor_ids is None
        ):
            raise ValueError("invalid actor relation scope")
        if not isinstance(actor_scope, bool):
            raise ValueError("invalid actor scope")
        self.store = store
        self.tenant_id = tenant_id
        self.sources = sources
        self.policy_fingerprint = policy_fingerprint
        self.actor_ids = actor_ids
        self.actor_relations = actor_relations
        self.actor_scope = actor_scope or actor_ids is not None

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
        """Rank every match under a short budget; otherwise rank the most recent."""

        arguments = {
            "since": since,
            "until": until,
            "candidate_limit": candidate_limit,
            "actor_ids": actor_ids,
            "actor_relations": actor_relations,
        }
        try:
            return self._lexical_query(
                lexical_query, order="rank",
                deadline_at=_phase_deadline(deadline_at, RANKED_PHASE_BUDGET_FRACTION),
                **arguments,
            ), "ok"
        except SearchDeadlineExceeded:
            pass
        try:
            return self._lexical_query(
                lexical_query, order="recent", deadline_at=deadline_at, **arguments,
            ), "ok-recent-first"
        except SearchDeadlineExceeded:
            return [], "deadline-exceeded"

    def _lexical_query(
        self,
        lexical_query: str,
        *,
        order: str,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> list[dict[str, Any]]:
        """One bounded lexical scan.

        `matched` touches only canonical_passages: the GIN match, the scope
        filters, and either the rank (reads every match's TOASTed
        search_vector) or recency (inline columns only). Joins to the current
        projection, evidence, and chunk liveness run afterwards on the bounded
        pool, never on the whole match set.
        """
        # Fusion consumes rank position, not score magnitude. Recency order is
        # therefore a complete ranking on its own and never reads the TOASTed
        # search_vector; the score column is reported for the rank mode only.
        if order == "rank":
            pool_order = (
                "ts_rank_cd(passage.search_vector,plainto_tsquery('simple',%s),32) DESC,"
                "passage.last_occurred_at DESC,passage.passage_id"
            )
            pool_limit = candidate_limit * LIVENESS_OVERSAMPLE
            order_values: tuple[str, ...] = (lexical_query,)
            score_sql = "ts_rank_cd(top.search_vector,plainto_tsquery('simple',%s),32)"
            score_values: tuple[str, ...] = (lexical_query,)
        elif order == "recent":
            pool_order = "passage.last_occurred_at DESC,passage.passage_id"
            pool_limit = candidate_limit * LIVENESS_OVERSAMPLE
            order_values = ()
            score_sql = "0.0::real"
            score_values = ()
        else:
            raise ValueError("unsupported lexical order")
        with self.store.connect() as connection:
            return self.store._execute_bounded(
                    connection,
                    f"""WITH matched AS MATERIALIZED (
                       SELECT passage.tenant_id,
                              passage.source_id,
                              passage.logical_document_id,
                              passage.revision,
                              passage.policy_fingerprint,
                              passage.passage_id,
                              passage.ordinal AS passage_ordinal,
                              passage.spans,passage.receipts,
                              passage.text_redacted,
                              passage.last_occurred_at,
                              passage.search_vector
                         FROM canonical_passages passage
                        WHERE passage.tenant_id=%s
                          AND passage.source_id=ANY(%s)
                          AND passage.policy_fingerprint=%s
                          AND (
                              %s::text[] IS NULL
                              OR EXISTS (
                                  SELECT 1
                                    FROM canonical_passage_actors actor
                                   WHERE actor.tenant_id=passage.tenant_id
                                     AND actor.source_id=passage.source_id
                                     AND actor.passage_id=passage.passage_id
                                     AND actor.actor_id=ANY(%s)
                                     AND (
                                         %s::text[] IS NULL
                                         OR actor.relation=ANY(%s)
                                     )
                              )
                          )
                          AND passage.search_vector @@
                              plainto_tsquery('simple',%s)
                          AND (%s::timestamptz IS NULL
                               OR passage.last_occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR passage.first_occurred_at<=%s)
                        ORDER BY {pool_order}
                        LIMIT %s
                       )
                       , top AS MATERIALIZED (
                       SELECT * FROM matched
                        ORDER BY {pool_order.replace("passage.", "matched.")}
                        LIMIT %s
                       )
                       SELECT top.source_id,top.logical_document_id,
                              top.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              top.passage_id,top.passage_ordinal,
                              top.spans,top.receipts,
                              top.text_redacted,
                              {score_sql} AS score
                         FROM top
                         JOIN canonical_passage_documents projected
                           USING(
                               tenant_id,source_id,logical_document_id,
                               revision,policy_fingerprint
                           )
                         JOIN canonical_evidence_documents evidence
                           USING(
                               tenant_id,source_id,logical_document_id,
                               revision
                           )
                        WHERE NOT EXISTS (
                              SELECT 1
                                FROM unnest(top.receipts)
                                     AS passage_receipt(receipt)
                                LEFT JOIN canonical_chunks live_chunk
                                  ON live_chunk.tenant_id=top.tenant_id
                                 AND live_chunk.source_id=top.source_id
                                 AND live_chunk.receipt=
                                     passage_receipt.receipt
                                 AND live_chunk.deleted_at IS NULL
                               WHERE live_chunk.receipt IS NULL
                          )
                        ORDER BY {pool_order.replace("passage.", "top.")}""",
                    (
                        self.tenant_id,
                        self.sources,
                        self.policy_fingerprint,
                        actor_ids,
                        actor_ids,
                        actor_relations,
                        actor_relations,
                        lexical_query,
                        since,
                        since,
                        until,
                        until,
                        *order_values,
                        pool_limit,
                        *order_values,
                        candidate_limit,
                        *score_values,
                        *order_values,
                    ),
                    deadline_at,
                ).fetchall()

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
        # The informative-term query is casefolded; check the original text
        # too so CamelCase symbols and ALL_CAPS codes still qualify.
        if not (
            sparse_arm_applies(lexical_query)
            or (original_query is not None and sparse_arm_applies(original_query))
        ):
            return [], "skipped-prose-query"
        arm_deadline = _phase_deadline(deadline_at, SPARSE_ARM_BUDGET_FRACTION)
        arguments = {
            "since": since,
            "until": until,
            "candidate_limit": candidate_limit,
            "actor_ids": actor_ids,
            "actor_relations": actor_relations,
        }
        try:
            return self._sparse_query(
                lexical_query, order="rank",
                deadline_at=_phase_deadline(arm_deadline, RANKED_PHASE_BUDGET_FRACTION),
                **arguments,
            ), "ok"
        except SearchDeadlineExceeded:
            pass
        try:
            return self._sparse_query(
                lexical_query, order="recent", deadline_at=arm_deadline, **arguments,
            ), "ok-recent-first"
        except SearchDeadlineExceeded:
            return [], "deadline-exceeded"

    def _sparse_query(
        self,
        lexical_query: str,
        *,
        order: str,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> list[dict[str, Any]]:
        """One bounded exact-identifier scan over canonical_chunks.

        `matched` touches only the chunk table through its GIN index; the
        document, event, evidence, actor, and time joins run on the bounded
        pool. Recency uses chunk.created_at (inline) so no TOASTed
        search_vector is read before the LIMIT.
        """
        if order == "rank":
            pool_order = (
                "ts_rank_cd(chunk.search_vector,plainto_tsquery('simple',%s),32) DESC,"
                "chunk.created_at DESC,chunk.chunk_id"
            )
            order_values: tuple[str, ...] = (lexical_query,)
            score_sql = "ts_rank_cd(top.search_vector,plainto_tsquery('simple',%s),32)"
            score_values: tuple[str, ...] = (lexical_query,)
        elif order == "recent":
            pool_order = "chunk.created_at DESC,chunk.chunk_id"
            order_values = ()
            score_sql = "0.0::real"
            score_values = ()
        else:
            raise ValueError("unsupported sparse order")
        pool_limit = candidate_limit * LIVENESS_OVERSAMPLE
        with self.store.connect() as connection:
            return self.store._execute_bounded(
                    connection,
                    f"""WITH matched AS MATERIALIZED (
                       SELECT chunk.tenant_id,chunk.source_id,
                              chunk.document_id,chunk.chunk_id,
                              chunk.ordinal,chunk.receipt,
                              chunk.text_redacted,chunk.created_at,
                              chunk.search_vector
                         FROM canonical_chunks chunk
                        WHERE chunk.tenant_id=%s
                          AND chunk.source_id=ANY(%s)
                          AND chunk.deleted_at IS NULL
                          AND chunk.search_vector @@
                              plainto_tsquery('simple',%s)
                        ORDER BY {pool_order}
                        LIMIT %s
                       ), top AS MATERIALIZED (
                       SELECT * FROM matched
                        ORDER BY {pool_order.replace("chunk.", "matched.")}
                        LIMIT %s
                       )
                       SELECT event.source_id,
                              evidence.logical_document_id,
                              evidence.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,
                              evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              top.receipt,top.text_redacted,
                              {score_sql} AS score
                         FROM top
                         JOIN canonical_documents document
                           USING(tenant_id,source_id,document_id)
                         JOIN canonical_events event
                           USING(tenant_id,source_id,event_id)
                         JOIN canonical_evidence_documents evidence
                           ON evidence.tenant_id=event.tenant_id
                          AND evidence.source_id=event.source_id
                          AND evidence.native_parent_id=COALESCE(
                              event.native_parent_id,event.native_id
                          )
                        WHERE document.is_current
                          AND document.deleted_at IS NULL
                          AND (
                              %s::text[] IS NULL
                              OR EXISTS (
                                  SELECT 1
                                    FROM canonical_evidence_document_actors actor
                                   WHERE actor.tenant_id=evidence.tenant_id
                                     AND actor.source_id=evidence.source_id
                                     AND actor.logical_document_id=
                                         evidence.logical_document_id
                                     AND actor.revision=evidence.revision
                                     AND actor.actor_id=ANY(%s)
                                     AND (
                                         %s::text[] IS NULL
                                         OR actor.relation=ANY(%s)
                                     )
                              )
                          )
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at<=%s)
                        ORDER BY score DESC,event.occurred_at DESC,
                                 top.chunk_id""",
                    (
                        self.tenant_id,
                        self.sources,
                        lexical_query,
                        *order_values,
                        pool_limit,
                        *order_values,
                        candidate_limit,
                        *score_values,
                        actor_ids,
                        actor_ids,
                        actor_relations,
                        actor_relations,
                        since,
                        since,
                        until,
                        until,
                    ),
                    deadline_at,
                ).fetchall()

    def _dense_scope_passage_count(
        self,
        *,
        since: str | None,
        until: str | None,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> int | None:
        key = (
            self.tenant_id,
            tuple(self.sources),
            self.policy_fingerprint,
            since,
            until,
            tuple(actor_ids) if actor_ids else None,
            tuple(actor_relations) if actor_relations else None,
        )
        cached = _scope_count_cache_get(key)
        if cached is not None:
            return cached
        count = self._dense_scope_passage_count_uncached(
            since=since,
            until=until,
            actor_ids=actor_ids,
            actor_relations=actor_relations,
            deadline_at=deadline_at,
        )
        if count is not None:
            _scope_count_cache_put(key, count)
        return count

    def _dense_scope_passage_count_uncached(
        self,
        *,
        since: str | None,
        until: str | None,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> int | None:
        try:
            with self.store.connect() as connection:
                row = self.store._execute_bounded(
                    connection,
                    """SELECT COALESCE(sum(projected.passage_count),0) AS count
                         FROM canonical_passage_documents projected
                         JOIN canonical_evidence_documents evidence
                           USING(
                               tenant_id,source_id,logical_document_id,revision
                           )
                        WHERE projected.tenant_id=%s
                          AND projected.source_id=ANY(%s)
                          AND projected.policy_fingerprint=%s
                          AND (%s::timestamptz IS NULL
                               OR evidence.last_occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR evidence.first_occurred_at<=%s)
                          AND (
                              %s::text[] IS NULL
                              OR EXISTS (
                                  SELECT 1
                                    FROM canonical_evidence_document_actors actor
                                   WHERE actor.tenant_id=projected.tenant_id
                                     AND actor.source_id=projected.source_id
                                     AND actor.logical_document_id=
                                         projected.logical_document_id
                                     AND actor.revision=projected.revision
                                     AND actor.actor_id=ANY(%s)
                                     AND (
                                         %s::text[] IS NULL
                                         OR actor.relation=ANY(%s)
                                     )
                              )
                          )""",
                    (
                        self.tenant_id,
                        self.sources,
                        self.policy_fingerprint,
                        since,
                        since,
                        until,
                        until,
                        actor_ids,
                        actor_ids,
                        actor_relations,
                        actor_relations,
                    ),
                    deadline_at,
                ).fetchone()
        except SearchDeadlineExceeded:
            return None
        return int(row["count"]) if row is not None else None

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
    ) -> tuple[list[dict[str, Any]], str, str, int | None]:
        runtime = self.store.semantic_runtime
        if runtime is None:
            return [], "disabled", "disabled", None
        try:
            bounded = getattr(runtime, "embed_query_bounded", None)
            vector = (
                bounded(query)
                if bounded is not None
                else runtime.embed_query(query)
            )
            temporal_scope = since is not None or until is not None
            dense_oversample = 50 if temporal_scope else 5
            scope_passages = self._dense_scope_passage_count(
                since=since,
                until=until,
                actor_ids=actor_ids,
                actor_relations=actor_relations,
                deadline_at=deadline_at,
            )
            exact_scope = (
                scope_passages is not None
                and scope_passages <= MAX_EXACT_DENSE_SCOPE_PASSAGES
            )
            if exact_scope:
                dense_strategy = "exact-scoped"
                nearest_sql = """WITH eligible AS MATERIALIZED (
                           SELECT embedding.tenant_id,
                                  embedding.source_id,
                                  embedding.passage_id,
                                  embedding.embedding
                             FROM canonical_passage_embeddings embedding
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,
                                   logical_document_id,
                                   revision,policy_fingerprint
                               )
                            WHERE embedding.tenant_id=%s
                              AND embedding.source_id=ANY(%s)
                              AND embedding.runtime_fingerprint=%s
                              AND projected.policy_fingerprint=%s
                              AND (%s::timestamptz IS NULL
                                   OR passage.last_occurred_at>=%s)
                              AND (%s::timestamptz IS NULL
                                   OR passage.first_occurred_at<=%s)
                              AND (
                                  %s::text[] IS NULL
                                  OR EXISTS (
                                      SELECT 1
                                        FROM canonical_passage_actors actor
                                       WHERE actor.tenant_id=embedding.tenant_id
                                         AND actor.source_id=embedding.source_id
                                         AND actor.passage_id=embedding.passage_id
                                         AND actor.actor_id=ANY(%s)
                                         AND (
                                             %s::text[] IS NULL
                                             OR actor.relation=ANY(%s)
                                         )
                                  )
                              )
                       ), nearest AS MATERIALIZED (
                           SELECT eligible.tenant_id,
                                  eligible.source_id,
                                  eligible.passage_id,
                                  eligible.embedding <=> %s::halfvec AS distance
                             FROM eligible
                            ORDER BY eligible.embedding <=> %s::halfvec
                            LIMIT %s
                       )"""
                nearest_values = (
                    self.tenant_id,
                    self.sources,
                    runtime.passage_fingerprint,
                    self.policy_fingerprint,
                    since,
                    since,
                    until,
                    until,
                    actor_ids,
                    actor_ids,
                    actor_relations,
                    actor_relations,
                    vector,
                    vector,
                    candidate_limit * dense_oversample,
                )
            else:
                dense_strategy = "ann-oversampled"
                nearest_limit = min(DENSE_NEAREST_LIMIT, candidate_limit * dense_oversample)
                # Liveness (forgotten chunks) is checked once on the
                # oversampled top-K in ranked_documents, never inside the
                # index scan: probing canonical_chunks per ANN candidate is
                # what turned every search into ~1,000 disk reads.
                nearest_sql = """WITH nearest AS MATERIALIZED (
                           SELECT embedding.tenant_id,
                                  embedding.source_id,
                                  embedding.passage_id,
                                  embedding.embedding <=> %s::halfvec AS distance
                             FROM canonical_passage_embeddings embedding
                            WHERE embedding.tenant_id=%s
                              AND embedding.source_id=ANY(%s)
                              AND embedding.runtime_fingerprint=%s
                            ORDER BY embedding.embedding <=> %s::halfvec
                            LIMIT %s
                       )"""
                nearest_values = (
                    vector,
                    self.tenant_id,
                    self.sources,
                    runtime.passage_fingerprint,
                    vector,
                    nearest_limit,
                )
            dense_sql = nearest_sql + """, ranked_documents AS MATERIALIZED (
                           SELECT DISTINCT ON (passage.logical_document_id)
                                  passage.source_id,
                                  passage.logical_document_id,
                                  passage.revision,
                                  evidence.native_parent_id,
                                  evidence.first_occurred_at,
                                  evidence.last_occurred_at,
                                  evidence.manifest_object_key,
                                  evidence.manifest_content_sha256,
                                  passage.passage_id,
                                  passage.ordinal AS passage_ordinal,
                                  passage.spans,passage.receipts,
                                  passage.text_redacted,
                                  nearest.distance
                             FROM nearest
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   revision,policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(
                                   tenant_id,source_id,
                                   logical_document_id,revision
                               )
                            WHERE projected.policy_fingerprint=%s
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM unnest(passage.receipts)
                                         AS passage_receipt(receipt)
                                    LEFT JOIN canonical_chunks live_chunk
                                      ON live_chunk.tenant_id=passage.tenant_id
                                     AND live_chunk.source_id=passage.source_id
                                     AND live_chunk.receipt=passage_receipt.receipt
                                     AND live_chunk.deleted_at IS NULL
                                   WHERE live_chunk.receipt IS NULL
                              )
                              AND (%s::timestamptz IS NULL
                                   OR passage.last_occurred_at>=%s)
                              AND (%s::timestamptz IS NULL
                                   OR passage.first_occurred_at<=%s)
                            ORDER BY passage.logical_document_id,
                                     nearest.distance,
                                     passage.last_occurred_at DESC,
                                     passage.passage_id
                       )
                       SELECT *,1-distance AS score
                         FROM ranked_documents
                        ORDER BY distance,last_occurred_at DESC,passage_id
                        LIMIT %s"""
            with self.store.connect() as connection:
                # Leave hnsw.iterative_scan / ef_search at the server defaults.
                # strict_order with a real (short) query vector walked the
                # graph for 16 s p50 in production, against 1.7 s measured
                # with a passage vector as the query.
                rows = self.store._execute_bounded(
                    connection,
                    dense_sql,
                    nearest_values + (
                        self.policy_fingerprint,
                        since,
                        since,
                        until,
                        until,
                        candidate_limit,
                    ),
                    deadline_at,
                ).fetchall()
        except (
            json.JSONDecodeError,
            SearchDeadlineExceeded,
            TimeoutError,
            urllib.error.URLError,
        ) as error:
            return (
                [],
                "deadline-exceeded"
                if isinstance(error, SearchDeadlineExceeded)
                else "unavailable",
                "unavailable",
                None,
            )
        return rows, "ok", dense_strategy, scope_passages

    def search(
        self,
        query: str,
        *,
        lexical_query: str,
        since: str | None,
        until: str | None,
        limit: int,
        include_arms: bool = False,
        deadline_at: float | None = None,
    ) -> dict[str, Any]:
        """Run independent hint arms concurrently and retain partial success."""

        started_at = time.monotonic()
        if deadline_at is None:
            deadline_at = started_at + self.store.search_deadline_ms / 1000
        candidate_limit = min(400, max(80, limit * 20))
        actor_ids = list(self.actor_ids) if self.actor_ids is not None else None
        actor_relations = (
            list(self.actor_relations)
            if self.actor_relations is not None
            else None
        )
        common = {
            "since": since,
            "until": until,
            "candidate_limit": candidate_limit,
            "actor_ids": actor_ids,
            "actor_relations": actor_relations,
            "deadline_at": deadline_at,
        }
        arm_elapsed_ms: dict[str, float] = {}

        def timed_arm(name: str, method: Any, *args: Any) -> Any:
            arm_started = time.monotonic()
            try:
                return method(*args, **common)
            finally:
                arm_elapsed_ms[name] = round(
                    (time.monotonic() - arm_started) * 1000, 3
                )

        with ThreadPoolExecutor(max_workers=3) as executor:
            lexical_future = executor.submit(
                timed_arm, "passage_lexical", self._lexical_candidates, lexical_query,
            )
            sparse_future = executor.submit(
                timed_arm, "sparse_exact",
                lambda text, **kwargs: self._sparse_candidates(
                    text, original_query=query, **kwargs
                ),
                lexical_query,
            )
            dense_future = executor.submit(
                timed_arm, "dense", self._dense_candidates, query,
            )
            lexical, lexical_status = lexical_future.result()
            sparse, sparse_status = sparse_future.result()
            (
                dense,
                dense_status,
                dense_strategy,
                dense_scope_passages,
            ) = dense_future.result()
        # A document containing every informative query term is stronger
        # evidence than a semantic neighbor. Dense retrieval remains the
        # fallback for paraphrases, but it must not bury an exact hit merely
        # because each arm returned candidates.
        legs = (
            ("dense", 0.15, dense),
            ("passage-lexical", 0.30, lexical),
            ("sparse-exact", 0.55, sparse),
        )
        results = collapse_document_candidates(legs, limit=limit)
        response = {
            "results": results,
            "diagnostics": {
                "engine": "lossless-passages-v1",
                "policy_fingerprint": self.policy_fingerprint,
                "dense_candidates": len(dense),
                "passage_lexical_candidates": len(lexical),
                "sparse_candidates": len(sparse),
                "dense_status": dense_status,
                "dense_strategy": dense_strategy,
                "dense_scope_passages": dense_scope_passages,
                "passage_lexical_status": lexical_status,
                "sparse_status": sparse_status,
                "arm_elapsed_ms": arm_elapsed_ms,
                "elapsed_ms": round(
                    (time.monotonic() - started_at) * 1000,
                    3,
                ),
                "deadline_ms": self.store.search_deadline_ms,
                "deadline_exceeded": "deadline-exceeded" in {
                    dense_status,
                    lexical_status,
                    sparse_status,
                },
                "partial_results_preserved": bool(results),
            },
        }
        if include_arms:
            response["arms"] = {
                name: collapse_document_candidates(
                    tuple(
                        (leg_name, 1.0, leg_rows)
                        for leg_name, _weight, leg_rows in legs
                        if leg_name == name
                    ),
                    limit=limit,
                )
                for name in ("dense", "passage-lexical", "sparse-exact")
            }
        return response

    def search_bundle(
        self,
        queries: tuple[str, ...],
        *,
        lexical_queries: tuple[str, ...],
        since: str | None,
        until: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Generate one broad pool from a frozen bundle of planned queries."""

        if (
            not queries
            or len(queries) > 8
            or len(queries) != len(lexical_queries)
            or any(
                not isinstance(query, str)
                or not query.strip()
                or len(query) > 2048
                for query in (*queries, *lexical_queries)
            )
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("invalid candidate query bundle")
        def search(pair: tuple[str, str]) -> dict[str, Any]:
            query, lexical_query = pair
            return self.search(
                query,
                lexical_query=lexical_query,
                since=since,
                until=until,
                limit=limit,
                include_arms=True,
            )

        pairs = tuple(zip(queries, lexical_queries, strict=True))
        with ThreadPoolExecutor(
            max_workers=min(MAX_BUNDLE_SEARCH_WORKERS, len(pairs)),
            thread_name_prefix="recall-passage-query",
        ) as executor:
            responses = list(executor.map(search, pairs))
        arm_names = ("dense", "passage-lexical", "sparse-exact")

        def status(name: str) -> str:
            values = [
                response["diagnostics"][name]
                for response in responses
            ]
            if "deadline-exceeded" in values:
                return "deadline-exceeded"
            if "unavailable" in values:
                return "unavailable"
            if all(value == "disabled" for value in values):
                return "disabled"
            return "ok"

        return {
            "results": fuse_document_rankings(
                tuple(response["results"] for response in responses),
                limit=limit,
            ),
            "arms": {
                arm: fuse_document_rankings(
                    tuple(
                        response["arms"][arm]
                        for response in responses
                    ),
                    limit=limit,
                )
                for arm in arm_names
            },
            "diagnostics": {
                "engine": "lossless-passages-v1-bundle",
                "query_count": len(queries),
                "dense_status": status("dense_status"),
                "passage_lexical_status": status(
                    "passage_lexical_status"
                ),
                "sparse_status": status("sparse_status"),
            },
        }

    def search_representation(
        self,
        query: str,
        *,
        lexical_query: str,
        runtime: Any,
        representation_fingerprint: str,
        context_fingerprint: str | None,
        since: str | None,
        until: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Search one isolated shadow representation without production cutover."""

        if (
            not isinstance(query, str)
            or not query.strip()
            or len(query) > 2048
            or not isinstance(lexical_query, str)
            or not lexical_query.strip()
            or len(lexical_query) > 2048
            or not FINGERPRINT_RE.fullmatch(representation_fingerprint)
            or (
                context_fingerprint is not None
                and not FINGERPRINT_RE.fullmatch(context_fingerprint)
            )
            or getattr(runtime, "dimensions", None) not in VECTOR_COLUMNS
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("invalid passage representation search")
        candidate_limit = min(400, max(80, limit * 20))
        dense: list[dict[str, Any]] = []
        dense_status = "ok"
        vector_column = VECTOR_COLUMNS[runtime.dimensions]
        try:
            bounded = getattr(runtime, "embed_query_bounded", None)
            vector = (
                bounded(query)
                if bounded is not None
                else runtime.embed_query(query)
            )
            temporal_scope = since is not None or until is not None
            dense_oversample = 50 if temporal_scope else 5
            with self.store.connect() as connection:
                dense = self.store._execute_bounded(
                    connection,
                    sql.SQL("""WITH nearest AS MATERIALIZED (
                           SELECT represented.tenant_id,
                                  represented.source_id,
                                  represented.passage_id,
                                  represented.{vector_column}
                                      <=> %s::halfvec
                                      AS distance
                             FROM canonical_passage_embedding_representations
                                  represented
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                            WHERE represented.tenant_id=%s
                              AND represented.source_id=ANY(%s)
                              AND represented.representation_fingerprint=%s
                              AND represented.{vector_column} IS NOT NULL
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM unnest(passage.receipts)
                                         AS passage_receipt(receipt)
                                    LEFT JOIN canonical_chunks live_chunk
                                      ON live_chunk.tenant_id=passage.tenant_id
                                     AND live_chunk.source_id=passage.source_id
                                     AND live_chunk.receipt=
                                         passage_receipt.receipt
                                     AND live_chunk.deleted_at IS NULL
                                   WHERE live_chunk.receipt IS NULL
                              )
                            ORDER BY represented.{vector_column}
                                     <=> %s::halfvec
                            LIMIT %s
                       ), ranked_documents AS MATERIALIZED (
                           SELECT DISTINCT ON (
                                      passage.logical_document_id
                                  )
                                  passage.source_id,
                                  passage.logical_document_id,
                                  passage.revision,
                                  evidence.native_parent_id,
                                  evidence.first_occurred_at,
                                  evidence.last_occurred_at,
                                  evidence.manifest_object_key,
                                  evidence.manifest_content_sha256,
                                  passage.passage_id,
                                  passage.ordinal AS passage_ordinal,
                                  passage.spans,passage.receipts,
                                  passage.text_redacted,
                                  nearest.distance
                             FROM nearest
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   revision,policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(
                                   tenant_id,source_id,
                                   logical_document_id,revision
                               )
                            WHERE projected.policy_fingerprint=%s
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM unnest(passage.receipts)
                                         AS passage_receipt(receipt)
                                    LEFT JOIN canonical_chunks live_chunk
                                      ON live_chunk.tenant_id=passage.tenant_id
                                     AND live_chunk.source_id=passage.source_id
                                     AND live_chunk.receipt=
                                         passage_receipt.receipt
                                     AND live_chunk.deleted_at IS NULL
                                   WHERE live_chunk.receipt IS NULL
                              )
                              AND (%s::timestamptz IS NULL
                                   OR passage.last_occurred_at>=%s)
                              AND (%s::timestamptz IS NULL
                                   OR passage.first_occurred_at<=%s)
                            ORDER BY passage.logical_document_id,
                                     nearest.distance,
                                     passage.last_occurred_at DESC,
                                     passage.passage_id
                       )
                       SELECT *,1-distance AS score
                         FROM ranked_documents
                        ORDER BY distance,last_occurred_at DESC,passage_id
                        LIMIT %s""").format(
                        vector_column=sql.Identifier(vector_column),
                    ),
                    (
                        vector,
                        self.tenant_id,
                        self.sources,
                        representation_fingerprint,
                        vector,
                        candidate_limit * dense_oversample,
                        self.policy_fingerprint,
                        since,
                        since,
                        until,
                        until,
                        candidate_limit,
                    ),
                    (
                        time.monotonic()
                        + self.store.search_deadline_ms / 1000
                    ),
                ).fetchall()
        except (
            json.JSONDecodeError,
            SearchDeadlineExceeded,
            TimeoutError,
            urllib.error.URLError,
        ) as error:
            dense = []
            dense_status = (
                "deadline-exceeded"
                if isinstance(error, SearchDeadlineExceeded)
                else "unavailable"
            )

        lexical: list[dict[str, Any]] = []
        lexical_status = "disabled"
        if context_fingerprint is not None:
            try:
                with self.store.connect() as connection:
                    lexical = self.store._execute_bounded(
                        connection,
                        """SELECT passage.source_id,
                                  passage.logical_document_id,
                                  passage.revision,
                                  evidence.native_parent_id,
                                  evidence.first_occurred_at,
                                  evidence.last_occurred_at,
                                  evidence.manifest_object_key,
                                  evidence.manifest_content_sha256,
                                  passage.passage_id,
                                  passage.ordinal AS passage_ordinal,
                                  passage.spans,passage.receipts,
                                  passage.text_redacted,
                                  ts_rank_cd(
                                      context.search_vector,
                                      plainto_tsquery('simple',%s),
                                      32
                                  ) AS score
                             FROM canonical_passage_contexts context
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   revision,policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(
                                   tenant_id,source_id,
                                   logical_document_id,revision
                               )
                            WHERE context.tenant_id=%s
                              AND context.source_id=ANY(%s)
                              AND context.context_fingerprint=%s
                              AND projected.policy_fingerprint=%s
                              AND context.search_vector @@
                                  plainto_tsquery('simple',%s)
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM unnest(passage.receipts)
                                         AS passage_receipt(receipt)
                                    LEFT JOIN canonical_chunks live_chunk
                                      ON live_chunk.tenant_id=passage.tenant_id
                                     AND live_chunk.source_id=passage.source_id
                                     AND live_chunk.receipt=
                                         passage_receipt.receipt
                                     AND live_chunk.deleted_at IS NULL
                                   WHERE live_chunk.receipt IS NULL
                              )
                              AND (%s::timestamptz IS NULL
                                   OR passage.last_occurred_at>=%s)
                              AND (%s::timestamptz IS NULL
                                   OR passage.first_occurred_at<=%s)
                            ORDER BY score DESC,
                                     passage.last_occurred_at DESC,
                                     passage.passage_id
                            LIMIT %s""",
                        (
                            lexical_query,
                            self.tenant_id,
                            self.sources,
                            context_fingerprint,
                            self.policy_fingerprint,
                            lexical_query,
                            since,
                            since,
                            until,
                            until,
                            candidate_limit,
                        ),
                        (
                            time.monotonic()
                            + self.store.search_deadline_ms / 1000
                        ),
                    ).fetchall()
                lexical_status = "ok"
            except SearchDeadlineExceeded:
                lexical = []
                lexical_status = "deadline-exceeded"
        legs = (
            ("representation-dense", 0.70, dense),
            ("context-lexical", 0.30, lexical),
        )
        return {
            "results": collapse_document_candidates(legs, limit=limit),
            "arms": {
                name: collapse_document_candidates(
                    ((name, 1.0, rows),),
                    limit=limit,
                )
                for name, rows in (
                    ("representation-dense", dense),
                    ("context-lexical", lexical),
                )
            },
            "diagnostics": {
                "engine": "lossless-passage-representation-v1",
                "representation_fingerprint": representation_fingerprint,
                "context_fingerprint": context_fingerprint,
                "representation_dense_status": dense_status,
                "context_lexical_status": lexical_status,
                "representation_dense_candidates": len(dense),
                "context_lexical_candidates": len(lexical),
            },
        }
