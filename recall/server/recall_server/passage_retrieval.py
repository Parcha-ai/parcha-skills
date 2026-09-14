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
from psycopg_pool import PoolTimeout

from .actor_attribution import ACTOR_ID_RE, ACTOR_RELATIONS
from .db import SearchDeadlineExceeded, bounded_search_text
from .fusion import (
    ARM_NAMES,
    DEFAULT_FUSION_ALPHAS,
    DEFAULT_FUSION_MODE,
    RRF_LEG_WEIGHTS,
    leg_document_scores,
)
from .passage_representations import FINGERPRINT_RE, VECTOR_COLUMNS
from .rerank import (
    DEFAULT_RERANK_BLEND,
    DEFAULT_RERANK_MIN_BUDGET_SECONDS,
    RerankUnavailable,
)


MAX_BUNDLE_SEARCH_WORKERS = 4
MAX_EXACT_DENSE_SCOPE_PASSAGES = 20_000
# Forgotten passages are rare: rank first, then drop the few whose chunks are
# gone from an oversampled top-K instead of probing canonical_chunks per hit.
LIVENESS_OVERSAMPLE = 2
# The sparse-exact arm reads the same canonical_passages GIN index as the
# passage-lexical arm. A passage qualifies when it contains every
# informative query term (the lexical arm's own predicate, which the GIN
# index answers with a small bitmap) and at least one identifier token as
# an intact phrase (adjacent lexemes). Matching identifier phrases alone
# is not an option: a phrase predicate needs the TOASTed tsvector of every
# candidate, so on a common token ("503", a date) the arm reads a large
# share of the corpus, runs to its budget on every query, and pins a pool
# connection for that long while the disk pressure slows the other arms.
# That starved the whole pool in production. For prose the arm is
# skipped: dense and passage-lexical already cover the words.
# Known loss: identifiers that occur only inside tool output are no longer
# matched by search; they stay reachable through recall_scan records.
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
# The sparse arm's phrase recheck reads the TOASTed tsvector of every row in
# the lexical bitmap (every informative term present, in scope). Before the
# ranked phase, the arm counts that bitmap with an index-only probe capped
# at this many rows plus one: a plain AND tsquery needs no recheck, so the
# probe touches the GIN index and heap visibility only, never TOAST. When
# the bitmap is larger than this, the arm is skipped (status
# `skipped-selectivity`): dense and passage-lexical still cover the query,
# and the recheck would have run to the arm's budget while pinning a pool
# connection. 2000 rows is ~6000 random TOAST reads on the managed
# instance, inside the arm's ranked-phase share of the budget.
# Per identifier token: a real identifier is rare. Measured 2026-09-14: '6076'
# matches 110 passages, a date fragment like '2-4' 1,041 and 'p2' 8,029; ranking
# 1,151 phrase-matched rows read their TOASTed tsvectors for 10 s.
SPARSE_MAX_LEXICAL_MATCHES = 300
# Statuses that mean an arm hit its budget before finishing its full work,
# or never got a pooled connection inside it.
TRUNCATED_ARM_STATUSES = frozenset({"deadline-exceeded", "ok-recent-first", "pool-exhausted"})
# Ranking every full-text match by ts_rank_cd is proportional to the size of
# the match set. Common words match most of the corpus. Each text arm first
# tries the full ranking under a short share of the budget, then falls back
# to ranking only the most recent matches, which the index can stop early.
RANKED_PHASE_BUDGET_FRACTION = 0.7
# Ranking every full-text match reads each passage's TOASTed search_vector:
# ~3 random disk reads per match on the managed instance. The fallback orders
# matches by recency (inline columns only) instead.
# HNSW cost grows with the requested neighbour count; 200 neighbours cost
# ~1.7 s cold on the managed instance versus 3+ s for 400 and far more for
# the temporal ×50 oversample. Documents are ranked after the scan anyway.
DENSE_NEAREST_LIMIT = 400
# candidate_limit (20) x 20 = 400 = DENSE_NEAREST_LIMIT for a normal query.
DENSE_PROSE_OVERSAMPLE = 20
# hnsw.ef_search for the dense pool; must be >= DENSE_NEAREST_LIMIT or the
# index scan silently returns fewer rows than the LIMIT asks for.
DENSE_EF_SEARCH = 400
def _phase_deadline(deadline_at: float, fraction: float) -> float:
    now = time.monotonic()
    return min(deadline_at, now + max(0.0, deadline_at - now) * fraction)


# Bound the AND-ed phrase query so a pasted log line cannot turn into an
# arbitrarily wide tsquery; the first identifiers carry the question.
MAX_SPARSE_IDENTIFIER_TOKENS = 8


def identifier_tokens(*queries: str) -> list[str]:
    """Distinct identifier-shaped tokens, casefolded, in first-seen order.

    The 'simple' text search configuration lowercases lexemes, so a
    CamelCase symbol and its casefolded form are one phrase query.
    """

    tokens: list[str] = []
    seen: set[str] = set()
    for query in queries:
        for token in query.split():
            stripped = token.strip("\"'`()[]{},;")
            if len(stripped) < 3 or not IDENTIFIER_TOKEN_RE.fullmatch(stripped):
                continue
            folded = stripped.casefold()
            if folded in seen:
                continue
            seen.add(folded)
            tokens.append(folded)
            if len(tokens) >= MAX_SPARSE_IDENTIFIER_TOKENS:
                return tokens
    return tokens


def sparse_arm_applies(lexical_query: str) -> bool:
    """True when the query has at least one token that looks like an identifier."""

    return bool(identifier_tokens(lexical_query))
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
    fusion: str = "rrf",
    alphas: dict[str, float] | None = None,
    fusion_report: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fuse mechanical hints while keeping their strongest exact ranges.

    ``fusion="rrf"`` adds ``weight / (60 + rank)`` per leg (the leg weight
    travels in ``legs``). ``fusion="convex"`` min-max normalises each leg's
    best document score and adds ``alphas[leg] × normalised`` (falling back
    to the leg weight when ``alphas`` lacks the leg); see ``fusion.py`` for
    the small-leg and recent-first fallbacks. ``fusion_report`` receives
    per-leg ``{"candidates", "documents", "normalized"}`` when given.
    """

    documents: dict[str, dict[str, Any]] = {}
    for leg_name, weight, rows in legs:
        leg_scores, normalized = leg_document_scores(rows, mode=fusion)
        leg_weight = (
            float(alphas.get(leg_name, weight))
            if fusion == "convex" and alphas is not None
            else weight
        )
        if fusion_report is not None:
            fusion_report[leg_name] = {
                "candidates": len(rows),
                "documents": len(leg_scores),
                "normalized": normalized,
            }
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
                    "_arm_scores": {},
                },
            )
            if document_id not in seen_documents:
                leg_entry = leg_scores[document_id]
                value["_score"] += leg_weight * float(leg_entry["normalized"])
                value["_arm_scores"][leg_name] = {
                    "score": round(float(leg_entry["score"]), 8),
                    "rank": int(leg_entry["rank"]),
                    "normalized": round(float(leg_entry["normalized"]), 8),
                }
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
                if row.get("passage_first_occurred_at") is not None and row.get(
                    "passage_last_occurred_at"
                ) is not None:
                    # Lets the time clip skip the per-receipt event lookup when
                    # the whole range already sits inside the requested window.
                    hint["passage_window"] = [
                        str(row["passage_first_occurred_at"]),
                        str(row["passage_last_occurred_at"]),
                    ]
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
        arm_scores = value.pop("_arm_scores")
        results.append({
            **value,
            "rank": round(score, 8),
            "reasons": reasons,
            "matching_ranges": ranges,
            # Content-free per-arm evidence (raw best score, arm rank, and
            # the normalised value the fused score used) so the systems-card
            # probe can save candidates for offline alpha tuning.
            "arm_scores": arm_scores,
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


def _range_key(item: dict[str, Any]) -> str | None:
    """The key ``collapse_document_candidates`` used for this hint."""

    receipts = item.get("receipts") or ()
    return item.get("passage_id") or (receipts[0] if receipts else None)


def select_rerank_candidates(
    results: list[dict[str, Any]],
    *,
    max_candidates: int,
) -> list[tuple[int, str]]:
    """Pick up to ``max_candidates`` passages from the fused document order.

    Round-robin over documents (each document's strongest hint first, then
    its second, ...) so the reranker sees the widest set of documents rather
    than three passages each from the top seventeen. Returns
    ``(document_index, range_key)`` pairs in send order.
    """

    selected: list[tuple[int, str]] = []
    seen: set[str] = set()
    depth = max((len(row.get("matching_ranges") or ()) for row in results), default=0)
    for position in range(depth):
        for document_index, row in enumerate(results):
            ranges = row.get("matching_ranges") or ()
            if position >= len(ranges):
                continue
            key = _range_key(ranges[position])
            if key is None or key in seen:
                continue
            seen.add(key)
            selected.append((document_index, key))
            if len(selected) >= max_candidates:
                return selected
    return selected


def _min_max(values: list[float]) -> list[float]:
    low, high = min(values), max(values)
    if high - low <= 1e-12:
        return [1.0 for _ in values]
    return [(value - low) / (high - low) for value in values]


def apply_rerank_scores(
    results: list[dict[str, Any]],
    scores: dict[str, float],
    *,
    blend: float = 1.0,
) -> list[dict[str, Any]]:
    """Re-order fused documents by their best reranked passage.

    A document's ``rerank_score`` is the maximum over its scored ranges; its
    ranges are re-ordered so scored ones lead (best first) and unscored ones
    keep their fused order behind them. Documents with no scored range keep
    the fused order after every reranked document, so a provider that
    returns fewer rows than it was sent never drops a candidate. ``rank``
    (the fused score) is left untouched for the diagnostics trail.
    """

    reranked: list[tuple[float, int, dict[str, Any]]] = []
    unscored: list[dict[str, Any]] = []
    for document_index, row in enumerate(results):
        best: float | None = None
        ranges = []
        for item in row.get("matching_ranges") or ():
            score = scores.get(_range_key(item) or "")
            if score is None:
                ranges.append(item)
                continue
            ranges.append({**item, "rerank_score": round(score, 8)})
            best = score if best is None else max(best, score)
        if best is None:
            unscored.append(row)
            continue
        ranges.sort(
            key=lambda item: ("rerank_score" in item, item.get("rerank_score", 0.0)),
            reverse=True,
        )
        reranked.append(
            (best, document_index, {**row, "matching_ranges": ranges, "rerank_score": round(best, 8)})
        )
    if reranked and 0.0 <= blend < 1.0:
        # Blend the reranker with the fused arm score (both min-max scaled
        # over the reranked set) so a document every arm agreed on is not
        # thrown away by one cross-encoder judgement.
        rerank_norm = _min_max([entry[0] for entry in reranked])
        fused_norm = _min_max([float(entry[2].get("rank") or 0.0) for entry in reranked])
        blended = []
        for (best, document_index, row), r_norm, f_norm in zip(reranked, rerank_norm, fused_norm, strict=True):
            final = blend * r_norm + (1.0 - blend) * f_norm
            blended.append((final, document_index, {**row, "blended_score": round(final, 8)}))
        blended.sort(key=lambda entry: (-entry[0], entry[1]))
        return [row for _score, _index, row in blended] + unscored
    reranked.sort(key=lambda entry: (-entry[0], entry[1]))
    return [row for _score, _index, row in reranked] + unscored


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
        # One pooled connection per arm: the recency fallback reuses the
        # connection of the cancelled ranked phase instead of queueing for
        # another one while the pool is under load.
        try:
            with self.store.connect() as connection:
                try:
                    return self._lexical_query(
                        connection, lexical_query, order="rank",
                        deadline_at=_phase_deadline(deadline_at, RANKED_PHASE_BUDGET_FRACTION),
                        **arguments,
                    ), "ok"
                except SearchDeadlineExceeded:
                    pass
                try:
                    return self._lexical_query(
                        connection, lexical_query, order="recent",
                        deadline_at=deadline_at, **arguments,
                    ), "ok-recent-first"
                except SearchDeadlineExceeded:
                    return [], "deadline-exceeded"
        except PoolTimeout:
            return [], "pool-exhausted"

    def _lexical_query(
        self,
        connection: Any,
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
                              passage.first_occurred_at,
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
                              evidence.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              top.passage_id,top.passage_ordinal,
                              top.spans,top.receipts,
                              top.text_redacted,
                              top.first_occurred_at AS passage_first_occurred_at,
                              top.last_occurred_at AS passage_last_occurred_at,
                              {score_sql} AS score
                         FROM top
                         JOIN canonical_passage_documents projected
                           USING(
                               tenant_id,source_id,logical_document_id,
                               policy_fingerprint
                           )
                         JOIN canonical_evidence_documents evidence
                           USING(tenant_id,source_id,logical_document_id)
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
        # The informative-term query is casefolded; read the original text
        # too so CamelCase symbols and ALL_CAPS codes still qualify.
        tokens = identifier_tokens(
            lexical_query, *(() if original_query is None else (original_query,))
        )
        if not tokens:
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
            with self.store.connect() as connection:
                # Probe each identifier on its own with a GIN-only lexeme
                # query (no phrase recheck) and keep only the rare ones; a
                # common token such as a version or a date fragment must not
                # drag thousands of rows into the ranked scan.
                kept: list[str] = []
                for token in tokens:
                    try:
                        matches = self._sparse_lexical_matches(
                            connection, (token,), deadline_at=arm_deadline, **arguments,
                        )
                    except SearchDeadlineExceeded:
                        return [], "deadline-exceeded"
                    if matches <= SPARSE_MAX_LEXICAL_MATCHES:
                        kept.append(token)
                if not kept:
                    return [], "skipped-selectivity"
                tokens = tuple(kept)
                try:
                    return self._sparse_query(
                        connection, tokens, lexical_query=lexical_query, order="rank",
                        deadline_at=_phase_deadline(arm_deadline, RANKED_PHASE_BUDGET_FRACTION),
                        **arguments,
                    ), "ok"
                except SearchDeadlineExceeded:
                    pass
                try:
                    return self._sparse_query(
                        connection, tokens, lexical_query=lexical_query, order="recent",
                        deadline_at=arm_deadline, **arguments,
                    ), "ok-recent-first"
                except SearchDeadlineExceeded:
                    return [], "deadline-exceeded"
        except PoolTimeout:
            return [], "pool-exhausted"

    def _sparse_lexical_matches(
        self,
        connection: Any,
        tokens: tuple[str, ...],
        *,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> int:
        """Size of one identifier's lexeme bitmap, capped (GIN only, no recheck).

        Same scope predicates as the sparse and lexical arms, the OR of the
        identifier phrases only (no rank), and a LIMIT one above the cap: the
        answer is exact up to the cap and the probe never reads more than
        cap + 1 index entries. An identifier that is common enough to blow
        the cap ("503", "v2") makes the arm skip itself; a rare one ("6076")
        qualifies on its own, which the previous AND-with-every-term
        predicate never allowed for a natural-language question.
        """
        del candidate_limit
        # plainto_tsquery over one identifier is an AND of its lexemes and is
        # answered from the GIN index alone; the phrase recheck is paid only
        # in the scan, and only for tokens that passed this probe.
        phrase_sql = "(" + " || ".join("plainto_tsquery('simple',%s)" for _ in tokens) + ")"
        rows = self.store._execute_bounded(
            connection,
            f"""SELECT count(*) AS n
                 FROM (
                   SELECT 1
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
                          {phrase_sql}
                      AND (%s::timestamptz IS NULL
                           OR passage.last_occurred_at>=%s)
                      AND (%s::timestamptz IS NULL
                           OR passage.first_occurred_at<=%s)
                    LIMIT %s
                 ) probe""",
            (
                self.tenant_id,
                self.sources,
                self.policy_fingerprint,
                actor_ids,
                actor_ids,
                actor_relations,
                actor_relations,
                *tokens,
                since,
                since,
                until,
                until,
                SPARSE_MAX_LEXICAL_MATCHES + 1,
            ),
            deadline_at,
        ).fetchall()
        return int(rows[0]["n"]) if rows else 0

    def _sparse_query(
        self,
        connection: Any,
        tokens: list[str],
        *,
        lexical_query: str,
        order: str,
        since: str | None,
        until: str | None,
        candidate_limit: int,
        actor_ids: list[str] | None,
        actor_relations: list[str] | None,
        deadline_at: float,
    ) -> list[dict[str, Any]]:
        """One bounded exact-identifier scan over canonical_passages.

        The match predicate is the lexical arm's `plainto_tsquery` over every
        informative term AND-ed with the OR of one `phraseto_tsquery` per
        identifier token, so the GIN index narrows the candidates to the
        lexical match set before any phrase recheck reads a tsvector. The
        ranked phase orders by the cover density of the whole query, then
        identifier density, then recency; the recent phase by recency
        alone. `matched` touches only the passage table with the same
        tenant, source, policy, actor, and time scope as the lexical arm;
        the projection, evidence, and chunk-liveness joins run on the
        bounded pool.
        """
        if not tokens:
            raise ValueError("sparse query needs identifier tokens")
        phrase_sql = "(" + " || ".join("phraseto_tsquery('simple',%s)" for _ in tokens) + ")"
        phrase_values: tuple[str, ...] = tuple(tokens)
        # Match on the identifier phrases alone; the whole-query cover density
        # still leads the ranking so a passage that also carries the other
        # terms outranks one that only mentions the identifier.
        query_sql = f"({phrase_sql})"
        query_values: tuple[str, ...] = tuple(phrase_values)
        if order == "rank":
            pool_order = (
                "ts_rank_cd(passage.search_vector,plainto_tsquery('simple',%s),32) DESC,"
                f"ts_rank_cd(passage.search_vector,{phrase_sql},32) DESC,"
                "passage.last_occurred_at DESC,passage.passage_id"
            )
            order_values: tuple[str, ...] = (lexical_query, *phrase_values)
            score_sql = (
                "ts_rank_cd(top.search_vector,plainto_tsquery('simple',%s),32)"
                f"+ts_rank_cd(top.search_vector,{phrase_sql},32)"
            )
            score_values: tuple[str, ...] = (lexical_query, *phrase_values)
        elif order == "recent":
            pool_order = "passage.last_occurred_at DESC,passage.passage_id"
            order_values = ()
            score_sql = "0.0::real"
            score_values = ()
        else:
            raise ValueError("unsupported sparse order")
        pool_limit = candidate_limit * LIVENESS_OVERSAMPLE
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
                              passage.first_occurred_at,
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
                          AND passage.search_vector @@ {query_sql}
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
                              evidence.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              top.passage_id,top.passage_ordinal,
                              top.spans,top.receipts,
                              top.text_redacted,
                              top.first_occurred_at AS passage_first_occurred_at,
                              top.last_occurred_at AS passage_last_occurred_at,
                              {score_sql} AS score
                         FROM top
                         JOIN canonical_passage_documents projected
                           USING(
                               tenant_id,source_id,logical_document_id,
                               policy_fingerprint
                           )
                         JOIN canonical_evidence_documents evidence
                           USING(tenant_id,source_id,logical_document_id)
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
                        *query_values,
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
                           USING(tenant_id,source_id,logical_document_id)
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
        except (PoolTimeout, SearchDeadlineExceeded):
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
            # A non-temporal query used to pull only candidate_limit x 5 = 100
            # nearest passages; one 5,000-passage session on a related topic
            # filled the whole pool and a small session's single passage never
            # reached the per-document collapse (validation recall@20 fell
            # 0.54 -> 0.46 after H1 re-windowed the big sessions). Pull the
            # full DENSE_NEAREST_LIMIT for every query; liveness is still
            # checked once on the pool, so the cost is bounded.
            dense_oversample = 50 if temporal_scope else DENSE_PROSE_OVERSAMPLE
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
                                   logical_document_id,policy_fingerprint
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
            # Identical passage text (subagent transcripts, replayed tool
            # output) carries identical vectors: production had 102k passages
            # in 12.8k duplicate groups, one text repeated 879 times. Keep one
            # row per distinct text before the per-document collapse so the
            # pool spans documents instead of copies.
            dense_sql = nearest_sql + """, distinct_texts AS MATERIALIZED (
                           SELECT DISTINCT ON (passage.text_sha256)
                                  nearest.tenant_id,
                                  nearest.source_id,
                                  nearest.passage_id,
                                  nearest.distance
                             FROM nearest
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                            ORDER BY passage.text_sha256,
                                     nearest.distance,
                                     passage.last_occurred_at DESC,
                                     passage.passage_id
                       ), ranked_documents AS MATERIALIZED (
                           SELECT DISTINCT ON (passage.logical_document_id)
                                  passage.source_id,
                                  passage.logical_document_id,
                                  evidence.revision,
                                  evidence.native_parent_id,
                                  evidence.first_occurred_at,
                                  evidence.last_occurred_at,
                                  evidence.manifest_object_key,
                                  evidence.manifest_content_sha256,
                                  passage.passage_id,
                                  passage.ordinal AS passage_ordinal,
                                  passage.spans,passage.receipts,
                                  passage.text_redacted,
                                  passage.first_occurred_at
                                      AS passage_first_occurred_at,
                                  passage.last_occurred_at
                                      AS passage_last_occurred_at,
                                  nearest.distance
                             FROM distinct_texts nearest
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(tenant_id,source_id,logical_document_id)
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
                # ef_search caps how many rows one index scan can return: at the
                # default 40 the 'top 400' pool was 40 passages (17 documents)
                # regardless of LIMIT. Raise it, transaction-locally, to the pool
                # size (753 ms cold for 400 rows on PS-160). iterative_scan stays
                # at the default: strict_order walked the graph for 16 s p50.
                connection.execute(
                    "SELECT set_config('hnsw.ef_search', %s, true)",
                    (str(int(DENSE_EF_SEARCH)),),
                )
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
            PoolTimeout,
            SearchDeadlineExceeded,
            TimeoutError,
            urllib.error.URLError,
        ) as error:
            if isinstance(error, SearchDeadlineExceeded):
                status = "deadline-exceeded"
            elif isinstance(error, PoolTimeout):
                status = "pool-exhausted"
            else:
                status = "unavailable"
            return [], status, "unavailable", None
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
        # Searches take up to three pooled connections each. The store
        # admits only as many concurrent searches as leave headroom for the
        # single-connection tools, so a slow arm can never starve show or
        # scope of a connection. Waiting counts against the search budget.
        admission = getattr(self.store, "search_admission", None)
        if admission is not None and not admission.acquire(
            timeout=max(0.0, deadline_at - time.monotonic())
        ):
            return {
                "results": [],
                "diagnostics": {
                    "engine": "lossless-passages-v1",
                    "policy_fingerprint": self.policy_fingerprint,
                    "reason": "search-admission-timeout",
                    "candidate_depth": candidate_limit,
                    "result_limit": limit,
                    "arms_truncated": ["dense", "passage_lexical", "sparse_exact"],
                    "elapsed_ms": round((time.monotonic() - started_at) * 1000, 3),
                    "deadline_ms": self.store.search_deadline_ms,
                    "deadline_exceeded": True,
                    "partial_results_preserved": False,
                },
            }
        try:
            return self._search_admitted(
                query, lexical_query=lexical_query, limit=limit, include_arms=include_arms,
                started_at=started_at, deadline_at=deadline_at, candidate_limit=candidate_limit,
                common=common, arm_elapsed_ms=arm_elapsed_ms,
            )
        finally:
            if admission is not None:
                admission.release()

    def _search_admitted(
        self,
        query: str,
        *,
        lexical_query: str,
        limit: int,
        include_arms: bool,
        started_at: float,
        deadline_at: float,
        candidate_limit: int,
        common: dict[str, Any],
        arm_elapsed_ms: dict[str, float],
    ) -> dict[str, Any]:
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
            ("dense", RRF_LEG_WEIGHTS["dense"], dense),
            ("passage-lexical", RRF_LEG_WEIGHTS["passage-lexical"], lexical),
            ("sparse-exact", RRF_LEG_WEIGHTS["sparse-exact"], sparse),
        )
        fusion_mode = getattr(self.store, "fusion_mode", DEFAULT_FUSION_MODE)
        fusion_alphas = dict(
            getattr(self.store, "fusion_alphas", None) or DEFAULT_FUSION_ALPHAS
        )
        fusion_legs: dict[str, Any] = {}
        rerank_runtime = getattr(self.store, "rerank_runtime", None)
        # H2-c: with a reranker the fused pool must reach past ``limit`` so a
        # document the arms placed at position 40 can still be promoted. The
        # first ``limit`` rows of the wider collapse are exactly the rows the
        # narrow collapse returns, so the disabled path stays byte-identical.
        collapse_limit = (
            max(limit, int(rerank_runtime.max_candidates))
            if rerank_runtime is not None
            else limit
        )
        results = collapse_document_candidates(
            legs,
            limit=collapse_limit,
            fusion=fusion_mode,
            alphas=fusion_alphas,
            fusion_report=fusion_legs,
        )
        # Arms that ran out of budget: either they returned nothing or the
        # text arms fell back from full ranking to a recency window. Lets a
        # card tell IO pressure (many truncated arms) from a ranking change.
        arms_truncated = [
            name
            for name, status in (
                ("dense", dense_status),
                ("passage_lexical", lexical_status),
                ("sparse_exact", sparse_status),
            )
            if status in TRUNCATED_ARM_STATUSES
        ]
        rerank_diagnostics: dict[str, Any] = {"rerank_status": "skipped-disabled"}
        if rerank_runtime is not None:
            results, rerank_diagnostics = self._rerank_fused(
                query,
                results,
                legs,
                runtime=rerank_runtime,
                deadline_at=deadline_at,
                arm_elapsed_ms=arm_elapsed_ms,
            )
        results = results[:limit]
        response = {
            "results": results,
            "diagnostics": {
                "engine": "lossless-passages-v1",
                "fusion": {
                    "mode": fusion_mode,
                    "alphas": (
                        {arm: fusion_alphas[arm] for arm in ARM_NAMES}
                        if fusion_mode == "convex"
                        else dict(RRF_LEG_WEIGHTS)
                    ),
                    "legs": fusion_legs,
                },
                "policy_fingerprint": self.policy_fingerprint,
                "candidate_depth": candidate_limit,
                "result_limit": limit,
                "sparse_source": "passages",
                "arms_truncated": arms_truncated,
                "dense_candidates": len(dense),
                "passage_lexical_candidates": len(lexical),
                "sparse_candidates": len(sparse),
                "dense_status": dense_status,
                "dense_strategy": dense_strategy,
                "dense_scope_passages": dense_scope_passages,
                "passage_lexical_status": lexical_status,
                "sparse_status": sparse_status,
                "arm_elapsed_ms": arm_elapsed_ms,
                **rerank_diagnostics,
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

    def _rerank_fused(
        self,
        query: str,
        results: list[dict[str, Any]],
        legs: tuple[tuple[str, float, list[dict[str, Any]]], ...],
        *,
        runtime: Any,
        deadline_at: float,
        arm_elapsed_ms: dict[str, float],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Rerank the fused passage pool; on any shortfall keep the fused order.

        Contract: the provider receives the query plus the redacted text of at
        most ``runtime.max_candidates`` passages (the runtime truncates each to
        its configured width). Statuses: ``ok`` (scores applied, or nothing to
        send), ``skipped-budget`` (less than the minimum budget remained after
        the arms), ``unavailable`` (``RerankUnavailable``; fused order kept).
        ``arm_elapsed_ms["rerank"]`` is set only when the provider was called
        so latency percentiles measure real round trips.
        """

        diagnostics: dict[str, Any] = {
            "rerank_status": "ok",
            "rerank_elapsed_ms": 0.0,
            "rerank_candidates": 0,
            "rerank_model": runtime.fingerprint,
        }
        remaining = deadline_at - time.monotonic()
        min_budget = getattr(
            self.store, "rerank_min_budget_seconds", DEFAULT_RERANK_MIN_BUDGET_SECONDS
        )
        if remaining < min_budget:
            diagnostics["rerank_status"] = "skipped-budget"
            return results, diagnostics
        texts: dict[str, str] = {}
        for _leg_name, _weight, rows in legs:
            for row in rows:
                key = row.get("passage_id") or row.get("receipt")
                if key and key not in texts:
                    texts[key] = row["text_redacted"]
        # The same passage can reach the pool under two keys (a passage id
        # from one arm, a receipt from another); send its text once and let
        # both keys share the score.
        documents: list[str] = []
        keys_by_document: list[list[str]] = []
        document_by_text: dict[str, int] = {}
        for _document_index, key in select_rerank_candidates(
            results, max_candidates=int(runtime.max_candidates)
        ):
            text = texts.get(key)
            if text is None:
                continue
            position = document_by_text.get(text)
            if position is None:
                position = len(documents)
                document_by_text[text] = position
                documents.append(text)
                keys_by_document.append([])
            keys_by_document[position].append(key)
        diagnostics["rerank_candidates"] = len(documents)
        if not documents:
            return results, diagnostics
        started = time.monotonic()
        try:
            scored = runtime.rerank(
                query,
                documents,
                deadline_seconds=deadline_at - started,
            )
        except RerankUnavailable as error:
            elapsed = round((time.monotonic() - started) * 1000, 3)
            arm_elapsed_ms["rerank"] = elapsed
            diagnostics.update({
                "rerank_status": "unavailable",
                "rerank_elapsed_ms": elapsed,
                "rerank_error": error.code,
            })
            return results, diagnostics
        elapsed = round((time.monotonic() - started) * 1000, 3)
        arm_elapsed_ms["rerank"] = elapsed
        diagnostics["rerank_elapsed_ms"] = elapsed
        scores = {
            key: float(score)
            for index, score in scored
            if 0 <= index < len(documents)
            for key in keys_by_document[index]
        }
        blend = float(getattr(self.store, "rerank_blend", DEFAULT_RERANK_BLEND))
        diagnostics["rerank_blend"] = blend
        return apply_rerank_scores(results, scores, blend=blend), diagnostics

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
            # A non-temporal query used to pull only candidate_limit x 5 = 100
            # nearest passages; one 5,000-passage session on a related topic
            # filled the whole pool and a small session's single passage never
            # reached the per-document collapse (validation recall@20 fell
            # 0.54 -> 0.46 after H1 re-windowed the big sessions). Pull the
            # full DENSE_NEAREST_LIMIT for every query; liveness is still
            # checked once on the pool, so the cost is bounded.
            dense_oversample = 50 if temporal_scope else DENSE_PROSE_OVERSAMPLE
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
                                  evidence.revision,
                                  evidence.native_parent_id,
                                  evidence.first_occurred_at,
                                  evidence.last_occurred_at,
                                  evidence.manifest_object_key,
                                  evidence.manifest_content_sha256,
                                  passage.passage_id,
                                  passage.ordinal AS passage_ordinal,
                                  passage.spans,passage.receipts,
                                  passage.text_redacted,
                                  passage.first_occurred_at
                                      AS passage_first_occurred_at,
                                  passage.last_occurred_at
                                      AS passage_last_occurred_at,
                                  nearest.distance
                             FROM nearest
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                             JOIN canonical_passage_documents projected
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(tenant_id,source_id,logical_document_id)
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
                                  evidence.revision,
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
                                   policy_fingerprint
                               )
                             JOIN canonical_evidence_documents evidence
                               USING(tenant_id,source_id,logical_document_id)
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
