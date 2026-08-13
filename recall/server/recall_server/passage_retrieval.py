"""Hybrid hints collapsed to authorized logical-document boundaries."""

from __future__ import annotations

import json
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
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT passage.source_id,
                              passage.logical_document_id,
                              passage.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,
                              evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              passage.passage_id,
                              passage.ordinal AS passage_ordinal,
                              passage.spans,passage.receipts,
                              passage.text_redacted,
                              ts_rank_cd(
                                  passage.search_vector,
                                  plainto_tsquery('simple',%s),
                                  32
                              ) AS score
                         FROM canonical_passages passage
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
                        WHERE passage.tenant_id=%s
                          AND passage.source_id=ANY(%s)
                          AND projected.policy_fingerprint=%s
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
                        ORDER BY score DESC,passage.last_occurred_at DESC,
                                 passage.passage_id
                        LIMIT %s""",
                    (
                        lexical_query,
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
                        candidate_limit,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return [], "deadline-exceeded"
        return rows, "ok"

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
    ) -> tuple[list[dict[str, Any]], str]:
        if self.actor_scope:
            return [], "skipped-actor-scope"
        try:
            with self.store.connect() as connection:
                rows = self.store._execute_bounded(
                    connection,
                    """SELECT event.source_id,
                              evidence.logical_document_id,
                              evidence.revision,evidence.native_parent_id,
                              evidence.first_occurred_at,
                              evidence.last_occurred_at,
                              evidence.manifest_object_key,
                              evidence.manifest_content_sha256,
                              chunk.receipt,chunk.text_redacted,
                              ts_rank_cd(
                                  chunk.search_vector,
                                  plainto_tsquery('simple',%s),
                                  32
                              ) AS score
                         FROM canonical_chunks chunk
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
                        WHERE chunk.tenant_id=%s
                          AND chunk.source_id=ANY(%s)
                          AND chunk.deleted_at IS NULL
                          AND document.is_current
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
                          AND chunk.search_vector @@
                              plainto_tsquery('simple',%s)
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at>=%s)
                          AND (%s::timestamptz IS NULL
                               OR event.occurred_at<=%s)
                        ORDER BY score DESC,event.occurred_at DESC,
                                 chunk.chunk_id
                        LIMIT %s""",
                    (
                        lexical_query,
                        self.tenant_id,
                        self.sources,
                        actor_ids,
                        actor_ids,
                        actor_relations,
                        actor_relations,
                        lexical_query,
                        since,
                        since,
                        until,
                        until,
                        candidate_limit,
                    ),
                    deadline_at,
                ).fetchall()
        except SearchDeadlineExceeded:
            return [], "deadline-exceeded"
        return rows, "ok"

    def _dense_scope_passage_count(
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
                nearest_sql = """WITH nearest AS MATERIALIZED (
                           SELECT embedding.tenant_id,
                                  embedding.source_id,
                                  embedding.passage_id,
                                  embedding.embedding <=> %s::halfvec AS distance
                             FROM canonical_passage_embeddings embedding
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                            WHERE embedding.tenant_id=%s
                              AND embedding.source_id=ANY(%s)
                              AND embedding.runtime_fingerprint=%s
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
                            ORDER BY embedding.embedding <=> %s::halfvec
                            LIMIT %s
                       )"""
                nearest_values = (
                    vector,
                    self.tenant_id,
                    self.sources,
                    runtime.passage_fingerprint,
                    vector,
                    candidate_limit * dense_oversample,
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
        with ThreadPoolExecutor(max_workers=3) as executor:
            lexical_future = executor.submit(
                self._lexical_candidates,
                lexical_query,
                **common,
            )
            sparse_future = executor.submit(
                self._sparse_candidates,
                lexical_query,
                **common,
            )
            dense_future = executor.submit(
                self._dense_candidates,
                query,
                **common,
            )
            lexical, lexical_status = lexical_future.result()
            sparse, sparse_status = sparse_future.result()
            (
                dense,
                dense_status,
                dense_strategy,
                dense_scope_passages,
            ) = dense_future.result()
        legs = (
            ("dense", 0.55, dense),
            ("passage-lexical", 0.30, lexical),
            ("sparse-exact", 0.15, sparse),
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
