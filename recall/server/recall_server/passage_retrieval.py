"""Hybrid hints collapsed to authorized logical-document boundaries."""

from __future__ import annotations

import json
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from psycopg import sql

from .db import SearchDeadlineExceeded, bounded_search_text
from .passage_representations import FINGERPRINT_RE, VECTOR_COLUMNS


MAX_BUNDLE_SEARCH_WORKERS = 4
EXACT_DENSE_EXECUTION = "exact-sequential-v1"
EXACT_DENSE_SETTINGS = {
    "enable_indexscan": "off",
    "enable_bitmapscan": "off",
    "enable_seqscan": "on",
}


def _configure_exact_dense_search(connection: Any) -> dict[str, str]:
    """Make one dense-query transaction use the measured exact oracle."""

    observed = connection.execute(
        """SELECT set_config('enable_indexscan','off',true)
                          AS enable_indexscan,
                  set_config('enable_bitmapscan','off',true)
                          AS enable_bitmapscan,
                  set_config('enable_seqscan','on',true)
                          AS enable_seqscan"""
    ).fetchone()
    settings = {
        name: str(observed[name])
        for name in EXACT_DENSE_SETTINGS
    }
    if settings != EXACT_DENSE_SETTINGS:
        raise RuntimeError("exact dense search settings were not applied")
    return settings


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


def fuse_need_rankings(
    rankings: tuple[list[dict[str, Any]], ...],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Reserve an equal candidate share per agent-authored evidence need."""

    if not rankings or limit < len(rankings):
        raise ValueError("invalid evidence need rankings")
    if len(rankings) == 1:
        return rankings[0][:limit]
    quota = max(1, limit // len(rankings))
    retained: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ranking in rankings:
        admitted = 0
        for candidate in ranking:
            document_id = candidate["logical_document_id"]
            if document_id in seen:
                continue
            retained.append(candidate)
            seen.add(document_id)
            admitted += 1
            if admitted == quota:
                break
    if len(retained) < limit:
        for candidate in fuse_document_rankings(
            rankings,
            limit=limit,
        ):
            document_id = candidate["logical_document_id"]
            if document_id in seen:
                continue
            retained.append(candidate)
            seen.add(document_id)
            if len(retained) == limit:
                break
    return fuse_document_rankings((retained,), limit=limit)


class PassageHintRetrieval:
    """Read-only retrieval over one selected lossless passage policy."""

    def __init__(
        self,
        store: Any,
        *,
        tenant_id: str,
        sources: list[str],
        policy_fingerprint: str,
    ) -> None:
        self.store = store
        self.tenant_id = tenant_id
        self.sources = sources
        self.policy_fingerprint = policy_fingerprint

    def search(
        self,
        query: str,
        *,
        lexical_query: str,
        since: str | None,
        until: str | None,
        limit: int,
        include_arms: bool = False,
    ) -> dict[str, Any]:
        candidate_limit = min(400, max(80, limit * 20))
        with self.store.connect() as connection:
            try:
                lexical = self.store._execute_bounded(
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
                          AND passage.search_vector @@
                              plainto_tsquery('simple',%s)
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
                        lexical_query,
                        since,
                        since,
                        until,
                        until,
                        candidate_limit,
                    ),
                    time.monotonic()
                    + self.store.search_deadline_ms / 1000,
                ).fetchall()
                lexical_status = "ok"
            except SearchDeadlineExceeded:
                lexical = []
                lexical_status = "deadline-exceeded"
        with self.store.connect() as connection:
            try:
                sparse = self.store._execute_bounded(
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
                        lexical_query,
                        since,
                        since,
                        until,
                        until,
                        candidate_limit,
                    ),
                    time.monotonic()
                    + self.store.search_deadline_ms / 1000,
                ).fetchall()
                sparse_status = "ok"
            except SearchDeadlineExceeded:
                sparse = []
                sparse_status = "deadline-exceeded"

        dense: list[dict[str, Any]] = []
        runtime = self.store.semantic_runtime
        dense_status = "disabled" if runtime is None else "ok"
        dense_latency_ms = 0.0
        if runtime is not None:
            dense_started = time.monotonic()
            try:
                bounded = getattr(runtime, "embed_query_bounded", None)
                vector = (
                    bounded(query)
                    if bounded is not None
                    else runtime.embed_query(query)
                )
                temporal_scope = since is not None or until is not None
                dense_oversample = 50 if temporal_scope else 5
                dense_requested_passages = candidate_limit * dense_oversample
                with self.store.connect() as connection:
                    _configure_exact_dense_search(connection)
                    dense = self.store._execute_bounded(
                        connection,
                        """WITH nearest AS MATERIALIZED (
                               SELECT embedding.tenant_id,
                                      embedding.source_id,
                                      embedding.passage_id,
                                      embedding.embedding
                                          <=> %s::halfvec AS distance
                                 FROM canonical_passage_embeddings embedding
                                WHERE embedding.tenant_id=%s
                                  AND embedding.source_id=ANY(%s)
                                  AND embedding.runtime_fingerprint=%s
                                ORDER BY embedding.embedding
                                         <=> %s::halfvec,
                                         embedding.source_id,
                                         embedding.passage_id
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
                                       tenant_id,source_id,
                                       logical_document_id,
                                       revision,policy_fingerprint
                                   )
                                 JOIN canonical_evidence_documents evidence
                                   USING(
                                       tenant_id,source_id,
                                       logical_document_id,revision
                                   )
                                WHERE projected.policy_fingerprint=%s
                                  AND (%s::timestamptz IS NULL
                                       OR passage.last_occurred_at>=%s)
                                  AND (%s::timestamptz IS NULL
                                       OR passage.first_occurred_at<=%s)
                                ORDER BY passage.logical_document_id,
                                         nearest.distance,
                                         passage.last_occurred_at DESC,
                                         passage.passage_id
                           )
                           SELECT *,1-distance AS score,
                                  (SELECT count(*) FROM nearest)
                                      AS dense_passage_count,
                                  (SELECT count(*) FROM ranked_documents)
                                      AS dense_document_count
                             FROM ranked_documents
                            ORDER BY distance,last_occurred_at DESC,passage_id
                            LIMIT %s""",
                        (
                            vector,
                            self.tenant_id,
                            self.sources,
                            runtime.passage_fingerprint,
                            vector,
                            dense_requested_passages,
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
            finally:
                dense_latency_ms = round(
                    (time.monotonic() - dense_started) * 1000,
                    3,
                )
        legs = (
            ("dense", 0.55, dense),
            ("passage-lexical", 0.30, lexical),
            ("sparse-exact", 0.15, sparse),
        )
        results = collapse_document_candidates(legs, limit=limit)
        dense_returned_passages = (
            int(dense[0]["dense_passage_count"])
            if dense
            else 0
        )
        dense_unique_documents = (
            int(dense[0]["dense_document_count"])
            if dense
            else 0
        )
        response = {
            "results": results,
            "diagnostics": {
                "engine": "lossless-passages-v1",
                "policy_fingerprint": self.policy_fingerprint,
                "dense_candidates": len(dense),
                "dense_latency_ms": dense_latency_ms,
                "dense_execution": (
                    EXACT_DENSE_EXECUTION
                    if runtime is not None
                    else "disabled"
                ),
                "dense_requested_passages": (
                    dense_requested_passages
                    if runtime is not None
                    else 0
                ),
                "dense_returned_passages": dense_returned_passages,
                "dense_unique_documents": dense_unique_documents,
                "dense_depth_complete": (
                    runtime is not None
                    and dense_returned_passages == dense_requested_passages
                ),
                "passage_lexical_candidates": len(lexical),
                "sparse_candidates": len(sparse),
                "dense_status": dense_status,
                "passage_lexical_status": lexical_status,
                "sparse_status": sparse_status,
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
        response = self._search_groups(
            (queries,),
            lexical_groups=(lexical_queries,),
            since=since,
            until=until,
            limit=limit,
            engine="lossless-passages-v1-bundle",
            preserve_arms=False,
        )
        response["diagnostics"].pop("need_count")
        return response

    def search_needs(
        self,
        query_groups: tuple[tuple[str, ...], ...],
        *,
        lexical_groups: tuple[tuple[str, ...], ...],
        since: str | None,
        until: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Retrieve one fair candidate pool for agent-authored evidence needs."""

        if (
            not 1 <= len(query_groups) <= 5
            or len(query_groups) != len(lexical_groups)
            or any(
                not 1 <= len(group) <= 2
                for group in (*query_groups, *lexical_groups)
            )
            or any(
                len(queries) != len(lexical_queries)
                for queries, lexical_queries in zip(
                    query_groups,
                    lexical_groups,
                    strict=True,
                )
            )
            or any(
                not isinstance(query, str)
                or not query.strip()
                or len(query) > 2048
                for groups in (query_groups, lexical_groups)
                for group in groups
                for query in group
            )
            or isinstance(limit, bool)
            or not len(query_groups) <= limit <= 100
        ):
            raise ValueError("invalid evidence need ledger")
        return self._search_groups(
            query_groups,
            lexical_groups=lexical_groups,
            since=since,
            until=until,
            limit=limit,
            engine="lossless-passages-v1-need-ledger",
            preserve_arms=True,
        )

    def _search_groups(
        self,
        query_groups: tuple[tuple[str, ...], ...],
        *,
        lexical_groups: tuple[tuple[str, ...], ...],
        since: str | None,
        until: str | None,
        limit: int,
        engine: str,
        preserve_arms: bool,
    ) -> dict[str, Any]:
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

        pairs = tuple(
            pair
            for queries, lexical_queries in zip(
                query_groups,
                lexical_groups,
                strict=True,
            )
            for pair in zip(queries, lexical_queries, strict=True)
        )
        with ThreadPoolExecutor(
            max_workers=min(MAX_BUNDLE_SEARCH_WORKERS, len(pairs)),
            thread_name_prefix="recall-passage-query",
        ) as executor:
            responses = list(executor.map(search, pairs))
        arm_names = ("dense", "passage-lexical", "sparse-exact")

        grouped_responses = []
        offset = 0
        for queries in query_groups:
            grouped_responses.append(responses[offset:offset + len(queries)])
            offset += len(queries)

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

        def query_ranking(response: dict[str, Any]) -> list[dict[str, Any]]:
            if not preserve_arms or limit < len(arm_names) + 1:
                return response["results"]
            return fuse_need_rankings(
                (
                    response["results"],
                    *(response["arms"][arm] for arm in arm_names),
                ),
                limit=limit,
            )

        def group_ranking(group: list[dict[str, Any]]) -> list[dict[str, Any]]:
            rankings = tuple(query_ranking(response) for response in group)
            return (
                fuse_need_rankings(rankings, limit=limit)
                if preserve_arms
                else fuse_document_rankings(rankings, limit=limit)
            )

        return {
            "results": fuse_need_rankings(
                tuple(
                    group_ranking(group)
                    for group in grouped_responses
                ),
                limit=limit,
            ),
            "arms": {
                arm: fuse_need_rankings(
                    tuple(
                        fuse_document_rankings(
                            tuple(response["arms"][arm] for response in group),
                            limit=limit,
                        )
                        for group in grouped_responses
                    ),
                    limit=limit,
                )
                for arm in arm_names
            },
            "diagnostics": {
                "engine": engine,
                "need_count": len(query_groups),
                "query_count": len(pairs),
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
                            WHERE represented.tenant_id=%s
                              AND represented.source_id=ANY(%s)
                              AND represented.representation_fingerprint=%s
                              AND represented.{vector_column} IS NOT NULL
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
