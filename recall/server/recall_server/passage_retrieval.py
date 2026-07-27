"""Hybrid hints collapsed to authorized logical-document boundaries."""

from __future__ import annotations

import json
import time
import urllib.error
from typing import Any

from .db import SearchDeadlineExceeded, bounded_search_text


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
        ranges = sorted(
            value.pop("_ranges").values(),
            key=lambda item: (item["score"], item["kind"]),
            reverse=True,
        )[:3]
        reasons = sorted(value.pop("_reasons"))
        score = value.pop("_score")
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
    ) -> dict[str, Any]:
        candidate_limit = min(400, max(80, limit * 20))
        deadline = time.monotonic() + self.store.search_deadline_ms / 1000
        with self.store.connect() as connection:
            try:
                lexical = self.store._execute_bounded(
                    connection,
                    """SELECT passage.source_id,passage.logical_document_id,
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
                    deadline,
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
                    deadline,
                ).fetchall()
                sparse_status = "ok"
            except SearchDeadlineExceeded:
                sparse = []
                sparse_status = "deadline-exceeded"

        dense: list[dict[str, Any]] = []
        runtime = self.store.semantic_runtime
        dense_status = "disabled" if runtime is None else "ok"
        if runtime is not None:
            try:
                bounded = getattr(runtime, "embed_query_bounded", None)
                vector = (
                    bounded(query)
                    if bounded is not None
                    else runtime.embed_query(query)
                )
                with self.store.connect() as connection:
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
                                         <=> %s::halfvec
                                LIMIT %s
                           )
                           SELECT passage.source_id,
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
                                  1-nearest.distance AS score
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
                                   tenant_id,source_id,logical_document_id,
                                   revision
                               )
                            WHERE projected.policy_fingerprint=%s
                              AND (%s::timestamptz IS NULL
                                   OR passage.last_occurred_at>=%s)
                              AND (%s::timestamptz IS NULL
                                   OR passage.first_occurred_at<=%s)
                            ORDER BY nearest.distance,
                                     passage.last_occurred_at DESC,
                                     passage.passage_id
                            LIMIT %s""",
                        (
                            vector,
                            self.tenant_id,
                            self.sources,
                            runtime.passage_fingerprint,
                            vector,
                            candidate_limit * 5,
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
        results = collapse_document_candidates(
            (
                ("dense", 0.55, dense),
                ("passage-lexical", 0.30, lexical),
                ("sparse-exact", 0.15, sparse),
            ),
            limit=limit,
        )
        return {
            "results": results,
            "diagnostics": {
                "engine": "lossless-passages-v1",
                "policy_fingerprint": self.policy_fingerprint,
                "dense_candidates": len(dense),
                "passage_lexical_candidates": len(lexical),
                "sparse_candidates": len(sparse),
                "dense_status": dense_status,
                "passage_lexical_status": lexical_status,
                "sparse_status": sparse_status,
            },
        }
