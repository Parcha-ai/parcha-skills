"""Find receipts inside one source session using the active turbopuffer index."""
from __future__ import annotations

import logging
import time
from typing import Any

from .turbopuffer_plane import TEXT_ATTRIBUTE
from .turbopuffer_retrieval import TurbopufferHintRetrieval, _value, scope_filters


logger = logging.getLogger(__name__)


def parent_scoped_receipts(
    store: Any,
    *,
    tenant_id: str,
    source_id: str,
    parent_id: str,
    terms: list[str],
    policy_fingerprint: str,
    since: str | None,
    until: str | None,
    limit: int,
    deadline_at: float,
) -> tuple[str, ...]:
    """BM25 ranks within the parent, then PostgreSQL verifies current receipts.

    The caller has already checked its source grants. Parent IDs are not
    filterable in the existing namespace; the catalog resolves their indexed
    logical-document ID before ranking. No body or FTS read is needed here.
    """
    if time.monotonic() >= deadline_at:
        return ()
    try:
        with store.connect() as connection:
            document = store._execute_bounded(
                connection,
                """SELECT logical_document_id,revision,manifest_content_sha256
                     FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (tenant_id, source_id, parent_id), deadline_at,
            ).fetchone()
        if document is None or time.monotonic() >= deadline_at:
            return ()
        retrieval = TurbopufferHintRetrieval(
            store, tenant_id=tenant_id, sources=[source_id],
            policy_fingerprint=policy_fingerprint,
        )
        filters = scope_filters(
            sources=[source_id], policy_fingerprint=policy_fingerprint,
            since=since, until=until, actor_ids=None, actor_relations=None,
        )
        filters.append(("logical_document_id", "Eq", document["logical_document_id"]))
        raw, status = retrieval._query(
            rank_by=(TEXT_ATTRIBUTE, "BM25", " ".join(terms)[:8000]),
            filters=filters, limit=min(400, max(50, limit * 4)),
            deadline_at=deadline_at,
        )
        if status != "ok" or time.monotonic() >= deadline_at:
            return ()
        candidates = {}
        for row in raw:
            passage_id = _value(row, "id")
            if (
                isinstance(passage_id, str)
                and _value(row, "source_id") == source_id
                and _value(row, "logical_document_id") == document["logical_document_id"]
                and _value(row, "native_parent_id") == parent_id
                and _value(row, "policy_fingerprint") == policy_fingerprint
                and _value(row, "revision") == document["revision"]
                and _value(row, "manifest_content_sha256") == document["manifest_content_sha256"]
                and isinstance(_value(row, "text_sha256"), str)
            ):
                candidates.setdefault(passage_id, row)
        if not candidates:
            return ()
        with store.connect() as connection:
            rows = store._execute_bounded(
                connection,
                """SELECT passage.passage_id,passage.text_sha256,
                          passage.revision,evidence.manifest_content_sha256,
                          chunk.receipt
                     FROM canonical_passages passage
                     JOIN canonical_evidence_documents evidence
                       USING(tenant_id,source_id,logical_document_id,revision)
                     JOIN LATERAL unnest(passage.receipts) WITH ORDINALITY
                       AS wanted(receipt,ordinal) ON true
                     JOIN canonical_chunks chunk
                       ON chunk.tenant_id=passage.tenant_id
                      AND chunk.source_id=passage.source_id
                      AND chunk.receipt=wanted.receipt
                     JOIN canonical_documents document
                       ON document.tenant_id=chunk.tenant_id
                      AND document.source_id=chunk.source_id
                      AND document.document_id=chunk.document_id
                     JOIN canonical_events event
                       ON event.tenant_id=document.tenant_id
                      AND event.source_id=document.source_id
                      AND event.event_id=document.event_id
                    WHERE passage.tenant_id=%s AND passage.source_id=%s
                      AND passage.logical_document_id=%s
                      AND passage.policy_fingerprint=%s
                      AND passage.passage_id=ANY(%s)
                      AND evidence.native_parent_id=%s
                      AND COALESCE(event.native_parent_id,event.native_id)=%s
                      AND chunk.deleted_at IS NULL
                      AND document.is_current AND document.deleted_at IS NULL
                      AND NOT event.is_tombstone
                      AND NOT EXISTS (
                          SELECT 1 FROM canonical_events later
                           WHERE later.tenant_id=document.tenant_id
                             AND later.source_id=document.source_id
                             AND later.native_id=document.native_id
                             AND later.revision>document.revision AND later.is_tombstone
                      )
                      AND (%s::timestamptz IS NULL OR event.occurred_at>=%s)
                      AND (%s::timestamptz IS NULL OR event.occurred_at<=%s)
                    ORDER BY array_position(%s::text[],passage.passage_id),wanted.ordinal
                    LIMIT 5000""",
                (tenant_id, source_id, document["logical_document_id"],
                 policy_fingerprint, list(candidates), parent_id, parent_id,
                 since, since, until, until, list(candidates)), deadline_at,
            ).fetchall()
        if time.monotonic() >= deadline_at:
            return ()
        verified: dict[str, list[str]] = {}
        for row in rows:
            candidate = candidates.get(row["passage_id"])
            if candidate is not None and all(
                row[key] == _value(candidate, key)
                for key in ("revision", "text_sha256", "manifest_content_sha256")
            ):
                verified.setdefault(row["passage_id"], []).append(row["receipt"])
        return tuple(dict.fromkeys(
            receipt for passage_id in candidates for receipt in verified.get(passage_id, ())
        ))[:limit]
    except Exception as error:
        logger.warning("parent_scoped_lookup_failed error_type=%s", type(error).__name__)
        # An optional session-deepening lookup must not fall back to the retired
        # text index or emit unverified receipts when either backend fails.
        return ()
