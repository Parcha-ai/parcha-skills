"""Thin redundant canonical bodies after immutable object projection succeeds."""

from __future__ import annotations

from typing import Any

from .db import BrainStore


def _compact_event_expression(alias: str = "event") -> str:
    """Retain only routing metadata used after the full body moves to raw storage."""

    def structural(path: str) -> str:
        return f"jsonb_strip_nulls(jsonb_build_object('role',{alias}.canonical_redacted #> '{{{path},role}}','type',{alias}.canonical_redacted #> '{{{path},type}}'))"

    content_metadata = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted #> '{{content,role}}',"
        f"'type',{alias}.canonical_redacted #> '{{content,type}}',"
        f"'message',{structural('content,message')},"
        f"'payload',{structural('content,payload')}))"
    )
    content = (
        f"CASE WHEN {alias}.canonical_redacted #>> '{{content,contract}}'="
        "'recall.oversized-projection.v1' "
        f"THEN {alias}.canonical_redacted->'content' ELSE {content_metadata} END"
    )
    return (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted->'role',"
        f"'type',{alias}.canonical_redacted->'type',"
        f"'provenance',{alias}.canonical_redacted->'provenance',"
        f"'content',{content},"
        f"'message',{structural('message')},"
        f"'payload',{structural('payload')}))"
    )


def thin_canonical_bodies(
    store: BrainStore,
    *,
    tenant_id: str,
    batch_size: int = 1_000,
    max_batches: int = 1,
) -> dict[str, Any]:
    """Remove duplicate bodies only after lossless chunks and S3 authority agree.

    The canonical chunk plane remains the single database-resident text copy. The
    raw and logical evidence objects remain the immutable full-document authority.
    Each batch fails closed for a document unless its ordered chunks concatenate
    byte-for-byte to the inline document body.
    """

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or not 1 <= batch_size <= 10_000
        or isinstance(max_batches, bool)
        or not isinstance(max_batches, int)
        or not 1 <= max_batches <= 10_000
    ):
        raise ValueError("canonical body thinning budget is invalid")

    documents = events = document_bytes = event_bytes = batches = candidates = 0
    compact_event = _compact_event_expression()
    for _ in range(max_batches):
        with store.connect() as connection:
            row = connection.execute(
                f"""WITH candidates AS MATERIALIZED (
                         SELECT document.tenant_id,document.source_id,
                                document.document_id,document.event_id,
                                document.text_redacted,
                                octet_length(document.text_redacted)::bigint
                                    AS document_bytes,
                                octet_length(event.canonical_redacted::text)::bigint
                                    AS event_bytes
                           FROM canonical_documents document
                           JOIN canonical_events event
                             USING(tenant_id,source_id,event_id)
                           JOIN raw_artifacts artifact
                             ON artifact.tenant_id=event.tenant_id
                            AND artifact.source_id=event.source_id
                            AND artifact.artifact_id=event.artifact_id
                           JOIN canonical_evidence_documents evidence
                             ON evidence.tenant_id=event.tenant_id
                            AND evidence.source_id=event.source_id
                            AND evidence.native_parent_id=COALESCE(
                                event.native_parent_id,event.native_id
                            )
                          WHERE document.tenant_id=%s
                            AND document.body_location='inline'
                            AND document.deleted_at IS NULL
                            AND artifact.storage_backend='s3'
                            AND artifact.state='live'
                            AND evidence.manifest_storage_backend='s3'
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM canonical_evidence_document_queue queued
                                 WHERE queued.tenant_id=event.tenant_id
                                   AND queued.source_id=event.source_id
                                   AND queued.native_parent_id=COALESCE(
                                       event.native_parent_id,event.native_id
                                   )
                            )
                          ORDER BY document.source_id,document.document_id
                          LIMIT %s
                          FOR UPDATE OF document,event SKIP LOCKED
                     ), verified AS MATERIALIZED (
                         SELECT candidate.*
                           FROM candidates candidate
                           JOIN LATERAL (
                                SELECT string_agg(
                                           chunk.text_redacted,''
                                           ORDER BY chunk.ordinal
                                       ) AS full_text,
                                       count(*)::integer AS chunk_count
                                  FROM canonical_chunks chunk
                                 WHERE chunk.tenant_id=candidate.tenant_id
                                   AND chunk.source_id=candidate.source_id
                                   AND chunk.document_id=candidate.document_id
                                   AND chunk.deleted_at IS NULL
                           ) chunks ON chunks.chunk_count>0
                          WHERE chunks.full_text=candidate.text_redacted
                     ), updated_documents AS (
                         UPDATE canonical_documents document
                            SET text_redacted='',body_location='chunks'
                           FROM verified
                          WHERE document.tenant_id=verified.tenant_id
                            AND document.source_id=verified.source_id
                            AND document.document_id=verified.document_id
                      RETURNING verified.event_id,
                                verified.tenant_id,verified.source_id,
                                verified.document_bytes,verified.event_bytes
                     ), updated_events AS (
                         UPDATE canonical_events event
                            SET canonical_redacted={compact_event},
                                body_location='raw'
                           FROM updated_documents updated
                          WHERE event.tenant_id=updated.tenant_id
                            AND event.source_id=updated.source_id
                            AND event.event_id=updated.event_id
                      RETURNING updated.document_bytes,updated.event_bytes
                     )
                     SELECT (SELECT count(*) FROM candidates)::integer
                                AS candidates,
                            count(*)::integer AS documents,
                            count(*)::integer AS events,
                            coalesce(sum(document_bytes),0)::bigint
                                AS document_bytes,
                            coalesce(sum(event_bytes),0)::bigint AS event_bytes
                       FROM updated_events""",
                (tenant_id, batch_size),
            ).fetchone()
        current_candidates = int(row["candidates"])
        current = int(row["documents"])
        candidates += current_candidates
        documents += current
        events += int(row["events"])
        document_bytes += int(row["document_bytes"])
        event_bytes += int(row["event_bytes"])
        batches += 1
        if current_candidates < batch_size or current < current_candidates:
            break
    refused = candidates - documents
    return {
        "status": (
            "refused"
            if refused
            else "complete"
            if candidates < batch_size * max_batches
            else "pending"
        ),
        "tenant_id": tenant_id,
        "batches": batches,
        "documents": documents,
        "events": events,
        "refused": refused,
        "document_bytes_removed": document_bytes,
        "event_bytes_replaced": event_bytes,
    }
