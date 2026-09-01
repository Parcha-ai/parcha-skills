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
    oversized_pointer = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'contract',{alias}.canonical_redacted #> '{{content,contract}}',"
        f"'schema_version',{alias}.canonical_redacted #> '{{content,schema_version}}',"
        f"'full_record_available',{alias}.canonical_redacted #> "
        "'{content,full_record_available}',"
        f"'full_content_sha256',{alias}.canonical_redacted #> "
        "'{content,full_content_sha256}',"
        f"'full_size_bytes',{alias}.canonical_redacted #> "
        "'{content,full_size_bytes}',"
        f"'archive_encoding',{alias}.canonical_redacted #> "
        "'{content,archive_encoding}'))"
    )
    content = (
        f"CASE WHEN {alias}.canonical_redacted #>> '{{content,contract}}'="
        "'recall.oversized-projection.v1' "
        f"THEN {oversized_pointer} ELSE {content_metadata} END"
    )
    provenance = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'connector_id',{alias}.canonical_redacted #> '{{provenance,connector_id}}',"
        f"'connector_schema_version',{alias}.canonical_redacted #> "
        "'{provenance,connector_schema_version}',"
        f"'collector_version',{alias}.canonical_redacted #> "
        "'{provenance,collector_version}',"
        f"'privacy_policy_version',{alias}.canonical_redacted #> "
        "'{provenance,privacy_policy_version}',"
        f"'harness',{alias}.canonical_redacted #> '{{provenance,harness}}',"
        f"'cwd',{alias}.canonical_redacted #> '{{provenance,cwd}}',"
        f"'branch',{alias}.canonical_redacted #> '{{provenance,branch}}',"
        f"'slot',{alias}.canonical_redacted #> '{{provenance,slot}}',"
        f"'byte_start',{alias}.canonical_redacted #> '{{provenance,byte_start}}',"
        f"'byte_end',{alias}.canonical_redacted #> '{{provenance,byte_end}}'))"
    )
    return (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted->'role',"
        f"'type',{alias}.canonical_redacted->'type',"
        f"'provenance',{provenance},"
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
    """Remove duplicate bodies only after searchable chunks and S3 authority exist.

    The canonical chunk plane remains the single database-resident text copy. The
    raw and logical evidence objects remain the immutable full-document authority.
    Each batch fails closed for a document unless it has at least one live chunk.
    We deliberately do not reread and concatenate the retained chunk corpus here:
    object storage, not a second SQL body copy, is the recovery authority.
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
                          WHERE document.tenant_id=%s
                            AND document.body_location='inline'
                            AND document.deleted_at IS NULL
                            AND artifact.storage_backend='s3'
                            AND artifact.state='live'
                            AND EXISTS (
                                SELECT 1
                                  FROM canonical_evidence_documents evidence
                                 WHERE evidence.tenant_id=event.tenant_id
                                   AND evidence.source_id=event.source_id
                                   AND evidence.native_parent_id=COALESCE(
                                       event.native_parent_id,event.native_id
                                   )
                                   AND evidence.manifest_storage_backend='s3'
                            )
                            AND EXISTS (
                                SELECT 1
                                  FROM canonical_chunks chunk
                                 WHERE chunk.tenant_id=document.tenant_id
                                   AND chunk.source_id=document.source_id
                                   AND chunk.document_id=document.document_id
                                   AND chunk.deleted_at IS NULL
                            )
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
                     ), updated_documents AS (
                         UPDATE canonical_documents document
                            SET text_redacted='',body_location='chunks'
                           FROM candidates candidate
                          WHERE document.tenant_id=candidate.tenant_id
                            AND document.source_id=candidate.source_id
                            AND document.document_id=candidate.document_id
                      RETURNING candidate.event_id,
                                candidate.tenant_id,candidate.source_id,
                                candidate.document_bytes,candidate.event_bytes
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
    with store.connect() as connection:
        oversized_events = int(
            connection.execute(
                f"""WITH candidates AS MATERIALIZED (
                         SELECT event.tenant_id,event.source_id,event.event_id
                           FROM canonical_events AS event
                           JOIN raw_artifacts AS artifact
                             ON artifact.tenant_id=event.tenant_id
                            AND artifact.source_id=event.source_id
                            AND artifact.artifact_id=event.artifact_id
                          WHERE event.tenant_id=%s
                            AND artifact.storage_backend='s3'
                            AND artifact.state='live'
                            AND event.canonical_redacted #>>
                                '{{content,contract}}'=
                                'recall.oversized-projection.v1'
                            AND event.canonical_redacted #>>
                                '{{content,full_record_available}}'='true'
                            AND (event.canonical_redacted->'content') ?| ARRAY[
                                'head','tail','archive_size_bytes'
                            ]
                          ORDER BY event.source_id,event.event_id
                          LIMIT %s
                          FOR UPDATE OF event SKIP LOCKED
                     ), updated AS (
                         UPDATE canonical_events AS event
                            SET canonical_redacted={compact_event},
                                body_location='raw'
                           FROM candidates AS candidate
                          WHERE event.tenant_id=candidate.tenant_id
                            AND event.source_id=candidate.source_id
                            AND event.event_id=candidate.event_id
                      RETURNING 1
                     )
                     SELECT count(*)::integer AS events FROM updated""",
                (tenant_id, batch_size * max_batches),
            ).fetchone()["events"]
        )
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
        "oversized_events": oversized_events,
        "refused": refused,
        "document_bytes_removed": document_bytes,
        "event_bytes_replaced": event_bytes,
    }
