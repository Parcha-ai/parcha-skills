from __future__ import annotations

import gzip
import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
    LogicalEvidenceUpload,
    ROLE_RE,
    logical_document_id,
)

OVERSIZED_MEDIA_TYPE = "application/vnd.recall.oversized-record+gzip"
MAX_RESTORED_RECORD_BYTES = 256 * 1024 * 1024
TEXT_SEGMENT_BYTES = 14 * 1024 * 1024


@dataclass(frozen=True)
class LogicalGroupCandidate:
    tenant_id: str
    source_id: str
    native_parent_id: str
    source_updated_at: datetime
    generation: int
    revision: int


def mark_logical_evidence_dirty(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_ids: list[str],
    reason: str,
) -> int:
    """Queue affected logical documents in the same transaction as canonical state."""

    if reason not in {"ingest", "forget"} or not native_ids:
        return 0
    result = connection.execute(
        """INSERT INTO canonical_evidence_document_queue(
               tenant_id,source_id,native_parent_id,generation,reason,changed_at
           )
           SELECT affected.tenant_id,affected.source_id,
                  affected.native_parent_id,1,%s,clock_timestamp()
             FROM (
                   SELECT DISTINCT event.tenant_id,event.source_id,
                          COALESCE(
                              event.native_parent_id,event.native_id
                          ) AS native_parent_id
                     FROM canonical_events event
                    WHERE event.tenant_id=%s AND event.source_id=%s
                      AND event.native_id=ANY(%s)
             ) affected
           ON CONFLICT(tenant_id,source_id,native_parent_id)
           DO UPDATE SET
               generation=canonical_evidence_document_queue.generation+1,
               reason=excluded.reason,
               changed_at=clock_timestamp()""",
        (reason, tenant_id, source_id, native_ids),
    )
    return max(0, result.rowcount)


def _timestamp(value: datetime | str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise LogicalEvidenceError("logical_evidence_state_invalid")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        return value
    raise LogicalEvidenceError("logical_evidence_state_invalid")


def _roles(value: Any) -> tuple[str, ...]:
    """Extract only explicit structural roles; never classify source prose."""

    found: set[str] = set()
    pending: list[tuple[Any, int]] = [(value, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 8:
            continue
        if isinstance(item, dict):
            for key, child in item.items():
                if (
                    key == "role"
                    and isinstance(child, str)
                    and ROLE_RE.fullmatch(child)
                ):
                    found.add(child)
                elif (
                    key == "type"
                    and child in {"user", "assistant", "system", "developer", "tool"}
                ):
                    found.add(child)
                elif isinstance(child, (dict, list)):
                    pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend(
                (child, depth + 1)
                for child in item
                if isinstance(child, (dict, list))
            )
    return tuple(sorted(found))


class CanonicalLogicalEvidenceProjector:
    """Project current canonical records into exact source-level evidence documents."""

    def __init__(
        self,
        store: Any,
        projection: LogicalEvidenceProjectionStore,
        *,
        bound_tenant_id: str | None = None,
        raw_archive: Any | None = None,
    ) -> None:
        if bound_tenant_id is not None and (
            not isinstance(bound_tenant_id, str)
            or not bound_tenant_id.strip()
            or len(bound_tenant_id) > 255
        ):
            raise LogicalEvidenceError("logical_evidence_tenant_invalid")
        self.store = store
        self.projection = projection
        self.bound_tenant_id = bound_tenant_id
        self.raw_archive = raw_archive

    def _tenant(self, tenant_id: str | None) -> str | None:
        if self.bound_tenant_id is None:
            return tenant_id
        if tenant_id is not None and tenant_id != self.bound_tenant_id:
            raise LogicalEvidenceError("logical_evidence_tenant_not_configured")
        return self.bound_tenant_id

    @staticmethod
    def _reference(
        row: dict[str, Any],
        *,
        prefix: str = "",
    ) -> dict[str, Any]:
        def field(name: str) -> Any:
            return row[prefix + name]

        created_at = field("created_at")
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": row["tenant_id"],
            "source_id": row["source_id"],
            "artifact_id": field("artifact_id"),
            "storage_backend": field("storage_backend"),
            "object_key": field("object_key"),
            "content_sha256": field("content_sha256"),
            "size_bytes": field("size_bytes"),
            "media_type": field("media_type"),
            "encryption": field("encryption"),
            "version_id": field("version_id"),
            "created_at": _timestamp(created_at),
        }

    @staticmethod
    def _text_segments(text: str) -> tuple[str, ...]:
        encoded = text.encode()
        if not encoded:
            return ("",)
        segments: list[str] = []
        offset = 0
        while offset < len(encoded):
            end = min(offset + TEXT_SEGMENT_BYTES, len(encoded))
            while end < len(encoded) and encoded[end] & 0xC0 == 0x80:
                end -= 1
            if end == offset:
                raise LogicalEvidenceError("logical_evidence_state_invalid")
            segments.append(encoded[offset:end].decode())
            offset = end
        return tuple(segments)

    def _restored_record_text(self, row: dict[str, Any], fallback: str) -> str:
        if row["raw_media_type"] != OVERSIZED_MEDIA_TYPE:
            return fallback
        content = row["canonical_redacted"].get("content")
        if (
            self.raw_archive is None
            or not isinstance(content, dict)
            or content.get("contract") != "recall.oversized-projection.v1"
            or content.get("full_record_available") is not True
            or content.get("archive_encoding") != "gzip"
            or isinstance(content.get("full_size_bytes"), bool)
            or not isinstance(content.get("full_size_bytes"), int)
            or not 1 <= content["full_size_bytes"] <= MAX_RESTORED_RECORD_BYTES
            or not isinstance(content.get("full_content_sha256"), str)
        ):
            raise LogicalEvidenceError(
                "logical_evidence_full_record_unavailable"
            )
        try:
            compressed = self.raw_archive.read_raw(
                self._reference(row, prefix="raw_")
            )
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
                payload = source.read(MAX_RESTORED_RECORD_BYTES + 1)
            if (
                len(payload) != content["full_size_bytes"]
                or len(payload) > MAX_RESTORED_RECORD_BYTES
                or hashlib.sha256(payload).hexdigest()
                != content["full_content_sha256"]
            ):
                raise LogicalEvidenceError(
                    "logical_evidence_full_record_corrupt"
                )
            text = payload.decode()
            if not isinstance(json.loads(text), dict):
                raise LogicalEvidenceError(
                    "logical_evidence_full_record_corrupt"
                )
            return text
        except LogicalEvidenceError:
            raise
        except Exception:
            raise LogicalEvidenceError(
                "logical_evidence_full_record_unavailable"
            ) from None

    def _event_records(
        self,
        row: dict[str, Any],
        *,
        chunks: list[str],
        receipts: list[str],
        start_ordinal: int,
    ):
        if (
            not receipts
            or len(receipts) != len(set(receipts))
            or row["chunk_ordinal"] != len(receipts) - 1
        ):
            raise LogicalEvidenceError("logical_evidence_state_invalid")
        text = self._restored_record_text(row, "".join(chunks))
        segments = self._text_segments(text)
        roles = _roles(row["canonical_redacted"])
        for segment_ordinal, segment in enumerate(segments):
            yield LogicalEvidenceRecord(
                ordinal=start_ordinal + segment_ordinal,
                event_native_id=row["native_id"],
                event_kind=row["kind"],
                occurred_at=_timestamp(row["occurred_at"]),
                roles=roles,
                receipts=tuple(receipts) if segment_ordinal == 0 else (),
                segment_ordinal=segment_ordinal,
                segment_count=len(segments),
                text=segment,
            )

    def _record_stream(self, cursor: Any):
        current_key: tuple[str, str] | None = None
        current_row: dict[str, Any] | None = None
        chunks: list[str] = []
        receipts: list[str] = []
        next_ordinal = 0
        expected_chunk = 0
        for row in cursor:
            key = (row["event_id"], row["document_id"])
            if current_key is not None and key != current_key:
                assert current_row is not None
                records = tuple(
                    self._event_records(
                        current_row,
                        chunks=chunks,
                        receipts=receipts,
                        start_ordinal=next_ordinal,
                    )
                )
                yield from records
                next_ordinal += len(records)
                chunks = []
                receipts = []
                expected_chunk = 0
            if key != current_key:
                current_key = key
                current_row = row
            if row["chunk_ordinal"] != expected_chunk:
                raise LogicalEvidenceError("logical_evidence_state_invalid")
            chunks.append(row["text_redacted"])
            receipts.append(row["receipt"])
            expected_chunk += 1
            current_row = row
        if current_row is not None:
            yield from self._event_records(
                current_row,
                chunks=chunks,
                receipts=receipts,
                start_ordinal=next_ordinal,
            )

    def _pending(
        self,
        *,
        tenant_id: str | None,
        limit: int,
    ) -> list[LogicalGroupCandidate]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT queue.tenant_id,queue.source_id,
                          queue.native_parent_id,
                          queue.changed_at AS source_updated_at,
                          queue.generation,
                          COALESCE(evidence.revision,0)+1 AS revision
                     FROM canonical_evidence_document_queue queue
                     LEFT JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=queue.tenant_id
                      AND evidence.source_id=queue.source_id
                      AND evidence.native_parent_id=queue.native_parent_id
                    WHERE (%s::text IS NULL OR queue.tenant_id=%s)
                    ORDER BY queue.changed_at,queue.tenant_id,
                             queue.source_id,queue.native_parent_id
                    LIMIT %s""",
                (tenant_id, tenant_id, limit),
            ).fetchall()
        return [
            LogicalGroupCandidate(
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                native_parent_id=row["native_parent_id"],
                source_updated_at=row["source_updated_at"],
                generation=int(row["generation"]),
                revision=int(row["revision"]),
            )
            for row in rows
        ]

    def seed_backfill(self, *, tenant_id: str | None = None) -> int:
        """Queue every current logical document absent from the v2 projection once."""

        tenant_id = self._tenant(tenant_id)
        with self.store.connect() as connection:
            result = connection.execute(
                """INSERT INTO canonical_evidence_document_queue(
                       tenant_id,source_id,native_parent_id,
                       generation,reason,changed_at
                   )
                   SELECT missing.tenant_id,missing.source_id,
                          missing.native_parent_id,
                          1,'backfill',clock_timestamp()
                     FROM (
                           SELECT DISTINCT event.tenant_id,event.source_id,
                                  COALESCE(
                                      event.native_parent_id,event.native_id
                                  ) AS native_parent_id
                             FROM canonical_documents document
                             JOIN canonical_events event
                               USING(tenant_id,source_id,event_id)
                            WHERE document.is_current
                              AND document.deleted_at IS NULL
                              AND (
                                  %s::text IS NULL
                                  OR document.tenant_id=%s
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                    FROM canonical_evidence_documents evidence
                                   WHERE evidence.tenant_id=event.tenant_id
                                     AND evidence.source_id=event.source_id
                                     AND evidence.native_parent_id=COALESCE(
                                         event.native_parent_id,event.native_id
                                     )
                              )
                     ) missing
                   ON CONFLICT DO NOTHING""",
                (tenant_id, tenant_id),
            )
        return max(0, result.rowcount)

    def _prepare_and_upload(
        self,
        candidate: LogicalGroupCandidate,
    ) -> LogicalEvidenceUpload:
        with self.store.connect() as connection:
            with connection.cursor(
                name="logical_evidence_stream",
            ) as cursor:
                cursor.itersize = 2_000
                cursor.execute(
                    """SELECT event.tenant_id,event.source_id,
                              event.event_id,event.native_id,event.kind,
                              event.occurred_at,
                              event.canonical_redacted,
                              document.document_id,
                              chunk.ordinal AS chunk_ordinal,
                              chunk.receipt,chunk.text_redacted,
                              artifact.artifact_id AS raw_artifact_id,
                              artifact.storage_backend AS raw_storage_backend,
                              artifact.object_key AS raw_object_key,
                              artifact.content_sha256 AS raw_content_sha256,
                              artifact.size_bytes AS raw_size_bytes,
                              artifact.media_type AS raw_media_type,
                              artifact.encryption AS raw_encryption,
                              artifact.version_id AS raw_version_id,
                              artifact.created_at AS raw_created_at
                         FROM canonical_events event
                         JOIN canonical_documents document
                           ON document.tenant_id=event.tenant_id
                          AND document.source_id=event.source_id
                          AND document.event_id=event.event_id
                          AND document.is_current
                          AND document.deleted_at IS NULL
                         JOIN raw_artifacts artifact
                           ON artifact.tenant_id=event.tenant_id
                          AND artifact.source_id=event.source_id
                          AND artifact.artifact_id=event.artifact_id
                         JOIN canonical_chunks chunk
                           ON chunk.tenant_id=document.tenant_id
                          AND chunk.source_id=document.source_id
                          AND chunk.document_id=document.document_id
                          AND chunk.deleted_at IS NULL
                        WHERE event.tenant_id=%s AND event.source_id=%s
                          AND COALESCE(
                              event.native_parent_id,event.native_id
                          )=%s
                        ORDER BY
                          CASE WHEN jsonb_typeof(
                              event.canonical_redacted
                                  #> '{provenance,byte_start}'
                          )='number' THEN 0 ELSE 1 END,
                          CASE WHEN jsonb_typeof(
                              event.canonical_redacted
                                  #> '{provenance,byte_start}'
                          )='number' THEN (
                              event.canonical_redacted
                                  #>> '{provenance,byte_start}'
                          )::bigint END,
                          event.occurred_at,event.native_id,chunk.ordinal""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                )
                return self.projection.put_records(
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    native_parent_id=candidate.native_parent_id,
                    revision=candidate.revision,
                    records=self._record_stream(cursor),
                )

    def _old_references(
        self,
        connection: Any,
        candidate: LogicalGroupCandidate,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        document = connection.execute(
            """SELECT tenant_id,source_id,
                      manifest_artifact_id AS artifact_id,
                      manifest_storage_backend AS storage_backend,
                      manifest_object_key AS object_key,
                      manifest_content_sha256 AS content_sha256,
                      manifest_size_bytes AS size_bytes,
                      manifest_media_type AS media_type,
                      manifest_encryption AS encryption,
                      manifest_version_id AS version_id,
                      created_at
                 FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s
                  AND native_parent_id=%s""",
            (
                candidate.tenant_id,
                candidate.source_id,
                candidate.native_parent_id,
            ),
        ).fetchone()
        parts = connection.execute(
            """SELECT part.tenant_id,part.source_id,part.artifact_id,
                      part.storage_backend,part.object_key,
                      part.content_sha256,part.size_bytes,part.media_type,
                      part.encryption,part.version_id,part.created_at
                 FROM canonical_evidence_document_parts part
                 JOIN canonical_evidence_documents document
                   USING(tenant_id,source_id,logical_document_id,revision)
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND document.native_parent_id=%s
                ORDER BY part.part_ordinal""",
            (
                candidate.tenant_id,
                candidate.source_id,
                candidate.native_parent_id,
            ),
        ).fetchall()
        return (
            self._reference(document) if document is not None else None,
            [self._reference(row) for row in parts],
        )

    @staticmethod
    def _enqueue_cleanup(
        connection: Any,
        references: tuple[dict[str, Any], ...],
    ) -> int:
        unique = {
            (
                reference["tenant_id"],
                reference["source_id"],
                reference["artifact_id"],
            ): reference
            for reference in references
        }
        if not unique:
            return 0
        with connection.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO canonical_evidence_cleanup_queue(
                       tenant_id,source_id,artifact_id,storage_backend,
                       object_key,content_sha256,size_bytes,media_type,
                       encryption,version_id,created_at
                   ) VALUES (
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                   )
                   ON CONFLICT(tenant_id,source_id,artifact_id) DO NOTHING""",
                [
                    (
                        reference["tenant_id"],
                        reference["source_id"],
                        reference["artifact_id"],
                        reference["storage_backend"],
                        reference["object_key"],
                        reference["content_sha256"],
                        reference["size_bytes"],
                        reference["media_type"],
                        reference["encryption"],
                        reference["version_id"],
                        reference["created_at"],
                    )
                    for reference in unique.values()
                ],
            )
        return len(unique)

    def _schedule_cleanup(
        self,
        references: tuple[dict[str, Any], ...],
    ) -> int:
        if not references:
            return 0
        with self.store.connect() as connection:
            with connection.transaction():
                return self._enqueue_cleanup(connection, references)

    def drain_cleanup(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, int | str]:
        if not 1 <= limit <= 5_000:
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        tenant_id = self._tenant(tenant_id)
        completed = deleted = failures = 0
        with self.store.connect() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """SELECT *
                         FROM canonical_evidence_cleanup_queue
                        WHERE (%s::text IS NULL OR tenant_id=%s)
                        ORDER BY queued_at,tenant_id,source_id,artifact_id
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED""",
                    (tenant_id, tenant_id, limit),
                ).fetchall()
                for row in rows:
                    reference = self._reference(row)
                    try:
                        removed = self.projection.delete_reference(reference)
                    except Exception:
                        connection.execute(
                            """UPDATE canonical_evidence_cleanup_queue
                                  SET attempts=attempts+1,
                                      last_attempt_at=clock_timestamp()
                                WHERE tenant_id=%s AND source_id=%s
                                  AND artifact_id=%s""",
                            (
                                row["tenant_id"],
                                row["source_id"],
                                row["artifact_id"],
                            ),
                        )
                        failures += 1
                        continue
                    connection.execute(
                        """DELETE FROM canonical_evidence_cleanup_queue
                            WHERE tenant_id=%s AND source_id=%s
                              AND artifact_id=%s""",
                        (
                            row["tenant_id"],
                            row["source_id"],
                            row["artifact_id"],
                        ),
                    )
                    completed += 1
                    deleted += int(removed)
        with self.store.connect() as connection:
            pending = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_evidence_cleanup_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s)""",
                (tenant_id, tenant_id),
            ).fetchone()["count"]
        return {
            "status": "complete",
            "completed": completed,
            "deleted": deleted,
            "failures": failures,
            "pending": int(pending),
        }

    def _commit(
        self,
        candidate: LogicalGroupCandidate,
        upload: LogicalEvidenceUpload,
    ) -> str:
        prepared = upload.prepared
        manifest_reference = upload.manifest_reference
        with self.store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (
                        "logical-evidence\x1f"
                        + prepared.logical_document_id,
                    ),
                )
                queued = connection.execute(
                    """SELECT generation,changed_at
                         FROM canonical_evidence_document_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s
                        FOR UPDATE""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                ).fetchone()
                current = connection.execute(
                    """SELECT revision,source_updated_at,receipt_count,
                              manifest_artifact_id
                         FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                ).fetchone()
                if (
                    queued is None
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.source_updated_at
                ):
                    if (
                        current is not None
                        and current["manifest_artifact_id"]
                        == manifest_reference["artifact_id"]
                    ):
                        return "adopted"
                    return "stale"
                old_manifest, old_parts = self._old_references(
                    connection,
                    candidate,
                )
                self._enqueue_cleanup(
                    connection,
                    tuple(
                        reference
                        for reference in (old_manifest, *old_parts)
                        if reference is not None
                    ),
                )
                connection.execute(
                    """DELETE FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_evidence_documents(
                           tenant_id,source_id,logical_document_id,
                           native_parent_id,revision,evidence_id,
                           manifest_artifact_id,manifest_storage_backend,
                           manifest_object_key,manifest_content_sha256,
                           manifest_size_bytes,manifest_media_type,
                           manifest_encryption,manifest_version_id,
                           document_content_sha256,record_count,receipt_count,
                           part_count,first_occurred_at,last_occurred_at,
                           source_updated_at,created_at
                       ) VALUES (
                           %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                           %s,%s,%s,%s,%s,%s,%s,%s
                       )""",
                    (
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.logical_document_id,
                        prepared.native_parent_id,
                        prepared.revision,
                        prepared.evidence_id,
                        manifest_reference["artifact_id"],
                        manifest_reference["storage_backend"],
                        manifest_reference["object_key"],
                        manifest_reference["content_sha256"],
                        manifest_reference["size_bytes"],
                        manifest_reference["media_type"],
                        manifest_reference["encryption"],
                        manifest_reference["version_id"],
                        prepared.document_content_sha256,
                        prepared.record_count,
                        prepared.receipt_count,
                        len(prepared.parts),
                        prepared.first_occurred_at,
                        prepared.last_occurred_at,
                        candidate.source_updated_at,
                        manifest_reference["created_at"],
                    ),
                )
                with connection.cursor() as cursor:
                    cursor.executemany(
                        """INSERT INTO canonical_evidence_document_parts(
                               tenant_id,source_id,logical_document_id,revision,
                               part_ordinal,artifact_id,storage_backend,object_key,
                               content_sha256,size_bytes,media_type,encryption,
                               version_id,first_record_ordinal,
                               last_record_ordinal,receipt_count,created_at
                           ) VALUES (
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s
                           )""",
                        [
                            (
                                prepared.tenant_id,
                                prepared.source_id,
                                prepared.logical_document_id,
                                prepared.revision,
                                part.ordinal,
                                reference["artifact_id"],
                                reference["storage_backend"],
                                reference["object_key"],
                                reference["content_sha256"],
                                reference["size_bytes"],
                                reference["media_type"],
                                reference["encryption"],
                                reference["version_id"],
                                part.first_record_ordinal,
                                part.last_record_ordinal,
                                part.receipt_count,
                                reference["created_at"],
                            )
                            for part, reference in zip(
                                prepared.parts,
                                upload.part_references,
                                strict=True,
                            )
                        ],
                    )
                deleted = connection.execute(
                    """DELETE FROM canonical_evidence_document_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s AND generation=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                        candidate.generation,
                    ),
                )
                if deleted.rowcount != 1:
                    raise LogicalEvidenceError("logical_evidence_queue_conflict")
        return "committed"

    def _commit_empty(
        self,
        candidate: LogicalGroupCandidate,
    ) -> str:
        with self.store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (
                        "logical-evidence\x1f"
                        + logical_document_id(
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.native_parent_id,
                        ),
                    ),
                )
                queued = connection.execute(
                    """SELECT generation,changed_at
                         FROM canonical_evidence_document_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s
                        FOR UPDATE""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                ).fetchone()
                if (
                    queued is None
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.source_updated_at
                ):
                    return "stale"
                old_manifest, old_parts = self._old_references(
                    connection,
                    candidate,
                )
                self._enqueue_cleanup(
                    connection,
                    tuple(
                        reference
                        for reference in (old_manifest, *old_parts)
                        if reference is not None
                    ),
                )
                connection.execute(
                    """DELETE FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                )
                deleted = connection.execute(
                    """DELETE FROM canonical_evidence_document_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s AND generation=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                        candidate.generation,
                    ),
                )
                if deleted.rowcount != 1:
                    raise LogicalEvidenceError("logical_evidence_queue_conflict")
        return "pruned"

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 25,
        max_batches: int = 10,
        upload_concurrency: int = 2,
    ) -> dict[str, int | str]:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= 250
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(upload_concurrency, bool)
            or not isinstance(upload_concurrency, int)
            or not 1 <= upload_concurrency <= 8
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        tenant_id = self._tenant(tenant_id)
        documents = records = receipts = objects = bytes_uploaded = batches = 0
        old_objects_deleted = cleanup_failures = source_races = pruned = 0
        cleanup_completed = cleanup_pending = 0
        cleanup = self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
        old_objects_deleted += int(cleanup["deleted"])
        cleanup_failures += int(cleanup["failures"])
        cleanup_completed += int(cleanup["completed"])
        cleanup_pending = int(cleanup["pending"])
        for _ in range(max_batches):
            candidates = self._pending(tenant_id=tenant_id, limit=batch_size)
            if not candidates:
                break
            uploads: list[LogicalEvidenceUpload | None] = []
            futures = []

            def prepare(
                candidate: LogicalGroupCandidate,
            ) -> LogicalEvidenceUpload | None:
                try:
                    return self._prepare_and_upload(candidate)
                except LogicalEvidenceError as error:
                    if str(error) == "logical_evidence_document_empty":
                        return None
                    raise

            try:
                with ThreadPoolExecutor(
                    max_workers=min(upload_concurrency, len(candidates)),
                    thread_name_prefix="recall-logical-evidence",
                ) as executor:
                    futures = [
                        executor.submit(prepare, candidate)
                        for candidate in candidates
                    ]
                    uploads = [future.result() for future in futures]
            except Exception:
                completed = list(uploads)
                for future in futures:
                    if not future.done():
                        future.cancel()
                        continue
                    try:
                        upload = future.result()
                    except Exception:
                        continue
                    if upload is not None and upload not in completed:
                        completed.append(upload)
                for upload in completed:
                    if upload is None:
                        continue
                    self._schedule_cleanup(upload.all_references)
                self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
                raise
            for candidate, upload in zip(candidates, uploads, strict=True):
                if upload is None:
                    status = self._commit_empty(candidate)
                    if status == "stale":
                        source_races += 1
                        continue
                    pruned += 1
                    continue
                try:
                    status = self._commit(candidate, upload)
                except Exception:
                    self._schedule_cleanup(upload.all_references)
                    self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
                    raise
                if status == "stale":
                    self._schedule_cleanup(upload.all_references)
                    source_races += 1
                    continue
                if status == "adopted":
                    continue
                documents += 1
                records += upload.prepared.record_count
                receipts += upload.prepared.receipt_count
                objects += len(upload.all_references)
                bytes_uploaded += sum(
                    int(reference["size_bytes"])
                    for reference in upload.all_references
                )
            batches += 1
            cleanup = self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
            old_objects_deleted += int(cleanup["deleted"])
            cleanup_failures += int(cleanup["failures"])
            cleanup_completed += int(cleanup["completed"])
            cleanup_pending = int(cleanup["pending"])
        return {
            "status": "complete",
            "documents": documents,
            "records": records,
            "receipts": receipts,
            "objects": objects,
            "bytes_uploaded": bytes_uploaded,
            "batches": batches,
            "old_objects_deleted": old_objects_deleted,
            "cleanup_completed": cleanup_completed,
            "cleanup_failures": cleanup_failures,
            "cleanup_pending": cleanup_pending,
            "source_races": source_races,
            "pruned": pruned,
        }

    def targets_for_receipts(
        self,
        *,
        tenant_id: str,
        source_ids: tuple[str, ...],
        receipts: tuple[str, ...],
        limit: int,
    ) -> list[dict[str, Any]]:
        tenant_id = self._tenant(tenant_id)
        assert tenant_id is not None
        if not source_ids or not receipts:
            return []
        if not 1 <= limit <= 100:
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        with self.store.connect() as connection:
            rows = connection.execute(
                """WITH hit_documents AS (
                       SELECT evidence.tenant_id,evidence.source_id,
                              evidence.logical_document_id,evidence.revision,
                              array_agg(
                                  DISTINCT chunk.receipt
                                  ORDER BY chunk.receipt
                              ) AS receipts
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
                          AND chunk.receipt=ANY(%s)
                          AND chunk.deleted_at IS NULL
                          AND document.is_current
                          AND document.deleted_at IS NULL
                        GROUP BY evidence.tenant_id,evidence.source_id,
                                 evidence.logical_document_id,evidence.revision
                   )
                   SELECT part.tenant_id,part.source_id,part.artifact_id,
                          part.storage_backend,part.object_key,
                          part.content_sha256,part.size_bytes,part.media_type,
                          part.encryption,part.version_id,part.created_at,
                          hit.receipts
                     FROM hit_documents hit
                     JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=hit.tenant_id
                      AND part.source_id=hit.source_id
                      AND part.logical_document_id=hit.logical_document_id
                      AND part.revision=hit.revision
                    ORDER BY part.logical_document_id,part.part_ordinal
                    LIMIT %s""",
                (tenant_id, list(source_ids), list(receipts), limit),
            ).fetchall()
        return [
            {
                "reference": self._reference(row),
                "receipts": tuple(row["receipts"]),
            }
            for row in rows
        ]

    def delete_native_ids(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_ids: list[str],
    ) -> int:
        tenant_id = self._tenant(tenant_id)
        assert tenant_id is not None
        if not native_ids:
            return 0
        with self.store.connect() as connection:
            with connection.transaction():
                parents = connection.execute(
                    """SELECT DISTINCT COALESCE(native_parent_id,native_id)
                                  AS native_parent_id
                         FROM canonical_events
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_id=ANY(%s)
                        ORDER BY native_parent_id""",
                    (tenant_id, source_id, native_ids),
                ).fetchall()
                references: list[dict[str, Any]] = []
                for parent in parents:
                    parent_id = parent["native_parent_id"]
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (
                            "logical-evidence\x1f"
                            + logical_document_id(
                                tenant_id,
                                source_id,
                                parent_id,
                            ),
                        ),
                    )
                    candidate = LogicalGroupCandidate(
                        tenant_id=tenant_id,
                        source_id=source_id,
                        native_parent_id=parent_id,
                        source_updated_at=datetime.now(timezone.utc),
                        generation=1,
                        revision=1,
                    )
                    manifest, parts = self._old_references(
                        connection,
                        candidate,
                    )
                    if manifest is not None:
                        references.extend((manifest, *parts))
                mark_logical_evidence_dirty(
                    connection,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    native_ids=native_ids,
                    reason="forget",
                )
                self._enqueue_cleanup(connection, tuple(references))
                connection.execute(
                    """DELETE FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=ANY(%s)""",
                    (
                        tenant_id,
                        source_id,
                        [row["native_parent_id"] for row in parents],
                    ),
                )
        self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
        artifact_ids = [reference["artifact_id"] for reference in references]
        with self.store.connect() as connection:
            remaining = (
                connection.execute(
                    """SELECT count(*) AS count
                         FROM canonical_evidence_cleanup_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND artifact_id=ANY(%s)""",
                    (tenant_id, source_id, artifact_ids),
                ).fetchone()["count"]
                if artifact_ids
                else 0
            )
        if remaining:
            raise LogicalEvidenceError("logical_evidence_delete_incomplete")
        return len(artifact_ids)
