from __future__ import annotations

import gzip
import hashlib
import io
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import groupby
from typing import Any

import orjson

from .actor_attribution import actor_links
from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
    LogicalEvidenceUpload,
    ROLE_RE,
    logical_document_id,
)
from .projectors import SOURCE_ID_RE

OVERSIZED_MEDIA_TYPE = "application/vnd.recall.oversized-record+gzip"
MAX_RESTORED_RECORD_BYTES = 256 * 1024 * 1024
TEXT_SEGMENT_BYTES = 14 * 1024 * 1024
DEFAULT_EXCLUDED_STRUCTURAL_TYPES = (
    "file-history-snapshot",
    "queue-operation",
    "token_count",
    "turn_context",
)
MAX_LOGICAL_EVIDENCE_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class LogicalGroupCandidate:
    tenant_id: str
    source_id: str
    native_parent_id: str
    source_updated_at: datetime
    generation: int
    revision: int
    estimated_records: int = 1
    estimated_bytes: int = 1


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


def _explicit_roles(values: Any) -> tuple[str, ...]:
    """Validate compact structural role values extracted by PostgreSQL."""

    if not isinstance(values, list):
        raise LogicalEvidenceError("logical_evidence_state_invalid")
    allowed = {"user", "assistant", "system", "developer", "tool"}
    aliases = {
        "agent_message": "assistant",
        "assistant_message": "assistant",
        "user_message": "user",
    }
    return tuple(
        sorted(
            {
                aliases.get(value, value)
                for value in values
                if isinstance(value, str)
                and ROLE_RE.fullmatch(value)
                and aliases.get(value, value) in allowed
            }
        )
    )


def _parsed_structural_values(
    text: str,
) -> tuple[bool, tuple[str, ...], tuple[str, ...], Any]:
    try:
        value = orjson.loads(text)
    except orjson.JSONDecodeError:
        return False, (), (), None
    if not isinstance(value, dict):
        return True, (), (), value

    def string_at(*path: str) -> str | None:
        current: Any = value
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current if isinstance(current, str) else None

    types = tuple(
        candidate
        for candidate in (
            string_at("type"),
            string_at("message", "type"),
            string_at("payload", "type"),
            string_at("payload", "message", "type"),
        )
        if candidate is not None
    )
    roles = tuple(
        candidate
        for candidate in (
            string_at("role"),
            string_at("type"),
            string_at("message", "role"),
            string_at("message", "type"),
            string_at("payload", "role"),
            string_at("payload", "type"),
            string_at("payload", "message", "role"),
            string_at("payload", "message", "type"),
        )
        if candidate is not None
    )
    return True, types, roles, value


def _structural_values(text: str) -> tuple[bool, tuple[str, ...], tuple[str, ...]]:
    parsed, types, roles, _content = _parsed_structural_values(text)
    return parsed, types, roles


class CanonicalLogicalEvidenceProjector:
    """Project current canonical records into exact source-level evidence documents."""

    def __init__(
        self,
        store: Any,
        projection: LogicalEvidenceProjectionStore,
        *,
        bound_tenant_id: str | None = None,
        raw_archive: Any | None = None,
        excluded_structural_types: tuple[str, ...] = (
            DEFAULT_EXCLUDED_STRUCTURAL_TYPES
        ),
        retention_profile: str = "conversation-useful-v1",
        cursor_fetch_rows: int = 10_000,
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
        if (
            not isinstance(excluded_structural_types, tuple)
            or not excluded_structural_types
            or any(
                not isinstance(value, str) or not value
                for value in excluded_structural_types
            )
            or len(set(excluded_structural_types)) != len(excluded_structural_types)
            or retention_profile != "conversation-useful-v1"
        ):
            raise LogicalEvidenceError("logical_evidence_retention_invalid")
        if (
            isinstance(cursor_fetch_rows, bool)
            or not isinstance(cursor_fetch_rows, int)
            or not 1_000 <= cursor_fetch_rows <= 50_000
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        self.excluded_structural_types = excluded_structural_types
        self.retention_profile = retention_profile
        self.cursor_fetch_rows = cursor_fetch_rows

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
        content = row["oversized_content"]
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
            raise LogicalEvidenceError("logical_evidence_full_record_unavailable")
        try:
            compressed = self.raw_archive.read_raw(self._reference(row, prefix="raw_"))
            with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
                payload = source.read(MAX_RESTORED_RECORD_BYTES + 1)
            if (
                len(payload) != content["full_size_bytes"]
                or len(payload) > MAX_RESTORED_RECORD_BYTES
                or hashlib.sha256(payload).hexdigest() != content["full_content_sha256"]
            ):
                raise LogicalEvidenceError("logical_evidence_full_record_corrupt")
            text = payload.decode()
            if not isinstance(json.loads(text), dict):
                raise LogicalEvidenceError("logical_evidence_full_record_corrupt")
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
        text: str,
        receipts: list[str],
        start_ordinal: int,
        canonical_content_bytes: bytes | None = None,
    ):
        if (
            not isinstance(text, str)
            or not receipts
            or len(receipts) != len(set(receipts))
            or row["chunk_count"] != len(receipts)
        ):
            raise LogicalEvidenceError("logical_evidence_state_invalid")
        text = self._restored_record_text(row, text)
        segments = self._text_segments(text)
        roles = _explicit_roles(row["explicit_role_values"])
        attributed = actor_links(row.get("actor_links") or ())
        use_cached_content = (
            canonical_content_bytes is not None
            and len(segments) == 1
        )
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
                canonical_content_bytes=(
                    canonical_content_bytes
                    if use_cached_content
                    else None
                ),
                actor_links=attributed,
            )

    def _record_stream(self, cursor: Any):
        next_ordinal = 0
        for row in cursor:
            text = row["event_text"]
            revision = row["document_revision"]
            if (
                not isinstance(text, str)
                or isinstance(revision, bool)
                or not isinstance(revision, int)
                or revision < 1
            ):
                raise LogicalEvidenceError("logical_evidence_state_invalid")
            (
                parsed,
                structural_types,
                structural_roles,
                canonical_content,
            ) = _parsed_structural_values(text)
            if not parsed:
                structural_types = tuple(row["fallback_type_values"])
                structural_roles = tuple(row["fallback_role_values"])
            if set(structural_types).intersection(
                self.excluded_structural_types
            ):
                continue
            canonical_content_bytes = None
            if parsed and row["raw_media_type"] != OVERSIZED_MEDIA_TYPE:
                source_bytes = text.encode()
                if len(source_bytes) <= TEXT_SEGMENT_BYTES:
                    candidate_bytes = orjson.dumps(
                        canonical_content,
                        option=orjson.OPT_SORT_KEYS,
                    )
                    if candidate_bytes == source_bytes:
                        canonical_content_bytes = candidate_bytes
            row["explicit_role_values"] = list(structural_roles)
            receipts = list(row["chunk_receipts"])
            records = tuple(
                self._event_records(
                    row,
                    text=text,
                    receipts=receipts,
                    start_ordinal=next_ordinal,
                    canonical_content_bytes=canonical_content_bytes,
                )
            )
            yield from records
            next_ordinal += len(records)

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
                          COALESCE(evidence.revision,0)+1 AS revision,
                          COALESCE(evidence.record_count,1)
                              AS estimated_records,
                          COALESCE(
                              evidence_size.estimated_bytes,
                              evidence.record_count,
                              1
                          ) AS estimated_bytes
                     FROM canonical_evidence_document_queue queue
                     LEFT JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=queue.tenant_id
                      AND evidence.source_id=queue.source_id
                      AND evidence.native_parent_id=queue.native_parent_id
                     LEFT JOIN LATERAL (
                              SELECT sum(part.size_bytes)
                                         AS estimated_bytes
                                FROM canonical_evidence_document_parts part
                               WHERE part.tenant_id=evidence.tenant_id
                                 AND part.source_id=evidence.source_id
                                 AND part.logical_document_id
                                     =evidence.logical_document_id
                                 AND part.revision=evidence.revision
                     ) evidence_size ON true
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
                estimated_records=max(1, int(row["estimated_records"])),
                estimated_bytes=max(1, int(row["estimated_bytes"])),
            )
            for row in rows
        ]

    def seed_backfill(
        self,
        *,
        tenant_id: str | None = None,
        source_id: str | None = None,
        include_existing: bool = False,
    ) -> int:
        """Queue missing projections, or every current logical document on request."""

        tenant_id = self._tenant(tenant_id)
        if source_id is not None and not SOURCE_ID_RE.fullmatch(source_id):
            raise LogicalEvidenceError("logical_evidence_rebuild_invalid")
        if not isinstance(include_existing, bool):
            raise LogicalEvidenceError("logical_evidence_rebuild_invalid")
        with self.store.connect() as connection:
            if include_existing:
                result = connection.execute(
                    """INSERT INTO canonical_evidence_document_queue(
                           tenant_id,source_id,native_parent_id,
                           generation,reason,changed_at
                       )
                       SELECT evidence.tenant_id,evidence.source_id,
                              evidence.native_parent_id,
                              1,'backfill',clock_timestamp()
                         FROM canonical_evidence_documents evidence
                        WHERE (%s::text IS NULL OR evidence.tenant_id=%s)
                          AND (%s::text IS NULL OR evidence.source_id=%s)
                       ON CONFLICT(tenant_id,source_id,native_parent_id)
                       DO UPDATE SET
                           generation=canonical_evidence_document_queue.generation+1,
                           reason='backfill',
                           changed_at=clock_timestamp()""",
                    (tenant_id, tenant_id, source_id, source_id),
                )
                return max(0, result.rowcount)
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
                              AND (
                                  %s::text IS NULL
                                  OR document.source_id=%s
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
                (tenant_id, tenant_id, source_id, source_id),
            )
        return max(0, result.rowcount)

    def _prepare_batch_and_upload(
        self,
        candidates: tuple[LogicalGroupCandidate, ...],
    ) -> list[LogicalEvidenceUpload | None]:
        if not candidates:
            return []
        uploads: list[LogicalEvidenceUpload | None] = [None] * len(candidates)
        completed: list[LogicalEvidenceUpload] = []
        try:
            with self.store.connect() as connection:
                existing_parts: dict[int, list[dict[str, Any]]] = {}
                part_rows = connection.execute(
                    """WITH selected(
                           candidate_ordinal,tenant_id,source_id,
                           native_parent_id
                       ) AS MATERIALIZED (
                           SELECT * FROM unnest(
                               %s::integer[],%s::text[],%s::text[],%s::text[]
                           )
                       )
                       SELECT selected.candidate_ordinal,part.*
                         FROM selected
                         JOIN canonical_evidence_documents evidence
                           ON evidence.tenant_id=selected.tenant_id
                          AND evidence.source_id=selected.source_id
                          AND evidence.native_parent_id
                              =selected.native_parent_id
                         JOIN canonical_evidence_document_parts part
                           ON part.tenant_id=evidence.tenant_id
                          AND part.source_id=evidence.source_id
                          AND part.logical_document_id
                              =evidence.logical_document_id
                          AND part.revision=evidence.revision
                        ORDER BY selected.candidate_ordinal,
                                 part.part_ordinal""",
                    (
                        list(range(len(candidates))),
                        [candidate.tenant_id for candidate in candidates],
                        [candidate.source_id for candidate in candidates],
                        [candidate.native_parent_id for candidate in candidates],
                    ),
                ).fetchall()
                for row in part_rows:
                    existing_parts.setdefault(
                        int(row["candidate_ordinal"]),
                        [],
                    ).append(self._reference(row))
                with connection.cursor(
                    name="logical_evidence_batch_stream",
                ) as cursor:
                    cursor.itersize = self.cursor_fetch_rows
                    cursor.execute(
                        """WITH selected(
                               candidate_ordinal,tenant_id,source_id,
                               native_parent_id
                           ) AS MATERIALIZED (
                               SELECT * FROM unnest(
                                   %s::integer[],%s::text[],%s::text[],%s::text[]
                               )
                           )
                           SELECT selected.candidate_ordinal,
                              event.tenant_id,event.source_id,
                              event.event_id,event.native_id,event.kind,
                              event.occurred_at,
                              CASE
                                  WHEN left(
                                      ltrim(document.text_redacted),1
                                  ) IN ('{','[') THEN '[]'::jsonb
                                  ELSE jsonb_build_array(
                                      event.canonical_redacted->>'role',
                                      event.canonical_redacted->>'type',
                                      event.canonical_redacted
                                          #>> '{content,role}',
                                      event.canonical_redacted
                                          #>> '{content,type}',
                                      event.canonical_redacted
                                          #>> '{content,message,role}',
                                      event.canonical_redacted
                                          #>> '{content,message,type}',
                                      event.canonical_redacted
                                          #>> '{content,payload,role}',
                                      event.canonical_redacted
                                          #>> '{content,payload,type}'
                                  )
                              END AS fallback_role_values,
                              CASE
                                  WHEN left(
                                      ltrim(document.text_redacted),1
                                  ) IN ('{','[') THEN '[]'::jsonb
                                  ELSE jsonb_build_array(
                                      event.canonical_redacted->>'type',
                                      event.canonical_redacted
                                          #>> '{content,type}',
                                      event.canonical_redacted
                                          #>> '{content,message,type}',
                                      event.canonical_redacted
                                          #>> '{content,payload,type}'
                                  )
                              END AS fallback_type_values,
                              CASE WHEN artifact.media_type=%s
                                   THEN event.canonical_redacted->'content'
                                   ELSE NULL
                              END AS oversized_content,
                              event.source_ordinal AS byte_start,
                              document.text_redacted AS event_text,
                              document.revision AS document_revision,
                              source_record.chunk_count,
                              source_record.chunk_receipts,
                              coalesce(
                                  attributed.actor_links,
                                  '[]'::jsonb
                              ) AS actor_links,
                              artifact.artifact_id AS raw_artifact_id,
                              artifact.storage_backend AS raw_storage_backend,
                              artifact.object_key AS raw_object_key,
                              artifact.content_sha256 AS raw_content_sha256,
                              artifact.size_bytes AS raw_size_bytes,
                              artifact.media_type AS raw_media_type,
                              artifact.encryption AS raw_encryption,
                              artifact.version_id AS raw_version_id,
                              artifact.created_at AS raw_created_at
                         FROM selected
                         JOIN canonical_events event
                           ON event.tenant_id=selected.tenant_id
                          AND event.source_id=selected.source_id
                          AND COALESCE(
                              event.native_parent_id,event.native_id
                          )=selected.native_parent_id
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
                         JOIN LATERAL (
                              SELECT count(*)::integer AS chunk_count,
                                     array_agg(
                                         chunk.receipt ORDER BY chunk.ordinal
                                     ) AS chunk_receipts
                                FROM canonical_chunks chunk
                               WHERE chunk.tenant_id=document.tenant_id
                                 AND chunk.source_id=document.source_id
                                 AND chunk.document_id=document.document_id
                                 AND chunk.deleted_at IS NULL
                         ) source_record
                           ON source_record.chunk_count>0
                         LEFT JOIN LATERAL (
                              SELECT jsonb_agg(
                                         jsonb_build_object(
                                             'actor_id',link.actor_id,
                                             'relation',link.relation
                                         )
                                         ORDER BY link.actor_id,link.relation
                                     ) AS actor_links
                                FROM (
                                      SELECT actor.actor_id,actor.relation
                                        FROM canonical_event_actors actor
                                       WHERE actor.tenant_id=event.tenant_id
                                         AND actor.source_id=event.source_id
                                         AND actor.event_id=event.event_id
                                      UNION
                                      SELECT binding.actor_id,binding.relation
                                        FROM canonical_source_actor_bindings
                                             binding
                                       WHERE binding.tenant_id=event.tenant_id
                                         AND binding.source_id=event.source_id
                                ) link
                         ) attributed ON true
                        ORDER BY
                          selected.candidate_ordinal,
                          event.source_ordinal IS NULL,
                          byte_start,event.occurred_at,event.native_id""",
                        (
                            list(range(len(candidates))),
                            [candidate.tenant_id for candidate in candidates],
                            [candidate.source_id for candidate in candidates],
                            [candidate.native_parent_id for candidate in candidates],
                            OVERSIZED_MEDIA_TYPE,
                        ),
                    )
                    previous_ordinal = -1
                    for ordinal, rows in groupby(
                        cursor,
                        key=lambda row: int(row["candidate_ordinal"]),
                    ):
                        if not previous_ordinal < ordinal < len(candidates):
                            raise LogicalEvidenceError("logical_evidence_state_invalid")
                        previous_ordinal = ordinal
                        candidate = candidates[ordinal]
                        try:
                            upload = self.projection.put_records(
                                tenant_id=candidate.tenant_id,
                                source_id=candidate.source_id,
                                native_parent_id=candidate.native_parent_id,
                                revision=candidate.revision,
                                records=self._record_stream(rows),
                                retention_profile=self.retention_profile,
                                existing_part_references=tuple(
                                    existing_parts.get(ordinal, ())
                                ),
                            )
                        except LogicalEvidenceError as error:
                            if str(error) == "logical_evidence_document_empty":
                                continue
                            raise
                        uploads[ordinal] = upload
                        completed.append(upload)
            return uploads
        except Exception:
            for upload in completed:
                self._schedule_cleanup(upload.all_references)
            self.drain_cleanup(
                tenant_id=candidates[0].tenant_id,
                limit=5_000,
            )
            raise

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

    def _schedule_upload_cleanup(
        self,
        uploads: list[LogicalEvidenceUpload],
    ) -> int:
        return self._schedule_cleanup(
            tuple(
                reference
                for upload in uploads
                for reference in upload.all_references
            )
        )

    def drain_cleanup(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 500,
        concurrency: int = 1,
    ) -> dict[str, int | str]:
        if (
            not 1 <= limit <= 5_000
            or isinstance(concurrency, bool)
            or not isinstance(concurrency, int)
            or not 1 <= concurrency <= 32
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        tenant_id = self._tenant(tenant_id)
        completed = deleted = failures = 0
        with self.store.connect() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """SELECT queue.*,
                              NOT EXISTS (
                                  SELECT 1
                                    FROM canonical_evidence_documents document
                                   WHERE document.tenant_id=queue.tenant_id
                                     AND document.source_id=queue.source_id
                                     AND document.manifest_artifact_id
                                         =queue.artifact_id
                                  UNION ALL
                                  SELECT 1
                                    FROM canonical_evidence_document_parts part
                                   WHERE part.tenant_id=queue.tenant_id
                                     AND part.source_id=queue.source_id
                                     AND part.artifact_id=queue.artifact_id
                              ) AS removable
                         FROM canonical_evidence_cleanup_queue queue
                        WHERE (%s::text IS NULL OR queue.tenant_id=%s)
                        ORDER BY queue.queued_at,queue.tenant_id,
                                 queue.source_id,queue.artifact_id
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED""",
                    (tenant_id, tenant_id, limit),
                ).fetchall()
                protected = [
                    row
                    for row in rows
                    if row["removable"] is not True
                ]
                removable = [
                    row
                    for row in rows
                    if row["removable"] is True
                ]
                references = [
                    self._reference(row)
                    for row in removable
                ]
                with ThreadPoolExecutor(
                    max_workers=min(concurrency, max(1, len(removable))),
                    thread_name_prefix="recall-logical-cleanup",
                ) as executor:
                    futures = [
                        executor.submit(
                            self.projection.delete_reference,
                            reference,
                        )
                        for reference in references
                    ]
                succeeded: list[tuple[str, str, str]] = [
                    (
                        row["tenant_id"],
                        row["source_id"],
                        row["artifact_id"],
                    )
                    for row in protected
                ]
                completed += len(protected)
                failed: list[tuple[str, str, str]] = []
                for row, future in zip(removable, futures, strict=True):
                    identity = (
                        row["tenant_id"],
                        row["source_id"],
                        row["artifact_id"],
                    )
                    try:
                        removed = future.result()
                    except Exception:
                        failed.append(identity)
                        failures += 1
                        continue
                    succeeded.append(identity)
                    completed += 1
                    deleted += int(removed)
                if succeeded:
                    connection.execute(
                        """WITH completed(
                               tenant_id,source_id,artifact_id
                           ) AS (
                               SELECT * FROM unnest(
                                   %s::text[],%s::text[],%s::text[]
                               )
                           )
                           DELETE FROM canonical_evidence_cleanup_queue queue
                           USING completed
                           WHERE queue.tenant_id=completed.tenant_id
                             AND queue.source_id=completed.source_id
                             AND queue.artifact_id=completed.artifact_id""",
                        (
                            [identity[0] for identity in succeeded],
                            [identity[1] for identity in succeeded],
                            [identity[2] for identity in succeeded],
                        ),
                    )
                if failed:
                    connection.execute(
                        """WITH failed(
                               tenant_id,source_id,artifact_id
                           ) AS (
                               SELECT * FROM unnest(
                                   %s::text[],%s::text[],%s::text[]
                               )
                           )
                           UPDATE canonical_evidence_cleanup_queue queue
                              SET attempts=queue.attempts+1,
                                  last_attempt_at=clock_timestamp()
                             FROM failed
                            WHERE queue.tenant_id=failed.tenant_id
                              AND queue.source_id=failed.source_id
                              AND queue.artifact_id=failed.artifact_id""",
                        (
                            [identity[0] for identity in failed],
                            [identity[1] for identity in failed],
                            [identity[2] for identity in failed],
                        ),
                    )
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
                    ("logical-evidence\x1f" + prepared.logical_document_id,),
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
                retained_artifacts = {
                    reference["artifact_id"]
                    for reference in upload.all_references
                }
                self._enqueue_cleanup(
                    connection,
                    tuple(
                        reference
                        for reference in (old_manifest, *old_parts)
                        if (
                            reference is not None
                            and reference["artifact_id"]
                            not in retained_artifacts
                        )
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
                connection.execute(
                    """INSERT INTO canonical_evidence_document_actors(
                           tenant_id,source_id,logical_document_id,revision,
                           actor_id,relation
                       )
                       SELECT %s,%s,%s,%s,link.actor_id,link.relation
                         FROM (
                               SELECT actor.actor_id,actor.relation
                                 FROM canonical_events event
                                 JOIN canonical_event_actors actor
                                   ON actor.tenant_id=event.tenant_id
                                  AND actor.source_id=event.source_id
                                  AND actor.event_id=event.event_id
                                WHERE event.tenant_id=%s
                                  AND event.source_id=%s
                                  AND coalesce(
                                      event.native_parent_id,event.native_id
                                  )=%s
                               UNION
                               SELECT binding.actor_id,binding.relation
                                 FROM canonical_source_actor_bindings binding
                                WHERE binding.tenant_id=%s
                                  AND binding.source_id=%s
                         ) link
                       ON CONFLICT DO NOTHING""",
                    (
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.logical_document_id,
                        prepared.revision,
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.native_parent_id,
                        prepared.tenant_id,
                        prepared.source_id,
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_passage_projection_queue(
                           tenant_id,source_id,logical_document_id,revision,
                           generation,reason,changed_at
                       ) VALUES (%s,%s,%s,%s,1,'logical-update',
                                 clock_timestamp())
                       ON CONFLICT(
                           tenant_id,source_id,logical_document_id
                       )
                       DO UPDATE SET
                           revision=excluded.revision,
                           generation=
                               canonical_passage_projection_queue.generation+1,
                           reason='logical-update',
                           changed_at=clock_timestamp()""",
                    (
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.logical_document_id,
                        prepared.revision,
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

    def _commit_upload(
        self,
        candidate: LogicalGroupCandidate,
        upload: LogicalEvidenceUpload | None,
    ) -> str:
        if upload is None:
            return self._commit_empty(candidate)
        try:
            status = self._commit(candidate, upload)
        except Exception:
            self._schedule_cleanup(upload.all_references)
            raise
        if status == "stale":
            self._schedule_cleanup(upload.all_references)
        return status

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
            or not 1 <= batch_size <= MAX_LOGICAL_EVIDENCE_BATCH_SIZE
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(upload_concurrency, bool)
            or not isinstance(upload_concurrency, int)
            or not 1 <= upload_concurrency <= 32
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        pool_size = getattr(self.store, "pool_max_size", upload_concurrency)
        if (
            isinstance(pool_size, bool)
            or not isinstance(pool_size, int)
            or upload_concurrency > pool_size
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        prepare_pool = getattr(self.store, "prepare_pool", None)
        if callable(prepare_pool):
            prepare_pool(min(upload_concurrency, batch_size))
        tenant_id = self._tenant(tenant_id)
        documents = records = receipts = objects = bytes_uploaded = batches = 0
        old_objects_deleted = cleanup_failures = source_races = pruned = 0
        cleanup_completed = cleanup_pending = 0
        cleanup = self.drain_cleanup(
            tenant_id=tenant_id,
            limit=5_000,
            concurrency=upload_concurrency,
        )
        old_objects_deleted += int(cleanup["deleted"])
        cleanup_failures += int(cleanup["failures"])
        cleanup_completed += int(cleanup["completed"])
        cleanup_pending = int(cleanup["pending"])
        for _ in range(max_batches):
            candidates = self._pending(tenant_id=tenant_id, limit=batch_size)
            if not candidates:
                break
            worker_count = min(upload_concurrency, len(candidates))
            shards: list[list[tuple[int, LogicalGroupCandidate]]] = [
                [] for _ in range(worker_count)
            ]
            shard_loads = [0] * worker_count
            weighted_candidates = sorted(
                enumerate(candidates),
                key=lambda value: (
                    -value[1].estimated_bytes,
                    value[0],
                ),
            )
            for index, candidate in weighted_candidates:
                shard_index = min(
                    range(worker_count),
                    key=lambda value: (shard_loads[value], value),
                )
                shards[shard_index].append((index, candidate))
                shard_loads[shard_index] += candidate.estimated_bytes
            uploads: list[LogicalEvidenceUpload | None] = [None] * len(candidates)
            successful: list[LogicalEvidenceUpload] = []
            failures: list[Exception] = []
            futures = []
            try:
                with ThreadPoolExecutor(
                    max_workers=worker_count,
                    thread_name_prefix="recall-logical-evidence",
                ) as executor:
                    futures = [
                        (
                            shard,
                            executor.submit(
                                self._prepare_batch_and_upload,
                                tuple(candidate for _, candidate in shard),
                            ),
                        )
                        for shard in shards
                    ]
                    for shard, future in futures:
                        try:
                            shard_uploads = future.result()
                        except Exception as error:
                            failures.append(error)
                            continue
                        for (index, _candidate), upload in zip(
                            shard,
                            shard_uploads,
                            strict=True,
                        ):
                            uploads[index] = upload
                            if upload is not None:
                                successful.append(upload)
            except BaseException:
                interrupted_uploads: list[LogicalEvidenceUpload] = []
                for _shard, future in futures:
                    if future.cancelled():
                        continue
                    try:
                        shard_uploads = future.result()
                    except BaseException:
                        continue
                    interrupted_uploads.extend(
                        upload
                        for upload in shard_uploads
                        if upload is not None
                    )
                self._schedule_upload_cleanup(interrupted_uploads)
                self.drain_cleanup(
                    tenant_id=tenant_id,
                    limit=5_000,
                    concurrency=upload_concurrency,
                )
                raise
            if failures:
                self._schedule_upload_cleanup(successful)
                self.drain_cleanup(
                    tenant_id=tenant_id,
                    limit=5_000,
                    concurrency=upload_concurrency,
                )
                raise failures[0]
            statuses: list[str | None] = [None] * len(candidates)
            failures = []
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="recall-logical-commit",
            ) as executor:
                commit_futures = [
                    (
                        index,
                        executor.submit(
                            self._commit_upload,
                            candidate,
                            upload,
                        ),
                    )
                    for index, (candidate, upload) in enumerate(
                        zip(candidates, uploads, strict=True)
                    )
                ]
                for index, future in commit_futures:
                    try:
                        statuses[index] = future.result()
                    except Exception as error:
                        failures.append(error)
            if failures:
                self.drain_cleanup(tenant_id=tenant_id, limit=5_000)
                raise failures[0]
            for upload, status in zip(uploads, statuses, strict=True):
                if status == "stale":
                    source_races += 1
                    continue
                if status == "adopted":
                    continue
                if status == "pruned":
                    pruned += 1
                    continue
                if status != "committed" or upload is None:
                    raise LogicalEvidenceError("logical_evidence_state_invalid")
                documents += 1
                records += upload.prepared.record_count
                receipts += upload.prepared.receipt_count
                objects += len(upload.all_references)
                bytes_uploaded += sum(
                    int(reference["size_bytes"]) for reference in upload.all_references
                )
            batches += 1
            cleanup = self.drain_cleanup(
                tenant_id=tenant_id,
                limit=5_000,
                concurrency=upload_concurrency,
            )
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
                          AND NOT (
                              ARRAY[
                                  event.canonical_redacted->>'type',
                                  event.canonical_redacted
                                      #>> '{content,type}',
                                  event.canonical_redacted
                                      #>> '{content,message,type}',
                                  event.canonical_redacted
                                      #>> '{content,payload,type}',
                                  event.canonical_redacted
                                      #>> '{message,type}',
                                  event.canonical_redacted
                                      #>> '{payload,type}'
                              ] && %s::text[]
                          )
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
                (
                    tenant_id,
                    list(source_ids),
                    list(receipts),
                    list(self.excluded_structural_types),
                    limit,
                ),
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
