from __future__ import annotations

import gzip
import logging
import hashlib
import io
import json
import pickle
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import groupby
from typing import Any

import orjson
import psycopg

from .actor_attribution import actor_links
from .canonical_text import MAX_CANONICAL_TEXT_BYTES, canonical_text_chunks
from .logical_archive_bodies import ArchivedBodyLookup
from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
    LogicalEvidenceUpload,
    ROLE_RE,
    logical_document_id,
)
from .projectors import SOURCE_ID_RE
from .passage_projection import decode_logical_record
from .search_outbox import record_passage_deletions

OVERSIZED_MEDIA_TYPE = "application/vnd.recall.oversized-record+gzip"
MAX_RESTORED_RECORD_BYTES = 256 * 1024 * 1024
TEXT_SEGMENT_BYTES = 14 * 1024 * 1024
DEFAULT_EXCLUDED_STRUCTURAL_TYPES = (
    "file-history-snapshot",
    "queue-operation",
    "token_count",
    "turn_context",
)
LOG = logging.getLogger(__name__)
MAX_LOGICAL_EVIDENCE_BATCH_SIZE = 10_000
# A queued group is retried with exponential backoff (60 s doubling, capped
# at 6 h) and quarantined after this many failed projection attempts.
MAX_LOGICAL_ATTEMPTS = 8
LOGICAL_BACKOFF_BASE_SECONDS = 60
LOGICAL_BACKOFF_CAP_SECONDS = 6 * 3600


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


class _LocatorSpool:
    """Private, replayable metadata stream; RAM does not grow with the parent."""

    def __init__(self):
        self._file = tempfile.TemporaryFile(mode="w+b")

    def append(self, location):
        self._file.write(orjson.dumps(location) + b"\n")

    def __iter__(self):
        self._file.seek(0)
        for line in self._file:
            yield tuple(orjson.loads(line))

    @property
    def closed(self):
        return self._file.closed

    def close(self):
        self._file.close()


@dataclass(frozen=True)
class _LocatedUpload(LogicalEvidenceUpload):
    # Ownership transfers from preparation to _commit_upload. No source text
    # or parent-sized Python list survives alongside the immutable upload.
    body_locators: _LocatorSpool | None = None


def _close_body_locators(upload):
    locators = getattr(upload, "body_locators", None)
    if locators is not None:
        locators.close()


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


def _validate_source_body(row: dict[str, Any]) -> None:
    """Verify the captured document and its exact stored chunk boundaries."""
    text = row.get("event_text")
    chunks = row.get("source_chunks")
    receipts = row.get("chunk_receipts")
    if (not isinstance(text, str) or not isinstance(chunks, list) or not chunks
            or not isinstance(receipts, list) or len(receipts) != len(chunks)
            or row.get("chunk_count") != len(chunks)):
        raise LogicalEvidenceError("logical_evidence_source_integrity_invalid")
    payload = text.encode()
    if hashlib.sha256(payload).hexdigest() != row.get("document_text_sha256"):
        raise LogicalEvidenceError("logical_evidence_source_integrity_invalid")
    offset = 0
    for ordinal, chunk in enumerate(chunks):
        if not isinstance(chunk, dict):
            raise LogicalEvidenceError("logical_evidence_source_integrity_invalid")
        size = chunk.get("size_bytes")
        if (type(size) is not int or size < 0 or chunk.get("ordinal") != ordinal
                or offset + size > len(payload)
                or hashlib.sha256(payload[offset:offset + size]).hexdigest()
                    != chunk.get("text_sha256")):
            raise LogicalEvidenceError("logical_evidence_source_integrity_invalid")
        offset += size
    if offset != len(payload):
        raise LogicalEvidenceError("logical_evidence_source_integrity_invalid")


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

    def _record_stream(self, cursor: Any, *, locate=None):
        next_ordinal = 0
        for row in cursor:
            _validate_source_body(row)
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
            if (locate is not None and row["raw_media_type"] != OVERSIZED_MEDIA_TYPE
                    and len(text.encode()) <= MAX_CANONICAL_TEXT_BYTES):
                pieces = [text] if row["chunk_count"] == 1 else canonical_text_chunks(text)
                if (len(pieces) == row["chunk_count"]
                        and all(hashlib.sha256(piece.encode()).hexdigest() == chunk["text_sha256"]
                                for piece, chunk in zip(pieces, row["source_chunks"]))):
                    locate((row["document_id"], next_ordinal, len(records)))
            yield from records
            next_ordinal += len(records)

    def _pending(
        self,
        *,
        tenant_id: str | None,
        limit: int,
        quiet_seconds: float = 0.0,
        max_wait_seconds: float = 0.0,
    ) -> list[LogicalGroupCandidate]:
        """Queued groups ready to project.

        With a quiet period, a group is ready only once it has not changed for
        `quiet_seconds`, or has been waiting longer than `max_wait_seconds`
        since it first entered the queue. Forget and backfill never wait.
        """
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
                      AND queue.attempts<%s
                      AND (
                          queue.next_attempt_at IS NULL
                          OR queue.next_attempt_at<=clock_timestamp()
                      )
                      AND (
                          %s::float8<=0
                          OR queue.reason IN ('forget','backfill')
                          OR queue.changed_at
                             < clock_timestamp()-%s*interval '1 second'
                          OR (
                              %s::float8>0
                              AND queue.first_queued_at
                                  < clock_timestamp()-%s*interval '1 second'
                          )
                      )
                    ORDER BY queue.changed_at,queue.tenant_id,
                             queue.source_id,queue.native_parent_id
                    LIMIT %s""",
                (
                    tenant_id, tenant_id,
                    MAX_LOGICAL_ATTEMPTS,
                    quiet_seconds, quiet_seconds,
                    max_wait_seconds, max_wait_seconds,
                    limit,
                ),
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

    def _mark_failed(
        self,
        candidate: LogicalGroupCandidate,
        error: BaseException,
    ) -> None:
        """Record a failed projection attempt and schedule the retry.

        The row keeps its generation and changed_at, so a later collector
        write still supersedes the backoff; only the worker's own retry is
        delayed. Logged content-free: identifiers and the error code only.
        """
        code = str(error) if isinstance(error, LogicalEvidenceError) else type(error).__name__
        with self.store.connect() as connection:
            with connection.transaction():
                row = connection.execute(
                    """UPDATE canonical_evidence_document_queue
                          SET attempts=attempts+1,
                              next_attempt_at=clock_timestamp()
                                  +least(%s,%s*power(2,attempts))
                                  *interval '1 second',
                              last_error_code=left(%s,120)
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s
                          AND generation=%s AND changed_at=%s
                    RETURNING attempts""",
                    (
                        LOGICAL_BACKOFF_CAP_SECONDS,
                        LOGICAL_BACKOFF_BASE_SECONDS,
                        code,
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                        candidate.generation,
                        candidate.source_updated_at,
                    ),
                ).fetchone()
        attempts = int(row["attempts"]) if row else 0
        LOG.warning(
            "logical projection failed source=%s parent=%s code=%s attempts=%s%s",
            candidate.source_id,
            candidate.native_parent_id,
            code,
            attempts,
            " quarantined" if attempts >= MAX_LOGICAL_ATTEMPTS else "",
        )

    def _prepare_batch_and_upload(
        self,
        candidates: tuple[LogicalGroupCandidate, ...],
    ) -> list[LogicalEvidenceUpload | None]:
        if not candidates:
            return []
        uploads: list[LogicalEvidenceUpload | None] = [None] * len(candidates)
        completed: list[LogicalEvidenceUpload] = []
        spool = tempfile.TemporaryFile(mode="w+b")
        ranges: list[tuple[int, int, int]] = []
        input_spool = None
        body_locators: dict[int, _LocatorSpool] = {}
        transferred = False
        try:
            input_spool = tempfile.TemporaryFile(mode="w+b")
            input_ranges: list[tuple[int, int, int]] = []
            pins: dict[int, dict[str, Any]] = {}
            pinned_parts: dict[int, list[dict[str, Any]]] = {}
            recovered: set[int] = set()
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
                       SELECT selected.candidate_ordinal,part.*,
                              to_jsonb(evidence) AS manifest
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
                    ordinal = int(row["candidate_ordinal"])
                    pins[ordinal] = row["manifest"]
                    pinned_parts.setdefault(ordinal, []).append(row)
                    existing_parts.setdefault(
                        int(row["candidate_ordinal"]),
                        [],
                    ).append(self._reference(row))
                with connection.cursor(
                    name="logical_evidence_batch_stream",
                ) as cursor:
                    cursor.itersize = self.cursor_fetch_rows
                    # Decode the root once to avoid repeated TOAST reads. The
                    # cheap match guard avoids decoding before parent filtering;
                    # the unchanged joins retain all scope/current-row authority.
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
                              document.document_id,
                              event.tenant_id,event.source_id,
                              event.event_id,event.native_id,event.kind,
                              event.occurred_at,
                              jsonb_build_array(
                                      root_fields.role,
                                      root_fields.type,
                                      root_fields.content->>'role',
                                      root_fields.content->>'type',
                                      root_fields.content #>> '{message,role}',
                                      root_fields.content #>> '{message,type}',
                                      root_fields.content #>> '{payload,role}',
                                      root_fields.content #>> '{payload,type}'
                              ) AS fallback_role_values,
                              jsonb_build_array(
                                      root_fields.type,
                                      root_fields.content->>'type',
                                      root_fields.content #>> '{message,type}',
                                      root_fields.content #>> '{payload,type}'
                              ) AS fallback_type_values,
                              CASE WHEN artifact.media_type=%s
                                   THEN root_fields.content
                                   ELSE NULL
                              END AS oversized_content,
                              event.source_ordinal AS byte_start,
                              source_record.event_text,
                              document.revision AS document_revision,
                              document.text_sha256 AS document_text_sha256,
                              source_record.source_chunks,
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
                         CROSS JOIN LATERAL jsonb_to_record(
                             CASE WHEN document.event_id=event.event_id
                                  AND COALESCE(
                                      event.native_parent_id,event.native_id
                                  )=selected.native_parent_id
                             THEN CASE WHEN jsonb_typeof(
                                      event.canonical_redacted
                                  )='object'
                                  THEN event.canonical_redacted
                                  ELSE '{}'::jsonb END
                             ELSE '{}'::jsonb END
                         ) AS root_fields(role text,type text,content jsonb)
                         JOIN LATERAL (
                              SELECT count(*)::integer AS chunk_count,
                                     jsonb_agg(jsonb_build_object(
                                         'ordinal',chunk.ordinal,
                                         'size_bytes',octet_length(chunk.text_redacted),
                                         'text_sha256',chunk.text_sha256
                                     ) ORDER BY chunk.ordinal) AS source_chunks,
                                     array_agg(
                                         chunk.receipt ORDER BY chunk.ordinal
                                     ) AS chunk_receipts,
                                     string_agg(
                                         chunk.text_redacted,''
                                         ORDER BY chunk.ordinal
                                     ) AS event_text
                                FROM canonical_chunks chunk
                               WHERE chunk.tenant_id=document.tenant_id
                                 AND chunk.source_id=document.source_id
                                 AND chunk.document_id=document.document_id
                                 AND chunk.deleted_at IS NULL
                         ) source_record ON true
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
                        start = input_spool.tell()
                        for row in rows:
                            # This file is private, self-produced and never
                            # accepted as input from another process or source.
                            pickle.dump(dict(row), input_spool, protocol=5)
                        input_ranges.append((ordinal, start, input_spool.tell()))
            # Archive recovery and oversized raw restoration run only after
            # the input cursor and its pool connection have been released.
            for ordinal, start, end in input_ranges:
                candidate = candidates[ordinal]
                input_spool.seek(start)
                lookup = None

                def resolved_rows():
                    nonlocal lookup
                    while input_spool.tell() < end:
                        row = pickle.load(input_spool)
                        try:
                            _validate_source_body(row)
                        except LogicalEvidenceError:
                            if lookup is None:
                                lookup = ArchivedBodyLookup()
                                lookup.load(
                                    self.projection, candidate=candidate,
                                    manifest=pins.get(ordinal),
                                    parts=pinned_parts.get(ordinal, ()),
                                    reference=self._reference,
                                )
                            row = lookup.restore(row)
                            _validate_source_body(row)
                            recovered.add(ordinal)
                        yield row

                final_start = spool.tell()
                locations = body_locators[ordinal] = _LocatorSpool()
                try:
                    for record in self._record_stream(resolved_rows(), locate=locations.append):
                        spool.write(record.encode(source_id=candidate.source_id))
                finally:
                    if lookup is not None:
                        lookup.close()
                ranges.append((ordinal, final_start, spool.tell()))
            if recovered:
                self._check_recovery_pins(candidates, pins, recovered)
            # No upload starts until every source row in this shard has
            # passed its document and chunk hashes. The SQL cursor and pool
            # connection are released before publishing the validated data.
            for ordinal, start, end in ranges:
                if start == end:
                    body_locators[ordinal].close()
                    continue
                candidate = candidates[ordinal]
                spool.seek(start)

                def records():
                    while spool.tell() < end:
                        yield decode_logical_record(
                            spool.readline(), source_id=candidate.source_id,
                            verify_canonical=False,
                        )

                upload = self.projection.put_records(
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    native_parent_id=candidate.native_parent_id,
                    revision=candidate.revision,
                    records=records(),
                    retention_profile=self.retention_profile,
                    existing_part_references=tuple(existing_parts.get(ordinal, ())),
                )
                upload = _LocatedUpload(**vars(upload), body_locators=body_locators[ordinal])
                uploads[ordinal] = upload
                completed.append(upload)
            transferred = True
            return uploads
        except Exception:
            for upload in completed:
                self._schedule_cleanup(upload.cleanup_references)
            self.drain_cleanup(
                tenant_id=candidates[0].tenant_id,
                limit=5_000,
            )
            raise
        finally:
            if not transferred:
                for locators in body_locators.values():
                    locators.close()
            spool.close()
            if input_spool is not None:
                input_spool.close()

    def _check_recovery_pins(self, candidates, pins, recovered):
        """Reject stale archive recovery before publishing any candidate."""
        with self.store.connect() as connection:
            for ordinal in sorted(recovered):
                candidate = candidates[ordinal]
                row = connection.execute(
                    """SELECT queue.generation,queue.changed_at,
                              to_jsonb(evidence) AS manifest
                         FROM canonical_evidence_document_queue queue
                         LEFT JOIN canonical_evidence_documents evidence
                           USING(tenant_id,source_id,native_parent_id)
                        WHERE queue.tenant_id=%s AND queue.source_id=%s
                          AND queue.native_parent_id=%s""",
                    (candidate.tenant_id, candidate.source_id, candidate.native_parent_id),
                ).fetchone()
                if (row is None or row["generation"] != candidate.generation
                        or row["changed_at"] != candidate.source_updated_at
                        or row["manifest"] != pins[ordinal]):
                    raise LogicalEvidenceError("logical_evidence_source_changed")

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
                   USING(tenant_id,source_id,logical_document_id)
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

    @staticmethod
    def _queue_parquet_scan(
        connection: Any,
        *,
        tenant_id: str,
        source_id: str,
        ranges: tuple[tuple[Any, Any], ...],
        reason: str,
        logical_document_id: str | None = None,
    ) -> int:
        """Invalidate only source/month fragments touched by a logical change.

        The queue row marks the source-month; the dirty-document row names the
        logical document so the scan projector rewrites only the fragments that
        hold it. A pending ``backfill`` (full rebuild) is never downgraded.
        """

        if not ranges:
            return 0
        def timestamp(value: Any) -> datetime:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    return parsed
            raise LogicalEvidenceError("logical_evidence_timestamp_invalid")

        starts = [timestamp(value[0]) for value in ranges]
        ends = [timestamp(value[1]) for value in ranges]
        result = connection.execute(
            """INSERT INTO canonical_parquet_scan_queue(
                   tenant_id,source_id,bucket_start,
                   generation,reason,changed_at
               )
               SELECT %s,%s,month.value::date,1,%s,clock_timestamp()
                 FROM unnest(%s::timestamptz[],%s::timestamptz[])
                      AS span(first_at,last_at)
                 CROSS JOIN LATERAL generate_series(
                     date_trunc('month',span.first_at),
                     date_trunc('month',span.last_at),
                     interval '1 month'
                 ) month(value)
                GROUP BY month.value
               ON CONFLICT(tenant_id,source_id,bucket_start)
               DO UPDATE SET
                   generation=canonical_parquet_scan_queue.generation+1,
                   reason=CASE
                       WHEN canonical_parquet_scan_queue.reason='backfill'
                       THEN 'backfill' ELSE excluded.reason END,
                   changed_at=clock_timestamp()""",
            (tenant_id, source_id, reason, starts, ends),
        )
        if logical_document_id is not None:
            connection.execute(
                """INSERT INTO canonical_parquet_scan_dirty_documents(
                       tenant_id,source_id,bucket_start,
                       logical_document_id,reason,queued_at
                   )
                   SELECT %s,%s,month.value::date,%s,%s,clock_timestamp()
                     FROM unnest(%s::timestamptz[],%s::timestamptz[])
                          AS span(first_at,last_at)
                     CROSS JOIN LATERAL generate_series(
                         date_trunc('month',span.first_at),
                         date_trunc('month',span.last_at),
                         interval '1 month'
                     ) month(value)
                    GROUP BY month.value
                   ON CONFLICT(tenant_id,source_id,bucket_start,logical_document_id)
                   DO UPDATE SET reason=excluded.reason,
                                 queued_at=clock_timestamp()""",
                (tenant_id, source_id, logical_document_id, reason, starts, ends),
            )
        return max(0, result.rowcount)

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
        try:
            return self._schedule_cleanup(
                tuple(
                    reference
                    for upload in uploads
                    for reference in upload.cleanup_references
                )
            )
        finally:
            for upload in uploads:
                _close_body_locators(upload)

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
                                  UNION ALL
                                  SELECT 1
                                    FROM canonical_parquet_scan_shards shard
                                   WHERE shard.tenant_id=queue.tenant_id
                                     AND shard.source_id=queue.source_id
                                     AND shard.artifact_id=queue.artifact_id
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
                              document_content_sha256,
                              manifest_artifact_id,first_occurred_at,
                              last_occurred_at
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
                locator_changes = self._publish_body_locators(connection, candidate, getattr(upload, "body_locators", None))
                old_manifest, old_parts = self._old_references(
                    connection,
                    candidate,
                )
                same_parts = tuple(
                    reference["artifact_id"] for reference in old_parts
                ) == tuple(
                    reference["artifact_id"]
                    for reference in upload.part_references
                )
                if (
                    current is not None
                    and old_manifest is not None
                    and current["document_content_sha256"]
                        == prepared.document_content_sha256
                    and same_parts
                ):
                    if locator_changes:
                        # The manifest is unchanged, but newly filled positions
                        # must revisit enabled retirement progress. Lock order
                        # remains catalog then ledger; NOWAIT avoids cycles with
                        # a clear that already owns a document row.
                        connection.execute(
                            """SELECT 1 FROM canonical_evidence_documents
                                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s
                                FOR UPDATE NOWAIT""",
                            (candidate.tenant_id, candidate.source_id, candidate.native_parent_id),
                        )
                        from .chunk_retirement import invalidate_parent_retirement
                        invalidate_parent_retirement(connection.execute,
                            (candidate.tenant_id, candidate.source_id, candidate.native_parent_id))
                    # Repairing an absent immutable object must not replace an
                    # identical database document. The old path cascaded
                    # through every passage, actor, context, and embedding even
                    # though their source bytes had not changed.
                    restored_manifest = (
                        self.projection.restore_manifest_revision(
                            upload,
                            revision=int(current["revision"]),
                        )
                    )
                    if (
                        restored_manifest["artifact_id"]
                        != old_manifest["artifact_id"]
                    ):
                        raise LogicalEvidenceError(
                            "logical_evidence_state_invalid"
                        )
                    if (
                        manifest_reference["artifact_id"]
                        != restored_manifest["artifact_id"]
                    ):
                        self._enqueue_cleanup(
                            connection,
                            (manifest_reference,),
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
                        raise LogicalEvidenceError(
                            "logical_evidence_queue_conflict"
                        )
                    return "repaired"
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
                # A revision updates the catalog row in place. The child
                # tables reference (tenant_id, source_id, logical_document_id)
                # without revision, so passages, embeddings, contexts, and
                # actors survive a session append; only the parts and actor
                # links below are rewritten, and only the passage queue tells
                # the passage projector to catch up. `created_at` is the
                # document's first projection time and is never overwritten.
                committed = connection.execute(
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
                       )
                       ON CONFLICT(tenant_id,source_id,native_parent_id)
                       DO UPDATE SET
                           revision=excluded.revision,
                           evidence_id=excluded.evidence_id,
                           manifest_artifact_id=excluded.manifest_artifact_id,
                           manifest_storage_backend=
                               excluded.manifest_storage_backend,
                           manifest_object_key=excluded.manifest_object_key,
                           manifest_content_sha256=
                               excluded.manifest_content_sha256,
                           manifest_size_bytes=excluded.manifest_size_bytes,
                           manifest_media_type=excluded.manifest_media_type,
                           manifest_encryption=excluded.manifest_encryption,
                           manifest_version_id=excluded.manifest_version_id,
                           document_content_sha256=
                               excluded.document_content_sha256,
                           record_count=excluded.record_count,
                           receipt_count=excluded.receipt_count,
                           part_count=excluded.part_count,
                           first_occurred_at=excluded.first_occurred_at,
                           last_occurred_at=excluded.last_occurred_at,
                           source_updated_at=excluded.source_updated_at
                       WHERE canonical_evidence_documents.logical_document_id
                             =excluded.logical_document_id
                         AND canonical_evidence_documents.revision
                             <excluded.revision""",
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
                if committed.rowcount != 1:
                    raise LogicalEvidenceError("logical_evidence_state_invalid")
                # Parts are replaced per revision; unchanged parts keep their
                # artifact ids, so the cleanup queue above received only the
                # manifest and the rewritten tail part(s).
                connection.execute(
                    """DELETE FROM canonical_evidence_document_parts
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s""",
                    (
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.logical_document_id,
                    ),
                )
                with connection.cursor() as cursor:
                    cursor.executemany(
                        """INSERT INTO canonical_evidence_document_parts(
                               tenant_id,source_id,logical_document_id,revision,
                               part_ordinal,artifact_id,storage_backend,object_key,
                               content_sha256,size_bytes,media_type,encryption,
                               version_id,first_record_ordinal,
                               last_record_ordinal,first_occurred_at,
                               last_occurred_at,receipt_count,created_at
                           ) VALUES (
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s,%s,%s
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
                                part.first_occurred_at,
                                part.last_occurred_at,
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
                # Actor links are tiny and derived; rewrite them for the new
                # revision without touching any passage-level attribution.
                connection.execute(
                    """DELETE FROM canonical_evidence_document_actors
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s""",
                    (
                        prepared.tenant_id,
                        prepared.source_id,
                        prepared.logical_document_id,
                    ),
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
                ranges = [
                    (prepared.first_occurred_at, prepared.last_occurred_at)
                ]
                if current is not None:
                    ranges.append(
                        (
                            current["first_occurred_at"],
                            current["last_occurred_at"],
                        )
                    )
                self._queue_parquet_scan(
                    connection,
                    tenant_id=prepared.tenant_id,
                    source_id=prepared.source_id,
                    ranges=tuple(ranges),
                    reason="logical-update",
                    logical_document_id=prepared.logical_document_id,
                )
                # Publish the retirement cooldown with the new catalog. An
                # older request may still be hydrating a newly located event
                # from PostgreSQL; an aged enabled ledger must not clear it yet.
                from .chunk_retirement import invalidate_parent_retirement
                invalidate_parent_retirement(connection.execute,
                    (candidate.tenant_id, candidate.source_id, candidate.native_parent_id))
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

    @staticmethod
    def _publish_body_locators(connection, candidate, locators):
        """Change positions in the same transaction as the parent catalog.

        Ingest takes document locks before its queue lock. We already hold the
        queue, so NOWAIT prevents a wait cycle and rolls back the whole attempt.
        Unchanged prefixes are neither locked nor updated on parent appends.
        COPY streams private metadata into a transaction-local table. The
        changed-row set stays in Postgres, where sorting can spill to disk.
        """
        connection.execute(
            """CREATE TEMP TABLE recall_body_locator_desired (
                   document_id text PRIMARY KEY,
                   record_ordinal integer NOT NULL CHECK (record_ordinal >= 0),
                   record_count integer NOT NULL CHECK (record_count >= 1)
               ) ON COMMIT DROP"""
        )
        if locators is not None:
            try:
                with connection.cursor() as cursor:
                    with cursor.copy(
                        "COPY pg_temp.recall_body_locator_desired "
                        "(document_id,record_ordinal,record_count) FROM STDIN"
                    ) as writer:
                        for location in locators:
                            writer.write_row(location)
            except psycopg.errors.UniqueViolation:
                raise LogicalEvidenceError("logical_evidence_state_invalid") from None
        connection.execute("ANALYZE pg_temp.recall_body_locator_desired")
        changed = connection.execute(
            """WITH changed AS MATERIALIZED (
                   SELECT document.document_id,desired.record_ordinal,desired.record_count
                     FROM canonical_documents document
                     JOIN canonical_events event USING(tenant_id,source_id,event_id)
                     LEFT JOIN pg_temp.recall_body_locator_desired desired USING(document_id)
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND COALESCE(event.native_parent_id,event.native_id)=%s
                      AND document.is_current AND document.deleted_at IS NULL
                      AND (document.body_record_ordinal,document.body_record_count)
                          IS DISTINCT FROM (desired.record_ordinal,desired.record_count)
                    ORDER BY document.document_id
                    FOR UPDATE OF document NOWAIT
               )
               UPDATE canonical_documents document
                  SET body_record_ordinal=changed.record_ordinal,
                      body_record_count=changed.record_count
                 FROM changed
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND document.document_id=changed.document_id""",
            (candidate.tenant_id, candidate.source_id, candidate.native_parent_id,
             candidate.tenant_id, candidate.source_id),
        )
        return changed.rowcount

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
                self._publish_body_locators(connection, candidate, ())
                old_manifest, old_parts = self._old_references(
                    connection,
                    candidate,
                )
                old_document = connection.execute(
                    """SELECT logical_document_id,first_occurred_at,
                              last_occurred_at
                         FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND native_parent_id=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.native_parent_id,
                    ),
                ).fetchone()
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
                if old_document is not None:
                    self._queue_parquet_scan(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        ranges=((
                            old_document["first_occurred_at"],
                            old_document["last_occurred_at"],
                        ),),
                        reason="forget",
                        logical_document_id=old_document["logical_document_id"],
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
        try:
            if upload is None:
                return self._commit_empty(candidate)
            status = self._commit(candidate, upload)
        except psycopg.errors.LockNotAvailable:
            # _commit's transaction has already rolled back and released its
            # queue lock. Retain the queue for a fresh attempt; contention must
            # not penalize a newer ingest generation or abort sibling work.
            if upload is not None:
                self._schedule_cleanup(upload.cleanup_references)
            return "stale"
        except Exception:
            if upload is not None:
                self._schedule_cleanup(upload.cleanup_references)
            raise
        finally:
            _close_body_locators(upload)
        if status == "stale":
            self._schedule_cleanup(upload.cleanup_references)
        return status

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 25,
        max_batches: int = 10,
        upload_concurrency: int = 2,
        quiet_seconds: float = 0.0,
        max_wait_seconds: float = 0.0,
        cleanup_concurrency: int | None = None,
    ) -> dict[str, int | str]:
        if cleanup_concurrency is None:
            cleanup_concurrency = upload_concurrency
        if (
            isinstance(cleanup_concurrency, bool)
            or not isinstance(cleanup_concurrency, int)
            or not 1 <= cleanup_concurrency <= 64
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        if (
            isinstance(quiet_seconds, bool)
            or not isinstance(quiet_seconds, (int, float))
            or not 0 <= quiet_seconds <= 3_600
            or isinstance(max_wait_seconds, bool)
            or not isinstance(max_wait_seconds, (int, float))
            or not 0 <= max_wait_seconds <= 86_400
            or (max_wait_seconds and max_wait_seconds < quiet_seconds)
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
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
        repaired = failed = 0
        old_objects_deleted = cleanup_failures = source_races = pruned = 0
        cleanup_completed = cleanup_pending = 0
        # Object deletes are S3 round trips, not database work: measured at
        # ~8 s each in production when serialized, they held one cycle for
        # 25 minutes. Fan them out independently of the upload budget.
        cleanup = self.drain_cleanup(
            tenant_id=tenant_id,
            limit=5_000,
            concurrency=cleanup_concurrency,
        )
        old_objects_deleted += int(cleanup["deleted"])
        cleanup_failures += int(cleanup["failures"])
        cleanup_completed += int(cleanup["completed"])
        cleanup_pending = int(cleanup["pending"])
        for _ in range(max_batches):
            candidates = self._pending(
                tenant_id=tenant_id,
                limit=batch_size,
                quiet_seconds=float(quiet_seconds),
                max_wait_seconds=float(max_wait_seconds),
            )
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
            failed_shards: list[tuple[list[tuple[int, LogicalGroupCandidate]], Exception]] = []
            skipped: set[int] = set()
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
                            failed_shards.append((shard, error))
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
                    concurrency=cleanup_concurrency,
                )
                raise
            # A failing group must not poison its batch, let alone the
            # worker: retry the members of a failed multi-group shard one by
            # one so only the guilty group is backed off; everything else in
            # the batch commits normally this cycle.
            for shard, error in failed_shards:
                if len(shard) == 1:
                    (index, candidate), = shard
                    skipped.add(index)
                    self._mark_failed(candidate, error)
                    continue
                for index, candidate in shard:
                    try:
                        (upload,) = self._prepare_batch_and_upload((candidate,))
                    except Exception as single_error:
                        skipped.add(index)
                        self._mark_failed(candidate, single_error)
                        continue
                    uploads[index] = upload
                    if upload is not None:
                        successful.append(upload)
            failed += len(skipped)
            statuses: list[str | None] = [None] * len(candidates)
            failures: list[Exception] = []
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
                    if index not in skipped
                ]
                for index, future in commit_futures:
                    try:
                        statuses[index] = future.result()
                    except Exception as error:
                        failures.append(error)
            if failures:
                self.drain_cleanup(
                    tenant_id=tenant_id,
                    limit=5_000,
                    concurrency=cleanup_concurrency,
                )
                raise failures[0]
            for index, (upload, status) in enumerate(
                zip(uploads, statuses, strict=True)
            ):
                if index in skipped:
                    continue
                if status == "stale":
                    source_races += 1
                    continue
                if status == "adopted":
                    continue
                if status == "repaired":
                    repaired += 1
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
                concurrency=cleanup_concurrency,
            )
            old_objects_deleted += int(cleanup["deleted"])
            cleanup_failures += int(cleanup["failures"])
            cleanup_completed += int(cleanup["completed"])
            cleanup_pending = int(cleanup["pending"])
        with self.store.connect() as connection:
            counts = connection.execute(
                """SELECT count(*) AS queued,
                          count(*) FILTER (
                              WHERE %s::float8>0
                                AND reason NOT IN ('forget','backfill')
                                AND changed_at
                                    >= clock_timestamp()-%s*interval '1 second'
                                AND NOT (
                                    %s::float8>0
                                    AND first_queued_at
                                        < clock_timestamp()-%s*interval '1 second'
                                )
                          ) AS waiting,
                          count(*) FILTER (
                              WHERE attempts>=%s
                          ) AS quarantined,
                          count(*) FILTER (
                              WHERE attempts<%s
                                AND next_attempt_at IS NOT NULL
                                AND next_attempt_at>clock_timestamp()
                          ) AS backoff
                     FROM canonical_evidence_document_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s)""",
                (
                    float(quiet_seconds), float(quiet_seconds),
                    float(max_wait_seconds), float(max_wait_seconds),
                    MAX_LOGICAL_ATTEMPTS, MAX_LOGICAL_ATTEMPTS,
                    tenant_id, tenant_id,
                ),
            ).fetchone()
        waiting = int(counts["waiting"])
        quarantined = int(counts["quarantined"])
        backoff = int(counts["backoff"])
        pending = max(
            0, int(counts["queued"]) - waiting - quarantined - backoff
        )
        return {
            "status": "complete" if int(pending) == 0 else "pending",
            "documents": documents,
            "repaired": repaired,
            "records": records,
            "receipts": receipts,
            "objects": objects,
            "bytes_uploaded": bytes_uploaded,
            "batches": batches,
            "old_objects_deleted": old_objects_deleted,
            "cleanup_completed": cleanup_completed,
            "cleanup_failures": cleanup_failures,
            "cleanup_pending": cleanup_pending,
            "pending": int(pending),
            "waiting": waiting,
            "failed": failed,
            "backoff": backoff,
            "quarantined": quarantined,
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
                    # Whole-parent deletion cannot discard the only bodies of
                    # surviving events. Serialize this check with retirement's
                    # FOR SHARE fence before inspecting any survivor.
                    try:
                        connection.execute(
                            """SELECT logical_document_id FROM canonical_evidence_documents
                               WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s
                               FOR UPDATE NOWAIT""",
                            (tenant_id, source_id, parent_id),
                        ).fetchall()
                    except psycopg.errors.LockNotAvailable:
                        raise LogicalEvidenceError("logical_evidence_parent_busy") from None
                    survivor = connection.execute(
                        """SELECT 1 FROM canonical_documents document
                           JOIN canonical_events event USING(tenant_id,source_id,event_id)
                           WHERE document.tenant_id=%s AND document.source_id=%s
                             AND COALESCE(event.native_parent_id,event.native_id)=%s
                             AND document.is_current AND document.deleted_at IS NULL
                             AND NOT (document.native_id=ANY(%s))
                             AND EXISTS (SELECT 1 FROM canonical_chunks chunk
                                 WHERE chunk.tenant_id=document.tenant_id
                                   AND chunk.source_id=document.source_id
                                   AND chunk.document_id=document.document_id
                                   AND chunk.deleted_at IS NULL AND chunk.text_redacted=''
                                   AND chunk.text_sha256<>%s)
                           LIMIT 1""",
                        (tenant_id, source_id, parent_id, native_ids, hashlib.sha256(b"").hexdigest()),
                    ).fetchone()
                    if survivor:
                        raise LogicalEvidenceError("logical_evidence_survivor_body_required")
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
                # H3-a: the passages about to cascade away with their evidence
                # documents become search tombstones, and every month they
                # spanned is queued for the Lance writer as ``forget``.
                doomed_passages = connection.execute(
                    """SELECT passage.passage_id,passage.first_occurred_at,
                              passage.last_occurred_at
                         FROM canonical_passages passage
                         JOIN canonical_evidence_documents document
                           USING(tenant_id,source_id,logical_document_id)
                        WHERE document.tenant_id=%s AND document.source_id=%s
                          AND document.native_parent_id=ANY(%s)""",
                    (
                        tenant_id,
                        source_id,
                        [row["native_parent_id"] for row in parents],
                    ),
                ).fetchall()
                record_passage_deletions(
                    connection,
                    tenant_id=tenant_id,
                    source_id=source_id,
                    passages=doomed_passages,
                    reason="forget",
                )
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
