"""Recover exact chunks from existing shared logical evidence, without new copies."""

from __future__ import annotations

import hashlib
from io import BytesIO
import math
import time
from typing import Any

import orjson

from .archive import ArchiveDeadlineExceeded
from .canonical_text import MAX_CANONICAL_TEXT_BYTES, canonical_text_chunks
from .db import SearchDeadlineExceeded
from .evidence_projection import CanonicalEvidenceProjector, DOCUMENT_ID_RE
from .logical_evidence import IDENTITY_RE, LogicalEvidenceProjectionStore, _receipt
from .passage_projection import decode_logical_record

MAX_DOCUMENTS = 100
MAX_READ_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024 * 1024
_EXCLUDED_TYPES = frozenset({"file-history-snapshot", "queue-operation", "token_count", "turn_context"})


class ChunkBodyError(ValueError):
    """Sanitized failure; corrupt evidence must never silently fall back."""


_CATALOG_SQL = """
    SELECT document.tenant_id,document.source_id,document.document_id,
           document.native_id,document.revision,document.text_sha256,
           document.body_record_ordinal,document.body_record_count,
           event.native_parent_id,event.kind,event.occurred_at,
           artifact.media_type AS raw_media_type,
           ARRAY[event.canonical_redacted->>'type',
                 event.canonical_redacted #>> '{content,type}',
                 event.canonical_redacted #>> '{content,message,type}',
                 event.canonical_redacted #>> '{content,payload,type}',
                 event.canonical_redacted #>> '{message,type}',
                 event.canonical_redacted #>> '{payload,type}'] AS structural_types,
           EXISTS (
               SELECT 1 FROM canonical_evidence_document_queue queued
                WHERE queued.tenant_id=event.tenant_id AND queued.source_id=event.source_id
                  AND queued.native_parent_id=COALESCE(event.native_parent_id,event.native_id)
           ) AS pending,
           to_jsonb(evidence) AS manifest,
           COALESCE(parts.items,'[]'::jsonb) AS parts,
           COALESCE(chunks.items,'[]'::jsonb) AS chunks
      FROM canonical_documents document
      JOIN canonical_events event USING(tenant_id,source_id,event_id)
      JOIN raw_artifacts artifact
        ON artifact.tenant_id=event.tenant_id AND artifact.source_id=event.source_id
       AND artifact.artifact_id=event.artifact_id
      LEFT JOIN canonical_evidence_documents evidence
        ON evidence.tenant_id=event.tenant_id AND evidence.source_id=event.source_id
       AND evidence.native_parent_id=COALESCE(event.native_parent_id,event.native_id)
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(to_jsonb(part) ORDER BY part.part_ordinal) AS items
            FROM canonical_evidence_document_parts part
           WHERE part.tenant_id=evidence.tenant_id AND part.source_id=evidence.source_id
             AND part.logical_document_id=evidence.logical_document_id
             AND part.revision=evidence.revision
      ) parts ON true
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(jsonb_build_object(
                     'ordinal',chunk.ordinal,'receipt',chunk.receipt,
                     'text_sha256',chunk.text_sha256
                 ) ORDER BY chunk.ordinal) AS items
            FROM canonical_chunks chunk
           WHERE chunk.tenant_id=document.tenant_id AND chunk.source_id=document.source_id
             AND chunk.document_id=document.document_id AND chunk.deleted_at IS NULL
      ) chunks ON true
     WHERE document.tenant_id=%s AND document.source_id=ANY(%s)
       AND document.document_id=ANY(%s)
       AND document.is_current AND document.deleted_at IS NULL
       AND NOT event.is_tombstone
       AND NOT EXISTS (
           SELECT 1 FROM canonical_events later
            WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
              AND later.native_id=document.native_id AND later.revision>document.revision
              AND later.is_tombstone
       )
     ORDER BY document.source_id,document.document_id
"""


def _check_deadline(deadline_at: float | None) -> None:
    if deadline_at is not None and time.monotonic() >= deadline_at:
        raise SearchDeadlineExceeded()


def _record_location(row: dict[str, Any]) -> tuple[int, int] | None:
    start, count = row.get("body_record_ordinal"), row.get("body_record_count")
    if start is None and count is None:
        return None
    if type(start) is not int or start < 0 or type(count) is not int or count < 1:
        raise ChunkBodyError("archived_chunk_body_unavailable")
    return start, start + count


def _older_revision_receipts(receipts: tuple[str, ...], row: dict[str, Any]) -> bool:
    """Recognize a complete older event revision, never an arbitrary mismatch."""
    prefix = f"recall://{row['source_id']}/{row['native_id']}?rev="
    if not receipts or not receipts[0].startswith(prefix):
        return False
    revision_text = receipts[0][len(prefix):].partition("#item=")[0]
    try:
        revision = int(revision_text)
    except ValueError:
        return False
    return (
        0 < revision < row["revision"]
        and revision_text == str(revision)
        and all(receipt == f"{prefix}{revision}#item={index}"
                for index, receipt in enumerate(receipts))
        and all(chunk["receipt"] == f"{prefix}{row['revision']}#item={index}"
                for index, chunk in enumerate(row["chunks"]))
    )


def _verified_body(row, segments, location, selected):
    """Verify every byte and boundary, retaining only requested chunk prose."""
    chunks = row["chunks"]
    if (not chunks or [c["ordinal"] for c in chunks] != list(range(len(chunks)))
            or not segments or segments[0].segment_count != len(segments)):
        raise ChunkBodyError("archived_chunk_body_unavailable")
    if location is not None and (
        segments[0].ordinal != location[0] or len(segments) != location[1] - location[0]
    ):
        raise ChunkBodyError("archived_chunk_body_unavailable")
    for index, segment in enumerate(segments):
        if (segment.segment_ordinal != index or segment.segment_count != len(segments)
                or segment.ordinal != segments[0].ordinal + index
                or segment.event_kind != segments[0].event_kind
                or (index > 0 and segment.receipts)):
            raise ChunkBodyError("archived_chunk_body_unavailable")
    if list(segments[0].receipts) != [c["receipt"] for c in chunks]:
        if location is None and row["pending"] and _older_revision_receipts(segments[0].receipts, row):
            return None
        raise ChunkBodyError("archived_chunk_body_unavailable")
    if segments[0].event_kind != row["kind"]:
        raise ChunkBodyError("archived_chunk_body_unavailable")
    text = "".join(segment.text for segment in segments)
    if hashlib.sha256(text.encode()).hexdigest() != row["text_sha256"]:
        raise ChunkBodyError("archived_chunk_body_unavailable")
    pieces = [text] if len(chunks) == 1 else canonical_text_chunks(text)
    if (len(pieces) != len(chunks)
            or any(hashlib.sha256(piece.encode()).hexdigest() != chunk["text_sha256"]
                   for piece, chunk in zip(pieces, chunks))):
        if location is not None or len(chunks) == 1:
            raise ChunkBodyError("archived_chunk_body_unavailable")
        return None
    if selected is not None and not set(selected) <= set(range(len(chunks))):
        raise ChunkBodyError("archived_chunk_body_unavailable")
    return [dict(ordinal=chunk["ordinal"], receipt=chunk["receipt"], text_redacted=piece)
            for chunk, piece in zip(chunks, pieces)
            if selected is None or chunk["ordinal"] in selected]


class _VerifiedArchive:
    def __init__(self, archive: Any, deadline_at: float | None):
        self.archive, self.deadline_at = archive, deadline_at

    def read_raw(self, reference: dict[str, Any]) -> bytes:
        _check_deadline(self.deadline_at)
        try:
            bounded_read = getattr(self.archive, "read_raw_bounded", None)
            if self.deadline_at is not None and callable(bounded_read):
                payload = bounded_read(reference, deadline_at=self.deadline_at)
            else:
                # Filesystem and in-memory archives have no network transport.
                # S3 always exposes read_raw_bounded and fails closed if its
                # separate deadline client was not configured.
                payload = self.archive.read_raw(reference)
        except ArchiveDeadlineExceeded:
            raise SearchDeadlineExceeded() from None
        finally:
            _check_deadline(self.deadline_at)
        if not isinstance(payload, bytes) or len(payload) != reference["size_bytes"]:
            raise ChunkBodyError("archived_chunk_body_unavailable")
        return payload


def read_archived_chunks(
    store: Any,
    archive: Any,
    *,
    tenant_id: str,
    source_ids: tuple[str, ...],
    document_ids: tuple[str, ...],
    deadline_at: float | None = None,
    chunk_ordinals: dict[tuple[str, str], tuple[int, ...]] | None = None,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Return only hash-verified chunks; unsupported documents are omitted.

    Existing logical objects serve multiple events and are read once per group.
    Located events fetch only intersecting immutable parts, independently of
    total parent size. NULL positions retain the bounded transitional scan.
    Every full event is verified, but chunk_ordinals lets context callers keep
    only the prose they will return. Parts and completed events are released
    sequentially; retained result text has its own explicit byte ceiling.
    Queued parent updates do not hide individually unchanged events. A new
    event absent from the older projection, or a proven older event revision,
    is omitted while its parent is pending, for hash-verified caller fallback.
    Excluded structural/oversized events, absent manifests, unlocated parents
    above the transitional scan budget, and historical chunk boundaries retain
    temporary fallback. Located event and retained-result budget excesses fail. Wrong
    content, receipts, incomplete segments, or corrupted objects fail closed.
    The caller supplies fresh source grants; database eligibility and immutable
    part references are checked again after reading. A publication race rejects
    this attempt; retry the whole request against the new catalog snapshot.

    Database work honors deadline_at. S3 reads use a separate no-retry client
    with bounded socket inactivity and checks around each sequential read.
    DNS and continuously arriving bytes can outlast the cooperative budget;
    late results are rejected, and no background I/O survives this call.
    """
    if (
        not isinstance(tenant_id, str) or not tenant_id
        or not isinstance(source_ids, tuple)
        or any(not isinstance(s, str) or not s for s in source_ids)
        or not isinstance(document_ids, tuple) or len(document_ids) > MAX_DOCUMENTS
        or any(not isinstance(d, str) or not DOCUMENT_ID_RE.fullmatch(d) for d in document_ids)
        or (deadline_at is not None and (
            isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float))
            or not math.isfinite(deadline_at)
        ))
    ):
        raise ChunkBodyError("archived_chunk_request_invalid")
    if not source_ids or not document_ids:
        return {}
    if chunk_ordinals is not None and (
        not isinstance(chunk_ordinals, dict)
        or any(not isinstance(key, tuple) or len(key) != 2
               or key[0] not in source_ids or key[1] not in document_ids
               or not isinstance(values, tuple)
               or any(type(value) is not int or value < 0 for value in values)
               for key, values in chunk_ordinals.items())
    ):
        raise ChunkBodyError("archived_chunk_request_invalid")
    params = (tenant_id, list(source_ids), list(document_ids))

    def snapshot():
        _check_deadline(deadline_at)
        with store.connect() as connection:
            rows = store._execute_bounded(connection, _CATALOG_SQL, params, deadline_at).fetchall()
        _check_deadline(deadline_at)
        result = {}
        for row in rows:
            key = row["source_id"], row["document_id"]
            if (row["tenant_id"] != tenant_id or key[0] not in source_ids
                    or key[1] not in document_ids or key in result):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            result[key] = row
        return result

    try:
        before = snapshot()
        groups = {}
        for row in before.values():
            location = _record_location(row)
            if location is not None and row["manifest"] is None:
                raise ChunkBodyError("archived_chunk_body_unavailable")
            if (row["manifest"] is None
                    or row["raw_media_type"] == "application/vnd.recall.oversized-record+gzip"
                    or _EXCLUDED_TYPES.intersection(row["structural_types"])):
                continue
            manifest = row["manifest"]
            key = row["source_id"], manifest["logical_document_id"], manifest["revision"]
            groups.setdefault(key, []).append(row)
        projection = LogicalEvidenceProjectionStore(_VerifiedArchive(archive, deadline_at))
        result, consumed, read_any, retained_bytes = {}, 0, False, 0
        for (source_id, _logical_id, _revision), rows in groups.items():
            manifest, parts = rows[0]["manifest"], rows[0]["parts"]
            if (not parts or len(parts) != manifest["part_count"]
                    or any(r["manifest"] != manifest or r["parts"] != parts for r in rows)):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            sizes = [part["size_bytes"] for part in parts]
            if any(type(size) is not int or size <= 0 for size in sizes):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            locations = {row["native_id"]: _record_location(row) for row in rows}
            full_read = (any(location is None for location in locations.values())
                         and sum(sizes) <= MAX_READ_BYTES - consumed)
            if full_read:
                consumed += sum(sizes)
            else:
                rows = [row for row in rows if locations[row["native_id"]] is not None]
                if not rows:
                    continue
            # Check the complete catalog topology, but fetch only the immutable
            # parts intersecting requested record positions. Parent growth is
            # not a body-read eligibility condition.
            next_ordinal = 0
            for ordinal, part in enumerate(parts):
                if (part["part_ordinal"] != ordinal
                        or part["first_record_ordinal"] != next_ordinal
                        or type(part["last_record_ordinal"]) is not int
                        or part["last_record_ordinal"] < next_ordinal
                        or part["logical_document_id"] != manifest["logical_document_id"]
                        or part["revision"] != manifest["revision"]
                        or part["tenant_id"] != tenant_id or part["source_id"] != source_id):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                next_ordinal = part["last_record_ordinal"] + 1
            if (next_ordinal != manifest["record_count"]
                    or any(location is not None and location[1] > next_ordinal
                           for location in locations.values())):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            wanted = {row["native_id"]: row for row in rows}
            if len(wanted) != len(rows):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            records = {native: [] for native in wanted}
            completed, buffered_bytes, active_native = set(), {}, None
            document_digest = hashlib.sha256()
            for part in parts:
                if not full_read and not any(
                    location is not None
                    and location[0] <= part["last_record_ordinal"]
                    and location[1] > part["first_record_ordinal"]
                    for native, location in locations.items() if native in wanted
                ):
                    continue
                if part["size_bytes"] > MAX_READ_BYTES:
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                expected_ordinal = part["first_record_ordinal"]
                payload = projection.read_part(
                    CanonicalEvidenceProjector._reference(part), tenant_id=tenant_id, source_id=source_id,
                )
                read_any = True
                document_digest.update(payload)
                if not payload.endswith(b"\n"):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                receipts_in_part = 0
                for line in BytesIO(payload):
                    _check_deadline(deadline_at)
                    # The immutable part and whole-document hashes cover every
                    # byte. Check routing metadata for all records, but avoid
                    # re-encoding large unrelated bodies into canonical JSON.
                    value = orjson.loads(line)
                    if (
                        not isinstance(value, dict)
                        or type(value.get("ordinal")) is not int
                        or value["ordinal"] != expected_ordinal
                        or not isinstance(value.get("event_native_id"), str)
                        or not IDENTITY_RE.fullmatch(value["event_native_id"])
                        or not isinstance(value.get("receipts"), list)
                    ):
                        raise ChunkBodyError("archived_chunk_body_unavailable")
                    for receipt in value["receipts"]:
                        _receipt(receipt, source_id)
                    if len(set(value["receipts"])) != len(value["receipts"]):
                        raise ChunkBodyError("archived_chunk_body_unavailable")
                    expected_ordinal += 1
                    receipts_in_part += len(value["receipts"])
                    if active_native is not None and value["event_native_id"] != active_native:
                        raise ChunkBodyError("archived_chunk_body_unavailable")
                    if value["event_native_id"] in records:
                        record = decode_logical_record(line, source_id=source_id)
                        native = record.event_native_id
                        if native in completed:
                            raise ChunkBodyError("archived_chunk_body_unavailable")
                        segments = records[native]
                        segments.append(record)
                        active_native = native
                        buffered_bytes[native] = buffered_bytes.get(native, 0) + len(record.text.encode())
                        if locations[native] is not None and buffered_bytes[native] > MAX_CANONICAL_TEXT_BYTES:
                            raise ChunkBodyError("archived_chunk_read_budget_exceeded")
                        if len(segments) == segments[0].segment_count:
                            row = wanted[native]
                            key = (source_id, row["document_id"])
                            selected = None if chunk_ordinals is None else chunk_ordinals.get(key, ())
                            chunks = _verified_body(row, segments, locations[native], selected)
                            if chunks is not None:
                                retained_bytes += sum(len(chunk["text_redacted"].encode()) for chunk in chunks)
                                if retained_bytes > MAX_RESULT_BYTES:
                                    raise ChunkBodyError("archived_chunk_read_budget_exceeded")
                                result[key] = chunks
                            completed.add(native)
                            active_native = None
                            segments.clear()
                            del record
                if (expected_ordinal - 1 != part["last_record_ordinal"]
                        or receipts_in_part != part["receipt_count"]):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                del payload, line, value
            if full_read and document_digest.hexdigest() != manifest["document_content_sha256"]:
                raise ChunkBodyError("archived_chunk_body_unavailable")
            for native, row in wanted.items():
                if native not in completed and (
                    records[native] or not row["pending"] or locations[native] is not None
                ):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
        if read_any and snapshot() != before:
            raise ChunkBodyError("archived_chunk_body_unavailable")
        _check_deadline(deadline_at)
        return result
    except SearchDeadlineExceeded:
        raise
    except ChunkBodyError:
        raise
    except Exception:
        raise ChunkBodyError("archived_chunk_body_unavailable") from None
