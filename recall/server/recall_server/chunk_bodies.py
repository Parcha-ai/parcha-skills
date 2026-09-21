"""Recover exact chunks from existing shared logical evidence, without new copies."""

from __future__ import annotations

import hashlib
from io import BytesIO
import math
import time
from typing import Any

import orjson

from .canonical_text import canonical_text_chunks
from .db import SearchDeadlineExceeded
from .evidence_projection import CanonicalEvidenceProjector, DOCUMENT_ID_RE
from .logical_evidence import IDENTITY_RE, LogicalEvidenceProjectionStore, _receipt
from .passage_projection import decode_logical_record

MAX_DOCUMENTS = 100
MAX_READ_BYTES = 64 * 1024 * 1024
_EXCLUDED_TYPES = frozenset({"file-history-snapshot", "queue-operation", "token_count", "turn_context"})


class ChunkBodyError(ValueError):
    """Sanitized failure; corrupt evidence must never silently fall back."""


_CATALOG_SQL = """
    SELECT document.tenant_id,document.source_id,document.document_id,
           document.native_id,document.revision,document.text_sha256,
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


class _VerifiedArchive:
    def __init__(self, archive: Any, deadline_at: float | None):
        self.archive, self.deadline_at = archive, deadline_at

    def read_raw(self, reference: dict[str, Any]) -> bytes:
        _check_deadline(self.deadline_at)
        try:
            payload = self.archive.read_raw(reference)
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
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Return only hash-verified chunks; unsupported documents are omitted.

    Existing logical objects serve multiple events and are read once per group.
    Pending projection, excluded structural/oversized events, absent manifests,
    read-budget overflow, or historical chunk boundaries use temporary caller
    fallback. Wrong content, receipts, incomplete segments, or corrupted objects
    fail closed. The caller supplies fresh source grants; database eligibility
    and immutable part references are checked again after reading.

    Database work honors deadline_at. The archive transport has no per-request
    deadline API: checks around each sequential read reject late results, but
    cannot interrupt an in-flight read. No background I/O survives this call.
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
            if (row["pending"] or row["manifest"] is None
                    or row["raw_media_type"] == "application/vnd.recall.oversized-record+gzip"
                    or _EXCLUDED_TYPES.intersection(row["structural_types"])):
                continue
            manifest = row["manifest"]
            key = row["source_id"], manifest["logical_document_id"], manifest["revision"]
            groups.setdefault(key, []).append(row)
        projection = LogicalEvidenceProjectionStore(_VerifiedArchive(archive, deadline_at))
        result, consumed = {}, 0
        for (source_id, _logical_id, _revision), rows in groups.items():
            manifest, parts = rows[0]["manifest"], rows[0]["parts"]
            if (not parts or len(parts) != manifest["part_count"]
                    or any(r["manifest"] != manifest or r["parts"] != parts for r in rows)):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            sizes = [part["size_bytes"] for part in parts]
            if any(type(size) is not int or size <= 0 for size in sizes):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            if sum(sizes) > MAX_READ_BYTES - consumed:
                continue
            consumed += sum(sizes)
            wanted = {row["native_id"]: row for row in rows}
            if len(wanted) != len(rows):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            records = {native: [] for native in wanted}
            expected_ordinal = 0
            document_digest = hashlib.sha256()
            for ordinal, part in enumerate(parts):
                if (part["part_ordinal"] != ordinal
                        or part["first_record_ordinal"] != expected_ordinal
                        or part["tenant_id"] != tenant_id or part["source_id"] != source_id):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                payload = projection.read_part(
                    CanonicalEvidenceProjector._reference(part), tenant_id=tenant_id, source_id=source_id,
                )
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
                    if value["event_native_id"] in records:
                        record = decode_logical_record(line, source_id=source_id)
                        records[record.event_native_id].append(record)
                if (expected_ordinal - 1 != part["last_record_ordinal"]
                        or receipts_in_part != part["receipt_count"]):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
            if (expected_ordinal != manifest["record_count"]
                    or document_digest.hexdigest() != manifest["document_content_sha256"]):
                raise ChunkBodyError("archived_chunk_body_unavailable")
            for native_id, row in wanted.items():
                segments = records[native_id]
                chunks = row["chunks"]
                if (not segments or not chunks
                        or [c["ordinal"] for c in chunks] != list(range(len(chunks)))
                        or segments[0].segment_count != len(segments)
                        or list(segments[0].receipts) != [c["receipt"] for c in chunks]):
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                for index, segment in enumerate(segments):
                    if (segment.segment_ordinal != index or segment.segment_count != len(segments)
                            or segment.ordinal != segments[0].ordinal + index
                            or segment.event_kind != row["kind"]
                            or (index > 0 and segment.receipts)):
                        raise ChunkBodyError("archived_chunk_body_unavailable")
                text = "".join(segment.text for segment in segments)
                if hashlib.sha256(text.encode()).hexdigest() != row["text_sha256"]:
                    raise ChunkBodyError("archived_chunk_body_unavailable")
                pieces = [text] if len(chunks) == 1 else canonical_text_chunks(text)
                if (len(pieces) != len(chunks)
                        or any(hashlib.sha256(piece.encode()).hexdigest() != chunk["text_sha256"]
                               for piece, chunk in zip(pieces, chunks))):
                    if len(chunks) == 1:
                        raise ChunkBodyError("archived_chunk_body_unavailable")
                    continue
                result[(source_id, row["document_id"])] = [
                    dict(ordinal=chunk["ordinal"], receipt=chunk["receipt"], text_redacted=piece)
                    for chunk, piece in zip(chunks, pieces)
                ]
        if consumed and snapshot() != before:
            raise ChunkBodyError("archived_chunk_body_unavailable")
        _check_deadline(deadline_at)
        return result
    except SearchDeadlineExceeded:
        raise
    except Exception:
        raise ChunkBodyError("archived_chunk_body_unavailable") from None
