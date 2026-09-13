"""Repair oversized transcript records whose declared metadata disagrees with the archive.

An oversized record is a canonical event whose ``content`` is a
``recall.oversized-projection.v1`` pointer and whose raw artifact is the gzip
of the full sanitized record. The logical evidence projector restores the full
record by decompressing that artifact and refusing anything whose length or
SHA-256 differs from the declared ``full_size_bytes`` and
``full_content_sha256``. When the declared values are wrong the whole logical
document is poisoned, so this module re-derives the declared metadata from the
archived bytes themselves. The archive object is the authority: it is
content-addressed and verified on read, while the declared values are plain
JSON that the collector computed once and that nothing verifies afterwards.

The repair never trusts an artifact blindly. The archived record must be a
valid gzip member that decodes to a JSON object, must stay under the restore
bound, and, when it carries a source timestamp, that timestamp must agree
with the event's ``occurred_at``; an artifact that belongs to a different
record is reported and left alone instead of being stitched onto the wrong
event.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
from datetime import datetime, timezone
from typing import Any

from .logical_evidence_projection import (
    MAX_RESTORED_RECORD_BYTES,
    OVERSIZED_MEDIA_TYPE,
    mark_logical_evidence_dirty,
)

OVERSIZED_CONTRACT = "recall.oversized-projection.v1"
COUNT_KEYS = (
    "events",
    "consistent",
    "repaired",
    "record_mismatch",
    "unreadable",
)


class OversizedRepairError(RuntimeError):
    """Content-free failure of the repair operation itself."""

    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


def _normalized_timestamp(value: Any) -> str | None:
    """Return the UTC ISO-8601 form the collector stamps on ``occurred_at``."""
    try:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            parsed = datetime.fromtimestamp(float(value), timezone.utc)
        elif isinstance(value, str) and value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        elif isinstance(value, datetime):
            parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        else:
            return None
    except (ValueError, TypeError, OverflowError):
        return None
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reference(row: dict[str, Any]) -> dict[str, Any]:
    created_at = row["raw_created_at"]
    if isinstance(created_at, datetime):
        created_at = _normalized_timestamp(created_at)
    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        "artifact_id": row["raw_artifact_id"],
        "storage_backend": row["raw_storage_backend"],
        "object_key": row["raw_object_key"],
        "content_sha256": row["raw_content_sha256"],
        "size_bytes": row["raw_size_bytes"],
        "media_type": row["raw_media_type"],
        "encryption": row["raw_encryption"],
        "version_id": row["raw_version_id"],
        "created_at": created_at,
    }


def archived_record(raw_archive: Any, row: dict[str, Any]) -> bytes | None:
    """Return the decompressed full record bytes, or ``None`` when unusable."""
    try:
        compressed = raw_archive.read_raw(_reference(row))
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as source:
            payload = source.read(MAX_RESTORED_RECORD_BYTES + 1)
        if not payload or len(payload) > MAX_RESTORED_RECORD_BYTES:
            return None
        if not isinstance(json.loads(payload.decode()), dict):
            return None
    except Exception:
        return None
    return payload


def classify(row: dict[str, Any], payload: bytes | None) -> str:
    """Name the repair outcome for one oversized event without mutating anything."""
    if payload is None:
        return "unreadable"
    record = json.loads(payload.decode())
    stamped = _normalized_timestamp(record.get("timestamp"))
    occurred = _normalized_timestamp(row["occurred_at"])
    if stamped is not None and occurred is not None and stamped != occurred:
        return "record_mismatch"
    content = row["content"]
    if (
        isinstance(content, dict)
        and content.get("full_size_bytes") == len(payload)
        and not isinstance(content.get("full_size_bytes"), bool)
        and content.get("full_content_sha256") == hashlib.sha256(payload).hexdigest()
        and content.get("archive_encoding") == "gzip"
        and content.get("full_record_available") is True
    ):
        return "consistent"
    return "repaired"


def repaired_content(content: dict[str, Any], payload: bytes) -> dict[str, Any]:
    """Return the pointer with every archive-derived field re-derived from bytes."""
    return {
        **content,
        "contract": OVERSIZED_CONTRACT,
        "full_record_available": True,
        "archive_encoding": "gzip",
        "full_size_bytes": len(payload),
        "full_content_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _oversized_rows(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_parent_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """SELECT event.tenant_id,event.source_id,event.event_id,
                  event.native_id,event.occurred_at,
                  event.canonical_redacted->'content' AS content,
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
             JOIN raw_artifacts artifact
               ON artifact.tenant_id=event.tenant_id
              AND artifact.source_id=event.source_id
              AND artifact.artifact_id=event.artifact_id
            WHERE event.tenant_id=%s AND event.source_id=%s
              AND COALESCE(event.native_parent_id,event.native_id)=%s
              AND NOT event.is_tombstone
              AND artifact.media_type=%s
              AND artifact.state='live'
              AND event.canonical_redacted #>> '{content,contract}'=%s
            ORDER BY event.native_id,event.revision""",
        (tenant_id, source_id, native_parent_id, OVERSIZED_MEDIA_TYPE, OVERSIZED_CONTRACT),
    ).fetchall()
    return [dict(row) for row in rows]


def repair_oversized_records(
    store: Any,
    raw_archive: Any,
    *,
    tenant_id: str,
    source_id: str,
    native_parent_id: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Re-derive oversized pointers of one logical group from their archived bytes.

    Returns counts only. With ``dry_run`` nothing is written. Otherwise every
    ``repaired`` event gets its pointer rewritten in one transaction, the
    group's queue row loses its backoff (``attempts``, ``next_attempt_at``,
    ``last_error_code``) and the logical document is queued again. A group
    with an ``unreadable`` or ``record_mismatch`` event is never re-queued:
    projecting it would fail the same way, and the counts say what to look at.
    """
    for name, value in (
        ("tenant_id", tenant_id),
        ("source_id", source_id),
        ("native_parent_id", native_parent_id),
    ):
        if not isinstance(value, str) or not value:
            raise OversizedRepairError(f"repair_{name}_invalid")
    if raw_archive is None:
        raise OversizedRepairError("repair_archive_unavailable")

    with store.connect() as connection:
        rows = _oversized_rows(
            connection,
            tenant_id=tenant_id,
            source_id=source_id,
            native_parent_id=native_parent_id,
        )

    counts = {key: 0 for key in COUNT_KEYS}
    counts["events"] = len(rows)
    updates: list[tuple[str, dict[str, Any]]] = []
    native_ids: list[str] = []
    for row in rows:
        payload = archived_record(raw_archive, row)
        outcome = classify(row, payload)
        counts[outcome] += 1
        if outcome == "repaired":
            updates.append((row["event_id"], repaired_content(row["content"], payload)))
            native_ids.append(row["native_id"])

    blocked = counts["unreadable"] + counts["record_mismatch"] > 0
    result = {
        **counts,
        "dry_run": dry_run,
        "written": 0,
        "requeued": False,
    }
    if dry_run or blocked or not rows:
        return result

    with store.connect() as connection:
        with connection.transaction():
            written = 0
            for event_id, content in updates:
                outcome = connection.execute(
                    """UPDATE canonical_events
                          SET canonical_redacted=jsonb_set(
                              canonical_redacted,'{content}',%s::jsonb,true
                          )
                        WHERE tenant_id=%s AND source_id=%s AND event_id=%s""",
                    (json.dumps(content), tenant_id, source_id, event_id),
                )
                written += max(0, outcome.rowcount)
            mark_logical_evidence_dirty(
                connection,
                tenant_id=tenant_id,
                source_id=source_id,
                native_ids=native_ids or [row["native_id"] for row in rows],
                reason="ingest",
            )
            connection.execute(
                """UPDATE canonical_evidence_document_queue
                      SET attempts=0,next_attempt_at=NULL,last_error_code=NULL
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (tenant_id, source_id, native_parent_id),
            )
    result["written"] = written
    result["requeued"] = True
    return result
