"""Hydrate authorized current chunk rows with one archive/fallback implementation."""
from __future__ import annotations

import hashlib
import time
from typing import Any


def hydrate_chunk_rows(
    store: Any,
    archive: Any,
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_ids: tuple[str, ...],
    text_key: str = "text_redacted",
    deadline_at: float | None = None,
) -> None:
    if archive is None or not rows:
        return
    from .chunk_bodies import ChunkBodyError, read_archived_chunks
    from .db import SearchDeadlineExceeded

    if deadline_at is not None and time.monotonic() >= deadline_at:
        raise SearchDeadlineExceeded()
    requested = {}
    for row in rows:
        requested.setdefault((row["source_id"], row["document_id"]), set()).add(row["ordinal"])
    archived = read_archived_chunks(
        store,
        archive,
        tenant_id=tenant_id,
        source_ids=source_ids,
        document_ids=tuple(dict.fromkeys(row["document_id"] for row in rows)),
        deadline_at=deadline_at,
        chunk_ordinals={key: tuple(sorted(ordinals)) for key, ordinals in requested.items()},
    )
    by_receipt = {
        chunk["receipt"]: chunk
        for chunks in archived.values()
        for chunk in chunks
    }
    missing = [
        row for row in rows
        if (row["source_id"], row["document_id"]) not in archived
    ]
    fallback = {}
    if missing:
        # Unsupported or not-yet-projected current documents retain verified
        # PostgreSQL text during migration. Object failures never reach here.
        with store.connect() as connection:
            fallback_rows = store._execute_bounded(
                connection,
                """SELECT chunk.receipt,chunk.text_redacted,chunk.text_sha256
                     FROM canonical_chunks chunk
                     JOIN canonical_documents document
                       USING(tenant_id,source_id,document_id)
                    WHERE chunk.tenant_id=%s AND chunk.source_id=ANY(%s)
                      AND chunk.receipt=ANY(%s)
                      AND chunk.deleted_at IS NULL
                      AND document.is_current AND document.deleted_at IS NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM canonical_events later
                           WHERE later.tenant_id=document.tenant_id
                             AND later.source_id=document.source_id
                             AND later.native_id=document.native_id
                             AND later.revision>document.revision
                             AND later.is_tombstone
                      )""",
                (tenant_id, list(source_ids),
                 [row["receipt"] for row in missing]),
                deadline_at,
            ).fetchall()
        for row in fallback_rows:
            if (not isinstance(row["text_redacted"], str)
                or hashlib.sha256(row["text_redacted"].encode()).hexdigest()
                != row["text_sha256"]):
                raise ChunkBodyError("archived_chunk_body_unavailable")
        fallback = {row["receipt"]: row["text_redacted"] for row in fallback_rows}
    if deadline_at is not None and time.monotonic() >= deadline_at:
        raise SearchDeadlineExceeded()
    for row in rows:
        if (row["source_id"], row["document_id"]) in archived:
            chunk = by_receipt.get(row["receipt"])
            if chunk is None or chunk["ordinal"] != row["ordinal"]:
                raise ChunkBodyError("archived_chunk_body_unavailable")
            row[text_key] = chunk["text_redacted"]
        elif row["receipt"] in fallback:
            row[text_key] = fallback[row["receipt"]]
        else:
            raise ChunkBodyError("archived_chunk_body_unavailable")
