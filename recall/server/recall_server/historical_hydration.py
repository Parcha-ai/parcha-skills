"""Resolve exact historical bundles with SQL fallback and deletion fences."""

from __future__ import annotations

import hashlib
import time

from .chunk_bodies import ChunkBodyError
from .evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceBundle,
    MAX_EVIDENCE_TEXT_BYTES,
)


def hydrate_historical_rows(
    store, archive, rows, *, tenant_id, source_ids, deadline_at
):
    """Read exact immutable revisions, then recheck catalog and deletion state."""
    if not rows:
        return

    def snapshot():
        with store.connect() as c:
            return store._execute_bounded(
                c,
                """SELECT evidence.*,document.native_id,
                document.revision AS canonical_revision,document.text_sha256 AS canonical_text_sha256,
                COALESCE(event.native_parent_id,event.native_id) AS native_parent_id
                FROM canonical_evidence_objects evidence
                JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                JOIN canonical_events event USING(tenant_id,source_id,event_id)
                WHERE evidence.tenant_id=%s AND evidence.source_id=ANY(%s)
                  AND evidence.document_id=ANY(%s) AND document.deleted_at IS NULL
                  AND NOT document.is_current
                  AND NOT EXISTS (SELECT 1 FROM canonical_events later
                    WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
                      AND later.native_id=document.native_id AND later.revision>document.revision
                      AND later.is_tombstone)
                ORDER BY evidence.source_id,evidence.document_id""",
                (tenant_id, list(source_ids), [key[1] for key in needed]),
                deadline_at,
            ).fetchall()

    def authorized_chunks():
        with store.connect() as c:
            return store._execute_bounded(
                c,
                """SELECT chunk.source_id,chunk.document_id,chunk.ordinal,chunk.receipt,chunk.text_sha256
                FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=ANY(%s)
                  AND chunk.receipt=ANY(%s) AND chunk.deleted_at IS NULL
                  AND document.deleted_at IS NULL AND NOT document.is_current
                  AND NOT EXISTS (SELECT 1 FROM canonical_events later
                    WHERE later.tenant_id=document.tenant_id AND later.source_id=document.source_id
                      AND later.native_id=document.native_id AND later.revision>document.revision AND later.is_tombstone)
                ORDER BY chunk.source_id,chunk.document_id,chunk.ordinal""",
                (tenant_id, list(source_ids), [row["receipt"] for row in rows]),
                deadline_at,
            ).fetchall()

    fields = ("source_id", "document_id", "ordinal", "receipt", "text_sha256")
    expected = sorted(tuple(row[key] for key in fields) for row in rows)
    if (
        sorted(tuple(row[key] for key in fields) for row in authorized_chunks())
        != expected
    ):
        raise ChunkBodyError("archived_chunk_body_unavailable")
    needed = {
        (row["source_id"], row["document_id"])
        for row in rows
        if not isinstance(row["text_redacted"], str)
        or hashlib.sha256(row["text_redacted"].encode()).hexdigest()
        != row["text_sha256"]
    }
    before = snapshot() if needed else []
    texts = {}
    for reference in before:
        if deadline_at is not None and time.monotonic() >= deadline_at:
            from .db import SearchDeadlineExceeded

            raise SearchDeadlineExceeded()
        if reference["size_bytes"] > MAX_EVIDENCE_TEXT_BYTES + 1_000_000:
            raise ChunkBodyError("archived_chunk_body_unavailable")
        contract = CanonicalEvidenceProjector._reference(reference)
        bounded = getattr(archive, "read_raw_bounded", None)
        try:
            payload = (
                bounded(contract, deadline_at=deadline_at)
                if callable(bounded) and deadline_at is not None
                else archive.read_raw(contract)
            )
            bundle = EvidenceBundle.decode(payload)
        except Exception as error:
            from .db import SearchDeadlineExceeded

            if isinstance(error, SearchDeadlineExceeded):
                raise
            raise ChunkBodyError("archived_chunk_body_unavailable") from None
        expected_session = hashlib.sha256(
            "\x1f".join(
                (tenant_id, reference["source_id"], reference["native_parent_id"])
            ).encode()
        ).hexdigest()
        if (
            reference["revision"] != reference["canonical_revision"]
            or reference["text_sha256"] != reference["canonical_text_sha256"]
            or bundle.evidence_id != reference["evidence_id"]
            or bundle.revision != reference["revision"]
            or bundle.text_sha256 != reference["text_sha256"]
            or len(bundle.chunks) != reference["receipt_count"]
            or bundle.session_sha256 != expected_session
        ):
            raise ChunkBodyError("archived_chunk_body_unavailable")
        for chunk in bundle.chunks:
            texts[
                (
                    reference["source_id"],
                    reference["document_id"],
                    chunk.ordinal,
                    chunk.receipt,
                )
            ] = chunk.text
    if (snapshot() if needed else []) != before or sorted(
        tuple(row[key] for key in fields) for row in authorized_chunks()
    ) != expected:
        raise ChunkBodyError("archived_chunk_body_unavailable")
    catalogued = {(row["source_id"], row["document_id"]) for row in before}
    for row in rows:
        text = (
            texts.get(
                (row["source_id"], row["document_id"], row["ordinal"], row["receipt"])
            )
            if (row["source_id"], row["document_id"]) in catalogued
            else row["text_redacted"]
        )
        if (
            not isinstance(text, str)
            or hashlib.sha256(text.encode()).hexdigest() != row["text_sha256"]
        ):
            raise ChunkBodyError("archived_chunk_body_unavailable")
        row["text_redacted"] = text
