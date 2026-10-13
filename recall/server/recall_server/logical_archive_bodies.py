"""Private, attempt-local body lookup for logical reprojection.

Read every pinned parent part once, keeping the index and prose on temporary
files. Only bodies with exact current receipts and hashes may leave this store.
"""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable

from .canonical_text import canonical_text_chunks
from .logical_evidence import LogicalEvidenceError, MAX_PART_BYTES
from .passage_projection import decode_logical_record

MAX_BODY_BYTES = 256 * 1024 * 1024


def _invalid():
    return LogicalEvidenceError("logical_evidence_source_integrity_invalid")


class ArchivedBodyLookup:
    def __init__(self):
        self.directory = tempfile.TemporaryDirectory(prefix="recall-logical-bodies-")
        self.index = None
        self.bodies = None
        try:
            self.index = sqlite3.connect(str(Path(self.directory.name) / "index.sqlite"))
            self.index.execute("PRAGMA cache_size=-2048")
            self.index.execute("PRAGMA temp_store=FILE")
            self.index.execute("CREATE TABLE bodies(native TEXT PRIMARY KEY, receipts TEXT, start INTEGER, size INTEGER)")
            self.bodies = tempfile.TemporaryFile(mode="w+b")
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.index is not None:
            self.index.close()
        if self.bodies is not None:
            self.bodies.close()
        self.directory.cleanup()

    def load(self, projection, *, candidate, manifest, parts, reference,
             checkpoint: Callable[[], None] | None = None):
        if (not manifest or not parts or len(parts) != manifest["part_count"]
                or manifest["tenant_id"] != candidate.tenant_id
                or manifest["source_id"] != candidate.source_id
                or manifest["native_parent_id"] != candidate.native_parent_id):
            raise _invalid()
        ordinal, receipt_count = 0, 0
        digest = hashlib.sha256()
        first = None
        segment_index = 0
        start = 0
        for part_ordinal, part in enumerate(parts):
            if checkpoint is not None:
                checkpoint()
            if (part["part_ordinal"] != part_ordinal
                    or part["first_record_ordinal"] != ordinal
                    or part["tenant_id"] != candidate.tenant_id
                    or part["source_id"] != candidate.source_id
                    or part["logical_document_id"] != manifest["logical_document_id"]
                    or part["revision"] != manifest["revision"]
                    or type(part["size_bytes"]) is not int
                    or not 0 < part["size_bytes"] <= MAX_PART_BYTES):
                raise _invalid()
            payload = projection.read_part(reference(part), tenant_id=candidate.tenant_id, source_id=candidate.source_id)
            if len(payload) != part["size_bytes"] or not payload.endswith(b"\n"):
                raise _invalid()
            digest.update(payload)
            part_receipts = 0
            for line in io.BytesIO(payload):
                if checkpoint is not None:
                    checkpoint()
                record = decode_logical_record(line, source_id=candidate.source_id)
                if record.ordinal != ordinal:
                    raise _invalid()
                ordinal += 1
                if first is None:
                    if record.segment_ordinal != 0 or not record.receipts:
                        raise _invalid()
                    first, segment_index, start = record, 0, self.bodies.tell()
                elif (record.event_native_id != first.event_native_id
                        or record.event_kind != first.event_kind
                        or record.occurred_at != first.occurred_at
                        or record.roles != first.roles
                        or record.actor_links != first.actor_links
                        or record.receipts):
                    raise _invalid()
                if record.segment_ordinal != segment_index or record.segment_count != first.segment_count:
                    raise _invalid()
                part_receipts += len(record.receipts)
                data = record.text.encode()
                if self.bodies.tell() - start + len(data) > MAX_BODY_BYTES:
                    raise _invalid()
                self.bodies.write(data)
                segment_index += 1
                if segment_index == first.segment_count:
                    try:
                        self.index.execute("INSERT INTO bodies VALUES(?,?,?,?)", (
                            first.event_native_id, json.dumps(first.receipts), start, self.bodies.tell() - start,
                        ))
                    except sqlite3.IntegrityError:
                        raise _invalid() from None
                    first = None
            if ordinal - 1 != part["last_record_ordinal"] or part_receipts != part["receipt_count"]:
                raise _invalid()
            receipt_count += part_receipts
        if (first is not None or ordinal != manifest["record_count"]
                or receipt_count != manifest["receipt_count"]
                or digest.hexdigest() != manifest["document_content_sha256"]):
            raise _invalid()
        self.index.commit()

    def restore(self, row: dict[str, Any]) -> dict[str, Any]:
        if row["raw_media_type"] == "application/vnd.recall.oversized-record+gzip":
            raise _invalid()
        found = self.index.execute("SELECT receipts,start,size FROM bodies WHERE native=?", (row["native_id"],)).fetchone()
        if found is None or json.loads(found[0]) != row["chunk_receipts"]:
            raise _invalid()
        self.bodies.seek(found[1])
        payload = self.bodies.read(found[2])
        if len(payload) != found[2] or hashlib.sha256(payload).hexdigest() != row["document_text_sha256"]:
            raise _invalid()
        text = payload.decode()
        chunks = row["source_chunks"]
        if not chunks or row["chunk_count"] != len(chunks):
            raise _invalid()
        pieces = [text] if len(chunks) == 1 else canonical_text_chunks(text)
        if (len(pieces) != len(chunks)
                or any(chunk["ordinal"] != ordinal
                       or hashlib.sha256(piece.encode()).hexdigest() != chunk["text_sha256"]
                       for ordinal, (piece, chunk) in enumerate(zip(pieces, chunks)))):
            raise _invalid()
        return dict(row, event_text=text, source_chunks=[
            dict(chunk, size_bytes=len(piece.encode())) for piece, chunk in zip(pieces, chunks)
        ])
