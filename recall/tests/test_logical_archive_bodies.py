from __future__ import annotations

import hashlib
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from server.recall_server.canonical_text import canonical_text_chunks
from server.recall_server.logical_archive_bodies import ArchivedBodyLookup
from server.recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore, LogicalEvidenceRecord, PART_MEDIA_TYPE


def sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Archive:
    def __init__(self):
        self.payloads, self.reads = {}, []

    def read_raw(self, reference):
        self.reads.append(reference["object_key"])
        return self.payloads[reference["object_key"]]


class ArchivedBodyLookupTests(unittest.TestCase):
    def setUp(self):
        self.archive = Archive()
        self.projection = LogicalEvidenceProjectionStore(self.archive)
        self.candidate = SimpleNamespace(tenant_id="tenant:test", source_id="codex:test", native_parent_id="parent")
        self.lookup = ArchivedBodyLookup()
        self.addCleanup(self.lookup.close)
        self.receipt = "recall://codex:test/event?rev=1#item=0"

    def record(self, text="body", **values):
        return replace(LogicalEvidenceRecord(
            ordinal=0, event_native_id="event", event_kind="transcript_record",
            occurred_at="2026-09-21T00:00:00Z", roles=("assistant",), receipts=(self.receipt,),
            segment_ordinal=0, segment_count=1, text=text,
        ), **values)

    def load(self, records, *, change_manifest=None):
        parts, digest = [], hashlib.sha256()
        for number, record in enumerate(records):
            payload = record.encode(source_id=self.candidate.source_id)
            digest.update(payload)
            key = str(number)
            self.archive.payloads[key] = payload
            parts.append(dict(
                tenant_id=self.candidate.tenant_id, source_id=self.candidate.source_id,
                part_ordinal=number, first_record_ordinal=number, last_record_ordinal=number,
                logical_document_id="logical", revision=1, size_bytes=len(payload),
                content_sha256=hashlib.sha256(payload).hexdigest(), object_key=key,
                media_type=PART_MEDIA_TYPE, receipt_count=len(record.receipts),
            ))
        manifest = dict(tenant_id=self.candidate.tenant_id, source_id=self.candidate.source_id,
                        native_parent_id="parent", logical_document_id="logical", revision=1,
                        part_count=len(parts), record_count=len(records),
                        receipt_count=sum(len(r.receipts) for r in records), document_content_sha256=digest.hexdigest())
        if change_manifest:
            manifest.update(change_manifest)
        self.lookup.load(self.projection, candidate=self.candidate, manifest=manifest, parts=parts, reference=lambda p: p)

    def row(self, text="body", pieces=None):
        pieces = pieces or canonical_text_chunks(text)
        receipts = [f"recall://codex:test/event?rev=1#item={i}" for i in range(len(pieces))]
        return dict(native_id="event", event_text="", document_text_sha256=sha(text),
                    raw_media_type="application/json", chunk_receipts=receipts, chunk_count=len(pieces),
                    source_chunks=[dict(ordinal=i, text_sha256=sha(piece), size_bytes=0) for i, piece in enumerate(pieces)])

    def test_segments_cross_parts_and_are_read_once(self):
        self.load([self.record("α", segment_count=2), self.record("🧠", ordinal=1, receipts=(), segment_ordinal=1, segment_count=2)])
        row = self.lookup.restore(self.row("α🧠"))
        self.assertEqual(row["event_text"], "α🧠")
        self.assertEqual(row["source_chunks"][0]["size_bytes"], 6)
        self.lookup.restore(self.row("α🧠"))
        self.assertEqual(self.archive.reads, ["0", "1"])

    def test_canonical_multichunk_hashes_restore_exact_bytes(self):
        text = "α 🧠 café\n" * 6000
        row = self.row(text)
        self.assertGreater(row["chunk_count"], 1)
        self.load([self.record(text, receipts=tuple(row["chunk_receipts"]))])
        self.assertEqual(self.lookup.restore(row)["event_text"], text)

    def test_current_receipt_revision_is_required(self):
        self.load([self.record()])
        row = self.row()
        row["chunk_receipts"] = [self.receipt.replace("rev=1", "rev=2")]
        with self.assertRaises(LogicalEvidenceError):
            self.lookup.restore(row)

    def test_document_and_chunk_hashes_are_independently_required(self):
        self.load([self.record()])
        for field in ("document", "chunk"):
            with self.subTest(field=field):
                row = self.row()
                if field == "document":
                    row["document_text_sha256"] = "0" * 64
                else:
                    row["source_chunks"][0]["text_sha256"] = "0" * 64
                with self.assertRaises(LogicalEvidenceError):
                    self.lookup.restore(row)

    def test_historical_boundaries_cannot_be_guessed_after_clear(self):
        row = self.row("body", pieces=["bo", "dy"])
        self.load([self.record(receipts=tuple(row["chunk_receipts"]))])
        with self.assertRaises(LogicalEvidenceError):
            self.lookup.restore(row)

    def test_oversized_excerpt_never_substitutes_full_record(self):
        self.load([self.record()])
        row = self.row()
        row["raw_media_type"] = "application/vnd.recall.oversized-record+gzip"
        with self.assertRaises(LogicalEvidenceError):
            self.lookup.restore(row)

    def test_incomplete_segments_fail(self):
        with self.assertRaises(LogicalEvidenceError):
            self.load([self.record(segment_count=2)])

    def test_duplicate_event_identity_fails(self):
        with self.assertRaises(LogicalEvidenceError):
            self.load([self.record(), self.record(ordinal=1)])

    def test_segment_metadata_mismatch_fails(self):
        with self.assertRaises(LogicalEvidenceError):
            self.load([self.record(segment_count=2), self.record(ordinal=1, receipts=(), segment_ordinal=1, segment_count=2, roles=("user",))])

    def test_whole_document_hash_required(self):
        with self.assertRaises(LogicalEvidenceError):
            self.load([self.record()], change_manifest={"document_content_sha256": "0" * 64})

    def test_body_limit_fails_without_unbounded_assembly(self):
        with patch("server.recall_server.logical_archive_bodies.MAX_BODY_BYTES", 3):
            with self.assertRaises(LogicalEvidenceError):
                self.load([self.record()])

    def test_empty_source_and_private_scratch_cleanup(self):
        self.load([self.record("")])
        self.assertEqual(self.lookup.restore(self.row(""))["event_text"], "")
        path = Path(self.lookup.directory.name)
        self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        self.lookup.close()
        self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
