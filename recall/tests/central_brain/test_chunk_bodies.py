"""Exact chunks from existing shared logical parts, without new objects."""
from contextlib import contextmanager
import copy
import hashlib
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks
from recall_server.canonical_text import canonical_text_chunks
from recall_server.db import SearchDeadlineExceeded
from recall_server.logical_evidence import LogicalEvidenceRecord, PART_MEDIA_TYPE
from recall_server.passage_projection import decode_logical_record


def digest(value):
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).hexdigest()


class Store:
    def __init__(self, rows):
        self.snapshots, self.calls, self.deadlines = [rows], [], []

    @contextmanager
    def connect(self):
        yield self

    def _execute_bounded(self, connection, sql, params, deadline_at):
        self.calls.append((sql, params))
        self.deadlines.append(deadline_at)
        return self

    def fetchall(self):
        return copy.deepcopy(self.snapshots[min(len(self.calls) - 1, len(self.snapshots) - 1)])


class Archive:
    def __init__(self, payloads):
        self.payloads, self.calls = payloads, []

    def read_raw(self, reference):
        self.calls.append(reference)
        value = self.payloads[reference["object_key"]]
        if isinstance(value, Exception):
            raise value
        return value


class ChunkBodyTests(unittest.TestCase):
    def document(self, native, text, *, source="source", pieces=None):
        return dict(
            tenant_id="tenant", source_id=source, document_id="doc_" + digest(source + native)[:32],
            native_id=native, native_parent_id="session", revision=2, kind="message",
            occurred_at="2026-09-20T00:00:00Z", text_sha256=digest(text),
            raw_media_type="application/json", structural_types=[], pending=False,
            chunks=[dict(ordinal=i, receipt=f"recall://{source}/{native}?rev=2#item={i}",
                         text_sha256=digest(piece)) for i, piece in enumerate([text] if pieces is None else pieces)],
        )

    def record(self, row, text, ordinal, **kwargs):
        return LogicalEvidenceRecord(**(dict(
            ordinal=ordinal, event_native_id=row["native_id"], event_kind=row["kind"],
            occurred_at=row["occurred_at"], roles=("assistant",),
            receipts=tuple(c["receipt"] for c in row["chunks"]),
            segment_ordinal=0, segment_count=1, text=text,
        ) | kwargs))

    def fixture(self, rows, records):
        payload = b"".join(r.encode(source_id=rows[0]["source_id"]) for r in records)
        part = dict(
            tenant_id="tenant", source_id=rows[0]["source_id"], artifact_id="art_" + "a" * 32,
            storage_backend="s3", object_key="objects/aa/" + digest(payload),
            content_sha256=digest(payload), size_bytes=len(payload), media_type=PART_MEDIA_TYPE,
            encryption="sse-s3", version_id="version", created_at="2026-09-20T00:00:00Z",
            part_ordinal=0, first_record_ordinal=0, last_record_ordinal=len(records) - 1,
            receipt_count=sum(len(r.receipts) for r in records),
        )
        for row in rows:
            row.update(manifest=dict(logical_document_id="ldoc_" + "a" * 32, revision=3,
                                     part_count=1, record_count=len(records),
                                     document_content_sha256=digest(payload),
                                     manifest_content_sha256="b" * 64), parts=[part])
        return Store(rows), Archive({part["object_key"]: payload})

    def read(self, store, archive, **overrides):
        rows = store.snapshots[0]
        args = dict(tenant_id="tenant", source_ids=tuple(dict.fromkeys(r["source_id"] for r in rows)),
                    document_ids=tuple(r["document_id"] for r in rows))
        return read_archived_chunks(store, archive, **(args | overrides))

    def test_shared_part_read_once_and_exact_json_or_text(self):
        texts = ('{"role":"user","text":"🐢"}', "literal whitespace  \n")
        rows = [self.document(f"native{i}", text) for i, text in enumerate(texts)]
        store, archive = self.fixture(rows, [self.record(r, text, i) for i, (r, text) in enumerate(zip(rows, texts))])
        result = self.read(store, archive)
        self.assertEqual(len(archive.calls), 1)
        self.assertEqual([result[(r["source_id"], r["document_id"])][0]["text_redacted"] for r in rows], list(texts))
        self.assertEqual(len(store.calls), 2)
        self.assertNotIn("chunk.text_redacted", store.calls[0][0])
        self.assertNotIn("canonical_evidence_objects", store.calls[0][0])

    def test_unrequested_large_content_skips_full_record_decode(self):
        wanted = self.document("wanted", "wanted body")
        large = '{"content":{"text":"' + "unrelated " * 20_000 + '"}}'
        unrelated = self.document("unrelated", large)
        store, archive = self.fixture([wanted], [
            self.record(unrelated, large, 0), self.record(wanted, "wanted body", 1),
        ])
        with patch("recall_server.chunk_bodies.decode_logical_record", wraps=decode_logical_record) as decode:
            result = self.read(store, archive)
        self.assertEqual(result[("source", wanted["document_id"])][0]["text_redacted"], "wanted body")
        self.assertEqual(decode.call_count, 1)
        self.assertEqual(orjson.loads(decode.call_args.args[0])["event_native_id"], "wanted")

    def test_requested_malformed_record_remains_rejected_with_valid_checksums(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        part = row["parts"][0]
        value = orjson.loads(archive.payloads[part["object_key"]])
        value["roles"] = [False]
        payload = orjson.dumps(value, option=orjson.OPT_SORT_KEYS) + b"\n"
        part.update(content_sha256=digest(payload), size_bytes=len(payload))
        row["manifest"]["document_content_sha256"] = digest(payload)
        archive.payloads[part["object_key"]] = payload
        with self.assertRaisesRegex(ChunkBodyError, "^archived_chunk_body_unavailable$"):
            self.read(store, archive)

    def test_unrequested_bad_order_or_receipt_scope_remains_rejected(self):
        for mutation in ({"ordinal": False}, {"ordinal": 99},
                         {"event_native_id": []}, {"receipts": "invalid"},
                         {"receipts": ["recall://other/native?rev=2#item=0"]}):
            wanted, other = self.document("wanted", "body"), self.document("other", "other")
            store, archive = self.fixture([wanted], [
                self.record(other, "other", 0), self.record(wanted, "body", 1),
            ])
            part = wanted["parts"][0]
            first, second = archive.payloads[part["object_key"]].splitlines(keepends=True)
            payload = orjson.dumps(orjson.loads(first) | mutation, option=orjson.OPT_SORT_KEYS) + b"\n" + second
            part.update(content_sha256=digest(payload), size_bytes=len(payload))
            wanted["manifest"]["document_content_sha256"] = digest(payload)
            archive.payloads[part["object_key"]] = payload
            with self.subTest(mutation=mutation), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_denied_unprojected_unsupported_do_no_archive_io(self):
        for mutation in ({"manifest": None},
                         {"raw_media_type": "application/vnd.recall.oversized-record+gzip"},
                         {"structural_types": ["token_count"]}):
            row = self.document("native", "body")
            store, archive = self.fixture([row], [self.record(row, "body", 0)])
            row.update(mutation)
            self.assertEqual(self.read(store, archive), {})
            self.assertEqual(archive.calls, [])
        store.calls = []
        self.assertEqual(self.read(store, archive, source_ids=()), {})
        self.assertEqual(store.calls, [])

    def test_historical_multichunk_fallback_but_wrong_text_errors(self):
        row = self.document("native", "abcdef", pieces=["abc", "def"])
        store, archive = self.fixture([row], [self.record(row, "abcdef", 0)])
        self.assertEqual(self.read(store, archive), {})
        row["text_sha256"] = digest("different")
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_pending_append_serves_unchanged_document_and_omits_new_document(self):
        old, new = self.document("old", "exact old body"), self.document("new", "new body")
        store, archive = self.fixture([old, new], [self.record(old, "exact old body", 0)])
        old["pending"] = new["pending"] = True
        result = self.read(store, archive)
        self.assertEqual(list(result), [("source", old["document_id"])])
        self.assertEqual(result[("source", old["document_id"])][0]["text_redacted"], "exact old body")
        self.assertEqual(len(archive.calls), 1)
        self.assertEqual(len(store.calls), 2)

    def test_pending_revision_omits_old_body_but_keeps_unchanged_sibling(self):
        revised, sibling = self.document("revised", "old body"), self.document("sibling", "same body")
        records = [self.record(revised, "old body", 0), self.record(sibling, "same body", 1)]
        store, archive = self.fixture([revised, sibling], records)
        revised.update(revision=3, text_sha256=digest("new body"), pending=True)
        revised["chunks"] = [dict(ordinal=0, receipt="recall://source/revised?rev=3#item=0", text_sha256=digest("new body"))]
        sibling["pending"] = True
        result = self.read(store, archive)
        self.assertEqual(list(result), [("source", sibling["document_id"])])
        self.assertEqual(result[("source", sibling["document_id"])][0]["text_redacted"], "same body")

    def test_pending_same_revision_hash_or_receipt_corruption_is_not_fallback(self):
        for mutation in ("hash", "receipt"):
            row = self.document("native", "body")
            record = self.record(row, "body", 0)
            store, archive = self.fixture([row], [record])
            row["pending"] = True
            if mutation == "hash":
                row["text_sha256"] = digest("changed without revision")
            else:
                row["chunks"][0]["receipt"] = "recall://source/other?rev=2#item=0"
            with self.subTest(mutation=mutation), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_pending_older_revision_requires_canonical_complete_receipts(self):
        for receipts in (
            ("recall://source/native?rev=0#item=0",),
            ("recall://source/native?rev=01#item=0",),
            ("recall://source/native?rev=3#item=0",),
            ("recall://source/native?rev=1#item=1",),
            ("recall://source/native?rev=1#item=0", "recall://source/native?rev=1#item=2"),
            ("recall://source/other?rev=1#item=0",),
        ):
            row = self.document("native", "current body")
            store, archive = self.fixture([row], [self.record(row, "old body", 0, receipts=receipts)])
            row["pending"] = True
            with self.subTest(receipts=receipts), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_pending_unsupported_and_historical_documents_remain_omitted(self):
        for mutation in (
            {"raw_media_type": "application/vnd.recall.oversized-record+gzip"},
            {"structural_types": ["token_count"]},
            {},
        ):
            row = self.document("native", "abcdef", pieces=["abc", "def"])
            store, archive = self.fixture([row], [self.record(row, "abcdef", 0)])
            row.update(pending=True, **mutation)
            with self.subTest(mutation=mutation):
                self.assertEqual(self.read(store, archive), {})
                self.assertEqual(len(archive.calls), 0 if mutation else 1)

    def test_pending_corrupt_part_and_incomplete_old_segments_fail_closed(self):
        row = self.document("native", "old body")
        first = self.record(row, "old", 0, segment_count=2)
        store, archive = self.fixture([row], [first])
        row.update(revision=3, pending=True, text_sha256=digest("new body"))
        row["chunks"] = [dict(ordinal=0, receipt="recall://source/native?rev=3#item=0", text_sha256=digest("new body"))]
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)
        archive.payloads[row["parts"][0]["object_key"]] = b"corrupt body"
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_pending_publication_race_fails_closed_until_request_retried(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        row["pending"] = True
        published = copy.deepcopy(row)
        published["pending"] = False
        published["manifest"]["revision"] += 1
        store.snapshots = [[row], [published]]
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)
        store.calls = []
        store.snapshots = [[published]]
        self.assertEqual(self.read(store, archive)[("source", row["document_id"])][0]["text_redacted"], "body")

    def test_current_multichunk_boundaries_require_every_hash(self):
        text = "multilingual 🐢 text\n" * 3000
        pieces = canonical_text_chunks(text)
        self.assertGreater(len(pieces), 1)
        row = self.document("native", text, pieces=pieces)
        store, archive = self.fixture([row], [self.record(row, text, 0)])
        chunks = self.read(store, archive)[("source", row["document_id"])]
        self.assertEqual([c["text_redacted"] for c in chunks], pieces)
        row["chunks"][0]["text_sha256"] = "0" * 64
        self.assertEqual(self.read(store, archive), {})

    def test_wrong_receipts_never_become_boundary_fallback(self):
        row = self.document("native", "body")
        record = self.record(row, "body", 0, receipts=("recall://source/other?rev=2#item=0",))
        store, archive = self.fixture([row], [record])
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_current_manifest_missing_record_or_part_errors(self):
        row, other = self.document("native", "body"), self.document("other", "other")
        store, archive = self.fixture([row], [self.record(other, "other", 0)])
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)
        row["parts"] = []
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_complete_segments_reconstruct_and_missing_segment_errors(self):
        row = self.document("native", "leftright")
        first = self.record(row, "left", 0, segment_count=2)
        last = self.record(row, "right", 1, segment_count=2, segment_ordinal=1, receipts=())
        store, archive = self.fixture([row], [first, last])
        self.assertEqual(self.read(store, archive)[("source", row["document_id"])][0]["text_redacted"], "leftright")
        store, archive = self.fixture([row], [first])
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_source_collision_cannot_reuse_another_sources_body(self):
        left, right = self.document("native", "left"), self.document("native", "right", source="other")
        store, archive = self.fixture([left], [self.record(left, "left", 0)])
        right.update(manifest=left["manifest"], parts=left["parts"])
        store.snapshots = [[left, right]]
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_same_native_and_logical_id_in_different_sources_stay_separate(self):
        left, right = self.document("native", "left"), self.document("native", "right", source="other")
        store, archive = self.fixture([left], [self.record(left, "left", 0)])
        _, other_archive = self.fixture([right], [self.record(right, "right", 0)])
        store.snapshots = [[left, right]]
        archive.payloads.update(other_archive.payloads)
        result = self.read(store, archive)
        self.assertEqual(result[("source", left["document_id"])][0]["text_redacted"], "left")
        self.assertEqual(result[("other", right["document_id"])][0]["text_redacted"], "right")
        self.assertEqual(len(archive.calls), 2)

    def test_budget_omits_group_before_object_read(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        row["parts"][0]["size_bytes"] = 65 * 1024 * 1024
        self.assertEqual(self.read(store, archive), {})
        self.assertEqual(archive.calls, [])

    def test_corrupt_missing_part_raises_sanitized_error(self):
        for value in (b"private corrupt body", RuntimeError("private bucket")):
            row = self.document("native", "body")
            store, archive = self.fixture([row], [self.record(row, "body", 0)])
            archive.payloads[row["parts"][0]["object_key"]] = value
            with self.assertRaisesRegex(ChunkBodyError, "^archived_chunk_body_unavailable$"):
                self.read(store, archive)

    def test_valid_outer_checksums_cannot_authorize_invalid_jsonl(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        payload = b'{"private":"invalid record"}\n'
        row["parts"][0].update(content_sha256=digest(payload), size_bytes=len(payload))
        row["manifest"]["document_content_sha256"] = digest(payload)
        archive.payloads[row["parts"][0]["object_key"]] = payload
        with self.assertRaisesRegex(ChunkBodyError, "^archived_chunk_body_unavailable$"):
            self.read(store, archive)

    def test_revocation_revision_change_after_io_fails(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        for after in ([], [row | {"pending": True}]):
            store.calls = []
            store.snapshots = [[row], after]
            with self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_deadline_propagates_and_expiry_is_not_sanitized(self):
        row = self.document("native", "body")
        store, archive = self.fixture([row], [self.record(row, "body", 0)])
        with patch("recall_server.chunk_bodies.time.monotonic", return_value=1):
            self.read(store, archive, deadline_at=10)
        self.assertEqual(store.deadlines, [10, 10])
        with patch("recall_server.chunk_bodies.time.monotonic", return_value=11):
            with self.assertRaises(SearchDeadlineExceeded):
                self.read(store, archive, deadline_at=10)


if __name__ == "__main__":
    unittest.main()
