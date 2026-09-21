"""Stable record positions route exact bodies without reading whole parents."""
import copy
import unittest
from unittest.mock import patch

from tests.central_brain import test_chunk_bodies as fixtures
from tests.central_brain.test_chunk_bodies import Archive, Store, digest
from recall_server.chunk_bodies import ChunkBodyError
from recall_server.canonical_text import canonical_text_chunks


class ChunkLocatorTests(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.ChunkBodyTests()

    def fixture(self, *, located=True):
        row = self.base.document("wanted", "exact body")
        other = self.base.document("other", "unrequested body" * 100)
        records = [self.base.record(other, "unrequested body" * 100, 0),
                   self.base.record(row, "exact body", 1)]
        store, archive = self.base.fixture([row], records)
        template = row["parts"][0]
        parts, payloads = [], {}
        for index, record in enumerate(records):
            payload = record.encode(source_id="source")
            part = dict(template, part_ordinal=index, first_record_ordinal=index,
                        last_record_ordinal=index, size_bytes=len(payload),
                        content_sha256=digest(payload), object_key=str(index),
                        receipt_count=len(record.receipts))
            parts.append(part)
            payloads[str(index)] = payload
        row["parts"] = parts
        row["manifest"]["part_count"] = len(parts)
        if located:
            row.update(body_record_ordinal=1, body_record_count=1)
        return row, Store([row]), Archive(payloads)

    def read(self, store, archive):
        return self.base.read(store, archive)

    def test_locator_reads_only_its_part_even_when_parent_exceeds_budget(self):
        row, store, archive = self.fixture()
        with patch("recall_server.chunk_bodies.MAX_READ_BYTES", row["parts"][1]["size_bytes"]):
            result = self.read(store, archive)
        self.assertEqual(result[("source", row["document_id"])][0]["text_redacted"], "exact body")
        self.assertEqual([ref["object_key"] for ref in archive.calls], ["1"])

    def test_null_locators_preserve_transitional_whole_parent_read(self):
        row, store, archive = self.fixture(located=False)
        result = self.read(store, archive)
        self.assertIn(("source", row["document_id"]), result)
        self.assertEqual(len(archive.calls), 2)

    def test_bad_locator_never_uses_inline_or_unlocated_fallback(self):
        for position, count in ((None, 1), (1, None), (-1, 1), (False, 1), (1, 0), (0, 1), (1, 2), (999, 1)):
            row, store, archive = self.fixture()
            row.update(body_record_ordinal=position, body_record_count=count, pending=True)
            with self.subTest(position=position, count=count), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_missing_manifest_and_part_for_locator_fail_closed(self):
        for mutation in ("manifest", "parts"):
            row, store, archive = self.fixture()
            row[mutation] = None if mutation == "manifest" else []
            with self.subTest(mutation=mutation), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_unrequested_corruption_does_not_require_full_parent_read(self):
        row, store, archive = self.fixture()
        archive.payloads["0"] = b"unrelated broken object"
        self.assertIn(("source", row["document_id"]), self.read(store, archive))
        archive.payloads["1"] = b"broken requested object"
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_locator_and_manifest_publication_are_fenced_together(self):
        row, store, archive = self.fixture()
        after = copy.deepcopy(row)
        after["body_record_ordinal"] = 0
        after["manifest"]["revision"] += 1
        store.snapshots.append([after])
        with self.assertRaises(ChunkBodyError):
            self.read(store, archive)

    def test_located_hash_or_revision_mismatch_is_not_pending_fallback(self):
        for mutation in ("hash", "receipt"):
            row, store, archive = self.fixture()
            row["pending"] = True
            if mutation == "hash":
                row["text_sha256"] = digest("wrong")
            else:
                row["revision"] = 3
                row["chunks"][0]["receipt"] = "recall://source/wanted?rev=3#item=0"
            with self.subTest(mutation=mutation), self.assertRaises(ChunkBodyError):
                self.read(store, archive)

    def test_canonical_chunk_boundaries_remain_a_storage_contract(self):
        # Fixed digests catch a changed splitter even when callers and tests
        # otherwise regenerate their expectations with that same splitter.
        text = "α🧠 café\n" * 7000
        pieces = canonical_text_chunks(text)
        self.assertEqual([len(piece.encode()) for piece in pieces], [23998, 23998, 23998, 19006])
        self.assertEqual([digest(piece) for piece in pieces], [
            "4421dbbc4c80225508207f25c37ad498ddcd770e25cf100e9178f1b0c66df4db",
        ] * 3 + ["ed26c6ad404ed1324e347b8197b4a3b4373cc16b7f11b0884072b3ac95ae7fb8"])
        self.assertEqual("".join(pieces), text)

    def test_context_selection_verifies_unreturned_chunks_and_bounds_result(self):
        text = "multichunk 🐢 content\n" * 4000
        pieces = canonical_text_chunks(text)
        row = self.base.document("wanted", text, pieces=pieces)
        store, archive = self.base.fixture([row], [self.base.record(row, text, 0)])
        row.update(body_record_ordinal=0, body_record_count=1)
        key = ("source", row["document_id"])
        with patch("recall_server.chunk_bodies.MAX_RESULT_BYTES", len(pieces[1].encode())):
            result = self.base.read(store, archive, chunk_ordinals={key: (1,)})
            self.assertEqual(result[key], [dict(ordinal=1, receipt=row["chunks"][1]["receipt"],
                                                text_redacted=pieces[1])])
            with self.assertRaisesRegex(ChunkBodyError, "archived_chunk_read_budget_exceeded"):
                self.base.read(store, archive)
        row["chunks"][0]["text_sha256"] = "0" * 64
        with self.assertRaisesRegex(ChunkBodyError, "archived_chunk_body_unavailable"):
            self.base.read(store, archive, chunk_ordinals={key: (1,)})

    def test_completed_event_buffers_released_before_next_event(self):
        from recall_server import chunk_bodies
        rows = [self.base.document(str(i), "large" * 2000) for i in range(3)]
        store, archive = self.base.fixture(rows, [
            self.base.record(row, "large" * 2000, i) for i, row in enumerate(rows)
        ])
        for i, row in enumerate(rows):
            row.update(body_record_ordinal=i, body_record_count=1)
        buffers = []
        original = chunk_bodies._verified_body

        def verify(row, segments, location, selected):
            self.assertTrue(all(not previous for previous in buffers))
            buffers.append(segments)
            return original(row, segments, location, selected)

        with patch.object(chunk_bodies, "_verified_body", side_effect=verify):
            self.base.read(store, archive)
        self.assertEqual(len(buffers), 3)
        self.assertTrue(all(not previous for previous in buffers))

    def test_part_identity_matches_current_manifest(self):
        for field, value in (("logical_document_id", "ldoc_" + "f" * 32), ("revision", 99)):
            row, store, archive = self.fixture()
            row["parts"][1][field] = value
            with self.subTest(field=field), self.assertRaises(ChunkBodyError):
                self.read(store, archive)
            self.assertEqual(archive.calls, [])

    def test_located_segments_cross_parts_without_reading_unrelated_part(self):
        row = self.base.document("wanted", "left🐢right")
        other = self.base.document("other", "unrelated")
        records = [self.base.record(other, "unrelated", 0),
                   self.base.record(row, "left🐢", 1, segment_count=2),
                   self.base.record(row, "right", 2, segment_count=2, segment_ordinal=1, receipts=())]
        store, archive = self.base.fixture([row], records)
        template = row["parts"][0]
        parts = []
        for ordinal, record in enumerate(records):
            payload = record.encode(source_id="source")
            key = str(ordinal)
            archive.payloads[key] = payload
            parts.append(dict(template, part_ordinal=ordinal, first_record_ordinal=ordinal,
                              last_record_ordinal=ordinal, content_sha256=digest(payload),
                              size_bytes=len(payload), object_key=key, receipt_count=len(record.receipts)))
        row.update(parts=parts, body_record_ordinal=1, body_record_count=2)
        row["manifest"]["part_count"] = 3
        result = self.read(store, archive)
        self.assertEqual(result[("source", row["document_id"])][0]["text_redacted"], "left🐢right")
        self.assertEqual([reference["object_key"] for reference in archive.calls], ["1", "2"])

    def test_located_event_limit_fails_without_pg_fallback(self):
        row, store, archive = self.fixture()
        with patch("recall_server.chunk_bodies.MAX_CANONICAL_TEXT_BYTES", 1):
            with self.assertRaisesRegex(ChunkBodyError, "archived_chunk_read_budget_exceeded"):
                self.read(store, archive)

    def test_invalid_chunk_selection_is_rejected(self):
        row, store, archive = self.fixture()
        key = ("source", row["document_id"])
        for selection in ({key: (-1,)}, {key: (True,)}, {key: [0]}, {("other", key[1]): (0,)}):
            with self.subTest(selection=selection), self.assertRaisesRegex(ChunkBodyError, "request_invalid"):
                self.base.read(store, archive, chunk_ordinals=selection)
        with self.assertRaisesRegex(ChunkBodyError, "body_unavailable"):
            self.base.read(store, archive, chunk_ordinals={key: (99,)})


class LocatorMigrationTests(unittest.TestCase):
    def test_normal_locator_migration_does_not_retire_postgres_plane(self):
        from tests.central_brain.test_retire_postgres_plane import _RecordingConnection, _store
        connection = _RecordingConnection(set(range(1, 67)))
        store = _store("postgres")
        with patch.object(store, "connect", return_value=connection):
            result = store.migrate()
        self.assertIn(68, result["applied"])
        self.assertNotIn(67, connection.versions)
        self.assertEqual(result["postgres_vector_plane"], "present")


class LocatorPublicationEligibilityTests(unittest.TestCase):
    def stream(self, text, pieces, **overrides):
        from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
        projector = CanonicalLogicalEvidenceProjector(None, None)
        row = dict(document_id="doc_" + "a" * 32, event_text=text,
                   document_text_sha256=digest(text), document_revision=1,
                   source_chunks=[dict(ordinal=i, size_bytes=len(piece.encode()), text_sha256=digest(piece))
                                  for i, piece in enumerate(pieces)],
                   chunk_receipts=[f"recall://source/native?rev=1#item={i}" for i in range(len(pieces))],
                   chunk_count=len(pieces), fallback_type_values=[], fallback_role_values=["assistant"],
                   raw_media_type="application/json", source_id="source", native_id="native",
                   kind="transcript_record", occurred_at="2026-09-21T00:00:00Z")
        row.update(overrides)
        locations = []
        # Locator eligibility is independent of the already-covered oversized
        # raw recovery transport. Keep this test about publication boundaries.
        with patch.object(projector, "_restored_record_text", return_value=text):
            records = list(projector._record_stream([row], locate=locations.append))
        return records, locations

    def test_canonical_and_single_chunk_empty_bodies_get_exact_positions(self):
        for text, pieces in (("", [""]), ("x" * 25000, ["x" * 25000]),
                             ("α🧠" * 10000, canonical_text_chunks("α🧠" * 10000))):
            with self.subTest(bytes=len(text.encode())):
                records, locations = self.stream(text, pieces)
                self.assertEqual(locations, [("doc_" + "a" * 32, 0, len(records))])

    def test_historical_boundaries_remain_unlocated_and_intact(self):
        records, locations = self.stream("abcdef", ["ab", "cdef"])
        self.assertEqual(locations, [])
        self.assertEqual("".join(record.text for record in records), "abcdef")

    def test_excluded_and_oversized_records_never_get_locators(self):
        records, locations = self.stream("body", ["body"], fallback_type_values=["token_count"])
        self.assertEqual((records, locations), ([], []))
        records, locations = self.stream("body", ["body"],
            raw_media_type="application/vnd.recall.oversized-record+gzip")
        self.assertEqual(locations, [])
        self.assertTrue(records)
        with patch("recall_server.logical_evidence_projection.MAX_CANONICAL_TEXT_BYTES", 3):
            records, locations = self.stream("body", ["body"])
        self.assertEqual(locations, [])


if __name__ == "__main__":
    unittest.main()
