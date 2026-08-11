from __future__ import annotations

import json
import os
import unittest
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from recall_server.parquet_scan import (
    CanonicalParquetScanProjector,
    ParquetScanError,
    ScanCandidate,
    ScanUpload,
    _month,
    _parquet_bytes,
    _parquet_parts,
    _schemas,
)


class _Archive:
    def __init__(self, record: dict):
        self.record = record
        self.reads = 0

    def read_raw(self, _reference):
        self.reads += 1
        return json.dumps(self.record, sort_keys=True).encode() + b"\n"


class _Evidence:
    def __init__(self, archive):
        self.archive = archive


class _SeedResult:
    rowcount = 1


class _SeedConnection:
    def __init__(self):
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, query, _parameters):
        self.query = query
        return _SeedResult()


class _SeedStore:
    def __init__(self):
        self.connection = _SeedConnection()

    def connect(self):
        return self.connection


def _part() -> dict:
    return {
        "tenant_id": "tenant:test",
        "source_id": "source:test",
        "part_ordinal": 0,
        "artifact_id": "artifact:test",
        "storage_backend": "filesystem",
        "object_key": "objects/test",
        "content_sha256": "a" * 64,
        "size_bytes": 1,
        "media_type": "application/x-ndjson",
        "encryption": "filesystem-private",
        "version_id": "v1",
        "first_occurred_at": datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
        "last_occurred_at": datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
        "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }


def _document(display_name: str) -> dict:
    return {
        "logical_document_id": "document:test",
        "revision": 1,
        "part_count": 1,
        "document_content_sha256": "b" * 64,
        "actor_links": [{
            "actor_id": "actor:employee",
            "display_name": display_name,
            "relation": "contributor",
        }],
        "parts": [_part()],
    }


def _candidate() -> ScanCandidate:
    return ScanCandidate(
        tenant_id="tenant:test",
        source_id="source:test",
        bucket_start=date(2026, 8, 1),
        generation=1,
        changed_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )


class _BuildProbe(CanonicalParquetScanProjector):
    def __init__(self, document: dict, archive: _Archive):
        super().__init__(None, _Evidence(archive))
        self.document = document

    def _documents(self, _candidate):
        return [self.document]

    def _current_upload(self, _candidate, _generation):
        return None

    def _upload(self, _candidate, *, generation, rows, first, last, created_at):
        return ScanUpload(
            generation,
            {},
            {(name, 0): len(values) for name, values in rows.items()},
            first,
            last,
            True,
        )


class _CheckpointProbe(CanonicalParquetScanProjector):
    def __init__(self, archive: _Archive):
        super().__init__(None, _Evidence(archive))
        self.checkpoints = []

    def _persist_part_bounds(self, bounds):
        self.checkpoints.append(len(bounds))


class ParquetScanContractTest(unittest.TestCase):
    def test_seed_deduplicates_documents_in_one_source_month_before_upsert(self):
        store = _SeedStore()
        projector = CanonicalParquetScanProjector(store, _Evidence(None))
        self.assertEqual(projector.seed_backfill(tenant_id="tenant:test"), 1)
        self.assertIn("SELECT DISTINCT", store.connection.query)
        self.assertIn("statement_timestamp()", store.connection.query)
        self.assertNotIn("'backfill',clock_timestamp()", store.connection.query)

    def test_bucket_must_be_the_first_utc_calendar_day(self):
        self.assertEqual(_month("2026-08-01"), date(2026, 8, 1))
        with self.assertRaisesRegex(ParquetScanError, "bucket_invalid"):
            _month("2026-08-02")

    def test_typed_parquet_round_trip_preserves_large_record_json(self):
        row = {
            "schema_version": 1,
            "tenant_id": "tenant:test",
            "source_id": "source:test",
            "logical_document_id": "document:test",
            "revision": 1,
            "ordinal": 7,
            "occurred_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=7"],
            "actor_ids": ["actor:employee"],
            "actor_names": ["Employee"],
            "actor_relations": ["contributor"],
            "search_text": "useful context",
            "record_json": "x" * 100_000,
        }
        payload = _parquet_bytes([row], _schemas()["records"])
        result = pq.read_table(pa.BufferReader(payload)).to_pylist()
        self.assertEqual(result, [row])

    def test_multipart_parquet_is_bounded_ordered_and_lossless(self):
        schema = pa.schema([("ordinal", pa.int64()), ("value", pa.binary())])
        rows = [
            {"ordinal": ordinal, "value": os.urandom(4_000)}
            for ordinal in range(12)
        ]
        parts = _parquet_parts(rows, schema, maximum_bytes=35_000)
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(payload) <= 35_000 for payload, _ in parts))
        self.assertEqual(sum(row_count for _, row_count in parts), len(rows))
        restored = []
        for payload, _ in parts:
            restored.extend(pq.read_table(pa.BufferReader(payload)).to_pylist())
        self.assertEqual(restored, rows)

    def test_one_unshardable_record_fails_content_free(self):
        schema = pa.schema([("value", pa.binary())])
        with self.assertRaisesRegex(ParquetScanError, "record_too_large"):
            _parquet_parts(
                [{"value": os.urandom(20_000)}],
                schema,
                maximum_bytes=5_000,
            )

    def test_record_attribution_never_falls_back_to_all_document_actors(self):
        record = {
            "ordinal": 0,
            "occurred_at": "2026-08-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["assistant"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "text": "assistant output",
        }
        projector = CanonicalParquetScanProjector(None, _Evidence(_Archive(record)))
        result = projector._project_document(
            _candidate(),
            _document("Employee"),
            bucket_start=datetime(2026, 8, 1, tzinfo=timezone.utc),
            bucket_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
            record_budget=10,
        )
        self.assertEqual(result.records[0]["actor_ids"], [])
        self.assertEqual(result.actors, [])

    def test_legacy_part_discovers_exact_bounds_once(self):
        record = {
            "ordinal": 0,
            "occurred_at": "2026-08-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "text": "useful context",
        }
        part = _part()
        part["first_occurred_at"] = None
        part["last_occurred_at"] = None
        document = _document("Employee")
        document["parts"] = [part]
        archive = _Archive(record)
        projector = CanonicalParquetScanProjector(None, _Evidence(archive))
        result = projector._project_document(
            _candidate(),
            document,
            bucket_start=datetime(2026, 8, 1, tzinfo=timezone.utc),
            bucket_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
            record_budget=10,
        )
        self.assertEqual(archive.reads, 1)
        self.assertEqual(len(result.part_bounds), 1)
        self.assertEqual(
            result.part_bounds[0].first_occurred_at,
            datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
        )

    def test_known_nonoverlapping_part_is_not_downloaded(self):
        record = {
            "ordinal": 0,
            "occurred_at": "2026-07-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "text": "older context",
        }
        part = _part()
        part["first_occurred_at"] = datetime(
            2026, 7, 5, 12, tzinfo=timezone.utc
        )
        part["last_occurred_at"] = part["first_occurred_at"]
        document = _document("Employee")
        document["parts"] = [part]
        archive = _Archive(record)
        result = CanonicalParquetScanProjector(
            None, _Evidence(archive)
        )._project_document(
            _candidate(),
            document,
            bucket_start=datetime(2026, 8, 1, tzinfo=timezone.utc),
            bucket_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
            record_budget=10,
        )
        self.assertEqual(archive.reads, 0)
        self.assertEqual(result.records, [])
        self.assertEqual(result.part_bounds, ())

    def test_legacy_bound_discovery_checkpoints_large_documents(self):
        record = {
            "ordinal": 0,
            "occurred_at": "2026-08-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "text": "useful context",
        }
        parts = []
        for ordinal in range(129):
            part = _part()
            part["part_ordinal"] = ordinal
            part["first_occurred_at"] = None
            part["last_occurred_at"] = None
            parts.append(part)
        document = _document("Employee")
        document["parts"] = parts
        archive = _Archive(record)
        projector = _CheckpointProbe(archive)

        result = projector._project_document(
            _candidate(),
            document,
            bucket_start=datetime(2026, 8, 1, tzinfo=timezone.utc),
            bucket_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
            record_budget=200,
        )

        self.assertEqual(projector.checkpoints, [128])
        self.assertEqual(len(result.part_bounds), 1)
        self.assertEqual(archive.reads, 129)

    def test_generation_changes_when_actor_projection_changes(self):
        record = {
            "ordinal": 0,
            "occurred_at": "2026-08-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "actor_links": [{
                "actor_id": "actor:employee",
                "relation": "contributor",
            }],
            "text": "employee prompt",
        }
        first = _BuildProbe(_document("First Name"), _Archive(record))._build(
            _candidate()
        )
        same = _BuildProbe(_document("First Name"), _Archive(record))._build(
            _candidate()
        )
        renamed = _BuildProbe(_document("Renamed"), _Archive(record))._build(
            _candidate()
        )
        self.assertEqual(first.generation_sha256, same.generation_sha256)
        self.assertNotEqual(first.generation_sha256, renamed.generation_sha256)


if __name__ == "__main__":
    unittest.main()
