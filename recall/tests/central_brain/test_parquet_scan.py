from __future__ import annotations

import json
import os
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from recall_server.parquet_scan import (
    SCAN_DATASETS,
    SCAN_DIRTY_ALL,
    CanonicalParquetScanProjector,
    FragmentMember,
    ParquetScanError,
    ScanCandidate,
    ScanCatalog,
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
        self.uploads = []

    def read_raw(self, _reference):
        self.reads += 1
        return json.dumps(self.record, sort_keys=True).encode() + b"\n"

    def put_raw(self, **values):
        self.uploads.append(values)
        ordinal = len(self.uploads)
        return {
            "tenant_id": values["tenant_id"],
            "source_id": values["source_id"],
            "artifact_id": f"artifact:{ordinal}",
            "storage_backend": "memory",
            "object_key": f"objects/{ordinal}",
            "content_sha256": f"{ordinal:064x}",
            "size_bytes": len(values["payload"]),
            "media_type": values["media_type"],
            "encryption": "test",
            "version_id": f"v{ordinal}",
            "created_at": values["created_at"],
        }


class _Evidence:
    def __init__(self, archive):
        self.archive = archive


class _ManyRecordArchive(_Archive):
    def __init__(self, count: int):
        super().__init__({})
        self.count = count

    def read_raw(self, _reference):
        self.reads += 1
        return b"".join(
            json.dumps(
                {
                    "ordinal": ordinal,
                    "occurred_at": "2026-08-05T12:00:00Z",
                    "event_kind": "transcript_record",
                    "roles": ["assistant"],
                    "receipts": [f"recall://source:test/doc?rev=1#item={ordinal}"],
                    "text": f"context-{ordinal}-" + "x" * 20_000,
                },
                sort_keys=True,
            ).encode()
            + b"\n"
            for ordinal in range(self.count)
        )


class _SeedResult:
    rowcount = 1


class _SeedConnection:
    def __init__(self):
        self.query = ""
        self.queries = []
        self.parameters = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    @contextmanager
    def transaction(self):
        yield self

    def execute(self, query, parameters):
        self.query = query
        self.queries.append(query)
        self.parameters.append(parameters)
        return _SeedResult()


class _SeedStore:
    def __init__(self):
        self.connection = _SeedConnection()

    def connect(self):
        return self.connection


class _LeaseResult:
    def __init__(self, acquired: bool):
        self.acquired = acquired

    def fetchone(self):
        return {"acquired": self.acquired}


class _LeaseConnection:
    def __init__(self, acquired: bool):
        self.acquired = acquired
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, query, parameters):
        self.calls.append((query, parameters))
        return _LeaseResult(self.acquired)


class _LeaseStore:
    def __init__(self, acquired: bool):
        self.connection = _LeaseConnection(acquired)

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
        "actor_links": [
            {
                "actor_id": "actor:employee",
                "display_name": display_name,
                "relation": "contributor",
            }
        ],
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
    """Build one month from in-memory documents with an empty catalog."""

    def __init__(
        self,
        document: dict,
        archive: _Archive,
        passages: list[dict] | None = None,
    ):
        super().__init__(None, _Evidence(archive), compaction_fragments=16)
        self.document = document
        self.passages = passages or []

    def _documents(self, _candidate):
        return [self.document]

    def _attach_parts(self, _candidate, _documents):
        return None

    def _catalog(self, _candidate):
        return ScanCatalog({}, {}, frozenset())

    def _passages(self, _candidate, document_ids):
        return [
            passage
            for passage in self.passages
            if passage["logical_document_id"] in document_ids
        ]


class _CheckpointProbe(CanonicalParquetScanProjector):
    def __init__(self, archive: _Archive):
        super().__init__(None, _Evidence(archive))
        self.checkpoints = []

    def _persist_part_bounds(self, bounds):
        self.checkpoints.append(len(bounds))


class _WindowProbe(CanonicalParquetScanProjector):
    def __init__(self):
        super().__init__(None, _Evidence(None))
        self.pending_limits = []
        self.built = []

    def _pending(self, *, tenant_id, limit):
        self.pending_limits.append((tenant_id, limit))
        base = _candidate()
        return [
            ScanCandidate(
                base.tenant_id,
                source_id,
                base.bucket_start,
                base.generation,
                base.changed_at,
            )
            for source_id in ("source:locked", "source:ready", "source:later")
        ]

    @contextmanager
    def _candidate_lease(self, candidate):
        yield candidate.source_id != "source:locked"

    def _build(self, candidate):
        self.built.append(candidate.source_id)
        return ScanUpload(
            "a" * 64,
            {},
            {("records", 0): 1},
            None,
            None,
            False,
        )

    def _commit(self, _candidate, _upload):
        return "committed"

    def _over_fragmented(self, *, tenant_id, limit):
        self.compaction_sweeps = getattr(self, "compaction_sweeps", [])
        self.compaction_sweeps.append((tenant_id, limit))
        return []

    def _fragment_total(self, *, tenant_id):
        return 0


class ParquetScanContractTest(unittest.TestCase):
    def test_seed_deduplicates_documents_in_one_source_month_before_upsert(self):
        store = _SeedStore()
        projector = CanonicalParquetScanProjector(store, _Evidence(None))
        self.assertEqual(projector.seed_backfill(tenant_id="tenant:test"), 1)
        queue_query, dirty_query = store.connection.queries
        self.assertIn("SELECT DISTINCT", queue_query)
        self.assertIn("statement_timestamp()", queue_query)
        self.assertNotIn("'backfill',clock_timestamp()", queue_query)
        # A backfill marks the whole source-month dirty: full rebuild.
        self.assertIn("canonical_parquet_scan_dirty_documents", dirty_query)
        self.assertEqual(store.connection.parameters[1][0], SCAN_DIRTY_ALL)

    def test_bucket_must_be_the_first_utc_calendar_day(self):
        self.assertEqual(_month("2026-08-01"), date(2026, 8, 1))
        with self.assertRaisesRegex(ParquetScanError, "bucket_invalid"):
            _month("2026-08-02")

    def test_candidate_lease_skips_contention_and_unlocks_ownership(self):
        contended = _LeaseStore(False)
        with CanonicalParquetScanProjector(
            contended, _Evidence(None)
        )._candidate_lease(_candidate()) as acquired:
            self.assertFalse(acquired)
        self.assertEqual(len(contended.connection.calls), 1)
        self.assertIn("pg_try_advisory_lock", contended.connection.calls[0][0])

        owned = _LeaseStore(True)
        with CanonicalParquetScanProjector(
            owned, _Evidence(None)
        )._candidate_lease(_candidate()) as acquired:
            self.assertTrue(acquired)
        self.assertEqual(len(owned.connection.calls), 2)
        self.assertIn("pg_advisory_unlock", owned.connection.calls[1][0])
        self.assertEqual(
            owned.connection.calls[0][1],
            owned.connection.calls[1][1],
        )

    def test_contended_head_does_not_hide_the_next_ready_candidate(self):
        projector = _WindowProbe()
        result = projector.project_pending(
            tenant_id="tenant:test",
            batch_size=1,
            max_batches=1,
        )
        self.assertEqual(projector.pending_limits, [("tenant:test", 8)])
        self.assertEqual(projector.built, ["source:ready"])
        self.assertEqual(result["shards"], 1)
        self.assertEqual(result["contended"], 1)

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

    def test_passage_pointer_schema_preserves_lossless_text_and_receipts(self):
        self.assertNotIn("record_json", _schemas()["passages"].names)
        row = {
            "schema_version": 2,
            "tenant_id": "tenant:test",
            "source_id": "source:test",
            "logical_document_id": "document:test",
            "revision": 1,
            "passage_id": "psg_" + "a" * 32,
            "ordinal": 3,
            "first_occurred_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
            "last_occurred_at": datetime(2026, 8, 2, tzinfo=timezone.utc),
            "token_count": 512,
            "roles": ["assistant", "user"],
            "receipts": ["recall://source:test/doc?rev=1#item=7"],
            "actor_ids": ["actor:employee"],
            "actor_names": ["Employee"],
            "actor_relations": ["contributor"],
            "text": "exact visible message bytes",
        }
        payload = _parquet_bytes([row], _schemas()["passages"])
        self.assertEqual(
            pq.read_table(pa.BufferReader(payload)).to_pylist(),
            [row],
        )

    def test_passage_plane_migration_rebuilds_and_expands_closed_dataset_enum(self):
        migration = (
            Path(__file__).resolve().parents[2]
            / "server/schema/053_parquet_passage_plane.sql"
        ).read_text()
        self.assertIn("'documents','passages','records','actors'", migration)
        self.assertIn("statement_timestamp()", migration)
        self.assertNotIn("clock_timestamp()", migration)
        self.assertIn("canonical_parquet_scan_queue", migration)
        self.assertIn("version=53", migration)

    def test_passage_plane_repair_migration_requeues_each_source_month_once(self):
        migration = (
            Path(__file__).resolve().parents[2]
            / "server/schema/054_requeue_parquet_passage_plane.sql"
        ).read_text()
        self.assertIn("SELECT DISTINCT shard.tenant_id,shard.source_id,shard.bucket_start", migration)
        self.assertIn("statement_timestamp()", migration)
        self.assertNotIn("clock_timestamp()", migration)
        self.assertIn("version=54", migration)

    def test_multipart_parquet_is_bounded_ordered_and_lossless(self):
        schema = pa.schema([("ordinal", pa.int64()), ("value", pa.binary())])
        rows = [
            {"ordinal": ordinal, "value": os.urandom(4_000)} for ordinal in range(12)
        ]
        parts = _parquet_parts(rows, schema, maximum_bytes=35_000)
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(payload) <= 35_000 for payload, _ in parts))
        self.assertEqual(sum(row_count for _, row_count in parts), len(rows))
        restored = []
        for payload, _ in parts:
            restored.extend(pq.read_table(pa.BufferReader(payload)).to_pylist())
        self.assertEqual(restored, rows)

    def test_multipart_encodes_each_bounded_arrow_slice_once(self):
        schema = pa.schema([("ordinal", pa.int64()), ("value", pa.binary())])
        rows = [
            {"ordinal": ordinal, "value": os.urandom(4_000)} for ordinal in range(12)
        ]
        encoded_rows = []

        def encode(table):
            encoded_rows.append(table.num_rows)
            sink = pa.BufferOutputStream()
            pq.write_table(
                table,
                sink,
                compression="zstd",
                use_dictionary=False,
            )
            return sink.getvalue().to_pybytes()

        with mock.patch(
            "recall_server.parquet_scan._parquet_table_bytes",
            side_effect=encode,
        ):
            parts = _parquet_parts(rows, schema, maximum_bytes=45_000)

        self.assertEqual(sum(encoded_rows), len(rows))
        self.assertLess(max(encoded_rows), len(rows))
        self.assertEqual(sum(row_count for _, row_count in parts), len(rows))

    def test_build_streams_month_records_through_bounded_uploads(self):
        archive = _ManyRecordArchive(30)
        projector = _BuildProbe(_document("Employee"), archive)

        with mock.patch(
            "recall_server.parquet_scan.PARQUET_RAW_SLICE_BYTES",
            100_000,
        ):
            result = projector._build(_candidate())

        record_uploads = [
            upload for upload in archive.uploads if ":records:" in upload["native_id"]
        ]
        self.assertGreater(len(record_uploads), 5)
        self.assertEqual(
            sum(
                count
                for (dataset, _), count in result.row_counts.items()
                if dataset == "records"
            ),
            30,
        )
        restored = []
        for upload in record_uploads:
            restored.extend(
                pq.read_table(pa.BufferReader(upload["payload"]))
                .column("ordinal")
                .to_pylist()
            )
        self.assertEqual(restored, list(range(30)))

    def test_build_adds_lossless_passage_pointer_shard(self):
        archive = _Archive({
            "ordinal": 0,
            "occurred_at": "2026-08-05T12:00:00Z",
            "event_kind": "transcript_record",
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "text": "canonical record",
        })
        passage = {
            "logical_document_id": "document:test",
            "revision": 1,
            "passage_id": "psg_" + "a" * 32,
            "ordinal": 0,
            "first_occurred_at": datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
            "last_occurred_at": datetime(2026, 8, 5, 12, tzinfo=timezone.utc),
            "token_count": 3,
            "roles": ["user"],
            "receipts": ["recall://source:test/doc?rev=1#item=0"],
            "actor_ids": ["actor:employee"],
            "actor_names": ["Employee"],
            "actor_relations": ["contributor"],
            "text_redacted": "exact visible prompt",
        }
        result = _BuildProbe(
            _document("Employee"),
            archive,
            passages=[passage],
        )._build(_candidate())
        self.assertEqual(
            sum(
                count
                for (dataset, _), count in result.row_counts.items()
                if dataset == "passages"
            ),
            1,
        )
        upload = next(
            item for item in archive.uploads if ":passages:" in item["native_id"]
        )
        restored = pq.read_table(pa.BufferReader(upload["payload"])).to_pylist()
        self.assertEqual(restored[0]["text"], "exact visible prompt")
        self.assertEqual(restored[0]["receipts"], passage["receipts"])

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
        part["first_occurred_at"] = datetime(2026, 7, 5, 12, tzinfo=timezone.utc)
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
            "actor_links": [
                {
                    "actor_id": "actor:employee",
                    "relation": "contributor",
                }
            ],
            "text": "employee prompt",
        }
        first_archive = _Archive(record)
        same_archive = _Archive(record)
        first = _BuildProbe(_document("First Name"), first_archive)._build(_candidate())
        same = _BuildProbe(_document("First Name"), same_archive)._build(_candidate())
        renamed = _BuildProbe(_document("Renamed"), _Archive(record))._build(
            _candidate()
        )
        self.assertEqual(first.generation_sha256, same.generation_sha256)
        self.assertEqual(
            [upload["native_id"] for upload in first_archive.uploads],
            [upload["native_id"] for upload in same_archive.uploads],
        )
        self.assertNotEqual(first.generation_sha256, renamed.generation_sha256)

    def test_generation_changes_when_passage_policy_changes(self):
        first = _document("Employee")
        first.update({
            "passage_policy_fingerprint": "a" * 64,
            "passage_source_sha256": first["document_content_sha256"],
            "passage_count": 3,
        })
        second = {**first, "passage_policy_fingerprint": "b" * 64}
        projector = CanonicalParquetScanProjector(None, _Evidence(None))
        self.assertNotEqual(
            projector._generation(_candidate(), [first]),
            projector._generation(_candidate(), [second]),
        )


class _DocumentArchive:
    """Serve one part per document; records carry the document id."""

    def __init__(self, records: dict[str, int]):
        self.records = records
        self.reads = []
        self.uploads = []

    def read_raw(self, reference):
        document_id = reference["artifact_id"].split(":", 1)[1]
        self.reads.append(document_id)
        return b"".join(
            json.dumps(
                {
                    "ordinal": ordinal,
                    "occurred_at": "2026-08-05T12:00:00Z",
                    "event_kind": "transcript_record",
                    "roles": ["user"],
                    "receipts": [f"recall://source:test/{document_id}#item={ordinal}"],
                    "text": f"{document_id} record {ordinal}",
                },
                sort_keys=True,
            ).encode()
            + b"\n"
            for ordinal in range(self.records[document_id])
        )

    def put_raw(self, **values):
        self.uploads.append(values)
        ordinal = len(self.uploads)
        return {
            "tenant_id": values["tenant_id"],
            "source_id": values["source_id"],
            "artifact_id": f"new:{ordinal}",
            "storage_backend": "memory",
            "object_key": f"objects/new/{ordinal}",
            "content_sha256": f"{ordinal:064x}",
            "size_bytes": len(values["payload"]),
            "media_type": values["media_type"],
            "encryption": "test",
            "version_id": f"v{ordinal}",
            "created_at": values["created_at"],
        }


def _month_document(document_id: str, content: str = "b") -> dict:
    part = _part()
    part["artifact_id"] = f"raw:{document_id}"
    return {
        "logical_document_id": document_id,
        "revision": 1,
        "part_count": 1,
        "document_content_sha256": content * 64,
        "actor_links": [],
        "parts": [part],
    }


def _live_shard(dataset: str, shard_index: int) -> dict:
    return {
        "tenant_id": "tenant:test",
        "source_id": "source:test",
        "bucket_start": date(2026, 8, 1),
        "dataset": dataset,
        "shard_index": shard_index,
        "generation_sha256": "c" * 64,
        "artifact_id": f"live:{dataset}:{shard_index}",
        "storage_backend": "memory",
        "object_key": f"objects/live/{dataset}/{shard_index}",
        "content_sha256": "d" * 64,
        "size_bytes": 1,
        "media_type": "application/vnd.apache.parquet",
        "encryption": "test",
        "version_id": "v1",
        "row_count": 1,
        "first_occurred_at": None,
        "last_occurred_at": None,
        "created_at": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }


def _catalog(
    layout: dict[int, list[dict]],
    *,
    dirty: set[str] = frozenset(),
    compaction: bool = False,
) -> ScanCatalog:
    """Build a catalog where every dataset has the same part -> documents layout."""

    shards = {}
    members = {}
    for shard_index, documents in layout.items():
        for dataset in SCAN_DATASETS:
            shards[(dataset, shard_index)] = _live_shard(dataset, shard_index)
            members[(dataset, shard_index)] = tuple(
                FragmentMember(
                    document["logical_document_id"],
                    int(document["revision"]),
                    CanonicalParquetScanProjector._fingerprint(document),
                )
                for document in documents
            )
    return ScanCatalog(shards, members, frozenset(dirty), compaction)


class _FragmentProbe(CanonicalParquetScanProjector):
    def __init__(
        self,
        documents: list[dict],
        catalog: ScanCatalog,
        archive: _DocumentArchive,
        *,
        compaction_fragments: int = 16,
    ):
        super().__init__(
            None,
            _Evidence(archive),
            compaction_fragments=compaction_fragments,
        )
        self.month_documents = documents
        self.catalog = catalog

    def _documents(self, _candidate):
        return [
            {key: value for key, value in document.items() if key != "parts"}
            for document in self.month_documents
        ]

    def _attach_parts(self, _candidate, documents):
        parts = {
            document["logical_document_id"]: document["parts"]
            for document in self.month_documents
        }
        for document in documents:
            document["parts"] = list(parts[document["logical_document_id"]])

    def _catalog(self, _candidate):
        return self.catalog

    def _passages(self, _candidate, _document_ids):
        return []


class _CommitResult:
    def __init__(self, rows=None, rowcount=1):
        self.rows = rows or []
        self.rowcount = rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _CommitCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def executemany(self, query, rows):
        self.connection.batches.append((query, list(rows)))


class _CommitConnection:
    def __init__(self, live: list[dict], candidate: ScanCandidate):
        self.live = live
        self.candidate = candidate
        self.statements = []
        self.batches = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    @contextmanager
    def transaction(self):
        yield self

    def cursor(self):
        return _CommitCursor(self)

    def execute(self, query, parameters=None):
        self.statements.append((query, parameters))
        if "FROM canonical_parquet_scan_queue" in query and "SELECT" in query:
            return _CommitResult(
                [
                    {
                        "generation": self.candidate.generation,
                        "changed_at": self.candidate.changed_at,
                    }
                ]
            )
        if "SELECT * FROM canonical_parquet_scan_shards" in query:
            return _CommitResult(self.live)
        return _CommitResult(rowcount=1)


class _CommitStore:
    def __init__(self, live: list[dict], candidate: ScanCandidate):
        self.connection = _CommitConnection(live, candidate)

    def connect(self):
        return self.connection


class ParquetFragmentDeltaTest(unittest.TestCase):
    def _records(self, archive: _DocumentArchive) -> dict[str, list[int]]:
        seen: dict[str, list[int]] = {}
        for upload in archive.uploads:
            if ":records:" not in upload["native_id"]:
                continue
            for row in pq.read_table(pa.BufferReader(upload["payload"])).to_pylist():
                seen.setdefault(row["logical_document_id"], []).append(row["ordinal"])
        return seen

    def test_delta_rebuild_rewrites_only_intersecting_fragments(self):
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        catalog = _catalog({0: [alpha], 1: [beta]}, dirty={"document:beta"})
        changed_beta = {**beta, "document_content_sha256": "e" * 64}
        archive = _DocumentArchive({"document:alpha": 2, "document:beta": 3})
        projector = _FragmentProbe([alpha, changed_beta], catalog, archive)

        result = projector._build(_candidate())

        self.assertEqual(result.mode, "delta")
        self.assertEqual(
            set(result.removed),
            {(dataset, 1) for dataset in SCAN_DATASETS},
        )
        # New parts land above every surviving index (alpha's part 0 stays);
        # the replaced index is free once the catalog flips, and every new
        # object key is fresh, so no live object is ever overwritten.
        self.assertEqual(
            set(result.references),
            {(dataset, 1) for dataset in ("documents", "records")},
        )
        live_keys = {row["object_key"] for row in catalog.shards.values()}
        self.assertFalse(
            {reference["object_key"] for reference in result.references.values()}
            & live_keys
        )
        self.assertEqual(archive.reads, ["document:beta"])
        for members in result.members.values():
            self.assertEqual(
                {member.logical_document_id for member in members},
                {"document:beta"},
            )
        self.assertEqual(result.documents_dirty, 1)
        self.assertEqual(result.documents_rewritten, 1)

    def test_backfill_reason_forces_full_rebuild(self):
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        catalog = _catalog({0: [alpha], 1: [beta]}, dirty={"document:beta"})
        changed_beta = {**beta, "document_content_sha256": "e" * 64}
        archive = _DocumentArchive({"document:alpha": 2, "document:beta": 3})
        projector = _FragmentProbe([alpha, changed_beta], catalog, archive)
        candidate = ScanCandidate(
            **{**_candidate().__dict__, "reason": "backfill"}
        )

        result = projector._build(candidate)

        self.assertEqual(result.mode, "full")
        self.assertEqual(set(result.removed), set(catalog.shards))
        self.assertEqual(sorted(archive.reads), ["document:alpha", "document:beta"])
        # Nothing survives, so the fresh parts start again at index 0.
        self.assertEqual(
            set(result.references),
            {(dataset, 0) for dataset in SCAN_DATASETS},
        )

    def test_whole_month_sentinel_forces_full_rebuild(self):
        alpha = _month_document("document:alpha")
        catalog = _catalog({0: [alpha]}, dirty={SCAN_DIRTY_ALL})
        changed = {**alpha, "document_content_sha256": "e" * 64}
        archive = _DocumentArchive({"document:alpha": 1})
        result = _FragmentProbe([changed], catalog, archive)._build(_candidate())
        self.assertEqual(result.mode, "full")
        self.assertEqual(set(result.removed), set(catalog.shards))

    def test_content_identical_backfill_reuses_live_fragments(self):
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        catalog = _catalog({0: [alpha], 1: [beta]}, dirty={SCAN_DIRTY_ALL})
        archive = _DocumentArchive({"document:alpha": 2, "document:beta": 3})
        projector = _FragmentProbe([alpha, beta], catalog, archive)
        candidate = ScanCandidate(
            **{**_candidate().__dict__, "reason": "backfill"}
        )

        result = projector._build(candidate)

        self.assertEqual(result.mode, "reuse")
        self.assertFalse(result.created)
        self.assertEqual(result.removed, ())
        self.assertEqual(set(result.references), set(catalog.shards))
        self.assertEqual(archive.reads, [])
        self.assertEqual(archive.uploads, [])

    def test_unchanged_dirty_document_is_a_no_op(self):
        alpha = _month_document("document:alpha")
        catalog = _catalog({0: [alpha]}, dirty={"document:alpha"})
        archive = _DocumentArchive({"document:alpha": 1})
        result = _FragmentProbe([alpha], catalog, archive)._build(_candidate())
        self.assertEqual(result.mode, "reuse")
        self.assertEqual(archive.uploads, [])

    def test_forgotten_document_drops_its_fragment_without_reading_siblings(self):
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        catalog = _catalog({0: [alpha], 1: [beta]}, dirty={"document:beta"})
        archive = _DocumentArchive({"document:alpha": 2})
        result = _FragmentProbe([alpha], catalog, archive)._build(_candidate())
        self.assertEqual(result.mode, "delta")
        self.assertEqual(
            set(result.removed),
            {(dataset, 1) for dataset in SCAN_DATASETS},
        )
        self.assertEqual(result.references, {})
        self.assertEqual(archive.reads, [])

    def test_fragment_rows_never_duplicate_a_document(self):
        # alpha spilled across parts 0 and 1 (superset membership); gamma shares
        # part 1 with alpha's tail; beta lives alone in part 2.
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        gamma = _month_document("document:gamma")
        catalog = _catalog(
            {0: [alpha], 1: [alpha, gamma], 2: [beta]},
            dirty={"document:gamma"},
        )
        changed_gamma = {**gamma, "document_content_sha256": "e" * 64}
        archive = _DocumentArchive(
            {"document:alpha": 4, "document:beta": 2, "document:gamma": 3}
        )
        projector = _FragmentProbe([alpha, beta, changed_gamma], catalog, archive)

        result = projector._build(_candidate())

        # Rewriting gamma's part drags alpha's tail, so alpha's head part goes
        # too: otherwise alpha's rows would exist twice.
        self.assertEqual(
            set(result.removed),
            {(dataset, index) for dataset in SCAN_DATASETS for index in (0, 1)},
        )
        self.assertEqual(sorted(archive.reads), ["document:alpha", "document:gamma"])
        records = self._records(archive)
        self.assertEqual(records["document:alpha"], [0, 1, 2, 3])
        self.assertEqual(records["document:gamma"], [0, 1, 2])
        self.assertNotIn("document:beta", records)
        live_after = (set(catalog.shards) - set(result.removed)) | set(
            result.references
        )
        live_members = [
            member.logical_document_id
            for identity in live_after
            for member in (
                result.members.get(identity) or catalog.members.get(identity) or ()
            )
            if identity[0] == "records"
        ]
        self.assertEqual(sorted(live_members), ["document:alpha", "document:beta", "document:gamma"])

    def test_new_parts_never_overwrite_live_objects(self):
        alpha = _month_document("document:alpha")
        beta = _month_document("document:beta")
        gamma = _month_document("document:gamma")
        catalog = _catalog({0: [alpha], 3: [beta], 7: [gamma]}, dirty={"document:alpha"})
        changed_alpha = {**alpha, "document_content_sha256": "e" * 64}
        archive = _DocumentArchive(
            {"document:alpha": 1, "document:beta": 1, "document:gamma": 1}
        )
        result = _FragmentProbe([changed_alpha, beta, gamma], catalog, archive)._build(
            _candidate()
        )
        live_after_removal = set(catalog.shards) - set(result.removed)
        self.assertFalse(set(result.references) & live_after_removal)
        for (dataset, shard_index) in result.references:
            self.assertGreater(
                shard_index,
                max(index for (live, index) in live_after_removal if live == dataset),
            )
        live_keys = {row["object_key"] for row in catalog.shards.values()}
        for reference in result.references.values():
            self.assertNotIn(reference["object_key"], live_keys)

    def test_commit_flips_catalog_and_queues_only_replaced_objects(self):
        candidate = _candidate()
        live = [_live_shard(dataset, index) for dataset in SCAN_DATASETS for index in (0, 1)]
        store = _CommitStore(live, candidate)
        projector = CanonicalParquetScanProjector(store, _Evidence(None))
        member = FragmentMember("document:beta", 1, "f" * 64)
        upload = ScanUpload(
            "a" * 64,
            {
                (dataset, 2): {
                    **_live_shard(dataset, 2),
                    "artifact_id": f"new:{dataset}",
                    "object_key": f"objects/new/{dataset}",
                }
                for dataset in SCAN_DATASETS
            },
            {(dataset, 2): 1 for dataset in SCAN_DATASETS},
            None,
            None,
            True,
            removed=tuple((dataset, 1) for dataset in SCAN_DATASETS),
            members={(dataset, 2): (member,) for dataset in SCAN_DATASETS},
            mode="delta",
        )

        self.assertEqual(projector._commit(candidate, upload), "committed")

        statements = store.connection.statements
        cleanup = [
            parameters
            for query, parameters in statements
            if "canonical_evidence_cleanup_queue" in query
        ]
        self.assertEqual(
            sorted(parameters[2] for parameters in cleanup),
            sorted(f"live:{dataset}:1" for dataset in SCAN_DATASETS),
        )
        deleted = [
            rows for query, rows in store.connection.batches
            if "DELETE FROM canonical_parquet_scan_shards" in query
        ]
        self.assertEqual(
            {(row[3], row[4]) for row in deleted[0]},
            {(dataset, 1) for dataset in SCAN_DATASETS},
        )
        inserted = [
            parameters
            for query, parameters in statements
            if "INSERT INTO canonical_parquet_scan_shards" in query
        ]
        self.assertEqual({(row[3], row[4]) for row in inserted}, set(upload.references))
        members = [
            rows for query, rows in store.connection.batches
            if "canonical_parquet_scan_fragment_documents" in query
        ]
        self.assertEqual(
            {(row[3], row[4], row[5]) for row in members[0]},
            {(dataset, 2, "document:beta") for dataset in SCAN_DATASETS},
        )
        self.assertTrue(
            any("DELETE FROM canonical_parquet_scan_dirty_documents" in q for q, _ in statements)
        )
        self.assertTrue(
            any("DELETE FROM canonical_parquet_scan_queue" in q for q, _ in statements)
        )
        order = [
            index
            for index, (query, _) in enumerate(statements)
            if "canonical_evidence_cleanup_queue" in query
            or "INSERT INTO canonical_parquet_scan_shards" in query
        ]
        self.assertEqual(order, sorted(order))

    def test_commit_refuses_to_overwrite_a_surviving_part(self):
        candidate = _candidate()
        live = [_live_shard(dataset, index) for dataset in SCAN_DATASETS for index in (0, 1)]
        projector = CanonicalParquetScanProjector(
            _CommitStore(live, candidate), _Evidence(None)
        )
        upload = ScanUpload(
            "a" * 64,
            {("records", 0): _live_shard("records", 0)},
            {("records", 0): 1},
            None,
            None,
            True,
            removed=(("records", 1),),
            mode="delta",
        )
        with self.assertRaises(ParquetScanError) as raised:
            projector._commit(candidate, upload)
        self.assertEqual(str(raised.exception), "parquet_scan_shard_conflict")

    def test_commit_is_stale_when_a_victim_vanished(self):
        candidate = _candidate()
        live = [_live_shard(dataset, 0) for dataset in SCAN_DATASETS]
        projector = CanonicalParquetScanProjector(
            _CommitStore(live, candidate), _Evidence(None)
        )
        upload = ScanUpload(
            "a" * 64,
            {("records", 5): _live_shard("records", 5)},
            {("records", 5): 1},
            None,
            None,
            True,
            removed=(("records", 4),),
            mode="delta",
        )
        self.assertEqual(projector._commit(candidate, upload), "stale")

    def test_compaction_when_fragments_exceed_the_cap(self):
        documents = [_month_document(f"document:{index}") for index in range(4)]
        catalog = _catalog(
            {index: [document] for index, document in enumerate(documents)},
            dirty={"document:3"},
        )
        changed = [*documents[:3], {**documents[3], "document_content_sha256": "e" * 64}]
        archive = _DocumentArchive({f"document:{index}": 1 for index in range(4)})
        result = _FragmentProbe(
            changed, catalog, archive, compaction_fragments=3
        )._build(_candidate())
        self.assertEqual(result.mode, "compaction")
        self.assertEqual(set(result.removed), set(catalog.shards))
        self.assertEqual(len(archive.reads), 4)
        self.assertEqual(
            set(result.references),
            {(dataset, 0) for dataset in SCAN_DATASETS},
        )

    def test_compaction_when_most_recorded_documents_are_dead(self):
        documents = [_month_document(f"document:{index}") for index in range(4)]
        catalog = _catalog(
            {0: documents[:2], 1: documents[2:]},
            dirty={"document:1", "document:2", "document:3"},
        )
        archive = _DocumentArchive({"document:0": 1})
        result = _FragmentProbe([documents[0]], catalog, archive)._build(_candidate())
        self.assertEqual(result.mode, "compaction")
        self.assertEqual(set(result.removed), set(catalog.shards))

    def test_compaction_sentinel_rewrites_unchanged_month(self):
        alpha = _month_document("document:alpha")
        catalog = _catalog({0: [alpha]}, dirty={SCAN_DIRTY_ALL}, compaction=True)
        archive = _DocumentArchive({"document:alpha": 1})
        result = _FragmentProbe([alpha], catalog, archive)._build(_candidate())
        self.assertEqual(result.mode, "compaction")
        self.assertEqual(set(result.removed), set(catalog.shards))
        self.assertTrue(result.created)

    def test_below_the_cap_a_delta_stays_a_delta(self):
        documents = [_month_document(f"document:{index}") for index in range(3)]
        catalog = _catalog(
            {index: [document] for index, document in enumerate(documents)},
            dirty={"document:2"},
        )
        changed = [*documents[:2], {**documents[2], "document_content_sha256": "e" * 64}]
        archive = _DocumentArchive({f"document:{index}": 1 for index in range(3)})
        result = _FragmentProbe(
            changed, catalog, archive, compaction_fragments=3
        )._build(_candidate())
        self.assertEqual(result.mode, "delta")

    def test_compaction_sweep_runs_within_its_budget(self):
        projector = _WindowProbe()
        projector.project_pending(tenant_id="tenant:test", batch_size=1, max_batches=1)
        self.assertEqual(projector.compaction_sweeps, [("tenant:test", 1)])
        projector = _WindowProbe()
        result = projector.project_pending(
            tenant_id="tenant:test",
            batch_size=1,
            max_batches=1,
            compaction_budget=0,
        )
        self.assertEqual(getattr(projector, "compaction_sweeps", []), [])
        self.assertEqual(result["fragments_rewritten"], 0)
        self.assertIn("documents_dirty", result)
        self.assertIn("fragments_total", result)

    def test_compaction_cap_is_configurable_from_the_environment(self):
        with mock.patch.dict(os.environ, {"RECALL_PARQUET_COMPACTION_FRAGMENTS": "5"}):
            projector = CanonicalParquetScanProjector(None, _Evidence(None))
        self.assertEqual(projector.compaction_fragments, 5)
        with self.assertRaises(ParquetScanError):
            CanonicalParquetScanProjector(None, _Evidence(None), compaction_fragments=0)

    def test_fragment_documents_migration_declares_catalog_tables(self):
        migration = (
            Path(__file__).resolve().parents[2]
            / "server/schema/061_parquet_scan_fragments.sql"
        ).read_text()
        self.assertIn("canonical_parquet_scan_dirty_documents", migration)
        self.assertIn("canonical_parquet_scan_fragment_documents", migration)
        self.assertIn("ON DELETE CASCADE", migration)
        self.assertIn("version=61", migration.replace(" ", "").replace("VALUES(61)", "version=61"))


class LogicalEnqueueDirtyDocumentTest(unittest.TestCase):
    def test_logical_enqueue_names_the_dirty_document_and_keeps_backfill(self):
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )

        connection = _SeedConnection()
        first = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)
        last = datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
        CanonicalLogicalEvidenceProjector._queue_parquet_scan(
            connection,
            tenant_id="tenant:test",
            source_id="source:test",
            ranges=((first, last),),
            reason="logical-update",
            logical_document_id="ldoc_test",
        )
        queue_query, dirty_query = connection.queries
        self.assertIn("canonical_parquet_scan_queue", queue_query)
        self.assertIn("WHEN canonical_parquet_scan_queue.reason='backfill'", queue_query)
        self.assertIn("canonical_parquet_scan_dirty_documents", dirty_query)
        self.assertEqual(connection.parameters[1][2], "ldoc_test")
        self.assertEqual(connection.parameters[1][3], "logical-update")

    def test_logical_enqueue_without_a_document_only_touches_the_queue(self):
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )

        connection = _SeedConnection()
        first = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)
        CanonicalLogicalEvidenceProjector._queue_parquet_scan(
            connection,
            tenant_id="tenant:test",
            source_id="source:test",
            ranges=((first, first),),
            reason="forget",
        )
        self.assertEqual(len(connection.queries), 1)


if __name__ == "__main__":
    unittest.main()
