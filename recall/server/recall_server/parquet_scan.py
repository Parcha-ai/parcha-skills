"""Rebuildable Parquet scan fragments over canonical evidence documents.

Each tenant/source/month is a set of ``{dataset}-part-{shard_index:05d}``
objects (fragments). The catalog records which logical documents each fragment
holds, so a change rewrites only the fragments that intersect the dirty
document set; everything else keeps its immutable, content-addressed object.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Iterator

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
SCAN_SCHEMA_VERSION = 2
SCAN_DATASETS = ("documents", "passages", "records", "actors")
MAX_SCAN_RECORDS = 5_000_000
MAX_PARQUET_OBJECT_BYTES = 48 * 1024 * 1024
PARQUET_RAW_SLICE_BYTES = 32 * 1024 * 1024
# A fragment closes on the next document boundary once it holds this many
# estimated row bytes, so a delta rebuild rewrites at most ~32 MiB per touched
# fragment (the encoded object ceiling stays MAX_PARQUET_OBJECT_BYTES).
FRAGMENT_TARGET_BYTES = 32 * 1024 * 1024
MAX_SHARD_INDEX = 99_999
DEFAULT_COMPACTION_FRAGMENTS = 16
SCAN_DIRTY_ALL = "*"
PART_TIME_BOUND_CHECKPOINT = 128

LOG = logging.getLogger(__name__)


class ParquetScanError(RuntimeError):
    """Content-free projection failure."""


def _month(value: date | datetime | str) -> date:
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            raise ParquetScanError("parquet_scan_bucket_invalid") from None
    elif isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        raise ParquetScanError("parquet_scan_bucket_invalid")
    if parsed.day != 1:
        raise ParquetScanError("parquet_scan_bucket_invalid")
    return parsed


def _next_month(value: date) -> date:
    return date(value.year + (value.month == 12), value.month % 12 + 1, 1)


def _timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ParquetScanError("parquet_scan_record_invalid") from None
    else:
        raise ParquetScanError("parquet_scan_record_invalid")
    if parsed.tzinfo is None:
        raise ParquetScanError("parquet_scan_record_invalid")
    return parsed.astimezone(timezone.utc)


def _reference(row: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    def field(name: str) -> Any:
        return row[prefix + name]

    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        "artifact_id": field("artifact_id"),
        "storage_backend": field("storage_backend"),
        "object_key": field("object_key"),
        "content_sha256": field("content_sha256"),
        "size_bytes": int(field("size_bytes")),
        "media_type": field("media_type"),
        "encryption": field("encryption"),
        "version_id": field("version_id"),
        "created_at": field("created_at").isoformat()
        if isinstance(field("created_at"), datetime)
        else field("created_at"),
    }


def _parquet_table_bytes(table: Any) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        compression_level=6,
        row_group_size=65_536,
        write_statistics=True,
        use_dictionary=True,
        data_page_version="2.0",
    )
    return sink.getvalue().to_pybytes()


def _parquet_bytes(rows: list[dict[str, Any]], schema: Any) -> bytes:
    return _parquet_table_bytes(pa.Table.from_pylist(rows, schema=schema))


def _estimated_row_bytes(row: dict[str, Any]) -> int:
    """Cheaply bound an Arrow conversion without copying record text."""

    size = 64 * len(row)
    for value in row.values():
        if isinstance(value, (str, bytes)):
            size += len(value)
        elif isinstance(value, (list, tuple)):
            size += sum(
                len(item) if isinstance(item, (str, bytes)) else 16 for item in value
            )
        else:
            size += 16
    return max(1, size)


def _parquet_parts(
    rows: list[dict[str, Any]],
    schema: Any,
    *,
    maximum_bytes: int = MAX_PARQUET_OBJECT_BYTES,
) -> list[tuple[bytes, int]]:
    """Encode ordered, independently readable parts below the archive ceiling."""

    if not 1 <= maximum_bytes <= MAX_PARQUET_OBJECT_BYTES:
        raise ParquetScanError("parquet_scan_budget_invalid")
    started = time.perf_counter()
    if not rows:
        table = pa.Table.from_pylist(rows, schema=schema)
        arrow_ms = round((time.perf_counter() - started) * 1_000)
        payload = _parquet_table_bytes(table)
        if len(payload) > maximum_bytes:
            raise ParquetScanError("parquet_scan_record_too_large")
        LOG.info(
            "parquet encode rows=0 arrow_bytes=%s parts=1 arrow_ms=%s encode_ms=%s",
            table.nbytes,
            arrow_ms,
            round((time.perf_counter() - started) * 1_000) - arrow_ms,
        )
        return [(payload, 0)]
    slice_bytes = min(PARQUET_RAW_SLICE_BYTES, maximum_bytes * 2 // 3)
    intervals: list[tuple[int, int]] = []
    interval_start = interval_bytes = 0
    for ordinal, row in enumerate(rows):
        row_bytes = _estimated_row_bytes(row)
        if ordinal > interval_start and interval_bytes + row_bytes > slice_bytes:
            intervals.append((interval_start, ordinal))
            interval_start = ordinal
            interval_bytes = 0
        interval_bytes += row_bytes
    intervals.append((interval_start, len(rows)))
    parts: list[tuple[int, bytes, int]] = []
    arrow_ms = arrow_bytes = 0
    for start, end in intervals:
        arrow_started = time.perf_counter()
        table = pa.Table.from_pylist(rows[start:end], schema=schema)
        arrow_ms += round((time.perf_counter() - arrow_started) * 1_000)
        arrow_bytes += table.nbytes
        pending = [(start, table)]
        while pending:
            offset, candidate = pending.pop()
            payload = _parquet_table_bytes(candidate)
            if len(payload) <= maximum_bytes:
                parts.append((offset, payload, candidate.num_rows))
                continue
            if candidate.num_rows <= 1:
                raise ParquetScanError("parquet_scan_record_too_large")
            left_rows = candidate.num_rows // 2
            pending.extend(
                (
                    (offset + left_rows, candidate.slice(left_rows)),
                    (offset, candidate.slice(0, left_rows)),
                )
            )
    result = [
        (payload, row_count)
        for _, payload, row_count in sorted(parts, key=lambda value: value[0])
    ]
    LOG.info(
        "parquet encode rows=%s arrow_bytes=%s parts=%s arrow_ms=%s encode_ms=%s",
        len(rows),
        arrow_bytes,
        len(result),
        arrow_ms,
        round((time.perf_counter() - started) * 1_000) - arrow_ms,
    )
    return result


def _schemas() -> dict[str, Any]:
    utc = pa.timestamp("us", tz="UTC")
    strings = pa.list_(pa.string())
    return {
        "documents": pa.schema(
            [
                ("schema_version", pa.int16()),
                ("tenant_id", pa.string()),
                ("source_id", pa.string()),
                ("logical_document_id", pa.string()),
                ("revision", pa.int32()),
                ("first_occurred_at", utc),
                ("last_occurred_at", utc),
                ("record_count", pa.int64()),
                ("part_count", pa.int32()),
                ("document_content_sha256", pa.string()),
                ("actor_ids", strings),
                ("actor_names", strings),
                ("actor_relations", strings),
            ]
        ),
        "records": pa.schema(
            [
                ("schema_version", pa.int16()),
                ("tenant_id", pa.string()),
                ("source_id", pa.string()),
                ("logical_document_id", pa.string()),
                ("revision", pa.int32()),
                ("ordinal", pa.int64()),
                ("occurred_at", utc),
                ("event_kind", pa.string()),
                ("roles", strings),
                ("receipts", strings),
                ("actor_ids", strings),
                ("actor_names", strings),
                ("actor_relations", strings),
                ("search_text", pa.large_string()),
                ("record_json", pa.large_string()),
            ]
        ),
        "passages": pa.schema(
            [
                ("schema_version", pa.int16()),
                ("tenant_id", pa.string()),
                ("source_id", pa.string()),
                ("logical_document_id", pa.string()),
                ("revision", pa.int32()),
                ("passage_id", pa.string()),
                ("ordinal", pa.int32()),
                ("first_occurred_at", utc),
                ("last_occurred_at", utc),
                ("token_count", pa.int32()),
                ("roles", strings),
                ("receipts", strings),
                ("actor_ids", strings),
                ("actor_names", strings),
                ("actor_relations", strings),
                ("text", pa.large_string()),
            ]
        ),
        "actors": pa.schema(
            [
                ("schema_version", pa.int16()),
                ("tenant_id", pa.string()),
                ("source_id", pa.string()),
                ("logical_document_id", pa.string()),
                ("revision", pa.int32()),
                ("record_ordinal", pa.int64()),
                ("actor_id", pa.string()),
                ("display_name", pa.string()),
                ("relation", pa.string()),
            ]
        ),
    }



@dataclass(frozen=True)
class ScanCandidate:
    tenant_id: str
    source_id: str
    bucket_start: date
    generation: int
    changed_at: datetime
    reason: str = "logical-update"


@dataclass(frozen=True)
class FragmentMember:
    """One logical document as recorded inside one live fragment."""

    logical_document_id: str
    revision: int
    generation_sha256: str


@dataclass(frozen=True)
class ScanCatalog:
    """The live catalog of one source-month: parts, their documents, dirty set."""

    shards: dict[tuple[str, int], dict[str, Any]]
    members: dict[tuple[str, int], tuple[FragmentMember, ...]]
    dirty: frozenset[str]
    compaction: bool = False

    def document_parts(self) -> dict[str, set[tuple[str, int]]]:
        parts: dict[str, set[tuple[str, int]]] = {}
        for identity, members in self.members.items():
            for member in members:
                parts.setdefault(member.logical_document_id, set()).add(identity)
        return parts

    def fragment_count(self) -> int:
        counts: dict[str, int] = {}
        for dataset, _ in self.shards:
            counts[dataset] = counts.get(dataset, 0) + 1
        return max(counts.values(), default=0)


@dataclass(frozen=True)
class ScanUpload:
    generation_sha256: str
    references: dict[tuple[str, int], dict[str, Any]]
    row_counts: dict[tuple[str, int], int]
    first_occurred_at: datetime | None
    last_occurred_at: datetime | None
    created: bool
    removed: tuple[tuple[str, int], ...] = ()
    members: dict[tuple[str, int], tuple[FragmentMember, ...]] = field(
        default_factory=dict
    )
    bounds: dict[tuple[str, int], tuple[datetime | None, datetime | None]] = field(
        default_factory=dict
    )
    mode: str = "full"
    documents_dirty: int = 0
    documents_rewritten: int = 0


@dataclass(frozen=True)
class DocumentProjection:
    records: list[dict[str, Any]]
    actors: list[dict[str, Any]]
    record_count: int
    first_occurred_at: datetime | None
    last_occurred_at: datetime | None
    part_bounds: tuple[PartTimeBound, ...]


@dataclass(frozen=True)
class PartTimeBound:
    tenant_id: str
    source_id: str
    logical_document_id: str
    revision: int
    part_ordinal: int
    content_sha256: str
    first_occurred_at: datetime
    last_occurred_at: datetime


def _row_bounds(dataset: str, row: dict[str, Any]) -> tuple[Any, Any]:
    if dataset == "records":
        return row.get("occurred_at"), row.get("occurred_at")
    if dataset in ("documents", "passages"):
        return row.get("first_occurred_at"), row.get("last_occurred_at")
    return None, None


class _StreamingUpload:
    """Upload bounded Parquet fragments without retaining a month of rows.

    A fragment closes on a document boundary once it holds at least
    ``FRAGMENT_TARGET_BYTES`` of estimated row bytes, so one logical document
    normally lives in exactly one part per dataset. A single document larger
    than a raw slice still spans several parts; the catalog records it in each
    and the delta planner treats those parts as one unit.
    """

    def __init__(
        self,
        projector: "CanonicalParquetScanProjector",
        candidate: ScanCandidate,
        *,
        generation: str,
        created_at: datetime,
        next_indexes: dict[str, int] | None = None,
    ):
        self.projector = projector
        self.candidate = candidate
        self.generation = generation
        self.created_at = created_at
        self.schemas = _schemas()
        self.buffers = {dataset: [] for dataset in SCAN_DATASETS}
        self.buffer_bytes = {dataset: 0 for dataset in SCAN_DATASETS}
        self.references: dict[tuple[str, int], dict[str, Any]] = {}
        self.row_counts: dict[tuple[str, int], int] = {}
        self.members: dict[tuple[str, int], tuple[FragmentMember, ...]] = {}
        self.bounds: dict[
            tuple[str, int], tuple[datetime | None, datetime | None]
        ] = {}
        self.part_indexes = {
            dataset: int((next_indexes or {}).get(dataset, 0))
            for dataset in SCAN_DATASETS
        }
        self.rows_seen = {dataset: 0 for dataset in SCAN_DATASETS}
        self.current_document: dict[str, str | None] = {
            dataset: None for dataset in SCAN_DATASETS
        }
        self.pending_members: dict[str, dict[str, FragmentMember]] = {
            dataset: {} for dataset in SCAN_DATASETS
        }
        self.pending_bounds: dict[str, tuple[datetime | None, datetime | None]] = {
            dataset: (None, None) for dataset in SCAN_DATASETS
        }
        self.flushes = 0
        self.upload_ms = 0
        self.maximum_buffer_bytes = 0

    def claim(self, dataset: str, member: FragmentMember) -> None:
        """Record document ownership in the fragment currently open."""

        if self.current_document[dataset] != member.logical_document_id:
            if (
                self.buffers[dataset]
                and self.buffer_bytes[dataset] >= FRAGMENT_TARGET_BYTES
            ):
                self._flush(dataset)
            self.current_document[dataset] = member.logical_document_id
        self.pending_members[dataset][member.logical_document_id] = member

    def add(self, dataset: str, row: dict[str, Any], *, member: FragmentMember) -> None:
        self.claim(dataset, member)
        row_bytes = _estimated_row_bytes(row)
        if self.buffers[dataset] and (
            self.buffer_bytes[dataset] + row_bytes > PARQUET_RAW_SLICE_BYTES
        ):
            self._flush(dataset)
            # The document continues in the next part.
            self.pending_members[dataset][member.logical_document_id] = member
        self.buffers[dataset].append(row)
        self.buffer_bytes[dataset] += row_bytes
        first, last = _row_bounds(dataset, row)
        known_first, known_last = self.pending_bounds[dataset]
        if isinstance(first, datetime):
            known_first = first if known_first is None else min(known_first, first)
        if isinstance(last, datetime):
            known_last = last if known_last is None else max(known_last, last)
        self.pending_bounds[dataset] = (known_first, known_last)
        self.maximum_buffer_bytes = max(
            self.maximum_buffer_bytes,
            self.buffer_bytes[dataset],
        )
        self.rows_seen[dataset] += 1

    def _flush(self, dataset: str, *, allow_empty: bool = False) -> None:
        rows = self.buffers[dataset]
        if not rows and not allow_empty:
            return
        self.buffers[dataset] = []
        self.buffer_bytes[dataset] = 0
        members = tuple(self.pending_members[dataset].values())
        self.pending_members[dataset] = {}
        bounds = self.pending_bounds[dataset]
        self.pending_bounds[dataset] = (None, None)
        self.current_document[dataset] = None
        for payload, row_count in _parquet_parts(rows, self.schemas[dataset]):
            shard_index = self.part_indexes[dataset]
            if shard_index > MAX_SHARD_INDEX:
                raise ParquetScanError("parquet_scan_shard_index_exhausted")
            identity = (dataset, shard_index)
            uploaded_at = time.perf_counter()
            self.references[identity] = self.projector.archive.put_raw(
                tenant_id=self.candidate.tenant_id,
                source_id=self.candidate.source_id,
                native_id=(
                    f"parquet-scan:{self.candidate.bucket_start.isoformat()}:"
                    f"{dataset}:{shard_index}:{self.generation}"
                ),
                payload=payload,
                media_type=PARQUET_MEDIA_TYPE,
                created_at=self.created_at.isoformat().replace("+00:00", "Z"),
            )
            self.upload_ms += round((time.perf_counter() - uploaded_at) * 1_000)
            self.row_counts[identity] = row_count
            # A flush split by the object ceiling maps every document of the
            # flush to every resulting part: a superset is safe for the delta
            # planner (it over-selects victims) and never loses ownership.
            self.members[identity] = members
            self.bounds[identity] = bounds if row_count else (None, None)
            self.part_indexes[dataset] += 1
        self.flushes += 1

    def finish(
        self,
        *,
        first: datetime | None,
        last: datetime | None,
        ensure_datasets: Iterable[str] = SCAN_DATASETS,
        removed: tuple[tuple[str, int], ...] = (),
        mode: str = "full",
        documents_dirty: int = 0,
        documents_rewritten: int = 0,
    ) -> ScanUpload:
        ensure = set(ensure_datasets)
        if not ensure <= set(SCAN_DATASETS):
            raise ParquetScanError("parquet_scan_dataset_invalid")
        for dataset in SCAN_DATASETS:
            self._flush(
                dataset,
                allow_empty=dataset in ensure and self.rows_seen[dataset] == 0,
            )
        LOG.info(
            "parquet stream documents=%s passages=%s records=%s actors=%s parts=%s "
            "flushes=%s max_buffer_bytes=%s upload_ms=%s mode=%s",
            self.rows_seen["documents"],
            self.rows_seen["passages"],
            self.rows_seen["records"],
            self.rows_seen["actors"],
            len(self.references),
            self.flushes,
            self.maximum_buffer_bytes,
            self.upload_ms,
            mode,
        )
        return ScanUpload(
            self.generation,
            self.references,
            self.row_counts,
            first,
            last,
            True,
            removed=removed,
            members=dict(self.members),
            bounds=dict(self.bounds),
            mode=mode,
            documents_dirty=documents_dirty,
            documents_rewritten=documents_rewritten,
        )

    def abort(self) -> None:
        self.projector._schedule_cleanup(self.references.values())


class CanonicalParquetScanProjector:
    """Materialize source/month fragments without changing canonical evidence.

    Objects are never overwritten: a rebuild uploads fresh content-addressed
    parts under shard indexes no live part uses, flips the catalog in one
    transaction, and hands the replaced parts to the evidence cleanup queue.
    """

    def __init__(
        self,
        store: Any,
        evidence_projection: Any,
        *,
        compaction_fragments: int | None = None,
    ):
        self.store = store
        self.archive = evidence_projection.archive
        if compaction_fragments is None:
            compaction_fragments = int(
                os.environ.get(
                    "RECALL_PARQUET_COMPACTION_FRAGMENTS",
                    DEFAULT_COMPACTION_FRAGMENTS,
                )
            )
        if not 1 <= compaction_fragments <= MAX_SHARD_INDEX:
            raise ParquetScanError("parquet_scan_budget_invalid")
        self.compaction_fragments = compaction_fragments

    def seed_backfill(
        self,
        *,
        tenant_id: str,
        source_id: str | None = None,
    ) -> int:
        with self.store.connect() as connection:
            with connection.transaction():
                result = connection.execute(
                    """INSERT INTO canonical_parquet_scan_queue(
                           tenant_id,source_id,bucket_start,
                           generation,reason,changed_at
                       )
                       SELECT DISTINCT document.tenant_id,document.source_id,
                              month.value::date,1,'backfill',statement_timestamp()
                         FROM canonical_evidence_documents document
                         CROSS JOIN LATERAL generate_series(
                             date_trunc('month',document.first_occurred_at),
                             date_trunc('month',document.last_occurred_at),
                             interval '1 month'
                         ) month(value)
                        WHERE document.tenant_id=%s
                          AND (%s::text IS NULL OR document.source_id=%s)
                       ON CONFLICT(tenant_id,source_id,bucket_start)
                       DO UPDATE SET
                           generation=canonical_parquet_scan_queue.generation+1,
                           reason='backfill',changed_at=clock_timestamp()""",
                    (tenant_id, source_id, source_id),
                )
                connection.execute(
                    """INSERT INTO canonical_parquet_scan_dirty_documents(
                           tenant_id,source_id,bucket_start,
                           logical_document_id,reason,queued_at
                       )
                       SELECT DISTINCT document.tenant_id,document.source_id,
                              month.value::date,%s,'backfill',statement_timestamp()
                         FROM canonical_evidence_documents document
                         CROSS JOIN LATERAL generate_series(
                             date_trunc('month',document.first_occurred_at),
                             date_trunc('month',document.last_occurred_at),
                             interval '1 month'
                         ) month(value)
                        WHERE document.tenant_id=%s
                          AND (%s::text IS NULL OR document.source_id=%s)
                       ON CONFLICT DO NOTHING""",
                    (SCAN_DIRTY_ALL, tenant_id, source_id, source_id),
                )
        return max(0, result.rowcount)

    def _pending(self, *, tenant_id: str | None, limit: int) -> list[ScanCandidate]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT tenant_id,source_id,bucket_start,generation,changed_at,
                          reason
                     FROM canonical_parquet_scan_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s)
                    ORDER BY changed_at,tenant_id,source_id,bucket_start
                    LIMIT %s""",
                (tenant_id, tenant_id, limit),
            ).fetchall()
        return [
            ScanCandidate(
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                bucket_start=_month(row["bucket_start"]),
                generation=int(row["generation"]),
                changed_at=row["changed_at"],
                reason=str(row.get("reason") or "logical-update"),
            )
            for row in rows
        ]

    @contextmanager
    def _candidate_lease(self, candidate: ScanCandidate) -> Iterator[bool]:
        """Hold a database-session lease while one source-month is built."""

        identity = (
            "parquet-scan-build\x1f"
            + candidate.tenant_id
            + "\x1f"
            + candidate.source_id
            + "\x1f"
            + candidate.bucket_start.isoformat()
        )
        with self.store.connect() as connection:
            acquired = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS acquired",
                (identity,),
            ).fetchone()["acquired"]
            if acquired is not True:
                yield False
                return
            try:
                yield True
            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                    (identity,),
                )

    def _documents(self, candidate: ScanCandidate) -> list[dict[str, Any]]:
        """Load month document metadata (no raw parts) ordered by document id."""

        bucket_end = _next_month(candidate.bucket_start)
        with self.store.connect() as connection:
            documents = connection.execute(
                """SELECT document.*,
                          pointer.policy_fingerprint AS passage_policy_fingerprint,
                          pointer.source_document_sha256 AS passage_source_sha256,
                          pointer.passage_count,
                          coalesce(attributed.links,'[]'::jsonb) AS actor_links
                     FROM canonical_evidence_documents document
                     LEFT JOIN canonical_passage_documents pointer
                       ON pointer.tenant_id=document.tenant_id
                      AND pointer.source_id=document.source_id
                      AND pointer.logical_document_id=document.logical_document_id
                     LEFT JOIN LATERAL (
                          SELECT jsonb_agg(jsonb_build_object(
                                     'actor_id',link.actor_id,
                                     'display_name',actor.display_name,
                                     'relation',link.relation
                                 ) ORDER BY link.actor_id,link.relation) AS links
                            FROM canonical_evidence_document_actors link
                            JOIN brain_actors actor
                              ON actor.tenant_id=link.tenant_id
                             AND actor.actor_id=link.actor_id
                           WHERE link.tenant_id=document.tenant_id
                             AND link.source_id=document.source_id
                             AND link.logical_document_id=
                                 document.logical_document_id
                     ) attributed ON true
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND document.last_occurred_at >= %s
                      AND document.first_occurred_at < %s
                    ORDER BY document.logical_document_id""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    candidate.bucket_start,
                    bucket_end,
                ),
            ).fetchall()
        return documents

    def _attach_parts(
        self,
        candidate: ScanCandidate,
        documents: list[dict[str, Any]],
    ) -> None:
        """Attach the raw-part references of exactly these documents."""

        for document in documents:
            document["parts"] = []
        if not documents:
            return
        bucket_end = _next_month(candidate.bucket_start)
        by_document = {
            document["logical_document_id"]: document for document in documents
        }
        with self.store.connect() as connection:
            parts = connection.execute(
                """SELECT part.*
                     FROM canonical_evidence_document_parts part
                     JOIN canonical_evidence_documents document
                       USING(tenant_id,source_id,logical_document_id,revision)
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND document.logical_document_id=ANY(%s)
                      AND (
                          part.first_occurred_at IS NULL
                          OR (
                              part.last_occurred_at >= %s
                              AND part.first_occurred_at < %s
                          )
                      )
                    ORDER BY part.logical_document_id,part.part_ordinal""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    list(by_document),
                    candidate.bucket_start,
                    bucket_end,
                ),
            ).fetchall()
        for part in parts:
            document = by_document.get(part["logical_document_id"])
            if document is not None:
                document["parts"].append(part)

    def _passages(
        self,
        candidate: ScanCandidate,
        document_ids: list[str],
    ) -> Iterator[dict[str, Any]]:
        """Read the compact pointer plane grouped by document, then by time."""

        if not document_ids:
            return
        bucket_end = _next_month(candidate.bucket_start)
        with self.store.connect() as connection:
            with connection.cursor(name="parquet_passage_stream") as cursor:
                cursor.itersize = 2_000
                cursor.execute(
                    """SELECT passage.logical_document_id,passage.revision,
                          passage.passage_id,passage.ordinal,
                          passage.first_occurred_at,passage.last_occurred_at,
                          passage.token_count,passage.roles,passage.receipts,
                          passage.text_redacted,
                          coalesce(attributed.actor_ids,ARRAY[]::text[]) AS actor_ids,
                          coalesce(attributed.actor_names,ARRAY[]::text[]) AS actor_names,
                          coalesce(attributed.actor_relations,ARRAY[]::text[])
                              AS actor_relations
                     FROM canonical_passages passage
                     LEFT JOIN LATERAL (
                          SELECT array_agg(link.actor_id ORDER BY link.actor_id,link.relation)
                                     AS actor_ids,
                                 array_agg(actor.display_name ORDER BY link.actor_id,link.relation)
                                     AS actor_names,
                                 array_agg(link.relation ORDER BY link.actor_id,link.relation)
                                     AS actor_relations
                            FROM canonical_passage_actors link
                            JOIN brain_actors actor
                              ON actor.tenant_id=link.tenant_id
                             AND actor.actor_id=link.actor_id
                           WHERE link.tenant_id=passage.tenant_id
                             AND link.source_id=passage.source_id
                             AND link.passage_id=passage.passage_id
                     ) attributed ON true
                    WHERE passage.tenant_id=%s AND passage.source_id=%s
                      AND passage.logical_document_id=ANY(%s)
                      AND passage.last_occurred_at >= %s
                      AND passage.first_occurred_at < %s
                    ORDER BY passage.logical_document_id,
                             passage.first_occurred_at,
                             passage.last_occurred_at,passage.passage_id""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        document_ids,
                        candidate.bucket_start,
                        bucket_end,
                    ),
                )
                yield from cursor

    def _catalog(self, candidate: ScanCandidate) -> ScanCatalog:
        """Read the live parts, their document membership, and the dirty set."""

        scope = (candidate.tenant_id, candidate.source_id, candidate.bucket_start)
        with self.store.connect() as connection:
            shards = connection.execute(
                """SELECT * FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                    ORDER BY dataset,shard_index""",
                scope,
            ).fetchall()
            members = connection.execute(
                """SELECT dataset,shard_index,logical_document_id,revision,
                          generation_sha256
                     FROM canonical_parquet_scan_fragment_documents
                    WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                    ORDER BY dataset,shard_index,logical_document_id""",
                scope,
            ).fetchall()
            dirty = connection.execute(
                """SELECT logical_document_id,reason
                     FROM canonical_parquet_scan_dirty_documents
                    WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s""",
                scope,
            ).fetchall()
        grouped: dict[tuple[str, int], list[FragmentMember]] = {}
        for row in members:
            grouped.setdefault((row["dataset"], int(row["shard_index"])), []).append(
                FragmentMember(
                    str(row["logical_document_id"]),
                    int(row["revision"]),
                    str(row["generation_sha256"]),
                )
            )
        return ScanCatalog(
            shards={(row["dataset"], int(row["shard_index"])): row for row in shards},
            members={identity: tuple(value) for identity, value in grouped.items()},
            dirty=frozenset(str(row["logical_document_id"]) for row in dirty),
            compaction=any(
                str(row["logical_document_id"]) == SCAN_DIRTY_ALL
                and str(row.get("reason")) == "compaction"
                for row in dirty
            ),
        )

    @staticmethod
    def _actor_columns(
        links: Iterable[dict[str, Any]],
    ) -> tuple[list[str], list[str], list[str]]:
        ordered = sorted(
            {
                (
                    str(link.get("actor_id", "")),
                    str(link.get("display_name", "")),
                    str(link.get("relation", "")),
                )
                for link in links
                if isinstance(link, dict)
                and link.get("actor_id")
                and link.get("relation")
            }
        )
        return (
            [value[0] for value in ordered],
            [value[1] for value in ordered],
            [value[2] for value in ordered],
        )

    @staticmethod
    def _record_content(record: dict[str, Any]) -> str:
        content = record.get("text", record.get("content_fragment"))
        if isinstance(content, str):
            return content
        return orjson.dumps(
            record.get("content", record),
            option=orjson.OPT_SORT_KEYS,
        ).decode()

    def _project_document(
        self,
        candidate: ScanCandidate,
        document: dict[str, Any],
        *,
        bucket_start: datetime,
        bucket_end: datetime,
        record_budget: int,
        record_sink: Callable[[dict[str, Any]], None] | None = None,
        actor_sink: Callable[[dict[str, Any]], None] | None = None,
        collect: bool = True,
    ) -> DocumentProjection:
        document_links = document.get("actor_links") or []
        actor_names = {
            link.get("actor_id"): link.get("display_name", "")
            for link in document_links
            if isinstance(link, dict)
        }
        records: list[dict[str, Any]] = []
        actors: list[dict[str, Any]] = []
        discovered_bounds: list[PartTimeBound] = []
        record_count = 0
        first = last = None
        for part in document["parts"]:
            known_first = part.get("first_occurred_at")
            known_last = part.get("last_occurred_at")
            if (known_first is None) != (known_last is None):
                raise ParquetScanError("parquet_scan_state_invalid")
            if known_first is not None:
                part_first = _timestamp(known_first)
                part_last = _timestamp(known_last)
                if part_first > part_last:
                    raise ParquetScanError("parquet_scan_state_invalid")
                if part_last < bucket_start or part_first >= bucket_end:
                    continue
            payload = self.archive.read_raw(_reference(part))
            observed_first = observed_last = None
            for line in payload.splitlines():
                try:
                    record = orjson.loads(line)
                except orjson.JSONDecodeError:
                    raise ParquetScanError("parquet_scan_record_invalid") from None
                if not isinstance(record, dict):
                    raise ParquetScanError("parquet_scan_record_invalid")
                occurred_at = _timestamp(record.get("occurred_at"))
                observed_first = (
                    occurred_at
                    if observed_first is None
                    else min(observed_first, occurred_at)
                )
                observed_last = (
                    occurred_at
                    if observed_last is None
                    else max(observed_last, occurred_at)
                )
                if not bucket_start <= occurred_at < bucket_end:
                    continue
                if record_count >= record_budget:
                    raise ParquetScanError("parquet_scan_budget_exceeded")
                links = record.get("actor_links")
                if not isinstance(links, list):
                    links = []
                enriched = [
                    {
                        **link,
                        "display_name": actor_names.get(link.get("actor_id"), ""),
                    }
                    for link in links
                    if isinstance(link, dict)
                ]
                ids, names, relations = self._actor_columns(enriched)
                ordinal = int(record.get("ordinal", -1))
                if ordinal < 0:
                    raise ParquetScanError("parquet_scan_record_invalid")
                projected_record = {
                    "schema_version": SCAN_SCHEMA_VERSION,
                    "tenant_id": candidate.tenant_id,
                    "source_id": candidate.source_id,
                    "logical_document_id": document["logical_document_id"],
                    "revision": int(document["revision"]),
                    "ordinal": ordinal,
                    "occurred_at": occurred_at,
                    "event_kind": str(record.get("event_kind", "unknown")),
                    "roles": sorted({str(value) for value in record.get("roles", [])}),
                    "receipts": [str(value) for value in record.get("receipts", [])],
                    "actor_ids": ids,
                    "actor_names": names,
                    "actor_relations": relations,
                    "search_text": self._record_content(record),
                    "record_json": orjson.dumps(
                        record,
                        option=orjson.OPT_SORT_KEYS,
                    ).decode(),
                }
                if record_sink is not None:
                    record_sink(projected_record)
                if collect:
                    records.append(projected_record)
                record_count += 1
                projected_actors = [
                    {
                        "schema_version": SCAN_SCHEMA_VERSION,
                        "tenant_id": candidate.tenant_id,
                        "source_id": candidate.source_id,
                        "logical_document_id": document["logical_document_id"],
                        "revision": int(document["revision"]),
                        "record_ordinal": ordinal,
                        "actor_id": actor_id,
                        "display_name": display_name,
                        "relation": relation,
                    }
                    for actor_id, display_name, relation in zip(
                        ids, names, relations, strict=True
                    )
                ]
                if actor_sink is not None:
                    for projected_actor in projected_actors:
                        actor_sink(projected_actor)
                if collect:
                    actors.extend(projected_actors)
                first = occurred_at if first is None else min(first, occurred_at)
                last = occurred_at if last is None else max(last, occurred_at)
            if known_first is None:
                if observed_first is None or observed_last is None:
                    raise ParquetScanError("parquet_scan_record_invalid")
                discovered_bounds.append(
                    PartTimeBound(
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        logical_document_id=document["logical_document_id"],
                        revision=int(document["revision"]),
                        part_ordinal=int(part["part_ordinal"]),
                        content_sha256=str(part["content_sha256"]),
                        first_occurred_at=observed_first,
                        last_occurred_at=observed_last,
                    )
                )
                if len(discovered_bounds) >= PART_TIME_BOUND_CHECKPOINT:
                    self._persist_part_bounds(discovered_bounds)
                    discovered_bounds.clear()
        return DocumentProjection(
            records,
            actors,
            record_count,
            first,
            last,
            tuple(discovered_bounds),
        )

    def _persist_part_bounds(self, bounds: list[PartTimeBound]) -> None:
        if not bounds:
            return
        with self.store.connect() as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.executemany(
                        """UPDATE canonical_evidence_document_parts
                              SET first_occurred_at=%s,last_occurred_at=%s
                            WHERE tenant_id=%s AND source_id=%s
                              AND logical_document_id=%s AND revision=%s
                              AND part_ordinal=%s AND content_sha256=%s
                              AND first_occurred_at IS NULL
                              AND last_occurred_at IS NULL""",
                        [
                            (
                                bound.first_occurred_at,
                                bound.last_occurred_at,
                                bound.tenant_id,
                                bound.source_id,
                                bound.logical_document_id,
                                bound.revision,
                                bound.part_ordinal,
                                bound.content_sha256,
                            )
                            for bound in bounds
                        ],
                    )
        LOG.info("parquet part time bounds observed=%s", len(bounds))

    def _schedule_cleanup(self, references: Iterable[dict[str, Any]]) -> None:
        references = tuple(references)
        if not references:
            return
        with self.store.connect() as connection:
            with connection.transaction():
                self._enqueue_cleanup(connection, references)

    @staticmethod
    def _fingerprint(document: dict[str, Any]) -> str:
        """Hash every input that changes a document's projected rows."""

        ids, names, relations = CanonicalParquetScanProjector._actor_columns(
            document.get("actor_links") or []
        )
        return hashlib.sha256(
            orjson.dumps(
                {
                    "actors": list(zip(ids, names, relations, strict=True)),
                    "content": document["document_content_sha256"],
                    "document": document["logical_document_id"],
                    "passage_count": document.get("passage_count"),
                    "passage_policy": document.get("passage_policy_fingerprint"),
                    "passage_source": document.get("passage_source_sha256"),
                    "revision": int(document["revision"]),
                    "schema": SCAN_SCHEMA_VERSION,
                },
                option=orjson.OPT_SORT_KEYS,
            )
        ).hexdigest()

    @staticmethod
    def _generation(candidate: ScanCandidate, documents: list[dict[str, Any]]) -> str:
        """Hash of one fragment's (or month's) document fingerprints."""

        digest = hashlib.sha256(
            f"recall.parquet-scan.v{SCAN_SCHEMA_VERSION}\0"
            f"{candidate.bucket_start.isoformat()}\n".encode()
        )
        for document in documents:
            digest.update(
                CanonicalParquetScanProjector._fingerprint(document).encode()
            )
            digest.update(b"\n")
        return digest.hexdigest()

    def _streaming_upload(
        self,
        candidate: ScanCandidate,
        *,
        generation: str,
        created_at: datetime,
        next_indexes: dict[str, int] | None = None,
    ) -> _StreamingUpload:
        return _StreamingUpload(
            self,
            candidate,
            generation=generation,
            created_at=created_at,
            next_indexes=next_indexes,
        )

    @staticmethod
    def _passage_row(
        candidate: ScanCandidate,
        passage: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": SCAN_SCHEMA_VERSION,
            "tenant_id": candidate.tenant_id,
            "source_id": candidate.source_id,
            "logical_document_id": passage["logical_document_id"],
            "revision": int(passage["revision"]),
            "passage_id": passage["passage_id"],
            "ordinal": int(passage["ordinal"]),
            "first_occurred_at": _timestamp(passage["first_occurred_at"]),
            "last_occurred_at": _timestamp(passage["last_occurred_at"]),
            "token_count": int(passage["token_count"]),
            "roles": [str(value) for value in passage["roles"]],
            "receipts": [str(value) for value in passage["receipts"]],
            "actor_ids": [str(value) for value in passage["actor_ids"]],
            "actor_names": [str(value) for value in passage["actor_names"]],
            "actor_relations": [str(value) for value in passage["actor_relations"]],
            "text": passage["text_redacted"],
        }

    def _plan(
        self,
        candidate: ScanCandidate,
        documents: list[dict[str, Any]],
        catalog: ScanCatalog,
    ) -> tuple[str, set[tuple[str, int]], set[str], set[str]]:
        """Decide what to rewrite.

        Returns ``(mode, victims, rewrite, dirty)``: ``mode`` is ``reuse``
        (nothing changed), ``delta`` (only intersecting fragments), ``full``
        (every fragment; backfill or first build), or ``compaction`` (a full
        rebuild forced by fragment count or dead-document ratio).
        """

        current = {
            document["logical_document_id"]: self._fingerprint(document)
            for document in documents
        }
        document_parts = catalog.document_parts()
        recorded: dict[str, set[str]] = {}
        for members in catalog.members.values():
            for member in members:
                recorded.setdefault(member.logical_document_id, set()).add(
                    member.generation_sha256
                )
        stale = {
            document_id
            for document_id, fingerprints in recorded.items()
            if document_id not in current or fingerprints != {current[document_id]}
        }
        uncovered = {
            document_id for document_id in current if document_id not in recorded
        }
        explicit = catalog.dirty - {SCAN_DIRTY_ALL}
        dirty = (explicit | stale | uncovered) & (set(current) | set(recorded))
        all_parts = set(catalog.shards)
        datasets_complete = {dataset for dataset, _ in all_parts} == set(
            SCAN_DATASETS
        )
        if not current:
            # No documents overlap the month: drop whatever is still listed.
            if not all_parts:
                return "reuse", set(), set(), dirty
            return "full", all_parts, set(), dirty
        if catalog.compaction:
            return "compaction", all_parts, set(current), dirty
        if not stale and not uncovered and datasets_complete:
            # Content-identical: keep every immutable object, clear the queue.
            return "reuse", set(), set(), dirty
        if (
            candidate.reason == "backfill"
            or SCAN_DIRTY_ALL in catalog.dirty
            or not catalog.members
            or not datasets_complete
        ):
            return "full", all_parts, set(current), dirty
        if catalog.fragment_count() > self.compaction_fragments or (
            len(stale) * 2 > len(recorded)
        ):
            return "compaction", all_parts, set(current), dirty
        effective = stale | uncovered
        victims: set[tuple[str, int]] = set()
        frontier = set(effective)
        while frontier:
            document_id = frontier.pop()
            for identity in document_parts.get(document_id, ()):
                if identity in victims:
                    continue
                victims.add(identity)
                for member in catalog.members.get(identity, ()):
                    if member.logical_document_id not in effective:
                        effective.add(member.logical_document_id)
                        frontier.add(member.logical_document_id)
        rewrite = {document_id for document_id in effective if document_id in current}
        if not victims and not rewrite:
            return "reuse", set(), set(), dirty
        return (
            "full" if victims == all_parts else "delta",
            victims,
            rewrite,
            dirty,
        )

    def _build(self, candidate: ScanCandidate) -> ScanUpload:
        started = time.perf_counter()
        documents = self._documents(candidate)
        catalog = self._catalog(candidate)
        mode, victims, rewrite, dirty = self._plan(candidate, documents, catalog)
        metadata_ms = round((time.perf_counter() - started) * 1_000)
        if mode == "reuse":
            LOG.info(
                "parquet build documents=%s fragments=%s dirty=%s metadata_ms=%s "
                "total_ms=%s mode=reuse",
                len(documents),
                len(catalog.shards),
                len(dirty),
                metadata_ms,
                round((time.perf_counter() - started) * 1_000),
            )
            return ScanUpload(
                self._generation(candidate, documents),
                {identity: _reference(row) for identity, row in catalog.shards.items()},
                {
                    identity: int(row["row_count"])
                    for identity, row in catalog.shards.items()
                },
                None,
                None,
                False,
                mode="reuse",
                documents_dirty=len(dirty),
            )
        bucket_start = datetime.combine(
            candidate.bucket_start,
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        bucket_end = datetime.combine(
            _next_month(candidate.bucket_start),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        rewrite_documents = [
            document
            for document in documents
            if document["logical_document_id"] in rewrite
        ]
        self._attach_parts(candidate, rewrite_documents)
        surviving = {
            dataset: [
                shard_index
                for (part_dataset, shard_index) in catalog.shards
                if part_dataset == dataset and (dataset, shard_index) not in victims
            ]
            for dataset in SCAN_DATASETS
        }
        next_indexes = {
            dataset: (max(indexes) + 1 if indexes else 0)
            for dataset, indexes in surviving.items()
        }
        ensure_datasets = tuple(
            dataset for dataset, indexes in surviving.items() if not indexes
        )
        upload = self._streaming_upload(
            candidate,
            generation=self._generation(candidate, rewrite_documents),
            created_at=bucket_start,
            next_indexes=next_indexes,
        )
        members = {
            document["logical_document_id"]: FragmentMember(
                document["logical_document_id"],
                int(document["revision"]),
                self._fingerprint(document),
            )
            for document in rewrite_documents
        }
        first = last = None
        discovered_bounds: list[PartTimeBound] = []
        removed = tuple(sorted(victims))
        try:
            for passage in self._passages(candidate, sorted(members)):
                upload.add(
                    "passages",
                    self._passage_row(candidate, passage),
                    member=members[passage["logical_document_id"]],
                )
            for document in rewrite_documents:
                member = members[document["logical_document_id"]]
                upload.claim("documents", member)
                links = document.get("actor_links") or []
                projected = self._project_document(
                    candidate,
                    document,
                    bucket_start=bucket_start,
                    bucket_end=bucket_end,
                    record_budget=(MAX_SCAN_RECORDS - upload.rows_seen["records"]),
                    record_sink=lambda row, member=member: upload.add(
                        "records", row, member=member
                    ),
                    actor_sink=lambda row, member=member: upload.add(
                        "actors", row, member=member
                    ),
                    collect=False,
                )
                discovered_bounds.extend(projected.part_bounds)
                if projected.record_count == 0:
                    continue
                if (
                    projected.first_occurred_at is None
                    or projected.last_occurred_at is None
                ):
                    raise ParquetScanError("parquet_scan_state_invalid")
                ids, names, relations = self._actor_columns(links)
                upload.add(
                    "documents",
                    {
                        "schema_version": SCAN_SCHEMA_VERSION,
                        "tenant_id": candidate.tenant_id,
                        "source_id": candidate.source_id,
                        "logical_document_id": document["logical_document_id"],
                        "revision": int(document["revision"]),
                        "first_occurred_at": projected.first_occurred_at,
                        "last_occurred_at": projected.last_occurred_at,
                        "record_count": projected.record_count,
                        "part_count": int(document["part_count"]),
                        "document_content_sha256": (
                            document["document_content_sha256"]
                        ),
                        "actor_ids": ids,
                        "actor_names": names,
                        "actor_relations": relations,
                    },
                    member=member,
                )
                for actor_id, display_name, relation in zip(
                    ids, names, relations, strict=True
                ):
                    upload.add(
                        "actors",
                        {
                            "schema_version": SCAN_SCHEMA_VERSION,
                            "tenant_id": candidate.tenant_id,
                            "source_id": candidate.source_id,
                            "logical_document_id": document["logical_document_id"],
                            "revision": int(document["revision"]),
                            "record_ordinal": None,
                            "actor_id": actor_id,
                            "display_name": display_name,
                            "relation": relation,
                        },
                        member=member,
                    )
                first = (
                    projected.first_occurred_at
                    if first is None
                    else min(first, projected.first_occurred_at)
                )
                last = (
                    projected.last_occurred_at
                    if last is None
                    else max(last, projected.last_occurred_at)
                )
            self._persist_part_bounds(discovered_bounds)
            if upload.rows_seen["documents"] == 0 and len(ensure_datasets) == len(
                SCAN_DATASETS
            ):
                # Nothing survives and nothing was projected: the month has no
                # live parts after this commit (matches the pre-fragment plane).
                finished = time.perf_counter()
                LOG.info(
                    "parquet build documents=%s records=0 metadata_ms=%s "
                    "stream_ms=%s total_ms=%s mode=%s empty=true",
                    len(documents),
                    metadata_ms,
                    round((finished - started) * 1_000) - metadata_ms,
                    round((finished - started) * 1_000),
                    mode,
                )
                upload.abort()
                return ScanUpload(
                    upload.generation,
                    {},
                    {},
                    None,
                    None,
                    False,
                    removed=removed,
                    mode=mode,
                    documents_dirty=len(dirty),
                    documents_rewritten=len(rewrite_documents),
                )
            result = upload.finish(
                first=first,
                last=last,
                ensure_datasets=ensure_datasets,
                removed=removed,
                mode=mode,
                documents_dirty=len(dirty),
                documents_rewritten=len(rewrite_documents),
            )
        except Exception as error:
            try:
                upload.abort()
            except Exception:
                raise ParquetScanError("parquet_scan_cleanup_enqueue_failed") from error
            raise
        finished = time.perf_counter()
        LOG.info(
            "parquet build documents=%s rewritten=%s dirty=%s passages=%s records=%s "
            "fragments_removed=%s fragments_written=%s metadata_ms=%s "
            "stream_ms=%s upload_ms=%s total_ms=%s mode=%s",
            len(documents),
            len(rewrite_documents),
            len(dirty),
            upload.rows_seen["passages"],
            upload.rows_seen["records"],
            len(removed),
            len(result.references),
            metadata_ms,
            round((finished - started) * 1_000) - metadata_ms,
            upload.upload_ms,
            round((finished - started) * 1_000),
            mode,
        )
        return result

    @staticmethod
    def _enqueue_cleanup(connection: Any, references: Iterable[dict[str, Any]]) -> None:
        for reference in references:
            connection.execute(
                """INSERT INTO canonical_evidence_cleanup_queue(
                       tenant_id,source_id,artifact_id,storage_backend,
                       object_key,content_sha256,size_bytes,media_type,
                       encryption,version_id,created_at
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                (
                    reference["tenant_id"],
                    reference["source_id"],
                    reference["artifact_id"],
                    reference["storage_backend"],
                    reference["object_key"],
                    reference["content_sha256"],
                    reference["size_bytes"],
                    reference["media_type"],
                    reference["encryption"],
                    reference["version_id"],
                    reference["created_at"],
                ),
            )

    def _commit(self, candidate: ScanCandidate, upload: ScanUpload) -> str:
        scope = (candidate.tenant_id, candidate.source_id, candidate.bucket_start)
        with self.store.connect() as connection:
            with connection.transaction():
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (
                        "parquet-scan\x1f"
                        + candidate.tenant_id
                        + "\x1f"
                        + candidate.source_id
                        + "\x1f"
                        + candidate.bucket_start.isoformat(),
                    ),
                )
                queued = connection.execute(
                    """SELECT generation,changed_at
                         FROM canonical_parquet_scan_queue
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                        FOR UPDATE""",
                    scope,
                ).fetchone()
                if (
                    queued is None
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.changed_at
                ):
                    return "stale"
                live = {
                    (row["dataset"], int(row["shard_index"])): row
                    for row in connection.execute(
                        """SELECT * FROM canonical_parquet_scan_shards
                            WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                            FOR UPDATE""",
                        scope,
                    ).fetchall()
                }
                if upload.mode == "reuse":
                    if set(live) != set(upload.references):
                        return "stale"
                else:
                    if any(identity not in live for identity in upload.removed):
                        # The catalog moved under the lease: rebuild from scratch.
                        return "stale"
                    surviving = set(live) - set(upload.removed)
                    if surviving & set(upload.references):
                        # Never overwrite a live part: indexes were allocated
                        # above every survivor, so a clash means a stale plan.
                        raise ParquetScanError("parquet_scan_shard_conflict")
                    retained = {
                        value["artifact_id"] for value in upload.references.values()
                    } | {
                        row["artifact_id"]
                        for identity, row in live.items()
                        if identity in surviving
                    }
                    self._enqueue_cleanup(
                        connection,
                        (
                            _reference(live[identity])
                            for identity in upload.removed
                            if live[identity]["artifact_id"] not in retained
                        ),
                    )
                    with connection.cursor() as cursor:
                        cursor.executemany(
                            """DELETE FROM canonical_parquet_scan_shards
                                WHERE tenant_id=%s AND source_id=%s
                                  AND bucket_start=%s AND dataset=%s
                                  AND shard_index=%s""",
                            [
                                (*scope, dataset, shard_index)
                                for dataset, shard_index in upload.removed
                            ],
                        )
                    for (dataset, shard_index), reference in upload.references.items():
                        first, last = upload.bounds.get(
                            (dataset, shard_index), (None, None)
                        )
                        connection.execute(
                            """INSERT INTO canonical_parquet_scan_shards(
                                   tenant_id,source_id,bucket_start,dataset,
                                   shard_index,generation_sha256,artifact_id,
                                   storage_backend,object_key,content_sha256,
                                   size_bytes,media_type,encryption,version_id,
                                   row_count,first_occurred_at,last_occurred_at,
                                   created_at
                               ) VALUES (
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                   %s,%s,%s,%s
                               )""",
                            (
                                *scope,
                                dataset,
                                shard_index,
                                upload.generation_sha256,
                                reference["artifact_id"],
                                reference["storage_backend"],
                                reference["object_key"],
                                reference["content_sha256"],
                                reference["size_bytes"],
                                reference["media_type"],
                                reference["encryption"],
                                reference["version_id"],
                                upload.row_counts[(dataset, shard_index)],
                                first,
                                last,
                                reference["created_at"],
                            ),
                        )
                    member_rows = [
                        (
                            *scope,
                            dataset,
                            shard_index,
                            member.logical_document_id,
                            member.revision,
                            member.generation_sha256,
                        )
                        for (dataset, shard_index), members in upload.members.items()
                        if (dataset, shard_index) in upload.references
                        for member in members
                    ]
                    if member_rows:
                        with connection.cursor() as cursor:
                            cursor.executemany(
                                """INSERT INTO
                                       canonical_parquet_scan_fragment_documents(
                                       tenant_id,source_id,bucket_start,dataset,
                                       shard_index,logical_document_id,revision,
                                       generation_sha256
                                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                                member_rows,
                            )
                connection.execute(
                    """DELETE FROM canonical_parquet_scan_dirty_documents
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s""",
                    scope,
                )
                deleted = connection.execute(
                    """DELETE FROM canonical_parquet_scan_queue
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                          AND generation=%s""",
                    (*scope, candidate.generation),
                )
                if deleted.rowcount != 1:
                    raise ParquetScanError("parquet_scan_queue_conflict")
        return "committed"

    def _over_fragmented(
        self,
        *,
        tenant_id: str | None,
        limit: int,
    ) -> list[ScanCandidate]:
        """Queue source-months whose fragment count exceeds the compaction cap."""

        with self.store.connect() as connection:
            with connection.transaction():
                rows = connection.execute(
                    """SELECT fragment.tenant_id,fragment.source_id,
                              fragment.bucket_start,max(fragment.parts) AS parts
                         FROM (
                               SELECT tenant_id,source_id,bucket_start,dataset,
                                      count(*) AS parts
                                 FROM canonical_parquet_scan_shards
                                WHERE (%s::text IS NULL OR tenant_id=%s)
                                GROUP BY tenant_id,source_id,bucket_start,dataset
                         ) fragment
                        WHERE fragment.parts > %s
                          AND NOT EXISTS (
                              SELECT 1 FROM canonical_parquet_scan_queue queue
                               WHERE queue.tenant_id=fragment.tenant_id
                                 AND queue.source_id=fragment.source_id
                                 AND queue.bucket_start=fragment.bucket_start
                          )
                        GROUP BY fragment.tenant_id,fragment.source_id,
                                 fragment.bucket_start
                        ORDER BY max(fragment.parts) DESC,fragment.tenant_id,
                                 fragment.source_id,fragment.bucket_start
                        LIMIT %s""",
                    (tenant_id, tenant_id, self.compaction_fragments, limit),
                ).fetchall()
                candidates = []
                for row in rows:
                    scope = (row["tenant_id"], row["source_id"], row["bucket_start"])
                    queued = connection.execute(
                        """INSERT INTO canonical_parquet_scan_queue(
                               tenant_id,source_id,bucket_start,
                               generation,reason,changed_at
                           ) VALUES (%s,%s,%s,1,'backfill',clock_timestamp())
                           ON CONFLICT(tenant_id,source_id,bucket_start)
                           DO UPDATE SET
                               generation=canonical_parquet_scan_queue.generation+1,
                               reason='backfill',changed_at=clock_timestamp()
                           RETURNING generation,changed_at""",
                        scope,
                    ).fetchone()
                    connection.execute(
                        """INSERT INTO canonical_parquet_scan_dirty_documents(
                               tenant_id,source_id,bucket_start,
                               logical_document_id,reason,queued_at
                           ) VALUES (%s,%s,%s,%s,'compaction',clock_timestamp())
                           ON CONFLICT(
                               tenant_id,source_id,bucket_start,logical_document_id
                           )
                           DO UPDATE SET reason='compaction',
                                         queued_at=clock_timestamp()""",
                        (*scope, SCAN_DIRTY_ALL),
                    )
                    candidates.append(
                        ScanCandidate(
                            tenant_id=row["tenant_id"],
                            source_id=row["source_id"],
                            bucket_start=_month(row["bucket_start"]),
                            generation=int(queued["generation"]),
                            changed_at=queued["changed_at"],
                            reason="backfill",
                        )
                    )
        return candidates

    def _fragment_total(self, *, tenant_id: str | None) -> int:
        with self.store.connect() as connection:
            row = connection.execute(
                """SELECT count(*) AS count FROM canonical_parquet_scan_shards
                    WHERE (%s::text IS NULL OR tenant_id=%s)""",
                (tenant_id, tenant_id),
            ).fetchone()
        return int(row["count"]) if row else 0

    def _process(
        self,
        candidate: ScanCandidate,
        totals: dict[str, int],
    ) -> bool:
        """Build and commit one candidate under its lease; True when handled."""

        with self._candidate_lease(candidate) as acquired:
            if not acquired:
                totals["contended"] += 1
                return False
            upload = self._build(candidate)
            status = self._commit(candidate, upload)
            if status == "committed":
                totals["committed"] += 1
                totals["rows"] += sum(upload.row_counts.values()) if upload.created else 0
                totals["fragments_rewritten"] += (
                    len(upload.references) if upload.created else 0
                )
                totals["documents_dirty"] += upload.documents_dirty
                if upload.mode == "compaction":
                    totals["compacted"] += 1
            else:
                totals["stale"] += 1
                if upload.created:
                    self._schedule_cleanup(upload.references.values())
            return True

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 4,
        max_batches: int = 1,
        compaction_budget: int = 1,
    ) -> dict[str, int | str]:
        if (
            not 1 <= batch_size <= 32
            or not 1 <= max_batches <= 100
            or not 0 <= compaction_budget <= 32
        ):
            raise ParquetScanError("parquet_scan_budget_invalid")
        totals = {
            "committed": 0,
            "stale": 0,
            "rows": 0,
            "contended": 0,
            "fragments_rewritten": 0,
            "documents_dirty": 0,
            "compacted": 0,
        }
        for _ in range(max_batches):
            candidate_limit = min(32, batch_size + 7)
            candidates = self._pending(
                tenant_id=tenant_id,
                limit=candidate_limit,
            )
            if not candidates:
                break
            batch_completed = 0
            for candidate in candidates:
                if batch_completed >= batch_size:
                    break
                if self._process(candidate, totals):
                    batch_completed += 1
            if len(candidates) < candidate_limit:
                break
        if compaction_budget:
            for candidate in self._over_fragmented(
                tenant_id=tenant_id,
                limit=compaction_budget,
            ):
                self._process(candidate, totals)
        return {
            "status": "complete",
            "shards": totals["committed"],
            "rows": totals["rows"],
            "stale": totals["stale"],
            "contended": totals["contended"],
            "fragments_rewritten": totals["fragments_rewritten"],
            "fragments_total": self._fragment_total(tenant_id=tenant_id),
            "documents_dirty": totals["documents_dirty"],
            "compacted": totals["compacted"],
        }
