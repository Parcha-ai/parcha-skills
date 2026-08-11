"""Rebuildable Parquet scan shards over canonical evidence documents."""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable, Iterator

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
SCAN_SCHEMA_VERSION = 1
SCAN_DATASETS = ("documents", "records", "actors")
MAX_SCAN_RECORDS = 5_000_000
MAX_PARQUET_OBJECT_BYTES = 48 * 1024 * 1024
PARQUET_RAW_SLICE_BYTES = 32 * 1024 * 1024
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


@dataclass(frozen=True)
class ScanUpload:
    generation_sha256: str
    references: dict[tuple[str, int], dict[str, Any]]
    row_counts: dict[tuple[str, int], int]
    first_occurred_at: datetime | None
    last_occurred_at: datetime | None
    created: bool


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


class _StreamingUpload:
    """Upload bounded Parquet parts without retaining a month of projected rows."""

    def __init__(
        self,
        projector: "CanonicalParquetScanProjector",
        candidate: ScanCandidate,
        *,
        generation: str,
        created_at: datetime,
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
        self.part_indexes = {dataset: 0 for dataset in SCAN_DATASETS}
        self.rows_seen = {dataset: 0 for dataset in SCAN_DATASETS}
        self.flushes = 0
        self.upload_ms = 0
        self.maximum_buffer_bytes = 0

    def add(self, dataset: str, row: dict[str, Any]) -> None:
        row_bytes = _estimated_row_bytes(row)
        if self.buffers[dataset] and (
            self.buffer_bytes[dataset] + row_bytes > PARQUET_RAW_SLICE_BYTES
        ):
            self._flush(dataset)
        self.buffers[dataset].append(row)
        self.buffer_bytes[dataset] += row_bytes
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
        for payload, row_count in _parquet_parts(rows, self.schemas[dataset]):
            shard_index = self.part_indexes[dataset]
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
            self.part_indexes[dataset] += 1
        self.flushes += 1

    def finish(
        self,
        *,
        first: datetime,
        last: datetime,
    ) -> ScanUpload:
        for dataset in SCAN_DATASETS:
            self._flush(dataset, allow_empty=self.rows_seen[dataset] == 0)
        LOG.info(
            "parquet stream documents=%s records=%s actors=%s parts=%s "
            "flushes=%s max_buffer_bytes=%s upload_ms=%s",
            self.rows_seen["documents"],
            self.rows_seen["records"],
            self.rows_seen["actors"],
            len(self.references),
            self.flushes,
            self.maximum_buffer_bytes,
            self.upload_ms,
        )
        return ScanUpload(
            self.generation,
            self.references,
            self.row_counts,
            first,
            last,
            True,
        )

    def abort(self) -> None:
        self.projector._schedule_cleanup(self.references.values())


class CanonicalParquetScanProjector:
    """Materialize source/month shards without changing canonical evidence."""

    def __init__(self, store: Any, evidence_projection: Any):
        self.store = store
        self.archive = evidence_projection.archive

    def seed_backfill(
        self,
        *,
        tenant_id: str,
        source_id: str | None = None,
    ) -> int:
        with self.store.connect() as connection:
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
        return max(0, result.rowcount)

    def _pending(self, *, tenant_id: str | None, limit: int) -> list[ScanCandidate]:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT tenant_id,source_id,bucket_start,generation,changed_at
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
        bucket_end = _next_month(candidate.bucket_start)
        with self.store.connect() as connection:
            documents = connection.execute(
                """SELECT document.*,
                          coalesce(attributed.links,'[]'::jsonb) AS actor_links
                     FROM canonical_evidence_documents document
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
                             AND link.revision=document.revision
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
            if not documents:
                return []
            parts = connection.execute(
                """SELECT part.*
                     FROM canonical_evidence_document_parts part
                     JOIN canonical_evidence_documents document
                       USING(tenant_id,source_id,logical_document_id,revision)
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND document.last_occurred_at >= %s
                      AND document.first_occurred_at < %s
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
                    candidate.bucket_start,
                    bucket_end,
                    candidate.bucket_start,
                    bucket_end,
                ),
            ).fetchall()
        by_document: dict[str, list[dict[str, Any]]] = {}
        for part in parts:
            by_document.setdefault(part["logical_document_id"], []).append(part)
        for document in documents:
            document["parts"] = by_document.get(document["logical_document_id"], [])
        return documents

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

    def _current_upload(
        self,
        candidate: ScanCandidate,
        generation: str,
    ) -> ScanUpload | None:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT * FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                    ORDER BY dataset,shard_index""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    candidate.bucket_start,
                ),
            ).fetchall()
        if {row["dataset"] for row in rows} != set(SCAN_DATASETS) or any(
            row["generation_sha256"] != generation for row in rows
        ):
            return None
        return ScanUpload(
            generation,
            {
                (row["dataset"], int(row["shard_index"])): _reference(row)
                for row in rows
            },
            {
                (row["dataset"], int(row["shard_index"])): int(row["row_count"])
                for row in rows
            },
            rows[0]["first_occurred_at"],
            rows[0]["last_occurred_at"],
            False,
        )

    def _schedule_cleanup(self, references: Iterable[dict[str, Any]]) -> None:
        references = tuple(references)
        if not references:
            return
        with self.store.connect() as connection:
            with connection.transaction():
                self._enqueue_cleanup(connection, references)

    @staticmethod
    def _generation(candidate: ScanCandidate, documents: list[dict[str, Any]]) -> str:
        digest = hashlib.sha256(
            f"recall.parquet-scan.v{SCAN_SCHEMA_VERSION}\0"
            f"{candidate.bucket_start.isoformat()}\n".encode()
        )
        for document in documents:
            ids, names, relations = CanonicalParquetScanProjector._actor_columns(
                document.get("actor_links") or []
            )
            digest.update(
                orjson.dumps(
                    {
                        "actors": list(zip(ids, names, relations, strict=True)),
                        "content": document["document_content_sha256"],
                        "document": document["logical_document_id"],
                        "revision": int(document["revision"]),
                    },
                    option=orjson.OPT_SORT_KEYS,
                )
            )
        return digest.hexdigest()

    def _streaming_upload(
        self,
        candidate: ScanCandidate,
        *,
        generation: str,
        created_at: datetime,
    ) -> _StreamingUpload:
        return _StreamingUpload(
            self,
            candidate,
            generation=generation,
            created_at=created_at,
        )

    def _build(self, candidate: ScanCandidate) -> ScanUpload:
        started = time.perf_counter()
        documents = self._documents(candidate)
        metadata_ms = round((time.perf_counter() - started) * 1_000)
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
        generation = self._generation(candidate, documents)
        current = self._current_upload(candidate, generation)
        if current is not None:
            finished = time.perf_counter()
            LOG.info(
                "parquet build documents=%s records=%s metadata_ms=%s "
                "stream_ms=0 upload_ms=0 total_ms=%s reused=true",
                len(documents),
                sum(
                    row_count
                    for (dataset, _), row_count in current.row_counts.items()
                    if dataset == "records"
                ),
                metadata_ms,
                round((finished - started) * 1_000),
            )
            return current
        upload = self._streaming_upload(
            candidate,
            generation=generation,
            created_at=bucket_start,
        )
        first = last = None
        discovered_bounds: list[PartTimeBound] = []
        try:
            for document in documents:
                links = document.get("actor_links") or []
                projected = self._project_document(
                    candidate,
                    document,
                    bucket_start=bucket_start,
                    bucket_end=bucket_end,
                    record_budget=(MAX_SCAN_RECORDS - upload.rows_seen["records"]),
                    record_sink=lambda row: upload.add("records", row),
                    actor_sink=lambda row: upload.add("actors", row),
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
            if upload.rows_seen["documents"] == 0:
                finished = time.perf_counter()
                LOG.info(
                    "parquet build documents=%s records=0 metadata_ms=%s "
                    "stream_ms=%s upload_ms=0 total_ms=%s reused=false",
                    len(documents),
                    metadata_ms,
                    round((finished - started) * 1_000) - metadata_ms,
                    round((finished - started) * 1_000),
                )
                return ScanUpload(generation, {}, {}, None, None, False)
            if first is None or last is None:
                raise ParquetScanError("parquet_scan_state_invalid")
            result = upload.finish(first=first, last=last)
        except Exception as error:
            try:
                upload.abort()
            except Exception:
                raise ParquetScanError("parquet_scan_cleanup_enqueue_failed") from error
            raise
        finished = time.perf_counter()
        LOG.info(
            "parquet build documents=%s records=%s metadata_ms=%s "
            "stream_ms=%s upload_ms=%s total_ms=%s reused=false",
            upload.rows_seen["documents"],
            upload.rows_seen["records"],
            metadata_ms,
            round((finished - started) * 1_000) - metadata_ms,
            upload.upload_ms,
            round((finished - started) * 1_000),
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
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.bucket_start,
                    ),
                ).fetchone()
                if (
                    queued is None
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.changed_at
                ):
                    return "stale"
                old = connection.execute(
                    """SELECT * FROM canonical_parquet_scan_shards
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.bucket_start,
                    ),
                ).fetchall()
                retained = {
                    value["artifact_id"] for value in upload.references.values()
                }
                self._enqueue_cleanup(
                    connection,
                    (
                        _reference(row)
                        for row in old
                        if row["artifact_id"] not in retained
                    ),
                )
                connection.execute(
                    """DELETE FROM canonical_parquet_scan_shards
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.bucket_start,
                    ),
                )
                for (dataset, shard_index), reference in upload.references.items():
                    connection.execute(
                        """INSERT INTO canonical_parquet_scan_shards(
                               tenant_id,source_id,bucket_start,dataset,shard_index,
                               generation_sha256,artifact_id,storage_backend,
                               object_key,content_sha256,size_bytes,media_type,
                               encryption,version_id,row_count,
                               first_occurred_at,last_occurred_at,created_at
                           ) VALUES (
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s
                           )""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.bucket_start,
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
                            upload.first_occurred_at,
                            upload.last_occurred_at,
                            reference["created_at"],
                        ),
                    )
                deleted = connection.execute(
                    """DELETE FROM canonical_parquet_scan_queue
                        WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                          AND generation=%s""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.bucket_start,
                        candidate.generation,
                    ),
                )
                if deleted.rowcount != 1:
                    raise ParquetScanError("parquet_scan_queue_conflict")
        return "committed"

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 4,
        max_batches: int = 1,
    ) -> dict[str, int | str]:
        if not 1 <= batch_size <= 32 or not 1 <= max_batches <= 100:
            raise ParquetScanError("parquet_scan_budget_invalid")
        committed = stale = rows = contended = 0
        for _ in range(max_batches):
            candidates = self._pending(tenant_id=tenant_id, limit=batch_size)
            if not candidates:
                break
            for candidate in candidates:
                with self._candidate_lease(candidate) as acquired:
                    if not acquired:
                        contended += 1
                        continue
                    upload = self._build(candidate)
                    status = self._commit(candidate, upload)
                    if status == "committed":
                        committed += 1
                        rows += sum(upload.row_counts.values())
                    else:
                        stale += 1
                        if upload.created:
                            self._schedule_cleanup(upload.references.values())
            if len(candidates) < batch_size:
                break
        return {
            "status": "complete",
            "shards": committed,
            "rows": rows,
            "stale": stale,
            "contended": contended,
        }
