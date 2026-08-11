"""Rebuildable Parquet scan shards over canonical evidence documents."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import orjson
import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
SCAN_SCHEMA_VERSION = 1
SCAN_DATASETS = ("documents", "records", "actors")
MAX_SCAN_RECORDS = 5_000_000


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


def _parquet_bytes(rows: list[dict[str, Any]], schema: Any) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
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


def _schemas() -> dict[str, Any]:
    utc = pa.timestamp("us", tz="UTC")
    strings = pa.list_(pa.string())
    return {
        "documents": pa.schema([
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
        ]),
        "records": pa.schema([
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
        ]),
        "actors": pa.schema([
            ("schema_version", pa.int16()),
            ("tenant_id", pa.string()),
            ("source_id", pa.string()),
            ("logical_document_id", pa.string()),
            ("revision", pa.int32()),
            ("record_ordinal", pa.int64()),
            ("actor_id", pa.string()),
            ("display_name", pa.string()),
            ("relation", pa.string()),
        ]),
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
    references: dict[str, dict[str, Any]]
    row_counts: dict[str, int]
    first_occurred_at: datetime | None
    last_occurred_at: datetime | None
    created: bool


@dataclass(frozen=True)
class DocumentProjection:
    records: list[dict[str, Any]]
    actors: list[dict[str, Any]]
    first_occurred_at: datetime | None
    last_occurred_at: datetime | None


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
                    ORDER BY part.logical_document_id,part.part_ordinal""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
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
        ordered = sorted({
            (
                str(link.get("actor_id", "")),
                str(link.get("display_name", "")),
                str(link.get("relation", "")),
            )
            for link in links
            if isinstance(link, dict)
            and link.get("actor_id")
            and link.get("relation")
        })
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
    ) -> DocumentProjection:
        document_links = document.get("actor_links") or []
        actor_names = {
            link.get("actor_id"): link.get("display_name", "")
            for link in document_links
            if isinstance(link, dict)
        }
        records: list[dict[str, Any]] = []
        actors: list[dict[str, Any]] = []
        first = last = None
        for part in document["parts"]:
            payload = self.archive.read_raw(_reference(part))
            for line in payload.splitlines():
                try:
                    record = orjson.loads(line)
                except orjson.JSONDecodeError:
                    raise ParquetScanError("parquet_scan_record_invalid") from None
                if not isinstance(record, dict):
                    raise ParquetScanError("parquet_scan_record_invalid")
                occurred_at = _timestamp(record.get("occurred_at"))
                if not bucket_start <= occurred_at < bucket_end:
                    continue
                if len(records) >= record_budget:
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
                records.append({
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
                })
                actors.extend(
                    {
                        "schema_version": SCAN_SCHEMA_VERSION,
                        "tenant_id": candidate.tenant_id,
                        "source_id": candidate.source_id,
                        "logical_document_id": document["logical_document_id"],
                        "revision": int(document["revision"]),
                        "record_ordinal": ordinal,
                        "actor_id": str(link["actor_id"]),
                        "display_name": str(link.get("display_name", "")),
                        "relation": str(link["relation"]),
                    }
                    for link in enriched
                    if link.get("actor_id") and link.get("relation")
                )
                first = occurred_at if first is None else min(first, occurred_at)
                last = occurred_at if last is None else max(last, occurred_at)
        return DocumentProjection(records, actors, first, last)

    def _current_upload(
        self,
        candidate: ScanCandidate,
        generation: str,
    ) -> ScanUpload | None:
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT * FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s AND source_id=%s AND bucket_start=%s
                    ORDER BY dataset""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    candidate.bucket_start,
                ),
            ).fetchall()
        if (
            len(rows) != len(SCAN_DATASETS)
            or {row["dataset"] for row in rows} != set(SCAN_DATASETS)
            or any(row["generation_sha256"] != generation for row in rows)
        ):
            return None
        return ScanUpload(
            generation,
            {row["dataset"]: _reference(row) for row in rows},
            {row["dataset"]: int(row["row_count"]) for row in rows},
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

    def _upload(
        self,
        candidate: ScanCandidate,
        *,
        generation: str,
        rows: dict[str, list[dict[str, Any]]],
        first: datetime,
        last: datetime,
        created_at: datetime,
    ) -> ScanUpload:
        references: dict[str, dict[str, Any]] = {}
        attempt = uuid.uuid4().hex
        try:
            schemas = _schemas()
            for dataset in SCAN_DATASETS:
                references[dataset] = self.archive.put_raw(
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    native_id=(
                        f"parquet-scan:{candidate.bucket_start.isoformat()}:"
                        f"{dataset}:{generation[:16]}:{attempt}"
                    ),
                    payload=_parquet_bytes(rows[dataset], schemas[dataset]),
                    media_type=PARQUET_MEDIA_TYPE,
                    created_at=created_at.isoformat().replace("+00:00", "Z"),
                )
        except Exception as error:
            try:
                self._schedule_cleanup(references.values())
            except Exception:
                raise ParquetScanError(
                    "parquet_scan_cleanup_enqueue_failed"
                ) from error
            raise
        return ScanUpload(
            generation,
            references,
            {dataset: len(rows[dataset]) for dataset in SCAN_DATASETS},
            first,
            last,
            True,
        )

    def _build(self, candidate: ScanCandidate) -> ScanUpload:
        documents = self._documents(candidate)
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
        rows: dict[str, list[dict[str, Any]]] = {
            dataset: [] for dataset in SCAN_DATASETS
        }
        digest = hashlib.sha256(
            f"recall.parquet-scan.v{SCAN_SCHEMA_VERSION}\0"
            f"{candidate.bucket_start.isoformat()}\n".encode()
        )
        first = last = None
        for document in documents:
            links = document.get("actor_links") or []
            projected = self._project_document(
                candidate,
                document,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                record_budget=MAX_SCAN_RECORDS - len(rows["records"]),
            )
            if not projected.records:
                continue
            if (
                projected.first_occurred_at is None
                or projected.last_occurred_at is None
            ):
                raise ParquetScanError("parquet_scan_state_invalid")
            ids, names, relations = self._actor_columns(links)
            rows["documents"].append({
                "schema_version": SCAN_SCHEMA_VERSION,
                "tenant_id": candidate.tenant_id,
                "source_id": candidate.source_id,
                "logical_document_id": document["logical_document_id"],
                "revision": int(document["revision"]),
                "first_occurred_at": projected.first_occurred_at,
                "last_occurred_at": projected.last_occurred_at,
                "record_count": len(projected.records),
                "part_count": int(document["part_count"]),
                "document_content_sha256": document["document_content_sha256"],
                "actor_ids": ids,
                "actor_names": names,
                "actor_relations": relations,
            })
            rows["records"].extend(projected.records)
            rows["actors"].extend(projected.actors)
            rows["actors"].extend(
                {
                    "schema_version": SCAN_SCHEMA_VERSION,
                    "tenant_id": candidate.tenant_id,
                    "source_id": candidate.source_id,
                    "logical_document_id": document["logical_document_id"],
                    "revision": int(document["revision"]),
                    "record_ordinal": None,
                    "actor_id": str(link["actor_id"]),
                    "display_name": str(link.get("display_name", "")),
                    "relation": str(link["relation"]),
                }
                for link in links
                if isinstance(link, dict)
                and link.get("actor_id")
                and link.get("relation")
            )
            digest.update(orjson.dumps({
                "actors": list(zip(ids, names, relations, strict=True)),
                "content": document["document_content_sha256"],
                "document": document["logical_document_id"],
                "revision": int(document["revision"]),
            }, option=orjson.OPT_SORT_KEYS))
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
        generation = digest.hexdigest()
        if not rows["documents"]:
            return ScanUpload(generation, {}, {}, None, None, False)
        rows["actors"] = sorted(
            {
                (
                    row["logical_document_id"],
                    row["revision"],
                    row["record_ordinal"],
                    row["actor_id"],
                    row["relation"],
                ): row
                for row in rows["actors"]
            }.values(),
            key=lambda row: (
                row["logical_document_id"],
                row["record_ordinal"] is not None,
                -1 if row["record_ordinal"] is None else row["record_ordinal"],
                row["actor_id"],
                row["relation"],
            ),
        )
        current = self._current_upload(candidate, generation)
        if current is not None:
            return current
        if first is None or last is None:
            raise ParquetScanError("parquet_scan_state_invalid")
        return self._upload(
            candidate,
            generation=generation,
            rows=rows,
            first=first,
            last=last,
            created_at=bucket_start,
        )

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
                for dataset, reference in upload.references.items():
                    connection.execute(
                        """INSERT INTO canonical_parquet_scan_shards(
                               tenant_id,source_id,bucket_start,dataset,
                               generation_sha256,artifact_id,storage_backend,
                               object_key,content_sha256,size_bytes,media_type,
                               encryption,version_id,row_count,
                               first_occurred_at,last_occurred_at,created_at
                           ) VALUES (
                               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                               %s,%s,%s,%s
                           )""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.bucket_start,
                            dataset,
                            upload.generation_sha256,
                            reference["artifact_id"],
                            reference["storage_backend"],
                            reference["object_key"],
                            reference["content_sha256"],
                            reference["size_bytes"],
                            reference["media_type"],
                            reference["encryption"],
                            reference["version_id"],
                            upload.row_counts[dataset],
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
        committed = stale = rows = 0
        for _ in range(max_batches):
            candidates = self._pending(tenant_id=tenant_id, limit=batch_size)
            if not candidates:
                break
            for candidate in candidates:
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
        }
