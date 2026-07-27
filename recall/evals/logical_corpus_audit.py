"""Content-free reconstruction audit for Recall logical evidence documents.

The audit recomputes each sampled document from current canonical rows through
the production projector, independently reads its persisted manifest and S3
parts, and compares exact encoded bytes by digest. It never prints source text,
native identifiers, object keys, receipts, or credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import orjson

from recall_server.archive_runtime import (
    build_archive_store,
    build_evidence_archive_store,
)
from recall_server.db import BrainStore
from recall_server.logical_evidence import (
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
)
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
    LogicalGroupCandidate,
)

from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.logical-corpus-audit.v1"
OVERSIZED_MEDIA_TYPE = "application/vnd.recall.oversized-record+gzip"


class LogicalCorpusAuditError(RuntimeError):
    """The corpus failed a content-free reconstruction invariant."""


def _timestamp(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    return value.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class _DigestPrepared:
    document_content_sha256: str
    record_count: int
    receipt_count: int
    first_occurred_at: str
    last_occurred_at: str


@dataclass(frozen=True)
class _DigestUpload:
    prepared: _DigestPrepared
    all_references: tuple[()] = ()


class _DigestProjection:
    """Consume production records without writing another evidence revision."""

    @staticmethod
    def put_records(
        *,
        source_id: str,
        records: Iterable[LogicalEvidenceRecord],
        **_values: Any,
    ) -> _DigestUpload:
        digest = hashlib.sha256()
        record_count = 0
        receipt_count = 0
        occurred: list[str] = []
        for record in records:
            digest.update(record.encode(source_id=source_id))
            record_count += 1
            receipt_count += len(record.receipts)
            occurred.append(record.occurred_at)
        if not record_count:
            raise LogicalCorpusAuditError("sampled logical document became empty")
        parsed = [
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            for value in occurred
        ]
        return _DigestUpload(
            prepared=_DigestPrepared(
                document_content_sha256=digest.hexdigest(),
                record_count=record_count,
                receipt_count=receipt_count,
                first_occurred_at=occurred[parsed.index(min(parsed))],
                last_occurred_at=occurred[parsed.index(max(parsed))],
            )
        )

    @staticmethod
    def delete_reference(_reference: dict[str, Any]) -> bool:
        return False


def _decode_record(line: bytes, *, source_id: str) -> LogicalEvidenceRecord:
    try:
        value = orjson.loads(line)
    except orjson.JSONDecodeError as error:
        raise LogicalCorpusAuditError("logical part contains invalid JSONL") from error
    if not isinstance(value, dict):
        raise LogicalCorpusAuditError("logical part record is not an object")
    base = {
        "event_kind",
        "event_native_id",
        "occurred_at",
        "ordinal",
        "receipts",
        "roles",
        "segment_count",
        "segment_ordinal",
    }
    payload_fields = set(value).intersection(
        {"content", "content_fragment", "text"}
    )
    if set(value) != base | payload_fields or len(payload_fields) != 1:
        raise LogicalCorpusAuditError("logical part record schema is invalid")
    if "content" in value:
        text = orjson.dumps(
            value["content"],
            option=orjson.OPT_SORT_KEYS,
        ).decode()
    elif "content_fragment" in value:
        text = value["content_fragment"]
    else:
        text = value["text"]
    try:
        record = LogicalEvidenceRecord(
            ordinal=value["ordinal"],
            event_native_id=value["event_native_id"],
            event_kind=value["event_kind"],
            occurred_at=value["occurred_at"],
            roles=tuple(value["roles"]),
            receipts=tuple(value["receipts"]),
            segment_ordinal=value["segment_ordinal"],
            segment_count=value["segment_count"],
            text=text,
        )
        encoded = record.encode(source_id=source_id)
    except (KeyError, TypeError, ValueError) as error:
        raise LogicalCorpusAuditError(
            "logical part record contract is invalid"
        ) from error
    if encoded != line:
        raise LogicalCorpusAuditError("logical part record is not canonical")
    return record


def _choose_sample(
    rows: list[dict[str, Any]],
    *,
    sample_size: int,
    seed: str,
) -> list[dict[str, Any]]:
    if not 1 <= sample_size <= len(rows):
        raise LogicalCorpusAuditError("logical corpus sample size is invalid")
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(row["source_id"], []).append(row)
    cells: list[list[dict[str, Any]]] = []
    for source_rows in by_source.values():
        ordered = sorted(
            source_rows,
            key=lambda value: (
                int(value["record_count"]),
                value["logical_document_id"],
            ),
        )
        buckets: list[list[dict[str, Any]]] = [[], [], [], []]
        for index, row in enumerate(ordered):
            bucket = min(3, index * 4 // len(ordered))
            buckets[bucket].append(row)
        cells.extend(bucket for bucket in buckets if bucket)
    for cell in cells:
        cell.sort(
            key=lambda value: hashlib.sha256(
                f"{seed}\0{value['logical_document_id']}".encode()
            ).digest()
        )
    selected: list[dict[str, Any]] = []
    used: set[str] = set()
    cursor = 0
    while len(selected) < sample_size and any(cells):
        cell = cells[cursor % len(cells)]
        cursor += 1
        while cell and cell[0]["logical_document_id"] in used:
            cell.pop(0)
        if not cell:
            cells.remove(cell)
            cursor = 0
            continue
        row = cell.pop(0)
        selected.append(row)
        used.add(row["logical_document_id"])
    if len(selected) != sample_size:
        raise LogicalCorpusAuditError("logical corpus sample is incomplete")
    return selected


def _manifest_reference(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        "artifact_id": row["manifest_artifact_id"],
        "storage_backend": row["manifest_storage_backend"],
        "object_key": row["manifest_object_key"],
        "content_sha256": row["manifest_content_sha256"],
        "size_bytes": row["manifest_size_bytes"],
        "media_type": row["manifest_media_type"],
        "encryption": row["manifest_encryption"],
        "version_id": row["manifest_version_id"],
        "created_at": _timestamp(row["created_at"]),
    }


def _part_reference(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        "artifact_id": row["artifact_id"],
        "storage_backend": row["storage_backend"],
        "object_key": row["object_key"],
        "content_sha256": row["content_sha256"],
        "size_bytes": row["size_bytes"],
        "media_type": row["media_type"],
        "encryption": row["encryption"],
        "version_id": row["version_id"],
        "created_at": _timestamp(row["created_at"]),
    }


def _read_stored(
    projection: LogicalEvidenceProjectionStore,
    row: dict[str, Any],
    parts: list[dict[str, Any]],
) -> dict[str, int | str]:
    manifest = projection.read_manifest(
        _manifest_reference(row),
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
    )
    expected_manifest = {
        "logical_document_id": row["logical_document_id"],
        "evidence_id": row["evidence_id"],
        "revision": row["revision"],
        "document_content_sha256": row["document_content_sha256"],
        "record_count": row["record_count"],
        "receipt_count": row["receipt_count"],
        "part_count": row["part_count"],
        "first_occurred_at": _timestamp(row["first_occurred_at"]),
        "last_occurred_at": _timestamp(row["last_occurred_at"]),
    }
    actual_manifest = {
        key: (
            len(manifest["parts"])
            if key == "part_count"
            else manifest.get(key)
        )
        for key in expected_manifest
    }
    if actual_manifest != expected_manifest:
        raise LogicalCorpusAuditError("logical manifest disagrees with catalog")
    if len(parts) != row["part_count"]:
        raise LogicalCorpusAuditError("logical part catalog is incomplete")

    document_digest = hashlib.sha256()
    record_count = 0
    receipt_count = 0
    seen_receipts: set[str] = set()
    byte_count = 0
    for part_ordinal, (part, catalog) in enumerate(
        zip(manifest["parts"], parts, strict=True)
    ):
        if (
            part.get("ordinal") != part_ordinal
            or catalog["part_ordinal"] != part_ordinal
            or any(
                part.get(name) != catalog[name]
                for name in (
                    "artifact_id",
                    "object_key",
                    "content_sha256",
                    "size_bytes",
                    "media_type",
                    "version_id",
                    "first_record_ordinal",
                    "last_record_ordinal",
                    "receipt_count",
                )
            )
        ):
            raise LogicalCorpusAuditError(
                "logical manifest disagrees with part catalog"
            )
        payload = projection.read_part(
            _part_reference(catalog),
            tenant_id=row["tenant_id"],
            source_id=row["source_id"],
        )
        if not payload.endswith(b"\n"):
            raise LogicalCorpusAuditError("logical part is not newline terminated")
        document_digest.update(payload)
        byte_count += len(payload)
        for line in payload.splitlines(keepends=True):
            record = _decode_record(line, source_id=row["source_id"])
            if record.ordinal != record_count:
                raise LogicalCorpusAuditError(
                    "logical document record order is invalid"
                )
            for receipt in record.receipts:
                if receipt in seen_receipts:
                    raise LogicalCorpusAuditError(
                        "logical document contains a duplicate receipt"
                    )
                seen_receipts.add(receipt)
            record_count += 1
            receipt_count += len(record.receipts)
    if (
        document_digest.hexdigest() != row["document_content_sha256"]
        or record_count != row["record_count"]
        or receipt_count != row["receipt_count"]
    ):
        raise LogicalCorpusAuditError(
            "logical document reconstruction does not match its catalog"
        )
    return {
        "document_content_sha256": document_digest.hexdigest(),
        "record_count": record_count,
        "receipt_count": receipt_count,
        "part_count": len(parts),
        "byte_count": byte_count,
    }


def _catalog(store: BrainStore, tenant_id: str) -> list[dict[str, Any]]:
    with store.connect() as connection:
        return connection.execute(
            """SELECT tenant_id,source_id,logical_document_id,
                      native_parent_id,revision,evidence_id,
                      manifest_artifact_id,manifest_storage_backend,
                      manifest_object_key,manifest_content_sha256,
                      manifest_size_bytes,manifest_media_type,
                      manifest_encryption,manifest_version_id,
                      document_content_sha256,record_count,receipt_count,
                      part_count,first_occurred_at,last_occurred_at,
                      source_updated_at,created_at
                 FROM canonical_evidence_documents
                WHERE tenant_id=%s
                ORDER BY source_id,logical_document_id""",
            (tenant_id,),
        ).fetchall()


def _parts(
    store: BrainStore,
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str, int], list[dict[str, Any]]]:
    with store.connect() as connection:
        values = connection.execute(
            """WITH selected(
                   source_id,logical_document_id,revision
               ) AS (
                   SELECT * FROM unnest(
                       %s::text[],%s::text[],%s::integer[]
                   )
               )
               SELECT part.*
                 FROM selected
                 JOIN canonical_evidence_document_parts part
                   USING(source_id,logical_document_id,revision)
                WHERE part.tenant_id=%s
                ORDER BY part.source_id,part.logical_document_id,
                         part.revision,part.part_ordinal""",
            (
                [row["source_id"] for row in rows],
                [row["logical_document_id"] for row in rows],
                [row["revision"] for row in rows],
                rows[0]["tenant_id"],
            ),
        ).fetchall()
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for row in values:
        identity = (
            row["source_id"],
            row["logical_document_id"],
            row["revision"],
        )
        grouped.setdefault(identity, []).append(row)
    return grouped


def audit_logical_corpus(
    store: BrainStore,
    projection: LogicalEvidenceProjectionStore,
    raw_archive: Any,
    *,
    tenant_id: str,
    sample_size: int = 200,
    seed: str = "recall-logical-corpus-v1",
    concurrency: int = 8,
) -> dict[str, Any]:
    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or not 1 <= concurrency <= 32
    ):
        raise LogicalCorpusAuditError("logical corpus audit arguments are invalid")
    catalog = _catalog(store, tenant_id)
    sample = _choose_sample(catalog, sample_size=sample_size, seed=seed)
    part_rows = _parts(store, sample)
    projector = CanonicalLogicalEvidenceProjector(
        store,
        _DigestProjection(),  # type: ignore[arg-type]
        bound_tenant_id=tenant_id,
        raw_archive=raw_archive,
    )
    worker_count = min(concurrency, len(sample))
    prepare_pool = getattr(store, "prepare_pool", None)
    if callable(prepare_pool):
        prepare_pool(worker_count)
    shards: list[list[dict[str, Any]]] = [[] for _ in range(worker_count)]
    loads = [0] * worker_count
    for row in sorted(
        sample,
        key=lambda value: -int(value["record_count"]),
    ):
        shard = min(range(worker_count), key=lambda value: (loads[value], value))
        shards[shard].append(row)
        loads[shard] += int(row["record_count"])

    def audit_shard(rows: list[dict[str, Any]]) -> dict[str, int]:
        candidates = tuple(
            LogicalGroupCandidate(
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                native_parent_id=row["native_parent_id"],
                source_updated_at=row["source_updated_at"],
                generation=0,
                revision=row["revision"],
                estimated_records=row["record_count"],
                estimated_bytes=max(1, int(row["record_count"])),
            )
            for row in rows
        )
        expected = projector._prepare_batch_and_upload(candidates)
        totals = {
            "documents": 0,
            "records": 0,
            "receipts": 0,
            "parts": 0,
            "bytes": 0,
        }
        for row, expected_upload in zip(rows, expected, strict=True):
            if expected_upload is None:
                raise LogicalCorpusAuditError(
                    "sampled logical document became empty"
                )
            identity = (
                row["source_id"],
                row["logical_document_id"],
                row["revision"],
            )
            stored = _read_stored(
                projection,
                row,
                part_rows.get(identity, []),
            )
            prepared = expected_upload.prepared
            if (
                stored["document_content_sha256"]
                != prepared.document_content_sha256
                or stored["record_count"] != prepared.record_count
                or stored["receipt_count"] != prepared.receipt_count
                or _timestamp(row["first_occurred_at"])
                != prepared.first_occurred_at
                or _timestamp(row["last_occurred_at"])
                != prepared.last_occurred_at
            ):
                raise LogicalCorpusAuditError(
                    "stored document differs from current canonical projection"
                )
            totals["documents"] += 1
            totals["records"] += int(stored["record_count"])
            totals["receipts"] += int(stored["receipt_count"])
            totals["parts"] += int(stored["part_count"])
            totals["bytes"] += int(stored["byte_count"])
        return totals

    started = time.monotonic()
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="recall-corpus-audit",
    ) as executor:
        receipts = list(executor.map(audit_shard, shards))
    totals = {
        key: sum(receipt[key] for receipt in receipts)
        for key in receipts[0]
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "sample": {
            **totals,
            "requested_documents": sample_size,
            "exact_reconstructions": totals["documents"],
            "source_count": len({row["source_id"] for row in sample}),
        },
        "corpus": {
            "documents": len(catalog),
            "sources": len({row["source_id"] for row in catalog}),
            "records": sum(int(row["record_count"]) for row in catalog),
            "receipts": sum(int(row["receipt_count"]) for row in catalog),
            "parts": sum(int(row["part_count"]) for row in catalog),
        },
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "seed_sha256": hashlib.sha256(seed.encode()).hexdigest(),
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-logical-corpus-audit")
    value.add_argument("--dsn", default=os.environ.get("RECALL_DATABASE_URL"))
    value.add_argument("--tenant", required=True)
    value.add_argument("--sample-size", type=int, default=200)
    value.add_argument("--seed", default="recall-logical-corpus-v1")
    value.add_argument("--concurrency", type=int, default=8)
    value.add_argument("--repo-root", required=True)
    return value


def main() -> None:
    args = parser().parse_args()
    if not args.dsn:
        raise SystemExit("RECALL_DATABASE_URL or --dsn is required")
    store = BrainStore(args.dsn, pool_max_size=args.concurrency)
    report = audit_logical_corpus(
        store,
        LogicalEvidenceProjectionStore(build_evidence_archive_store()),
        build_archive_store(),
        tenant_id=args.tenant,
        sample_size=args.sample_size,
        seed=args.seed,
        concurrency=args.concurrency,
    )
    root = Path(args.repo_root).resolve(strict=True)
    report["pins"] = {
        "git_sha": git_sha(root),
        "git_dirty": git_dirty(root),
        "auditor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
