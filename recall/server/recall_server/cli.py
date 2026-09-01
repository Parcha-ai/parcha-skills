from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path

from . import SCHEMA_VERSION
from .app import serve, serve_unix
from .archive import ArchiveError, ArchiveRequest, S3ArchiveStore
from .archive_runtime import (
    build_archive_store,
    build_evidence_archive_store,
    probe_archive,
)
from .canonical_retrieval import CanonicalRetrieval
from .canonical_thinning import thin_canonical_bodies
from .capabilities import CapabilityError, probe_database
from .control import ControlPlane, SecretBox
from .db import BrainStore
from .deployment import DeploymentManifestError, load_manifest, preview
from .embedding_worker import run_canonical_embedding_worker
from .evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceProjectionStore,
)
from .evidence_worker import (
    run_canonical_evidence_worker,
    run_logical_evidence_worker,
)
from .logical_evidence import LogicalEvidenceProjectionStore
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector
from .passage_projection import PassagePolicy
from .parquet_scan import CanonicalParquetScanProjector
from .passage_worker import run_passage_worker
from .projection_worker import run_projection_worker
from .federation import QUALITY_SCORES, SOURCE_FAMILIES
from .live_providers import (
    LiveProviderError,
    build_live_adapters,
)
from .managed_apply import (
    ApprovalError,
    approval_status,
    load_approvals,
    reconcile_infrastructure,
)
from .managed_worker import run_managed_worker
from .mcp_conformance import (
    ConformanceError,
    McpConformanceConfig,
    run_conformance,
)
from .semantic import SemanticRuntime


def _worker_pool_max_size(args: argparse.Namespace) -> int | None:
    """Size worker pools from real concurrency, not an unrelated floor."""

    if args.command in {
        "backfill-lossless-passages",
        "lossless-passage-worker",
    }:
        return max(4, args.concurrency)
    if args.command == "projection-worker":
        return max(
            4,
            args.passage_concurrency,
            args.upload_concurrency,
        )
    return None


def _storage_footprint(store: BrainStore) -> dict[str, object]:
    """Return aggregate relation sizes without reading user content."""

    with store.connect() as connection:
        rows = connection.execute(
            """SELECT schemaname,relname,
                      pg_total_relation_size(relid) AS total_bytes,
                      pg_relation_size(relid) AS heap_bytes,
                      pg_indexes_size(relid) AS index_bytes,
                      n_live_tup,n_dead_tup
                 FROM pg_stat_user_tables
                ORDER BY total_bytes DESC,schemaname,relname"""
        ).fetchall()
    relations = [
        {
            "schema": row["schemaname"],
            "relation": row["relname"],
            "total_bytes": int(row["total_bytes"]),
            "heap_bytes": int(row["heap_bytes"]),
            "index_bytes": int(row["index_bytes"]),
            "live_rows_estimate": int(row["n_live_tup"]),
            "dead_rows_estimate": int(row["n_dead_tup"]),
        }
        for row in rows
    ]
    return {
        "status": "ok",
        "total_relation_bytes": sum(
            int(relation["total_bytes"]) for relation in relations
        ),
        "relations": relations,
    }


def _compact_storage(
    store: BrainStore,
    relations: list[str],
) -> dict[str, object]:
    """Rewrite named user relations to reclaim bloat without deleting rows."""

    if (
        not relations
        or len(relations) > 100
        or len(set(relations)) != len(relations)
        or any(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None for name in relations)
    ):
        raise ValueError("storage compaction relation list is invalid")
    with store.connect() as connection:
        existing = {
            row["relname"]
            for row in connection.execute(
                """SELECT relname
                     FROM pg_stat_user_tables
                    WHERE schemaname='public' AND relname=ANY(%s)""",
                (relations,),
            ).fetchall()
        }
    if existing != set(relations):
        raise ValueError("storage compaction relation is unavailable")

    results = []
    for relation in relations:
        with store.connect() as connection:
            connection.autocommit = True
            try:
                before = int(
                    connection.execute(
                        "SELECT pg_total_relation_size(to_regclass(%s)) AS bytes",
                        (f"public.{relation}",),
                    ).fetchone()["bytes"]
                )
                logging.getLogger(__name__).info(
                    "storage compaction relation=%s phase=started before_bytes=%s",
                    relation,
                    before,
                )
                # The closed identifier grammar above is the injection boundary;
                # maintenance statements cannot parameterize identifiers.
                connection.execute(
                    f'VACUUM (FULL, ANALYZE) public."{relation}"'
                )
                logging.getLogger(__name__).info(
                    "storage compaction relation=%s phase=reindexing",
                    relation,
                )
                # PlanetScale recommends concurrent reindexing separately from
                # table compaction because index bloat is not reclaimed by its
                # supported table-maintenance path.
                connection.execute(
                    f'REINDEX TABLE CONCURRENTLY public."{relation}"'
                )
                after = int(
                    connection.execute(
                        "SELECT pg_total_relation_size(to_regclass(%s)) AS bytes",
                        (f"public.{relation}",),
                    ).fetchone()["bytes"]
                )
            finally:
                connection.autocommit = False
        logging.getLogger(__name__).info(
            "storage compaction relation=%s phase=complete before_bytes=%s after_bytes=%s",
            relation,
            before,
            after,
        )
        results.append(
            {
                "relation": relation,
                "before_bytes": before,
                "after_bytes": after,
                "reclaimed_bytes": max(0, before - after),
            }
        )
    return {
        "status": "ok",
        "before_bytes": sum(int(row["before_bytes"]) for row in results),
        "after_bytes": sum(int(row["after_bytes"]) for row in results),
        "reclaimed_bytes": sum(
            int(row["reclaimed_bytes"]) for row in results
        ),
        "relations": results,
    }


_DISCARDABLE_DERIVED_RELATIONS = frozenset(
    {
        "canonical_chunk_embeddings",
        "canonical_passage_contexts",
        "canonical_passage_embedding_representations",
        "item_embeddings",
        "turn_embeddings",
    }
)

_EMPTY_LEGACY_RELATIONS = (
    "chunks",
    "entities",
    "item_embeddings",
    "items",
    "source_events",
    "turn_embedding_items",
    "turn_embeddings",
)

_LEGACY_GAP_ARCHIVE_OPERATION = "storage.legacy-gap-archive"
_LEGACY_GAP_ARCHIVE_TENANT = "tenant:system:legacy"
_LEGACY_GAP_ARCHIVE_SOURCE = "system:legacy-gap"
_LEGACY_GAP_SHARD_BYTES = 40 * 1024 * 1024


def _legacy_identity_coverage(connection: object) -> dict[str, int]:
    """Count legacy identities represented by any live S3 canonical revision."""

    row = connection.execute(
        """SELECT count(*)::bigint AS total,
                  count(*) FILTER (
                      WHERE EXISTS (
                          SELECT 1
                            FROM canonical_events event
                            JOIN raw_artifacts artifact
                              ON artifact.tenant_id=event.tenant_id
                             AND artifact.source_id=event.source_id
                             AND artifact.artifact_id=event.artifact_id
                           WHERE event.source_id=source_events.source_id
                             AND event.native_id=source_events.native_id
                             AND artifact.storage_backend='s3'
                             AND artifact.state='live'
                      )
                  )::bigint AS canonical_identity_covered
             FROM source_events"""
    ).fetchone()
    return {
        key: int(row[key])
        for key in ("total", "canonical_identity_covered")
    }


def _legacy_uncovered_identity_query() -> str:
    return """SELECT legacy.id,legacy.source_id,legacy.native_id,
                     legacy.native_parent_id,legacy.kind,legacy.occurred_at,
                     legacy.observed_at,legacy.principal_id,legacy.visibility,
                     legacy.content_type,legacy.content_sha256,legacy.revision,
                     legacy.envelope,legacy.is_tombstone,legacy.batch_id,
                     legacy.created_at
                FROM source_events legacy
               WHERE legacy.id<=%s
                 AND NOT EXISTS (
                     SELECT 1
                       FROM canonical_events event
                       JOIN raw_artifacts artifact
                         ON artifact.tenant_id=event.tenant_id
                        AND artifact.source_id=event.source_id
                        AND artifact.artifact_id=event.artifact_id
                      WHERE event.source_id=legacy.source_id
                        AND event.native_id=legacy.native_id
                        AND artifact.storage_backend='s3'
                        AND artifact.state='live'
                 )
               ORDER BY legacy.id"""


def _legacy_gap_digest(rows: object) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(str(row["id"]).encode())
        digest.update(b"\0")
        digest.update(row["source_id"].encode())
        digest.update(b"\0")
        digest.update(row["native_id"].encode())
        digest.update(b"\0")
        digest.update(row["content_sha256"].encode())
        digest.update(b"\n")
        count += 1
    return digest.hexdigest(), count


def _legacy_event_json(row: object) -> bytes:
    value = {
        key: row[key]
        for key in (
            "id", "source_id", "native_id", "native_parent_id", "kind",
            "occurred_at", "observed_at", "principal_id", "visibility",
            "content_type", "content_sha256", "revision", "envelope",
            "is_tombstone", "batch_id", "created_at",
        )
    }
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        ).encode()
        + b"\n"
    )


def _archive_uncovered_legacy_storage(
    store: BrainStore,
    archive: S3ArchiveStore,
) -> dict[str, object]:
    """Move only legacy identities absent from canonical S3 into a private archive."""

    created_at = datetime.now(timezone.utc).isoformat()
    with tempfile.TemporaryDirectory(prefix="recall-legacy-gap-") as directory:
        root = Path(directory)
        shard_paths: list[tuple[Path, int]] = []
        identity_digest = hashlib.sha256()
        row_count = 0
        shard_count = 0
        uncompressed_bytes = 0
        current_rows = 0
        raw_file = None
        compressed = None

        def close_shard() -> None:
            nonlocal raw_file, compressed, current_rows, uncompressed_bytes
            if compressed is None or raw_file is None:
                return
            compressed.close()
            raw_file.close()
            path = root / f"legacy-gap-{len(shard_paths):06d}.jsonl.gz"
            shard_paths.append((path, current_rows))
            raw_file = None
            compressed = None
            current_rows = 0
            uncompressed_bytes = 0

        with store.connect() as connection:
            highwater = int(
                connection.execute(
                    "SELECT coalesce(max(id),0)::bigint AS value FROM source_events"
                ).fetchone()["value"]
            )
            cursor = connection.cursor(name="recall_legacy_gap_archive")
            cursor.itersize = 1000
            cursor.execute(_legacy_uncovered_identity_query(), (highwater,))
            for row in cursor:
                line = _legacy_event_json(row)
                if compressed is None:
                    path = root / f"legacy-gap-{len(shard_paths):06d}.jsonl.gz"
                    raw_file = path.open("wb")
                    compressed = gzip.GzipFile(
                        filename="", mode="wb", fileobj=raw_file, mtime=0,
                    )
                if current_rows and uncompressed_bytes + len(line) > _LEGACY_GAP_SHARD_BYTES:
                    close_shard()
                    path = root / f"legacy-gap-{len(shard_paths):06d}.jsonl.gz"
                    raw_file = path.open("wb")
                    compressed = gzip.GzipFile(
                        filename="", mode="wb", fileobj=raw_file, mtime=0,
                    )
                compressed.write(line)
                identity_digest.update(str(row["id"]).encode())
                identity_digest.update(b"\0")
                identity_digest.update(row["source_id"].encode())
                identity_digest.update(b"\0")
                identity_digest.update(row["native_id"].encode())
                identity_digest.update(b"\0")
                identity_digest.update(row["content_sha256"].encode())
                identity_digest.update(b"\n")
                current_rows += 1
                row_count += 1
                uncompressed_bytes += len(line)
            close_shard()

        digest = identity_digest.hexdigest()
        shard_contracts = []
        for index, (path, rows) in enumerate(shard_paths):
            payload = path.read_bytes()
            reference = archive.put(ArchiveRequest(
                tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
                source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
                native_id=f"legacy-gap:{digest[:24]}:{index:06d}",
                media_type="application/gzip",
                payload=payload,
            ))
            archive.verify(
                reference,
                tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
                source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
            )
            shard_contracts.append({
                "rows": rows,
                "artifact": reference.to_contract(
                    tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
                    source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
                    created_at=created_at,
                ),
            })
            shard_count += 1

        manifest = {
            "schema_version": 1,
            "kind": "recall.legacy-gap-archive",
            "created_at": created_at,
            "highwater_id": highwater,
            "row_count": row_count,
            "identity_sha256": digest,
            "shards": shard_contracts,
        }
        manifest_payload = json.dumps(
            manifest, sort_keys=True, separators=(",", ":"),
        ).encode()
        manifest_reference = archive.put(ArchiveRequest(
            tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
            source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
            native_id=f"legacy-gap-manifest:{digest[:32]}",
            media_type="application/json",
            payload=manifest_payload,
        ))
        if archive.read(
            manifest_reference,
            tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
            source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
        ) != manifest_payload:
            raise ArchiveError("legacy gap manifest verification failed")
        manifest_contract = manifest_reference.to_contract(
            tenant_id=_LEGACY_GAP_ARCHIVE_TENANT,
            source_id=_LEGACY_GAP_ARCHIVE_SOURCE,
            created_at=created_at,
        )

        with store.connect() as connection:
            connection.execute(
                """INSERT INTO audit_events(operation,status,metadata)
                   VALUES (%s,'success',%s)""",
                (
                    _LEGACY_GAP_ARCHIVE_OPERATION,
                    json.dumps({
                        "schema_version": 1,
                        "highwater_id": highwater,
                        "row_count": row_count,
                        "identity_sha256": digest,
                        "manifest": manifest_contract,
                    }),
                ),
            )
    return {
        "status": "ok",
        "highwater_id": highwater,
        "row_count": row_count,
        "identity_sha256": digest,
        "shards": shard_count,
        "manifest": manifest_contract,
    }


def _discard_derived_storage(
    store: BrainStore,
    relations: list[str],
) -> dict[str, object]:
    """Discard only rebuildable retrieval projections and reclaim their pages."""

    if (
        not relations
        or len(relations) > len(_DISCARDABLE_DERIVED_RELATIONS)
        or len(set(relations)) != len(relations)
        or not set(relations) <= _DISCARDABLE_DERIVED_RELATIONS
    ):
        raise ValueError("derived storage discard relation list is invalid")

    expanded = set(relations)
    if "turn_embeddings" in expanded:
        expanded.add("turn_embedding_items")
    if "canonical_passage_contexts" in expanded:
        expanded.add("canonical_passage_embedding_representations")
    ordered = sorted(expanded)

    with store.connect() as connection:
        existing = {
            row["relname"]
            for row in connection.execute(
                """SELECT relname
                     FROM pg_stat_user_tables
                    WHERE schemaname='public' AND relname=ANY(%s)""",
                (ordered,),
            ).fetchall()
        }
        if existing != set(ordered):
            raise ValueError("derived storage discard relation is unavailable")
        before_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (ordered,),
        ).fetchall()
        before = {
            row["relname"]: int(row["total_bytes"])
            for row in before_rows
        }

        # The fixed allowlist above is the identifier injection boundary.
        identifiers = ",".join(f'public."{name}"' for name in ordered)
        connection.execute(f"TRUNCATE TABLE {identifiers}")

        after_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (ordered,),
        ).fetchall()
        after = {
            row["relname"]: int(row["total_bytes"])
            for row in after_rows
        }

    results = [
        {
            "relation": relation,
            "before_bytes": before[relation],
            "after_bytes": after[relation],
            "reclaimed_bytes": max(0, before[relation] - after[relation]),
        }
        for relation in ordered
    ]
    return {
        "status": "ok",
        "before_bytes": sum(row["before_bytes"] for row in results),
        "after_bytes": sum(row["after_bytes"] for row in results),
        "reclaimed_bytes": sum(row["reclaimed_bytes"] for row in results),
        "relations": results,
    }


def _discard_empty_legacy_storage(store: BrainStore) -> dict[str, object]:
    """Reclaim the legacy plane only when its authoritative event table is empty."""

    with store.connect() as connection:
        existing = {
            row["relname"]
            for row in connection.execute(
                """SELECT relname
                     FROM pg_stat_user_tables
                    WHERE schemaname='public' AND relname=ANY(%s)""",
                (list(_EMPTY_LEGACY_RELATIONS),),
            ).fetchall()
        }
        if existing != set(_EMPTY_LEGACY_RELATIONS):
            raise ValueError("empty legacy storage relation is unavailable")

        has_source_events = bool(
            connection.execute(
                "SELECT EXISTS(SELECT 1 FROM public.source_events LIMIT 1) AS present"
            ).fetchone()["present"]
        )
        if has_source_events:
            raise ValueError("legacy source events are not empty")

        before_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (list(_EMPTY_LEGACY_RELATIONS),),
        ).fetchall()
        before = {
            row["relname"]: int(row["total_bytes"])
            for row in before_rows
        }

        identifiers = ",".join(
            f'public."{name}"' for name in _EMPTY_LEGACY_RELATIONS
        )
        connection.execute(f"TRUNCATE TABLE {identifiers}")

        after_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (list(_EMPTY_LEGACY_RELATIONS),),
        ).fetchall()
        after = {
            row["relname"]: int(row["total_bytes"])
            for row in after_rows
        }

    results = [
        {
            "relation": relation,
            "before_bytes": before[relation],
            "after_bytes": after[relation],
            "reclaimed_bytes": max(0, before[relation] - after[relation]),
        }
        for relation in _EMPTY_LEGACY_RELATIONS
    ]
    return {
        "status": "ok",
        "before_bytes": sum(row["before_bytes"] for row in results),
        "after_bytes": sum(row["after_bytes"] for row in results),
        "reclaimed_bytes": sum(row["reclaimed_bytes"] for row in results),
        "relations": results,
    }


def _legacy_storage_coverage(connection: object) -> dict[str, int]:
    """Count legacy events durably represented by an S3-backed canonical event."""

    row = connection.execute(
        """SELECT count(*)::bigint AS total,
                  count(*) FILTER (
                      WHERE EXISTS (
                          SELECT 1
                            FROM canonical_events event
                            JOIN raw_artifacts artifact
                              ON artifact.tenant_id=event.tenant_id
                             AND artifact.source_id=event.source_id
                             AND artifact.artifact_id=event.artifact_id
                           WHERE event.source_id=source_events.source_id
                             AND event.native_id=source_events.native_id
                             AND event.content_sha256=source_events.content_sha256
                             AND artifact.storage_backend='s3'
                             AND artifact.state='live'
                      )
                  )::bigint AS canonical_covered
             FROM source_events"""
    ).fetchone()
    return {key: int(row[key]) for key in ("total", "canonical_covered")}


def _create_legacy_coverage_index(store: BrainStore) -> None:
    """Build the one-time cross-tenant lookup without blocking canonical writes."""

    with store.connect() as connection:
        connection.autocommit = True
        try:
            connection.execute(
                """CREATE INDEX CONCURRENTLY IF NOT EXISTS
                          recall_legacy_coverage_tmp_idx
                       ON public.canonical_events(
                          source_id,native_id,content_sha256
                       ) INCLUDE (tenant_id,artifact_id)"""
            )
        finally:
            connection.autocommit = False


def _drop_legacy_coverage_index(store: BrainStore) -> None:
    """Remove the one-time lookup after the legacy plane is cut or refused."""

    with store.connect() as connection:
        connection.autocommit = True
        try:
            connection.execute(
                """DROP INDEX CONCURRENTLY IF EXISTS
                          public.recall_legacy_coverage_tmp_idx"""
            )
        finally:
            connection.autocommit = False


def _verified_legacy_gap_archive(
    store: BrainStore,
    archive: S3ArchiveStore,
) -> dict[str, object] | None:
    with store.connect() as connection:
        row = connection.execute(
            """SELECT metadata
                 FROM audit_events
                WHERE operation=%s AND status='success'
                ORDER BY id DESC
                LIMIT 1""",
            (_LEGACY_GAP_ARCHIVE_OPERATION,),
        ).fetchone()
    if row is None:
        return None
    metadata = row["metadata"]
    if not isinstance(metadata, dict):
        raise ValueError("legacy gap archive metadata is invalid")
    manifest_reference = metadata.get("manifest")
    if not isinstance(manifest_reference, dict):
        raise ValueError("legacy gap archive metadata is invalid")
    manifest_payload = archive.read_raw(manifest_reference)
    try:
        manifest = json.loads(manifest_payload)
    except json.JSONDecodeError:
        raise ValueError("legacy gap archive manifest is invalid") from None
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("kind") != "recall.legacy-gap-archive"
        or manifest.get("row_count") != metadata.get("row_count")
        or manifest.get("identity_sha256") != metadata.get("identity_sha256")
        or manifest.get("highwater_id") != metadata.get("highwater_id")
        or not isinstance(manifest.get("shards"), list)
        or sum(
            shard.get("rows", -1)
            for shard in manifest["shards"]
            if isinstance(shard, dict)
        ) != manifest["row_count"]
    ):
        raise ValueError("legacy gap archive manifest is invalid")
    for shard in manifest["shards"]:
        if not isinstance(shard, dict) or not isinstance(shard.get("artifact"), dict):
            raise ValueError("legacy gap archive manifest is invalid")
        archive.verify_raw(shard["artifact"])
    return manifest


def _discard_covered_legacy_storage(
    store: BrainStore,
    archive: S3ArchiveStore | None = None,
) -> dict[str, object]:
    """Reclaim legacy rows superseded by S3 identity or a verified gap archive."""

    verified_archive = (
        _verified_legacy_gap_archive(store, archive)
        if archive is not None
        else None
    )

    with store.connect() as connection:
        existing = {
            row["relname"]
            for row in connection.execute(
                """SELECT relname
                     FROM pg_stat_user_tables
                    WHERE schemaname='public' AND relname=ANY(%s)""",
                (list(_EMPTY_LEGACY_RELATIONS),),
            ).fetchall()
        }
        if existing != set(_EMPTY_LEGACY_RELATIONS):
            raise ValueError("covered legacy storage relation is unavailable")

        # Freeze legacy writers and the canonical proof rows for the short final
        # gate. This prevents a covered row from changing between proof and cut.
        connection.execute("LOCK TABLE public.source_events IN ACCESS EXCLUSIVE MODE")
        connection.execute(
            "LOCK TABLE public.canonical_events,public.raw_artifacts IN SHARE MODE"
        )
        coverage = _legacy_identity_coverage(connection)
        highwater = int(
            connection.execute(
                "SELECT coalesce(max(id),0)::bigint AS value FROM source_events"
            ).fetchone()["value"]
        )
        uncovered_cursor = connection.execute(
            _legacy_uncovered_identity_query(), (highwater,),
        )
        uncovered_digest, uncovered_count = _legacy_gap_digest(uncovered_cursor)
        if (
            coverage["canonical_identity_covered"] + uncovered_count
            != coverage["total"]
        ):
            raise ValueError("legacy identity coverage is inconsistent")
        expected_archive = verified_archive or {}
        archived = (
            uncovered_count == 0
            or expected_archive.get("row_count") == uncovered_count
            and expected_archive.get("identity_sha256") == uncovered_digest
            and int(expected_archive.get("highwater_id", -1)) <= highwater
        )
        if not archived:
            raise ValueError("legacy source events are not fully S3-identity-covered")

        before_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (list(_EMPTY_LEGACY_RELATIONS),),
        ).fetchall()
        before = {
            row["relname"]: int(row["total_bytes"])
            for row in before_rows
        }

        identifiers = ",".join(
            f'public."{name}"' for name in _EMPTY_LEGACY_RELATIONS
        )
        connection.execute(f"TRUNCATE TABLE {identifiers}")

        after_rows = connection.execute(
            """SELECT relname,
                      pg_total_relation_size(relid) AS total_bytes
                 FROM pg_stat_user_tables
                WHERE schemaname='public' AND relname=ANY(%s)""",
            (list(_EMPTY_LEGACY_RELATIONS),),
        ).fetchall()
        after = {
            row["relname"]: int(row["total_bytes"])
            for row in after_rows
        }

    results = [
        {
            "relation": relation,
            "before_bytes": before[relation],
            "after_bytes": after[relation],
            "reclaimed_bytes": max(0, before[relation] - after[relation]),
        }
        for relation in _EMPTY_LEGACY_RELATIONS
    ]
    return {
        "status": "ok",
        "coverage": {
            **coverage,
            "archived_uncovered": uncovered_count,
        },
        "before_bytes": sum(row["before_bytes"] for row in results),
        "after_bytes": sum(row["after_bytes"] for row in results),
        "reclaimed_bytes": sum(row["reclaimed_bytes"] for row in results),
        "relations": results,
    }


def _discard_covered_legacy_storage_indexed(
    store: BrainStore,
    archive: S3ArchiveStore | None = None,
) -> dict[str, object]:
    """Use a disposable lookup index for the one-time global coverage cut."""

    _create_legacy_coverage_index(store)
    try:
        if archive is None:
            return _discard_covered_legacy_storage(store)
        return _discard_covered_legacy_storage(store, archive)
    finally:
        _drop_legacy_coverage_index(store)


def _storage_authority_audit(
    store: BrainStore,
    tenant_id: str,
) -> dict[str, object]:
    """Prove database-side coverage before bodies move to object storage only."""

    with store.connect() as connection:
        legacy_report = _legacy_storage_coverage(connection)
        documents = connection.execute(
            """SELECT count(*)::bigint AS total,
                      count(*) FILTER (
                          WHERE artifact.storage_backend='s3'
                            AND artifact.state='live'
                      )::bigint AS s3_raw_covered
                 FROM canonical_documents document
                 JOIN canonical_events event
                   USING(tenant_id,source_id,event_id)
                 JOIN raw_artifacts artifact
                   ON artifact.tenant_id=event.tenant_id
                  AND artifact.source_id=event.source_id
                  AND artifact.artifact_id=event.artifact_id
                WHERE document.tenant_id=%s
                  AND document.is_current
                  AND document.deleted_at IS NULL""",
            (tenant_id,),
        ).fetchone()
        logical = connection.execute(
            """WITH source_groups AS (
                       SELECT DISTINCT event.source_id,
                              COALESCE(event.native_parent_id,event.native_id)
                                  AS native_parent_id
                         FROM canonical_documents document
                         JOIN canonical_events event
                           USING(tenant_id,source_id,event_id)
                        WHERE document.tenant_id=%s
                          AND document.is_current
                          AND document.deleted_at IS NULL
                   )
                   SELECT count(*)::bigint AS total,
                          count(evidence.logical_document_id)::bigint
                              AS projected
                     FROM source_groups source_group
                     LEFT JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=%s
                      AND evidence.source_id=source_group.source_id
                      AND evidence.native_parent_id=source_group.native_parent_id""",
            (tenant_id, tenant_id),
        ).fetchone()
        evidence = connection.execute(
            """WITH part_coverage AS (
                       SELECT document.logical_document_id,document.source_id,
                              count(part.part_ordinal)::bigint AS actual_parts,
                              count(*) FILTER (
                                  WHERE part.storage_backend<>'s3'
                              )::bigint AS non_s3_parts
                         FROM canonical_evidence_documents document
                         LEFT JOIN canonical_evidence_document_parts part
                           USING(tenant_id,source_id,logical_document_id,revision)
                        WHERE document.tenant_id=%s
                        GROUP BY document.logical_document_id,document.source_id
                   )
                   SELECT count(*)::bigint AS total,
                          count(*) FILTER (
                              WHERE document.manifest_storage_backend='s3'
                                AND coverage.actual_parts=document.part_count
                                AND coverage.actual_parts>0
                                AND coverage.non_s3_parts=0
                          )::bigint AS s3_pointer_complete,
                          count(passage.logical_document_id)::bigint
                              AS passage_projected
                     FROM canonical_evidence_documents document
                     JOIN part_coverage coverage
                       USING(logical_document_id,source_id)
                     LEFT JOIN canonical_passage_documents passage
                       ON passage.tenant_id=document.tenant_id
                      AND passage.source_id=document.source_id
                      AND passage.logical_document_id=document.logical_document_id
                      AND passage.revision=document.revision
                    WHERE document.tenant_id=%s""",
            (tenant_id, tenant_id),
        ).fetchone()
        queues = connection.execute(
            """SELECT
                   (SELECT count(*) FROM canonical_evidence_document_queue
                     WHERE tenant_id=%s)::bigint AS logical,
                   (SELECT count(*) FROM canonical_passage_projection_queue
                     WHERE tenant_id=%s)::bigint AS passage,
                   (SELECT count(*) FROM canonical_evidence_cleanup_queue
                     WHERE tenant_id=%s)::bigint AS cleanup""",
            (tenant_id, tenant_id, tenant_id),
        ).fetchone()

    document_report = {key: int(documents[key]) for key in documents}
    logical_report = {key: int(logical[key]) for key in logical}
    evidence_report = {key: int(evidence[key]) for key in evidence}
    queue_report = {key: int(queues[key]) for key in queues}
    database_coverage_complete = all(
        (
            legacy_report["canonical_covered"] == legacy_report["total"],
            document_report["s3_raw_covered"] == document_report["total"],
            logical_report["projected"] == logical_report["total"],
            evidence_report["s3_pointer_complete"] == evidence_report["total"],
            evidence_report["passage_projected"] == evidence_report["total"],
            queue_report["logical"] == 0,
            queue_report["passage"] == 0,
        )
    )
    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "database_coverage_complete": database_coverage_complete,
        "object_verification_required": database_coverage_complete,
        "legacy": legacy_report,
        "canonical_documents": document_report,
        "logical_groups": logical_report,
        "logical_evidence": evidence_report,
        "queues": queue_report,
    }


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(prog="recall-server")
    ap.add_argument("--dsn", default=os.environ.get("RECALL_DATABASE_URL"))
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    sub.add_parser("storage-footprint")
    compact_storage = sub.add_parser("storage-compact")
    compact_storage.add_argument(
        "--relation",
        action="append",
        required=True,
        help="exact public user relation to rewrite; repeat for more than one",
    )
    discard_derived = sub.add_parser("storage-discard-derived")
    discard_derived.add_argument(
        "--relation",
        action="append",
        required=True,
        choices=sorted(_DISCARDABLE_DERIVED_RELATIONS),
        help="exact rebuildable projection to discard; repeat for more than one",
    )
    sub.add_parser("storage-discard-empty-legacy")
    sub.add_parser("storage-archive-legacy-gap")
    sub.add_parser("storage-discard-covered-legacy")
    authority_audit = sub.add_parser("storage-authority-audit")
    authority_audit.add_argument("--tenant", required=True)
    thin_bodies = sub.add_parser("storage-thin-canonical-bodies")
    thin_bodies.add_argument("--tenant", required=True)
    thin_bodies.add_argument("--batch-size", type=int, default=1_000)
    thin_bodies.add_argument("--max-batches", type=int, default=1)
    sub.add_parser("archive-check")
    sub.add_parser("evidence-archive-check")
    publish_duckdb = sub.add_parser("publish-archil-duckdb")
    publish_duckdb.add_argument("--path", type=Path, required=True)
    publish_duckdb.add_argument("--version", required=True)
    publish_duckdb.add_argument("--sha256", required=True)
    capability = sub.add_parser("capability-check")
    capability.add_argument(
        "--profile", choices=("production", "local-fixture"), default="production"
    )
    deployment = sub.add_parser("deployment-preview")
    deployment.add_argument("--manifest", type=Path, required=True)
    approval = sub.add_parser("deployment-approval-check")
    approval.add_argument("--manifest", type=Path, required=True)
    approval.add_argument("--approvals", type=Path, required=True)
    apply = sub.add_parser("deployment-apply")
    apply.add_argument("--manifest", type=Path, required=True)
    apply.add_argument("--approvals", type=Path, required=True)
    apply.add_argument("--planetscale-organization", required=True)
    apply.add_argument("--database-name", required=True)
    apply.add_argument("--render-owner-id", required=True)
    apply.add_argument("--core-name", required=True)
    apply.add_argument("--gateway-name", required=True)
    apply.add_argument("--tailnet-hostname", required=True)
    apply.add_argument("--tailnet-tag", required=True)
    sub.add_parser("rebuild")
    backfill_entities = sub.add_parser("backfill-entities")
    backfill_entities.add_argument("--batch-size", type=int, default=5000)
    backfill_entities.add_argument("--max-batches", type=int)
    backfill_redaction = sub.add_parser("backfill-redaction")
    backfill_redaction.add_argument("--batch-size", type=int, default=5000)
    backfill_redaction.add_argument("--max-batches", type=int)
    backfill_redaction.add_argument("--workers", type=int, default=1)
    backfill_cowork = sub.add_parser("backfill-cowork-sessions")
    backfill_cowork.add_argument("--batch-size", type=int, default=5000)
    backfill_cowork.add_argument("--max-batches", type=int)
    backfill_embeddings = sub.add_parser("backfill-embeddings")
    backfill_embeddings.add_argument("--batch-size", type=int, default=128)
    backfill_embeddings.add_argument("--max-batches", type=int)
    backfill_embeddings.add_argument("--source-id")
    backfill_embeddings.add_argument("--surface")
    backfill_turn_embeddings = sub.add_parser("backfill-turn-embeddings")
    backfill_turn_embeddings.add_argument("--batch-size", type=int, default=128)
    backfill_turn_embeddings.add_argument("--max-batches", type=int)
    backfill_turn_embeddings.add_argument("--source-id")
    canonical_embeddings = sub.add_parser("backfill-canonical-embeddings")
    canonical_embeddings.add_argument("--tenant")
    canonical_embeddings.add_argument("--batch-size", type=int, default=100)
    canonical_embeddings.add_argument("--max-batches", type=int, default=10)
    canonical_embedding_worker = sub.add_parser("canonical-embedding-worker")
    canonical_embedding_worker.add_argument("--tenant")
    canonical_embedding_worker.add_argument("--batch-size", type=int, default=128)
    canonical_embedding_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    canonical_embedding_worker.add_argument("--parallel-tenants", type=int, default=1)
    canonical_embedding_worker.add_argument("--interval-seconds", type=float, default=5)
    canonical_embedding_worker.add_argument("--once", action="store_true")
    canonical_evidence = sub.add_parser("backfill-canonical-evidence")
    canonical_evidence.add_argument("--tenant", required=True)
    canonical_evidence.add_argument("--batch-size", type=int, default=100)
    canonical_evidence.add_argument("--max-batches", type=int, default=10)
    canonical_evidence_worker = sub.add_parser("canonical-evidence-worker")
    canonical_evidence_worker.add_argument("--tenant", required=True)
    canonical_evidence_worker.add_argument("--batch-size", type=int, default=100)
    canonical_evidence_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    canonical_evidence_worker.add_argument("--interval-seconds", type=float, default=5)
    canonical_evidence_worker.add_argument("--once", action="store_true")
    logical_evidence = sub.add_parser("backfill-logical-evidence")
    logical_evidence.add_argument("--tenant", required=True)
    logical_evidence.add_argument(
        "--source",
        help="queue only this exact source within the tenant",
    )
    logical_evidence.add_argument("--batch-size", type=int, default=25)
    logical_evidence.add_argument("--max-batches", type=int, default=10)
    logical_evidence.add_argument("--upload-concurrency", type=int, default=2)
    logical_evidence.add_argument(
        "--cursor-fetch-rows",
        type=int,
        default=10_000,
    )
    logical_evidence.add_argument(
        "--rebuild-existing",
        action="store_true",
        help="queue current logical documents even when a projection already exists",
    )
    logical_evidence_worker = sub.add_parser("logical-evidence-worker")
    logical_evidence_worker.add_argument("--tenant", required=True)
    logical_evidence_worker.add_argument("--batch-size", type=int, default=25)
    logical_evidence_worker.add_argument(
        "--max-batches-per-cycle", type=int, default=10
    )
    logical_evidence_worker.add_argument("--upload-concurrency", type=int, default=2)
    logical_evidence_worker.add_argument(
        "--cursor-fetch-rows",
        type=int,
        default=10_000,
    )
    logical_evidence_worker.add_argument("--interval-seconds", type=float, default=5)
    logical_evidence_worker.add_argument("--once", action="store_true")
    passage_backfill = sub.add_parser("backfill-lossless-passages")
    passage_backfill.add_argument("--tenant", required=True)
    passage_backfill.add_argument("--target-tokens", type=int, default=1024)
    passage_backfill.add_argument("--overlap-tokens", type=int, default=128)
    passage_backfill.add_argument("--batch-size", type=int, default=100)
    passage_backfill.add_argument("--max-batches", type=int, default=10)
    passage_backfill.add_argument("--concurrency", type=int, default=4)
    passage_worker = sub.add_parser("lossless-passage-worker")
    passage_worker.add_argument("--tenant", required=True)
    passage_worker.add_argument("--target-tokens", type=int, default=1024)
    passage_worker.add_argument("--overlap-tokens", type=int, default=128)
    passage_worker.add_argument(
        "--projection-batch-size",
        type=int,
        default=100,
    )
    passage_worker.add_argument(
        "--embedding-batch-size",
        type=int,
        default=128,
    )
    passage_worker.add_argument(
        "--max-batches-per-cycle",
        type=int,
        default=10,
    )
    passage_worker.add_argument("--concurrency", type=int, default=4)
    passage_worker.add_argument(
        "--interval-seconds",
        type=float,
        default=5,
    )
    passage_worker.add_argument("--once", action="store_true")
    parquet_backfill = sub.add_parser("backfill-parquet-scan")
    parquet_backfill.add_argument("--tenant", required=True)
    parquet_backfill.add_argument("--source")
    parquet_backfill.add_argument("--batch-size", type=int, default=4)
    parquet_backfill.add_argument("--max-batches", type=int, default=10)
    projection_worker = sub.add_parser("projection-worker")
    projection_worker.add_argument("--tenant", required=True)
    projection_worker.add_argument("--target-tokens", type=int, default=1024)
    projection_worker.add_argument("--overlap-tokens", type=int, default=128)
    projection_worker.add_argument("--logical-batch-size", type=int, default=25)
    projection_worker.add_argument("--passage-batch-size", type=int, default=100)
    projection_worker.add_argument("--embedding-batch-size", type=int, default=128)
    projection_worker.add_argument("--max-batches-per-cycle", type=int, default=10)
    projection_worker.add_argument("--upload-concurrency", type=int, default=2)
    projection_worker.add_argument("--passage-concurrency", type=int, default=4)
    projection_worker.add_argument("--cursor-fetch-rows", type=int, default=10_000)
    projection_worker.add_argument("--interval-seconds", type=float, default=5)
    projection_worker.add_argument("--once", action="store_true")
    sub.add_parser("export")
    conformance = sub.add_parser("mcp-conformance")
    conformance.add_argument("--config", type=Path, required=True)
    create_token = sub.add_parser("token-create")
    create_token.add_argument("name")
    create_token.add_argument("--source")
    create_token.add_argument(
        "--tenant",
        help="bind a canonical v2 write credential to exactly one tenant",
    )
    create_token.add_argument(
        "--principal",
        help="read every source granted to this principal; writes stay source-bound",
    )
    create_token.add_argument(
        "--capture-origin",
        help="host-bound origin for deliberate MCP capture tools",
    )
    create_token.add_argument(
        "--webhook-privacy-mode",
        choices=("scrub", "drop"),
        help="server-owned privacy mode for a source-scoped webhook capability",
    )
    create_token.add_argument("--scopes", default="read,write")
    create_token.add_argument(
        "--output",
        required=True,
        help="write the one-time plaintext credential to a new mode-0600 file",
    )
    revoke_token = sub.add_parser("token-revoke")
    revoke_token.add_argument("name")
    brain = sub.add_parser("brain-provision")
    brain.add_argument("--organization", required=True)
    brain.add_argument("--kind", choices=("personal", "company"), required=True)
    brain.add_argument("--display-name", required=True)
    brain.add_argument("--tenant", required=True)
    brain.add_argument("--slug", required=True)
    brain.add_argument("--owner-principal", required=True)
    bind_employee = sub.add_parser("employee-source-bind")
    bind_employee.add_argument("--tenant", required=True)
    bind_employee.add_argument("--employee-key", required=True)
    bind_employee.add_argument("--display-name", required=True)
    bind_employee.add_argument("--source", action="append", required=True)
    create_mcp_token = sub.add_parser("mcp-token-create")
    create_mcp_token.add_argument("name")
    create_mcp_token.add_argument("--tenant", required=True)
    create_mcp_token.add_argument("--principal", required=True)
    create_mcp_token.add_argument(
        "--principal-kind", choices=("human", "workload"), required=True
    )
    create_mcp_token.add_argument("--scopes", default="read")
    create_mcp_token.add_argument("--expires-in-days", type=int, default=30)
    create_mcp_token.add_argument("--output", required=True)
    revoke_mcp_token = sub.add_parser("mcp-token-revoke")
    revoke_mcp_token.add_argument("name")
    create_admin_token = sub.add_parser("admin-token-create")
    create_admin_token.add_argument("name")
    create_admin_token.add_argument("--principal", required=True)
    create_admin_token.add_argument("--expires-in-days", type=int, default=30)
    create_admin_token.add_argument("--output", required=True)
    revoke_admin_token = sub.add_parser("admin-token-revoke")
    revoke_admin_token.add_argument("name")
    managed_worker = sub.add_parser("managed-worker")
    managed_worker.add_argument(
        "--state-root",
        type=Path,
        default=Path("/var/lib/recall/worker"),
    )
    managed_worker.add_argument("--once", action="store_true")
    managed_worker.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
    )
    source_profile = sub.add_parser("source-profile-set")
    source_profile.add_argument("source_id")
    source_profile.add_argument(
        "--family", choices=sorted(SOURCE_FAMILIES), required=True
    )
    source_profile.add_argument(
        "--quality", choices=sorted(QUALITY_SCORES), required=True
    )
    source_profile.add_argument("--freshness-half-life-days", type=int, required=True)
    source_alias = sub.add_parser("source-alias-set")
    source_alias.add_argument("alias")
    source_alias.add_argument("source_id")
    sub.add_parser("federation-scoreboard")
    server = sub.add_parser("serve")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8788)
    server.add_argument("--unix-socket")
    server.add_argument("--require-auth", action="store_true")
    server.add_argument(
        "--capability-profile",
        choices=("production", "local-fixture"),
    )
    args = ap.parse_args()
    if args.command == "deployment-preview":
        try:
            print(json.dumps(preview(load_manifest(args.manifest)), sort_keys=True))
        except DeploymentManifestError:
            print(
                json.dumps({"status": "rejected", "code": "manifest_invalid"}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "deployment-approval-check":
        try:
            manifest = load_manifest(args.manifest)
            plan_sha256 = preview(manifest)["plan_sha256"]
            print(
                json.dumps(
                    approval_status(
                        load_approvals(args.approvals, plan_sha256),
                    ),
                    sort_keys=True,
                )
            )
        except (ApprovalError, DeploymentManifestError) as error:
            code = (
                error.code if isinstance(error, ApprovalError) else "manifest_invalid"
            )
            print(json.dumps({"status": "rejected", "code": code}), file=sys.stderr)
            raise SystemExit(2) from None
        return
    if args.command == "deployment-apply":
        try:
            manifest = load_manifest(args.manifest)
            approvals = load_approvals(args.approvals, preview(manifest)["plan_sha256"])
            pending = approval_status(approvals)["pending_gates"]
            if any(gate != "writer-cutover" for gate in pending):
                raise ApprovalError("infrastructure_approval_required")
            adapters = build_live_adapters(
                planetscale_organization=args.planetscale_organization,
                database_name=args.database_name,
                render_owner_id=args.render_owner_id,
                core_name=args.core_name,
                gateway_name=args.gateway_name,
                tailnet_hostname=args.tailnet_hostname,
                tailnet_tag=args.tailnet_tag,
            )
            print(
                json.dumps(
                    reconcile_infrastructure(manifest, approvals, adapters),
                    sort_keys=True,
                )
            )
        except (
            ApprovalError,
            DeploymentManifestError,
            LiveProviderError,
        ) as error:
            code = (
                error.code
                if isinstance(error, (ApprovalError, LiveProviderError))
                else "manifest_invalid"
            )
            print(
                json.dumps({"status": "rejected", "code": code}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "mcp-conformance":
        try:
            report = run_conformance(McpConformanceConfig.load(args.config))
            print(json.dumps(report, sort_keys=True))
        except ConformanceError:
            print(
                json.dumps({"status": "rejected", "code": "mcp_conformance_failed"}),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "archive-check":
        try:
            print(json.dumps(probe_archive(build_archive_store()), sort_keys=True))
        except (ArchiveError, ValueError):
            print(
                json.dumps(
                    {"status": "rejected", "code": "archive_check_failed"},
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "evidence-archive-check":
        try:
            print(
                json.dumps(
                    probe_archive(build_evidence_archive_store()),
                    sort_keys=True,
                )
            )
        except (ArchiveError, ValueError):
            print(
                json.dumps(
                    {"status": "rejected", "code": "evidence_archive_check_failed"},
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            raise SystemExit(2) from None
        return
    if args.command == "publish-archil-duckdb":
        if (
            not args.path.is_file()
            or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", args.version) is None
        ):
            raise ValueError("DuckDB tool artifact is invalid")
        payload = args.path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if (
            not 1_000_000 <= len(payload) <= 100_000_000
            or digest != args.sha256
        ):
            raise ValueError("DuckDB tool artifact is invalid")
        reference = build_evidence_archive_store().put_raw(
            tenant_id="tenant:system:tools",
            source_id="source:system:tools",
            # Archil serverless execution currently runs on aarch64. Keep the
            # architecture in the immutable source identity so an x86 build
            # cannot silently replace the executable used by recall_scan.
            native_id=f"archil-duckdb:{args.version}:linux-arm64",
            payload=payload,
            media_type="application/vnd.duckdb.cli",
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        print(json.dumps({
            "status": "published",
            "version": args.version,
            "object_key": reference["object_key"],
            "content_sha256": reference["content_sha256"],
            "size_bytes": reference["size_bytes"],
        }, sort_keys=True))
        return
    if not args.dsn:
        ap.error("--dsn or RECALL_DATABASE_URL is required")
    if args.command == "capability-check":
        try:
            print(json.dumps(probe_database(args.dsn, args.profile), sort_keys=True))
        except CapabilityError as error:
            print(
                json.dumps({"status": "rejected", "code": error.code}), file=sys.stderr
            )
            raise SystemExit(2) from None
        return
    if args.command == "serve" and args.capability_profile:
        try:
            probe_database(args.dsn, args.capability_profile)
        except CapabilityError as error:
            print(
                json.dumps({"status": "rejected", "code": error.code}), file=sys.stderr
            )
            raise SystemExit(2) from None
    pool_max_size = _worker_pool_max_size(args)
    store = BrainStore(
        args.dsn,
        semantic_runtime=SemanticRuntime.from_env(),
        pool_max_size=pool_max_size,
    )
    if args.command == "migrate":
        store.migrate()
        print(json.dumps({"status": "ok", "schema_version": SCHEMA_VERSION}))
    elif args.command == "storage-footprint":
        print(json.dumps(_storage_footprint(store), sort_keys=True))
    elif args.command == "storage-compact":
        print(
            json.dumps(
                _compact_storage(store, args.relation),
                sort_keys=True,
            )
        )
    elif args.command == "storage-discard-derived":
        print(
            json.dumps(
                _discard_derived_storage(store, args.relation),
                sort_keys=True,
            )
        )
    elif args.command == "storage-discard-empty-legacy":
        print(json.dumps(_discard_empty_legacy_storage(store), sort_keys=True))
    elif args.command == "storage-archive-legacy-gap":
        print(
            json.dumps(
                _archive_uncovered_legacy_storage(store, build_archive_store()),
                sort_keys=True,
            )
        )
    elif args.command == "storage-discard-covered-legacy":
        print(
            json.dumps(
                _discard_covered_legacy_storage_indexed(
                    store, build_archive_store(),
                ),
                sort_keys=True,
            )
        )
    elif args.command == "storage-authority-audit":
        print(json.dumps(_storage_authority_audit(store, args.tenant), sort_keys=True))
    elif args.command == "storage-thin-canonical-bodies":
        print(
            json.dumps(
                thin_canonical_bodies(
                    store,
                    tenant_id=args.tenant,
                    batch_size=args.batch_size,
                    max_batches=args.max_batches,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "rebuild":
        print(json.dumps(store.rebuild(), sort_keys=True))
    elif args.command == "managed-worker":
        print(
            json.dumps(
                run_managed_worker(
                    store,
                    state_root=args.state_root,
                    once=args.once,
                    interval_seconds=args.interval_seconds,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-entities":
        print(
            json.dumps(
                store.backfill_entities(args.batch_size, args.max_batches),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-redaction":
        print(
            json.dumps(
                store.backfill_redaction(
                    args.batch_size,
                    args.max_batches,
                    args.workers,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-cowork-sessions":
        print(
            json.dumps(
                store.backfill_cowork_sessions(
                    args.batch_size,
                    args.max_batches,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-embeddings":
        print(
            json.dumps(
                store.embed_pending(
                    args.batch_size,
                    args.max_batches,
                    args.source_id,
                    args.surface,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-turn-embeddings":
        print(
            json.dumps(
                store.embed_pending_turns(
                    args.batch_size,
                    args.max_batches,
                    args.source_id,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "backfill-canonical-embeddings":
        print(
            json.dumps(
                CanonicalRetrieval(store).embed_pending(
                    tenant_id=args.tenant,
                    batch_size=args.batch_size,
                    max_batches=args.max_batches,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "canonical-embedding-worker":
        print(
            json.dumps(
                run_canonical_embedding_worker(
                    CanonicalRetrieval(store),
                    tenant_id=args.tenant,
                    parallel_tenants=args.parallel_tenants,
                    batch_size=args.batch_size,
                    max_batches_per_cycle=args.max_batches_per_cycle,
                    interval_seconds=args.interval_seconds,
                    once=args.once,
                ),
                sort_keys=True,
            )
        )
    elif args.command in {
        "backfill-canonical-evidence",
        "canonical-evidence-worker",
    }:
        projector = CanonicalEvidenceProjector(
            store,
            EvidenceProjectionStore(build_evidence_archive_store()),
            bound_tenant_id=args.tenant,
        )
        if args.command == "backfill-canonical-evidence":
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
            )
        else:
            result = run_canonical_evidence_worker(
                projector,
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command in {
        "backfill-logical-evidence",
        "logical-evidence-worker",
    }:
        projector = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
                part_upload_concurrency=min(4, args.upload_concurrency),
            ),
            bound_tenant_id=args.tenant,
            raw_archive=build_archive_store(),
            cursor_fetch_rows=args.cursor_fetch_rows,
        )
        if args.command == "backfill-logical-evidence":
            seeded = projector.seed_backfill(
                tenant_id=args.tenant,
                source_id=args.source,
                include_existing=args.rebuild_existing,
            )
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
                upload_concurrency=args.upload_concurrency,
            )
            result = {"seeded": seeded, **result}
        else:
            result = run_logical_evidence_worker(
                projector,
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                upload_concurrency=args.upload_concurrency,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command in {
        "backfill-lossless-passages",
        "lossless-passage-worker",
    }:
        projector = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
            ),
            policy=PassagePolicy(
                target_tokens=args.target_tokens,
                overlap_tokens=args.overlap_tokens,
            ),
            bound_tenant_id=args.tenant,
        )
        if args.command == "backfill-lossless-passages":
            seeded = projector.seed_backfill(tenant_id=args.tenant)
            result = projector.project_pending(
                tenant_id=args.tenant,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
                concurrency=args.concurrency,
            )
            result = {"seeded": seeded, **result}
        else:
            result = run_passage_worker(
                projector,
                tenant_id=args.tenant,
                projection_batch_size=args.projection_batch_size,
                embedding_batch_size=args.embedding_batch_size,
                max_batches_per_cycle=args.max_batches_per_cycle,
                concurrency=args.concurrency,
                interval_seconds=args.interval_seconds,
                once=args.once,
            )
        print(json.dumps(result, sort_keys=True))
    elif args.command == "backfill-parquet-scan":
        projector = CanonicalParquetScanProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
        )
        seeded = projector.seed_backfill(
            tenant_id=args.tenant,
            source_id=args.source,
        )
        print(
            json.dumps(
                {
                    "seeded": seeded,
                    **projector.project_pending(
                        tenant_id=args.tenant,
                        batch_size=args.batch_size,
                        max_batches=args.max_batches,
                    ),
                },
                sort_keys=True,
            )
        )
    elif args.command == "projection-worker":
        logical = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store(),
                part_upload_concurrency=min(4, args.upload_concurrency),
            ),
            bound_tenant_id=args.tenant,
            raw_archive=build_archive_store(),
            cursor_fetch_rows=args.cursor_fetch_rows,
        )
        passages = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
            policy=PassagePolicy(
                target_tokens=args.target_tokens,
                overlap_tokens=args.overlap_tokens,
            ),
            bound_tenant_id=args.tenant,
        )
        scan = CanonicalParquetScanProjector(
            store,
            LogicalEvidenceProjectionStore(build_evidence_archive_store()),
        )
        print(
            json.dumps(
                run_projection_worker(
                    logical,
                    passages,
                    scan,
                    tenant_id=args.tenant,
                    logical_batch_size=args.logical_batch_size,
                    passage_batch_size=args.passage_batch_size,
                    embedding_batch_size=args.embedding_batch_size,
                    max_batches_per_cycle=args.max_batches_per_cycle,
                    upload_concurrency=args.upload_concurrency,
                    passage_concurrency=args.passage_concurrency,
                    interval_seconds=args.interval_seconds,
                    once=args.once,
                    body_thinner=lambda: thin_canonical_bodies(
                        store,
                        tenant_id=args.tenant,
                        batch_size=1_000,
                        max_batches=1,
                    ),
                ),
                sort_keys=True,
            )
        )
    elif args.command == "export":
        for envelope in store.export_raw():
            print(json.dumps(envelope, sort_keys=True))
    elif args.command == "token-create":
        credential = store.create_collector_token(
            args.name,
            args.source,
            [scope.strip() for scope in args.scopes.split(",") if scope.strip()],
            tenant_id=args.tenant,
            principal_id=args.principal,
            capture_origin=args.capture_origin,
            webhook_privacy_mode=args.webhook_privacy_mode,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "token-revoke":
        print(json.dumps({"revoked": store.revoke_collector_token(args.name)}))
    elif args.command == "brain-provision":
        print(
            json.dumps(
                store.provision_brain(
                    organization_id=args.organization,
                    organization_kind=args.kind,
                    display_name=args.display_name,
                    tenant_id=args.tenant,
                    brain_kind=args.kind,
                    slug=args.slug,
                    owner_principal_id=args.owner_principal,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "employee-source-bind":
        print(
            json.dumps(
                store.bind_coding_sources_to_employee(
                    tenant_id=args.tenant,
                    employee_key=args.employee_key,
                    display_name=args.display_name,
                    source_ids=args.source,
                ),
                sort_keys=True,
            )
        )
    elif args.command == "mcp-token-create":
        credential = store.create_mcp_token(
            args.name,
            tenant_id=args.tenant,
            principal_id=args.principal,
            principal_kind=args.principal_kind,
            scopes=[scope.strip() for scope in args.scopes.split(",") if scope.strip()],
            expires_in_days=args.expires_in_days,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(
            args.output,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "mcp-token-revoke":
        print(json.dumps({"revoked": store.revoke_mcp_token(args.name)}))
    elif args.command == "admin-token-create":
        credential = ControlPlane(store, SecretBox.from_env(), {}).create_admin_token(
            args.name,
            principal_id=args.principal,
            expires_in_days=args.expires_in_days,
        )
        payload = (json.dumps(credential, sort_keys=True) + "\n").encode()
        descriptor = os.open(
            args.output,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
        print(
            json.dumps(
                {key: value for key, value in credential.items() if key != "token"},
                sort_keys=True,
            )
        )
    elif args.command == "admin-token-revoke":
        print(
            json.dumps(
                {
                    "revoked": ControlPlane(
                        store, SecretBox.from_env(), {}
                    ).revoke_admin_token(args.name)
                }
            )
        )
    elif args.command == "source-profile-set":
        print(
            json.dumps(
                store.set_source_profile(
                    {
                        "source_id": args.source_id,
                        "family": args.family,
                        "quality": args.quality,
                        "freshness_half_life_days": args.freshness_half_life_days,
                    }
                ),
                sort_keys=True,
            )
        )
    elif args.command == "source-alias-set":
        print(
            json.dumps(
                store.set_source_alias(args.alias, args.source_id), sort_keys=True
            )
        )
    elif args.command == "federation-scoreboard":
        print(json.dumps(store.federation_scoreboard(), sort_keys=True))
    else:
        if args.require_auth:
            os.environ["RECALL_AUTH_REQUIRED"] = "1"
        if args.unix_socket:
            serve_unix(args.dsn, args.unix_socket)
        else:
            serve(args.dsn, args.host, args.port)


if __name__ == "__main__":
    main()
