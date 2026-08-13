#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for Codex archive restoration through Parquet."""

from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import sys
import tempfile
import threading
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path[:0] = [str(RECALL), str(SERVER)]

from client.mac import CanonicalArchiveClient, CanonicalBrainWriter  # noqa: E402
from collector.collector import Collector  # noqa: E402
from recall_server.app import Handler  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalPlane  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.parquet_scan import CanonicalParquetScanProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402


SESSION_ID = "019f1111-2222-7333-8444-555555555555"
OCCURRED_AT = "2026-08-10T12:00:00Z"


def line(value: dict) -> str:
    return json.dumps(value, sort_keys=True) + "\n"


def rollout(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        line({
            "timestamp": OCCURRED_AT,
            "type": "session_meta",
            "payload": {"id": SESSION_ID, "cwd": "/synthetic"},
        })
        + line({
            "timestamp": OCCURRED_AT,
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": marker}],
            },
        })
    )


def reference(row: dict) -> dict:
    result = {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        **{
            key: row[key]
            for key in (
                "artifact_id",
                "storage_backend",
                "object_key",
                "content_sha256",
                "size_bytes",
                "media_type",
                "encryption",
                "version_id",
                "created_at",
            )
        },
    }
    result["created_at"] = result["created_at"].isoformat()
    return result


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:company:archive-restore-{nonce}"
    principal = f"principal:archive-owner-{nonce}"
    source_id = f"codex:linux:archive-restore-{nonce}"
    marker = f"codex-archive-restore-marker-{nonce}"
    credential = store.create_collector_token(
        "codex-archive-restore-" + nonce,
        source_id,
        ["write"],
        tenant_id=tenant,
        principal_id=principal,
    )
    previous = {
        name: os.environ.get(name)
        for name in (
            "RECALL_AUTH_REQUIRED",
            "RECALL_HTTP_PROFILE",
            "RECALL_CANONICAL_INGEST_PUBLIC",
        )
    }
    os.environ.update({
        "RECALL_AUTH_REQUIRED": "1",
        "RECALL_HTTP_PROFILE": "public-mcp",
        "RECALL_CANONICAL_INGEST_PUBLIC": "1",
    })
    logs = io.StringIO()
    handler = logging.StreamHandler(logs)
    logger = logging.getLogger("recall.brainstore")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    server = None
    output: dict[str, object] | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="recall-codex-restore-") as value:
            root = Path(value)
            active = root / "sessions"
            archived = root / "archived_sessions"
            archived.mkdir(parents=True)
            session = active / f"rollout-{SESSION_ID}.jsonl"
            rollout(session, marker)
            spool = root / "spool" / "collector.db"
            archive = FilesystemArchiveStore(
                root / "archive",
                namespace_key=b"r" * 32,
            )
            plane = CanonicalPlane(store, archive)
            Handler.store = store
            Handler.archive_store = archive
            Handler.canonical_plane = plane
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            endpoint = f"http://127.0.0.1:{server.server_port}"
            common = {
                "endpoint": endpoint,
                "token": credential["token"],
                "source_id": source_id,
                "tenant_id": tenant,
                "principal_id": principal,
            }

            def collector(*, archive_root: Path | None) -> Collector:
                return Collector(
                    root=active,
                    archive_root=archive_root,
                    harness="codex",
                    source_id=source_id,
                    spool_path=spool,
                    endpoint=endpoint,
                    token=credential["token"],
                    principal_id=principal,
                    visibility="private",
                    brain_writer=CanonicalBrainWriter(**common),
                    archive=CanonicalArchiveClient(**common),
                    tenant_id=tenant,
                    archive_workers=1,
                    max_scan_records=100_000,
                    max_scan_seconds=60,
                )

            initial = collector(archive_root=archived)
            assert initial.scan()["records_queued"] == 2
            assert initial.flush()["acked"] == 2
            native_ids = {
                row["native_id"]
                for row in initial.db.execute(
                    "SELECT native_id FROM active_records"
                )
            }
            initial.close()

            archived_session = archived / session.name
            session.replace(archived_session)
            old_single_root = collector(archive_root=None)
            assert old_single_root.scan()["tombstones_queued"] == 2
            assert old_single_root.flush()["acked"] == 2
            old_single_root.close()
            assert plane.source_status(
                tenant_id=tenant,
                principal_id=principal,
                source_id=source_id,
            )["live_events"] == 0

            restored = collector(archive_root=archived)
            restoration = restored.scan()
            assert restoration["restored_records_queued"] == 2
            assert restoration["tombstones_queued"] == 0
            assert restored.doctor()["archive_backlog"] == 1
            assert restored.flush()["acked"] == 2
            assert restored.doctor()["archive_backlog"] == 0
            assert restored.scan()["records_queued"] == 0
            assert restored.flush()["acked"] == 0
            with sqlite3.connect(spool) as local:
                assert local.execute(
                    "SELECT count(*) FROM outbox WHERE state='pending'"
                ).fetchone()[0] == 0
            restored.close()

            status = plane.source_status(
                tenant_id=tenant,
                principal_id=principal,
                source_id=source_id,
            )
            assert status["live_events"] == 2
            assert status["current_documents"] == 2
            assert status["tombstoned_identities"] == 0
            with store.connect() as connection:
                lifecycle = connection.execute(
                    """SELECT native_id,count(*) AS revisions,
                              array_agg(revision ORDER BY revision) AS revision_ids,
                              (array_agg(is_tombstone ORDER BY revision DESC))[1]
                                  AS latest_tombstone,
                              (array_agg(
                                  canonical_redacted #>>
                                      '{content,_recall_collector_generation}'
                                  ORDER BY revision DESC
                              ))[1] AS latest_generation
                         FROM canonical_events
                        WHERE tenant_id=%s AND source_id=%s
                        GROUP BY native_id ORDER BY native_id""",
                    (tenant, source_id),
                ).fetchall()
            assert {row["native_id"] for row in lifecycle} == native_ids
            assert all(row["revisions"] == 3 for row in lifecycle)
            assert all(row["revision_ids"] == [1, 2, 3] for row in lifecycle)
            assert all(row["latest_tombstone"] is False for row in lifecycle)
            assert all(row["latest_generation"] == "1" for row in lifecycle)

            logical_store = LogicalEvidenceProjectionStore(archive)
            logical = CanonicalLogicalEvidenceProjector(
                store,
                logical_store,
                bound_tenant_id=tenant,
                raw_archive=archive,
            )
            projected = logical.project_pending(
                tenant_id=tenant,
                batch_size=10,
                max_batches=2,
                upload_concurrency=2,
            )
            assert projected["documents"] == 1
            assert projected["records"] == 2
            scoped = BoundCanonicalRetrieval(
                store,
                tenant_id=tenant,
                principal_id=principal,
                authorized_sources=(source_id,),
            ).scope_documents(limit=10)
            assert scoped["complete"] is True
            assert len(scoped["documents"]) == 1
            assert scoped["documents"][0]["record_count"] == 2

            passages = CanonicalPassageProjector(
                store,
                logical_store,
                policy=DEFAULT_PASSAGE_POLICY,
                bound_tenant_id=tenant,
            )
            passage_result = passages.project_pending(
                tenant_id=tenant,
                batch_size=10,
                max_batches=2,
                concurrency=2,
            )
            assert passage_result["documents"] == 1

            parquet = CanonicalParquetScanProjector(store, logical_store)
            parquet_result = parquet.project_pending(
                tenant_id=tenant,
                batch_size=4,
                max_batches=2,
            )
            assert parquet_result["shards"] == 1
            assert parquet_result["stale"] == 0
            with store.connect() as connection:
                shards = connection.execute(
                    """SELECT * FROM canonical_parquet_scan_shards
                        WHERE tenant_id=%s AND source_id=%s
                        ORDER BY dataset,shard_index""",
                    (tenant, source_id),
                ).fetchall()
                queue_depth = connection.execute(
                    """SELECT count(*) AS count
                         FROM canonical_parquet_scan_queue
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, source_id),
                ).fetchone()["count"]
            assert queue_depth == 0
            assert {row["dataset"] for row in shards} == {
                "actors", "documents", "passages", "records",
            }
            assert len({row["generation_sha256"] for row in shards}) == 1
            tables: dict[str, list[dict]] = {}
            for row in shards:
                table = pq.read_table(
                    pa.BufferReader(archive.read_raw(reference(row)))
                ).to_pylist()
                tables.setdefault(row["dataset"], []).extend(table)
            assert len(tables["documents"]) == 1
            assert len(tables["passages"]) == 1
            assert len(tables["records"]) == 2
            assert tables["actors"] == []
            assert marker in tables["passages"][0]["text"]
            assert tables["passages"][0]["receipts"]
            assert marker in "\n".join(
                row["search_text"] for row in tables["records"]
            )
            assert len({
                json.loads(row["record_json"])["event_native_id"]
                for row in tables["records"]
            }) == 2
            assert logical.project_pending(
                tenant_id=tenant,
                batch_size=10,
                max_batches=1,
            )["documents"] == 0
            assert parquet.project_pending(
                tenant_id=tenant,
                batch_size=4,
                max_batches=1,
            )["shards"] == 0

            output = {
                "status": "pass",
                "restored_native_ids": 2,
                "revisions_per_identity": 3,
                "restoration_duplicates": 0,
                "archive_backlog": 0,
                "logical_documents": 1,
                "logical_records": 2,
                "parquet_generations": 1,
                "parquet_documents": 1,
                "parquet_passages": 1,
                "parquet_records": 2,
                "parquet_queue_depth": 0,
                "content_in_logs": int(marker in logs.getvalue()),
            }
            assert output["content_in_logs"] == 0
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        logger.removeHandler(handler)
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        store.close()
    assert output is not None
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()
