#!/usr/bin/env python3
"""Legacy coding sources gain actor scope without attributing shared sources."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path[:0] = [str(RECALL), str(SERVER)]

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.deep_inspection import DeepInspectionError  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.parquet_scan import CanonicalParquetScanProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:company:legacy-actors-{nonce}"
    owner = f"principal:legacy-owner-{nonce}"
    store.provision_brain(
        organization_id=f"org:company:legacy-actors-{nonce}",
        organization_kind="company",
        display_name="Legacy Actor Company",
        tenant_id=tenant,
        brain_kind="company",
        slug=f"legacy-actors-{nonce}",
        owner_principal_id=owner,
    )
    coding_source = f"codex:linux:legacy-{nonce}"
    communications_source = f"slack:company:legacy-{nonce}"
    with store.connect() as connection:
        with connection.transaction():
            for source_id, family in (
                (coding_source, "coding_history"),
                (communications_source, "communications"),
            ):
                CanonicalPlane.register_source(
                    connection,
                    tenant_id=tenant,
                    principal_id=owner,
                    source_id=source_id,
                )
                connection.execute(
                    """INSERT INTO sources(id,principal_id)
                       VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                    (source_id, owner),
                )
                connection.execute(
                    """INSERT INTO source_profiles(
                           source_id,family,quality,freshness_half_life_days
                       ) VALUES (%s,%s,'trusted',30)""",
                    (source_id, family),
                )

    occurred_at = "2026-08-05T12:00:00Z"
    content = {
        "type": "event_msg",
        "payload": {
            "type": "user_message",
            "message": "legacy actor decision marker",
        },
    }
    with tempfile.TemporaryDirectory(prefix="recall-legacy-actor-") as directory:
        archive = FilesystemArchiveStore(
            Path(directory) / "archive",
            namespace_key=b"a" * 32,
        )
        gateway = CanonicalArchiveGateway(
            store,
            archive,
            tenant_id=tenant,
            principal_id=owner,
        )
        raw = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
        artifact = gateway.put_raw(
            tenant_id=tenant,
            source_id=coding_source,
            native_id=f"legacy-event-{nonce}",
            payload=raw,
            media_type="application/json",
            created_at=occurred_at,
        )
        plane = CanonicalPlane(store, archive)
        result = plane.ingest_document(
            tenant_id=tenant,
            principal_id=owner,
            connector_id="local.codex",
            artifact_ref=artifact,
            envelope={
                "schema_version": 1,
                "source_id": coding_source,
                "native_id": f"legacy-event-{nonce}",
                "native_parent_id": f"legacy-session-{nonce}",
                "kind": "connector_record",
                "occurred_at": occurred_at,
                "observed_at": occurred_at,
                "principal_id": owner,
                "visibility": "shared",
                "content_type": "application/json",
                "content": content,
                "provenance": {
                    "connector_id": "local.codex",
                    "connector_schema_version": 1,
                    "harness": "codex",
                    "artifact_ref": artifact,
                },
                "content_sha256": hashlib.sha256(
                    canonical_json(content)
                ).hexdigest(),
            },
            text_redacted="legacy actor decision marker",
        )
        assert result["inserted"] == 1
        projector = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(archive),
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        before = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=2,
        )
        assert before["documents"] == 1
        with store.connect() as connection:
            assert connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_evidence_document_actors
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, coding_source),
            ).fetchone()["count"] == 0

        shared_content = {
            "type": "message",
            "text": "shared communication seed marker",
        }
        shared_raw = json.dumps(
            shared_content,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        shared_artifact = gateway.put_raw(
            tenant_id=tenant,
            source_id=communications_source,
            native_id=f"shared-event-{nonce}",
            payload=shared_raw,
            media_type="application/json",
            created_at=occurred_at,
        )
        shared_result = plane.ingest_document(
            tenant_id=tenant,
            principal_id=owner,
            connector_id="remote.slack",
            artifact_ref=shared_artifact,
            envelope={
                "schema_version": 1,
                "source_id": communications_source,
                "native_id": f"shared-event-{nonce}",
                "native_parent_id": f"shared-thread-{nonce}",
                "kind": "connector_record",
                "occurred_at": occurred_at,
                "observed_at": occurred_at,
                "principal_id": owner,
                "visibility": "shared",
                "content_type": "application/json",
                "content": shared_content,
                "provenance": {
                    "connector_id": "remote.slack",
                    "connector_schema_version": 1,
                    "artifact_ref": shared_artifact,
                },
                "content_sha256": hashlib.sha256(
                    canonical_json(shared_content)
                ).hexdigest(),
            },
            text_redacted="shared communication seed marker",
        )
        assert shared_result["inserted"] == 1
        shared_projection = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=2,
        )
        assert shared_projection["documents"] == 1

        # Emulate applying migration 050 to a populated production database.
        # Migration 049 queues the coding document for attribution repair; 050
        # must seed the stable shared document and leave the dirty one alone.
        with store.connect() as connection:
            connection.execute(
                "DELETE FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                (tenant,),
            )
            connection.execute(
                "DELETE FROM schema_migrations WHERE version=50"
            )

        # Re-running migrations emulates deploying migration 049 over an
        # already-populated service. Every migration is intentionally idempotent.
        store.migrate()
        with store.connect() as connection:
            coding_bindings = connection.execute(
                """SELECT actor_id,relation
                     FROM canonical_source_actor_bindings
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, coding_source),
            ).fetchall()
            shared_bindings = connection.execute(
                """SELECT actor_id,relation
                     FROM canonical_source_actor_bindings
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, communications_source),
            ).fetchall()
            seeded_scan_sources = connection.execute(
                """SELECT source_id
                     FROM canonical_parquet_scan_queue
                    WHERE tenant_id=%s
                    ORDER BY source_id""",
                (tenant,),
            ).fetchall()
        assert len(coding_bindings) == 1
        assert coding_bindings[0]["relation"] == "contributor"
        assert shared_bindings == []
        assert [row["source_id"] for row in seeded_scan_sources] == [
            communications_source
        ]

        repaired = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=2,
        )
        assert repaired["documents"] == 1
        bound = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id=owner,
            authorized_sources=(coding_source, communications_source),
        )
        scoped = bound.scope_documents(
            filters={"person": owner, "source_family": "coding_history"},
            limit=10,
        )
        assert scoped["complete"] is True
        assert len(scoped["documents"]) == 1
        assert scoped["documents"][0]["source_id"] == coding_source

        passages = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=tenant,
        )
        passage_projection = passages.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=2,
            concurrency=2,
        )
        assert passage_projection["documents"] == 2

        scan = CanonicalParquetScanProjector(
            store,
            LogicalEvidenceProjectionStore(archive),
        )
        projected_scan = scan.project_pending(
            tenant_id=tenant,
            batch_size=4,
            max_batches=2,
        )
        assert projected_scan["shards"] == 2
        with store.connect() as connection:
            shards = connection.execute(
                """SELECT * FROM canonical_parquet_scan_shards
                    WHERE tenant_id=%s AND source_id=%s
                    ORDER BY dataset""",
                (tenant, coding_source),
            ).fetchall()
        assert [row["dataset"] for row in shards] == [
            "actors", "documents", "passages", "records"
        ]
        import pyarrow as pa
        import pyarrow.parquet as pq

        tables = {}
        for row in shards:
            reference = {
                "contract": "recall.artifact-ref.v1",
                "schema_version": 1,
                "tenant_id": row["tenant_id"],
                "source_id": row["source_id"],
                **{
                    key: row[key]
                    for key in (
                        "artifact_id", "storage_backend", "object_key",
                        "content_sha256", "size_bytes", "media_type",
                        "encryption", "version_id", "created_at",
                    )
                },
            }
            reference["created_at"] = reference["created_at"].isoformat()
            tables[row["dataset"]] = pq.read_table(
                pa.BufferReader(archive.read_raw(reference))
            ).to_pylist()
        assert len(tables["documents"]) == 1
        assert len(tables["passages"]) == 1
        assert len(tables["records"]) == 1
        assert tables["passages"][0]["text"] == (
            "legacy actor decision marker"
        )
        assert tables["passages"][0]["actor_names"] == [owner]
        assert tables["passages"][0]["receipts"]
        assert tables["records"][0]["search_text"] == (
            "legacy actor decision marker"
        )
        assert tables["documents"][0]["actor_names"] == [owner]
        assert {
            (row["display_name"], row["relation"])
            for row in tables["actors"]
        } == {(owner, "contributor")}

        # A content-identical rebuild retains the immutable objects and does
        # not enqueue an active Parquet artifact for cleanup.
        original_objects = {
            row["dataset"]: row["object_key"] for row in shards
        }
        assert scan.seed_backfill(tenant_id=tenant) == 2
        rebuilt_scan = scan.project_pending(
            tenant_id=tenant,
            batch_size=4,
            max_batches=2,
        )
        assert rebuilt_scan["shards"] == 2
        with store.connect() as connection:
            rebuilt_objects = {
                row["dataset"]: row["object_key"]
                for row in connection.execute(
                    """SELECT dataset,object_key
                         FROM canonical_parquet_scan_shards
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, coding_source),
                ).fetchall()
            }
            parquet_cleanup = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_evidence_cleanup_queue
                    WHERE tenant_id=%s AND source_id=%s
                      AND media_type='application/vnd.apache.parquet'""",
                (tenant, coding_source),
            ).fetchone()["count"]
        assert rebuilt_objects == original_objects
        assert parquet_cleanup == 0

        class ScanInspector:
            def __init__(self, stdout: str):
                self.stdout = stdout
                self.calls = []

            def execute_scan(self, **values):
                self.calls.append(values)
                return {
                    "provider": "synthetic-archil",
                    "stdout": self.stdout,
                    "stderr": "",
                    "exit_code": 0,
                    "complete": True,
                    "stopped_reason": "completed",
                    "output_truncated": False,
                    "timing": {"totalMs": 1, "queueMs": 0, "executeMs": 1},
                }

        inspector = ScanInspector(tables["records"][0]["record_json"] + "\n")
        from recall_server.deep_inspection import agent_evidence_receipts

        assert agent_evidence_receipts(inspector.stdout)
        scan_bound = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id=owner,
            authorized_sources=(coding_source, communications_source),
            deep_inspector=inspector,
        )
        scanned = scan_bound.execute_parquet_scan(
            "duckdb -json -c 'select record_json from "
            "read_parquet(\"/datasets/*/*/records-part-*.parquet\")'",
            filters={
                "person": owner,
                "source_family": "coding_history",
                "since": "2026-08-05T00:00:00Z",
                "until": "2026-08-06T00:00:00Z",
            },
            timeout_seconds=20,
        )
        assert len(scanned["opened_receipts"]) == 1
        assert scanned["sources_available"] == 1
        assert scanned["datasets_available"] == 1
        assert all(
            alias.startswith("s1/2026-08/")
            for alias in inspector.calls[0]["dataset_aliases"].values()
        )

        inspector.stdout = json.dumps({
            **json.loads(tables["records"][0]["record_json"]),
            "receipts": [
                "recall://source:outside/not-authorized?rev=1#item=0"
            ],
        })
        try:
            scan_bound.execute_parquet_scan(
                "duckdb -json -c 'select 1'",
                filters={"source_family": "coding_history"},
                timeout_seconds=20,
            )
        except DeepInspectionError as error:
            assert error.code == "deep_inspector_receipt_scope_violation"
        else:
            raise AssertionError("unauthorized Parquet receipt was accepted")

        # Receipt authority is checked against current event/source actor
        # attribution, not a stale document-level person hint.
        inspector.stdout = tables["records"][0]["record_json"]
        with store.connect() as connection:
            connection.execute(
                """DELETE FROM canonical_source_actor_bindings
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, coding_source),
            )
        try:
            scan_bound.execute_parquet_scan(
                "duckdb -noheader -list -c 'select record_json'",
                filters={
                    "person": owner,
                    "source_family": "coding_history",
                },
                timeout_seconds=20,
            )
        except DeepInspectionError as error:
            assert error.code == "deep_inspector_receipt_scope_violation"
        else:
            raise AssertionError("stale document actor authorized a receipt")
        with store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_source_actor_bindings(
                       tenant_id,source_id,actor_id,relation
                   ) VALUES (%s,%s,%s,'contributor')""",
                (tenant, coding_source, coding_bindings[0]["actor_id"]),
            )

        # A second repair run is a no-op: no duplicate binding or requeue.
        store.migrate()
        with store.connect() as connection:
            assert connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_source_actor_bindings
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, coding_source),
            ).fetchone()["count"] == 1
            assert connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_parquet_scan_queue
                    WHERE tenant_id=%s""",
                (tenant,),
            ).fetchone()["count"] == 0

    store.close()
    print(json.dumps({
        "status": "pass",
        "legacy_coding_bindings": 1,
        "shared_source_false_attributions": 0,
        "person_scoped_documents": 1,
        "parquet_documents": 1,
        "parquet_passages": 1,
        "parquet_records": 1,
        "parquet_actor_leaks": 0,
        "parquet_opened_receipts": 1,
        "parquet_unauthorized_receipts": 0,
        "parquet_stale_actor_receipts": 0,
        "parquet_rebuild_reused_objects": 4,
        "parquet_upgrade_seeded_stable_sources": 1,
        "parquet_upgrade_skipped_dirty_sources": 1,
        "idempotent": True,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
