#!/usr/bin/env python3
"""PostgreSQL E2E for the H3-b turbopuffer search plane writer.

ingest (two months) -> seed -> drain: the fake namespace holds exactly the
live passages; append -> only the new passages are re-upserted (incremental
read above the shard watermark); forget -> tombstones become deletes; a
second drain writes nothing. The CLI is exercised through RECALL_TPUF_FAKE=1.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.parquet_scan import CanonicalParquetScanProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.projection_worker import run_projection_worker  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402
from recall_server.search_outbox import seed_search_outbox  # noqa: E402
from recall_server.turbopuffer_plane import (  # noqa: E402
    EMBED_TEXT_ATTRIBUTE,
    TEXT_ATTRIBUTE,
    TurbopufferSettings,
    namespace_schema,
)
from recall_server.turbopuffer_projection import drain_search_outbox  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402

JULY, AUGUST = date(2026, 7, 1), date(2026, 8, 1)
SETTINGS = TurbopufferSettings(api_key="synthetic-e2e-key", write_batch_rows=2)


def outbox_rows(store, tenant: str) -> dict[date, dict]:
    with store.connect() as connection:
        return {
            row["month"]: dict(row)
            for row in connection.execute(
                """SELECT month,generation,reason FROM search_projection_outbox
                    WHERE tenant_id=%s ORDER BY month""",
                (tenant,),
            ).fetchall()
        }


def shard_rows(store, tenant: str) -> dict[date, dict]:
    with store.connect() as connection:
        return {
            row["month"]: dict(row)
            for row in connection.execute(
                """SELECT month,generation,dataset_uri,row_count,built_at
                     FROM search_projection_shards
                    WHERE tenant_id=%s ORDER BY month""",
                (tenant,),
            ).fetchall()
        }


def tombstone_ids(store, tenant: str) -> set[str]:
    with store.connect() as connection:
        return {
            row["passage_id"]
            for row in connection.execute(
                "SELECT passage_id FROM search_projection_tombstones WHERE tenant_id=%s",
                (tenant,),
            ).fetchall()
        }


def live_passages(store, tenant: str) -> dict[str, dict]:
    with store.connect() as connection:
        return {
            row["passage_id"]: dict(row)
            for row in connection.execute(
                """SELECT passage_id,text_redacted,header_redacted,first_occurred_at,
                          created_at
                     FROM canonical_passages passage
                    WHERE tenant_id=%s
                      AND NOT EXISTS (
                          SELECT 1 FROM unnest(passage.receipts) AS passage_receipt(receipt)
                            LEFT JOIN canonical_chunks live_chunk
                              ON live_chunk.tenant_id=passage.tenant_id
                             AND live_chunk.source_id=passage.source_id
                             AND live_chunk.receipt=passage_receipt.receipt
                             AND live_chunk.deleted_at IS NULL
                           WHERE live_chunk.receipt IS NULL)""",
                (tenant,),
            ).fetchall()
        }


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(dsn)
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:search-plane:{nonce}"
    principal = f"principal:search-plane:{nonce}"
    source = f"source:search-plane:{nonce}"
    parent = f"session-search-plane-{nonce}"
    namespace_name = SETTINGS.namespace(tenant)
    client = FakeTurbopuffer()

    with tempfile.TemporaryDirectory(prefix="recall-search-plane-") as temporary:
        archive = FilesystemArchiveStore(
            Path(temporary) / "archive", namespace_key=b"p" * 32,
        )
        gateway = CanonicalArchiveGateway(
            store, archive, tenant_id=tenant, principal_id=principal,
        )
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive,
        )
        plane = CanonicalPlane(store, archive, evidence_projector=logical)
        passages = CanonicalPassageProjector(
            store,
            logical_store,
            policy=PassagePolicy(target_tokens=4, overlap_tokens=1),
            bound_tenant_id=tenant,
        )

        def ingest(native: str, text: str, role: str, occurred_at: str) -> dict:
            payload = json.dumps(
                {"native_id": native, "content": {"text": text}},
                sort_keys=True, separators=(",", ":"),
            ).encode()
            artifact = gateway.put_raw(
                tenant_id=tenant, source_id=source, native_id=native,
                payload=payload, media_type="application/json",
                created_at=occurred_at,
            )
            content = {"text": text, "role": role}
            envelope = {
                "schema_version": 1,
                "source_id": source,
                "native_id": native,
                "native_parent_id": parent,
                "kind": "connector_record",
                "occurred_at": occurred_at,
                "observed_at": occurred_at,
                "principal_id": principal,
                "visibility": "private",
                "content_type": "application/json",
                "content": content,
                "provenance": {
                    "connector_id": "synthetic.search-plane",
                    "connector_schema_version": 1,
                    "artifact_ref": artifact,
                },
                "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
            }
            return plane.ingest_document(
                tenant_id=tenant, principal_id=principal,
                connector_id="synthetic.search-plane", artifact_ref=artifact,
                envelope=envelope, text_redacted=text,
            )

        def project() -> dict:
            logical.seed_backfill(tenant_id=tenant)
            built = logical.project_pending(
                tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1,
            )
            assert built["documents"] == 1, built
            projected = passages.project_pending(
                tenant_id=tenant, batch_size=10, max_batches=1, concurrency=1,
            )
            assert projected["documents"] == 1, projected
            return projected

        def drain(max_months: int = 4) -> dict:
            return drain_search_outbox(
                store, SETTINGS, tenant_id=tenant, max_months=max_months, client=client,
            )

        # 1. ingest two months, project, seed a backfill, drain.
        first = ingest(
            f"{parent}:july", "the gateway kept every tenant boundary intact",
            "user", "2026-07-31T23:50:00Z",
        )
        ingest(
            f"{parent}:august", "and the reviewer confirmed the isolation held",
            "assistant", "2026-08-01T00:10:00Z",
        )
        project()
        scan = CanonicalParquetScanProjector(store, logical_store)
        assert scan.seed_backfill(tenant_id=tenant) == 2
        assert scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)["shards"] == 2
        with store.connect() as connection:
            with connection.transaction():
                assert seed_search_outbox(connection, tenant_id=tenant) == 2
        queued = outbox_rows(store, tenant)
        assert {row["reason"] for row in queued.values()} == {"backfill"}, queued
        live_before = live_passages(store, tenant)
        assert len(live_before) >= 2, live_before

        drained = drain()
        assert drained["status"] == "complete", drained
        assert drained["months"] == 2 and drained["failed"] == 0, drained
        assert drained["rows"] == len(live_before), (drained, len(live_before))
        assert drained["deleted"] == 0 and drained["pending"] == 0, drained
        namespace = client.namespace(namespace_name)
        assert set(namespace.rows) == set(live_before), (set(namespace.rows) ^ set(live_before))
        assert namespace.schema == namespace_schema(SETTINGS)
        assert namespace.distance_metric == "cosine_distance"
        for passage_id, row in namespace.rows.items():
            catalog = live_before[passage_id]
            assert row[TEXT_ATTRIBUTE] == catalog["text_redacted"]
            assert row["header"] == (catalog["header_redacted"] or "")
            assert row[EMBED_TEXT_ATTRIBUTE].endswith(catalog["text_redacted"])
            assert row["month"] == catalog["first_occurred_at"].strftime("%Y-%m"), row["month"]
            assert row["native_parent_id"] == parent
            assert row["source_id"] == source
            assert row["actor_ids"] == sorted(row["actor_ids"])
        assert max(len(write["upserts"]) for write in namespace.writes) <= SETTINGS.write_batch_rows
        assert outbox_rows(store, tenant) == {}
        shards = shard_rows(store, tenant)
        assert set(shards) == {JULY, AUGUST}, shards
        assert sum(row["row_count"] for row in shards.values()) == len(live_before), shards
        assert all(
            row["dataset_uri"] == f"turbopuffer://{SETTINGS.region}/{namespace_name}"
            for row in shards.values()
        ), shards
        assert all(row["generation"] == queued[month]["generation"] for month, row in shards.items())
        watermarks = {month: row["built_at"] for month, row in shards.items()}
        assert all(row["created_at"] <= watermarks[row["first_occurred_at"].date().replace(day=1)]
                   for row in live_before.values())

        # 2. a second drain with nothing queued writes nothing.
        writes_before = len(namespace.writes)
        idle = drain()
        assert idle == {
            "status": "complete", "months": 0, "rows": 0, "deleted": 0,
            "failed": 0, "requeued": 0, "pending": 0,
        }, idle
        assert len(namespace.writes) == writes_before

        # 3. append: only the passages created after the August watermark are
        # re-upserted; the ids the differential commit deleted are removed.
        ingest(
            f"{parent}:august-2", "then we shipped the fix before the retro",
            "assistant", "2026-08-15T09:00:00Z",
        )
        appended = project()
        assert appended["inserted"] >= 1, appended
        live_after_append = live_passages(store, tenant)
        new_ids = set(live_after_append) - set(live_before)
        gone_ids = set(live_before) - set(live_after_append)
        assert new_ids, "the append produced no new passages"
        assert tombstone_ids(store, tenant) == gone_ids
        requeued = outbox_rows(store, tenant)
        assert requeued and all(row["reason"] == "logical-update" for row in requeued.values()), requeued
        writes_before = len(namespace.writes)
        incremental = drain()
        assert incremental["status"] == "complete", incremental
        assert incremental["deleted"] == len(gone_ids), (incremental, gone_ids)
        upserted = {
            passage_id
            for write in namespace.writes[writes_before:]
            for passage_id in write["upserts"]
        }
        assert upserted == new_ids, (upserted ^ new_ids)
        assert incremental["rows"] == len(new_ids), incremental
        assert set(namespace.rows) == set(live_after_append), (
            set(namespace.rows) ^ set(live_after_append)
        )
        assert tombstone_ids(store, tenant) == set()
        assert outbox_rows(store, tenant) == {}

        # 4. forget: every passage of the group is tombstoned; the drain
        # deletes them from the namespace and nothing is upserted.
        forgotten = plane.forget({
            "contract": "recall.forget-request.v1",
            "schema_version": 1,
            "tenant_id": tenant,
            "principal_id": principal,
            "source_id": source,
            "target_receipt": first["receipt"],
            "mode": "explicit_forget",
            "reason": "owner_requested",
            "requested_at": "2026-08-20T12:05:00Z",
            "idempotency_key": "forget-search-plane-" + nonce,
        })
        assert forgotten["status"] == "deleted", forgotten
        assert live_passages(store, tenant) == {}
        assert tombstone_ids(store, tenant) == set(live_after_append)
        assert {row["reason"] for row in outbox_rows(store, tenant).values()} == {"forget"}
        writes_before = len(namespace.writes)
        forget_drain = drain()
        assert forget_drain["status"] == "complete", forget_drain
        assert forget_drain["rows"] == 0, forget_drain
        assert forget_drain["deleted"] == len(live_after_append), forget_drain
        assert namespace.rows == {}, namespace.rows
        assert all(not write["upserts"] for write in namespace.writes[writes_before:])
        assert tombstone_ids(store, tenant) == set()
        assert outbox_rows(store, tenant) == {}
        assert set(shard_rows(store, tenant)) == {JULY, AUGUST}

        # 5. the CLI drains the same outbox through the in-process fake; with
        # nothing queued it reports an empty cycle, and a re-seed is drained
        # to zero pending (the fake is per process, so only counts are visible).
        env = {
            **os.environ,
            "RECALL_DATABASE_URL": dsn,
            "RECALL_TPUF_API_KEY": "synthetic-e2e-key",
            "RECALL_TPUF_FAKE": "1",
            "PYTHONPATH": os.pathsep.join([str(ROOT), str(SERVER)]),
        }

        def cli(*extra: str) -> dict:
            completed = subprocess.run(
                [
                    sys.executable, "-m", "recall_server.cli",
                    "search-plane-project", "--tenant", tenant, *extra,
                ],
                cwd=str(SERVER), env=env, capture_output=True, text=True, check=True,
            )
            assert "synthetic-e2e-key" not in completed.stdout + completed.stderr
            return json.loads(completed.stdout.strip().splitlines()[-1])

        empty = cli("--once")
        assert empty["months"] == 0 and empty["pending"] == 0, empty
        with store.connect() as connection:
            with connection.transaction():
                assert seed_search_outbox(connection, tenant_id=tenant) == 2
        reseeded = cli("--max-months", "1")
        assert reseeded["cycles"] == 2 and reseeded["months"] == 2, reseeded
        assert reseeded["rows"] == 0 and reseeded["pending"] == 0, reseeded
        assert reseeded["status"] == "complete", reseeded
        assert outbox_rows(store, tenant) == {}

        # 6. the projection worker runs the phase after parquet and reports
        # it in its cycle result; without a search plane the phase is skipped.
        with store.connect() as connection:
            with connection.transaction():
                assert seed_search_outbox(connection, tenant_id=tenant) == 2
        worker_kwargs = dict(
            tenant_id=tenant, logical_batch_size=10, passage_batch_size=10,
            embedding_batch_size=8, max_batches_per_cycle=1, upload_concurrency=1,
            passage_concurrency=1, interval_seconds=1, once=True,
            skip_embedding=True, sleep=lambda _seconds: None,
        )
        worker_result = run_projection_worker(
            logical, passages, scan, search_plane=lambda: drain(2), **worker_kwargs,
        )
        assert worker_result["search_plane_months"] == 2, worker_result
        assert worker_result["search_plane_rows"] == 0, worker_result
        assert worker_result["search_plane_failed"] == 0, worker_result
        assert worker_result["search_outbox_pending"] == 0, worker_result
        assert outbox_rows(store, tenant) == {}
        skipped = run_projection_worker(logical, passages, scan, **worker_kwargs)
        assert skipped["search_plane_months"] == 0 and skipped["search_plane_elapsed_ms"] == 0, skipped

        # 7. metrics: outbox depth and built source-months are exported.
        metrics = store.service_metrics()
        assert metrics["search_plane_shards"] >= 2, metrics
        assert metrics["search_plane_pending"] >= 0, metrics

    print(json.dumps({
        "status": "ok",
        "passages_backfilled": drained["rows"],
        "passages_appended": incremental["rows"],
        "passages_deleted_on_append": incremental["deleted"],
        "passages_deleted_on_forget": forget_drain["deleted"],
        "cli_reseed": reseeded,
        "worker_phase": {
            key: worker_result[key]
            for key in ("search_plane_months", "search_plane_rows", "search_plane_failed")
        },
    }, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
