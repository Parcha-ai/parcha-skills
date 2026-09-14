#!/usr/bin/env python3
"""PostgreSQL E2E for the H3-a search projection outbox.

project (two months) -> append (same month) -> header fill -> forget -> seed:
every passage-plane write leaves the outbox, tombstone and shard-catalog rows
the Lance writer (H3-b) needs, in the transaction that made the change.
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
from recall_server.projectors import canonical_json  # noqa: E402
from recall_server.search_outbox import (  # noqa: E402
    search_outbox_pending,
    seed_search_outbox,
)

JULY, AUGUST = date(2026, 7, 1), date(2026, 8, 1)


def outbox_rows(store, tenant: str, source: str) -> dict[date, dict]:
    with store.connect() as connection:
        return {
            row["month"]: dict(row)
            for row in connection.execute(
                """SELECT month,generation,reason,queued_at,first_queued_at
                     FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s
                    ORDER BY month""",
                (tenant, source),
            ).fetchall()
        }


def tombstone_rows(store, tenant: str, source: str) -> dict[str, dict]:
    with store.connect() as connection:
        return {
            row["passage_id"]: dict(row)
            for row in connection.execute(
                """SELECT passage_id,month,deleted_at
                     FROM search_projection_tombstones
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchall()
        }


def passage_rows(store, tenant: str, source: str) -> dict[str, dict]:
    with store.connect() as connection:
        return {
            row["passage_id"]: dict(row)
            for row in connection.execute(
                """SELECT passage_id,first_occurred_at,last_occurred_at,
                          header_redacted
                     FROM canonical_passages
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchall()
        }


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(dsn)
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:search-outbox:{nonce}"
    principal = f"principal:search-outbox:{nonce}"
    source = f"source:search-outbox:{nonce}"
    parent = f"session-search-outbox-{nonce}"

    with tempfile.TemporaryDirectory(prefix="recall-search-outbox-") as temporary:
        archive = FilesystemArchiveStore(
            Path(temporary) / "archive", namespace_key=b"o" * 32,
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
                    "connector_id": "synthetic.outbox",
                    "connector_schema_version": 1,
                    "artifact_ref": artifact,
                },
                "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
            }
            return plane.ingest_document(
                tenant_id=tenant, principal_id=principal,
                connector_id="synthetic.outbox", artifact_ref=artifact,
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

        # 1. project: one logical document whose records straddle July/August.
        first = ingest(
            f"{parent}:july", "the gateway kept every tenant boundary intact",
            "user", "2026-07-31T23:50:00Z",
        )
        ingest(
            f"{parent}:august", "and the reviewer confirmed the isolation held",
            "assistant", "2026-08-01T00:10:00Z",
        )
        projected = project()
        assert projected["inserted"] >= 2, projected
        before_append = passage_rows(store, tenant, source)
        months = {
            row["first_occurred_at"].date().replace(day=1)
            for row in before_append.values()
        } | {
            row["last_occurred_at"].date().replace(day=1)
            for row in before_append.values()
        }
        assert months == {JULY, AUGUST}, months
        queued = outbox_rows(store, tenant, source)
        assert set(queued) == {JULY, AUGUST}, queued
        assert all(row["generation"] == 1 for row in queued.values()), queued
        assert all(row["reason"] == "logical-update" for row in queued.values()), queued
        assert tombstone_rows(store, tenant, source) == {}
        with store.connect() as connection:
            assert search_outbox_pending(connection, tenant_id=tenant) == 2
            assert search_outbox_pending(connection) >= 2

        # A same-revision re-projection changes nothing: no outbox churn.
        with store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                       tenant_id,source_id,logical_document_id,revision,
                       generation,reason,changed_at
                   )
                   SELECT tenant_id,source_id,logical_document_id,revision,
                          1,'backfill',clock_timestamp()
                     FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s
                   ON CONFLICT(tenant_id,source_id,logical_document_id)
                   DO UPDATE SET revision=excluded.revision,
                       generation=canonical_passage_projection_queue.generation+1,
                       reason='backfill',changed_at=clock_timestamp()""",
                (tenant, source),
            )
            connection.commit()
        noop = passages.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=1, concurrency=1,
        )
        assert noop["inserted"] == 0 and noop["deleted"] == 0, noop
        assert outbox_rows(store, tenant, source) == queued

        # 2. append: a third record in August. The differential commit inserts
        # August passages (and may replace the boundary one), so August is
        # queued again with a higher generation; the deleted ids are
        # tombstoned; first_queued_at never moves.
        ingest(
            f"{parent}:august-2", "then we shipped the fix before the retro",
            "assistant", "2026-08-15T09:00:00Z",
        )
        appended = project()
        assert appended["inserted"] >= 1, appended
        after_append = passage_rows(store, tenant, source)
        deleted_ids = set(before_append) - set(after_append)
        requeued = outbox_rows(store, tenant, source)
        assert set(requeued) == {JULY, AUGUST}, requeued
        assert requeued[AUGUST]["generation"] == 2, requeued
        assert requeued[AUGUST]["queued_at"] > queued[AUGUST]["queued_at"]
        assert requeued[AUGUST]["first_queued_at"] == queued[AUGUST]["first_queued_at"]
        assert requeued[AUGUST]["reason"] == "logical-update", requeued
        tombstones = tombstone_rows(store, tenant, source)
        assert set(tombstones) == deleted_ids, (set(tombstones), deleted_ids)
        for passage_id in deleted_ids:
            assert tombstones[passage_id]["month"] == (
                before_append[passage_id]["first_occurred_at"].date().replace(day=1)
            )
        if deleted_ids:
            # A deleted July/August boundary passage queues July too.
            assert requeued[JULY]["generation"] >= 1

        # 3. header change: rows projected before schema 064 get a header;
        # the months of the rows that actually changed are queued once as
        # header-change, and a second pass (nothing missing) queues nothing.
        with store.connect() as connection:
            connection.execute(
                """UPDATE canonical_passages
                      SET header_redacted=NULL,embed_sha256=NULL
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            )
            connection.commit()
        before_headers = outbox_rows(store, tenant, source)
        filled = passages.backfill_headers(tenant_id=tenant, batch_size=100)
        assert filled["status"] == "complete", filled
        assert filled["updated"] == len(after_append), filled
        headed = outbox_rows(store, tenant, source)
        for month in (JULY, AUGUST):
            assert headed[month]["generation"] == before_headers[month]["generation"] + 1, (
                month, headed[month], before_headers[month],
            )
            assert headed[month]["reason"] == "header-change", headed[month]
        again = passages.backfill_headers(tenant_id=tenant, batch_size=100)
        assert again == {"status": "complete", "updated": 0, "batches": 0}, again
        assert outbox_rows(store, tenant, source) == headed
        assert all(
            row["header_redacted"] is not None
            for row in passage_rows(store, tenant, source).values()
        )

        # Build the parquet shard for the seed step while the passages exist.
        scan = CanonicalParquetScanProjector(store, logical_store)
        assert scan.seed_backfill(tenant_id=tenant) == 2
        scanned = scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)
        assert scanned["shards"] == 2, scanned
        with store.connect() as connection:
            shard_months = {
                row["bucket_start"]
                for row in connection.execute(
                    """SELECT DISTINCT bucket_start FROM canonical_parquet_scan_shards
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, source),
                ).fetchall()
            }
        assert shard_months == {JULY, AUGUST}, shard_months

        # 4. forget: every passage of the group is tombstoned in the same
        # transaction that drops its evidence document, and both months are
        # queued as forget.
        live_before_forget = passage_rows(store, tenant, source)
        assert live_before_forget
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
            "idempotency_key": "forget-search-outbox-" + nonce,
        })
        assert forgotten["status"] == "deleted", forgotten
        assert passage_rows(store, tenant, source) == {}
        tombstones = tombstone_rows(store, tenant, source)
        assert set(live_before_forget) <= set(tombstones), (
            set(live_before_forget) - set(tombstones)
        )
        for passage_id in deleted_ids:
            # Earlier tombstones keep their original deleted_at.
            assert tombstones[passage_id]["deleted_at"] < tombstones[
                next(iter(live_before_forget))
            ]["deleted_at"]
        after_forget = outbox_rows(store, tenant, source)
        for month in (JULY, AUGUST):
            assert after_forget[month]["reason"] == "forget", after_forget[month]
            assert after_forget[month]["generation"] == headed[month]["generation"] + 1

        # 5. seed: one backfill row per parquet shard month; sticky; idempotent.
        with store.connect() as connection:
            with connection.transaction():
                seeded = seed_search_outbox(connection, tenant_id=tenant)
        assert seeded == 2, seeded
        backfilled = outbox_rows(store, tenant, source)
        assert {row["reason"] for row in backfilled.values()} == {"backfill"}, backfilled
        for month in (JULY, AUGUST):
            assert backfilled[month]["generation"] == after_forget[month]["generation"] + 1
        with store.connect() as connection:
            with connection.transaction():
                assert seed_search_outbox(connection, tenant_id=tenant) == 0
                assert seed_search_outbox(
                    connection, tenant_id=tenant, source_id=source,
                ) == 0
        assert outbox_rows(store, tenant, source) == backfilled
        cli = subprocess.run(
            [
                sys.executable, "-m", "recall_server.cli",
                "search-outbox-seed", "--tenant", tenant,
            ],
            cwd=str(SERVER),
            env={
                **os.environ,
                "RECALL_DATABASE_URL": dsn,
                "PYTHONPATH": os.pathsep.join([str(ROOT), str(SERVER)]),
            },
            capture_output=True,
            text=True,
            check=True,
        )
        assert json.loads(cli.stdout.strip().splitlines()[-1]) == {
            "pending": 2, "seeded": 0,
        }, cli.stdout
        assert outbox_rows(store, tenant, source) == backfilled

        # The shard catalog exists for the Lance writer and is empty until it runs.
        with store.connect() as connection:
            shards = connection.execute(
                """SELECT count(*) AS count FROM search_projection_shards
                    WHERE tenant_id=%s""",
                (tenant,),
            ).fetchone()["count"]
        assert shards == 0, shards

    print(json.dumps({
        "status": "ok",
        "passages_projected": len(before_append),
        "passages_after_append": len(after_append),
        "tombstones": len(tombstones),
        "outbox_generations": {
            str(month): row["generation"] for month, row in backfilled.items()
        },
    }, sort_keys=True))


if __name__ == "__main__":
    main()
