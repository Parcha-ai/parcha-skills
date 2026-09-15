#!/usr/bin/env python3
"""PostgreSQL E2E for H3-e': retiring the Postgres vector plane.

postgres plane: ingest, project, embed (the halfvec table fills as before)
-> flip to the turbopuffer plane with the file-backed fake -> seed + drain ->
``search-plane-status`` reports drift 0 -> ``migrate --retire-postgres-plane``
refuses from a postgres-plane process and applies from a turbopuffer-plane
one (tables, index and generated column gone; ``canonical_chunks.search_vector``
kept) -> search on the turbopuffer plane still answers -> a second migrate is
a no-op -> a store on the postgres plane refuses to start -> the turbopuffer
writers keep projecting without the embeddings table.

Run from ``recall/`` (the ``tests`` package must import). Counts only are
printed; never the key, never passage text.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from e2e_lossless_passages import SyntheticEmbeddingRuntime  # noqa: E402
from recall_server import MANDATORY_SCHEMA_VERSION, RETIRE_POSTGRES_PLANE_VERSION  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore, SearchPlaneSchemaError  # noqa: E402
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
from recall_server.turbopuffer_plane import turbopuffer_settings_from_env  # noqa: E402

FAKE_FACTORY = "tests.central_brain.fake_turbopuffer:factory"
QUERY = "why did the gateway preserve tenant boundaries?"
PLANE_KEYS = (
    "RECALL_SEARCH_PLANE", "RECALL_TPUF_API_KEY", "RECALL_TPUF_CLIENT_FACTORY",
    "RECALL_TPUF_FAKE_STATE", "RECALL_TPUF_WRITE_BATCH_ROWS",
)


def relation_exists(store, name: str) -> bool:
    with store.connect() as connection:
        return bool(
            connection.execute("SELECT to_regclass(%s) AS value", (f"public.{name}",)).fetchone()["value"]
        )


def column_exists(store, table: str, column: str) -> bool:
    with store.connect() as connection:
        return bool(
            connection.execute(
                """SELECT 1 FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s AND column_name=%s""",
                (table, column),
            ).fetchone()
        )


def recorded_versions(store) -> list[int]:
    with store.connect() as connection:
        return [
            int(row["version"])
            for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
        ]


def count(store, sql: str, *params) -> int:
    with store.connect() as connection:
        return int(connection.execute(sql, params).fetchone()["n"])


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    for key in PLANE_KEYS:
        os.environ.pop(key, None)
    runtime = SyntheticEmbeddingRuntime()
    store = BrainStore(dsn, semantic_runtime=runtime)  # type: ignore[arg-type]
    assert store.search_plane == "postgres"
    migrated = store.migrate()
    assert migrated["deferred"] == [RETIRE_POSTGRES_PLANE_VERSION], migrated
    assert migrated["schema_version"] == MANDATORY_SCHEMA_VERSION, migrated
    assert relation_exists(store, "canonical_passage_embeddings")
    assert relation_exists(store, "canonical_embedding_ledger")
    assert column_exists(store, "canonical_passages", "search_vector")

    nonce = uuid.uuid4().hex
    tenant = f"tenant:retire-plane:{nonce}"
    principal = f"principal:retire-plane:{nonce}"
    source = f"source:retire-plane:{nonce}"
    parent = f"session-retire-plane-{nonce}"

    with tempfile.TemporaryDirectory(prefix="recall-retire-plane-") as temporary:
        state_path = Path(temporary) / "fake-plane.json"
        archive = FilesystemArchiveStore(Path(temporary) / "archive", namespace_key=b"p" * 32)
        gateway = CanonicalArchiveGateway(store, archive, tenant_id=tenant, principal_id=principal)
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive,
        )
        plane = CanonicalPlane(store, archive, evidence_projector=logical)
        passage_policy = PassagePolicy(target_tokens=4, overlap_tokens=1)
        passages = CanonicalPassageProjector(
            store, logical_store, policy=passage_policy, bound_tenant_id=tenant,
        )
        scan = CanonicalParquetScanProjector(store, logical_store)

        def ingest(native: str, text: str, role: str, occurred_at: str) -> dict:
            payload = json.dumps(
                {"native_id": native, "content": {"text": text}}, sort_keys=True, separators=(",", ":"),
            ).encode()
            artifact = gateway.put_raw(
                tenant_id=tenant, source_id=source, native_id=native,
                payload=payload, media_type="application/json", created_at=occurred_at,
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
                    "connector_id": "synthetic.retire-plane",
                    "connector_schema_version": 1,
                    "artifact_ref": artifact,
                },
                "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
            }
            return plane.ingest_document(
                tenant_id=tenant, principal_id=principal, connector_id="synthetic.retire-plane",
                artifact_ref=artifact, envelope=envelope, text_redacted=text,
            )

        def project(projector: CanonicalPassageProjector, logical_projector) -> dict:
            logical_projector.seed_backfill(tenant_id=tenant)
            built = logical_projector.project_pending(
                tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1,
            )
            assert built["documents"] == 1, built
            projected = projector.project_pending(
                tenant_id=tenant, batch_size=10, max_batches=1, concurrency=1,
            )
            assert projected["documents"] == 1, projected
            return projected

        # 1. postgres plane, exactly as today: passages project, the
        #    embedding path fills the halfvec table, the ledger exists.
        ingest(f"{parent}:july", "the gateway kept every tenant boundary intact", "user", "2026-07-31T23:50:00Z")
        ingest(f"{parent}:august", "and the reviewer confirmed the isolation held", "assistant", "2026-08-01T00:10:00Z")
        project(passages, logical)
        embedded = passages.embed_pending(tenant_id=tenant, batch_size=8, max_batches=2)
        assert embedded["status"] == "complete" and embedded["processed"] >= 2, embedded
        live_passages = count(
            store,
            """SELECT count(*) AS n FROM canonical_passages passage
                JOIN canonical_passage_documents projected
                  USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)
               WHERE passage.tenant_id=%s""",
            tenant,
        )
        vectors = count(store, "SELECT count(*) AS n FROM canonical_passage_embeddings WHERE tenant_id=%s", tenant)
        assert vectors == live_passages >= 2, (vectors, live_passages)
        assert scan.seed_backfill(tenant_id=tenant) == 2
        assert scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)["shards"] == 2
        document_id = None
        with store.connect() as connection:
            document_id = connection.execute(
                "SELECT logical_document_id FROM canonical_passage_documents WHERE tenant_id=%s", (tenant,),
            ).fetchone()["logical_document_id"]
            with connection.transaction():
                assert seed_search_outbox(connection, tenant_id=tenant) == 2
        store.close()

        # 2. flip to the turbopuffer plane (file-backed fake) and drain.
        os.environ.update({
            "RECALL_SEARCH_PLANE": "turbopuffer",
            "RECALL_TPUF_API_KEY": "synthetic",
            "RECALL_TPUF_CLIENT_FACTORY": FAKE_FACTORY,
            "RECALL_TPUF_FAKE_STATE": str(state_path),
            "RECALL_TPUF_WRITE_BATCH_ROWS": "2",
        })
        settings = turbopuffer_settings_from_env(required=True)
        namespace_name = settings.namespace(tenant)
        base_env = {
            **os.environ,
            "RECALL_DATABASE_URL": dsn,
            "PYTHONPATH": os.pathsep.join([str(ROOT), str(SERVER)]),
        }

        def cli(*argv: str, plane: str = "turbopuffer", check: bool = True) -> tuple[dict, subprocess.CompletedProcess]:
            env = {**base_env, "RECALL_SEARCH_PLANE": plane}
            completed = subprocess.run(
                [sys.executable, "-m", "recall_server.cli", *argv],
                cwd=str(SERVER), env=env, capture_output=True, text=True, check=False,
            )
            assert "synthetic" not in completed.stdout, completed.stdout
            assert "synthetic" not in completed.stderr, completed.stderr
            if check:
                assert completed.returncode == 0, (argv, completed.returncode, completed.stderr[-2000:])
            lines = [
                line for stream in (completed.stdout, completed.stderr)
                for line in stream.strip().splitlines() if line.startswith("{")
            ]
            return (json.loads(lines[-1]) if lines else {}), completed

        drained, _ = cli("search-plane-project", "--tenant", tenant, "--max-months", "4")
        assert drained["status"] == "complete" and drained["pending"] == 0, drained
        assert drained["rows"] == live_passages, (drained, live_passages)
        status, _ = cli("search-plane-status", "--tenant", tenant, "--target-tokens", "4", "--overlap-tokens", "1")
        assert status["status"] == "ok", status
        assert status["passages"] == live_passages and status["rows"] == live_passages, status
        assert status["drift"] == 0 and status["outbox_pending"] == 0 and status["shards"] == 2, status
        assert status["namespace"] == namespace_name, status
        assert "text" not in json.dumps(status).casefold().replace("policy_fingerprint", "")

        # 3. the guard: a postgres-plane process cannot retire the plane.
        refused, completed = cli("migrate", "--retire-postgres-plane", plane="postgres", check=False)
        assert completed.returncode == 2, completed.returncode
        assert refused["status"] == "error", refused
        assert "RECALL_SEARCH_PLANE=turbopuffer" in refused["error"], refused
        assert "067" in refused["error"], refused
        reader = BrainStore(dsn)
        assert reader.search_plane == "turbopuffer"
        assert relation_exists(reader, "canonical_passage_embeddings"), "the refused migrate dropped the table"
        assert RETIRE_POSTGRES_PLANE_VERSION not in recorded_versions(reader)
        # A plain migrate on either plane defers 067.
        plain, _ = cli("migrate", plane="postgres")
        assert plain["deferred"] == [RETIRE_POSTGRES_PLANE_VERSION] and plain["applied"] == [], plain
        plain, _ = cli("migrate")
        assert plain["deferred"] == [RETIRE_POSTGRES_PLANE_VERSION] and plain["applied"] == [], plain

        # 4. retire from the turbopuffer plane.
        retired, _ = cli("migrate", "--retire-postgres-plane")
        assert retired["applied"] == [RETIRE_POSTGRES_PLANE_VERSION], retired
        assert retired["deferred"] == [] and retired["postgres_vector_plane"] == "retired", retired
        assert retired["schema_version"] == RETIRE_POSTGRES_PLANE_VERSION, retired
        assert recorded_versions(reader)[-1] == RETIRE_POSTGRES_PLANE_VERSION
        assert not relation_exists(reader, "canonical_passage_embeddings")
        assert not relation_exists(reader, "canonical_passage_embeddings_hnsw_idx")
        assert not relation_exists(reader, "canonical_embedding_ledger")
        assert not relation_exists(reader, "canonical_passages_search_idx")
        assert not column_exists(reader, "canonical_passages", "search_vector")
        assert column_exists(reader, "canonical_chunks", "search_vector"), "legacy chunk tsvector must stay"
        assert column_exists(reader, "canonical_passage_contexts", "search_vector")
        assert count(reader, "SELECT count(*) AS n FROM canonical_passages WHERE tenant_id=%s", tenant) == live_passages

        # 5. search on the turbopuffer plane still answers from the namespace.
        bound = BoundCanonicalRetrieval(
            reader, tenant_id=tenant, principal_id=principal,
            authorized_sources=(source,), passage_policy=passage_policy,
        )
        found = bound.passage_hints(QUERY, limit=5)
        diagnostics = found["diagnostics"]
        assert diagnostics["search_plane"] == "turbopuffer", diagnostics
        assert diagnostics["dense_status"] == "ok" and diagnostics["passage_lexical_status"] == "ok", diagnostics
        assert document_id in {row["logical_document_id"] for row in found["results"]}, found["results"]

        # 6. idempotent: a second retirement run and a plain run change nothing.
        again, _ = cli("migrate", "--retire-postgres-plane")
        assert again["applied"] == [] and again["deferred"] == [], again
        assert again["skipped"] == RETIRE_POSTGRES_PLANE_VERSION, again
        again, _ = cli("migrate")
        assert again["applied"] == [] and again["postgres_vector_plane"] == "retired", again
        assert recorded_versions(reader) == list(range(1, RETIRE_POSTGRES_PLANE_VERSION + 1))

        # 7. a postgres-plane store refuses to start against the retired schema.
        os.environ["RECALL_SEARCH_PLANE"] = "postgres"
        try:
            stale = BrainStore(dsn)
        finally:
            os.environ["RECALL_SEARCH_PLANE"] = "turbopuffer"
        assert stale.search_plane == "postgres"
        try:
            with stale.connect():
                pass
        except SearchPlaneSchemaError as error:
            refusal = str(error)
        else:
            raise AssertionError("a postgres-plane store started against the retired schema")
        assert "RECALL_SEARCH_PLANE=turbopuffer" in refusal and "067" in refusal, refusal
        stale.close()
        refused_cli, completed = cli("migrate", plane="postgres", check=False)
        assert completed.returncode != 0, "cli migrate started on the postgres plane after 067"
        assert "067" in completed.stderr, completed.stderr[-500:]
        refused_worker, completed = cli("embedding-worker", "--tenant", tenant, "--once", check=False)
        assert completed.returncode == 2, completed.returncode
        assert refused_worker["status"] == "not-applicable", refused_worker
        assert "suspend this service" in refused_worker["error"], refused_worker

        # 8. the turbopuffer writers keep working without the embeddings
        #    table: a new record projects, the worker skips embedding, the
        #    gauges and the source status read 0 for the retired plane.
        writer = BrainStore(dsn, semantic_runtime=runtime)  # type: ignore[arg-type]
        assert writer.search_plane == "turbopuffer"
        gateway_after = CanonicalArchiveGateway(writer, archive, tenant_id=tenant, principal_id=principal)
        logical_after = CanonicalLogicalEvidenceProjector(
            writer, logical_store, bound_tenant_id=tenant, raw_archive=archive,
        )
        plane_after = CanonicalPlane(writer, archive, evidence_projector=logical_after)
        passages_after = CanonicalPassageProjector(
            writer, logical_store, policy=passage_policy, bound_tenant_id=tenant,
        )
        gateway, plane = gateway_after, plane_after
        ingest(f"{parent}:august-2", "then we shipped the fix before the retro", "assistant", "2026-08-15T09:00:00Z")
        appended = project(passages_after, logical_after)
        assert appended["inserted"] >= 1, appended
        not_applicable = passages_after.embed_pending(tenant_id=tenant, batch_size=8, max_batches=1)
        assert not_applicable == {"status": "not-applicable", "processed": 0, "batches": 0, "plane": "turbopuffer"}, not_applicable
        assert passages_after.contract_coverage(tenant_id=tenant)["status"] == "not-applicable"
        worker = run_projection_worker(
            logical_after, passages_after, CanonicalParquetScanProjector(writer, logical_store),
            tenant_id=tenant, logical_batch_size=10, passage_batch_size=10, embedding_batch_size=8,
            max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
            interval_seconds=1, once=True, sleep=lambda _seconds: None,
        )
        assert worker["embedded"] == 0 and worker["embed_elapsed_ms"] == 0, worker
        metrics = writer.service_metrics()
        assert metrics["passages_unembedded"] == 0 and metrics["embedding_daily_total"] == 0, metrics
        source_status = plane_after.source_status(tenant_id=tenant, principal_id=principal, source_id=source)
        assert source_status["passage_embeddings"] == 0 and source_status["missing_passage_embeddings"] == 0, source_status
        assert source_status["passages"] >= live_passages, source_status
        drained_after, _ = cli("search-plane-project", "--tenant", tenant, "--max-months", "4")
        assert drained_after["status"] == "complete" and drained_after["failed"] == 0, drained_after
        status_after, _ = cli("search-plane-status", "--tenant", tenant, "--target-tokens", "4", "--overlap-tokens", "1")
        assert status_after["drift"] == 0 and status_after["outbox_pending"] == 0, status_after
        writer.close()
        reader.close()

    print(json.dumps({
        "status": "ok",
        "postgres_plane_vectors_before": vectors,
        "drained_rows": drained["rows"],
        "drift_before_retirement": status["drift"],
        "retired": retired["applied"],
        "search_after_retirement_results": len(found["results"]),
        "postgres_plane_refused": True,
        "drift_after_append": status_after["drift"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
