#!/usr/bin/env python3
"""Fresh PostgreSQL + fake turbopuffer proof of scoped, live receipt retrieval."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(SERVER.parent))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings, passage_row  # noqa: E402
from recall_server.storage_retirement import retire_chunk_search_index  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_parent_scope_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, source, principal = "tenant:parent:e2e", "codex:parent:e2e", "principal:parent:e2e"
        parent = "session:wanted"
        with tempfile.TemporaryDirectory(prefix="recall-parent-e2e-") as temporary:
            archive = FilesystemArchiveStore(Path(temporary) / "archive", namespace_key=b"p" * 32)
            with store.connect() as connection:
                insert_source(connection, tenant, principal, source)
                first = insert_record(connection, tenant=tenant, source=source, parent=parent,
                    native="wanted-first", text="migration first explanation", role="assistant", byte_start=0)
                second = insert_record(connection, tenant=tenant, source=source, parent=parent,
                    native="wanted-second", text="migration second explanation", role="assistant", byte_start=10)
                insert_record(connection, tenant=tenant, source=source, parent="session:other",
                    native="other-event", text="migration lock deployment other explanation", role="assistant", byte_start=0)
                connection.execute("UPDATE canonical_events SET occurred_at='2026-07-27T01:00:00Z' WHERE native_id='wanted-second'")
            projection = LogicalEvidenceProjectionStore(archive)
            logical = CanonicalLogicalEvidenceProjector(store, projection, bound_tenant_id=tenant, raw_archive=archive)
            logical.seed_backfill(tenant_id=tenant)
            assert logical.project_pending(batch_size=10, max_batches=1, upload_concurrency=1)["documents"] == 2
            policy = PassagePolicy(target_tokens=32, overlap_tokens=4)
            passages = CanonicalPassageProjector(store, projection, policy=policy, bound_tenant_id=tenant)
            assert passages.project_pending(batch_size=10, max_batches=1, concurrency=1)["documents"] == 2
            with store.connect() as connection:
                rows = connection.execute("""SELECT passage.*,evidence.native_parent_id,
                    evidence.first_occurred_at AS doc_first_occurred_at,
                    evidence.last_occurred_at AS doc_last_occurred_at,
                    evidence.manifest_object_key,evidence.manifest_content_sha256
                    FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                    USING(tenant_id,source_id,logical_document_id,revision)
                    WHERE passage.tenant_id=%s""", (tenant,)).fetchall()
            store.turbopuffer = TurbopufferSettings(api_key="synthetic")
            store.turbopuffer_client = FakeTurbopuffer()
            store.search_plane = "turbopuffer"
            ns = store.turbopuffer_client.namespace(store.turbopuffer.namespace(tenant))
            ns.write(upsert_rows=[passage_row(row) for row in rows])
            bound = BoundCanonicalRetrieval(store, tenant_id=tenant, principal_id=principal,
                authorized_sources=(source,), passage_policy=policy)

            def lookup(filters=None, wanted_source=source):
                return bound._parent_scoped_receipts(source_id=wanted_source, parent_id=parent,
                    terms=["migration", "lock", "deployment"], filters=filters, limit=10)

            assert set(lookup()) == {first, second}, lookup()
            with store.connect() as connection:
                chunks_before = connection.execute("SELECT chunk_id,text_redacted,text_sha256,receipt FROM canonical_chunks ORDER BY chunk_id").fetchall()
            preview = retire_chunk_search_index(store)
            assert preview["status"] == "preview" and preview["bytes_before"] > 0
            retired = retire_chunk_search_index(store, apply=True)
            assert retired["status"] == "retired" and retired["bytes_reclaimed"] == preview["bytes_before"]
            assert retire_chunk_search_index(store, apply=True)["status"] == "already_absent"
            with store.connect() as connection:
                assert connection.execute("SELECT to_regclass('public.canonical_chunks_search_idx') AS idx").fetchone()["idx"] is None
                chunks_after = connection.execute("SELECT chunk_id,text_redacted,text_sha256,receipt FROM canonical_chunks ORDER BY chunk_id").fetchall()
            assert chunks_after == chunks_before, "index retirement must preserve exact chunk rows and bodies"
            assert set(lookup()) == {first, second}
            assert lookup({"since": "2026-07-27T00:30:00Z"}) == (second,)
            assert lookup({"until": "2026-07-27T00:30:00Z"}) == (first,)
            assert lookup(wanted_source="codex:ungranted") == ()
            foreign = BoundCanonicalRetrieval(store, tenant_id="tenant:other", principal_id=principal,
                authorized_sources=(source,), passage_policy=policy)
            assert foreign._parent_scoped_receipts(source_id=source, parent_id=parent,
                terms=["migration"], filters=None, limit=10) == ()

            wanted = [row for row in rows if row["native_parent_id"] == parent]
            assert wanted
            ldoc = wanted[0]["logical_document_id"]
            manifest = wanted[0]["manifest_content_sha256"]
            with store.connect() as connection:
                connection.execute("UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE logical_document_id=%s", ("f" * 64, ldoc))
            assert lookup() == (), "stale manifest pins must not return receipts"
            with store.connect() as connection:
                connection.execute("UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE logical_document_id=%s", (manifest, ldoc))
            ns.write(upsert_rows=[{**passage_row(row), "revision": row["revision"] + 1} for row in wanted])
            assert lookup() == (), "remote revision ahead of current catalog must be denied"
            ns.write(upsert_rows=[passage_row(row) for row in wanted])

            for table, assignment, restore in [
                ("canonical_chunks", "deleted_at=now()", "deleted_at=NULL"),
                ("canonical_documents", "is_current=false", "is_current=true"),
                ("canonical_documents", "deleted_at=now(),is_current=false", "deleted_at=NULL,is_current=true"),
            ]:
                with store.connect() as connection:
                    connection.execute(f"UPDATE {table} SET {assignment} WHERE tenant_id=%s AND source_id=%s", (tenant, source))
                assert lookup() == (), (table, assignment)
                with store.connect() as connection:
                    connection.execute(f"UPDATE {table} SET {restore} WHERE tenant_id=%s AND source_id=%s", (tenant, source))
            assert set(lookup()) == {first, second}

            with store.connect() as connection:
                connection.execute("""INSERT INTO canonical_events(
                    tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                    kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                    SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,
                    kind,%s,revision+1,occurred_at,observed_at,true,'{}'::jsonb FROM canonical_events
                    WHERE tenant_id=%s AND source_id=%s AND native_id='wanted-first'""",
                    ("evt_" + uuid.uuid4().hex, "f" * 64, tenant, source))
            assert lookup() == (second,), "later tombstone must reject only the deleted event"
            ns.fail_queries = TimeoutError("synthetic timeout")
            assert lookup() == ()
        print(json.dumps({"status": "pass", "parent_scope_before_ranking": True,
            "exact_event_time_and_tombstones": True, "live_receipts_and_pins_only": True,
            "chunk_gin_absent": True, "provider_failure_closed": True}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
