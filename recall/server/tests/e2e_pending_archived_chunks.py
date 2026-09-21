#!/usr/bin/env python3
"""An unfinished parent projection does not hide unchanged archived events."""
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
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty  # noqa: E402


def unavailable(callback):
    try:
        callback()
    except ChunkBodyError as error:
        assert str(error) == "archived_chunk_body_unavailable"
    else:
        raise AssertionError("unverified archive or fallback body returned")


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_pending_archive_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        tenant, source, principal, parent = "tenant:pending:test", "codex:pending:test", "principal:pending:test", "session:pending:test"
        with tempfile.TemporaryDirectory(prefix="recall-pending-archive-") as temporary:
            archive = FilesystemArchiveStore(Path(temporary) / "archive", namespace_key=b"p" * 32)
            projection = CanonicalLogicalEvidenceProjector(store, LogicalEvidenceProjectionStore(archive),
                bound_tenant_id=tenant, raw_archive=archive)
            args = dict(tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
            pg = BoundCanonicalRetrieval(store, **args)
            archived = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **args)

            def insert(connection, native, text, ordinal):
                return insert_record(connection, tenant=tenant, source=source, parent=parent,
                                     native=native, text=text, role="assistant", byte_start=ordinal * 10)

            def dirty(connection, native):
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                           native_ids=[native], reason="ingest")

            def document_id(native):
                with store.connect() as connection:
                    return connection.execute("""SELECT document_id FROM canonical_documents
                        WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_current""",
                        (tenant, source, native)).fetchone()["document_id"]

            def bodies(*native_ids, reader=archive):
                return read_archived_chunks(store, reader, tenant_id=tenant, source_ids=(source,),
                                            document_ids=tuple(document_id(n) for n in native_ids))

            with store.connect() as connection:
                insert_source(connection, tenant, principal, source)
                a = insert(connection, "a", "A exact café 🧠", 0)
                c = insert(connection, "c", "C unchanged sibling", 1)
            before_a, before_c = pg.show(a), pg.show(c)
            assert projection.seed_backfill(tenant_id=tenant) == 1
            assert projection.project_pending(batch_size=10, max_batches=1, upload_concurrency=1)["documents"] == 1
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
                b = insert(connection, "b", "B newly appended", 2)
                dirty(connection, "b")
                assert connection.execute("SELECT count(*) AS n FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s", (tenant, source)).fetchone()["n"] == 1
            before_b = pg.show(b)
            assert archived.show(a) == before_a
            assert archived.show(c) == before_c
            assert archived.show(b) == before_b
            assert set(bodies("a", "b", "c")) == {(source, document_id("a")), (source, document_id("c"))}
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE receipt=%s", (b,))
            unavailable(lambda: archived.show(b))
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted=%s WHERE receipt=%s", ("B newly appended", b))

            # A2 has new content and receipts; the old shared part still contains A1.
            with store.connect() as connection:
                connection.execute("UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s AND native_id='a'", (tenant, source))
                temporary_receipt = insert(connection, "a-v2", "A second revision", 3)
                connection.execute("UPDATE canonical_events SET native_id='a',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='a-v2'", (tenant, source))
                connection.execute("UPDATE canonical_documents SET native_id='a',revision=2 WHERE tenant_id=%s AND source_id=%s AND native_id='a-v2'", (tenant, source))
                a2 = f"recall://{source}/a?rev=2#item=0"
                connection.execute("UPDATE canonical_chunks SET receipt=%s WHERE receipt=%s", (a2, temporary_receipt))
                dirty(connection, "a")
            assert archived.show(a) is None
            assert archived.show(a2) == pg.show(a2)
            assert archived.show(c) == before_c
            assert (source, document_id("a")) not in bodies("a", "c")
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE receipt=%s", (a2,))
            unavailable(lambda: archived.show(a2))
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted=%s WHERE receipt=%s", ("A second revision", a2))
                connection.execute("""INSERT INTO canonical_events(
                    tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                    kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                    SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,
                    kind,%s,revision+1,occurred_at,observed_at,true,'{}'::jsonb FROM canonical_events
                    WHERE tenant_id=%s AND source_id=%s AND native_id='a' AND revision=2""",
                    ("evt_" + uuid.uuid4().hex, "f" * 64, tenant, source))
                dirty(connection, "a")
                part = connection.execute("SELECT * FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal LIMIT 1", (tenant, source)).fetchone()
            assert archived.show(a) is None and archived.show(a2) is None
            assert archived.show(c) == before_c
            assert (source, document_id("a")) not in bodies("a", "c")

            path = archive.root / part["object_key"] / "data"
            payload = path.read_bytes()
            try:
                path.write_bytes(b"corrupt pending shared part")
                unavailable(lambda: bodies("c"))
                unavailable(lambda: archived.show(c))
            finally:
                path.write_bytes(payload)
                path.chmod(0o600)

            class PublishingArchive:
                published = False

                def read_raw(self, reference):
                    result = archive.read_raw(reference)
                    if not self.published:
                        self.published = True
                        with store.connect() as connection:
                            connection.execute("UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s AND source_id=%s", ("e" * 64, tenant, source))
                    return result

            publisher = PublishingArchive()
            unavailable(lambda: bodies("c", reader=publisher))
            assert publisher.published
            assert bodies("c")[(source, document_id("c"))][0]["text_redacted"] == "C unchanged sibling"
            assert archived.show(c) == before_c
            assert archived.show(b) == before_b
        print(json.dumps({"status": "pass", "pending_append_keeps_unchanged_archive": True,
            "new_and_revised_documents_use_verified_inline_only": True, "old_revision_and_tombstone_denied": True,
            "pending_corruption_fails_closed": True, "publication_race_rejected_retry_succeeds": True}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
