#!/usr/bin/env python3
"""Reprojection preserves verified archived turns after PostgreSQL body retirement."""
from __future__ import annotations

import json
import hashlib
import errno
import os
import sys
import tempfile
import uuid
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from e2e_logical_source_integrity import TrackedStore, CountingArchive, SmallPartProjection, snapshot  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.chunk_bodies import read_archived_chunks  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty  # noqa: E402
from recall_server.passage_projection import decode_logical_record  # noqa: E402


class ReadCountingArchive(CountingArchive):
    def __init__(self, delegate, store):
        super().__init__(delegate)
        self.store = store
        self.reads = Counter()
        self.on_read = None

    def read_raw(self, reference):
        assert self.store.active_connections == 0, "archive read holds a database connection"
        self.reads[reference["artifact_id"]] += 1
        if self.on_read is not None:
            callback, self.on_read = self.on_read, None
            callback()
        return self.delegate.read_raw(reference)


def fixture(store, root, count=3):
    nonce = uuid.uuid4().hex
    tenant, source = f"tenant:reprojection:{nonce}", f"codex:reprojection:{nonce}"
    archive = ReadCountingArchive(FilesystemArchiveStore(root / nonce, namespace_key=b"r" * 32), store)
    projection = SmallPartProjection(archive, store)
    projector = CanonicalLogicalEvidenceProjector(store, projection, bound_tenant_id=tenant, raw_archive=archive)
    texts = {f"event-{n:04}": (f"source {n} α 🧠 café\n" * 100) for n in range(count)}
    with store.connect() as connection:
        insert_source(connection, tenant, "principal:reprojection", source)
        for n, (native, text) in enumerate(texts.items()):
            insert_record(connection, tenant=tenant, source=source, parent="session", native=native,
                          text=text, role="assistant", byte_start=n * 10)
    assert projector.seed_backfill(tenant_id=tenant) == 1
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["failed"] == 0, report
    archive.uploads = projection.upload_calls = 0
    archive.reads.clear()
    return tenant, source, archive, projection, projector, texts


def read_records(store, projector, projection, tenant, source):
    with store.connect() as connection:
        parts = connection.execute(
            "SELECT * FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal",
            (tenant, source),
        ).fetchall()
    return [decode_logical_record(line + b"\n", source_id=source)
            for part in parts
            for line in projection.read_part(projector._reference(part), tenant_id=tenant, source_id=source).splitlines()]


def append_after_clear(store, root, count):
    tenant, source, archive, projection, projector, texts = fixture(store, root, count)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
        insert_record(connection, tenant=tenant, source=source, parent="session", native="new-event",
                      text="new source text", role="user", byte_start=count * 10)
        mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source, native_ids=["new-event"], reason="ingest")
        old_parts = connection.execute("SELECT artifact_id FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s", (tenant, source)).fetchall()
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["failed"] == 0, report
    assert archive.reads == Counter({part["artifact_id"]: 1 for part in old_parts}), archive.reads
    texts["new-event"] = "new source text"
    records = read_records(store, projector, projection, tenant, source)
    assert {record.event_native_id: record.text for record in records} == texts
    assert [record.event_native_id for record in records] == list(texts)
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) AS count FROM canonical_evidence_objects WHERE tenant_id=%s", (tenant,)).fetchone()["count"] == 0
        assert connection.execute("SELECT count(*) AS count FROM canonical_evidence_document_queue WHERE tenant_id=%s", (tenant,)).fetchone()["count"] == 0
        documents = connection.execute("SELECT document_id,native_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY native_id LIMIT 3", (tenant, source)).fetchall()
    hydrated = read_archived_chunks(store, archive, tenant_id=tenant, source_ids=(source,), document_ids=tuple(d["document_id"] for d in documents))
    for document in documents:
        assert "".join(chunk["text_redacted"] for chunk in hydrated[(source, document["document_id"])]) == texts[document["native_id"]]


def mark_dirty(store, tenant, source):
    with store.connect() as connection:
        mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                    native_ids=["event-0000"], reason="ingest")


def revision_tombstone_actor_backfill(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    revised = "revised source bytes"
    actor = "actor_" + uuid.uuid4().hex
    with store.connect() as connection:
        scope = (tenant, source)
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", scope)
        connection.execute("UPDATE canonical_documents SET revision=2,text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND native_id='event-0001'", (hashlib.sha256(revised.encode()).hexdigest(), *scope))
        connection.execute("UPDATE canonical_chunks SET text_redacted=%s,text_sha256=%s,receipt=replace(receipt,'?rev=1#','?rev=2#') WHERE tenant_id=%s AND source_id=%s AND receipt LIKE '%%/event-0001?%%'", (revised, hashlib.sha256(revised.encode()).hexdigest(), *scope))
        connection.execute("UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND native_id='event-0002'", scope)
        connection.execute("UPDATE canonical_events SET source_ordinal=CASE WHEN native_id='event-0000' THEN 999 ELSE 0 END WHERE tenant_id=%s AND source_id=%s", scope)
        connection.execute("INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES(%s,%s,'human','Current actor')", (tenant, actor))
        connection.execute("INSERT INTO canonical_source_actor_bindings(tenant_id,source_id,actor_id,relation) VALUES(%s,%s,%s,'contributor')", (tenant, source, actor))
    mark_dirty(store, tenant, source)
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["failed"] == 0, report
    records = read_records(store, projector, projection, tenant, source)
    assert [(r.event_native_id, r.text) for r in records] == [("event-0001", revised), ("event-0000", texts["event-0000"])]
    assert "?rev=2#" in records[0].receipts[0]
    assert all(r.actor_links[0].actor_id == actor for r in records)


def fails_before_upload(store, root, mode):
    tenant, source, archive, projection, projector, _texts = fixture(store, root)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
        part = connection.execute("SELECT * FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal LIMIT 1", (tenant, source)).fetchone()
    mark_dirty(store, tenant, source)
    before = snapshot(store, tenant, source)
    path = archive.delegate.root / part["object_key"] / "data"
    if mode == "missing":
        path.unlink()
    elif mode == "corrupt":
        path.write_bytes(b"bad archive")
    elif mode == "generation_race":
        archive.on_read = lambda: mark_dirty(store, tenant, source)
    elif mode == "manifest_race":
        def replace_manifest():
            with store.connect() as connection:
                connection.execute("UPDATE canonical_evidence_documents SET source_updated_at=source_updated_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
        archive.on_read = replace_manifest
    elif mode == "tombstone_race":
        def tombstone():
            with store.connect() as connection:
                connection.execute("UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND native_id='event-0000'", (tenant, source))
                mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source, native_ids=["event-0000"], reason="forget")
        archive.on_read = tombstone
    elif mode != "disk_full":
        raise AssertionError(mode)
    if mode == "disk_full":
        with patch("recall_server.logical_archive_bodies.sqlite3.connect", side_effect=OSError(errno.ENOSPC, "synthetic full lookup")):
            report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    else:
        report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 0 and report["failed"] == 1, report
    assert archive.uploads == projection.upload_calls == 0
    if mode != "manifest_race":
        assert snapshot(store, tenant, source) == before
    with store.connect() as connection:
        queued = connection.execute("SELECT generation,attempts,next_attempt_at FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s", (tenant, source)).fetchone()
        assert queued is not None
        if mode in ("generation_race", "tombstone_race"):
            assert queued["generation"] >= 2 and queued["attempts"] == 0 and queued["next_attempt_at"] is None, queued
        else:
            assert queued["attempts"] == 1, queued


def historical_intact_bodies_keep_their_boundaries(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    text = texts["event-0000"]
    left, right = text[:12], text[12:]
    with store.connect() as connection:
        chunk = connection.execute("SELECT * FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND receipt LIKE '%%/event-0000?%%'", (tenant, source)).fetchone()
        connection.execute("UPDATE canonical_chunks SET text_redacted=%s,text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND chunk_id=%s", (left, hashlib.sha256(left.encode()).hexdigest(), tenant, source, chunk["chunk_id"]))
        connection.execute("INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256) VALUES(%s,%s,%s,%s,1,%s,%s,%s)",
                           (tenant, source, "chk_" + uuid.uuid4().hex, chunk["document_id"], chunk["receipt"].replace("#item=0", "#item=1"), right, hashlib.sha256(right.encode()).hexdigest()))
        connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s AND document_id<>%s", (tenant, source, chunk["document_id"]))
    mark_dirty(store, tenant, source)
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["failed"] == 0, report
    records = read_records(store, projector, projection, tenant, source)
    assert {r.event_native_id: r.text for r in records} == texts
    assert len(records[0].receipts) == 2


def invalid_pg_prefix_preserves_current_metadata(store, root):
    tenant, source, archive, projection, projector, texts = fixture(store, root)
    with store.connect() as connection:
        connection.execute("UPDATE canonical_chunks SET text_redacted='{bad source' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
    mark_dirty(store, tenant, source)
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["failed"] == 0, report
    records = read_records(store, projector, projection, tenant, source)
    assert {r.event_native_id: r.text for r in records} == texts
    assert all(r.roles == ("assistant",) for r in records), "corrupt PG prefix changed current role metadata"


def intact_pg_never_reads_archive(store, root):
    tenant, source, archive, projection, projector, _texts = fixture(store, root)
    # Any archive lookup would fail; valid PG bodies still reproject normally.
    archive.on_read = lambda: (_ for _ in ()).throw(AssertionError("unexpected archive lookup"))
    mark_dirty(store, tenant, source)
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["repaired"] == 1 and report["failed"] == 0, report
    assert not archive.reads


def main():
    admin = os.environ["RECALL_DATABASE_URL"]
    database = "recall_archive_reprojection_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin)
    settings["dbname"] = database
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        with tempfile.TemporaryDirectory(prefix="recall-reprojection-") as directory:
            for count in (3, 105):
                append_after_clear(store, Path(directory), count)
            revision_tombstone_actor_backfill(store, Path(directory))
            intact_pg_never_reads_archive(store, Path(directory))
            invalid_pg_prefix_preserves_current_metadata(store, Path(directory))
            historical_intact_bodies_keep_their_boundaries(store, Path(directory))
            for mode in ("missing", "corrupt", "generation_race", "manifest_race", "tombstone_race", "disk_full"):
                fails_before_upload(store, Path(directory), mode)
        print(json.dumps({"status": "pass", "append_after_clear": True, "parts_read_once": True}))
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == "__main__":
    main()
