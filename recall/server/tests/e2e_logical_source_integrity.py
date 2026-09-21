#!/usr/bin/env python3
"""Reject corrupt canonical sources before any logical object is uploaded."""
from __future__ import annotations

import hashlib
import errno
import json
import os
import sys
import tempfile
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)


class CountingArchive:
    def __init__(self, delegate):
        self.delegate = delegate
        self.uploads = 0

    def put_raw(self, **values):
        self.uploads += 1
        return self.delegate.put_raw(**values)

    def __getattr__(self, name):
        return getattr(self.delegate, name)


class SmallPartProjection(LogicalEvidenceProjectionStore):
    def __init__(self, archive, store):
        super().__init__(archive)
        self.store = store
        self.upload_calls = 0

    def put_records(self, **values):
        assert self.store.active_connections == 0, "upload holds a database connection"
        self.upload_calls += 1
        # The healthy prefix exceeds a part, so checking only while uploading
        # would publish objects before a late invalid input is encountered.
        return super().put_records(**values, part_bytes=1024)


class TrackedStore(BrainStore):
    def __init__(self, dsn):
        self.local = threading.local()
        super().__init__(dsn)

    @property
    def active_connections(self):
        return getattr(self.local, "active", 0)

    @contextmanager
    def connect(self):
        with super().connect() as connection:
            self.local.active = self.active_connections + 1
            try:
                yield connection
            finally:
                self.local.active -= 1


class FullSpool:
    """A disk-full failure after one healthy record has been captured."""

    def __init__(self):
        self.file = tempfile.TemporaryFile(mode="w+b")
        self.writes = 0

    def write(self, payload):
        self.writes += 1
        if self.writes == 2:
            raise OSError(errno.ENOSPC, "synthetic full spool")
        return self.file.write(payload)

    def tell(self):
        return self.file.tell()

    def close(self):
        self.file.close()


def snapshot(store, tenant, source):
    with store.connect() as connection:
        return connection.execute(
            """SELECT to_jsonb(document) AS value FROM canonical_evidence_documents document
               WHERE tenant_id=%s AND source_id=%s ORDER BY logical_document_id""",
            (tenant, source),
        ).fetchall()


def run_case(store, root, *, position, corruption):
    nonce = uuid.uuid4().hex
    tenant, source, principal = f"tenant:integrity:{nonce}", f"codex:integrity:{nonce}", f"principal:{nonce}"
    parent = "session:integrity"
    archive = CountingArchive(FilesystemArchiveStore(root / nonce, namespace_key=b"i" * 32))
    projection = SmallPartProjection(archive, store)
    projector = CanonicalLogicalEvidenceProjector(store, projection, bound_tenant_id=tenant, raw_archive=archive)
    with store.connect() as connection:
        insert_source(connection, tenant, principal, source)
        for ordinal in range(3):
            insert_record(
                connection, tenant=tenant, source=source, parent=parent,
                native=f"event-{ordinal:04}", text=(f"source {ordinal} α 🧠 café\n" * 200),
                role="assistant", byte_start=ordinal * 10,
            )
    assert projector.seed_backfill(tenant_id=tenant) == 1
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["failed"] == 0, report
    before = snapshot(store, tenant, source)
    assert len(before) == 1
    with store.connect() as connection:
        document = connection.execute(
            "SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id=%s",
            (tenant, source, f"event-{position:04}"),
        ).fetchone()["document_id"]
        scope = (tenant, source, document)
        where = "WHERE tenant_id=%s AND source_id=%s AND document_id=%s"
        if corruption == "cleared_body":
            connection.execute("UPDATE canonical_chunks SET text_redacted='' " + where, scope)
        elif corruption == "document_hash":
            connection.execute("UPDATE canonical_documents SET text_sha256=%s " + where, ("0" * 64, *scope))
        elif corruption == "chunk_hash":
            connection.execute("UPDATE canonical_chunks SET text_sha256=%s " + where, ("0" * 64, *scope))
        elif corruption == "missing_chunks":
            connection.execute("DELETE FROM canonical_chunks " + where, scope)
        elif corruption != "spool_failure":
            raise AssertionError(corruption)
        mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                    native_ids=[f"event-{position:04}"], reason="ingest")
        queued = connection.execute(
            "SELECT generation,changed_at FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        ).fetchone()
    archive.uploads = projection.upload_calls = 0
    if corruption == "spool_failure":
        full_spool = FullSpool()
        with patch("recall_server.logical_evidence_projection.tempfile.TemporaryFile", return_value=full_spool):
            report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
        assert full_spool.file.closed
    else:
        report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert archive.uploads == 0, (corruption, position, "archive uploads", archive.uploads)
    assert projection.upload_calls == 0, (corruption, position, "put_records called before complete validation")
    assert report["failed"] == 1 and report["documents"] == 0, report
    assert snapshot(store, tenant, source) == before, "invalid inputs replaced the live manifest"
    with store.connect() as connection:
        retained = connection.execute(
            "SELECT generation,changed_at,attempts,last_error_code FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        ).fetchone()
    assert retained is not None
    assert retained["generation"] == queued["generation"] and retained["changed_at"] == queued["changed_at"]
    code = "OSError" if corruption == "spool_failure" else "logical_evidence_source_integrity_invalid"
    assert retained["attempts"] == 1 and retained["last_error_code"] == code, retained


def empty_source_is_valid(store, root):
    nonce = uuid.uuid4().hex
    tenant, source = f"tenant:empty:{nonce}", f"codex:empty:{nonce}"
    archive = FilesystemArchiveStore(root / nonce, namespace_key=b"e" * 32)
    projection = LogicalEvidenceProjectionStore(archive)
    projector = CanonicalLogicalEvidenceProjector(store, projection, bound_tenant_id=tenant, raw_archive=archive)
    with store.connect() as connection:
        insert_source(connection, tenant, "principal:empty", source)
        receipt = insert_record(connection, tenant=tenant, source=source, parent="empty-session",
                                native="empty-record", text="", role="assistant", byte_start=0)
    assert projector.seed_backfill(tenant_id=tenant) == 1
    report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 1 and report["records"] == 1 and report["failed"] == 0, report
    target, = projector.targets_for_receipts(tenant_id=tenant, source_ids=(source,), receipts=(receipt,), limit=1)
    record, = [json.loads(line) for line in projection.read_part(target["reference"], tenant_id=tenant, source_id=source).splitlines()]
    assert record["text"] == "" and record["receipts"] == [receipt]
    assert hashlib.sha256(record["text"].encode()).hexdigest() == hashlib.sha256(b"").hexdigest()


def late_candidate_stops_entire_shard(store, root):
    nonce = uuid.uuid4().hex
    tenant, source = f"tenant:shard:{nonce}", f"codex:shard:{nonce}"
    archive = CountingArchive(FilesystemArchiveStore(root / nonce, namespace_key=b"s" * 32))
    projection = SmallPartProjection(archive, store)
    projector = CanonicalLogicalEvidenceProjector(store, projection, bound_tenant_id=tenant, raw_archive=archive)
    with store.connect() as connection:
        insert_source(connection, tenant, "principal:shard", source)
        for ordinal in range(2):
            insert_record(connection, tenant=tenant, source=source, parent=f"parent-{ordinal}",
                          native=f"event-{ordinal}", text="valid original text " * 200,
                          role="assistant", byte_start=0)
    assert projector.seed_backfill(tenant_id=tenant) == 2
    report = projector.project_pending(tenant_id=tenant, batch_size=2, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 2, report
    before = snapshot(store, tenant, source)
    with store.connect() as connection:
        connection.execute(
            """UPDATE canonical_documents SET text_sha256=%s
               WHERE tenant_id=%s AND source_id=%s AND native_id='event-1'""",
            ("0" * 64, tenant, source),
        )
        mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=source,
                                    native_ids=["event-0", "event-1"], reason="ingest")
    candidates = tuple(sorted(projector._pending(tenant_id=tenant, limit=2), key=lambda c: c.native_parent_id))
    assert len(candidates) == 2 and candidates[-1].native_parent_id == "parent-1"
    archive.uploads = projection.upload_calls = 0
    try:
        projector._prepare_batch_and_upload(candidates)
    except LogicalEvidenceError as error:
        assert str(error) == "logical_evidence_source_integrity_invalid"
    else:
        raise AssertionError("late invalid candidate did not stop the shard")
    assert archive.uploads == projection.upload_calls == 0
    assert snapshot(store, tenant, source) == before
    assert len(projector._pending(tenant_id=tenant, limit=2)) == 2


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_source_integrity_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        with tempfile.TemporaryDirectory(prefix="recall-source-integrity-") as directory:
            root = Path(directory)
            for corruption in ("cleared_body", "document_hash", "chunk_hash", "missing_chunks"):
                for position in (0, 2):
                    run_case(store, root, position=position, corruption=corruption)
            run_case(store, root, position=2, corruption="spool_failure")
            empty_source_is_valid(store, root)
            late_candidate_stops_entire_shard(store, root)
        print(json.dumps({"status": "pass", "invalid_cases": 8, "invalid_source_uploads": 0,
                          "manifests_unchanged": True, "queues_retained": True, "empty_source_preserved": True,
                          "late_candidate_stops_shard": True, "full_spool_stops_before_upload": True,
                          "upload_holds_connection": False}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
