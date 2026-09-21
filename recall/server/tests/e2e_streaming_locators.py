#!/usr/bin/env python3
"""Real PG: streaming locator publication preserves bodies and disabled scope."""

from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture, ReadCountingArchive
from e2e_logical_source_integrity import TrackedStore
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_retirement import (
    ParentRetirementLimits,
    set_parent_retirement_enabled,
)
from recall_server import streaming_locators as locators


def denied(callback):
    try:
        callback()
    except locators.LocatorPublicationError as error:
        return error
    raise AssertionError("unsafe locator publication accepted")


def setup(store, root, count=10):
    tenant, source, archive, _, projector, _ = fixture(store, root, count=count)
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,'principal:reprojection','owner')",
            (tenant, source),
        )
        connection.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        )
    return (
        dict(
            tenant_id=tenant,
            source_id=source,
            native_parent_id="session",
            owner_principal_id="principal:reprojection",
        ),
        archive,
        projector,
    )


def state(store, scope):
    with store.connect() as connection:
        return connection.execute(
            """SELECT document.document_id,document.body_record_ordinal,document.body_record_count,
            chunk.ordinal,chunk.receipt,chunk.text_redacted FROM canonical_documents document JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id)
            WHERE document.tenant_id=%s AND document.source_id=%s ORDER BY document.document_id,chunk.ordinal""",
            (scope["tenant_id"], scope["source_id"]),
        ).fetchall()


def basic(store, root):
    scope, archive, _ = setup(store, root)
    limits = ParentRetirementLimits(batch_documents=3, max_batches=2)
    initial = state(store, scope)
    pg = BoundCanonicalRetrieval(
        store,
        tenant_id=scope["tenant_id"],
        principal_id=scope["owner_principal_id"],
        authorized_sources=(scope["source_id"],),
    )
    receipt = f"recall://{scope['source_id']}/event-0005?rev=1#item=0"
    expected = (
        pg.show(receipt),
        pg.session_context(receipt, before=1, after=1),
        pg.related(limit=10),
    )
    preview = locators.publish_parent_locators(store, archive, **scope)
    assert preview["proposed_documents"] == 10 and state(store, scope) == initial
    reviewed = json.loads(json.dumps(preview["plan"], default=str))
    with store.connect() as connection:
        connection.execute(
            "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s",
            (scope["tenant_id"], scope["source_id"]),
        )
    denied(
        lambda: locators.publish_parent_locators(
            store, archive, **scope, apply=True, reviewed_plan=reviewed
        )
    )
    assert state(store, scope) == initial, (
        "stale reviewed catalog identity changed locators"
    )
    preview = locators.publish_parent_locators(store, archive, **scope)
    reviewed = json.loads(json.dumps(preview["plan"], default=str))
    archive.reads.clear()
    first = locators.publish_parent_locators(
        store, archive, **scope, apply=True, limits=limits, reviewed_plan=reviewed
    )
    assert (
        first["published_documents"] == 6
        and not first["complete"]
        and first["batches"] == 2
    ), first
    assert sum(archive.reads.values()) == 10 and max(archive.reads.values()) == 1
    again = locators.publish_parent_locators(store, archive, **scope, apply=True)
    assert again["published_documents"] == 4 and again["complete"], again
    final = state(store, scope)
    assert all(row["body_record_ordinal"] is not None for row in final)
    assert [
        {k: v for k, v in row.items() if not k.startswith("body_record")}
        for row in initial
    ] == [
        {k: v for k, v in row.items() if not k.startswith("body_record")}
        for row in final
    ]
    archived = BoundCanonicalRetrieval(
        store,
        tenant_id=scope["tenant_id"],
        principal_id=scope["owner_principal_id"],
        authorized_sources=(scope["source_id"],),
        chunk_body_archive=archive,
    )
    assert (
        archived.show(receipt),
        archived.session_context(receipt, before=1, after=1),
        archived.related(limit=10),
    ) == expected
    assert store.resolve(
        receipt,
        tenant_id=scope["tenant_id"],
        authorized_sources=(scope["source_id"],),
        chunk_body_archive=archive,
    ) == store.resolve(
        receipt, tenant_id=scope["tenant_id"], authorized_sources=(scope["source_id"],)
    )
    assert (
        locators.publish_parent_locators(store, archive, **scope, apply=True)[
            "published_documents"
        ]
        == 0
    )
    with store.connect() as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (scope["tenant_id"], scope["source_id"]),
            ).fetchone()
            is None
        )


def failures(store, root):
    scope, archive, _ = setup(store, root)
    initial = state(store, scope)
    read = archive.read_raw
    calls = []

    def corrupt_last(reference):
        payload = read(reference)
        calls.append(True)
        return b"x" * len(payload) if len(calls) == 10 else payload

    with patch.object(archive, "read_raw", side_effect=corrupt_last):
        denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
    assert len(calls) == 10 and state(store, scope) == initial
    archive.reads.clear()
    denied(
        lambda: locators.publish_parent_locators(
            store,
            archive,
            **(scope | {"owner_principal_id": "principal:denied"}),
            apply=True,
        )
    )
    assert not archive.reads
    set_parent_retirement_enabled(
        store,
        **{k: v for k, v in scope.items() if k != "owner_principal_id"},
        enabled=True,
    )
    denied(
        lambda: locators.publish_parent_locators(store, archive, **scope, apply=True)
    )
    assert not archive.reads
    set_parent_retirement_enabled(
        store,
        **{k: v for k, v in scope.items() if k != "owner_principal_id"},
        enabled=False,
    )
    execute = store._execute_bounded
    updated = []

    def fail_after_update(connection, sql, values, deadline_at):
        result = execute(connection, sql, values, deadline_at)
        if sql.lstrip().startswith(
            "UPDATE canonical_documents SET body_record_ordinal="
        ):
            updated.append(True)
            raise RuntimeError("synthetic after update")
        return result

    with patch.object(store, "_execute_bounded", side_effect=fail_after_update):
        failure = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
    assert (
        updated
        and failure.committed["published_documents"] == 0
        and state(store, scope) == initial
    )
    publish = locators._publish_batch
    for mutation in ("revoke", "recreate", "revision", "enabled"):

        def change(*args, **kwargs):
            with store.connect() as connection:
                if mutation == "revoke":
                    connection.execute(
                        "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                        (scope["tenant_id"], scope["source_id"]),
                    )
                elif mutation == "recreate":
                    connection.execute(
                        "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s",
                        (scope["tenant_id"], scope["source_id"]),
                    )
                elif mutation == "revision":
                    connection.execute(
                        "UPDATE canonical_documents SET revision=revision+1 WHERE document_id=%s",
                        (kwargs["rows"][0]["document_id"],),
                    )
                else:
                    connection.execute(
                        "UPDATE canonical_chunk_retirement_progress SET enabled=true,status='pending' WHERE tenant_id=%s AND source_id=%s",
                        (scope["tenant_id"], scope["source_id"]),
                    )
            return publish(*args, **kwargs)

        with patch.object(locators, "_publish_batch", side_effect=change):
            denied(
                lambda: locators.publish_parent_locators(
                    store, archive, **scope, apply=True
                )
            )
        with store.connect() as connection:
            if mutation == "revoke":
                connection.execute(
                    "INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,'principal:reprojection','owner')",
                    (scope["tenant_id"], scope["source_id"]),
                )
            elif mutation == "revision":
                connection.execute(
                    "UPDATE canonical_documents SET revision=1 WHERE tenant_id=%s AND source_id=%s",
                    (scope["tenant_id"], scope["source_id"]),
                )
            elif mutation == "enabled":
                connection.execute(
                    "UPDATE canonical_chunk_retirement_progress SET enabled=false,status='disabled' WHERE tenant_id=%s AND source_id=%s",
                    (scope["tenant_id"], scope["source_id"]),
                )
        assert state(store, scope) == initial
    report = locators.publish_parent_locators(store, archive, **scope, apply=True)
    assert report["published_documents"] == 10
    with store.connect() as connection:
        assert (
            connection.execute(
                "SELECT enabled FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (scope["tenant_id"], scope["source_id"]),
            ).fetchone()["enabled"]
            is False
        )


def commit_races(store, root):
    scope, archive, _ = setup(store, root, count=4)
    limits = ParentRetirementLimits(batch_documents=2)
    initial = state(store, scope)
    publish = locators._publish_batch
    calls = []

    def enable_after_one(*args, **kwargs):
        count = publish(*args, **kwargs)
        calls.append(True)
        if len(calls) == 1:
            set_parent_retirement_enabled(
                store,
                **{k: v for k, v in scope.items() if k != "owner_principal_id"},
                enabled=True,
            )
        return count

    with patch.object(locators, "_publish_batch", side_effect=enable_after_one):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True, limits=limits
            )
        )
    assert (
        error.committed == dict(batches=1, published_documents=2)
        and not error.commit_unknown
    )
    assert (
        sum(row["body_record_ordinal"] is not None for row in state(store, scope)) == 2
    )
    set_parent_retirement_enabled(
        store,
        **{k: v for k, v in scope.items() if k != "owner_principal_id"},
        enabled=False,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s",
            (scope["tenant_id"], scope["source_id"]),
        )
    execute = store._execute_bounded
    updated = []

    def observe_update(connection, sql, values, deadline_at):
        result = execute(connection, sql, values, deadline_at)
        if sql.lstrip().startswith(
            "UPDATE canonical_documents SET body_record_ordinal="
        ):
            updated.append(True)
        return result

    with patch.object(store, "_execute_bounded", side_effect=observe_update):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True, should_stop=lambda: bool(updated)
            )
        )
    assert (
        updated
        and error.committed["published_documents"] == 0
        and state(store, scope) == initial
    )
    # Expiry after actual UPDATE must roll back the current transaction too.
    from recall_server.db import SearchDeadlineExceeded

    check = locators._check_deadline
    updated.clear()

    def expire(deadline_at):
        if updated:
            raise SearchDeadlineExceeded()
        check(deadline_at)

    with (
        patch.object(store, "_execute_bounded", side_effect=observe_update),
        patch.object(locators, "_check_deadline", side_effect=expire),
    ):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
    assert (
        updated
        and error.committed["published_documents"] == 0
        and state(store, scope) == initial
    )
    # The native lock is shared with both canonical ingest paths.
    with psycopg.connect(store.dsn) as blocker:
        blocker.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"v2\x1f{scope['tenant_id']}\x1f{scope['source_id']}\x1fevent-0000",),
        )
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
        assert error.committed["published_documents"] == 0
    assert state(store, scope) == initial
    # Lost acknowledgment after a real COMMIT must never claim rollback.
    from contextlib import contextmanager

    transaction = psycopg.Connection.transaction
    updated.clear()

    @contextmanager
    def commit_then_disconnect(connection, *args, **kwargs):
        with transaction(connection, *args, **kwargs):
            yield
        if updated:
            raise psycopg.OperationalError("synthetic lost commit acknowledgement")

    with (
        patch.object(store, "_execute_bounded", side_effect=observe_update),
        patch.object(psycopg.Connection, "transaction", commit_then_disconnect),
    ):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True, limits=limits
            )
        )
    assert error.commit_unknown and error.committed == dict(
        batches=0, published_documents=0
    ), error.committed
    assert (
        sum(row["body_record_ordinal"] is not None for row in state(store, scope)) == 2
    )
    report = locators.publish_parent_locators(store, archive, **scope, apply=True)
    assert report["published_documents"] == 2 and report["complete"]


def absent_ledger_enrollment_races(store, root):
    scope, archive, _ = setup(store, root, count=2)
    parent_scope = {
        key: value for key, value in scope.items() if key != "owner_principal_id"
    }
    execute = store._execute_bounded
    attempted = []

    def enroll_during_publication(connection, sql, values, deadline_at):
        result = execute(connection, sql, values, deadline_at)
        if (
            sql.lstrip().startswith(
                "UPDATE canonical_documents SET body_record_ordinal="
            )
            and not attempted
        ):
            attempted.append(True)
            try:
                # A distinct pooled connection tries the real enrollment API
                # while the first transaction owns the publication catalog.
                set_parent_retirement_enabled(store, **parent_scope, enabled=True)
            except psycopg.errors.LockNotAvailable:
                pass
            else:
                raise AssertionError("enabled enrollment crossed absent-ledger fence")
        return result

    with patch.object(store, "_execute_bounded", side_effect=enroll_during_publication):
        report = locators.publish_parent_locators(store, archive, **scope, apply=True)
    assert attempted and report["published_documents"] == 2
    with store.connect() as connection:
        assert (
            connection.execute(
                "SELECT 1 FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (scope["tenant_id"], scope["source_id"]),
            ).fetchone()
            is None
        )
        connection.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s",
            (scope["tenant_id"], scope["source_id"]),
        )
    initial = state(store, scope)
    with psycopg.connect(store.dsn) as enrolling:
        # The reverse order: an uncommitted enabled row remains invisible to
        # the publisher, but enrollment's catalog SHARE lock must exclude it.
        catalog = enrolling.execute(
            """SELECT logical_document_id FROM canonical_evidence_documents
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s FOR SHARE NOWAIT""",
            (scope["tenant_id"], scope["source_id"], scope["native_parent_id"]),
        ).fetchone()
        enrolling.execute(
            """INSERT INTO canonical_chunk_retirement_progress
            (tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
            VALUES(%s,%s,%s,%s,true,'pending')""",
            (
                scope["tenant_id"],
                scope["source_id"],
                scope["native_parent_id"],
                catalog[0],
            ),
        )
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
        assert (
            error.committed["published_documents"] == 0
            and state(store, scope) == initial
        )
        enrolling.rollback()


def large_parent(store, root):
    # Exceeds the old20K metadata planner and64MiB interactive fullscan limits.
    nonce = uuid.uuid4().hex
    tenant = "tenant:large:" + nonce
    source = "source:large:" + nonce
    archive = ReadCountingArchive(
        FilesystemArchiveStore(root / nonce, namespace_key=b"s" * 32), store
    )
    projection = LogicalEvidenceProjectionStore(archive)
    projector = CanonicalLogicalEvidenceProjector(
        store, projection, bound_tenant_id=tenant, raw_archive=archive
    )
    count = 20_003
    with store.connect() as connection:
        insert_source(connection, tenant, "principal:large", source)
        connection.execute(
            "INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,'principal:large','owner')",
            (tenant, source),
        )
        for n in range(count):
            insert_record(
                connection,
                tenant=tenant,
                source=source,
                parent="session",
                native=f"event-{n:05}",
                text=f"{n}: " + ("λ" * 1900),
                role="assistant",
                byte_start=n,
            )
    assert projector.seed_backfill(tenant_id=tenant) == 1
    projected = projector.project_pending(
        tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1
    )
    assert projected["documents"] == 1 and projected["failed"] == 0, projected
    with store.connect() as connection:
        parts = connection.execute(
            "SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        ).fetchall()
        assert sum(p["size_bytes"] for p in parts) > 64 * 1024**2
        connection.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        )
    scope = dict(
        tenant_id=tenant,
        source_id=source,
        native_parent_id="session",
        owner_principal_id="principal:large",
    )
    # A fresh interpreter separates proof RSS from fixture/projector memory.
    child = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--large-proof-child"],
        input=json.dumps(dict(root=str(root / nonce), scope=scope)),
        text=True,
        capture_output=True,
        env=dict(os.environ, RECALL_DATABASE_URL=store.dsn),
        timeout=360,
    )
    assert child.returncode == 0, "isolated locator proof failed"
    report = json.loads(child.stdout)
    assert report["published_documents"] == count and report["complete"], report
    assert report["archive_gets"] == len(parts) and report["max_gets_per_part"] == 1
    with store.connect() as connection:
        counters = connection.execute(
            """SELECT count(*) AS documents,count(*) FILTER(WHERE body_record_ordinal IS NULL) AS missing
            FROM canonical_documents WHERE tenant_id=%s AND source_id=%s""",
            (tenant, source),
        ).fetchone()
        assert counters == dict(documents=count, missing=0), counters
        assert (
            connection.execute(
                "SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted=''",
                (tenant, source),
            ).fetchone()["n"]
            == 0
        )
    return dict(
        documents=count,
        archive_bytes=report["archive_bytes"],
        archive_gets=report["archive_gets"],
        batches=report["batches"],
        elapsed_ms=report["elapsed_ms"],
        baseline_rss_kib=report["baseline_rss_kib"],
        peak_rss_kib=report["peak_rss_kib"],
        spool_bytes=report["spool_bytes"],
    )


def large_proof_child():
    config = json.loads(sys.stdin.read())
    store = TrackedStore(os.environ["RECALL_DATABASE_URL"])
    archive = ReadCountingArchive(
        FilesystemArchiveStore(Path(config["root"]), namespace_key=b"s" * 32), store
    )

    def rss(field):
        # getrusage.ru_maxrss retains the pre-exec parent's high water on Linux.
        # /proc belongs to this fresh interpreter's current address space.
        line = next(
            value
            for value in Path("/proc/self/status").read_text().splitlines()
            if value.startswith(field + ":")
        )
        return int(line.split()[1])

    baseline = rss("VmRSS")
    started = time.monotonic()
    spool_bytes = []
    publish = locators._publish_batch

    def measured(*args, **kwargs):
        spool_bytes.append(
            (Path(kwargs["proof"]["spool"].directory.name) / "metadata.sqlite")
            .stat()
            .st_size
        )
        return publish(*args, **kwargs)

    try:
        with patch.object(locators, "_publish_batch", side_effect=measured):
            report = locators.publish_parent_locators(
                store,
                archive,
                **config["scope"],
                apply=True,
                deadline_at=started + 300,
                limits=ParentRetirementLimits(max_spool_bytes=128 * 1024**2),
            )
        report.pop("plan")
        report.update(
            elapsed_ms=round((time.monotonic() - started) * 1000),
            baseline_rss_kib=baseline,
            peak_rss_kib=rss("VmHWM"),
            max_gets_per_part=max(archive.reads.values()),
            spool_bytes=max(spool_bytes),
        )
        print(json.dumps(report))
    finally:
        store.close()


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_streaming_locator_" + uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(
        make_conninfo(**(conninfo_to_dict(admin_dsn) | {"dbname": database}))
    )
    try:
        store.migrate()
        with tempfile.TemporaryDirectory() as directory:
            basic(store, Path(directory))
            failures(store, Path(directory))
            commit_races(store, Path(directory))
            absent_ledger_enrollment_races(store, Path(directory))
            large = large_parent(store, Path(directory))
        print(
            json.dumps(
                dict(
                    status="pass",
                    batches_resume=True,
                    bodies_and_routes_exact=True,
                    disabled_preserved=True,
                    authority_and_catalog_races_refused=True,
                    large_parent=large,
                )
            )
        )
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}" WITH(FORCE)')


if __name__ == "__main__":
    if sys.argv[1:] == ["--large-proof-child"]:
        large_proof_child()
    else:
        main()
