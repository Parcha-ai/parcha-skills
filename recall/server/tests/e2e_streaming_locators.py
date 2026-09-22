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
from recall_server import parent_chunk_proof


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


def metadata_fetch_protocol(store, root):
    """Real portal results, deadline cancellation and cleanup through _capture."""
    scope, _, _ = setup(store, root, count=65)
    before = state(store, scope)
    execute = psycopg.Cursor.execute
    statements = []

    def observed(cursor, query, *args, **kwargs):
        text = query.as_string() if isinstance(query, psycopg.sql.Composable) else str(query)
        if text.startswith("SELECT set_config('statement_timeout',") and '; FETCH FORWARD' in text:
            statements.append(text)
            assert kwargs == {'prepare': False}
            assert text.endswith('FETCH FORWARD 32 FROM "retirement_metadata"')
        return execute(cursor, query, *args, **kwargs)

    def capture(deadline):
        path = None
        try:
            with parent_chunk_proof.ParentMetadataSpool(max_bytes=1024**2) as spool:
                path = Path(spool.directory.name)
                result = parent_chunk_proof._capture(
                    store, spool,
                    (scope['tenant_id'], scope['source_id'], scope['native_parent_id']),
                    ParentRetirementLimits(), deadline,
                )
                assert result[2:] == (65, 65)
                assert spool.index.execute('SELECT count(*) FROM documents').fetchone()[0] == 65
        finally:
            assert (path is None or not path.exists()) and store.active_connections == 0

    with patch.object(psycopg.Cursor, 'execute', observed):
        capture(time.monotonic() + 30)
    assert len(statements) == 4  # Three bounded data FETCHes and one empty FETCH.
    assert state(store, scope) == before

    for stage in ('setting', 'fetch'):
        injected = []

        def stalled(cursor, query, *args, **kwargs):
            text = query.as_string() if isinstance(query, psycopg.sql.Composable) else str(query)
            if text.startswith("SELECT set_config('statement_timeout',") and '; FETCH FORWARD' in text:
                assert not injected
                injected.append(stage)
                with cursor.connection.cursor() as control:
                    if stage == 'setting':
                        execute(control, "SET LOCAL statement_timeout='25ms'")
                        query = psycopg.sql.SQL(
                            "SELECT set_config('statement_timeout','50ms',true) "
                            'FROM (SELECT pg_sleep(2)) delayed; '
                            'FETCH FORWARD 32 FROM "retirement_metadata"'
                        )
                    else:
                        execute(control, 'CLOSE "retirement_metadata"; '
                                'DECLARE "retirement_metadata" CURSOR FOR SELECT pg_sleep(2)', prepare=False)
                # FETCH uses the production remaining deadline, not a test override.
            return execute(cursor, query, *args, **kwargs)

        with patch.object(psycopg.Cursor, 'execute', stalled):
            try:
                capture(time.monotonic() + .5)
            except psycopg.errors.QueryCanceled:
                pass
            else:
                raise AssertionError('stalled metadata request accepted')
        assert injected == [stage] and store.active_connections == 0
        with store.connect() as connection:
            assert connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            assert connection.execute(
                "SELECT count(*) AS count FROM pg_cursors WHERE name='retirement_metadata'"
            ).fetchone()['count'] == 0
        assert state(store, scope) == before


def state(store, scope):
    with store.connect() as connection:
        return connection.execute(
            """SELECT document.document_id,document.body_record_ordinal,document.body_record_count,
            chunk.ordinal,chunk.receipt,chunk.text_redacted FROM canonical_documents document JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id)
            WHERE document.tenant_id=%s AND document.source_id=%s ORDER BY document.document_id,chunk.ordinal""",
            (scope["tenant_id"], scope["source_id"]),
        ).fetchall()


def is_locator_update(statement):
    return statement.lstrip().startswith("UPDATE canonical_documents") and (
        "SET body_record_ordinal=" in statement
    )


def batch_updates(store, root):
    scope, archive, _ = setup(store, root, count=256)
    initial = state(store, scope)
    execute = store._execute_bounded
    updates = []

    def observe(connection, statement, values, deadline_at):
        result = execute(connection, statement, values, deadline_at)
        if is_locator_update(statement):
            updates.append(result.rowcount)
        return result

    with patch.object(store, "_execute_bounded", side_effect=observe):
        report = locators.publish_parent_locators(
            store,
            archive,
            **scope,
            apply=True,
            limits=ParentRetirementLimits(batch_documents=256),
        )
    assert report["published_documents"] == 256 and report["batches"] == 1
    assert updates == [256], updates
    final = state(store, scope)
    assert all(row["body_record_ordinal"] is not None for row in final)
    assert [
        {k: v for k, v in row.items() if not k.startswith("body_record")}
        for row in final
    ] == [
        {k: v for k, v in row.items() if not k.startswith("body_record")}
        for row in initial
    ]
    updates.clear()
    with patch.object(store, "_execute_bounded", side_effect=observe):
        assert (
            locators.publish_parent_locators(store, archive, **scope, apply=True)[
                "published_documents"
            ]
            == 0
        )
    assert updates == []


def batch_mismatch_and_duplicate(store, root):
    scope, archive, _ = setup(store, root, count=4)
    initial = state(store, scope)
    execute = store._execute_bounded
    attempted = []

    def change_locked_row(connection, statement, values, deadline_at):
        if is_locator_update(statement) and not attempted:
            # Change a predicate after the owning snapshots on the same
            # connection, so the actual UPDATE count guard must roll back.
            connection.execute(
                """UPDATE canonical_documents SET text_sha256=%s
                WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                (
                    "f" * 64,
                    scope["tenant_id"],
                    scope["source_id"],
                    initial[-1]["document_id"],
                ),
            )
            attempted.append(True)
        return execute(connection, statement, values, deadline_at)

    with patch.object(store, "_execute_bounded", side_effect=change_locked_row):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
    assert attempted and str(error) == "locator_publication_document_changed"
    assert not error.commit_unknown and error.committed == dict(
        batches=0, published_documents=0
    )
    assert state(store, scope) == initial
    # A fresh full proof also verifies that the injected document hash rolled
    # back; the existing state helper intentionally contains only body/locators.
    assert (
        locators.publish_parent_locators(store, archive, **scope)["proposed_documents"]
        == 4
    )
    publish = locators._publish_batch

    def duplicate(*args, **kwargs):
        kwargs["rows"] = kwargs["rows"] + [kwargs["rows"][0]]
        return publish(*args, **kwargs)

    with patch.object(locators, "_publish_batch", side_effect=duplicate):
        error = denied(
            lambda: locators.publish_parent_locators(
                store, archive, **scope, apply=True
            )
        )
    assert str(error) == "locator_publication_document_changed"
    assert not error.commit_unknown and error.committed["published_documents"] == 0
    assert state(store, scope) == initial


def lock_statement_diagnostics(store, root):
    """Real SQL lock refusal after an acknowledged prefix, without retrying it."""
    cases = [
        ("source_authority", "canonical_sources", "FOR UPDATE"),
        ("source_authority", "canonical_source_grants", "FOR UPDATE"),
        ("current_rows", "canonical_documents", "FOR UPDATE"),
        ("current_rows", "canonical_events", "FOR UPDATE"),
        ("current_rows", "raw_artifacts", "FOR UPDATE"),
        ("parent_share", "canonical_evidence_documents", "FOR UPDATE"),
        ("parent_exclusive", "canonical_evidence_documents", "FOR SHARE"),
        ("retirement_ledger", "canonical_chunk_retirement_progress", "FOR UPDATE"),
        ("chunks", "canonical_chunks", "FOR UPDATE"),
        ("locator_update", "canonical_documents", None),
    ]
    markers = {
        "source_authority": "FOR SHARE OF source,grant_row NOWAIT",
        "current_rows": "FOR UPDATE OF document NOWAIT",
        "parent_share": "FOR SHARE OF evidence NOWAIT",
        "parent_exclusive": "AND native_parent_id=%s FOR UPDATE NOWAIT",
        "retirement_ledger": "SELECT enabled FROM canonical_chunk_retirement_progress",
        "chunks": "ORDER BY document_id,ordinal LIMIT %s FOR SHARE NOWAIT",
        "locator_update": "UPDATE canonical_documents AS document",
    }
    observed = []
    for stage, table, mode in cases:
        scope, archive, _ = setup(store, root, count=4)
        if stage == "retirement_ledger":
            set_parent_retirement_enabled(
                store,
                **{k: scope[k] for k in ("tenant_id", "source_id", "native_parent_id")},
                enabled=False,
            )
        initial = state(store, scope)
        execute, publish = store._execute_bounded, locators._publish_batch
        attempts, locked = [], []
        with psycopg.connect(store.dsn) as blocker:

            def track(*args, **kwargs):
                attempts.append(True)
                return publish(*args, **kwargs)

            def contention(connection, statement, values, deadline_at):
                if len(attempts) == 2 and not locked and markers[stage] in statement:
                    if mode is None:
                        blocker.execute("LOCK TABLE canonical_documents IN SHARE MODE")
                        connection.execute("SET LOCAL lock_timeout='100ms'")
                    else:
                        # Identifiers come only from the literal test case list.
                        blocker.execute(
                            f"SELECT 1 FROM {table} WHERE tenant_id=%s AND source_id=%s {mode}",
                            (scope["tenant_id"], scope["source_id"]),
                        )
                    locked.append(True)
                return execute(connection, statement, values, deadline_at)

            with (
                patch.object(locators, "_publish_batch", side_effect=track),
                patch.object(store, "_execute_bounded", side_effect=contention),
            ):
                error = denied(
                    lambda: locators.publish_parent_locators(
                        store,
                        archive,
                        **scope,
                        apply=True,
                        limits=ParentRetirementLimits(batch_documents=2),
                    )
                )
            blocker.rollback()
        assert locked and len(attempts) == 2, stage
        assert str(error) == "locator_publication_lock_busy", stage
        assert isinstance(error.__context__, psycopg.errors.LockNotAvailable), stage
        assert error.__context__.sqlstate == "55P03", stage
        assert error.statement_stage == stage, (stage, error.statement_stage)
        assert not error.commit_unknown and error.committed == dict(
            batches=1, published_documents=2
        )
        final = state(store, scope)
        assert sum(row["body_record_ordinal"] is not None for row in final) == 2
        assert [row["text_redacted"] for row in final] == [
            row["text_redacted"] for row in initial
        ]
        # NOWAIT errors do not reliably populate PG diagnostic table/schema.
        observed.append(
            dict(
                stage=stage,
                table_metadata=error.__context__.diag.table_name,
                schema_metadata=error.__context__.diag.schema_name,
            )
        )
    return observed


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
        if is_locator_update(sql):
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
        if is_locator_update(sql):
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


def lost_ack_after_prefix(store, root):
    from contextlib import contextmanager

    scope, archive, _ = setup(store, root, count=6)
    initial = state(store, scope)
    publish = locators._publish_batch
    transaction = psycopg.Connection.transaction
    attempts = []

    def track(*args, **kwargs):
        attempts.append(True)
        return publish(*args, **kwargs)

    @contextmanager
    def commit_then_disconnect(connection, *args, **kwargs):
        with transaction(connection, *args, **kwargs):
            yield
        if len(attempts) == 2:
            raise psycopg.OperationalError("synthetic lost commit acknowledgement")

    with (
        patch.object(locators, "_publish_batch", side_effect=track),
        patch.object(psycopg.Connection, "transaction", commit_then_disconnect),
    ):
        error = denied(
            lambda: locators.publish_parent_locators(
                store,
                archive,
                **scope,
                apply=True,
                limits=ParentRetirementLimits(batch_documents=2),
            )
        )
    assert len(attempts) == 2 and error.commit_unknown
    assert error.statement_stage == "commit_or_cleanup"
    assert error.committed == dict(batches=1, published_documents=2)
    assert (
        sum(row["body_record_ordinal"] is not None for row in state(store, scope)) == 4
    )
    report = locators.publish_parent_locators(store, archive, **scope, apply=True)
    assert report["published_documents"] == 2 and report["complete"]
    assert [row["text_redacted"] for row in state(store, scope)] == [
        row["text_redacted"] for row in initial
    ]


def absent_ledger_enrollment_races(store, root):
    scope, archive, _ = setup(store, root, count=2)
    parent_scope = {
        key: value for key, value in scope.items() if key != "owner_principal_id"
    }
    execute = store._execute_bounded
    attempted = []

    def enroll_during_publication(connection, sql, values, deadline_at):
        result = execute(connection, sql, values, deadline_at)
        if is_locator_update(sql) and not attempted:
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
            metadata_fetch_protocol(store, Path(directory))
            lock_statement_diagnostics(store, Path(directory))
            batch_updates(store, Path(directory))
            batch_mismatch_and_duplicate(store, Path(directory))
            basic(store, Path(directory))
            failures(store, Path(directory))
            commit_races(store, Path(directory))
            lost_ack_after_prefix(store, Path(directory))
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
