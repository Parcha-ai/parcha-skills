#!/usr/bin/env python3
"""Real PG: finite enrollment, restore intent, ownership, claim races and resume."""

from pathlib import Path
import json
import os
import sys
import tempfile
import uuid
from unittest.mock import patch
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture
from e2e_logical_evidence_projection import insert_record
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
from e2e_logical_source_integrity import TrackedStore
from recall_server.chunk_retirement import (
    ChunkRetirementError,
    ParentRetirementLimits,
    retire_current_chunks,
)
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server import retirement_runner as runner


def refused(callback):
    try:
        callback()
    except ChunkRetirementError:
        return
    raise AssertionError("unsafe runner action succeeded")


def scenario(store, root):
    tenant, source, archive, _, _, _ = fixture(store, root, count=4)
    scope = runner.RetirementScope(tenant, "principal:reprojection", (source,))
    with store.connect() as c:
        c.execute(
            "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
            (tenant, scope.principal_id, source),
        )
        docs = c.execute(
            "SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY native_id",
            (tenant, source),
        ).fetchall()

    def state():
        with store.connect() as c:
            return c.execute(
                "SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchone()

    def ready():
        with store.connect() as c:
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            )

    def remaining():
        with store.connect() as c:
            return c.execute(
                "SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted<>''",
                (tenant, source),
            ).fetchone()["n"]

    # A restore before enrollment must leave a disabled marker, even if no bytes
    # needed restoration. Reviewed pages cannot override that operator intent.
    page = runner.plan_cohort(store, scope=scope, limit=1)
    assert len(page["parents"]) == 1 and not archive.reads
    target = dict(
        tenant_id=tenant, source_id=source, document_ids=(docs[0]["document_id"],)
    )
    plan = retire_current_chunks(store, archive, **target, restore=True)
    retire_current_chunks(
        store, archive, **target, restore=True, apply=True, reviewed_plan=plan["plan"]
    )
    assert state()["enabled"] is False
    disabled = state()
    assert (
        runner.enroll_cohort(store, scope=scope, reviewed_plan=page)["inserted_parents"]
        == 0
    )
    assert state() == disabled
    assert (
        runner.run_retirement(store, archive, scope=scope, apply=True)[
            "attempted_parents"
        ]
        == 0
    )
    # Synthetic explicit override models a later operator decision, never runner
    # enrollment behavior. Its cursor is preserved when the page is replayed.
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_chunk_retirement_progress SET enabled=true,status='pending' WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        )
    ready()
    reader = BoundCanonicalRetrieval(
        store,
        tenant_id=tenant,
        principal_id=scope.principal_id,
        authorized_sources=(source,),
        chunk_body_archive=archive,
    )
    anchor = f"recall://{source}/event-0001?rev=1#item=0"

    def reads():
        return (
            reader.show(anchor),
            reader.session_context(anchor, before=1, after=1),
            store.resolve(
                anchor,
                tenant_id=tenant,
                authorized_sources=(source,),
                chunk_body_archive=archive,
            ),
        )

    expected = reads()
    archive.reads.clear()
    first = runner.run_retirement(
        store,
        archive,
        scope=scope,
        apply=True,
        parent_limits=ParentRetirementLimits(batch_documents=1, max_batches=1),
    )
    assert (
        first["cleared_documents"] == 1
        and first["partial_parents"] == 1
        and remaining() == 3
    ), first
    assert all(n == 1 for n in archive.reads.values()), "runner repeated archive proof"
    partial = state()
    runner.enroll_cohort(store, scope=scope, reviewed_plan=page)
    assert state() == partial
    assert (
        runner.run_retirement(store, archive, scope=scope, apply=True)[
            "attempted_parents"
        ]
        == 0
    ), "cooldown ignored"
    ready()
    second = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert (
        second["cleared_documents"] == 3
        and remaining() == 0
        and state()["status"] == "complete"
    ), second
    assert reads() == expected
    assert state()["cumulative_cleared_documents"] == 4
    # Auth failure must happen before claim and archive access.
    archive.reads.clear()
    refused(
        lambda: runner.run_retirement(
            store,
            archive,
            scope=runner.RetirementScope(tenant, "other-principal", (source,)),
            apply=True,
        )
    )
    assert not archive.reads


def races(store, root):
    for kind in ("epoch", "owner", "stop", "unlocated"):
        tenant, source, archive, _, _, _ = fixture(store, root, count=3)
        scope = runner.RetirementScope(tenant, "principal:reprojection", (source,))
        with store.connect() as c:
            c.execute(
                "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
                (tenant, scope.principal_id, source),
            )
        page = runner.plan_cohort(store, scope=scope)
        assert (
            runner.enroll_cohort(store, scope=scope, reviewed_plan=page)[
                "inserted_parents"
            ]
            == 1
        )
        with store.connect() as c:
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            )
            if kind == "unlocated":
                c.execute(
                    "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s",
                    (tenant, source),
                )
        stopped = []

        def race():
            if kind == "stop":
                stopped.append(True)
                return
            with store.connect() as c:
                if kind == "epoch":
                    c.execute(
                        "UPDATE canonical_chunk_retirement_progress SET scope_epoch=scope_epoch+1 WHERE tenant_id=%s AND source_id=%s",
                        (tenant, source),
                    )
                elif kind == "owner":
                    c.execute(
                        "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                        (tenant, source),
                    )

        if kind != "unlocated":
            archive.on_read = race
        result = runner.run_retirement(
            store, archive, scope=scope, apply=True, should_stop=lambda: bool(stopped)
        )
        assert result["cleared_documents"] == 0, result
        with store.connect() as c:
            assert (
                c.execute(
                    "SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted<>''",
                    (tenant, source),
                ).fetchone()["n"]
                == 3
            )
        if kind in ("epoch", "owner"):
            assert result["failed_parents"] == 1, result
        if kind == "unlocated":
            assert (
                result["excluded"]["unlocated"] == 3
                and result["eligible_documents"] == 0
            ), result


def paging_and_fairness(store, root):
    tenant, source, archive, _, projector, _ = fixture(store, root, count=2)
    scope = runner.RetirementScope(tenant, "principal:reprojection", (source,))
    with store.connect() as c:
        c.execute(
            "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
            (tenant, scope.principal_id, source),
        )
        for parent in ("alpha", "zulu"):
            insert_record(
                c,
                tenant=tenant,
                source=source,
                parent=parent,
                native=parent + "-event",
                text="new exact body",
                role="user",
                byte_start=100,
            )
            mark_logical_evidence_dirty(
                c,
                tenant_id=tenant,
                source_id=source,
                native_ids=[parent + "-event"],
                reason="ingest",
            )
    projection = projector.project_pending(
        tenant_id=tenant, batch_size=3, max_batches=1, upload_concurrency=1
    )
    assert projection["documents"] == 2, projection
    archive.reads.clear()
    first = runner.plan_cohort(store, scope=scope, limit=1)
    second = runner.plan_cohort(store, scope=scope, after=first["next_cursor"], limit=1)
    third = runner.plan_cohort(store, scope=scope, after=second["next_cursor"], limit=1)
    assert first["more"] and second["more"] and not third["more"]
    assert [p["parents"][0]["native_parent_id"] for p in (first, second, third)] == [
        "alpha",
        "session",
        "zulu",
    ]
    for page in (first, second):
        runner.enroll_cohort(store, scope=scope, reviewed_plan=page)
    # zulu deliberately remains outside the reviewed cohort.
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        )
        bad = c.execute(
            "SELECT artifact_id FROM canonical_evidence_document_parts JOIN canonical_evidence_documents USING(tenant_id,source_id,logical_document_id,revision) WHERE tenant_id=%s AND source_id=%s AND native_parent_id='alpha' ORDER BY part_ordinal LIMIT 1",
            (tenant, source),
        ).fetchone()["artifact_id"]
    read = archive.read_raw

    def corrupt(reference):
        payload = read(reference)
        return b"x" * len(payload) if reference["artifact_id"] == bad else payload

    with patch.object(archive, "read_raw", side_effect=corrupt):
        result = runner.run_retirement(store, archive, scope=scope, apply=True)
    # Transport/decode errors have unknown provenance and stop conservatively;
    # the failed claim rotates on the NEXT invocation rather than monopolizing it.
    assert result["failed_parents"] == 1 and result["cleared_documents"] == 0, result
    healthy = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert healthy["cleared_documents"] == 2, healthy
    with store.connect() as c:
        assert (
            c.execute(
                "SELECT count(*) AS n FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchone()["n"]
            == 2
        )
        assert (
            c.execute(
                "SELECT canonical_chunks.text_redacted FROM canonical_chunks JOIN canonical_documents USING(tenant_id,source_id,document_id) WHERE tenant_id=%s AND source_id=%s AND native_id='zulu-event'",
                (tenant, source),
            ).fetchone()["text_redacted"]
            == "new exact body"
        )
    # An older review cannot enroll a recreated catalog with identical manifest
    # bytes. Catalog creation identity is separate from body-proof identity.
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s AND native_parent_id='zulu'",
            (tenant, source),
        )
    refused(lambda: runner.enroll_cohort(store, scope=scope, reviewed_plan=third))
    third = runner.plan_cohort(store, scope=scope, after=second["next_cursor"], limit=1)
    # A later catalog identity change invalidates the private page before enrollment.
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_evidence_documents SET document_content_sha256=%s WHERE tenant_id=%s AND source_id=%s AND native_parent_id='zulu'",
            ("0" * 64, tenant, source),
        )
    refused(lambda: runner.enroll_cohort(store, scope=scope, reviewed_plan=third))


def publication_restarts_grace(store, root):
    tenant, source, archive, _, projector, _ = fixture(store, root, count=2)
    scope = runner.RetirementScope(tenant, "principal:reprojection", (source,))
    with store.connect() as c:
        c.execute(
            "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
            (tenant, scope.principal_id, source),
        )
    runner.enroll_cohort(
        store, scope=scope, reviewed_plan=runner.plan_cohort(store, scope=scope)
    )

    def age():
        with store.connect() as c:
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            )

    def state():
        with store.connect() as c:
            return c.execute(
                "SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchone()

    age()
    before = state()
    with store.connect() as c:
        insert_record(
            c,
            tenant=tenant,
            source=source,
            parent="session",
            native="event-0002",
            text="Appended exact body",
            role="assistant",
            byte_start=30,
        )
        mark_logical_evidence_dirty(
            c,
            tenant_id=tenant,
            source_id=source,
            native_ids=["event-0002"],
            reason="ingest",
        )
    report = projector.project_pending(
        tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1
    )
    assert report["documents"] == 1 and report["failed"] == 0, report
    with store.connect() as c:
        assert (
            c.execute(
                "SELECT body_record_ordinal FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id='event-0002'",
                (tenant, source),
            ).fetchone()["body_record_ordinal"]
            is not None
        )
        assert not c.execute(
            "SELECT 1 FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        ).fetchone()
    archive.reads.clear()
    result = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert result["attempted_parents"] == 0, (
        "new publication bypassed 60s grace",
        result,
    )
    assert not archive.reads
    after = state()
    assert after["scope_epoch"] == before["scope_epoch"] + 1
    assert after["status"] == "pending" and after["manifest_artifact_id"] is None
    assert after["last_record_ordinal"] == -1 and after["enabled"]

    # Same-content repairs must retain their existing cooldown behavior too.
    age()
    before = state()
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND native_id='event-0002'",
            (tenant, source),
        )
        mark_logical_evidence_dirty(
            c,
            tenant_id=tenant,
            source_id=source,
            native_ids=["event-0002"],
            reason="ingest",
        )
    report = projector.project_pending(
        tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1
    )
    assert report["failed"] == 0 and report["repaired"] == 1, report
    archive.reads.clear()
    assert (
        runner.run_retirement(store, archive, scope=scope, apply=True)[
            "attempted_parents"
        ]
        == 0
    )
    assert not archive.reads and state()["scope_epoch"] == before["scope_epoch"] + 1
    age()
    result = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert result["cleared_documents"] == 3, result

    # Publishing another append cannot re-enable explicitly disabled progress.
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_chunk_retirement_progress SET enabled=false,status='disabled' WHERE tenant_id=%s AND source_id=%s",
            (tenant, source),
        )
    before = state()
    with store.connect() as c:
        insert_record(
            c,
            tenant=tenant,
            source=source,
            parent="session",
            native="event-0003",
            text="Disabled parent keeps this body",
            role="assistant",
            byte_start=40,
        )
        mark_logical_evidence_dirty(
            c,
            tenant_id=tenant,
            source_id=source,
            native_ids=["event-0003"],
            reason="ingest",
        )
    report = projector.project_pending(
        tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1
    )
    assert report["documents"] == 1 and report["failed"] == 0, report
    assert state() == before


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_runner_" + uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as c:
        c.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(
        make_conninfo(**(conninfo_to_dict(admin_dsn) | {"dbname": database}))
    )
    store.search_deadline_ms = 30000
    try:
        store.migrate()
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.dict(os.environ, RECALL_CHUNK_BODY_READS="archive"),
        ):
            scenario(store, Path(tmp))
            races(store, Path(tmp))
            paging_and_fairness(store, Path(tmp))
            publication_restarts_grace(store, Path(tmp))
        print(
            json.dumps(
                dict(
                    status="pass",
                    absent_restore_marker=True,
                    disabled_never_reenrolled=True,
                    bounded_resume=True,
                    read_parity=True,
                    owner_and_epoch_races=True,
                    stop_before_commit=True,
                    null_locators_reported=True,
                    publication_restarts_grace=True,
                )
            )
        )
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            c.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == "__main__":
    main()
