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


def claim_unknown_state_recovery(store, root):
    """A new invocation recovers from state, not an invented old claim ACK.

    Exercise the live runner API unchanged. The injected errors happen after
    real PostgreSQL UPDATE work, either inside its transaction or after COMMIT.
    Fixture-only timestamp aging avoids a minute's sleep; no recovery operation
    resets a production epoch, cursor, enable flag or ledger.
    """
    for outcome, changed_manifest in (
        ("rollback", False),
        ("commit_reply_lost", False),
        ("rollback", True),
        ("commit_reply_lost", True),
    ):
        tenant, source, archive, _, projector, _ = fixture(store, root, count=4)
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

        def state():
            with store.connect() as c:
                return c.execute(
                    "SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s",
                    (tenant, source),
                ).fetchone()

        def age():
            # Test time advancement only, never a recovery prescription.
            with store.connect() as c:
                c.execute(
                    "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
                    (tenant, source),
                )

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

        expected_reads = reads()
        age()
        first = runner.run_retirement(
            store,
            archive,
            scope=scope,
            apply=True,
            parent_limits=ParentRetirementLimits(batch_documents=1, max_batches=1),
        )
        assert first["cleared_documents"] == first["partial_parents"] == 1, first
        age()
        before = state()
        old_plan = page["parents"][0]["manifest"]
        original_claim = runner._claim
        original_query = store._execute_bounded
        lost = []
        archive.reads.clear()

        def rollback_after_update(connection, sql, values, deadline_at):
            cursor = original_query(connection, sql, values, deadline_at)
            if "SET scope_epoch=scope_epoch+1" in sql:
                lost.append(cursor.fetchone()["scope_epoch"])
                raise TimeoutError("synthetic claim reply lost before COMMIT")
            return cursor

        def lose_committed_reply(*args, **kwargs):
            candidate = original_claim(*args, **kwargs)
            assert candidate is not None
            lost.append(candidate["scope_epoch"])
            raise TimeoutError("synthetic claim reply lost after COMMIT")

        context = (
            patch.object(store, "_execute_bounded", side_effect=rollback_after_update)
            if outcome == "rollback"
            else patch.object(runner, "_claim", side_effect=lose_committed_reply)
        )
        with context:
            try:
                runner.run_retirement(store, archive, scope=scope, apply=True)
            except TimeoutError:
                pass
            else:
                raise AssertionError("unknown first claim was silently accepted")
        assert len(lost) == 1 and not archive.reads
        after = state()
        assert lost[0] == before["scope_epoch"] + 1
        assert after["scope_epoch"] == before["scope_epoch"] + int(
            outcome == "commit_reply_lost"
        )
        for field in (
            "status",
            "last_record_ordinal",
            "manifest_artifact_id",
            "cumulative_cleared_documents",
            "cumulative_cleared_chunks",
            "cumulative_cleared_utf8_bytes",
        ):
            assert after[field] == before[field], (outcome, field)
        if outcome == "rollback":
            assert after["updated_at"] == before["updated_at"]
        else:
            assert after["updated_at"] > before["updated_at"]
            assert (
                runner.run_retirement(store, archive, scope=scope, apply=True)[
                    "attempted_parents"
                ]
                == 0
            )
            assert not archive.reads, "lost committed claim bypassed cooldown"

        assert reads() == expected_reads
        archive.reads.clear()

        # Existing disabled intent and queue exclusion still win after either
        # unknown outcome. These fixture toggles test the guard, not a reset API.
        age()
        with store.connect() as c:
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET enabled=false WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            )
        disabled = state()
        assert (
            runner.run_retirement(store, archive, scope=scope, apply=True)[
                "attempted_parents"
            ]
            == 0
        )
        assert state() == disabled and not archive.reads
        with store.connect() as c:
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET enabled=true WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            )
        if changed_manifest:
            with store.connect() as c:
                insert_record(
                    c,
                    tenant=tenant,
                    source=source,
                    parent="session",
                    native="event-0004",
                    text="New revision after unknown claim",
                    role="assistant",
                    byte_start=40,
                )
                mark_logical_evidence_dirty(
                    c,
                    tenant_id=tenant,
                    source_id=source,
                    native_ids=["event-0004"],
                    reason="ingest",
                )
            queued = state()
            assert (
                runner.run_retirement(store, archive, scope=scope, apply=True)[
                    "attempted_parents"
                ]
                == 0
            )
            assert state() == queued and not archive.reads
            projected = projector.project_pending(
                tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1
            )
            assert projected["documents"] == 1 and projected["failed"] == 0, projected
            current_page = runner.plan_cohort(store, scope=scope)
            assert current_page["parents"][0]["manifest"] != old_plan
            expected_reads = reads()  # Exact new-manifest public reads before recovery.
            archive.reads.clear()
            assert (
                runner.run_retirement(store, archive, scope=scope, apply=True)[
                    "attempted_parents"
                ]
                == 0
            )
            assert not archive.reads, "changed manifest bypassed publication grace"
        else:
            current_page = runner.plan_cohort(store, scope=scope)
            assert current_page["parents"][0]["manifest"] == old_plan
            # No publication/reset intervenes: the original partial cursor and
            # durable counters must drive recovery from the actual old state.
            unchanged = state()
            for field in (
                "scope_epoch",
                "last_record_ordinal",
                "manifest_artifact_id",
                "status",
            ):
                assert unchanged[field] == after[field]
        age()
        fresh_before = state()
        original_retire = runner.retire_parent_chunks
        stale_checks = []
        # An uncommitted epoch number can legitimately be reused after rollback.
        # Fence only the last actually committed old epoch. The old invocation
        # has terminated; its unreturned candidate is never a live authority.
        stale_epoch = before["scope_epoch"] if outcome == "rollback" else lost[0]

        def assert_old_epoch_fenced(*args, **kwargs):
            assert kwargs["required_scope_epoch"] > stale_epoch
            assert (
                kwargs["reviewed_plan"]["manifest"]
                == current_page["parents"][0]["manifest"]
            )
            prior_reads = dict(archive.reads)
            try:
                original_retire(*args, **dict(kwargs, required_scope_epoch=stale_epoch))
            except ChunkRetirementError as error:
                assert error.error_code == "parent_retirement_disabled"
            else:
                raise AssertionError("stale retained claim cleared a body")
            assert dict(archive.reads) == prior_reads
            stale_checks.append(True)
            return original_retire(*args, **kwargs)

        with patch.object(
            runner, "retire_parent_chunks", side_effect=assert_old_epoch_fenced
        ):
            recovered = runner.run_retirement(store, archive, scope=scope, apply=True)
        assert stale_checks == [True] and recovered["completed_parent_proofs"] == 1
        assert (
            recovered["cleared_documents"] == 3 + int(changed_manifest)
            and recovered["failed_parents"] == 0
        ), recovered
        assert not recovered["commit_outcome_unknown"]
        assert archive.reads and all(n == 1 for n in archive.reads.values()), (
            "new invocation did not make one fresh proof"
        )
        final = state()
        assert final["scope_epoch"] == fresh_before["scope_epoch"] + 1
        assert final["status"] == "complete" and final[
            "cumulative_cleared_documents"
        ] == 4 + int(changed_manifest)
        for field in ("cleared_documents", "cleared_chunks", "cleared_utf8_bytes"):
            assert final["cumulative_" + field] == first[field] + recovered[field]
        archive.reads.clear()
        assert (
            runner.run_retirement(store, archive, scope=scope, apply=True)[
                "attempted_parents"
            ]
            == 0
        )
        assert not archive.reads and state() == final, (
            "completed current manifest was recounted"
        )
        assert reads() == expected_reads


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
            claim_unknown_state_recovery(store, Path(tmp))
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
                    claim_unknown_state_recovery=True,
                )
            )
        )
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as c:
            c.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == "__main__":
    main()
