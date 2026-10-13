#!/usr/bin/env python3
# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Real PostgreSQL and local archive: finite enrollment preserves proof boundaries."""

import json
import contextlib
import hashlib
import io
import importlib
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import retire_current_chunks
from recall_server.retirement_campaign import Campaign
from recall_server.retirement_enrollment import (
    EnrollmentOwner,
    capture_inventory,
    capture_page,
    observe_enrollment,
)
from recall_server.retirement_jobs import package_body, decode_result


def prepare(store, root):
    tenant, source, archive, _, _, _ = fixture(store, root, count=2)
    scope = runner.RetirementScope(tenant, "principal:reprojection", (source,))
    with store.connect() as c:
        c.execute(
            "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
            (tenant, scope.principal_id, source),
        )
    return scope, archive, runner.plan_cohort(store, scope=scope, limit=1)


def scenario(store, root):
    scope, archive, plan = prepare(store, root)
    inventory = capture_inventory(store, scope=scope, deadline_at=time.monotonic() + 30)
    assert len(inventory["plans"]) == 1
    with Campaign(root / "campaign", inventory, inventory["sha256"]) as campaign:
        result = campaign.step(EnrollmentOwner(store, archive))
        assert result["outcome"]["status"] == "enrolled", result
        assert result["outcome"]["owner_result"]["archive_gets"] == 0
        assert not archive.reads, "metadata no-op read archive"
        assert campaign.report()["retirement_complete"] is None
    observation = observe_enrollment(
        store, inventory["plans"], deadline_at=time.monotonic() + 30
    )
    assert observation["parents"][0]["ledger"]["status"] == "pending"
    assert observation["archive_calls"] == 0 and not archive.reads
    # A read-only snapshot must refuse each unsafe current state, without changing it.
    for sql, code in (
        (
            "UPDATE canonical_chunk_retirement_progress SET enabled=false WHERE tenant_id=%s",
            "ledger_changed",
        ),
        (
            "UPDATE canonical_chunk_retirement_progress SET status='partial' WHERE tenant_id=%s",
            "ledger_changed",
        ),
        (
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s",
            "locator_missing",
        ),
        ("DELETE FROM canonical_source_grants WHERE tenant_id=%s", "owner_required"),
        (
            "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s",
            "catalog_changed",
        ),
    ):
        # Dedicated fixture keeps committed changes visible to the observer's own connection.
        test_scope, test_archive, test_plan = prepare(store, root)
        EnrollmentOwner(store, test_archive)(test_plan, lambda *args: None)
        with store.connect() as c:
            c.execute(sql, (test_scope.tenant_id,))
        try:
            observe_enrollment(store, [test_plan], deadline_at=time.monotonic() + 30)
        except ValueError as error:
            assert code in str(error), error
        else:
            raise AssertionError(code)
    # A second capture cannot reset a represented parent, whatever its state.
    excluded_page = capture_page(
        store, scope=scope, limit=1, deadline_at=time.monotonic() + 30
    )
    assert excluded_page["plans"] == [] and len(excluded_page["considered"]) == 1
    assert excluded_page["next_cursor"] == [scope.source_ids[0], "session"]
    assert excluded_page["excluded"] == ["session"]
    try:
        capture_inventory(store, scope=scope, deadline_at=time.monotonic() + 30)
    except ValueError as error:
        assert str(error) == "inventory_empty"
    else:
        raise AssertionError("represented parent recaptured")
    immediate = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert immediate["attempted_parents"] == 0, immediate
    assert not archive.reads, "cooldown bypassed"
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s",
            (scope.tenant_id,),
        )
    cleared = runner.run_retirement(store, archive, scope=scope, apply=True)
    assert cleared["cleared_documents"] == 2, cleared
    assert archive.reads, "unchanged full verifier did not read archive"
    observation = observe_enrollment(
        store, inventory["plans"], deadline_at=time.monotonic() + 30
    )
    ledger = observation["parents"][0]["ledger"]
    assert (
        ledger["status"] == "complete" and ledger["cumulative_cleared_documents"] == 2
    )
    assert observation["archive_calls"] == 0
    with store.connect() as c:
        row = c.execute(
            "SELECT count(*) AS n, sum(length(text_redacted)) AS bytes FROM canonical_chunks WHERE tenant_id=%s",
            (scope.tenant_id,),
        ).fetchone()
        assert row == dict(n=2, bytes=0), row

    # Missing locators go through the original publisher before absent-only enrollment.
    scope, archive, plan = prepare(store, root)
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s",
            (scope.tenant_id,),
        )
    result = EnrollmentOwner(store, archive)(plan, lambda *args: None)
    assert result["status"] == "enrolled", result
    assert result["owner_result"]["totals"]["published_documents"] == 2
    assert archive.reads

    # A restore before enrollment writes an explicit disabled marker.
    scope, archive, plan = prepare(store, root)
    with store.connect() as c:
        document = c.execute(
            "SELECT document_id FROM canonical_documents WHERE tenant_id=%s LIMIT 1",
            (scope.tenant_id,),
        ).fetchone()["document_id"]
    target = dict(
        tenant_id=scope.tenant_id,
        source_id=scope.source_ids[0],
        document_ids=(document,),
    )
    restore = retire_current_chunks(store, archive, **target, restore=True)
    retire_current_chunks(
        store,
        archive,
        **target,
        restore=True,
        apply=True,
        reviewed_plan=restore["plan"],
    )
    archive.reads.clear()
    result = EnrollmentOwner(store, archive)(plan, lambda *args: None)
    assert result["status"] == "disabled" and not archive.reads, result
    with store.connect() as c:
        assert (
            c.execute(
                "SELECT enabled FROM canonical_chunk_retirement_progress WHERE tenant_id=%s",
                (scope.tenant_id,),
            ).fetchone()["enabled"]
            is False
        )

    # A catalog recreation cannot reuse a previously frozen identity.
    scope, archive, plan = prepare(store, root)
    with store.connect() as c:
        c.execute(
            "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s",
            (scope.tenant_id,),
        )
    result = EnrollmentOwner(store, archive)(plan, lambda *args: None)
    assert result["status"] == "catalog_changed" and not archive.reads, result
    with store.connect() as c:
        assert not c.execute(
            "SELECT 1 FROM canonical_chunk_retirement_progress WHERE tenant_id=%s",
            (scope.tenant_id,),
        ).fetchone()


def packaged_job(store, root):
    scope, archive, plan = prepare(store, root)
    names = (
        "retirement_runner",
        "chunk_retirement",
        "parent_chunk_proof",
        "chunk_bodies",
        "streaming_locators",
        "locator_backfill_plan",
    )
    config = dict(
        commit="synthetic-commit",
        service_id="synthetic-service",
        owner_modules={
            "recall_server." + name: hashlib.sha256(
                Path(
                    importlib.import_module("recall_server." + name).__file__
                ).read_bytes()
            ).hexdigest()
            for name in names
        },
    )

    def run(payload):
        body, pin = package_body(config, payload)
        output = io.StringIO()
        with (
            patch.dict(
                os.environ,
                RENDER_GIT_COMMIT=config["commit"],
                RENDER_SERVICE_ID=config["service_id"],
                RECALL_CHUNK_BODY_READS="archive",
                RECALL_SEARCH_PLANE="turbopuffer",
                RECALL_TPUF_API_KEY="synthetic-test-key",
                RECALL_DATABASE_URL=store.dsn,
            ),
            patch(
                "recall_server.archive_runtime.build_evidence_archive_store",
                return_value=archive,
            ),
            contextlib.redirect_stdout(output),
            patch("recall_server.db.build_client", return_value=None),
        ):
            exec(compile(body, "test_packaged_render_body", "exec"), {})
        return decode_result(
            [json.loads(line) for line in output.getvalue().splitlines()], pin
        )

    # Same packaged body used by the remote transport, actual SQL and archive owner.
    page = run(
        dict(
            operation="capture",
            tenant=scope.tenant_id,
            principal=scope.principal_id,
            source=scope.source_ids[0],
            limit=100,
            after=None,
        )
    )
    assert len(page["plans"]) == 1 and not page["more"]
    result = run(dict(operation="enroll", plans=page["plans"]))
    assert result[0]["status"] == "enrolled" and not archive.reads, result
    observed = run(dict(operation="observe", plans=page["plans"]))
    assert observed["read_only"] and observed["archive_calls"] == 0
    assert observed["parents"][0]["ledger"]["status"] == "pending"
    assert not archive.reads


def main():
    admin = os.environ["RECALL_DATABASE_URL"]
    database = "recall_finite_" + uuid.uuid4().hex
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(
        make_conninfo(**(conninfo_to_dict(admin) | {"dbname": database}))
    )
    store.search_deadline_ms = 30000
    try:
        store.migrate()
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch.dict(os.environ, RECALL_CHUNK_BODY_READS="archive"),
        ):
            scenario(store, Path(temporary))
            packaged_job(store, Path(temporary))
        print(
            json.dumps(
                dict(
                    status="pass",
                    metadata_skips_archive=True,
                    clear_uses_full_proof=True,
                    missing_locators_published=True,
                    disabled_preserved=True,
                    changed_catalog_refused=True,
                    cooldown_preserved=True,
                )
            )
        )
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == "__main__":
    main()
