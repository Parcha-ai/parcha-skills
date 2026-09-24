#!/usr/bin/env python3
"""Real owning heartbeat: lock deadlines roll back both writes and release the pool."""
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path
import json
import io
import os
import sys
import time
import uuid
from unittest.mock import Mock

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_logical_evidence_projection import insert_source
from recall_server.db import BrainStore, SearchDeadlineExceeded
from recall_server.app import Handler


def report(version):
    return dict(schema_version=1, collector_kind="codex", collector_version=version,
                status="ready", scan_complete=True, pending_records=0, dead_records=0,
                coverage_percent=100, archive_coverage_percent=100, archive_backlog=0,
                last_success_epoch=None, last_error_code=f"synthetic_report_{version}")


def scenario(store, dsn):
    installation = uuid.uuid4()
    with store.connect() as con:
        insert_source(con, "tenant", "owner", "source")
        con.execute("INSERT INTO brain_organizations VALUES('organization','personal','synthetic',now())")
        con.execute("INSERT INTO brain_spaces VALUES('tenant','organization','personal','synthetic',now())")
        con.execute("INSERT INTO brain_access_grants VALUES('tenant','owner','owner',now())")
        con.execute("""INSERT INTO connector_installations(
            id,tenant_id,principal_id,connector_id,source_id,execution,state,privacy_mode,device_id)
            VALUES(%s,'tenant','owner','local.codex','source','source_local','enabled','scrub','synthetic')""",
                    (installation,))

    # A successful upload proves transfer, not complete local coverage. Local
    # installations need a heartbeat before the fleet may label them ready.
    with store.connect() as con:
        con.execute("UPDATE connector_installations SET last_success_at=now() WHERE id=%s", (installation,))
    assert store.fleet_status(["tenant"])[0]["health"] == "unknown"
    ready = report(1) | {"last_error_code": None}
    store.record_collector_health(tenant_id="tenant", source_id="source",
                                  installation_id=installation, report=ready)
    assert store.fleet_status(["tenant"])[0]["health"] == "ready"
    with store.connect() as con:
        con.execute("UPDATE collector_health_reports SET reported_at=now()-interval '3 minutes'")
    assert store.fleet_status(["tenant"])[0]["health"] == "stale"

    def heartbeat(version):
        return store.record_collector_health(tenant_id="tenant", source_id="source",
                                            installation_id=installation, report=report(version))

    def handler_heartbeat(version):
        handler = object.__new__(Handler)
        body = json.dumps(dict(tenant_id="tenant", principal_id="owner",
                               source_id="source", report=report(version))).encode()
        handler.path = "/v2/collector/health"
        handler.rfile = io.BytesIO(body)
        handler.hide_non_public_route = Mock(return_value=False)
        handler.admin_web_enabled = Mock(return_value=False)
        handler.require = Mock(return_value={"installation_id": installation})
        handler.body_length = Mock(return_value=len(body))
        handler.canonical_authority = Mock(return_value=("tenant", "owner", "source"))
        handler.store = store
        handler.send_json = Mock()
        Handler.do_POST(handler)
        handler.send_json.assert_called_once()
        return handler.send_json.call_args.args

    def state(con):
        return (
            con.execute("SELECT * FROM collector_health_reports WHERE tenant_id='tenant' AND source_id='source'").fetchone(),
            con.execute("SELECT last_success_at,last_error_code,updated_at FROM connector_installations WHERE id=%s",
                        (installation,)).fetchone(),
        )

    results = []
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as monitor:
        for stage in ("health_upsert", "installation_update"):
            assert heartbeat(1)["status"] == "accepted"
            with store.connect() as con:
                pooled_pid = con.info.backend_pid
            prior = state(monitor)
            executor = ThreadPoolExecutor(max_workers=1)
            with psycopg.connect(dsn, row_factory=dict_row) as blocker:
                blocker.execute("SET idle_in_transaction_session_timeout='15s'")
                if stage == "health_upsert":
                    blocker.execute("SELECT source_id FROM collector_health_reports WHERE tenant_id='tenant' AND source_id='source' FOR UPDATE")
                else:
                    blocker.execute("SELECT id FROM connector_installations WHERE id=%s FOR NO KEY UPDATE", (installation,))
                started = time.monotonic()
                future = executor.submit(handler_heartbeat if stage == "installation_update" else heartbeat, 2)
                try:
                    wait_end = time.monotonic() + 2
                    while time.monotonic() < wait_end:
                        waiting = monitor.execute("SELECT pg_blocking_pids(%s) AS blockers", (pooled_pid,)).fetchone()
                        if blocker.info.backend_pid in waiting["blockers"]:
                            break
                        time.sleep(.005)
                    else:
                        raise AssertionError("expected heartbeat lock wait not witnessed")
                    # The second-statement case already performed its UPSERT;
                    # neither statement may become visible before commit.
                    assert state(monitor) == prior
                    try:
                        response = future.result(timeout=6)
                    except SearchDeadlineExceeded:
                        assert stage == "health_upsert"
                    except FutureTimeout as error:
                        raise AssertionError("heartbeat SQL exceeded shared five-second deadline") from error
                    else:
                        assert stage == "installation_update"
                        assert response == (503, {"error": "collector health unavailable"})
                    elapsed = time.monotonic() - started
                    assert 4 <= elapsed < 7, elapsed
                    assert state(monitor) == prior
                    with store.connect() as con:
                        assert con.info.backend_pid == pooled_pid
                        assert con.execute("SELECT 1 AS n").fetchone()["n"] == 1
                        assert con.execute("SHOW statement_timeout").fetchone()["statement_timeout"] == "0"
                    assert monitor.execute("SELECT pg_blocking_pids(%s) AS blockers", (pooled_pid,)).fetchone()["blockers"] == []
                    results.append(dict(stage=stage,elapsed_ms=round(elapsed*1000),atomic_rollback=True,pool_reused=True))
                finally:
                    blocker.rollback()
                    executor.shutdown(wait=True, cancel_futures=True)
            status, accepted = handler_heartbeat(3)
            assert status == 202 and accepted["status"] == "accepted"
            health, installed = state(monitor)
            assert health["collector_version"] == 3 and installed["last_error_code"] == "synthetic_report_3"
    return results


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "collector_health_deadline_" + uuid.uuid4().hex[:12]
    with psycopg.connect(admin_dsn, autocommit=True) as con:
        con.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    dsn = make_conninfo(**(conninfo_to_dict(admin_dsn) | {"dbname": database}))
    store = None
    try:
        store = BrainStore(dsn, pool_max_size=4)
        store.migrate()
        # Constrain only this test pool so reuse must include the exact backend
        # that was canceled, rather than another idle connection.
        store._pool.close()
        store._pool = ConnectionPool(dsn, min_size=1, max_size=1,
                                     kwargs={"row_factory": dict_row}, timeout=5)
        print(json.dumps(dict(status="passed",cases=scenario(store, dsn))))
    finally:
        if store is not None:
            store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as con:
            con.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(database)))


if __name__ == "__main__":
    main()
