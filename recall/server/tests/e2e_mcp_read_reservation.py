#!/usr/bin/env python3
"""Real HTTP reads retain capacity while real PostgreSQL SyncRep commits wait."""

import contextlib
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server"), str(RECALL / "server/tests")]

import psycopg
from psycopg.rows import dict_row
from http.server import ThreadingHTTPServer

import e2e_retained_passage_receipts as fixture
from recall_server import app
from recall_server.db import BrainStore
from recall_server.authorization import POLICY_VERSION
from psycopg_pool import PoolTimeout


class ReadReservation(unittest.TestCase):
    ingest = fixture.RetainedPassageReceipts.ingest
    project = fixture.RetainedPassageReceipts.project

    def setUp(self):
        fixture.RetainedPassageReceipts.setUp(self)
        self.receipt = self.ingest(1)["receipt"]
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        organization = "org:" + uuid.uuid4().hex
        with self.store.connect() as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS reservation_commit_fixture(id text PRIMARY KEY)")
            connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Reservation fixture',now())", (organization,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company',%s)", (self.tenant, organization, organization.replace(":", "-")))
            connection.execute("INSERT INTO brain_memberships(organization_id,principal_id,role) VALUES (%s,%s,'owner')", (organization, self.principal))
            connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'owner')", (self.tenant, self.principal))
            connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'owner') ON CONFLICT DO NOTHING", (self.tenant, self.principal, self.source))
        self.credential = self.store.create_mcp_token("reservation-" + uuid.uuid4().hex,
            tenant_id=self.tenant, principal_id=self.principal)
        self.enterContext(patch.dict(os.environ, {
            "RECALL_AUTH_REQUIRED": "1", "RECALL_HTTP_PROFILE": "public-mcp",
            "RECALL_CANONICAL_V2_ENABLED": "1", "RECALL_CANONICAL_MCP_ENABLED": "1",
            "RECALL_EVIDENCE_ENABLED": "0", "RECALL_CHUNK_BODY_READS": "postgres",
            "RECALL_ADMIN_WEB_ENABLED": "0", "RECALL_DATABASE_POOL_MAX_SIZE": "8",
            "RECALL_MCP_READ_RESERVE": "2", "RECALL_AUDIT_BATCH_ROWS": "500",
            "RECALL_AUDIT_BATCH_SECONDS": "60",
        }))
        with patch.object(app, "build_archive_store", return_value=self.archive):
            app.configure_runtime(os.environ["RECALL_DATABASE_URL"])
        self.serving = app.Handler.store
        self.serving.search_plane = "turbopuffer"
        self.serving.turbopuffer = self.settings
        self.serving.turbopuffer_client = self.client
        self.addCleanup(self.serving.close)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def tool(self, name, arguments, *, token=None):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                           "params": {"name": name, "arguments": arguments}})
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=8)
        started = time.monotonic()
        try:
            connection.request("POST", "/mcp", body=body, headers={
                "Authorization": "Bearer " + (token or self.credential["token"]),
                "Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": "2025-11-25"})
            response = connection.getresponse()
            return response.status, json.loads(response.read()), (time.monotonic() - started) * 1000
        finally:
            connection.close()

    @contextlib.contextmanager
    def blocked_commits(self):
        # Disposable local fixture only. ALTER SYSTEM is never sent to a remote DSN.
        dsn = os.environ["RECALL_DATABASE_URL"]
        self.assertIn("127.0.0.1:", dsn)
        with self.serving.connect():
            pass
        expected = self.serving._pool.max_size
        control = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        pids, failures, threads = [], [], []
        mutex = threading.Lock()

        def writer():
            try:
                with self.serving.connect() as connection:
                    with mutex:
                        pids.append(connection.info.backend_pid)
                    connection.execute("SET LOCAL statement_timeout='25s'")
                    connection.execute("INSERT INTO reservation_commit_fixture VALUES (%s)", (uuid.uuid4().hex,))
            except Exception as error:
                failures.append(type(error).__name__)

        try:
            control.execute("ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (reservation_fixture_absent)'")
            control.execute("SELECT pg_reload_conf()")
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                if "reservation_fixture_absent" in control.execute("SHOW synchronous_standby_names").fetchone()["synchronous_standby_names"]:
                    break
                time.sleep(.02)
            for _ in range(expected):
                thread = threading.Thread(target=writer, daemon=True)
                threads.append(thread)
                thread.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                observed = control.execute("SELECT count(*) AS n FROM pg_stat_activity WHERE pid=ANY(%s) AND wait_event='SyncRep'", (list(pids),)).fetchone()["n"]
                if observed == expected:
                    break
                time.sleep(.02)
            self.assertEqual(observed, expected, {"expected": expected, "failures": failures})
            print(json.dumps({"phase": "real_syncrep", "blocked_general_commits": observed}), flush=True)
            yield control
        finally:
            control.execute("ALTER SYSTEM SET synchronous_standby_names=''")
            control.execute("SELECT pg_reload_conf()")
            for thread in threads:
                thread.join(timeout=5)
            control.close()
            self.assertTrue(all(not t.is_alive() for t in threads), "fixture commit did not release")
            self.assertEqual(failures, [])

    def test_http_show_survives_general_syncrep_commit_stall(self):
        before = self.tool("recall_show", {"target": self.receipt})
        self.assertEqual(before[0], 200)
        self.assertIn("result", before[1])
        self.serving.flush_authorization_audit()
        with self.blocked_commits():
            status, result, elapsed = self.tool("recall_show", {"target": self.receipt})
            print(json.dumps({"phase": "http_show", "status": status, "elapsed_ms": round(elapsed, 3)}), flush=True)
            self.assertEqual(status, 200, result)
            self.assertEqual(result, before[1])
            self.assertLess(elapsed, 2000)
        self.serving.flush_authorization_audit()

    def test_reserved_checkout_honors_caller_timeout_and_recovers(self):
        view = self.serving.read_only_view()
        with contextlib.ExitStack() as held:
            for _ in range(self.serving.reserved_read_connections):
                held.enter_context(view.connect())
            started = time.monotonic()
            with self.assertRaises(PoolTimeout):
                with view.connect(timeout=.05):
                    self.fail("exhausted reserved pool admitted a read")
            self.assertLess(time.monotonic() - started, 1)
        with view.connect(timeout=.5) as connection:
            self.assertTrue(connection.read_only)
            self.assertEqual(connection.execute("SELECT 1 AS value").fetchone()["value"], 1)

    def test_http_search_survives_general_syncrep_commit_stall(self):
        arguments = {"query": "Sapphire archive receipt", "limit": 10}
        before = self.tool("recall_search", arguments)
        self.assertEqual(before[0], 200)
        original = before[1]["result"]["structuredContent"]
        self.assertTrue(original["results"])
        self.serving.flush_authorization_audit()

        with self.blocked_commits():
            status, result, elapsed = self.tool("recall_search", arguments)
            print(json.dumps({"phase": "http_search", "status": status, "elapsed_ms": round(elapsed, 3)}), flush=True)
            self.assertEqual(status, 200, result)
            answer = result["result"]["structuredContent"]
            self.assertEqual(answer["diagnostics"]["authority_status"], "ok")
            self.assertEqual(answer["results"], original["results"])
            self.assertLess(elapsed, 2000)
        self.serving.flush_authorization_audit()

    def test_http_readiness_survives_general_stall_after_busy_grace(self):
        self.serving.readiness()
        self.serving._last_ready_at = time.monotonic() - self.serving.READINESS_BUSY_GRACE_SECONDS - 1
        with self.blocked_commits():
            connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=8)
            try:
                connection.request("GET", "/readyz")
                response = connection.getresponse()
                result = json.loads(response.read())
                self.assertEqual(response.status, 200, result)
                self.assertEqual(result["status"], "ready")
            finally:
                connection.close()

    def test_saturated_audit_fails_explicitly_without_disclosing_or_dropping_queued_rows(self):
        principal = dict(principal_kind="workload", principal_id=self.principal, tenant_id=self.tenant)
        capacity = self.serving.audit_batcher.queue_capacity
        with self.blocked_commits():
            for _ in range(capacity):
                self.serving.record_authorization_event(principal, action="mcp.fixture_queued", allowed=True,
                    reason="fixture", policy_version=POLICY_VERSION)
            self.assertEqual(len(self.serving.audit_batcher), capacity)
            status, result, elapsed = self.tool("recall_show", {"target": self.receipt})
            print(json.dumps({"phase": "audit_backpressure", "status": status, "elapsed_ms": round(elapsed, 3)}), flush=True)
            self.assertEqual(status, 503, result)
            self.assertEqual(result["error"], "brain_busy")
            self.assertNotIn("Sapphire", json.dumps(result))
            self.assertEqual(len(self.serving.audit_batcher), capacity)
        self.serving.flush_authorization_audit()
        with self.store.connect() as connection:
            actual = connection.execute("SELECT count(*) AS n FROM authorization_audit_events WHERE tenant_id=%s AND action='mcp.fixture_queued'", (self.tenant,)).fetchone()["n"]
        self.assertEqual(actual, capacity)

    def test_revoked_token_and_source_still_refuse_with_general_pool_stalled(self):
        revoked = self.store.create_mcp_token("revoked-" + uuid.uuid4().hex,
            tenant_id=self.tenant, principal_id=self.principal)
        self.assertTrue(self.store.revoke_mcp_token(revoked["name"]))
        with self.store.connect() as connection:
            connection.execute("DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s", (self.tenant, self.principal))
        with self.blocked_commits():
            status, result, _ = self.tool("recall_show", {"target": self.receipt}, token=revoked["token"])
            self.assertEqual(status, 401)
            status, result, _ = self.tool("recall_show", {"target": self.receipt})
            self.assertEqual(status, 200)
            self.assertIn("error", result)
            self.assertNotIn("Sapphire", json.dumps(result))

    def test_denial_audit_remains_general_and_no_durable_denial_is_invented(self):
        with self.blocked_commits():
            status, result, _ = self.tool("recall_forget", {"receipt": self.receipt})
            self.assertEqual(status, 503, result)
            self.assertEqual(result["error"], "brain_busy")
            self.assertNotIn("result", result)

    def test_concurrent_search_show_and_auth_share_reserved_capacity(self):
        self.serving.flush_authorization_audit()
        barrier = threading.Barrier(4)

        def request(index):
            barrier.wait(timeout=3)
            if index % 2:
                return self.tool("recall_search", {"query": "Sapphire archive receipt", "limit": 10})
            return self.tool("recall_show", {"target": self.receipt})

        with self.blocked_commits(), ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(request, range(4)))
            for index, (status, result, elapsed) in enumerate(results):
                self.assertEqual(status, 200, result)
                self.assertIn("result", result)
                self.assertLess(elapsed, 2000)
                if index % 2:
                    content = result["result"]["structuredContent"]
                    self.assertEqual(content["diagnostics"]["authority_status"], "ok")
                    self.assertTrue(content["results"])
            print(json.dumps({"phase": "concurrent_reads", "elapsed_ms": [round(r[2], 3) for r in results]}), flush=True)
        self.serving.flush_authorization_audit()

    def test_authorized_http_forget_uses_general_pool_and_revokes_reserved_reads(self):
        credential = self.store.create_mcp_token("forget-" + uuid.uuid4().hex,
            tenant_id=self.tenant, principal_id=self.principal, scopes=["read", "forget"])
        with self.blocked_commits():
            status, result, _ = self.tool("recall_forget", {"receipt": self.receipt}, token=credential["token"])
            self.assertEqual(status, 503, result)
            self.assertEqual(result["error"], "brain_busy")
        status, result, _ = self.tool("recall_forget", {"receipt": self.receipt}, token=credential["token"])
        self.assertEqual(status, 200, result)
        self.assertIn("result", result)
        status, result, _ = self.tool("recall_show", {"target": self.receipt})
        self.assertEqual(status, 200, result)
        self.assertNotIn("Sapphire", json.dumps(result))
        status, result, _ = self.tool("recall_search", {"query": "Sapphire archive receipt", "limit": 10})
        self.assertEqual(status, 200, result)
        self.assertEqual(result["result"]["structuredContent"]["results"], [])

    def test_read_only_exception_rollback_and_capacity_recover(self):
        with self.assertRaises(psycopg.errors.ReadOnlySqlTransaction):
            with self.serving.read_connection() as connection:
                connection.execute("INSERT INTO reservation_commit_fixture VALUES ('forbidden-read-write')")
        with self.assertRaisesRegex(RuntimeError, "fixture_cancel"):
            with self.serving.read_connection() as connection:
                connection.execute("SELECT 1")
                raise RuntimeError("fixture_cancel")
        with self.assertRaises(psycopg.errors.QueryCanceled):
            with self.serving.read_connection() as connection:
                connection.execute("SET LOCAL statement_timeout='20ms'")
                connection.execute("SELECT pg_sleep(1)")
        with self.serving.read_connection() as connection:
            self.assertEqual(connection.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"], "on")
        with self.serving.connect() as connection:
            self.assertEqual(connection.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"], "off")
            connection.execute("INSERT INTO reservation_commit_fixture VALUES (%s)", (uuid.uuid4().hex,))
        with self.assertRaises(AttributeError):
            self.serving.read_only_view().capture
        with contextlib.ExitStack() as stack:
            for _ in range(2):
                stack.enter_context(self.serving.read_connection())
            with self.assertRaises(PoolTimeout):
                with self.serving.read_connection():
                    self.fail("read pool exceeded its reservation")
        status, result, _ = self.tool("recall_show", {"target": self.receipt})
        self.assertEqual(status, 200, result)
        self.serving.flush_authorization_audit()
        for pool in (self.serving._pool, self.serving._read_pool):
            stats = pool.get_stats()
            self.assertEqual(stats["pool_available"], stats["pool_size"])
        self.assertEqual(self.serving._pool.max_size + self.serving._read_pool.max_size, 8)

    def test_background_constructor_does_not_inherit_http_reservation(self):
        worker = BrainStore(os.environ["RECALL_DATABASE_URL"], pool_max_size=8)
        try:
            self.assertEqual(worker.reserved_read_connections, 0)
            with worker.connect():
                pass
            self.assertEqual(worker._pool.max_size, 8)
            self.assertIsNone(worker._read_pool)
        finally:
            worker.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
