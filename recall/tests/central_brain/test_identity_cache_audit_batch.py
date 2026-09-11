"""Unit coverage for the identity-write cache and batched authorization audit.

Both features exist because Recall is write-dominated: every collector write
re-ran the identity registration statements, and every MCP request wrote its
audit row synchronously. These tests use fake connections so they run with no
database; the e2e scripts cover the real PostgreSQL path.
"""

from __future__ import annotations

import contextlib
import os
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = types.ModuleType("psycopg")
    psycopg_rows = types.ModuleType("psycopg.rows")
    psycopg_rows.dict_row = object()
    psycopg.rows = psycopg_rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = psycopg_rows

from recall_server import identity_cache
from recall_server.audit_batch import (
    AUTHORIZATION_AUDIT_INSERT,
    AuthorizationAuditBatcher,
    batch_settings_from_env,
)
from recall_server.canonical import CanonicalLifecycleError, CanonicalPlane
from recall_server.db import BrainStore
from recall_server.identity_cache import IdentityRegistrationCache

TENANT = "tenant:synthetic:cache"
PRINCIPAL = "principal:synthetic:owner"
SOURCE = "source:synthetic:laptop"
KEY = (TENANT, PRINCIPAL, SOURCE)

PRINCIPAL_ROW = {
    "principal_kind": "human",
    "principal_id": PRINCIPAL,
    "tenant_id": TENANT,
}


class Result:
    def __init__(self, *, rowcount: int = 1, one=None):
        self.rowcount = rowcount
        self._one = one

    def fetchone(self):
        return self._one

    def fetchall(self):
        return [] if self._one is None else [self._one]


class RegistrationConnection:
    """Fake connection for register_source with a controllable source row."""

    def __init__(self, *, source_exists: bool = True, owner: str = PRINCIPAL):
        self.source_exists = source_exists
        self.owner = owner
        self.statements: list[tuple[str, tuple]] = []

    def transaction(self):
        return contextlib.nullcontext()

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.statements.append((normalized, params))
        if normalized.startswith("INSERT INTO canonical_sources"):
            return Result(rowcount=0 if self.source_exists else 1)
        if normalized.startswith("SELECT owner_principal_id"):
            return Result(one={"owner_principal_id": self.owner})
        return Result()


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def executemany(self, sql, rows):
        self.connection.record(sql, list(rows))


class AuditConnection:
    """Fake pooled connection that records audit inserts and their timing."""

    def __init__(self, *, fail: bool = False):
        self.writes: list[tuple[str, list]] = []
        self.fail = fail
        self.lock = threading.Lock()
        self.threads: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def transaction(self):
        return contextlib.nullcontext()

    def cursor(self):
        return Cursor(self)

    def record(self, sql, rows):
        if self.fail:
            raise RuntimeError("synthetic database outage")
        with self.lock:
            self.writes.append((" ".join(sql.split()), rows))
            self.threads.append(threading.current_thread().name)

    def execute(self, sql, params=None):
        self.record(sql, [params])
        return Result()

    @property
    def rows(self):
        with self.lock:
            return [row for _sql, rows in self.writes for row in rows]


def audit_store(connection, **kwargs) -> BrainStore:
    store = BrainStore("postgresql://synthetic.invalid/db", **kwargs)
    store.connect = lambda: connection  # type: ignore[method-assign]
    store.audit_batcher._connect = store.connect
    return store


def wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class IdentityRegistrationCacheTest(unittest.TestCase):
    def test_register_source_cache_hit_runs_zero_statements(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        first = RegistrationConnection(source_exists=True)
        CanonicalPlane.register_source(
            first,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(len(first.statements), 6)
        self.assertIn(KEY, [key for key in cache._entries])

        second = RegistrationConnection(source_exists=True)
        CanonicalPlane.register_source(
            second,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(second.statements, [])
        self.assertEqual(cache.hits, 1)

    def test_cache_key_is_the_full_identity_tuple(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        CanonicalPlane.register_source(
            RegistrationConnection(),
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        for tenant_id, principal_id, source_id in (
            ("tenant:synthetic:other", PRINCIPAL, SOURCE),
            (TENANT, "principal:synthetic:other", SOURCE),
            (TENANT, PRINCIPAL, "source:synthetic:other"),
        ):
            conn = RegistrationConnection(owner=principal_id)
            CanonicalPlane.register_source(
                conn,
                tenant_id=tenant_id,
                principal_id=principal_id,
                source_id=source_id,
                cache=cache,
            )
            self.assertEqual(len(conn.statements), 6)

    def test_foreign_principal_is_still_forbidden_after_owner_cached(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        CanonicalPlane.register_source(
            RegistrationConnection(),
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        foreign = RegistrationConnection(owner=PRINCIPAL)
        with self.assertRaises(CanonicalLifecycleError) as raised:
            CanonicalPlane.register_source(
                foreign,
                tenant_id=TENANT,
                principal_id="principal:synthetic:intruder",
                source_id=SOURCE,
                cache=cache,
            )
        self.assertEqual(raised.exception.error_code, "canonical_authority_forbidden")
        self.assertEqual(len(cache), 1)
        self.assertFalse(
            cache.contains((TENANT, "principal:synthetic:intruder", SOURCE))
        )

    def test_new_source_is_not_cached_until_a_later_committed_write(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        fresh = RegistrationConnection(source_exists=False)
        CanonicalPlane.register_source(
            fresh,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(len(fresh.statements), 6)
        self.assertEqual(len(cache), 0)
        repeat = RegistrationConnection(source_exists=True)
        CanonicalPlane.register_source(
            repeat,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(len(repeat.statements), 6)
        self.assertEqual(len(cache), 1)

    def test_register_source_cache_invalidated_on_membership_change(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        other_tenant = "tenant:synthetic:untouched"
        for tenant_id in (TENANT, other_tenant):
            CanonicalPlane.register_source(
                RegistrationConnection(),
                tenant_id=tenant_id,
                principal_id=PRINCIPAL,
                source_id=SOURCE,
                cache=cache,
            )
        self.assertEqual(len(cache), 2)

        with mock.patch.object(identity_cache, "REGISTRATION_CACHE", cache):
            removed = identity_cache.invalidate_tenant(TENANT)
        self.assertEqual(removed, 1)
        self.assertFalse(cache.contains(KEY))
        self.assertTrue(cache.contains((other_tenant, PRINCIPAL, SOURCE)))

        after = RegistrationConnection()
        CanonicalPlane.register_source(
            after,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(len(after.statements), 6)

    def test_membership_mutations_invalidate_through_store_hooks(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=10)
        cache.remember(KEY)
        calls: list[str] = []

        class ProvisionConnection:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def transaction(self):
                return contextlib.nullcontext()

            def execute(self, sql, params=None):
                normalized = " ".join(sql.split())
                if normalized.startswith("SELECT actor_id"):
                    return Result(one={"actor_id": "actor:synthetic"})
                if normalized.startswith("SELECT organization.organization_kind"):
                    return Result(
                        one={
                            "organization_kind": "company",
                            "brain_kind": "company",
                            "organization_id": "org:synthetic",
                            "slug": "synthetic",
                        }
                    )
                return Result()

        store = audit_store(ProvisionConnection(), audit_batch_rows=0)
        with mock.patch.object(identity_cache, "REGISTRATION_CACHE", cache), \
                mock.patch(
                    "recall_server.db.invalidate_registration_cache",
                    side_effect=lambda tenant: calls.append(tenant)
                    or identity_cache.invalidate_tenant(tenant),
                ):
            store.provision_brain(
                organization_id="org:synthetic",
                organization_kind="company",
                display_name="Synthetic",
                tenant_id=TENANT,
                brain_kind="company",
                slug="synthetic",
                owner_principal_id=PRINCIPAL,
            )
        self.assertEqual(calls, [TENANT])
        self.assertEqual(len(cache), 0)

    def test_cache_ttl_expiry_forces_reregistration(self) -> None:
        now = [1_000.0]
        cache = IdentityRegistrationCache(
            ttl_seconds=600, max_entries=10, clock=lambda: now[0]
        )
        CanonicalPlane.register_source(
            RegistrationConnection(),
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        now[0] += 599.0
        self.assertTrue(cache.contains(KEY))
        now[0] += 1.0
        self.assertFalse(cache.contains(KEY))
        self.assertEqual(len(cache), 0)
        expired = RegistrationConnection()
        CanonicalPlane.register_source(
            expired,
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            source_id=SOURCE,
            cache=cache,
        )
        self.assertEqual(len(expired.statements), 6)
        self.assertTrue(cache.contains(KEY))

    def test_zero_ttl_disables_cache(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=0, max_entries=10)
        self.assertFalse(cache.enabled)
        for _ in range(2):
            conn = RegistrationConnection()
            CanonicalPlane.register_source(
                conn,
                tenant_id=TENANT,
                principal_id=PRINCIPAL,
                source_id=SOURCE,
                cache=cache,
            )
            self.assertEqual(len(conn.statements), 6)
        self.assertEqual(len(cache), 0)

    def test_cache_evicts_least_recently_used_at_capacity(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=2)
        first = (TENANT, PRINCIPAL, "source:a")
        second = (TENANT, PRINCIPAL, "source:b")
        third = (TENANT, PRINCIPAL, "source:c")
        cache.remember(first)
        cache.remember(second)
        self.assertTrue(cache.contains(first))
        cache.remember(third)
        self.assertEqual(len(cache), 2)
        self.assertFalse(cache.contains(second))
        self.assertTrue(cache.contains(first))
        self.assertTrue(cache.contains(third))

    def test_cache_settings_come_from_env(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_IDENTITY_CACHE_TTL_SECONDS": "0",
                "RECALL_IDENTITY_CACHE_MAX": "7",
            },
        ):
            cache = IdentityRegistrationCache.from_env()
        self.assertFalse(cache.enabled)
        self.assertEqual(cache.max_entries, 7)
        with mock.patch.dict(os.environ, {"RECALL_IDENTITY_CACHE_MAX": "0"}):
            with self.assertRaises(ValueError):
                IdentityRegistrationCache.from_env()

    def test_cache_is_safe_under_concurrent_writers(self) -> None:
        cache = IdentityRegistrationCache(ttl_seconds=600, max_entries=100)
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                for step in range(200):
                    key = (TENANT, PRINCIPAL, f"source:{(index * step) % 150}")
                    cache.remember(key)
                    cache.contains(key)
                    if step % 50 == 0:
                        cache.invalidate_tenant(TENANT)
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertLessEqual(len(cache), 100)


class AuthorizationAuditBatchTest(unittest.TestCase):
    def test_denied_audit_is_written_synchronously(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=500, audit_batch_seconds=60)
        store.record_authorization_event(
            PRINCIPAL_ROW,
            action="mcp.recall_search",
            allowed=False,
            reason="tenant_forbidden",
            policy_version="recall.authorization.v1",
        )
        self.assertEqual(len(connection.rows), 1)
        self.assertEqual(connection.rows[0][4], "denied")
        self.assertEqual(connection.threads, [threading.current_thread().name])
        self.assertEqual(len(store.audit_batcher), 0)
        self.assertIn("authorization_audit_events", connection.writes[0][0])

    def test_allowed_audit_is_batched_and_flushed(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=500, audit_batch_seconds=60)
        for index in range(3):
            store.record_authorization_event(
                PRINCIPAL_ROW,
                action="mcp.recall_search",
                allowed=True,
                reason=f"allowed_{index}",
                policy_version="recall.authorization.v1",
            )
        self.assertEqual(connection.rows, [])
        self.assertEqual(len(store.audit_batcher), 3)

        self.assertEqual(store.flush_authorization_audit(), 3)
        self.assertEqual(len(store.audit_batcher), 0)
        self.assertEqual(len(connection.writes), 1)
        rows = connection.rows
        self.assertEqual([row[4] for row in rows], ["allowed"] * 3)
        self.assertEqual([row[5] for row in rows], ["allowed_0", "allowed_1", "allowed_2"])
        self.assertEqual(
            [row[:4] for row in rows],
            [("human", PRINCIPAL, TENANT, "mcp.recall_search")] * 3,
        )
        self.assertEqual(store.flush_authorization_audit(), 0)

    def test_row_threshold_triggers_background_flush(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=2, audit_batch_seconds=60)
        try:
            for index in range(2):
                store.record_authorization_event(
                    PRINCIPAL_ROW,
                    action="mcp.recall_show",
                    allowed=True,
                    reason="allowed",
                    policy_version="recall.authorization.v1",
                )
            self.assertTrue(wait_until(lambda: len(connection.rows) == 2))
            self.assertEqual(len(store.audit_batcher), 0)
            self.assertEqual(connection.threads, ["recall-audit-flush"] * 1)
        finally:
            store.close()

    def test_time_threshold_triggers_background_flush(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=500, audit_batch_seconds=0.05)
        try:
            store.record_authorization_event(
                PRINCIPAL_ROW,
                action="mcp.recall_show",
                allowed=True,
                reason="allowed",
                policy_version="recall.authorization.v1",
            )
            self.assertTrue(wait_until(lambda: len(connection.rows) == 1))
            self.assertEqual(connection.threads, ["recall-audit-flush"])
        finally:
            store.close()

    def test_queue_overflow_falls_back_to_synchronous_write(self) -> None:
        connection = AuditConnection()
        batcher = AuthorizationAuditBatcher(
            lambda: connection, batch_rows=2, batch_seconds=60, queue_capacity=2
        )
        # Keep the daemon flusher out of this test so the overflow path is
        # exercised deterministically rather than racing a threshold flush.
        batcher._ensure_thread_locked = lambda: None
        row = ("human", PRINCIPAL, TENANT, "mcp.recall_search", "allowed", "ok", "v")
        self.assertTrue(batcher.enqueue(row))
        self.assertTrue(batcher.enqueue(row))
        self.assertFalse(batcher.enqueue(row))
        self.assertEqual(len(connection.rows), 1)
        self.assertEqual(batcher.overflow_sync_total, 1)
        self.assertEqual(batcher.flush(), 2)
        self.assertEqual(len(connection.rows), 3)
        batcher.close()

    def test_failed_flush_keeps_rows_queued(self) -> None:
        connection = AuditConnection(fail=True)
        batcher = AuthorizationAuditBatcher(
            lambda: connection, batch_rows=100, batch_seconds=60
        )
        row = ("human", PRINCIPAL, TENANT, "mcp.recall_search", "allowed", "ok", "v")
        batcher.enqueue(row)
        with self.assertRaises(RuntimeError):
            batcher.flush()
        self.assertEqual(len(batcher), 1)
        self.assertEqual(batcher.flush_failures, 1)
        connection.fail = False
        self.assertEqual(batcher.flush(), 1)
        self.assertEqual(connection.rows, [row])
        batcher.close()

    def test_store_close_flushes_queue_before_pool_close(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=500, audit_batch_seconds=60)
        store.record_authorization_event(
            PRINCIPAL_ROW,
            action="mcp.recall_search",
            allowed=True,
            reason="allowed",
            policy_version="recall.authorization.v1",
        )
        store.close()
        self.assertEqual(len(connection.rows), 1)
        # After close, nothing queues: the row is written synchronously.
        store.record_authorization_event(
            PRINCIPAL_ROW,
            action="mcp.recall_search",
            allowed=True,
            reason="late",
            policy_version="recall.authorization.v1",
        )
        self.assertEqual(len(connection.rows), 2)
        self.assertEqual(len(store.audit_batcher), 0)

    def test_zero_batch_rows_disables_batching(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=0)
        store.record_authorization_event(
            PRINCIPAL_ROW,
            action="mcp.recall_search",
            allowed=True,
            reason="allowed",
            policy_version="recall.authorization.v1",
        )
        self.assertEqual(len(connection.rows), 1)
        self.assertEqual(connection.writes[0][0], " ".join(AUTHORIZATION_AUDIT_INSERT.split()))

    def test_batch_settings_come_from_env(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"RECALL_AUDIT_BATCH_ROWS": "50", "RECALL_AUDIT_BATCH_SECONDS": "0.5"},
        ):
            self.assertEqual(batch_settings_from_env(), (50, 0.5))
        with mock.patch.dict(os.environ, {"RECALL_AUDIT_BATCH_SECONDS": "0"}):
            with self.assertRaises(ValueError):
                batch_settings_from_env()
        with mock.patch.dict(os.environ, {"RECALL_AUDIT_BATCH_ROWS": "-1"}):
            with self.assertRaises(ValueError):
                batch_settings_from_env()

    def test_invalid_identity_still_rejected_before_queueing(self) -> None:
        connection = AuditConnection()
        store = audit_store(connection, audit_batch_rows=500, audit_batch_seconds=60)
        with self.assertRaises(ValueError):
            store.record_authorization_event(
                {**PRINCIPAL_ROW, "tenant_id": ""},
                action="mcp.recall_search",
                allowed=True,
                reason="allowed",
                policy_version="recall.authorization.v1",
            )
        self.assertEqual(len(store.audit_batcher), 0)


if __name__ == "__main__":
    unittest.main()
