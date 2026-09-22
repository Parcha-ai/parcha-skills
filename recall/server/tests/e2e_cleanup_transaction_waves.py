#!/usr/bin/env python3
"""Actual PostgreSQL cleanup waves, locked deletion, and uncertain-commit replay."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(RECALL / 'server'))

import psycopg  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from e2e_logical_evidence_projection import insert_source  # noqa: E402


class TrackedStore:
    """Real connections/transactions; inject only a lost commit observation."""
    def __init__(self, store):
        self.store = store
        self.claims = []
        self.released = 0
        self.fail_commit = None

    @contextmanager
    def connect(self):
        with self.store.connect() as connection:
            yield TrackedConnection(self, connection)
        self.released += 1


class TrackedConnection:
    def __init__(self, owner, connection):
        self.owner, self.connection = owner, connection
        self.wave = None

    @contextmanager
    def transaction(self):
        with self.connection.transaction():
            yield
            if self.owner.fail_commit == (self.wave, 'before'):
                raise psycopg.OperationalError('synthetic commit outcome unavailable')
        if self.owner.fail_commit == (self.wave, 'after'):
            raise psycopg.OperationalError('synthetic commit outcome unavailable')

    def execute(self, query, values=()):
        cursor = self.connection.execute(query, values)
        if 'SELECT queue.*' not in query:
            return cursor
        rows = cursor.fetchall()
        xid = self.connection.execute('SELECT pg_current_xact_id()::text AS xid').fetchone()['xid']
        self.wave = len(self.owner.claims) + 1
        self.owner.claims.append((xid, [r['artifact_id'] for r in rows], self.owner.released))
        return type('Rows', (), {'fetchall': lambda _: rows})()


class HookProjection(LogicalEvidenceProjectionStore):
    def __init__(self, archive):
        super().__init__(archive)
        self.calls = []
        self.lock = threading.Lock()
        self.hook = lambda _: None

    def delete_reference(self, reference):
        with self.lock:
            self.calls.append(reference['artifact_id'])
        self.hook(reference)
        return super().delete_reference(reference)


class CleanupWaves(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import recall_server.logical_evidence_projection as owner
        assert Path(owner.__file__).resolve() == RECALL / 'server/recall_server/logical_evidence_projection.py'
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store._pool.close()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='recall-cleanup-wave-')
        self.addCleanup(self.tmp.cleanup)
        nonce = uuid.uuid4().hex
        self.tenant, self.source = 'tenant:wave:' + nonce, 'source:wave:' + nonce
        with self.store.connect() as connection:
            insert_source(connection, self.tenant, 'principal:' + nonce, self.source)
        self.archive = FilesystemArchiveStore(Path(self.tmp.name), namespace_key=b'synthetic-cleanup-wave-key-32-byte')
        self.projection = HookProjection(self.archive)
        self.tracked = TrackedStore(self.store)
        self.projector = CanonicalLogicalEvidenceProjector(self.tracked, self.projection, bound_tenant_id=self.tenant)

    def enqueue(self, count):
        refs = [self.archive.put_raw(tenant_id=self.tenant, source_id=self.source,
                native_id='record-' + str(i), payload=('wave object ' + str(i)).encode(),
                media_type='application/json', created_at='2026-09-22T00:00:00Z') for i in range(count)]
        with self.store.connect() as connection:
            self.projector._enqueue_cleanup(connection, tuple(refs))
            for i, ref in enumerate(refs):
                connection.execute('UPDATE canonical_evidence_cleanup_queue SET queued_at=\'2026-09-22\'::timestamptz + %s * interval \'1 second\' WHERE tenant_id=%s AND artifact_id=%s', (i, self.tenant, ref['artifact_id']))
        return refs

    def queued(self):
        with self.store.connect() as connection:
            return {r['artifact_id']: r['attempts'] for r in connection.execute('SELECT artifact_id,attempts FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()}

    def exists(self, ref):
        return (Path(self.tmp.name) / ref['object_key'] / 'data').exists()

    def test_each_concurrency_sized_wave_commits_and_releases(self):
        refs = self.enqueue(6)
        result = self.projector.drain_cleanup(limit=5, concurrency=2)
        claims = [c for c in self.tracked.claims if c[1]]
        self.assertEqual([len(c[1]) for c in claims], [2, 2, 1])
        self.assertEqual(len({c[0] for c in claims}), 3)
        self.assertEqual([c[2] for c in claims], [0, 1, 2])
        self.assertEqual((result['completed'], result['deleted'], result['pending']), (5, 5, 1))
        self.assertEqual(list(self.queued()), [refs[5]['artifact_id']])
        self.assertTrue(self.exists(refs[5]))

    def test_previous_wave_visible_and_concurrent_drain_skips_locked_wave(self):
        refs = self.enqueue(6)
        blocked = {r['artifact_id'] for r in refs[2:4]}
        entered, release = threading.Event(), threading.Event()
        def hook(ref):
            if ref['artifact_id'] in blocked:
                entered.set()
                if not release.wait(10):
                    raise TimeoutError('local test barrier expired')
        self.projection.hook = hook
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.projector.drain_cleanup, limit=4, concurrency=2)
            try:
                self.assertTrue(entered.wait(5))
                queued = self.queued()
                self.assertTrue(all(r['artifact_id'] not in queued for r in refs[:2]))
                with self.store.connect() as connection:
                    with self.assertRaises(psycopg.errors.LockNotAvailable):
                        connection.execute('SELECT artifact_id FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s AND artifact_id=%s FOR UPDATE NOWAIT', (self.tenant, refs[2]['artifact_id']))
                    connection.rollback()
                other = CanonicalLogicalEvidenceProjector(self.store, self.projection, bound_tenant_id=self.tenant)
                result = other.drain_cleanup(limit=2, concurrency=2)
                self.assertEqual(result['completed'], 2)
                self.assertTrue(all(not self.exists(r) for r in refs[4:]))
                self.assertTrue(all(self.exists(r) for r in refs[2:4]))
            finally:
                release.set()
            self.assertEqual(future.result(timeout=10)['completed'], 4)
        self.assertFalse(self.queued())
        self.assertEqual(len(self.projection.calls), len(set(self.projection.calls)))

    def test_failed_identity_attempted_once_then_later_invocation_retries(self):
        refs = self.enqueue(5)
        bad = refs[0]['artifact_id']
        def fail(ref):
            if ref['artifact_id'] == bad:
                raise OSError('synthetic unavailable object')
        self.projection.hook = fail
        result = self.projector.drain_cleanup(limit=5, concurrency=2)
        self.assertEqual((result['completed'], result['failures'], result['pending']), (4, 1, 1))
        self.assertEqual(self.queued(), {bad: 1})
        self.assertEqual(self.projection.calls.count(bad), 1)
        self.assertEqual(len(self.projection.calls), 5)
        self.projection.hook = lambda _: None
        self.assertEqual(self.projector.drain_cleanup(concurrency=2)['deleted'], 1)
        self.assertFalse(self.queued())

    def test_current_parquet_reference_is_protected_and_acknowledged(self):
        refs = self.enqueue(3)
        protected = refs[1]
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_parquet_scan_shards(
                tenant_id,source_id,bucket_start,dataset,generation_sha256,artifact_id,
                storage_backend,object_key,content_sha256,size_bytes,media_type,encryption,
                version_id,row_count,created_at)
                SELECT tenant_id,source_id,'2026-09-01','records',content_sha256,artifact_id,
                    storage_backend,object_key,content_sha256,size_bytes,'application/vnd.apache.parquet',
                    encryption,version_id,1,created_at
                FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s AND artifact_id=%s''', (self.tenant, protected['artifact_id']))
        result = self.projector.drain_cleanup(limit=3, concurrency=1)
        self.assertEqual((result['completed'], result['deleted'], result['failures'], result['pending']), (3, 2, 0, 0))
        self.assertNotIn(protected['artifact_id'], self.projection.calls)
        self.assertTrue(self.exists(protected))
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT artifact_id FROM canonical_parquet_scan_shards WHERE tenant_id=%s', (self.tenant,)).fetchone()['artifact_id'], protected['artifact_id'])

    def uncertain_commit(self, when):
        refs = self.enqueue(3)
        self.tracked.fail_commit = (2, when)
        with self.assertRaisesRegex(psycopg.OperationalError, 'synthetic commit outcome unavailable'):
            self.projector.drain_cleanup(limit=3, concurrency=1)
        self.assertFalse(self.exists(refs[0]))
        self.assertFalse(self.exists(refs[1]))
        self.assertTrue(self.exists(refs[2]))
        expected = refs[1:] if when == 'before' else refs[2:]
        self.assertEqual(set(self.queued()), {r['artifact_id'] for r in expected})
        self.tracked.fail_commit = None
        result = self.projector.drain_cleanup(limit=3, concurrency=1)
        self.assertEqual((result['completed'], result['deleted'], result['pending']), (len(expected), 1, 0))
        self.assertEqual(self.projection.calls.count(refs[0]['artifact_id']), 1)
        self.assertEqual(self.projection.calls.count(refs[1]['artifact_id']), 2 if when == 'before' else 1)

    def test_unknown_commit_before_ack_replays_idempotently(self):
        self.uncertain_commit('before')

    def test_lost_successful_commit_response_stops_and_pool_reuses(self):
        self.uncertain_commit('after')


if __name__ == '__main__':
    unittest.main()
