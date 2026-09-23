#!/usr/bin/env python3
"""Real PostgreSQL authority, atomicity and revisitation for bounded thinning."""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import sys
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from recall_server.canonical_thinning import CanonicalBodyThinner, thin_canonical_bodies  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from e2e_canonical_body_thinning import insert_document  # noqa: E402


class Thinning(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import recall_server.canonical_thinning as owner
        assert Path(owner.__file__).resolve() == RECALL / 'server/recall_server/canonical_thinning.py'
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store._pool.close()

    def setUp(self):
        self.tenant = 'tenant:bounded:' + uuid.uuid4().hex
        self.source = 'source:test'
        self.suffix_prefix = uuid.uuid4().hex + ':'
        self.thinner = CanonicalBodyThinner(self.store, tenant_id=self.tenant)

    def insert(self, suffix, **kwargs):
        with self.store.connect() as c:
            return insert_document(c, tenant=self.tenant, principal='principal:test',
                                   source=kwargs.pop('source', self.source),
                                   suffix=self.suffix_prefix + suffix, text='immutable fixture body ' * 40,
                                   **kwargs)

    def queue(self, suffix):
        with self.store.connect() as c:
            c.execute('''INSERT INTO canonical_evidence_document_queue
                         (tenant_id,source_id,native_parent_id,generation,reason)
                         VALUES(%s,%s,%s,1,'ingest')''',
                      (self.tenant, self.source, 'session:' + self.suffix_prefix + suffix))

    def remaining(self):
        with self.store.connect() as c:
            return c.execute('''SELECT count(*) AS n FROM canonical_documents
                                WHERE tenant_id=%s AND body_location='inline' ''',
                             (self.tenant,)).fetchone()['n']

    def test_ready_suffix_not_skipped_and_authority_parity(self):
        for i in range(25):
            self.insert(str(i))
        self.insert('missing', omit_chunks=True)
        self.insert('queued')
        self.queue('queued')
        with self.store.connect() as c:
            before = c.execute('''SELECT md5(string_agg(row_to_json(t)::text,''
                                 ORDER BY chunk_id)) AS digest
                                 FROM canonical_chunks t WHERE tenant_id=%s''',
                               (self.tenant,)).fetchone()['digest']
        reports = [self.thinner.thin(batch_size=10) for _ in range(3)]
        self.assertEqual([r['documents'] for r in reports], [10, 10, 5])
        self.assertEqual(self.remaining(), 2)
        with self.store.connect() as c:
            after = c.execute('''SELECT md5(string_agg(row_to_json(t)::text,''
                                ORDER BY chunk_id)) AS digest
                                FROM canonical_chunks t WHERE tenant_id=%s''',
                              (self.tenant,)).fetchone()['digest']
        self.assertEqual(before, after)
        # Same existing standalone authority cannot clear either refused body.
        self.assertEqual(thin_canonical_bodies(self.store, tenant_id=self.tenant,
                         batch_size=10)['documents'], 0)

    def test_locked_document_and_event_are_revisited(self):
        event, doc = self.insert('locked')
        for table, column, value in [('canonical_documents', 'document_id', doc),
                                     ('canonical_events', 'event_id', event)]:
            with self.store.connect() as held:
                held.execute(f'SELECT 1 FROM {table} WHERE tenant_id=%s AND {column}=%s FOR UPDATE',
                             (self.tenant, value)).fetchone()
                self.assertEqual(self.thinner.thin(batch_size=10)['documents'], 0)
                self.assertEqual(self.remaining(), 1)
        self.assertEqual(self.thinner.thin(batch_size=10)['documents'], 1)

    def test_queued_then_ready_and_arrivals_both_sides_of_cursor(self):
        values = sorted((self.insert(str(i))[1], str(i)) for i in range(8))
        for _, suffix in values:
            self.queue(suffix)
        # Small local window exercises the same owner wrap with multiple calls.
        self.thinner.WINDOW_SIZE = 2
        self.assertEqual(self.thinner.thin(batch_size=1)['documents'], 0)
        old_after, high = self.thinner._after[1], self.thinner._through[1]
        behind = above = None
        for i in range(1000):
            suffix = 'arrival:' + str(i)
            doc = 'doc_' + hashlib.sha256(('document:' + self.suffix_prefix + suffix).encode()).hexdigest()[:32]
            if behind is None and doc < old_after:
                self.insert(suffix); behind = doc
            if above is None and doc > high:
                self.insert(suffix); above = doc
            if behind and above:
                break
        self.assertIsNotNone(behind)
        self.assertIsNotNone(above)
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                      (self.tenant,))
        total = 0
        for _ in range(25):
            total += self.thinner.thin(batch_size=1)['documents']
            if self.remaining() == 0:
                break
        self.assertEqual(total, 10)
        self.assertEqual(self.remaining(), 0)

    def test_source_collision_stays_zipped_and_tenant_scoped(self):
        _, first = self.insert('collision:a', source='source:a')
        _, second = self.insert('collision:b', source='source:b', omit_chunks=True)
        with self.store.connect() as c:
            # Build an intentional cross-source document-id collision while
            # preserving foreign keys; source identities remain distinct.
            c.execute('UPDATE canonical_documents SET document_id=%s WHERE tenant_id=%s AND source_id=%s AND document_id=%s',
                      (first, self.tenant, 'source:b', second))
            c.execute('''INSERT INTO canonical_chunks
                         (tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                         SELECT tenant_id,'source:b',chunk_id,document_id,ordinal,
                                receipt || '/b',text_redacted,text_sha256
                           FROM canonical_chunks WHERE tenant_id=%s AND source_id='source:a' ''',
                      (self.tenant,))
        self.thinner.WINDOW_SIZE = 1
        self.assertEqual(self.thinner.thin(batch_size=10)['documents'], 1)
        self.assertEqual(self.remaining(), 1)
        self.assertEqual(self.thinner.thin(batch_size=10)['documents'], 1)

    def test_ready_window_does_not_lock_unselected_suffix(self):
        ids = sorted(self.insert('locks:' + str(i))[1] for i in range(1024))
        # This fixture isolates row-lock scope with usable statistics. It is
        # not a performance proof: stale-statistics probes can hit the worker's
        # statement budget and are covered separately by deadline/plan tests.
        with self.store.connect() as c:
            for table in ('canonical_documents', 'canonical_events',
                          'raw_artifacts', 'canonical_chunks',
                          'canonical_evidence_documents',
                          'canonical_evidence_document_queue'):
                c.execute('ANALYZE ' + table)
        owner = self
        class CheckLocks:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield connection
                    with owner.store.connect() as other:
                        other.execute("SET LOCAL lock_timeout='100ms'")
                        other.execute("SELECT 1 FROM canonical_documents WHERE tenant_id=%s AND document_id=ANY(%s) FOR UPDATE NOWAIT",
                                      (owner.tenant, ids[1:])).fetchall()
                        other.execute("SELECT 1 FROM canonical_events event JOIN canonical_documents document USING(tenant_id,source_id,event_id) WHERE document.tenant_id=%s AND document.document_id=ANY(%s) FOR UPDATE OF event NOWAIT",
                                      (owner.tenant, ids[1:])).fetchall()
        thinner = CanonicalBodyThinner(CheckLocks(), tenant_id=self.tenant)
        self.assertEqual(thinner.thin(batch_size=1)['documents'], 1)

    def test_authority_rechecked_after_unlocked_probe(self):
        self.insert('race')
        owner = self
        injected = [False]
        class Connection:
            def __init__(self, connection): self.connection = connection
            def transaction(self): return self.connection.transaction()
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if 'updated_documents AS' in query and not injected[0]:
                    owner.queue('race')
                    injected[0] = True
                return self.connection.execute(query, params)
        class RaceStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        thinner = CanonicalBodyThinner(RaceStore(), tenant_id=self.tenant)
        self.assertEqual(thinner.thin(batch_size=10)['documents'], 0)
        self.assertEqual(self.remaining(), 1)
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                      (self.tenant,))
        self.assertEqual(thinner.thin(batch_size=10)['documents'], 1)

    def test_rollback_and_lost_commit_ack_do_not_advance_state(self):
        owner = self
        class FaultStore:
            mode = 'before'
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield connection
                    if self.mode == 'before':
                        raise RuntimeError('commit unavailable')
                if self.mode == 'after':
                    raise RuntimeError('commit unavailable')
        self.insert('rollback')
        fault = FaultStore()
        thinner = CanonicalBodyThinner(fault, tenant_id=self.tenant)
        for mode, remaining in [('before', 1), ('after', 0)]:
            fault.mode = mode
            with self.assertRaisesRegex(RuntimeError, 'commit unavailable'):
                thinner.thin(batch_size=10)
            self.assertIsNone(thinner._after)
            self.assertIsNone(thinner._through)
            self.assertEqual(self.remaining(), remaining)
        fault.mode = None
        self.assertEqual(thinner.thin(batch_size=10)['documents'], 0)

    def test_real_probe_timeout_rolls_back_savepoint_and_thins_fresh_hint(self):
        ids = sorted([self.insert('historical')[1], self.insert('fresh')[1]])
        owner = self
        injected = []
        class Connection:
            def __init__(self, connection): self.connection = connection
            def transaction(self): return self.connection.transaction()
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if 'WITH candidates AS MATERIALIZED' in query and 'updated_documents AS' not in query and not injected:
                    injected.append(True)
                    # A real server-side statement timeout aborts this savepoint.
                    # Rollback must restore the original2s setting before hints.
                    self.connection.execute("SET LOCAL statement_timeout='10ms'")
                    self.connection.execute('SELECT pg_sleep(0.05)')
                    raise AssertionError('actual statement timeout did not fire')
                return self.connection.execute(query, params)
        class TimeoutStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        thinner = CanonicalBodyThinner(TimeoutStore(), tenant_id=self.tenant)
        first = thinner.thin(batch_size=1,
                             committed_keys=((self.tenant, self.source, ids[1]),))
        self.assertEqual(first['historical_probe_timeouts'], 1)
        self.assertEqual(first['historical_window_size'], 512)
        self.assertEqual(first['documents'], 1)
        self.assertEqual(first['status'], 'pending')
        self.assertEqual(first['committed_hints_pending'], 0)
        self.assertIsNone(thinner._after)
        self.assertFalse(first['pass_complete'])
        with self.store.connect() as c:
            locations = {row['document_id']: row['body_location'] for row in c.execute(
                'SELECT document_id,body_location FROM canonical_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()}
        self.assertEqual(locations[ids[0]], 'inline')
        self.assertEqual(locations[ids[1]], 'chunks')
        second = thinner.thin(batch_size=1)
        self.assertEqual(second['historical_probe_timeouts'], 0)
        self.assertEqual(second['documents'], 1)
        self.assertEqual(self.remaining(), 0)

    def test_missing_manifest_raw_authority_and_deleted_document_refuse(self):
        for suffix in ('manifest', 'raw', 'deleted'):
            event, doc = self.insert(suffix)
            with self.store.connect() as c:
                if suffix == 'manifest':
                    c.execute('DELETE FROM canonical_evidence_documents WHERE tenant_id=%s AND native_parent_id=%s',
                              (self.tenant, 'session:' + self.suffix_prefix + suffix))
                elif suffix == 'raw':
                    c.execute("UPDATE raw_artifacts SET state='deleted',deleted_at=now() WHERE tenant_id=%s AND artifact_id=(SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND event_id=%s)",
                              (self.tenant, self.tenant, event))
                else:
                    c.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s AND document_id=%s',
                              (self.tenant, doc))
        self.assertEqual(self.thinner.thin(batch_size=10)['documents'], 0)
        self.assertEqual(self.remaining(), 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
