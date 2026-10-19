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
        # Reserve real hash-key extremes before inserting the middle. Randomly
        # searching for a key above the maximum of eight initial keys can fail
        # even after 1000 tries; fixture construction must guarantee both sides.
        values = sorted((
            'doc_' + hashlib.sha256(('document:' + self.suffix_prefix + str(i)).encode()).hexdigest()[:32],
            str(i),
        ) for i in range(10))
        self.assertEqual(len({doc for doc, _ in values}), 10)
        (behind, behind_suffix), (above, above_suffix) = values[0], values[-1]
        for expected_doc, suffix in values[1:-1]:
            self.assertEqual(self.insert(suffix)[1], expected_doc)
            self.queue(suffix)
        # Small local window exercises the same owner wrap with multiple calls.
        self.thinner.WINDOW_SIZE = 2
        self.assertEqual(self.thinner.thin(batch_size=1)['documents'], 0)
        old_after, high = self.thinner._after[1], self.thinner._through[1]
        self.assertLess(behind, old_after)
        self.assertLessEqual(old_after, high)
        self.assertLess(high, above)
        self.assertEqual(self.insert(behind_suffix)[1], behind)
        self.assertEqual(self.insert(above_suffix)[1], above)
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

    def test_real_key_timeout_retains_prefix_and_still_thins_committed_hint(self):
        ids = sorted([self.insert('key-history')[1], self.insert('key-fresh')[1]])
        owner, windows = self, []
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if (query.lstrip().startswith('SELECT source_id,document_id')
                        and 'ORDER BY source_id DESC' not in query):
                    windows.append(params)
                    if len(windows) == 1:
                        # Keep the actual worker2s statement limit. No fake
                        # Python cancellation or modified native proof query.
                        self.connection.execute('SELECT pg_sleep(3)')
                        raise AssertionError('native key deadline did not fire')
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
        self.assertEqual(first['scanned_keys'], 0)
        self.assertFalse(first['pass_complete'])
        self.assertIsNone(thinner._after)
        self.assertEqual(thinner._through, (self.source, ids[1]))
        with self.store.connect() as c:
            locations = {row['document_id']: row['body_location'] for row in c.execute(
                'SELECT document_id,body_location FROM canonical_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()}
        self.assertEqual(locations, {ids[0]: 'inline', ids[1]: 'chunks'})
        second = thinner.thin(batch_size=1)
        self.assertEqual(second['historical_probe_timeouts'], 0)
        self.assertEqual(second['documents'], 1)
        self.assertEqual(self.remaining(), 0)
        self.assertEqual([row[-1] for row in windows], [1024, 512])
        self.assertEqual(windows[0][:-1], windows[1][:-1])

    def test_successful_full_key_window_recovers_capacity_after_transient_cancel(self):
        documents = sorted(self.insert('capacity:' + str(i))[1] for i in range(3))
        owner, injected = self, []
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if (query.lstrip().startswith('SELECT source_id,document_id')
                        and 'ORDER BY source_id DESC' not in query and not injected):
                    injected.append(True)
                    self.connection.execute("SET LOCAL statement_timeout='10ms'")
                    self.connection.execute('SELECT pg_sleep(0.05)')
                    raise AssertionError('native key timeout did not fire')
                return self.connection.execute(query, params)
        class TimeoutStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        thinner = CanonicalBodyThinner(TimeoutStore(), tenant_id=self.tenant)
        thinner.WINDOW_SIZE = 2
        first = thinner.thin(batch_size=10)
        self.assertEqual(first['historical_probe_timeouts'], 1)
        self.assertEqual(first['historical_window_size'], 1)
        self.assertIsNone(thinner._after)
        self.assertEqual(self.remaining(), 3)
        second = thinner.thin(batch_size=10)
        self.assertEqual(second['documents'], 1)
        self.assertEqual(thinner._after, (self.source, documents[0]))
        self.assertEqual(second['historical_window_size'], 2)
        third = thinner.thin(batch_size=10)
        self.assertEqual(third['documents'], 2)
        self.assertEqual(third['historical_window_size'], 2)
        self.assertEqual(self.remaining(), 0)

    def test_upper_key_timeout_aborts_without_hints_or_invented_bound(self):
        from psycopg.errors import QueryCanceled
        _, document = self.insert('upper-timeout')
        owner = self
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if 'ORDER BY source_id DESC' in query:
                    self.connection.execute("SET LOCAL statement_timeout='10ms'")
                    self.connection.execute('SELECT pg_sleep(0.05)')
                    raise AssertionError('native bound timeout did not fire')
                return self.connection.execute(query, params)
        class TimeoutStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        thinner = CanonicalBodyThinner(TimeoutStore(), tenant_id=self.tenant)
        with self.assertRaises(QueryCanceled):
            thinner.thin(batch_size=1,
                committed_keys=((self.tenant, self.source, document),))
        self.assertEqual(self.remaining(), 1)
        self.assertIsNone(thinner._through)
        self.assertIsNone(thinner._after)
        self.assertEqual(thinner._window_size, 1024)
        self.assertEqual(list(thinner._committed_keys), [(self.source, document)])

    def test_key_timeout_still_refuses_newly_queued_hint(self):
        _, document = self.insert('hint-revoked')
        owner, injected = self, []
        class Connection:
            def __init__(self, connection): self.connection = connection
            def __getattr__(self, name): return getattr(self.connection, name)
            def execute(self, query, params):
                if (query.lstrip().startswith('SELECT source_id,document_id')
                        and 'ORDER BY source_id DESC' not in query and not injected):
                    injected.append(True)
                    owner.queue('hint-revoked')
                    self.connection.execute("SET LOCAL statement_timeout='10ms'")
                    self.connection.execute('SELECT pg_sleep(0.05)')
                    raise AssertionError('native key timeout did not fire')
                return self.connection.execute(query, params)
        class TimeoutStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        thinner = CanonicalBodyThinner(TimeoutStore(), tenant_id=self.tenant)
        first = thinner.thin(batch_size=1,
            committed_keys=((self.tenant, self.source, document),))
        self.assertEqual(first['historical_probe_timeouts'], 1)
        self.assertEqual(first['documents'], 0)
        self.assertEqual(first['committed_hints_pending'], 1)
        self.assertIsNone(thinner._after)
        self.assertEqual(self.remaining(), 1)
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,))
        self.assertEqual(thinner.thin(batch_size=1)['documents'], 1)
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


class ScheduledThinning(unittest.TestCase):
    setUpClass = classmethod(Thinning.setUpClass.__func__)
    tearDownClass = classmethod(Thinning.tearDownClass.__func__)
    setUp = Thinning.setUp
    insert = Thinning.insert
    remaining = Thinning.remaining
    queue = Thinning.queue

    def collect(self, scheduler):
        import time

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            result = scheduler.tick(True)
            if result is not None:
                return result
            time.sleep(0.005)
        self.fail("bounded background owner did not complete")

    def test_shared_pool_progress_during_blocked_sql_and_cursor_survives_next_tick(
        self,
    ):
        import threading
        from functools import partial
        from recall_server.projection_worker import BackgroundBodyThinner

        documents = sorted(self.insert(str(i))[1] for i in range(2))
        owner, entered = self, threading.Event()
        calls, prepared_threads, now = [], [], [0.0]
        lock_key = int(uuid.uuid4().hex[:12], 16)

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def execute(self, query, params):
                result = self.connection.execute(query, params)
                if query.startswith("SET LOCAL statement_timeout") and not calls:
                    calls.append(threading.get_ident())
                    entered.set()
                    self.connection.execute(
                        "SELECT pg_advisory_xact_lock(%s)", (lock_key,)
                    )
                return result

        class SharedStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)

        thinner = CanonicalBodyThinner(SharedStore(), tenant_id=self.tenant)

        def prepare(busy):
            prepared_threads.append(threading.get_ident())
            return partial(thinner.thin, batch_size=1)

        with BackgroundBodyThinner(prepare, clock=lambda: now[0]) as scheduler:
            with self.store.connect() as held:
                held.execute("SELECT pg_advisory_xact_lock(%s)", (lock_key,))
                scheduler.tick(True)
                self.assertTrue(entered.wait(1))
                # Same production pool serves the coordinator while its retained
                # owner is blocked inside an actual PostgreSQL statement.
                with self.store.connect() as foreground:
                    self.assertEqual(
                        foreground.execute("SELECT 123 AS n").fetchone()["n"], 123
                    )
                for _ in range(12):
                    now[0] += 5
                    self.assertIsNone(scheduler.tick(True))
                self.assertEqual(prepared_threads, [threading.get_ident()])
                self.assertNotEqual(calls[0], threading.get_ident())
            self.assertEqual(self.collect(scheduler)["documents"], 1)
            self.assertEqual(thinner._after, (self.source, documents[0]))
            self.assertEqual(self.remaining(), 1)
            now[0] += 30
            scheduler.tick(True)
            self.assertEqual(self.collect(scheduler)["documents"], 1)
            self.assertEqual(self.remaining(), 0)
            self.assertIsNone(thinner._after)
        print(
            "background_pg: shared pool foreground completed while SQL blocked; two acknowledged cursor steps"
        )

    def test_statement_deadline_survives_background_execution_and_retry(self):
        import threading
        import time
        from functools import partial
        from recall_server.projection_worker import BackgroundBodyThinner

        self.insert("deadline")
        owner, entered = self, threading.Event()
        injected, now = [], [0.0]

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def execute(self, query, params):
                result = self.connection.execute(query, params)
                if query.startswith("SET LOCAL statement_timeout") and not injected:
                    injected.append(True)
                    entered.set()
                    self.connection.execute("SELECT pg_sleep(10)")
                return result

        class SharedStore:
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)

        thinner = CanonicalBodyThinner(SharedStore(), tenant_id=self.tenant)
        with BackgroundBodyThinner(
            lambda busy: partial(thinner.thin, batch_size=1), clock=lambda: now[0]
        ) as scheduler:
            started = time.monotonic()
            scheduler.tick(True)
            self.assertTrue(entered.wait(1))
            with self.store.connect() as foreground:
                self.assertEqual(
                    foreground.execute("SELECT 456 AS n").fetchone()["n"], 456
                )
            from recall_server.projection_worker import run_projection_worker
            from tests.central_brain.test_projection_worker import _Logical, _Passages
            class Logical(_Logical):
                def project_pending(inner, **kwargs):
                    deadline = time.monotonic() + 5
                    while not getattr(scheduler, "failed_attempts", 0):
                        owner.assertLess(time.monotonic(), deadline)
                        kwargs["on_heartbeat"]()
                        time.sleep(0.005)
                    owner.queue("foreground-published")
                    return super().project_pending(**kwargs)
            with self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
                result = run_projection_worker(
                    Logical([]), _Passages([]), tenant_id=self.tenant, once=True,
                    logical_batch_size=5, passage_batch_size=5, embedding_batch_size=64,
                    max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                    interval_seconds=5, background_thinner=scheduler,
                )
            self.assertEqual(result["documents"], 2)
            self.assertEqual(result["canonical_bodies_thinned"], 0)
            self.assertEqual(scheduler.failed_attempts, 1)
            self.assertIn("type=QueryCanceled failed_attempts=1", logs.output[0])
            self.assertIn("canonical_thinning.py", logs.output[0])
            self.assertIn("in thin", logs.output[0])
            self.assertIn("canceling statement due to statement timeout", logs.output[0])
            with self.store.connect() as foreground:
                self.assertEqual(foreground.execute(
                    "SELECT count(*) AS n FROM canonical_evidence_document_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"], 1)
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 5)
            self.assertIsNone(thinner._after)
            self.assertEqual(self.remaining(), 1)
            now[0] += 30
            scheduler.tick(True)
            self.assertEqual(self.collect(scheduler)["documents"], 1)
        print(
            f"background_pg: actual statement timeout {elapsed:.3f}s; cursor unchanged; retry acknowledged"
        )

    def test_lost_commit_ack_remains_unknown_and_is_not_counted_as_success(self):
        from functools import partial
        from recall_server.projection_worker import (
            BackgroundBodyThinner,
            projection_totals,
        )

        self.insert("unknown")
        owner, now = self, [0.0]

        class FaultStore:
            fail = True

            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield connection
                if self.fail:
                    raise ConnectionError("lost commit acknowledgment")

        fault = FaultStore()
        thinner = CanonicalBodyThinner(fault, tenant_id=self.tenant)
        before = projection_totals()["bodies_thinned"]
        with BackgroundBodyThinner(
            lambda busy: partial(thinner.thin, batch_size=1), clock=lambda: now[0]
        ) as scheduler:
            scheduler.tick(True)
            import time
            with self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
                deadline = time.monotonic() + 5
                while not getattr(scheduler, "failed_attempts", 0):
                    self.assertLess(time.monotonic(), deadline)
                    scheduler.tick(True)
                    time.sleep(0.005)
            self.assertIn("type=ConnectionError failed_attempts=1", logs.output[0])
            self.assertIn("canonical_thinning.py", logs.output[0])
            self.assertIn("lost commit acknowledgment", logs.output[0])
            self.assertEqual(scheduler.failed_attempts, 1)
            self.assertEqual(self.remaining(), 0)
            self.assertIsNone(thinner._after)
            self.assertEqual(projection_totals()["bodies_thinned"], before)
            fault.fail = False
            now[0] += 30
            scheduler.tick(True)
            self.assertEqual(self.collect(scheduler)["documents"], 0)
        self.assertEqual(projection_totals()["bodies_thinned"], before)


if __name__ == '__main__':
    unittest.main(verbosity=2)
