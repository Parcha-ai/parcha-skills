#!/usr/bin/env python3
"""Durable search publication across canonical deletion and delayed vendor ACKs."""
import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.types.json import Jsonb

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
import e2e_receipt_membership as fixtures
import e2e_singleton_search_authority as search_fixtures
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.search_outbox import enqueue_search_outbox, record_passage_deletions


class PublicationReconcile(unittest.TestCase):
    setUpClass = classmethod(fixtures.ReceiptMembershipTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ReceiptMembershipTests.tearDownClass.__func__)
    setUp = fixtures.ReceiptMembershipTests.setUp
    ingest = fixtures.ReceiptMembershipTests.ingest
    prepare_search = search_fixtures.SingletonSearchTests.prepare_search

    def publisher(self):
        return TurbopufferProjector(self.store, TurbopufferSettings(api_key='synthetic'),
                                   client=self.client)

    def claims(self, publisher):
        with self.store.connect() as connection:
            return publisher.claim_months(connection, tenant_id=self.tenant, limit=10)

    def forget(self):
        return self.plane.forget({
            'contract': 'recall.forget-request.v1', 'schema_version': 1,
            'tenant_id': self.tenant, 'principal_id': self.principal,
            'source_id': self.source, 'target_receipt': self.receipt,
            'mode': 'explicit_forget', 'reason': 'owner_requested',
            'requested_at': '2026-10-15T00:00:00Z', 'idempotency_key': 'forget-late-upsert',
        })

    def test_late_upsert_after_forget_delete_ack_has_durable_repair(self):
        self.plane.evidence_projector = self.projector
        self.prepare_search()
        old = self.publisher()
        old_claim = self.claims(old)[0]
        write_page = old._write_page
        blocked = []

        def forget_and_finish_delete_before_old_upsert(namespace, budget, page, timing):
            started = time.monotonic()
            forgotten = self.forget()
            self.assertEqual(forgotten['status'], 'deleted')
            self.assertLess(time.monotonic() - started, 2, 'publication must not block native forget')
            fresh = self.publisher()
            for claim in self.claims(fresh):
                blocked.append(not fresh.project_month(claim)['retired'])
            return write_page(namespace, budget, page, timing)

        with patch.object(old, '_write_page', side_effect=forget_and_finish_delete_before_old_upsert):
            old.project_month(old_claim)
        fresh = self.publisher()
        self.assertTrue(not self.namespace.rows or self.claims(fresh),
                        'late vendor upsert survived after all deletion work was acknowledged')
        self.assertEqual(blocked, [True])
        for claim in self.claims(fresh):
            fresh.project_month(claim)
        self.assertEqual(self.namespace.rows, {})

    def test_old_claim_cannot_ack_recreated_generation_one(self):
        self.prepare_search()
        publisher = self.publisher()
        old = self.claims(publisher)[0]
        publisher.project_month(old)
        with self.store.connect() as connection:
            enqueue_search_outbox(connection, tenant_id=self.tenant, source_id=self.source,
                                  months=[old['month']], reason='logical-update')
            watermark = publisher.read_watermark(connection)
        self.assertEqual(self.claims(publisher)[0]['generation'], old['generation'])
        with self.store.connect() as connection:
            retired = publisher.finish_month(connection, old, namespace=self.namespace.name,
                                              rows_written=0, tombstones=[], watermark=watermark)
        self.assertFalse(retired)
        self.assertEqual(len(self.claims(publisher)), 1)

    def test_old_tombstone_ack_cannot_consume_repeated_deletion(self):
        self.prepare_search()
        publisher = self.publisher()
        with self.store.connect() as connection:
            passages = connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s',
                                          (self.tenant,)).fetchall()
            record_passage_deletions(connection, tenant_id=self.tenant, source_id=self.source,
                                    passages=passages, reason='forget')
        old = self.claims(publisher)[0]
        with self.store.connect() as connection:
            tombstones = publisher.capture_tombstones(connection, old)
            watermark = publisher.read_watermark(connection)
        with self.store.connect() as connection:
            record_passage_deletions(connection, tenant_id=self.tenant, source_id=self.source,
                                    passages=passages, reason='forget')
        with self.store.connect() as connection:
            self.assertFalse(publisher.finish_month(connection, old, namespace=self.namespace.name,
                rows_written=0, tombstones=tombstones, watermark=watermark))
            remaining = publisher.capture_tombstones(connection, old)
        self.assertEqual(len(remaining), len(tombstones))
        self.assertNotEqual(remaining, tombstones)

    def test_historical_orphan_is_only_queued_then_native_drain_deletes(self):
        self.plane.evidence_projector = self.projector
        self.prepare_search()
        publisher = self.publisher()
        saved = [dict(row) for row in self.namespace.rows.values()]
        self.assertEqual(self.forget()['status'], 'deleted')
        publisher.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(self.claims(publisher), [])
        self.namespace.write(upsert_rows=saved)  # Previously stranded vendor row.
        ids = [row['id'] for row in saved]
        self.assertEqual(publisher.queue_stale_passages(self.tenant, ids), len(ids))
        self.assertEqual(set(self.namespace.rows), set(ids))
        self.assertTrue(self.claims(publisher))
        self.assertTrue(all(q['include_attributes'] == ['source_id', 'first_occurred_at']
                            for q in self.namespace.queries))
        result = publisher.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(result['deleted'], len(ids))
        self.assertEqual(self.namespace.rows, {})

    def test_exact_point_recheck_preserves_a_current_id(self):
        self.prepare_search()
        publisher = self.publisher()
        publisher.drain(tenant_id=self.tenant, max_months=10)
        before = dict(self.namespace.rows)
        self.assertEqual(publisher.queue_stale_passages(self.tenant, list(before)), 0)
        self.assertEqual(self.namespace.rows, before)
        self.assertEqual(self.claims(publisher), [])

    def test_repair_upsert_uses_same_lease_and_can_retry_after_release(self):
        self.prepare_search()
        publisher = self.publisher()
        claim = self.claims(publisher)[0]
        ids = list(self.namespace.rows)
        writes = len(self.namespace.writes)
        with publisher._publication_lease(self.tenant, self.source, claim['month']) as acquired:
            self.assertTrue(acquired)
            with self.assertRaisesRegex(ValueError, '^search_publication_busy$'):
                self.publisher().upsert_passages(self.tenant, ids)
            self.assertEqual(len(self.namespace.writes), writes)
        self.assertEqual(self.publisher().upsert_passages(self.tenant, ids), len(ids))

    def test_current_row_reappearing_after_cleanup_queue_survives_drain(self):
        self.prepare_search()
        publisher = self.publisher()
        publisher.drain(tenant_id=self.tenant, max_months=10)
        with self.store.connect() as connection:
            rows = connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s',
                                      (self.tenant,)).fetchall()
            columns = [row['column_name'] for row in connection.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='canonical_passages' "
                "AND is_generated='NEVER' ORDER BY ordinal_position").fetchall()]
            connection.execute('DELETE FROM canonical_passages WHERE tenant_id=%s', (self.tenant,))
        ids = [row['passage_id'] for row in rows]
        self.assertEqual(publisher.queue_stale_passages(self.tenant, ids), len(ids))
        # A later canonical commit recreates exactly the stable ID before delivery.
        with self.store.connect() as connection:
            names = ','.join(columns)
            for row in rows:
                value = json.loads(json.dumps(dict(row), default=str))
                connection.execute(f'INSERT INTO canonical_passages ({names}) SELECT {names} '
                    'FROM jsonb_populate_record(NULL::canonical_passages,%s)', (Jsonb(value),))
            enqueue_search_outbox(connection, tenant_id=self.tenant, source_id=self.source,
                months=[rows[0]['first_occurred_at']], reason='logical-update')
        result = publisher.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(result['deleted'], 0)
        self.assertEqual(set(self.namespace.rows), set(ids))
        self.assertEqual(self.claims(publisher), [])

    def test_cooperative_yield_releases_lease_and_no_session_survives_close(self):
        self.prepare_search()
        publisher = self.publisher()
        publisher.page_rows = 1
        claim = self.claims(publisher)[0]
        steps = publisher._month_steps(claim, cooperative=True)
        self.assertGreater(next(steps)['rows'], 0)
        with self.publisher()._publication_lease(self.tenant, self.source, claim['month']) as acquired:
            self.assertTrue(acquired)
        steps.close()
        with self.publisher()._publication_lease(self.tenant, self.source, claim['month']) as acquired:
            self.assertTrue(acquired)
        self.assertTrue(self.claims(publisher))
        self.assertEqual(self.publisher().drain(tenant_id=self.tenant, max_months=10)['failed'], 0)

    def test_unrequested_vendor_identity_is_refused_without_queue_or_delete(self):
        self.prepare_search()
        publisher = self.publisher()
        publisher.drain(tenant_id=self.tenant, max_months=10)
        ids = list(self.namespace.rows)
        before = list(self.namespace.writes)
        wrong = dict(next(iter(self.namespace.rows.values())), id='psg_' + '0' * 32)
        with patch.object(self.namespace, 'query', return_value={'rows': [wrong]}):
            with self.assertRaisesRegex(ValueError, '^search_reconcile_identity_invalid$'):
                publisher.queue_stale_passages(self.tenant, ids)
        self.assertEqual(self.namespace.writes, before)
        self.assertEqual(self.claims(publisher), [])

    def assert_backend_locks_released(self, pid, *, timeout=1.0):
        # Closing libpq discards the client immediately. PostgreSQL releases
        # the backend's session locks after processing the socket close.
        deadline = time.monotonic() + timeout
        with self.store.connect() as connection:
            self.assertNotEqual(connection.info.backend_pid, pid,
                                'discarded backend was returned to the pool')
            while True:
                locks = connection.execute("SELECT count(*) AS count FROM pg_locks "
                    "WHERE locktype='advisory' AND granted AND pid=%s", (pid,)).fetchone()['count']
                if locks == 0 or time.monotonic() >= deadline:
                    break
                time.sleep(.01)
        self.assertEqual(locks, 0, 'discarded backend retained advisory locks')

    def test_cleanup_observer_rejects_a_live_leaked_lock(self):
        with self.store.connect() as leaked:
            leaked.execute('SELECT pg_advisory_lock(7314159)')
            try:
                with self.assertRaisesRegex(AssertionError, 'retained advisory locks'):
                    self.assert_backend_locks_released(leaked.info.backend_pid, timeout=.05)
            finally:
                leaked.execute('SELECT pg_advisory_unlock(7314159)')

    def test_failed_unlock_discards_session_instead_of_leaking_pool_lock(self):
        self.prepare_search()
        publisher = self.publisher()
        claim = self.claims(publisher)[0]
        execute = psycopg.Connection.execute

        discarded = []

        def abort_unlock(connection, query, *args, **kwargs):
            if isinstance(query, str) and query.startswith('SELECT pg_advisory_unlock('):
                discarded.append((connection, connection.info.backend_pid))
                return execute(connection, 'SELECT 1/0')
            return execute(connection, query, *args, **kwargs)

        with patch.object(psycopg.Connection, 'execute', abort_unlock):
            with self.assertRaises(psycopg.errors.DivisionByZero):
                with publisher._publication_lease(self.tenant, self.source, claim['month']) as acquired:
                    self.assertTrue(acquired)
        self.assertTrue(discarded[0][0].closed)
        self.assert_backend_locks_released(discarded[0][1])
        # Pool rollback cannot release a session lock. A different backend must
        # be able to acquire it after failed cleanup; reentrant same-session
        # success would hide the bug, so inspect the cluster lock table too.
        identity = 'search-publication\x1f' + self.tenant + '\x1f' + self.source + '\x1f' + claim['month'].isoformat()
        with self.store.connect() as connection:
            locks = connection.execute("""SELECT count(*) AS count FROM pg_locks
                WHERE locktype='advisory' AND granted
                AND classid=((hashtextextended(%s,0) >> 32) & 4294967295)::oid
                AND objid=(hashtextextended(%s,0) & 4294967295)::oid""", (identity,identity)).fetchone()['count']
        self.assertEqual(locks, 0, 'failed cleanup returned a locked session to the pool')
        with self.publisher()._publication_lease(self.tenant, self.source, claim['month']) as acquired:
            self.assertTrue(acquired)

    def test_lost_lock_acquisition_ack_discards_uncertain_session(self):
        self.prepare_search()
        publisher = self.publisher()
        claim = self.claims(publisher)[0]
        execute = psycopg.Connection.execute
        acquired_pid = []
        acquired_connections = []

        def lost_ack(connection, query, *args, **kwargs):
            result = execute(connection, query, *args, **kwargs)
            if isinstance(query, str) and query.startswith('SELECT pg_try_advisory_lock('):
                self.assertTrue(result.fetchone()['acquired'])
                acquired_pid.append(connection.info.backend_pid)
                acquired_connections.append(connection)
                raise OSError('synthetic acquisition acknowledgement loss')
            return result

        with patch.object(psycopg.Connection, 'execute', lost_ack):
            with self.assertRaises(OSError):
                with publisher._publication_lease(self.tenant, self.source, claim['month']):
                    self.fail('uncertain lease must not admit work')
        self.assertTrue(acquired_connections[0].closed)
        self.assert_backend_locks_released(acquired_pid[0])
        with self.store.connect() as connection:
            locks = connection.execute("SELECT count(*) AS count FROM pg_locks "
                "WHERE locktype='advisory' AND granted AND pid=%s", (acquired_pid[0],)).fetchone()['count']
        self.assertEqual(locks, 0, 'uncertain acquisition leaked a session lock')

    def test_lost_vendor_ack_keeps_work_and_releases_lease(self):
        self.plane.evidence_projector = self.projector
        self.prepare_search()
        publisher = self.publisher()
        claim = self.claims(publisher)[0]
        write = self.namespace.write

        def lost_ack(**kwargs):
            self.assertEqual(self.forget()['status'], 'deleted')
            write(**kwargs)
            raise OSError('synthetic lost vendor acknowledgement')

        with patch.object(self.namespace, 'write', side_effect=lost_ack):
            with self.assertRaises(OSError):
                publisher.project_month(claim)
        self.assertTrue(self.namespace.rows)
        self.assertTrue(self.claims(publisher))
        result = self.publisher().drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(self.namespace.rows, {})


if __name__ == '__main__':
    unittest.main()
