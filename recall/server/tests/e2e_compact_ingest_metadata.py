#!/usr/bin/env python3
"""Canonical receipts must not require permanent per-event operational rows."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch
import uuid
from psycopg import sql

import e2e_slack_deletion_ordering as fixture
from recall_server.canonical import _ensure_lineage_jobs


class CompactIngestMetadata(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    setUp = fixture.SlackDeletionOrdering.setUp
    ingest = fixture.SlackDeletionOrdering.ingest
    delete = fixture.SlackDeletionOrdering.delete
    current = fixture.SlackDeletionOrdering.current

    def jobs(self):
        with self.store.connect() as connection:
            return connection.execute('''SELECT job_id,connector_id,mode,status,attempt,
                created_at,updated_at FROM canonical_ingest_jobs
                WHERE tenant_id=%s AND source_id=%s ORDER BY connector_id''',
                (self.tenant, self.source)).fetchall()

    def test_thousand_events_keep_two_lineage_rows_and_no_success_audit(self):
        first = self.ingest(self.root, path='document')
        events = []
        for number in range(1, 1000):
            prepared = self.ingest(
                {'ts': f'{1784332800+number}.000100', 'user': 'U111', 'text': f'Synthetic {number}'},
                path='prepare', connector_id='slack.messages' if number < 500 else 'connector.synthetic')
            events.append(prepared['envelope'])
        # This fixture creates many rows before autovacuum's first statistics pass.
        # Give the existing history lookup current cardinalities before its large input.
        with self.store.connect() as connection:
            connection.execute('ANALYZE canonical_events,canonical_documents,canonical_chunks')
        result = self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=events)
        self.assertEqual(result['inserted'], 999)
        self.assertEqual(len(set(result['receipts'])), 999)
        self.assertNotIn(first['receipt'], result['receipts'])
        jobs = self.jobs()
        self.assertEqual(len(jobs), 2, 'lineage cardinality follows connectors, not events')
        self.assertEqual({row['connector_id'] for row in jobs}, {'slack.messages', 'connector.synthetic'})
        self.assertTrue(all(row['attempt'] == 0 and row['created_at'] == row['updated_at'] for row in jobs))
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s',
                                               (self.tenant,)).fetchone()['n'], 1000)
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM canonical_audit_events WHERE tenant_id=%s AND operation='ingest.commit'",
                                               (self.tenant,)).fetchone()['n'], 0)
        replay = fixture.CanonicalPlane(self.store, self.archive).ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=list(reversed(events)))
        self.assertEqual(replay['inserted'], 0)
        self.assertEqual(replay['duplicate_events'], 999)
        self.assertEqual(replay['receipts'], list(reversed(result['receipts'])))
        self.assertEqual(self.jobs(), jobs, 'replay must not mutate shared lineage timestamps')

    def test_conflicting_lineage_metadata_fails_without_ack_or_partial_event(self):
        original_source = self.source
        for path in ('document', 'batch'):
            for field, value in [('connector_id', 'connector.unexpected'), ('mode', 'backfill'),
                                 ('status', 'failed'), ('attempt', 7), ('updated_at', None)]:
                with self.subTest(path=path, field=field):
                    self.source = original_source + ':' + uuid.uuid4().hex
                    self.ingest(self.root, path=path)
                    with self.store.connect() as connection:
                        if field == 'updated_at':
                            connection.execute("UPDATE canonical_ingest_jobs SET updated_at=created_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s", (self.tenant, self.source))
                        else:
                            connection.execute(sql.SQL('UPDATE canonical_ingest_jobs SET {}=%s WHERE tenant_id=%s AND source_id=%s').format(sql.Identifier(field)), (value, self.tenant, self.source))
                    with self.assertRaises(fixture.CanonicalLifecycleError) as error:
                        self.ingest(self.reply, path=path)
                    self.assertEqual(error.exception.error_code, 'canonical_job_conflict')
                    self.assertEqual(self.current(self.reply), 0)
                    with self.store.connect() as connection:
                        self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['n'], 1)

    def test_legacy_event_job_and_audit_survive_new_ingestion_and_replay(self):
        first = self.ingest(self.root, path='document')
        legacy_job = 'job_' + uuid.uuid4().hex
        with self.store.connect() as connection:
            current_job = connection.execute('SELECT job_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['job_id']
            connection.execute("""INSERT INTO canonical_ingest_jobs(tenant_id,source_id,job_id,connector_id,mode,status,attempt,created_at,updated_at)
                SELECT tenant_id,source_id,%s,connector_id,'incremental','committed',1,created_at,created_at
                FROM canonical_ingest_jobs WHERE tenant_id=%s AND source_id=%s AND job_id=%s""", (legacy_job, self.tenant, self.source, current_job))
            connection.execute('UPDATE canonical_events SET job_id=%s WHERE tenant_id=%s AND source_id=%s', (legacy_job, self.tenant, self.source))
            connection.execute("INSERT INTO canonical_audit_events(tenant_id,source_id,audit_id,operation,status,item_count) VALUES (%s,%s,%s,'ingest.commit','success',1)", (self.tenant, self.source, 'audit_' + uuid.uuid4().hex))
            before = connection.execute('SELECT * FROM canonical_ingest_jobs WHERE tenant_id=%s AND source_id=%s AND job_id=%s', (self.tenant, self.source, legacy_job)).fetchone()
        self.ingest(self.reply, path='batch')
        replay = self.ingest(self.root, path='document')
        self.assertEqual(replay['receipt'], first['receipt'])
        self.assertEqual(replay['inserted'], 0)
        with self.store.connect() as connection:
            after = connection.execute('SELECT * FROM canonical_ingest_jobs WHERE tenant_id=%s AND source_id=%s AND job_id=%s', (self.tenant, self.source, legacy_job)).fetchone()
            self.assertEqual(after, before)
            self.assertEqual(connection.execute("SELECT count(*) AS n FROM canonical_audit_events WHERE tenant_id=%s AND source_id=%s AND operation='ingest.commit'", (self.tenant, self.source)).fetchone()['n'], 1)

    def test_concurrent_first_admissions_share_one_immutable_lineage(self):
        inputs = [self.ingest({'ts': f'{1784333000+n}.000100', 'user': 'U111',
                               'text': f'Concurrent {n}'}, path='prepare') for n in range(8)]
        barrier = threading.Barrier(8)
        def ingest(item):
            barrier.wait(timeout=10)
            return fixture.CanonicalPlane(self.store, self.archive).ingest_document(**item)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(ingest, inputs))
        self.assertEqual(sum(r['inserted'] for r in results), 8)
        self.assertEqual(len({r['receipt'] for r in results}), 8)
        jobs = self.jobs()
        self.assertEqual(len(jobs), 1)
        self.ingest(self.root, path='document')
        self.assertEqual(self.jobs(), jobs)

    def test_shared_lineage_remains_scoped_to_tenant_source_and_connector(self):
        self.ingest(self.root, path='document')
        self.ingest(self.root, path='batch', source=self.source + ':other')
        self.ingest(self.root, path='document', tenant=self.tenant + ':other')
        with self.store.connect() as connection:
            rows = connection.execute("""SELECT tenant_id,source_id,job_id FROM canonical_ingest_jobs
                WHERE tenant_id=ANY(%s) ORDER BY tenant_id,source_id""",
                ([self.tenant, self.tenant + ':other'],)).fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(len({row['job_id'] for row in rows}), 3)

    def test_colliding_lineage_ids_fail_before_creating_either_event(self):
        events = [self.ingest(value, path='prepare', connector_id=connector)['envelope']
                  for value, connector in [(self.root, 'slack.messages'),
                                           (self.reply, 'connector.synthetic')]]
        with patch('recall_server.canonical._lineage_job_id', return_value='job_forced_collision'):
            with self.assertRaises(fixture.CanonicalLifecycleError) as error:
                self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                       events=events)
        self.assertEqual(error.exception.error_code, 'canonical_job_conflict')
        self.assertEqual(self.jobs(), [])
        self.assertEqual(self.current(self.root), 0)
        self.assertEqual(self.current(self.reply), 0)

    def test_missing_lineage_insert_fails_closed(self):
        # A suppressed INSERT models a missing FK target independently of row metadata.
        with self.store.connect() as connection:
            connection.execute("""CREATE FUNCTION pg_temp.suppress_lineage() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$""")
            connection.execute("""CREATE TRIGGER test_suppress_lineage BEFORE INSERT
                ON canonical_ingest_jobs FOR EACH ROW EXECUTE FUNCTION pg_temp.suppress_lineage()""")
            try:
                with self.assertRaises(fixture.CanonicalLifecycleError) as error:
                    _ensure_lineage_jobs(connection, tenant_id=self.tenant, source_id=self.source,
                                         connector_ids=['slack.messages'])
                self.assertEqual(error.exception.error_code, 'canonical_job_conflict')
            finally:
                connection.execute('DROP TRIGGER test_suppress_lineage ON canonical_ingest_jobs')

    def test_concurrent_reversed_connector_batches_do_not_deadlock(self):
        batches = []
        for batch_number in range(2):
            connectors = ['slack.messages', 'connector.synthetic']
            if batch_number:
                connectors.reverse()
            batches.append([self.ingest(
                {'ts': f'{1784343000 + batch_number * 10 + number}.000100',
                 'user': 'U111', 'text': 'Concurrent batch'},
                path='prepare', connector_id=connector)['envelope']
                for number, connector in enumerate(connectors)])
        barrier = threading.Barrier(2)
        def ingest(events):
            barrier.wait(timeout=10)
            return fixture.CanonicalPlane(self.store, self.archive).ingest_batch(
                tenant_id=self.tenant, principal_id=self.principal, events=events)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(ingest, batches))
        self.assertEqual(sum(result['inserted'] for result in results), 4)
        self.assertEqual(len(self.jobs()), 2)

    def test_bad_lineage_rolls_back_other_new_lineage_and_canonical_changes(self):
        self.ingest(self.root, path='document')
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_ingest_jobs SET attempt=7 WHERE tenant_id=%s AND source_id=%s", (self.tenant, self.source))
        before = self.jobs()
        changed = self.ingest({**self.root, 'text': 'New revision that must roll back',
                              'edited': {'ts': '1784400000.000100'}}, path='prepare')['envelope']
        other = self.ingest(self.reply, path='prepare', connector_id='connector.synthetic')['envelope']
        with self.assertRaises(fixture.CanonicalLifecycleError):
            self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                   events=[changed, other])
        self.assertEqual(self.jobs(), before)
        self.assertEqual(self.current(self.root), 1)
        self.assertEqual(self.current(self.reply), 0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['n'], 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
