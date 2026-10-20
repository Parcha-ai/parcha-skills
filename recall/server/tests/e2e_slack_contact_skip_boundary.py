#!/usr/bin/env python3
"""Native authority races at the contacts-only metadata skip checkpoint."""
import json
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import psycopg
from e2e_managed_slack_captured_actors import CapturedSlackActors
from recall_server import slack_captured_attribution as captured


class ContactSkipBoundary(CapturedSlackActors):
    def check_boundary(self, boundary):
        worker, _ = self.captured()
        original_record, original_save = captured._record, captured._save
        original_repair = captured.repair_slice
        original_execute = psycopg.Connection.execute
        original_transaction = psycopg.Connection.transaction
        active = False
        seen = False
        sql_count = 0
        transactions = 0
        saved = None
        target = self.message.native_id
        with self.store.connect() as c:
            event_row = c.execute('SELECT canonical_redacted,artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                (self.tenant, self.source, target)).fetchone()
            event = event_row['canonical_redacted']
            receipt = c.execute('SELECT receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s AND document.is_current ORDER BY chunk.ordinal LIMIT 1',
                (self.tenant, self.source, target)).fetchone()['receipt']

        original_read = worker.archive.read_raw
        target_reads = []
        def read(reference):
            if reference['artifact_id'] == event_row['artifact_id']:
                target_reads.append(reference['artifact_id'])
            return original_read(reference)

        def execute(connection, *args, **kwargs):
            nonlocal sql_count
            if active:
                sql_count += 1
            return original_execute(connection, *args, **kwargs)

        @contextmanager
        def transaction(connection, *args, **kwargs):
            nonlocal transactions
            with original_transaction(connection, *args, **kwargs) as entered:
                if active:
                    transactions += 1
                yield entered

        def mutate():
            nonlocal active, seen, saved
            self.assertFalse(seen)
            seen = True
            saved = self.captured_scheduling_rows()
            active = False
            if boundary == 'authority':
                with self.store.connect() as c:
                    c.execute("UPDATE provider_connections SET status='revoked',revoked_at=now() WHERE id=%s", (self.connection_id,))
            elif boundary == 'grant':
                with self.store.connect() as c:
                    c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                        (self.tenant, self.source, self.principal))
            elif boundary == 'forget':
                self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-20T03:20:00Z', idempotency_key=uuid.uuid4().hex))
            elif boundary == 'revision':
                from client.mac import canonical_envelope
                changed = canonical_envelope(source_id=self.source, native_id=target,
                    kind='connector_record', content={**event['content'], 'author_id': 'slack:T123:U999'},
                    principal_id=self.principal, visibility='private', occurred_at=self.message.occurred_at,
                    provenance=event['provenance'], parent=self.message.native_parent_id)
                self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[changed])
            if boundary == 'thinned':
                self.thin_history()
            active = True

        def record(*args, **kwargs):
            nonlocal active
            if args[2]['native_id'] == target and kwargs.get('contacts_only'):
                active = True
            return original_record(*args, **kwargs)

        class SpoolBoundary:
            def __init__(self, database):
                self.database = database
            def __getattr__(self, name):
                return getattr(self.database, name)
            def execute(self, query, parameters=()):
                import hashlib
                target_key = captured.DEBT + hashlib.sha256(target.encode()).hexdigest()
                if query == 'SELECT 1 FROM meta WHERE key=?' and parameters == (target_key,) and not seen:
                    mutate()
                return self.database.execute(query, parameters)

        def repair(worker, row, runner, **kwargs):
            database = runner.db
            runner.db = SpoolBoundary(database)
            try:
                return original_repair(worker, row, runner, **kwargs)
            finally:
                runner.db = database

        def save(db, state, **kwargs):
            nonlocal active
            if seen:
                active = False
            return original_save(db, state, **kwargs)

        with patch.object(captured, 'repair_slice', side_effect=repair), \
                patch.object(captured, '_record', side_effect=record), \
                patch.object(captured, '_save', side_effect=save), \
                patch.object(psycopg.Connection, 'execute', execute), \
                patch.object(psycopg.Connection, 'transaction', transaction), \
                patch.object(worker.archive, 'read_raw', side_effect=read):
            report = self.step(worker)
        active = False
        self.assertTrue(seen)
        self.assertEqual(target_reads, [])
        if boundary in ('authority', 'grant', 'revision'):
            self.assertEqual(report['failed'], 1)
            self.assertEqual(self.captured_scheduling_rows(), saved)
        elif boundary == 'thinned':
            self.assertEqual(report['failed'], 0)
            self.assertEqual(self.captured_scheduling_rows(), saved)
            self.assertEqual(self.counts()[1], 0)
        else:
            self.assertEqual(report['failed'], 0)
            # A skip is no attribution or publication acknowledgment. Changes
            # after its native read remain for the messages/currentness owner.
            self.assertEqual(self.counts()[1], 0)
            self.assertNotEqual(self.captured_scheduling_rows(), saved)
        if boundary == 'unchanged':
            self.assertEqual(sql_count, 6)
            self.assertEqual(transactions, 1)
        if boundary == 'thinned':
            from recall_server.archive import ArchiveNotFound
            with patch.object(worker.archive, 'read_raw', side_effect=ArchiveNotFound('synthetic')):
                self.assertEqual(self.step(worker)['failed'], 0)
            self.assertGreater(self.captured_checkpoint()['unresolved'], 0)
            self.converge(worker)
        print('SKIP_BOUNDARY ' + json.dumps(dict(boundary=boundary, sql=sql_count,
            entered_transactions=transactions, failed=report['failed']), sort_keys=True))

    def test_authority_revocation_after_skip_read_preserves_checkpoint(self):
        self.check_boundary('authority')

    def test_source_grant_revocation_after_skip_read_preserves_checkpoint(self):
        self.check_boundary('grant')

    def test_forget_after_skip_read_does_not_attribute(self):
        self.check_boundary('forget')

    def test_new_revision_after_skip_read_does_not_attribute(self):
        self.check_boundary('revision')

    def test_thinned_representation_does_not_advance_fast_skip(self):
        self.check_boundary('thinned')

    def test_unchanged_skip_counts_native_work(self):
        self.check_boundary('unchanged')


if __name__ == '__main__':
    names = [name for name in ContactSkipBoundary.__dict__ if name.startswith('test_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(ContactSkipBoundary(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
