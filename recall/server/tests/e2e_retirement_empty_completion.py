"""Native retirement completion after every singleton has already been retired."""
from dataclasses import replace
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL/'server')]
import e2e_singleton_metadata_admission as original
from recall_server import chunk_retirement as retirement
from recall_server import retirement_runner as runner
from recall_server.db import BrainStore


class EmptyCompletion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.fixture = original.SingletonMetadataAdmission('runTest')
        self.fixture.store = self.store
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.archive = self.fixture.archive
        self.tenant = self.fixture.tenant
        self.source = self.fixture.source
        self.principal = self.fixture.principal
        self.receipts = [self.fixture.ingest('turn/'+name, 'Actual native singleton '+name)['receipt']
                         for name in ('one', 'two')]
        projected = self.fixture.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        self.assertEqual(projected['failed'], 0)
        self.env = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        self.env.start()
        self.addCleanup(self.env.stop)
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=True)

    def run_parent(self, batches):
        with self.store.connect() as connection:
            connection.execute("""UPDATE canonical_chunk_retirement_progress
                SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s""", (self.tenant,))
        return runner.run_retirement(self.store, self.archive,
            scope=runner.RetirementScope(self.tenant, self.principal, (self.source,)),
            apply=True, limits=runner.RunnerLimits(max_parents=1),
            parent_limits=replace(retirement.ParentRetirementLimits(), batch_documents=1, max_batches=batches),
            metadata_mode='singleton', deadline_at=time.monotonic()+20)

    def test_drained_parent_finishes_without_reopening_archive_or_claiming_full_proof(self):
        first = self.run_parent(2)
        self.assertEqual(first['cleared_documents'], 2)
        self.assertEqual(first['retired_metadata_chunks'], 2)
        self.assertEqual(first['completed_parent_proofs'], 0)
        with self.store.connect() as connection:
            before = connection.execute("""SELECT status,last_record_ordinal,
                cumulative_cleared_utf8_bytes FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s""", (self.tenant,)).fetchone()
            chunks = connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s',
                                        (self.tenant,)).fetchone()['n']
        self.assertEqual(chunks, 0)
        self.assertEqual(before['status'], 'partial')
        with patch.object(self.archive, 'read_raw', side_effect=AssertionError('already drained archive reopened')) as read:
            resumed = self.run_parent(1)
        self.assertFalse(resumed['commit_outcome_unknown'])
        self.assertEqual(resumed['failed_parents'], 0)
        self.assertEqual(resumed['cleared_utf8_bytes'], 0)
        self.assertEqual(resumed['retired_metadata_chunks'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 0)
        self.assertEqual(resumed['completed_noop_parents'], 1)
        read.assert_not_called()
        with self.store.connect() as connection:
            after = connection.execute("""SELECT status,last_record_ordinal,
                cumulative_cleared_utf8_bytes FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s""", (self.tenant,)).fetchone()
        self.assertEqual(after, dict(before, status='complete'))
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)
        for receipt in self.receipts:
            self.assertIsNotNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def drain(self):
        result = self.run_parent(2)
        self.assertEqual(result['retired_metadata_chunks'], 2)
        with self.store.connect() as connection:
            return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()

    def test_inline_parent_finishes_after_thinning_and_native_metadata_retirement(self):
        from e2e_thinning_retirement_transition import ThinningRetirement

        lifecycle = ThinningRetirement('runTest')
        lifecycle.store = self.store
        lifecycle.setUp()
        self.addCleanup(lifecycle.doCleanups)
        # Retain the existing inline -> rearm -> physical retirement assertions.
        lifecycle.test_thinning_after_eof_reaches_newly_eligible_metadata()
        before = lifecycle.ledger()
        self.assertEqual(before['status'], 'partial')
        with patch.object(lifecycle.archive, 'read_raw',
                          side_effect=AssertionError('drained archive reopened')) as read:
            finished = lifecycle.run_parent(1)
        read.assert_not_called()
        self.assertEqual(finished['completed_noop_parents'], 1)
        self.assertEqual(finished['completed_parent_proofs'], 0)
        self.assertEqual(finished['failed_parents'], 0)
        self.assertFalse(finished['commit_outcome_unknown'])
        self.assertEqual(finished['cleared_utf8_bytes'], 0)
        self.assertEqual(finished['retired_metadata_chunks'], 0)
        after = lifecycle.ledger()
        self.assertEqual(after['status'], 'complete')
        self.assertEqual(after['scope_epoch'], before['scope_epoch'] + 1)
        for field in ('last_record_ordinal', 'cumulative_cleared_documents',
                      'cumulative_cleared_chunks', 'cumulative_cleared_utf8_bytes'):
            self.assertEqual(after[field], before[field])
        self.assertEqual(lifecycle.run_parent(1)['attempted_parents'], 0)

    def ledger(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()

    def after_prepare(self, action):
        from recall_server import parent_chunk_window as window
        original_prepare = window.prepare_window
        def prepared(*args, **kwargs):
            result = original_prepare(*args, **kwargs)
            action()
            return result
        return patch.object(window, 'prepare_window', side_effect=prepared)

    def test_new_epoch_after_eof_snapshot_refuses_completion(self):
        self.drain()
        def change():
            retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
                source_id=self.source, native_parent_id='session', enabled=False)
        with self.after_prepare(change):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['errors'], {'parent_retirement_disabled': 1})
        self.assertEqual(self.ledger()['status'], 'disabled')

    def test_revoked_grant_after_eof_snapshot_refuses_completion(self):
        self.drain()
        def revoke():
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s',
                                   (self.tenant, self.principal))
        with self.after_prepare(revoke):
            result = self.run_parent(1)
        self.assertEqual(result['errors'], {'retirement_owner_required': 1})
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')

    def forget(self, native):
        result = self.fixture.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=f'recall://{self.source}/{native}?rev=1#item=0', mode='explicit_forget',
            reason='owner_requested', requested_at='2026-10-17T00:00:00Z', idempotency_key='empty-'+native))
        self.assertEqual(result['status'], 'deleted')

    def test_native_forget_after_eof_snapshot_does_not_complete_old_scope(self):
        self.drain()
        with self.after_prepare(lambda: self.forget('turn/one')):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')
        self.assertIsNone(self.store.resolve(self.receipts[0], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def test_parent_forget_after_eof_snapshot_does_not_complete_old_scope(self):
        self.drain()
        with self.after_prepare(lambda: self.forget('session')):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')

    def test_append_after_eof_snapshot_refuses_completion_and_retains_new_work(self):
        before = self.drain()
        with self.after_prepare(lambda: self.fixture.ingest('turn/three', 'New collector ACK before completion')):
            result = self.run_parent(1)
        self.assertEqual(result['errors'], {'parent_retirement_window_pending': 1})
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')
        self.assertEqual(self.ledger()['cumulative_cleared_utf8_bytes'], before['cumulative_cleared_utf8_bytes'])

    def test_append_after_completion_is_queued_then_publication_invalidates_epoch(self):
        from recall_server import parent_chunk_window as window
        before = self.drain()
        complete = window._complete_without_physical_work
        appended = []
        def append_after(*args, **kwargs):
            result = complete(*args, **kwargs)
            self.assertEqual(self.ledger()['status'], 'complete')
            appended.append(self.fixture.ingest('turn/three', 'New collector ACK after completion')['receipt'])
            with self.store.connect() as connection:
                self.assertIsNotNone(connection.execute('SELECT 1 FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                                                       (self.tenant,)).fetchone())
            self.fixture.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
            return result
        with patch.object(window, '_complete_without_physical_work', side_effect=append_after):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 1)
        current = self.ledger()
        self.assertEqual(current['status'], 'pending')
        self.assertGreater(current['scope_epoch'], before['scope_epoch'])
        self.assertEqual(current['last_record_ordinal'], -1)
        self.assertIsNotNone(self.store.resolve(appended[0], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive))
        self.assertEqual(self.run_parent(4)['attempted_parents'], 1)

    def restore_metadata(self):
        from recall_server.singleton_metadata_restore import restore_singleton_metadata
        with self.store.connect() as connection:
            document = connection.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s AND is_current',
                                          (self.tenant, 'turn/one')).fetchone()['document_id']
        args = dict(tenant_id=self.tenant, source_id=self.source, principal_id=self.principal, document_ids=(document,))
        preview = restore_singleton_metadata(self.store, self.archive, **args)
        return restore_singleton_metadata(self.store, self.archive, **args, apply=True, reviewed_plan=preview['plan'])

    def test_restored_empty_metadata_uses_original_full_proof(self):
        self.drain()
        self.assertEqual(self.restore_metadata()['restored_documents'], 1)
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['retired_metadata_chunks'], 0)
        self.assertEqual(result['completed_parent_proofs'], 1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 1)

    def test_deleted_different_ordinal_physical_row_cannot_pass_noop(self):
        self.drain()
        self.restore_metadata()
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunks SET ordinal=7,deleted_at=now() WHERE tenant_id=%s", (self.tenant,))
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s', (self.tenant,))
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s',
                                                (self.tenant,)).fetchone()['n'], 1)

    def test_no_current_documents_cannot_become_noop_completion(self):
        self.drain()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s', (self.tenant,))
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)

    def test_missing_profile_without_physical_rows_is_not_completion_evidence(self):
        self.drain()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=NULL WHERE tenant_id=%s', (self.tenant,))
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')

    def test_metadata_restore_catalog_lock_prevents_concurrent_noop(self):
        from recall_server import singleton_metadata_restore as restore
        self.drain()
        observed = []
        original_restore = restore.restore_outgoing
        def while_locked(*args, **kwargs):
            observed.append(self.run_parent(1))
            return original_restore(*args, **kwargs)
        with patch.object(restore, 'restore_outgoing', side_effect=while_locked):
            self.restore_metadata()
        self.assertEqual(observed[0]['errors'], {'parent_retirement_lock_busy': 1})
        self.assertEqual(observed[0]['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')

    def test_actual_completion_commit_lost_ack_never_claims_acknowledged_noop(self):
        from contextlib import contextmanager
        import psycopg
        self.drain()
        transaction = psycopg.Connection.transaction
        execute = self.store._execute_bounded
        armed = False
        def capture_write(connection, sql, *args, **kwargs):
            nonlocal armed
            result = execute(connection, sql, *args, **kwargs)
            if "SET status='complete'" in sql:
                armed = True
            return result
        @contextmanager
        def commit_then_disconnect(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic lost completion acknowledgement')
        with patch.object(self.store, '_execute_bounded', side_effect=capture_write), \
                patch.object(psycopg.Connection, 'transaction', commit_then_disconnect):
            result = self.run_parent(1)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(self.ledger()['status'], 'complete')
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)


    def test_append_between_empty_observation_and_status_write_refuses_completion(self):
        self.drain()
        execute = self.store._execute_bounded
        appended = False
        def append_after_observation(connection, sql, *args, **kwargs):
            nonlocal appended
            result = execute(connection, sql, *args, **kwargs)
            if 'END AS drained' in sql and not appended:
                appended = True
                self.fixture.ingest('turn/three', 'Concurrent collector ACK before the status write')
            return result
        with patch.object(self.store, '_execute_bounded', side_effect=append_after_observation):
            result = self.run_parent(1)
        self.assertTrue(appended)
        self.assertEqual(result['errors'], {'parent_retirement_window_pending': 1})
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')

    def test_body_restore_after_eof_snapshot_disables_old_completion(self):
        self.drain()
        def restore():
            self.restore_metadata()
            with self.store.connect() as connection:
                document = connection.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s AND is_current',
                    (self.tenant, 'turn/one')).fetchone()['document_id']
            scope = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(document,), restore=True)
            preview = retirement.retire_current_chunks(self.store, self.archive, **scope)
            retirement.retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=preview['plan'])
        with self.after_prepare(restore):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['errors'], {'parent_retirement_disabled': 1})
        self.assertFalse(self.ledger()['enabled'])
        with self.store.connect() as connection:
            self.assertGreater(connection.execute('SELECT sum(octet_length(text_redacted)) AS n FROM canonical_chunks WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_inactive_historical_rows_are_outside_current_parent_completion(self):
        self.drain()
        self.restore_metadata()
        self.fixture.ingest('turn/one', 'New current revision with independent historical retention')
        self.fixture.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        with self.store.connect() as connection:
            historical = connection.execute('SELECT chunk.chunk_id FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE chunk.tenant_id=%s AND NOT document.is_current',
                (self.tenant,)).fetchall()
        self.assertEqual(len(historical), 1)
        result = self.run_parent(4)
        self.assertEqual(result['completed_noop_parents'], 1)
        self.assertEqual(result['completed_parent_proofs'], 0)
        with self.store.connect() as connection:
            after = connection.execute('SELECT chunk.chunk_id FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE chunk.tenant_id=%s AND NOT document.is_current',
                (self.tenant,)).fetchall()
        self.assertEqual(after, historical)


if __name__ == '__main__':
    unittest.main(verbosity=2)
