#!/usr/bin/env python3
"""Exercise the explicit reverse path before singleton admission can exist."""
import unittest
from unittest.mock import patch
from e2e_receipt_membership import ReceiptMembershipTests
from recall_server.chunk_retirement import ChunkRetirementError


class SingletonMetadataRestore(ReceiptMembershipTests):
    def prepare(self):
        written = self.ingest('turn/restore', 'Exact archive rollback body α 🧠')
        original = self.baseline(written['receipt'])
        self.public_response = self.store.resolve(written['receipt'], tenant_id=self.tenant, authorized_sources=(self.source,))
        self.admit_fixture(original['document_id'])
        return original

    def restore(self, original, **kwargs):
        from recall_server.singleton_metadata_restore import restore_singleton_metadata
        with patch.object(self.store, 'verified_singleton_receipts', True):
            return restore_singleton_metadata(
                self.store, self.archive, tenant_id=self.tenant, source_id=self.source,
                principal_id=self.principal, document_ids=(original['document_id'],), **kwargs)

    def test_restore_recreates_exact_physical_receipt_and_disables_profile(self):
        original = self.prepare()
        preview = self.restore(original)
        self.assertEqual(preview['restored_documents'], 0)
        self.assertIsNone(self.baseline(original['receipt']))
        result = self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(result['restored_documents'], 1)
        self.assertEqual(self.baseline(original['receipt']), original)
        self.assertEqual(self.store.resolve(original['receipt'], tenant_id=self.tenant, authorized_sources=(self.source,)), self.public_response)
        with self.store.connect() as connection:
            row = connection.execute('''SELECT chunk.text_redacted,document.chunk_receipt_profile
                FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE chunk.tenant_id=%s AND chunk.receipt=%s''',
                (self.tenant, original['receipt'])).fetchone()
        self.assertEqual(row, dict(text_redacted='Exact archive rollback body α 🧠', chunk_receipt_profile=None))
        with self.store.connect() as connection:
            progress = connection.execute('SELECT enabled FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s', (self.tenant, self.source, 'session')).fetchone()
        self.assertEqual(progress, {'enabled': False})

    def test_restore_requires_the_existing_source_owner_grant(self):
        original = self.prepare()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                               (self.tenant, self.source, self.principal))
        with self.assertRaises(ChunkRetirementError):
            self.restore(original)
        self.assertIsNone(self.baseline(original['receipt']))

    def test_restore_refuses_a_reviewed_plan_after_revision_change(self):
        original = self.prepare()
        preview = self.restore(original)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.ingest('turn/restore', 'A newer revision owns the current document')
        with self.assertRaises(ChunkRetirementError):
            self.restore(original, apply=True, reviewed_plan=preview['plan'])


    def test_restore_rechecks_grant_after_archive_staging(self):
        from contextlib import contextmanager
        from recall_server import singleton_metadata_restore as owner
        original = self.prepare()
        preview = self.restore(original)
        stage = owner.stage_history_rows
        @contextmanager
        def revoke_after_stage(*args, **kwargs):
            with stage(*args, **kwargs) as staged:
                with self.store.connect() as connection:
                    connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                                       (self.tenant, self.source, self.principal))
                yield staged
        with patch.object(owner, 'stage_history_rows', revoke_after_stage):
            with self.assertRaises(ChunkRetirementError):
                self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertIsNone(self.baseline(original['receipt']))

    def test_restore_missing_archive_never_creates_metadata(self):
        from recall_server.canonical_history import HistoryUnavailable
        original = self.prepare()
        with patch.object(self.archive, 'read_raw', side_effect=OSError('synthetic archive unavailable')):
            with self.assertRaises(HistoryUnavailable):
                self.restore(original)
        self.assertIsNone(self.baseline(original['receipt']))

    def test_restore_actual_commit_lost_ack_has_no_acknowledged_result(self):
        from contextlib import contextmanager
        import psycopg
        from recall_server import singleton_metadata_restore as owner
        original = self.prepare()
        preview = self.restore(original)
        transaction = psycopg.Connection.transaction
        restore = owner.restore_outgoing
        armed = False
        def record_write(*args, **kwargs):
            nonlocal armed
            result = restore(*args, **kwargs)
            armed = True
            return result
        @contextmanager
        def commit_then_disconnect(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic commit acknowledgement lost')
        with patch.object(owner, 'restore_outgoing', record_write), patch.object(psycopg.Connection, 'transaction', commit_then_disconnect):
            with self.assertRaises(psycopg.OperationalError):
                self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.baseline(original['receipt']), original)
        observed = owner.inspect_singleton_restoration(self.store, principal_id=self.principal, reviewed_plan=preview['plan'])
        self.assertEqual(observed['restored_documents'], 1)
        self.assertEqual(observed['original_commit_outcome'], 'unknown')
        with self.assertRaises(ChunkRetirementError):
            self.restore(original, apply=True, reviewed_plan=preview['plan'])


    def test_restore_reads_archive_without_a_database_checkout(self):
        from contextlib import contextmanager
        original = self.prepare()
        connect, read = self.store.connect, self.archive.read_raw
        checked_out = reads = 0
        @contextmanager
        def tracked_connect(*args, **kwargs):
            nonlocal checked_out
            with connect(*args, **kwargs) as connection:
                checked_out += 1
                try:
                    yield connection
                finally:
                    checked_out -= 1
        def checked_read(*args, **kwargs):
            nonlocal reads
            self.assertEqual(checked_out, 0)
            reads += 1
            return read(*args, **kwargs)
        with patch.object(self.store, 'connect', tracked_connect), patch.object(self.archive, 'read_raw', checked_read):
            self.restore(original)
        self.assertGreater(reads, 0)

    def test_restore_rejects_catalog_or_profile_changes_after_staging(self):
        from contextlib import contextmanager
        from recall_server import singleton_metadata_restore as owner
        original = self.prepare()
        preview = self.restore(original)
        stage = owner.stage_history_rows
        for mutation in ('UPDATE canonical_evidence_documents SET created_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s',
                         'UPDATE canonical_documents SET chunk_receipt_profile=NULL WHERE tenant_id=%s AND source_id=%s'):
            @contextmanager
            def change_after_stage(*args, **kwargs):
                with stage(*args, **kwargs) as staged:
                    with self.store.connect() as connection:
                        connection.execute(mutation, (self.tenant, self.source))
                    yield staged
            with patch.object(owner, 'stage_history_rows', change_after_stage):
                with self.assertRaises(ChunkRetirementError):
                    self.restore(original, apply=True, reviewed_plan=preview['plan'])
            self.assertIsNone(self.baseline(original['receipt']))
            if 'created_at' in mutation:
                preview = self.restore(original)

    def test_restore_forget_during_staging_cannot_resurrect_metadata(self):
        from contextlib import contextmanager
        from recall_server import singleton_metadata_restore as owner
        original = self.prepare()
        preview = self.restore(original)
        stage = owner.stage_history_rows
        @contextmanager
        def forget_after_stage(*args, **kwargs):
            with stage(*args, **kwargs) as staged:
                self.ingest('turn/restore', '', tombstone=True)
                yield staged
        with patch.object(owner, 'stage_history_rows', forget_after_stage):
            with self.assertRaises(ChunkRetirementError):
                self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertIsNone(self.baseline(original['receipt']))
        self.assertIsNone(self.store.resolve(original['receipt'], tenant_id=self.tenant, authorized_sources=(self.source,)))

    def test_restore_actual_native_lock_conflict_cannot_mutate(self):
        from recall_server.chunk_retirement import _try_parent_native_locks
        original = self.prepare()
        preview = self.restore(original)
        with self.store.connect() as competing, competing.transaction():
            self.assertTrue(_try_parent_native_locks(competing.execute, (self.tenant, self.source), [original]))
            with self.assertRaises(ChunkRetirementError):
                self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertIsNone(self.baseline(original['receipt']))

    def test_restore_failed_transaction_rolls_back_rows_and_profile(self):
        from recall_server import singleton_metadata_restore as owner
        original = self.prepare()
        preview = self.restore(original)
        with patch.object(owner, 'disable_parent_retirement_after_restore', side_effect=ChunkRetirementError('synthetic rollback')):
            with self.assertRaises(ChunkRetirementError):
                self.restore(original, apply=True, reviewed_plan=preview['plan'])
        self.assertIsNone(self.baseline(original['receipt']))
        with self.store.connect() as connection:
            row = connection.execute('SELECT chunk_receipt_profile FROM canonical_documents WHERE tenant_id=%s AND document_id=%s',
                                     (self.tenant, original['document_id'])).fetchone()
        self.assertEqual(row, {'chunk_receipt_profile': 1})

    def test_restore_expired_deadline_never_reads_archive_or_writes(self):
        import time
        original = self.prepare()
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
            with self.assertRaises(ChunkRetirementError):
                self.restore(original, deadline_at=time.monotonic()-1)
        read.assert_not_called()
        self.assertIsNone(self.baseline(original['receipt']))


if __name__ == '__main__':
    suite = unittest.TestSuite(SingletonMetadataRestore(name) for name in dir(SingletonMetadataRestore)
                               if name.startswith('test_restore_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
