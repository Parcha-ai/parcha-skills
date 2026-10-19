#!/usr/bin/env python3
"""Prove exact singleton retirement and its empty-body reverse operation."""
import os
import unittest
from unittest.mock import patch
from e2e_receipt_membership import ReceiptMembershipTests
from e2e_compact_canonical_bodies import DurableArchiveFixture
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.chunk_retirement import ChunkRetirementError, retire_current_chunks


class SingletonMetadataAdmission(ReceiptMembershipTests):
    def setUp(self):
        ReceiptMembershipTests.setUp(self)
        self.archive = DurableArchiveFixture(self.archive.root, namespace_key=b'm' * 32)
        self.projector = CanonicalLogicalEvidenceProjector(self.store,
            LogicalEvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant,
            raw_archive=self.archive)
        self.plane = CanonicalPlane(self.store, self.archive, chunk_body_archive=self.archive)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=self.tenant, principal_id=self.principal)

    def prepare(self, *, retire_body=True):
        written = self.ingest('turn/admit', 'Verified singleton admission evidence α 🧠')
        original = self.baseline(written['receipt'])
        self.public_response = self.store.resolve(written['receipt'], tenant_id=self.tenant, authorized_sources=(self.source,))
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        if retire_body:
            with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
                scope = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(original['document_id'],))
                preview = retire_current_chunks(self.store, self.archive, **scope)
                retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=preview['plan'])
        return original

    def admit(self, original, **kwargs):
        from recall_server.chunk_retirement import retire_singleton_metadata
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(self.store, 'search_plane', 'turbopuffer'), patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            return retire_singleton_metadata(self.store, self.archive, tenant_id=self.tenant,
                source_id=self.source, principal_id=self.principal,
                document_ids=(original['document_id'],), **kwargs)

    def test_admission_removes_only_verified_empty_singleton_and_reverses_exactly(self):
        from recall_server.singleton_metadata_restore import restore_singleton_metadata
        original = self.prepare()
        with self.store.connect() as connection:
            before = connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s AND receipt=%s', (self.tenant, original['receipt'])).fetchone()['row']
        preview = self.admit(original)
        self.assertEqual(preview['retired_chunks'], 0)
        self.assertEqual(self.baseline(original['receipt']), original)
        result = self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(result['retired_chunks'], 1)
        self.assertIsNone(self.baseline(original['receipt']))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(self.store.resolve(original['receipt'], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive), self.public_response)
            scope = dict(tenant_id=self.tenant, source_id=self.source, principal_id=self.principal,
                         document_ids=(original['document_id'],))
            reverse = restore_singleton_metadata(self.store, self.archive, **scope)
            restored = restore_singleton_metadata(self.store, self.archive, **scope, apply=True, reviewed_plan=reverse['plan'])
        self.assertEqual(restored['restored_body_bytes'], 0)
        with self.store.connect() as connection:
            after = connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s AND receipt=%s', (self.tenant, original['receipt'])).fetchone()['row']
        self.assertEqual(after, before)

    def test_admission_rejects_unretired_body_without_archive_reads(self):
        original = self.prepare(retire_body=False)
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
            with self.assertRaises(ChunkRetirementError):
                self.admit(original)
        read.assert_not_called()
        self.assertEqual(self.baseline(original['receipt']), original)

    def test_admission_rejects_changed_chunk_timestamp(self):
        original = self.prepare()
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunks SET created_at=created_at-interval '1 second' WHERE tenant_id=%s AND receipt=%s", (self.tenant, original['receipt']))
        with self.assertRaises(ChunkRetirementError):
            self.admit(original)
        self.assertEqual(self.baseline(original['receipt']), original)

    def test_admission_requires_existing_source_authority_before_archive(self):
        original = self.prepare()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s', (self.tenant,self.source,self.principal))
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
            with self.assertRaises(ChunkRetirementError):
                self.admit(original)
        read.assert_not_called()


    def state(self, original):
        with self.store.connect() as connection:
            return connection.execute('''SELECT document.chunk_receipt_profile,
                (SELECT count(*) FROM canonical_chunks chunk WHERE chunk.tenant_id=document.tenant_id
                 AND chunk.source_id=document.source_id AND chunk.document_id=document.document_id) AS chunks
                FROM canonical_documents document WHERE tenant_id=%s AND source_id=%s AND document_id=%s''',
                (self.tenant, self.source, original['document_id'])).fetchone()

    def test_admission_runtime_contract_refuses_before_archive(self):
        from recall_server.chunk_retirement import retire_singleton_metadata
        original = self.prepare()
        for body_mode, search_plane, flag in (('postgres', 'turbopuffer', True),
                                              ('archive', 'sql', True), ('archive', 'turbopuffer', False)):
            with self.subTest(body_mode=body_mode, search_plane=search_plane, flag=flag):
                with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': body_mode}), patch.object(self.store, 'search_plane', search_plane), patch.object(self.store, 'verified_singleton_receipts', flag), patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as read:
                    with self.assertRaisesRegex(ChunkRetirementError, 'archive_runtime_required'):
                        retire_singleton_metadata(self.store, self.archive, tenant_id=self.tenant,
                            source_id=self.source, principal_id=self.principal,
                            document_ids=(original['document_id'],))
                read.assert_not_called()
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_archive_reads_hold_no_sql_checkout(self):
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
            self.admit(original)
        self.assertGreater(reads, 0)

    def test_admission_rechecks_revocation_after_archive_proof(self):
        from recall_server import chunk_retirement as owner
        original = self.prepare()
        preview = self.admit(original)
        proof = owner.retire_current_chunks
        def revoke_after_proof(*args, **kwargs):
            result = proof(*args, **kwargs)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                    (self.tenant, self.source, self.principal))
            return result
        with patch.object(owner, 'retire_current_chunks', revoke_after_proof):
            with self.assertRaises(ChunkRetirementError):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_rechecks_catalog_and_late_edit_after_archive_proof(self):
        from recall_server import chunk_retirement as owner
        original = self.prepare()
        for action in ('catalog', 'edit'):
            preview = self.admit(original)
            proof = owner.retire_current_chunks
            def change_after_proof(*args, **kwargs):
                result = proof(*args, **kwargs)
                if action == 'edit':
                    self.ingest('turn/admit', 'Later current revision must retain its physical metadata')
                else:
                    with self.store.connect() as connection:
                        connection.execute('UPDATE canonical_evidence_documents SET created_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
                return result
            with self.subTest(action=action), patch.object(owner, 'retire_current_chunks', change_after_proof):
                with self.assertRaises(ChunkRetirementError):
                    self.admit(original, apply=True, reviewed_plan=preview['plan'])
            self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_forget_during_archive_proof_never_marks_deleted_document(self):
        from recall_server import chunk_retirement as owner
        original = self.prepare()
        preview = self.admit(original)
        proof = owner.retire_current_chunks
        def forget_after_proof(*args, **kwargs):
            result = proof(*args, **kwargs)
            self.ingest('turn/admit', '', tombstone=True)
            return result
        with patch.object(owner, 'retire_current_chunks', forget_after_proof):
            with self.assertRaises(ChunkRetirementError):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})
        self.assertIsNone(self.store.resolve(original['receipt'], tenant_id=self.tenant, authorized_sources=(self.source,)))

    def test_admission_real_native_lock_refuses_without_removal(self):
        from recall_server.chunk_retirement import _try_parent_native_locks
        original = self.prepare()
        preview = self.admit(original)
        with self.store.connect() as competing, competing.transaction():
            self.assertTrue(_try_parent_native_locks(competing.execute, (self.tenant, self.source), [original]))
            with self.assertRaisesRegex(ChunkRetirementError, 'lock_busy'):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_rollback_after_actual_delete_restores_row_and_profile(self):
        original = self.prepare()
        preview = self.admit(original)
        execute = self.store._execute_bounded
        deleted = False
        def delete_then_fail(connection, sql, values, deadline_at):
            nonlocal deleted
            result = execute(connection, sql, values, deadline_at)
            if sql.lstrip().startswith('DELETE FROM canonical_chunks'):
                self.assertEqual(result.rowcount, 1)
                deleted = True
                raise ChunkRetirementError('synthetic failure after actual delete')
            return result
        with patch.object(self.store, '_execute_bounded', delete_then_fail):
            with self.assertRaisesRegex(ChunkRetirementError, 'synthetic failure'):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertTrue(deleted)
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})
        self.assertEqual(self.baseline(original['receipt']), original)

    def test_admission_commit_lost_ack_keeps_profile_and_never_replays(self):
        from contextlib import contextmanager
        import psycopg
        from recall_server.receipt_membership import lookup_receipt_metadata
        original = self.prepare()
        preview = self.admit(original)
        execute, transaction = self.store._execute_bounded, psycopg.Connection.transaction
        armed = False
        def arm_after_delete(connection, sql, values, deadline_at):
            nonlocal armed
            result = execute(connection, sql, values, deadline_at)
            if sql.lstrip().startswith('DELETE FROM canonical_chunks'):
                armed = True
            return result
        @contextmanager
        def commit_then_disconnect(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic commit acknowledgement lost')
        with patch.object(self.store, '_execute_bounded', arm_after_delete), patch.object(psycopg.Connection, 'transaction', commit_then_disconnect):
            with self.assertRaises(psycopg.OperationalError):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': 1, 'chunks': 0})
        self.assertEqual(lookup_receipt_metadata(self.store, tenant_id=self.tenant,
            authorized_sources=(self.source,), receipts=(original['receipt'],),
            allow_verified_singletons=True), {original['receipt']: original})
        with self.assertRaises(ChunkRetirementError):
            self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': 1, 'chunks': 0})


    def test_admission_rejects_legacy_embedding_dependency_without_cascade(self):
        original = self.prepare()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunk_embeddings
                (tenant_id,source_id,chunk_id,model,dimensions,content_sha256,runtime_fingerprint,embedding)
                VALUES (%s,%s,%s,'synthetic',512,%s,'synthetic',%s::halfvec)''',
                (self.tenant, self.source, original['chunk_id'], original['text_sha256'], '['+','.join(['0']*512)+']'))
        with self.assertRaisesRegex(ChunkRetirementError, 'dependency_present'):
            self.admit(original)
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunk_embeddings WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 1)

    def test_admission_rejects_contradictory_physical_metadata(self):
        original = self.prepare()
        for assignment in ("ordinal=1", "deleted_at=now()", "chunk_id=chunk_id||'bad'", "text_sha256=repeat('0',64)"):
            with self.subTest(assignment=assignment), self.store.connect() as connection, connection.transaction(force_rollback=True):
                # The exact candidate uses this transaction so corrupted fixture
                # state is rolled back between independent contradiction cases.
                from contextlib import nullcontext
                from e2e_logical_evidence_projection import legacy_canonical_state
                installed = connection.execute('SELECT 1 FROM schema_migrations WHERE version=92').fetchone()
                # Only this mismatched ordinal needs historical fixture setup.
                # Restore the installed collector before testing native admission.
                fixture = legacy_canonical_state(connection) if installed and assignment == 'ordinal=1' else nullcontext()
                with fixture:
                    connection.execute('UPDATE canonical_chunks SET '+assignment+' WHERE tenant_id=%s AND receipt=%s', (self.tenant, original['receipt']))
                from recall_server.chunk_retirement import _singleton_metadata_candidates
                import time
                with self.assertRaises(ChunkRetirementError):
                    _singleton_metadata_candidates(self.store, connection, tenant_id=self.tenant,
                        source_id=self.source, document_ids=(original['document_id'],), deadline_at=time.monotonic()+5)
        self.assertEqual(self.baseline(original['receipt']), original)

    def test_admission_current_writer_rejects_mismatched_physical_ordinal(self):
        import psycopg
        with self.store.connect() as connection:
            if not connection.execute('SELECT 1 FROM schema_migrations WHERE version=92').fetchone():
                self.skipTest('schema091 permits historical contradictory metadata')
        original = self.prepare()
        with self.assertRaisesRegex(psycopg.errors.CheckViolation, 'canonical_physical_receipt_identity_invalid'):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET ordinal=1 WHERE tenant_id=%s AND receipt=%s',
                    (self.tenant, original['receipt']))
        self.assertEqual(self.baseline(original['receipt']), original)
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_leaves_unrelated_metadata_unchanged(self):
        original = self.prepare()
        other = self.ingest('turn/unrelated', 'Unrelated physical receipt remains present')
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        with self.store.connect() as connection:
            before = connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s AND receipt=%s', (self.tenant, other['receipt'])).fetchone()['row']
        preview = self.admit(original)
        self.admit(original, apply=True, reviewed_plan=preview['plan'])
        with self.store.connect() as connection:
            after = connection.execute('SELECT to_jsonb(chunk) AS row FROM canonical_chunks chunk WHERE tenant_id=%s AND receipt=%s', (self.tenant, other['receipt'])).fetchone()['row']
        self.assertEqual(after, before)


    def test_admission_rejects_more_than_one_physical_chunk(self):
        original = self.prepare()
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunks
                (tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,chunk_id||'extra',document_id,1,
                    replace(receipt,'#item=0','#item=1'),'',text_sha256
                FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s''', (self.tenant, original['receipt']))
        with self.assertRaisesRegex(ChunkRetirementError, 'target_ineligible'):
            self.admit(original)
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 2})

    def test_admission_new_parent_work_after_proof_refuses_old_review(self):
        from recall_server import chunk_retirement as owner
        original = self.prepare()
        preview = self.admit(original)
        proof = owner.retire_current_chunks
        def append_after_proof(*args, **kwargs):
            result = proof(*args, **kwargs)
            self.ingest('turn/later', 'New parent work must prevent stale catalog admission')
            return result
        with patch.object(owner, 'retire_current_chunks', append_after_proof):
            with self.assertRaisesRegex(ChunkRetirementError, 'proof_changed'):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_archive_failure_keeps_physical_receipt(self):
        original = self.prepare()
        preview = self.admit(original)
        with patch.object(self.archive, 'read_raw', side_effect=OSError('synthetic unavailable archive')):
            with self.assertRaises(ChunkRetirementError):
                self.admit(original, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(self.state(original), {'chunk_receipt_profile': None, 'chunks': 1})

    def test_admission_mixed_cohort_is_atomic_when_one_document_becomes_invalid(self):
        from recall_server import chunk_retirement as owner
        original = self.prepare()
        second = self.baseline(self.ingest('turn/second', 'Second exact singleton')['receipt'])
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        scope = dict(tenant_id=self.tenant, source_id=self.source,
            document_ids=tuple(sorted((original['document_id'], second['document_id']))))
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}), patch.object(self.store, 'verified_singleton_receipts', True), patch.object(self.store, 'search_plane', 'turbopuffer'):
            bodies = retire_current_chunks(self.store, self.archive, **scope)
            retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=bodies['plan'])
            preview = owner.retire_singleton_metadata(self.store, self.archive, principal_id=self.principal, **scope)
            proof = owner.retire_current_chunks
            def invalidate_one_after_proof(*args, **kwargs):
                result = proof(*args, **kwargs)
                with self.store.connect() as connection:
                    connection.execute("UPDATE canonical_chunks SET created_at=created_at-interval '1 second' WHERE tenant_id=%s AND receipt=%s", (self.tenant, second['receipt']))
                return result
            with patch.object(owner, 'retire_current_chunks', invalidate_one_after_proof):
                with self.assertRaises(ChunkRetirementError):
                    owner.retire_singleton_metadata(self.store, self.archive, principal_id=self.principal,
                        **scope, apply=True, reviewed_plan=preview['plan'])
        for row in (original, second):
            self.assertEqual(self.state(row), {'chunk_receipt_profile': None, 'chunks': 1})


from e2e_singleton_receipt_integration import SingletonIntegrationTests


class ActualAdmissionLifecycle(SingletonIntegrationTests):
    setUp = SingletonMetadataAdmission.setUp

    def admit_fixture(self, document_id):
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        scope = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(document_id,))
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            body_plan = retire_current_chunks(self.store, self.archive, **scope)['plan']
            retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=body_plan)
        from recall_server.chunk_retirement import retire_singleton_metadata
        with patch.object(self.store, 'verified_singleton_receipts', True), patch.object(self.store, 'search_plane', 'turbopuffer'), patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            preview = retire_singleton_metadata(self.store, self.archive, principal_id=self.principal, **scope)
            result = retire_singleton_metadata(self.store, self.archive, principal_id=self.principal,
                **scope, apply=True, reviewed_plan=preview['plan'])
        self.assertEqual(result['retired_chunks'], 1)


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        case(name) for case, prefix in ((SingletonMetadataAdmission, 'test_admission_'), (ActualAdmissionLifecycle, 'test_integration_'))
        for name in dir(case) if name.startswith(prefix)))
    raise SystemExit(not result.wasSuccessful())
