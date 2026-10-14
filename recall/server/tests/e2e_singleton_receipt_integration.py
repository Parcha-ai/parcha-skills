#!/usr/bin/env python3
"""Exact public receipt behavior after separately verified singleton admission."""
import unittest
from contextlib import contextmanager
import psycopg
from unittest.mock import patch
import e2e_receipt_membership as fixtures


class SingletonIntegrationTests(fixtures.ReceiptMembershipTests):
    def forget_request(self, receipt):
        return dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-14T00:01:00Z', idempotency_key='singleton-forget')

    def test_integration_resolve_matches_original_public_response(self):
        from recall_server.db import BrainStore
        written = self.ingest('turn/public', 'Exact public body α 🧠')
        receipt = written['receipt']
        original = self.store.resolve(receipt, tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        document = self.baseline(receipt)
        self.admit_fixture(document['document_id'])
        enabled = BrainStore(self.store.dsn, verified_singleton_receipts=True)
        self.addCleanup(enabled.close)
        resolved = enabled.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive)
        self.assertEqual(resolved, original)

    def test_integration_edit_preserves_exact_old_revision_and_replay(self):
        written = self.ingest('turn/edit', 'Original revision α')
        receipt = written['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,),
                     chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        document = self.baseline(receipt)
        self.admit_fixture(document['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            replay = self.ingest('turn/edit', 'Original revision α')
            self.assertEqual(replay['receipt'], receipt)
            newer = self.ingest('turn/edit', 'Changed revision β')
            self.assertNotEqual(newer['receipt'], receipt)
            self.assertEqual(self.store.resolve(receipt, **scope), original)
            self.assertEqual(self.store.resolve(newer['receipt'], **scope)['items'][0]['text_redacted'],
                             'Changed revision β')

    def test_integration_forget_preserves_count_and_idempotent_retry(self):
        receipt = self.ingest('turn/forget', 'Forget this exact admitted body')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        request = self.forget_request(receipt)
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            result = self.plane.forget(request)
            self.assertEqual(result['status'], 'deleted')
            self.assertEqual(result['projections_deleted'], 1)
            self.assertTrue(self.plane.forget(request)['replay'])
            self.assertIsNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def test_integration_failed_archive_delete_stays_hidden_and_can_finish(self):
        from recall_server.canonical import CanonicalLifecycleError
        receipt = self.ingest('turn/retry', 'A failed physical delete remains hidden')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        request = self.forget_request(receipt)
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            with patch.object(self.archive, 'delete_internal_raw_bounded', side_effect=OSError('synthetic deletion failure')):
                with self.assertRaisesRegex(CanonicalLifecycleError, 'archive_delete_failed'):
                    self.plane.forget(request)
            self.assertEqual(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive)['items'], [])
            self.assertEqual(self.plane.forget(request)['status'], 'deleted')

    def test_integration_full_publication_preserves_admitted_parent_member(self):
        receipt = self.ingest('turn/retained', 'The archived member must remain in the rebuilt parent')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,),
                     chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            added = self.ingest('turn/new', 'A new parent member')
            report = self.projector.project_pending(tenant_id=self.tenant,
                batch_size=10, max_batches=1, upload_concurrency=1)
            self.assertEqual(report['failed'], 0)
            self.assertEqual(report['documents'], 1)
            self.assertEqual(self.store.resolve(receipt, **scope), original)
            self.assertEqual(self.store.resolve(added['receipt'], **scope)['items'][0]['text_redacted'],
                             'A new parent member')

    def test_integration_retention_change_cannot_remove_sole_admitted_body(self):
        receipt = self.ingest('turn/retention', '{"type":"custom-excluded","text":"Durable evidence"}')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,),
                     chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            self.ingest('turn/next', 'Another parent member')
            with patch.object(self.projector, 'excluded_structural_types', frozenset({'custom-excluded'})):
                report = self.projector.project_pending(tenant_id=self.tenant,
                    batch_size=10, max_batches=1, upload_concurrency=1)
            self.assertEqual(report['failed'], 1)
            self.assertEqual(self.store.resolve(receipt, **scope), original)

    def test_integration_batch_edit_uses_the_same_outgoing_history_owner(self):
        receipt = self.ingest('turn/batch', 'Original scalar body')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,), chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        def batch(**kwargs):
            return self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                          events=[kwargs['envelope']])
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            with patch.object(self.plane, 'ingest_document', batch):
                result = self.ingest('turn/batch', 'New batch body')
            self.assertEqual(result['inserted'], 1)
            self.assertEqual(self.store.resolve(receipt, **scope), original)

    def test_integration_forget_during_history_preparation_cannot_resurrect(self):
        from recall_server.canonical import CanonicalLifecycleError
        receipt = self.ingest('turn/race', 'Original before deletion')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        prepare = self.plane.prepare_history
        @contextmanager
        def forgotten_after_preparation(**kwargs):
            with prepare(**kwargs) as staged:
                self.plane.forget(self.forget_request(receipt))
                yield staged
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            with patch.object(self.plane, 'prepare_history', forgotten_after_preparation):
                with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_identity_forgotten'):
                    self.ingest('turn/race', 'This must not resurrect')
            self.assertIsNone(self.store.resolve(receipt, tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def test_integration_lost_edit_commit_ack_does_not_change_retry_identity(self):
        receipt = self.ingest('turn/unknown', 'Before uncertain commit')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,), chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        connect, restore = self.store.connect, self.plane._restore_history
        restored = [False]
        def track_restore(*args, **kwargs):
            restore(*args, **kwargs)
            restored[0] = True
        @contextmanager
        def lost_ack():
            with connect() as connection:
                yield connection
            if restored[0]:
                raise psycopg.OperationalError('synthetic acknowledgment lost after COMMIT')
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            with patch.object(self.plane, '_restore_history', track_restore), patch.object(self.store, 'connect', lost_ack):
                with self.assertRaises(psycopg.OperationalError):
                    self.ingest('turn/unknown', 'Committed changed body')
            replay = self.ingest('turn/unknown', 'Committed changed body')
            self.assertTrue(replay['replay'])
            self.assertEqual(self.store.resolve(receipt, **scope), original)
            self.assertEqual(self.store.resolve(replay['receipt'], **scope)['items'][0]['text_redacted'],
                             'Committed changed body')

    def test_integration_deleted_during_archive_read_is_not_returned(self):
        from recall_server.chunk_bodies import ChunkBodyError
        receipt = self.ingest('turn/revoked', 'Do not return after deletion')['receipt']
        document = self.baseline(receipt)
        self.admit_fixture(document['document_id'])
        read, connect = self.archive.read_raw, self.store.connect
        active = [0]
        @contextmanager
        def tracked():
            with connect() as connection:
                active[0] += 1
                try:
                    yield connection
                finally:
                    active[0] -= 1
        def revoke(reference):
            self.assertEqual(active[0], 0, 'archive read holds a SQL lease')
            payload = read(reference)
            with connect() as connection:
                connection.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s AND document_id=%s',
                                   (self.tenant, document['document_id']))
            return payload
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            with patch.object(self.store, 'connect', tracked), patch.object(self.archive, 'read_raw', revoke):
                with self.assertRaises(ChunkBodyError):
                    self.store.resolve(receipt, tenant_id=self.tenant,
                        authorized_sources=(self.source,), chunk_body_archive=self.archive)

    def test_integration_lost_publication_ack_keeps_committed_receipts_readable(self):
        receipt = self.ingest('turn/publish-unknown', 'Admitted before unknown publication')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,), chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        commit = self.projector._commit
        def lost_ack(*args, **kwargs):
            commit(*args, **kwargs)
            raise psycopg.OperationalError('synthetic acknowledgment lost after publication COMMIT')
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            added = self.ingest('turn/added-unknown', 'Newly committed member')
            with patch.object(self.projector, '_commit', lost_ack):
                with self.assertRaises(psycopg.OperationalError):
                    self.projector.project_pending(tenant_id=self.tenant,
                        batch_size=10, max_batches=1, upload_concurrency=1)
            self.projector.drain_cleanup(tenant_id=self.tenant)
            self.assertEqual(self.store.resolve(receipt, **scope), original)
            self.assertEqual(self.store.resolve(added['receipt'], **scope)['items'][0]['text_redacted'],
                             'Newly committed member')

    def test_integration_append_keeps_admitted_prefix_without_reading_its_sql_body(self):
        ingest = self.plane.ingest_document
        ordinal = [10]
        def positioned(**kwargs):
            kwargs['envelope']['provenance']['byte_start'] = ordinal[0]
            return ingest(**kwargs)
        with patch.object(self.plane, 'ingest_document', positioned):
            receipt = self.ingest('turn/prefix', 'Archived prefix member')['receipt']
        scope = dict(tenant_id=self.tenant, authorized_sources=(self.source,), chunk_body_archive=self.archive)
        original = self.store.resolve(receipt, **scope)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        records, observed = self.projector._record_stream, []
        def observe(rows, **kwargs):
            def inputs():
                for row in rows:
                    observed.append(row['native_id'])
                    yield row
            return records(inputs(), **kwargs)
        with patch.object(self.store, 'verified_singleton_receipts', True, create=True):
            ordinal[0] = 20
            with patch.object(self.plane, 'ingest_document', positioned):
                added = self.ingest('turn/suffix', 'New appended member')
            with patch.object(self.projector, 'append_enabled', True), patch.object(self.projector, '_record_stream', observe):
                report = self.projector.project_pending(tenant_id=self.tenant,
                    batch_size=1, max_batches=1, upload_concurrency=1)
            self.assertEqual(report['failed'], 0)
            self.assertEqual(observed, ['turn/suffix'])
            self.assertEqual(self.store.resolve(receipt, **scope), original)
            self.assertEqual(self.store.resolve(added['receipt'], **scope)['items'][0]['text_redacted'],
                             'New appended member')


if __name__ == '__main__':
    names = [name for name in dir(SingletonIntegrationTests) if name.startswith('test_integration_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        SingletonIntegrationTests(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
