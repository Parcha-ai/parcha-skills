#!/usr/bin/env python3
"""Whole-parent logical deletion must retain an admitted survivor's sole body."""
import os
import unittest
from unittest.mock import patch
import e2e_receipt_membership as membership
import e2e_singleton_metadata_admission as admission
from recall_server.canonical import CanonicalLifecycleError, CanonicalPlane
from recall_server.chunk_retirement import retire_current_chunks, retire_singleton_metadata
from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.chunk_bodies import ChunkBodyError


class SingletonSurvivorFence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        membership.ReceiptMembershipTests.setUpClass.__func__(cls)

    @classmethod
    def tearDownClass(cls):
        membership.ReceiptMembershipTests.tearDownClass.__func__(cls)

    setUp = admission.SingletonMetadataAdmission.setUp
    ingest = membership.ReceiptMembershipTests.ingest
    baseline = membership.ReceiptMembershipTests.baseline

    def prepare(self):
        self.written = [self.ingest('turn/' + name, 'Exact archived survivor ' + name) for name in ('first', 'second')]
        self.original = [self.baseline(row['receipt']) for row in self.written]
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        self.store.verified_singleton_receipts = True
        self.addCleanup(setattr, self.store, 'verified_singleton_receipts', False)
        mode = patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'})
        mode.start(); self.addCleanup(mode.stop)
        plane = patch.object(self.store, 'search_plane', 'turbopuffer')
        plane.start(); self.addCleanup(plane.stop)
        scope = dict(tenant_id=self.tenant, source_id=self.source,
                     document_ids=tuple(row['document_id'] for row in self.original))
        preview = retire_current_chunks(self.store, self.archive, **scope)
        retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=preview['plan'])
        scope['principal_id'] = self.principal
        preview = retire_singleton_metadata(self.store, self.archive, **scope)
        retire_singleton_metadata(self.store, self.archive, **scope, apply=True, reviewed_plan=preview['plan'])
        self.response = self.resolve(1)
        self.assertIsNotNone(self.response)

    def resolve(self, index):
        return self.store.resolve(self.written[index]['receipt'], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)

    def test_direct_whole_parent_delete_refuses_admitted_survivor(self):
        self.prepare()
        with self.assertRaisesRegex(LogicalEvidenceError, 'logical_evidence_survivor_body_required'):
            self.projector.delete_native_ids(tenant_id=self.tenant, source_id=self.source,
                                            native_ids=['turn/first'])
        self.assertEqual(self.resolve(1), self.response)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)

    def test_injected_public_forget_refuses_without_destroying_survivor(self):
        self.prepare()
        plane = CanonicalPlane(self.store, self.archive, evidence_projector=self.projector,
                               chunk_body_archive=self.archive)
        request = dict(contract='recall.forget-request.v1', schema_version=1, tenant_id=self.tenant,
            principal_id=self.principal, source_id=self.source, target_receipt=self.written[0]['receipt'],
            mode='explicit_forget', reason='owner_requested', requested_at='2026-10-14T00:00:00Z',
            idempotency_key='singleton-survivor-forget')
        for attempt in range(2):
            with self.subTest(attempt=attempt), self.assertRaisesRegex(CanonicalLifecycleError, 'archive_delete_failed'):
                plane.forget(request)
            self.assertEqual(self.resolve(1), self.response)
        self.assertEqual(self.resolve(0)['items'], [])

    def test_unverified_profile_marker_can_only_refuse_deletion(self):
        self.prepare()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=9 WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.original[1]['document_id']))
        with self.assertRaisesRegex(LogicalEvidenceError, 'logical_evidence_survivor_body_required'):
            self.projector.delete_native_ids(tenant_id=self.tenant, source_id=self.source,
                                            native_ids=['turn/first'])
        with self.store.connect() as connection:
            self.assertIsNotNone(connection.execute('SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone())

    def test_whole_parent_delete_with_all_members_keeps_existing_cleanup(self):
        self.prepare()
        self.projector.delete_native_ids(tenant_id=self.tenant, source_id=self.source,
                                        native_ids=['turn/first', 'turn/second'])
        with self.store.connect() as connection:
            self.assertIsNone(connection.execute('SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone())
        with self.assertRaisesRegex(ChunkBodyError, 'archived_chunk_body_unavailable'):
            self.resolve(1)


if __name__ == '__main__':
    unittest.main()
