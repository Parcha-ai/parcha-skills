"""Actual retirement with repeated windows over the same immutable part."""
from dataclasses import replace
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import e2e_selective_retirement_experiment as original
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import SmallPartProjection
from recall_server import chunk_retirement as owner, retirement_runner as runner
from recall_server.logical_evidence import LogicalEvidenceProjectionStore


class RunnerPartReuse(unittest.TestCase):
    setUp = original.SelectiveExperiment.setUp
    state = original.SelectiveExperiment.state

    def one_part_parent(self, count=256):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        def normal_parts(projection, **values):
            self.assertEqual(projection.store.active_connections, 0)
            return LogicalEvidenceProjectionStore.put_records(projection, **values, part_bytes=1024**2)
        with patch.object(SmallPartProjection, 'put_records', normal_parts):
            self.tenant, self.source, self.archive, self.projection, self.projector, _ = fixture(self.store, Path(root.name), count=count)
        self.scope = self.tenant, self.source, 'session'
        with self.store.connect() as connection:
            connection.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())", (self.tenant,self.principal,self.source))
            parts = connection.execute('SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s', (self.tenant,)).fetchall()
        self.assertEqual(len(parts), 1)
        owner.set_parent_retirement_enabled(self.store, tenant_id=self.tenant, source_id=self.source, native_parent_id='session', enabled=True)
        self.archive.reads.clear()
        return parts[0]['size_bytes']

    def retire(self, metadata_mode='keep', *, max_batches=3):
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        return runner.run_retirement(self.store, self.archive,
            scope=runner.RetirementScope(self.tenant,self.principal,(self.source,)), apply=True,
            limits=runner.RunnerLimits(max_parents=1), parent_limits=replace(self.limits,batch_documents=64,max_batches=max_batches),
            # This 256-document fixture checks proof and reuse, not host speed.
            deadline_at=time.monotonic()+60, metadata_mode=metadata_mode)

    def test_three_windows_use_one_fetch_and_preserve_nonselected_rows(self):
        part_bytes = self.one_part_parent()
        before = self.state()
        result = self.retire()
        after = self.state()
        self.assertEqual(result['cleared_documents'], 192)
        self.assertEqual(result['batches'], 3)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(result['archive_gets'], 6)
        self.assertEqual(result['archive_bytes'], 6*part_bytes)
        self.assertEqual(result['archive_reused_gets'], 5)
        self.assertEqual(result['archive_reused_bytes'], 5*part_bytes)
        self.assertEqual(sum(self.archive.reads.values()), 1)
        self.assertEqual(sum(old != new for old,new in zip(before,after)), 192)
        self.assertEqual([r for r in after if r['text_redacted']], [r for r in before if r in after])
        with self.store.connect() as connection:
            progress = owner._parent_progress(self.store,connection,self.scope,time.monotonic()+5)
        self.assertEqual(progress['last_record_ordinal'], 191)
        self.assertEqual(progress['status'], 'partial')

    def test_final_tail_keeps_original_full_proof(self):
        from recall_server.parent_chunk_proof import prove_parent_chunks
        self.one_part_parent(192)
        with patch('recall_server.parent_chunk_proof.prove_parent_chunks', wraps=prove_parent_chunks) as proof:
            # Three selected-body commits plus the unchanged EOF completion.
            result = self.retire(max_batches=4)
        self.assertEqual(proof.call_count, 1)
        self.assertEqual(result['cleared_documents'], 192)
        self.assertEqual(result['completed_parent_proofs'], 1)
        self.assertEqual(sum(self.archive.reads.values()), 1)

    def test_singleton_windows_preserve_public_receipt_after_metadata_removal(self):
        # Synthetic insert_record uses different chunk IDs. Use the real writer
        # for the exact singleton admission and public receipt contract.
        helper = original.SelectiveExperiment.actual_writer_fixture(self)
        for index in range(254):
            helper.ingest(f'turn/{index:04}', f'Canonical singleton body {index}')
        report = self.projector.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        self.assertEqual(report['failed'], 0)
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='session',enabled=True)
        with self.store.connect() as connection:
            receipt = connection.execute('SELECT receipt FROM canonical_chunks JOIN canonical_documents USING(tenant_id,source_id,document_id) WHERE tenant_id=%s ORDER BY body_record_ordinal LIMIT 1', (self.tenant,)).fetchone()['receipt']
        with patch.object(self.store,'verified_singleton_receipts',True), patch.object(self.store,'search_plane','turbopuffer'):
            public = self.store.resolve(receipt,tenant_id=self.tenant,authorized_sources=(self.source,),chunk_body_archive=self.archive)
            self.assertIsNotNone(public)
            self.assertEqual(len(public['items']), 1)
            self.assertTrue(public['items'][0]['text_redacted'])
            with patch.object(self.archive,'read_raw',wraps=self.archive.read_raw) as read:
                result = self.retire(metadata_mode='singleton')
            self.assertEqual(result['retired_metadata_chunks'], 192, result)
            self.assertEqual(read.call_count, 1)
            self.assertEqual(self.store.resolve(receipt,tenant_id=self.tenant,authorized_sources=(self.source,),chunk_body_archive=self.archive), public)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 64)

    def test_new_epoch_refuses_even_with_retained_part(self):
        self.one_part_parent()
        commit = owner._retire_parent_batch
        def disable(*args, **kwargs):
            result = commit(*args, **kwargs)
            owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='session',enabled=False)
            return result
        with patch.object(owner,'_retire_parent_batch',side_effect=disable):
            result = self.retire()
        self.assertEqual(result['cleared_documents'], 64)
        self.assertEqual(result['errors'], {'parent_retirement_disabled':1})
        self.assertEqual(sum(self.archive.reads.values()), 1)

    def test_actual_forget_after_reused_proof_refuses_write(self):
        from recall_server.canonical import CanonicalPlane
        self.one_part_parent()
        plane = CanonicalPlane(self.store,self.archive,evidence_projector=self.projector,chunk_body_archive=self.archive)
        commit = owner._retire_parent_batch
        after_forget = []
        def forget(*args, **kwargs):
            native = kwargs['rows'][0]['native_id']
            result = plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
                tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
                target_receipt=f'recall://{self.source}/{native}?rev=1#item=0',mode='explicit_forget',
                reason='owner_requested',requested_at='2026-10-16T00:00:00Z',idempotency_key='reuse-forget'))
            self.assertEqual(result['status'], 'deleted')
            after_forget.extend(self.state())
            return commit(*args, **kwargs)
        with patch.object(owner,'_retire_parent_batch',side_effect=forget):
            result = self.retire()
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['failed_parents'], 1)
        self.assertGreater(result['archive_reused_gets'], 0)
        self.assertEqual(self.state(), after_forget)

    def test_changed_publication_after_reused_proof_refuses_write(self):
        from e2e_logical_evidence_projection import insert_record
        from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
        self.one_part_parent()
        commit = owner._retire_parent_batch
        after_publication = []
        def publish(*args, **kwargs):
            with self.store.connect() as connection:
                insert_record(connection,tenant=self.tenant,source=self.source,parent='session',native='new-event',
                    text='New appended evidence',role='assistant',byte_start=9999)
                mark_logical_evidence_dirty(connection,tenant_id=self.tenant,source_id=self.source,native_ids=['new-event'],reason='ingest')
            report = self.projector.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
            self.assertEqual(report['failed'], 0)
            self.assertEqual(report['documents'], 1)
            after_publication.extend(self.state())
            return commit(*args, **kwargs)
        with patch.object(owner,'_retire_parent_batch',side_effect=publish):
            result = self.retire()
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['failed_parents'], 1)
        self.assertGreater(result['archive_reused_gets'], 0)
        self.assertEqual(self.state(), after_publication)

    def test_corrupt_first_fetch_cannot_clear_anything(self):
        self.one_part_parent()
        before = self.state()
        read = self.archive.read_raw
        def damaged(reference):
            return b'x'*len(read(reference))
        with patch.object(self.archive,'read_raw',side_effect=damaged):
            result = self.retire()
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['failed_parents'], 1)
        self.assertEqual(result['archive_reused_gets'], 0)
        self.assertEqual(self.state(), before)


if __name__ == '__main__':
    unittest.main()
