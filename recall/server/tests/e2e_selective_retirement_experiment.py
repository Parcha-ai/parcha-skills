#!/usr/bin/env python3
"""LOCAL experiment only: selected proof feeds the unchanged bounded writer."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server import chunk_retirement as owner
from recall_server.chunk_bodies import chunk_catalog_snapshot, read_archived_chunks
from recall_server.parent_chunk_proof import manifest_identity, read_parent_catalog, prove_parent_chunks


def selected_proof(store, archive, scope, principal, documents, limits, deadline):
    """Test adapter, deliberately not installed in any worker or runtime path."""
    if not documents or len(documents) != len(set(documents)) or len(documents) > min(100, limits.batch_documents):
        raise ValueError('selected_document_bound')
    documents = tuple(sorted(documents))
    with store.connect() as c, c.transaction():
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        owner.require_retirement_owner(store, c, scope[:2], principal, deadline, lock=False)
        current = read_parent_catalog(store, c, scope, deadline)
        progress = owner._parent_progress(store, c, scope, deadline)
        if not progress or not progress['enabled']:
            raise ValueError('disabled')
        catalog = chunk_catalog_snapshot(store, tenant_id=scope[0], source_ids=(scope[1],),
            document_ids=documents, deadline_at=deadline, connection=c, located_only=True)
        if set(catalog) != {(scope[1], d) for d in documents}:
            raise ValueError('selected_ineligible')
        physical = store._execute_bounded(c, '''SELECT document_id,ordinal,receipt,text_sha256,
            octet_length(text_redacted) AS pg_bytes FROM canonical_chunks
            WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)
              AND deleted_at IS NULL ORDER BY document_id,ordinal''',
            (*scope[:2], list(documents)), deadline).fetchall()
    identity = manifest_identity(current['manifest'])
    parts = {}
    rows = []
    for row in catalog.values():
        if row['native_parent_id'] != scope[2] or manifest_identity(row['manifest']) != identity:
            raise ValueError('selected_parent')
        first, count = row['body_record_ordinal'], row['body_record_count']
        if type(first) is not int or type(count) is not int or first < 0 or count < 1:
            raise ValueError('selected_locator')
        for part in row['parts']:
            if first <= part['last_record_ordinal'] and first + count > part['first_record_ordinal']:
                parts[part['artifact_id']] = part
        lengths = {(r['ordinal'], r['receipt'], r['text_sha256']): r['pg_bytes']
                   for r in physical if r['document_id'] == row['document_id']}
        chunks = [dict(ch, pg_bytes=lengths.get((ch['ordinal'], ch['receipt'], ch['text_sha256']), 0))
                  for ch in row['chunks']]
        rows.append(dict(row, chunks=chunks, pg_body_bytes=sum(ch['pg_bytes'] for ch in chunks)))
    selected_bytes = sum(p['size_bytes'] for p in parts.values())
    if not parts or selected_bytes > limits.max_archive_bytes:
        raise ValueError('selected_archive_budget')
    # Exactly one grouped call. Reader owns part/event/receipt/content verification.
    archived = read_archived_chunks(store, archive, tenant_id=scope[0], source_ids=(scope[1],),
        document_ids=documents, located_only=True, deadline_at=deadline)
    if set(archived) != set(catalog):
        raise ValueError('selected_archive_missing')
    for row in rows:
        actual = archived[(scope[1], row['document_id'])]
        if len(actual) != len(row['chunks']) or any(a['ordinal'] != b['ordinal'] or a['receipt'] != b['receipt']
                or hashlib.sha256(a['text_redacted'].encode()).hexdigest() != b['text_sha256']
                for a, b in zip(actual, row['chunks'])):
            raise ValueError('selected_archive_changed')
    after = chunk_catalog_snapshot(store, tenant_id=scope[0], source_ids=(scope[1],),
        document_ids=documents, located_only=True, deadline_at=deadline)
    if after != catalog:
        raise ValueError('selected_catalog_changed')
    return dict(manifest=identity, scope_epoch=progress['scope_epoch']), rows, dict(
        selected_documents=len(rows), selected_part_bytes=selected_bytes, selected_part_ids=set(parts))


class SelectiveExperiment(unittest.TestCase):
    def setUp(self):
        self.store = TrackedStore(os.environ['RECALL_DATABASE_URL'])
        self.addCleanup(self.store.close)
        self.store.migrate()
        root = tempfile.TemporaryDirectory(); self.addCleanup(root.cleanup)
        self.tenant,self.source,self.archive,self.projection,self.projector,self.texts = fixture(self.store,Path(root.name),count=12)
        self.scope=(self.tenant,self.source,'session'); self.principal='principal:reprojection'
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",(self.tenant,self.principal,self.source))
            self.docs=c.execute('SELECT document_id,native_id FROM canonical_documents WHERE tenant_id=%s ORDER BY native_id',(self.tenant,)).fetchall()
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='session',enabled=True)
        self.limits=owner.ParentRetirementLimits(); self.deadline=time.monotonic()+30
        self.env=patch.dict(os.environ,RECALL_CHUNK_BODY_READS='archive');self.env.start();self.addCleanup(self.env.stop)

    def capture(self, indexes=(2,3), limits=None):
        return selected_proof(self.store,self.archive,self.scope,self.principal,
            tuple(self.docs[i]['document_id'] for i in indexes),limits or self.limits,self.deadline)

    def apply(self, proof, rows, metadata_mode="keep"):
        return owner._retire_parent_batch(self.store,proof=proof,rows=rows,scope=self.scope,
            limits=self.limits,deadline_at=self.deadline,complete=False,
            remaining_bytes=self.limits.max_clear_bytes,owner_principal_id=self.principal,metadata_mode=metadata_mode)

    def state(self):
        with self.store.connect() as c:
            return c.execute('SELECT document_id,ordinal,text_redacted FROM canonical_chunks WHERE tenant_id=%s ORDER BY document_id,ordinal',(self.tenant,)).fetchall()

    def test_selected_matches_whole_parent_oracle_and_nonselected_survive(self):
        with prove_parent_chunks(self.store,self.archive,scope=self.scope,limits=self.limits,deadline_at=self.deadline) as whole:
            oracle={r['document_id']:r for r in whole['spool'].verified()}
        self.archive.reads.clear(); before=self.state();statements=[]
        execute=self.store._execute_bounded
        def measured(connection,sql,values,deadline):
            statements.append(sql)
            return execute(connection,sql,values,deadline)
        with patch.object(self.store,'_execute_bounded',measured):proof,rows,cost=self.capture()
        self.assertFalse(any('retirement_metadata' in str(q) for q in statements))
        print(json.dumps(dict(experiment='selected2_of12',sql_statements=len(statements),
            archive_gets=sum(self.archive.reads.values()),selected_part_bytes=cost['selected_part_bytes'],
            selected_documents=cost['selected_documents'],full_parent_metadata_capture=False)))
        for row in rows:
            self.assertEqual(row['chunks'],oracle[row['document_id']]['chunks'])
        self.assertEqual(set(self.archive.reads),cost['selected_part_ids'])
        self.assertTrue(all(n==1 for n in self.archive.reads.values()))
        result=self.apply(proof,rows); self.assertEqual(result['cleared_documents'],2)
        selected={r['document_id'] for r in rows}
        self.assertEqual(self.state(),[dict(r,text_redacted='' if r['document_id'] in selected else r['text_redacted'])for r in before])
        with self.store.connect() as c:
            progress=owner._parent_progress(self.store,c,self.scope,self.deadline)
        self.assertEqual(progress['status'],'partial')

    def test_duplicate_selected_ids_refuse_before_archive(self):
        self.assertRaises(ValueError,self.capture,(2,2));self.assertFalse(self.archive.reads)

    def test_aggregate_part_budget_refuses_before_archive(self):
        self.assertRaises(ValueError,self.capture,(2,3),owner.ParentRetirementLimits(max_archive_bytes=1))
        self.assertFalse(self.archive.reads)

    def test_corrupt_selected_part_refuses_before_clear(self):
        before=self.state();read=self.archive.read_raw
        with patch.object(self.archive,'read_raw',side_effect=lambda ref: b'x'*len(read(ref))):
            self.assertRaises(Exception,self.capture)
        self.assertEqual(self.state(),before)

    def test_revocation_between_proof_and_commit_refuses(self):
        proof,rows,_=self.capture();before=self.state()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s',(self.tenant,))
        self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows)
        self.assertEqual(self.state(),before)

    def test_epoch_change_between_proof_and_commit_refuses(self):
        proof,rows,_=self.capture();before=self.state()
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='session',enabled=False)
        self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows)
        self.assertEqual(self.state(),before)

    def test_changed_pg_body_refuses_without_clearing_other_rows(self):
        proof,rows,_=self.capture()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET text_redacted='corrupted' WHERE tenant_id=%s AND document_id=%s",(self.tenant,rows[0]['document_id']))
        before=self.state();self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows)
        self.assertEqual(self.state(),before)


    def test_split_event_reads_all_intersecting_parts_once(self):
        from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
        root = tempfile.TemporaryDirectory(); self.addCleanup(root.cleanup)
        with patch.object(CanonicalLogicalEvidenceProjector, '_text_segments', staticmethod(lambda text: (text[:500], text[500:]))):
            tenant,source,archive,*_ = fixture(self.store,Path(root.name),count=4)
        scope=(tenant,source,'session')
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",(tenant,self.principal,source))
            documents=c.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s ORDER BY native_id LIMIT 2',(tenant,)).fetchall()
        owner.set_parent_retirement_enabled(self.store,tenant_id=tenant,source_id=source,native_parent_id='session',enabled=True)
        proof,rows,cost=selected_proof(self.store,archive,scope,self.principal,tuple(d['document_id'] for d in documents),self.limits,self.deadline)
        self.assertTrue(all(row['body_record_count']==2 for row in rows))
        self.assertGreaterEqual(len(cost['selected_part_ids']),2)
        self.assertEqual(set(archive.reads),cost['selected_part_ids']);self.assertTrue(all(v==1 for v in archive.reads.values()))
        result=owner._retire_parent_batch(self.store,proof=proof,rows=rows,scope=scope,limits=self.limits,
            deadline_at=self.deadline,complete=False,remaining_bytes=self.limits.max_clear_bytes,owner_principal_id=self.principal)
        self.assertEqual(result['cleared_documents'],2)

    def test_structural_selection_refuses_and_does_not_clear(self):
        from psycopg.types.json import Jsonb
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET canonical_redacted=canonical_redacted || %s WHERE tenant_id=%s AND native_id=%s",(Jsonb({'type':'token_count'}),self.tenant,self.docs[2]['native_id']))
        before=self.state();self.assertRaises(Exception,self.capture,(2,));self.assertEqual(self.state(),before)
        self.assertFalse(self.archive.reads)

    def test_revoked_before_capture_performs_no_archive_io(self):
        with self.store.connect() as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s',(self.tenant,))
        self.assertRaises(owner.ChunkRetirementError,self.capture);self.assertFalse(self.archive.reads)

    def test_native_lock_overlap_refuses(self):
        proof,rows,_=self.capture();before=self.state()
        with self.store.connect() as c,c.transaction():
            c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(f"v2\x1f{self.tenant}\x1f{self.source}\x1f{rows[0]['native_id']}",))
            self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows)
        self.assertEqual(self.state(),before)

    def test_manifest_change_after_proof_refuses(self):
        proof,rows,_=self.capture();before=self.state()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_evidence_documents SET document_content_sha256=repeat('0',64) WHERE tenant_id=%s",(self.tenant,))
        self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows);self.assertEqual(self.state(),before)

    def test_actual_native_forget_after_proof_refuses(self):
        from recall_server.canonical import CanonicalPlane
        proof,rows,_=self.capture()
        plane=CanonicalPlane(self.store,self.archive,evidence_projector=self.projector,chunk_body_archive=self.archive)
        result=plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,
            principal_id=self.principal,source_id=self.source,target_receipt=f"recall://{self.source}/{rows[0]['native_id']}?rev=1#item=0",
            mode='explicit_forget',reason='owner_requested',requested_at='2026-10-15T00:00:00Z',idempotency_key='selective-forget'))
        self.assertEqual(result['status'],'deleted');before=self.state()
        self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows);self.assertEqual(self.state(),before)

    def test_committed_then_lost_ack_has_durable_partial_progress(self):
        proof,rows,_=self.capture()
        def lose():
            self.apply(proof,rows)
            raise ConnectionError('synthetic lost acknowledgement after real commit')
        self.assertRaises(ConnectionError,lose)
        with self.store.connect() as c:
            progress=owner._parent_progress(self.store,c,self.scope,self.deadline)
        self.assertEqual(progress['status'],'partial');self.assertEqual(progress['cumulative_cleared_documents'],2)
        self.assertEqual(sum(not r['text_redacted'] for r in self.state()),2)

    def test_unread_corruption_is_not_global_integrity_or_completion(self):
        with self.store.connect() as c:
            last=c.execute('SELECT artifact_id FROM canonical_evidence_document_parts WHERE tenant_id=%s ORDER BY part_ordinal DESC LIMIT 1',(self.tenant,)).fetchone()['artifact_id']
        read=self.archive.read_raw
        def damaged(reference):
            payload=read(reference)
            return b'x'*len(payload) if reference['artifact_id']==last else payload
        with patch.object(self.archive,'read_raw',damaged):
            with self.assertRaises(Exception):
                with prove_parent_chunks(self.store,self.archive,scope=self.scope,limits=self.limits,deadline_at=self.deadline):pass
            self.archive.reads.clear();proof,rows,cost=self.capture((0,1))
            self.assertNotIn(last,cost['selected_part_ids'])
            self.assertEqual(self.apply(proof,rows)['cleared_documents'],2)
        with self.store.connect() as c:self.assertEqual(owner._parent_progress(self.store,c,self.scope,self.deadline)['status'],'partial')

    def actual_writer_fixture(self, include_parent=False):
        from e2e_singleton_metadata_admission import SingletonMetadataAdmission
        helper=SingletonMetadataAdmission('runTest');helper.store=self.store;helper.setUp()
        self.addCleanup(helper.doCleanups)
        for native in (['session'] if include_parent else []) + ['turn/a','turn/b']:
            helper.ingest(native, 'Actual canonical writer body '+native)
        report=helper.projector.project_pending(tenant_id=helper.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(report['failed'],0)
        self.tenant,self.source,self.principal=helper.tenant,helper.source,helper.principal
        self.archive,self.projector=helper.archive,helper.projector
        self.scope=(self.tenant,self.source,'session')
        with self.store.connect() as c:
            self.docs=c.execute('SELECT document_id,native_id FROM canonical_documents WHERE tenant_id=%s ORDER BY native_id',(self.tenant,)).fetchall()
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='session',enabled=True)
        return helper

    def test_physical_and_admitted_mix_uses_original_membership_owner(self):
        with self.store.connect() as c:c.execute((SERVER/'schema/080_singleton_receipt_profile.sql').read_text())
        self.actual_writer_fixture()
        self.store.verified_singleton_receipts=True;self.store.search_plane='turbopuffer'
        proof,rows,_=self.capture((0,));result=self.apply(proof,rows,metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],1)
        proof,rows,_=self.capture((0,1))
        self.assertEqual([r['pg_body_bytes']==0 for r in sorted(rows,key=lambda r:r['native_id'])],[True,False])
        result=self.apply(proof,rows,metadata_mode='singleton')
        self.assertEqual(result['cleared_documents'],1);self.assertEqual(result['retired_metadata_chunks'],1)

    def test_actual_parent_forget_after_proof_refuses(self):
        helper=self.actual_writer_fixture(include_parent=True)
        proof,rows,_=self.capture((0,1))
        helper.plane.evidence_projector=helper.projector
        result=helper.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,
            principal_id=self.principal,source_id=self.source,target_receipt=f"recall://{self.source}/session?rev=1#item=0",
            mode='explicit_forget',reason='owner_requested',requested_at='2026-10-15T00:00:00Z',idempotency_key='selective-parent-forget'))
        self.assertEqual(result['status'],'deleted');before=self.state()
        self.assertRaises(owner.ChunkRetirementError,self.apply,proof,rows);self.assertEqual(self.state(),before)
        self.assertFalse(before)

if __name__=='__main__':unittest.main()
