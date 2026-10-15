#!/usr/bin/env python3
"""Inactive receipt lookup parity against actual canonical writers and archive proof."""
import hashlib
import json
import time
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

RECALL=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(RECALL),str(RECALL/'server')]
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway,CanonicalPlane
from recall_server.chunk_retirement import retire_current_chunks
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json

class ReceiptMembershipTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store=BrainStore(os.environ['RECALL_DATABASE_URL']);cls.store.migrate()
    @classmethod
    def tearDownClass(cls):
        cls.store.close()
    def setUp(self):
        nonce=uuid.uuid4().hex
        self.tenant,self.source,self.principal='tenant:membership:'+nonce,'source:membership:'+nonce,'principal:membership:'+nonce
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.archive=FilesystemArchiveStore(Path(temp.name),namespace_key=b'm'*32)
        self.projector=CanonicalLogicalEvidenceProjector(self.store,LogicalEvidenceProjectionStore(self.archive),bound_tenant_id=self.tenant,raw_archive=self.archive)
        self.plane=CanonicalPlane(self.store,self.archive,chunk_body_archive=self.archive)
        self.gateway=CanonicalArchiveGateway(self.store,self.archive,tenant_id=self.tenant,principal_id=self.principal)
    def ingest(self,native,text,*,tombstone=False):
        content={'target_native_id':native} if tombstone else {'text':text,'role':'user'};payload=canonical_json(content)
        ref=self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id=native,payload=payload,media_type='application/json',created_at='2026-10-14T00:00:00Z')
        envelope=dict(schema_version=1,source_id=self.source,native_id=native,native_parent_id='session',kind='tombstone' if tombstone else 'connector_record',occurred_at='2026-10-14T00:00:00Z',observed_at='2026-10-14T00:00:00Z',principal_id=self.principal,visibility='private',content_type='application/json',content=content,provenance=dict(connector_id='synthetic.membership',artifact_ref=ref),content_sha256=hashlib.sha256(payload).hexdigest())
        return self.plane.ingest_document(tenant_id=self.tenant,principal_id=self.principal,connector_id='synthetic.membership',artifact_ref=ref,envelope=envelope,text_redacted=text)
    def baseline(self,receipt):
        with self.store.connect() as connection:
            return connection.execute('''SELECT chunk.tenant_id,chunk.source_id,chunk.chunk_id,chunk.document_id,
                document.native_id,document.revision,chunk.ordinal,chunk.receipt,chunk.text_sha256,document.is_current
                FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE chunk.tenant_id=%s AND chunk.receipt=%s''',(self.tenant,receipt)).fetchone()
    def admit_fixture(self,document_id):
        report=self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(report['failed'],0)
        with patch.dict(os.environ,{'RECALL_CHUNK_BODY_READS':'archive'}):
            preview=retire_current_chunks(self.store,self.archive,tenant_id=self.tenant,source_id=self.source,document_ids=(document_id,))
            retired=retire_current_chunks(self.store,self.archive,tenant_id=self.tenant,source_id=self.source,document_ids=(document_id,),apply=True,reviewed_plan=preview['plan'])
            self.assertEqual(retired['status'],'applied')
        # Simulate the separately reviewed future admission owner only after the
        # existing independent verifier has proved exact immutable bodies.
        with self.store.connect() as connection:
            rows=connection.execute('SELECT ordinal,text_sha256,receipt FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=%s',(self.tenant,self.source,document_id)).fetchall()
            self.assertEqual(len(rows),1);self.assertEqual(rows[0]['ordinal'],0)
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND source_id=%s AND document_id=%s',(self.tenant,self.source,document_id))
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=%s',(self.tenant,self.source,document_id))
    def test_exact_metadata_survives_verified_singleton_row_removal_only_when_enabled(self):
        from recall_server.receipt_membership import lookup_receipt_metadata
        written=self.ingest('turn/one','Synthetic exact singleton α 🧠')
        receipt=written['receipt'];baseline=self.baseline(receipt)
        scope=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,))
        self.assertEqual(lookup_receipt_metadata(self.store,**scope),{receipt:baseline})
        self.admit_fixture(baseline['document_id'])
        self.assertEqual(lookup_receipt_metadata(self.store,**scope),{})
        self.assertEqual(lookup_receipt_metadata(self.store,**scope,allow_verified_singletons=True),{receipt:baseline})

    def test_profile_cannot_override_a_retained_deleted_chunk_fence(self):
        from recall_server.receipt_membership import lookup_receipt_metadata, ReceiptMembershipError
        written=self.ingest('turn/fenced','Synthetic fenced body')
        receipt=written['receipt'];baseline=self.baseline(receipt)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',(self.tenant,baseline['document_id']))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND receipt=%s',(self.tenant,receipt))
        with self.assertRaises(ReceiptMembershipError):
            lookup_receipt_metadata(self.store,tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True)

    def test_profile_cannot_hide_a_different_physical_ordinal(self):
        from recall_server.receipt_membership import lookup_receipt_metadata, ReceiptMembershipError
        written=self.ingest('turn/other-ordinal','Synthetic inconsistent profile')
        receipt=written['receipt'];baseline=self.baseline(receipt)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',(self.tenant,baseline['document_id']))
            connection.execute("UPDATE canonical_chunks SET ordinal=1,receipt=replace(receipt,'#item=0','#item=1') WHERE tenant_id=%s AND receipt=%s",(self.tenant,receipt))
        with self.assertRaises(ReceiptMembershipError):
            lookup_receipt_metadata(self.store,tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True)

    def test_scope_noncanonical_and_nonexistent_receipts_do_not_gain_membership(self):
        from recall_server.receipt_membership import lookup_receipt_metadata, ReceiptMembershipError
        written=self.ingest('turn/scope','Synthetic scoped singleton');receipt=written['receipt']
        baseline=self.baseline(receipt);self.admit_fixture(baseline['document_id'])
        args=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True)
        self.assertEqual(lookup_receipt_metadata(self.store,**dict(args,tenant_id=self.tenant+':other')), {})
        self.assertEqual(lookup_receipt_metadata(self.store,**dict(args,authorized_sources=())), {})
        self.assertEqual(lookup_receipt_metadata(self.store,**dict(args,receipts=(receipt.replace('#item=0','#item=1'),))), {})
        self.assertEqual(lookup_receipt_metadata(self.store,**dict(args,receipts=(receipt.replace('?rev=1','?rev=2'),))), {})
        for invalid in (receipt.replace('?rev=1','?rev=01'),receipt+'extra',receipt.replace('#item=0','#item=-1'),receipt.replace('?rev=1','?rev=2147483648')):
            with self.subTest(invalid=invalid),self.assertRaises(ReceiptMembershipError):
                lookup_receipt_metadata(self.store,**dict(args,receipts=(invalid,)))

    def test_document_deletion_and_supersession_fences_apply_to_admitted_metadata(self):
        from recall_server.receipt_membership import lookup_receipt_metadata, ReceiptMembershipError
        written=self.ingest('turn/deletion','Synthetic deletion fence');receipt=written['receipt']
        baseline=self.baseline(receipt);self.admit_fixture(baseline['document_id'])
        args=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND document_id=%s',(self.tenant,baseline['document_id']))
        self.assertEqual(lookup_receipt_metadata(self.store,**args),{})
        # An ordinary edit must restore outgoing rows before superseding; an
        # admitted historical row means that writer contract was not followed.
        with self.assertRaises(ReceiptMembershipError):lookup_receipt_metadata(self.store,**args,current_only=False)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',(self.tenant,baseline['document_id']))
        self.assertEqual(lookup_receipt_metadata(self.store,**args,current_only=False),{})

    def test_default_path_does_not_require_future_column_and_multichunk_rows_are_unchanged(self):
        from recall_server.receipt_membership import lookup_receipt_metadata
        self.ingest('turn/multiple','Synthetic multi-chunk text α '*2000)
        with self.store.connect() as connection:
            receipts=tuple(row['receipt'] for row in connection.execute('SELECT receipt FROM canonical_chunks WHERE tenant_id=%s ORDER BY ordinal',(self.tenant,)))
        self.assertGreater(len(receipts),1)
        expected={receipt:self.baseline(receipt) for receipt in receipts}
        args=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=receipts)
        self.assertEqual(lookup_receipt_metadata(self.store,**args),expected)
        self.assertEqual(lookup_receipt_metadata(self.store,**args,allow_verified_singletons=True),expected)
        with self.store.connect() as connection,connection.transaction(force_rollback=True):
            connection.execute('ALTER TABLE canonical_documents RENAME COLUMN chunk_receipt_profile TO fixture_hidden_profile')
            self.assertEqual(lookup_receipt_metadata(self.store,**args,connection=connection),expected)

    def test_unprofiled_missing_rows_never_infer_archive_membership(self):
        from recall_server.receipt_membership import lookup_receipt_metadata
        written=self.ingest('turn/missing','Synthetic unadmitted body');receipt=written['receipt']
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND receipt=%s',(self.tenant,receipt))
        self.assertEqual(lookup_receipt_metadata(self.store,tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True),{})

    def test_same_hash_replay_and_later_tombstone_remain_event_authoritative(self):
        from recall_server.receipt_membership import lookup_receipt_metadata
        native='turn/replay';text='Synthetic replay identity'
        written=self.ingest(native,text);receipt=written['receipt'];baseline=self.baseline(receipt)
        self.admit_fixture(baseline['document_id'])
        replay=self.ingest(native,text)
        self.assertTrue(replay['replay']);self.assertEqual(replay['receipt'],receipt)
        self.ingest(native,'',tombstone=True)
        args=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,),allow_verified_singletons=True,current_only=False)
        self.assertEqual(lookup_receipt_metadata(self.store,**args),{})
        # A stale document flag cannot override the independently stored later
        # tombstone. This models a lagging or corrupted metadata projection.
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=true,deleted_at=NULL WHERE tenant_id=%s AND document_id=%s',(self.tenant,baseline['document_id']))
        self.assertEqual(lookup_receipt_metadata(self.store,**args),{})

    def test_exact_batch_uses_indexed_keys_and_shared_deadline(self):
        from recall_server.receipt_membership import lookup_receipt_metadata
        from recall_server.db import SearchDeadlineExceeded
        receipt=self.ingest('turn/target','Synthetic indexed target')['receipt'];baseline=self.baseline(receipt)
        with self.store.connect() as connection:
            connection.execute("SET LOCAL statement_timeout='15s'")
            connection.execute("""INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                SELECT tenant_id,source_id,'evt_'||md5('padding'||n),'padding:'||n,native_parent_id,artifact_id,job_id,kind,content_sha256,1,occurred_at,observed_at,false,canonical_redacted
                FROM canonical_events CROSS JOIN generate_series(1,20000)n WHERE tenant_id=%s AND source_id=%s""",(self.tenant,self.source))
            connection.execute('ANALYZE canonical_events')
            connection.execute("""INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,content_sha256,revision,is_current,text_redacted,text_sha256)
                SELECT tenant_id,source_id,'doc_'||md5('padding'||n),'evt_'||md5('padding'||n),artifact_id,'padding:'||n,content_sha256,1,true,text_redacted,text_sha256
                FROM canonical_documents CROSS JOIN generate_series(1,20000)n WHERE tenant_id=%s AND source_id=%s""",(self.tenant,self.source))
            connection.execute('ANALYZE canonical_documents')
            connection.execute("""INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,'chk_'||md5('padding'||n),'doc_'||md5('padding'||n),0,'recall://'||source_id||'/padding:'||n||'?rev=1#item=0',text_redacted,text_sha256
                FROM canonical_chunks CROSS JOIN generate_series(1,20000)n WHERE tenant_id=%s AND source_id=%s""",(self.tenant,self.source))
            for relation in ('canonical_events','canonical_documents','canonical_chunks'):
                connection.execute('ANALYZE '+relation)
        args=dict(tenant_id=self.tenant,authorized_sources=(self.source,),receipts=(receipt,receipt),allow_verified_singletons=True)
        plans=[];execute=self.store._execute_bounded
        def capture(connection,statement,parameters,deadline):
            plans.append(connection.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+statement,parameters).fetchone()['QUERY PLAN'][0])
            return execute(connection,statement,parameters,deadline)
        with patch.object(self.store,'_execute_bounded',side_effect=capture):
            self.assertEqual(lookup_receipt_metadata(self.store,**args,deadline_at=time.monotonic()+3),{receipt:baseline})
        self.assertEqual(len(plans),1)
        nodes=[]
        def walk(node):
            nodes.append(node)
            for child in node.get('Plans',[]):walk(child)
        walk(plans[0]['Plan'])
        canonical=[node for node in nodes if node.get('Relation Name') in ('canonical_events','canonical_documents','canonical_chunks')]
        self.assertTrue(canonical)
        self.assertTrue(all(node['Node Type'] in ('Index Scan','Index Only Scan') for node in canonical),canonical)
        print(json.dumps(dict(diagnostic='receipt_membership_local_plan',fixture_documents=20001,requested_receipts=2,unique_receipts=1,execution_ms=plans[0]['Execution Time'],shared_hit_blocks=plans[0]['Plan'].get('Shared Hit Blocks'),shared_read_blocks=plans[0]['Plan'].get('Shared Read Blocks'),scan_types=[dict(node_type=node['Node Type'],relation=node.get('Relation Name'),index=node.get('Index Name'),actual_rows=node.get('Actual Rows'),loops=node.get('Actual Loops'),condition_has_native='native_id' in node.get('Index Cond',''),condition_has_revision='revision' in node.get('Index Cond','')) for node in canonical])),flush=True)
        self.assertTrue(any('native_id' in node.get('Index Cond','') and 'revision' in node.get('Index Cond','') for node in canonical),canonical)
        self.assertLessEqual(sum(node['Actual Rows']*node['Actual Loops'] for node in canonical),8)
        with self.assertRaises(SearchDeadlineExceeded):
            lookup_receipt_metadata(self.store,**args,deadline_at=time.monotonic()-1)

if __name__=='__main__':unittest.main(verbosity=2)
