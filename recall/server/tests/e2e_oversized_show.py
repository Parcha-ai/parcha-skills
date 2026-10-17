import gzip,hashlib,json,sys,time,unittest,uuid
from pathlib import Path
from unittest.mock import patch
R=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(R/'recall'),str(R/'recall/server'),str(R/'recall/server/tests'),str(Path(__file__).parent)]
import e2e_slack_deletion_ordering as fixture
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server import mcp
SHA=lambda value:hashlib.sha256(value).hexdigest()
from recall_server.canonical import CanonicalArchiveGateway
from recall_server.logical_evidence_projection import LogicalEvidenceError
from recall_server.projectors import canonical_json

class Opening(unittest.TestCase):
    setUpClass=classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass=classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    ingest=fixture.SlackDeletionOrdering.ingest
    def setUp(self):
        fixture.SlackDeletionOrdering.setUp(self)
        prepared=self.ingest(self.root,path='prepare',connector_id='claude.jsonl')
        full={'type':'user','message':{'role':'user','content':'Exact complete retained evidence ñ. '*2000},'_recall_collector_generation':0}
        from collector.collector import sanitize
        full=sanitize(dict(full,api_key='synthetic-private-value'))
        self.full=canonical_json(full);packed=gzip.compress(self.full,mtime=0)
        gateway=CanonicalArchiveGateway(self.store,self.archive,tenant_id=self.tenant,principal_id=self.principal)
        artifact=gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id=prepared['envelope']['native_id']+':full',payload=packed,media_type='application/vnd.recall.oversized-record+gzip',created_at=prepared['envelope']['occurred_at'])
        content={'contract':'recall.oversized-projection.v1','schema_version':1,'_recall_collector_generation':0,'full_record_available':True,'full_content_sha256':SHA(self.full),'full_size_bytes':len(self.full),'archive_encoding':'gzip','archive_size_bytes':len(packed),'head':'Exact complete','tail':'evidence.','content_fidelity':'head_tail'}
        prepared['artifact_ref']=artifact;prepared['envelope']['provenance']['artifact_ref']=artifact
        prepared['envelope'].update(kind='transcript_record',content=content,content_sha256=SHA(canonical_json(content)))
        prepared['envelope']['provenance']['connector_schema_version']=1
        prepared['text_redacted']=canonical_json(content).decode()
        result=self.plane.ingest_document(**prepared)
        with self.store.connect() as c:
            organization='organization:'+uuid.uuid4().hex
            c.execute("INSERT INTO brain_organizations VALUES(%s,'company','Opening fixture',now())",(organization,))
            c.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','opening')",(self.tenant,organization))
            c.execute("INSERT INTO brain_access_grants VALUES(%s,%s,'owner',now())",(self.tenant,self.principal))
        receipt=result['receipt'] if '#item=' in result['receipt'] else result['receipt']+'#item=0'
        self.target=dict(source_id=self.source,native_id=prepared['envelope']['native_id'],receipt=receipt,active_digest=prepared['envelope']['content_sha256'],expected_base_sha256=SHA(canonical_json({k:v for k,v in full.items() if k!='_recall_collector_generation'})),expected_full_sha256=SHA(self.full),expected_full_bytes=len(self.full))
        self.spec=dict(tenant_id=self.tenant,principal_id=self.principal)
        self.reader=BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,authorized_sources=(self.source,),archive=self.archive)
    def call(self,**options):
        return mcp._call_tool(self.reader,{},'recall_show',dict(target=self.target['receipt'],**options))
    def test_public_receipt_restores_full_source_in_pages(self):
        pages=[];cursor=None
        while True:
            page=self.call(page_bytes=8192,**({'cursor':cursor}if cursor else{}))
            self.assertEqual(page['scope'],'full_source_record')
            self.assertEqual(page['opened_receipts'],[self.target['receipt']])
            self.assertLessEqual(mcp._encoded_result_size(page),8192)
            self.assertEqual(page['source_text_sha256'],SHA(self.full))
            pages.append(page['chunks'][0]['text']);cursor=page['next_cursor']
            if cursor is None:
                self.assertTrue(page['complete']);break
            self.assertFalse(page['complete'])
        self.assertEqual(''.join(pages).encode(),self.full)
        self.assertGreater(len(pages),1)
        self.assertNotIn('synthetic-private-value',''.join(pages));self.assertIn('[REDACTED]',''.join(pages))


    def forget(self):
        self.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,target_receipt=self.target['receipt'],mode='explicit_forget',reason='owner_requested',requested_at='2026-10-17T11:00:00Z',idempotency_key=uuid.uuid4().hex))
    def mutate_content(self,updates):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{content}',(canonical_redacted->'content')||%s::jsonb) WHERE tenant_id=%s AND source_id=%s AND native_id=%s",(json.dumps(updates),self.tenant,self.source,self.target['native_id']))
    def test_grant_revoked_before_read(self):
        with self.store.connect() as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',(self.tenant,self.source))
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(mcp.McpProtocolError):self.call()
    def test_grant_revoked_during_read(self):
        original=self.archive.read_raw
        def revoke(ref):
            data=original(ref)
            with self.store.connect() as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',(self.tenant,self.source))
            return data
        with patch.object(self.archive,'read_raw',side_effect=revoke),self.assertRaises(mcp.McpProtocolError):self.call()
    def test_forget_between_pages(self):
        cursor=self.call()['next_cursor'];self.assertIsNotNone(cursor);self.forget()
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(mcp.McpProtocolError) as caught:self.call(cursor=cursor)
        self.assertEqual(caught.exception.code,-32602)
    def test_forget_during_read(self):
        original=self.archive.read_raw
        def forget(ref):
            data=original(ref);self.forget();return data
        with patch.object(self.archive,'read_raw',side_effect=forget),self.assertRaises(mcp.McpProtocolError):self.call()
    def test_changed_commitment_invalidates_cursor_before_read(self):
        cursor=self.call()['next_cursor'];self.mutate_content({'full_content_sha256':'0'*64})
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(mcp.McpProtocolError) as caught:self.call(cursor=cursor)
        self.assertEqual(caught.exception.code,-32602)
        self.assertEqual(caught.exception.message,'source record unavailable for current receipt')
    def test_full_hash_mismatch_refuses(self):
        self.mutate_content({'full_content_sha256':'0'*64})
        with self.assertRaises(LogicalEvidenceError):self.call()
    def test_malformed_gzip_refuses(self):
        original=self.archive.read_raw
        with patch.object(self.archive,'read_raw',side_effect=lambda ref:b'X'*len(original(ref))),self.assertRaises(LogicalEvidenceError):self.call()
    def test_native_decompression_bound_not_diagnostic_limit(self):
        from recall_server import logical_evidence_projection as logical
        self.mutate_content({'full_size_bytes':1024})
        with patch.object(logical,'MAX_RESTORED_RECORD_BYTES',1024),self.assertRaises(LogicalEvidenceError):self.call()
    def test_outer_deadline_not_renewed(self):
        from recall_server.db import SearchDeadlineExceeded
        from recall_server import canonical_retrieval as retrieval
        original=self.reader._receipt_event
        def delayed(*args,**kwargs):
            row=original(*args,**kwargs);time.sleep(.03);return row
        with patch.object(self.store,'search_deadline_ms',10),patch.object(self.reader,'_receipt_event',side_effect=delayed),patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(SearchDeadlineExceeded):self.call()
    def test_page_fit_never_serializes_record_sized_prefix(self):
        original=mcp._encoded_result_size;sizes=[]
        def measured(value):
            sizes.append(len(value['chunks'][0]['text']));return original(value)
        with patch.object(mcp,'_encoded_result_size',side_effect=measured):self.call(page_bytes=4096)
        self.assertTrue(sizes);self.assertLessEqual(max(sizes),4096)
    def test_ordinary_receipt_unchanged_and_no_raw_read(self):
        result=self.ingest(dict(self.root,ts='1784332800.999999',text='ordinary'),path='direct')
        receipt=result['receipt'];receipt=receipt if '#item='in receipt else receipt+'#item=0'
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')):
            r=mcp._call_tool(self.reader,{},'recall_show',dict(target=receipt))
        self.assertNotIn('scope',r);self.assertTrue(r['chunks']);self.assertIn('ordinary',r['chunks'][0]['text'])
    def test_same_bytes_as_native_logical_source_publication(self):
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
        projection=LogicalEvidenceProjectionStore(self.archive)
        projector=CanonicalLogicalEvidenceProjector(self.store,projection,bound_tenant_id=self.tenant,raw_archive=self.archive)
        result=projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=2,upload_concurrency=1)
        self.assertEqual(result['documents'],1)
        targets=projector.targets_for_receipts(tenant_id=self.tenant,source_ids=(self.source,),receipts=(self.target['receipt'],),limit=10)
        rows=[json.loads(line)for target in targets for line in projection.read_part(target['reference'],tenant_id=self.tenant,source_id=self.source).splitlines()]
        records=[r for r in rows if r['event_native_id']==self.target['native_id']]
        payload=b''.join(canonical_json(r['content']) if 'content' in r else r.get('text',r.get('content_fragment','')).encode() for r in records)
        self.assertEqual(payload,self.full)
        r=self.call(page_bytes=1024*1024)
        self.assertEqual(r['chunks'][0]['text'].encode(),payload)
        self.assertNotEqual(r['source_text_sha256'],r['projection_text_sha256'])


    def test_each_page_uses_one_raw_artifact_and_no_executor(self):
        original=self.archive.read_raw;reads=[]
        def counted(ref):reads.append(ref['size_bytes']);return original(ref)
        self.reader.deep_inspector=object()
        with patch.object(self.archive,'read_raw',side_effect=counted):
            first=self.call(page_bytes=8192);self.call(page_bytes=8192,cursor=first['next_cursor'])
        self.assertEqual(len(reads),2);self.assertEqual(reads[0],reads[1])
        self.assertLess(reads[0],len(self.full))
    def test_page_bytes_and_cursor_refusal_before_archive(self):
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')):
            for options in ({'page_bytes':True},{'page_bytes':1024*1024+1},{'cursor':'broken'},{'cursor':'0'*64+':1'}):
                with self.assertRaises((ValueError,mcp.McpProtocolError)):self.call(**options)
    def test_parent_forget_marker_refuses(self):
        from recall_server.canonical import _identity_sha256
        with self.store.connect() as c:
            row=c.execute('SELECT native_parent_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',(self.tenant,self.source,self.target['native_id'])).fetchone()
            self.assertIsNotNone(row['native_parent_id'])
            c.execute("""INSERT INTO forget_tombstones(tenant_id,source_id,target_identity_sha256,mode,reason,deleted_at,status,idempotency_key) VALUES(%s,%s,%s,'explicit_forget','owner_requested',now(),'deleting','oversized-parent')""",(self.tenant,self.source,_identity_sha256(self.tenant,self.source,row['native_parent_id'])))
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(mcp.McpProtocolError) as caught:self.call()
        self.assertEqual(caught.exception.code,-32602)
    def test_archive_deadline_failure_no_retry(self):
        from recall_server.archive import ArchiveDeadlineExceeded
        class Bounded:
            calls=0
            def read_raw_bounded(self,ref,*,deadline_at):
                self.calls+=1;raise ArchiveDeadlineExceeded('fixture')
        archive=Bounded();self.reader.archive=archive
        with self.assertRaises(LogicalEvidenceError):self.call()
        self.assertEqual(archive.calls,1)
    def test_distinct_evidence_archive_is_not_used_for_full_source(self):
        class Evidence:
            def read_raw(self,ref):raise AssertionError('not the raw archive')
        self.reader.chunk_body_archive=Evidence()
        result=self.call(page_bytes=1024*1024)
        self.assertEqual(result['chunks'][0]['text'].encode(),self.full)

    def test_partial_event_forget_refuses_full_record(self):
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256,deleted_at)
                SELECT tenant_id,source_id,chunk_id||'-removed',document_id,ordinal+1,replace(receipt,'#item=0','#item=1'),text_redacted,text_sha256,now()
                FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND receipt=%s""",(self.tenant,self.source,self.target['receipt']))
        with patch.object(self.archive,'read_raw',side_effect=AssertionError('must not read')),self.assertRaises(mcp.McpProtocolError):self.call()

if __name__=="__main__":unittest.main()
