#!/usr/bin/env python3
"""Disposable PostgreSQL + real logical archive + existing selected-source reader.

Tool span metadata is a test-only canonical fixture: production emission remains
disabled. Public metadata/source-open owners are real; exec runs locally against
their verified archived fixture rather than invoking a remote sandbox service.
"""
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace

RECALL = Path(__file__).resolve().parents[2]
for directory in (RECALL, RECALL / 'server', RECALL / 'tests'):
    sys.path.insert(0, str(directory))

from e2e_logical_evidence_projection import insert_record, insert_source
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy
from recall_server import mcp
from evals.candidate_capture import CandidateCapture
from evals.systems_card.mcp_client import CallOutcome
from test_candidate_capture import LocalClient, digest


def main():
    store=BrainStore(os.environ['RECALL_DATABASE_URL']);store.migrate()
    nonce=uuid.uuid4().hex;tenant='tenant:tool-unit:'+nonce;principal='principal:tool-unit:'+nonce
    source='codex:tool-unit:'+nonce;parent='codex-session-'+nonce[:24]
    text='Café result, with scope retained.'
    with tempfile.TemporaryDirectory(prefix='recall-tool-contract-') as temp:
        root=Path(temp);archive=FilesystemArchiveStore(root/'archive',namespace_key=b't'*32)
        projection=LogicalEvidenceProjectionStore(archive)
        for form in [('function_call','arguments','input'),('custom_tool_call','input','input'),
                     ('function_call_output','output','result'),('custom_tool_call_output','output','result')]:
            item_type, field, kind = form
            this_parent='codex-session-'+digest((parent+item_type).encode())[:24]
            content={'type':'response_item','payload':{'type':item_type,field:text,'call_id':'call-proof','name':'UnknownTool'}}
            legacy={'type':'response_item','payload':{'type':'message','role':'assistant','content':[
                {'type':'output_text','text':'Legacy visible text stays unchanged.'}]}}
            with store.connect() as connection:
                insert_source(connection,tenant,principal,source)
                insert_record(connection,tenant=tenant,source=source,parent=this_parent,native=item_type+'-legacy',
                    text=json.dumps(legacy),role='response_item',byte_start=0,canonical_content=legacy)
                receipt=insert_record(connection,tenant=tenant,source=source,parent=this_parent,native=item_type+'-tool',
                    text=json.dumps(content),role='response_item',byte_start=1000,canonical_content=content)
            logical=CanonicalLogicalEvidenceProjector(store,projection,bound_tenant_id=tenant,raw_archive=archive)
            logical.seed_backfill(tenant_id=tenant)
            logical.project_pending(tenant_id=tenant,batch_size=10,max_batches=1,upload_concurrency=1)
            CanonicalPassageProjector(store,projection,policy=PassagePolicy(1024,128),bound_tenant_id=tenant).project_pending(
                tenant_id=tenant,batch_size=10,max_batches=1,concurrency=1)
            with store.connect() as connection:
                document=connection.execute('SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s',(tenant,source,this_parent)).fetchone()
                row=connection.execute('SELECT * FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s',(tenant,source,document['logical_document_id'])).fetchone()
                manifest_ref,part_refs=logical._old_references(connection,SimpleNamespace(tenant_id=tenant,source_id=source,native_parent_id=this_parent))
            mount=root/item_type;mount.mkdir();(mount/'manifest.json').write_bytes(archive.read_raw(manifest_ref))
            for index,reference in enumerate(part_refs):
                (mount/f'part-{index:05d}.jsonl').write_bytes(projection.read_part(reference,tenant_id=tenant,source_id=source))
            before={p.name:digest(p.read_bytes())for p in mount.iterdir()}
            records=[json.loads(line)for file in sorted(mount.glob('part-*.jsonl'))for line in file.read_bytes().splitlines()]
            selected=next(record for record in records if receipt in record['receipts'])
            assert selected['roles']==[],selected
            ref='codex.tool-'+kind+'.string.v1#/payload/'+field
            span=dict(record_ordinal=selected['ordinal'],record_count=1,message_index=0,source_byte_start=0,source_byte_end=len(text.encode()),
                      passage_byte_start=0,passage_byte_end=len(text.encode()),source_unit=ref)
            # A fixture only, not a newly enabled production projector.
            with store.connect() as connection:
                connection.execute('UPDATE canonical_passages SET spans=%s,text_redacted=%s,text_sha256=%s,receipts=%s WHERE tenant_id=%s AND source_id=%s AND passage_id=%s',
                    (json.dumps([span]),text,digest(text.encode()),[receipt],tenant,source,row['passage_id']))
                row['receipts']=[receipt]
                org='org:'+item_type+nonce
                connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Tool source contract',now())",(org,))
                connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company',%s) ON CONFLICT DO NOTHING",(tenant,org,item_type.replace('_','-')+nonce))
                connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read') ON CONFLICT DO NOTHING",(tenant,principal))
                connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read') ON CONFLICT DO NOTHING",(tenant,principal,source))
            bound=BoundCanonicalRetrieval(store,tenant_id=tenant,principal_id=principal,authorized_sources=(source,))
            pins=dict(source_id=source,logical_document_id=document['logical_document_id'],revision=document['revision'],manifest_content_sha256=document['manifest_content_sha256'])
            locator={**pins,'passage_id':row['passage_id']}
            hit={**pins,'native_parent_id':this_parent,'first_occurred_at':document['first_occurred_at'].isoformat(),
                 'last_occurred_at':document['last_occurred_at'].isoformat(),'matching_ranges':[
                     dict(passage_id=row['passage_id'],text=text[:3],receipts=row['receipts'],spans=[],spans_omitted=True)]}
            class NativeClient(LocalClient):
                def call_tool(self,name,args,**kwargs):
                    if name=='recall_passage_metadata':
                        return CallOutcome(name,True,0,result=mcp._call_tool(bound,{},name,args))
                    opened=bound.show(locator)
                    assert set(row['receipts']) <= set(opened['opened_receipts'])
                    return super().call_tool(name,args,**kwargs)
            client=NativeClient(mount)
            cap=CandidateCapture(root/('capture-'+item_type),client_factory=lambda:client,protected_families=set())
            captured=cap.capture_case(kind,'Exact fixture source',{'results':[hit]})['candidates'][0]
            assert captured['complete_selected_passages'],captured
            assert captured['text']==text and captured['source_evidence'][0]['spans'][0]['source_origin']=='tool_'+kind
            assert captured['source_evidence'][0]['spans'][0]['source_tool']==dict(type=item_type,role='assistant'if kind=='input'else'tool',call_id='call-proof',name='UnknownTool')
            assert before=={p.name:digest(p.read_bytes())for p in mount.iterdir()}
            # Invalid source-unit metadata cannot turn another field or byte
            # interval into evidence, even with current native authorization.
            for label, invalid in [('wrong-kind',dict(span,source_unit=ref.replace('tool-'+kind,'tool-'+('result'if kind=='input'else'input')))),
                                   ('non-tool-field',dict(span,source_unit=ref.rsplit('/',1)[0]+'/name')),
                                   ('outside',dict(span,source_byte_end=len(text.encode())+1)),
                                   ('utf8-split',dict(span,source_byte_end=4,passage_byte_end=4))]:
                with store.connect() as connection:
                    connection.execute('UPDATE canonical_passages SET spans=%s WHERE tenant_id=%s AND source_id=%s AND passage_id=%s',
                        (json.dumps([invalid]),tenant,source,row['passage_id']))
                invalid_cap=CandidateCapture(root/('invalid-'+item_type+'-'+label),client_factory=lambda:client,protected_families=set())
                rejected=invalid_cap.capture_case(label,'Invalid source coordinates',{'results':[hit]})['candidates'][0]
                assert not rejected['complete_selected_passages'] and 'text'not in rejected,rejected
            with store.connect() as connection:
                connection.execute('UPDATE canonical_passages SET spans=%s WHERE tenant_id=%s AND source_id=%s AND passage_id=%s',
                    (json.dumps([span]),tenant,source,row['passage_id']))
            def denied(target=locator):
                try:bound.show(target)
                except ValueError:return
                raise AssertionError('source opening unexpectedly authorized')
            denied({**locator,'revision':locator['revision']+1})
            for table in ['brain_access_grants','canonical_source_grants']:
                with store.connect() as connection:
                    connection.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND principal_id=%s',(tenant,principal))
                denied()
                with store.connect() as connection:
                    if table=='brain_access_grants':
                        connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')",(tenant,principal))
                    else:
                        connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read')",(tenant,principal,source))
            with store.connect() as connection:
                connection.execute("INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) VALUES (%s,%s,%s,'forget')",(tenant,source,this_parent))
            denied()
            with store.connect() as connection:
                connection.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s',(tenant,source,this_parent))
            assert bound.show(locator)['complete']
    print('PASS: four Codex provider forms / two origins through native PostgreSQL metadata/source-opening and exact archived-source capture; current grant/revision/Forget refusal')


if __name__=='__main__':main()
