#!/usr/bin/env python3
"""Fresh PostgreSQL proof of exact passage metadata authorization and pins."""
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.mcp import _encoded_result_size  # noqa: E402


def unavailable(bound, args):
    try:
        bound.passage_metadata(**args)
    except ValueError as error:
        assert str(error) == 'passage_metadata_unavailable'
    else:
        raise AssertionError('ineligible passage metadata was exposed')


def main():
    store = BrainStore(os.environ['RECALL_DATABASE_URL'])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant, other_tenant = f'tenant:metadata:{nonce}', f'tenant:metadata-other:{nonce}'
    source, other_source = f'codex:metadata:{nonce}', f'codex:metadata-other:{nonce}'
    principal = f'principal:metadata:{nonce}'
    policy = PassagePolicy(target_tokens=4, overlap_tokens=1)
    with tempfile.TemporaryDirectory(prefix='recall-metadata-e2e-') as temporary:
        archive = FilesystemArchiveStore(Path(temporary)/'archive', namespace_key=b'm'*32)
        projection = LogicalEvidenceProjectionStore(archive)
        for t,s,parent in [(tenant,source,'one'),(tenant,other_source,'two'),(other_tenant,source,'three')]:
            with store.connect() as connection:
                insert_source(connection,t,principal,s)
                insert_record(connection,tenant=t,source=s,parent=parent,native=parent+':event',
                              text=('A café confirmed the tenant boundary and retained source receipts. ' * 12),role='assistant',byte_start=0)
            logical=CanonicalLogicalEvidenceProjector(store,projection,bound_tenant_id=t,raw_archive=archive)
            logical.seed_backfill(tenant_id=t)
            logical.project_pending(tenant_id=t,batch_size=10,max_batches=1,upload_concurrency=1)
            passages=CanonicalPassageProjector(store,projection,policy=policy,bound_tenant_id=t)
            passages.project_pending(tenant_id=t,batch_size=10,max_batches=1,concurrency=1)
        bound=BoundCanonicalRetrieval(store,tenant_id=tenant,principal_id=principal,authorized_sources=(source,))
        with store.connect() as connection:
            document=connection.execute('SELECT logical_document_id,revision,manifest_content_sha256 FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s',(tenant,source)).fetchone()
            rows=connection.execute('SELECT passage_id,ordinal,policy_fingerprint,text_sha256,spans,receipts FROM canonical_passages WHERE tenant_id=%s AND source_id=%s ORDER BY ordinal',(tenant,source)).fetchall()
            foreign=connection.execute('SELECT passage_id FROM canonical_passages WHERE tenant_id=%s AND source_id=%s LIMIT 1',(tenant,other_source)).fetchone()['passage_id']
        args={**document,'source_id':source,'passage_ids':[r['passage_id'] for r in rows[:2]]}
        assert len(args['passage_ids'])==2
        first=bound.passage_metadata(**args)
        second=bound.passage_metadata(**args,cursor=first['next_cursor'])
        assert second['complete'] and second['next_cursor'] is None
        for page,row in zip([first,second],rows):
            assert page['passage']['spans']==row['spans'] and page['passage']['receipts']==row['receipts']
            assert page['passage']['text_sha256']==row['text_sha256']
            assert 'text' not in page['passage'] and 'opened_receipts' not in page
            assert _encoded_result_size(page)<=16384
        for changed in [{'source_id':other_source},{'logical_document_id':'ldoc_'+'0'*32},
                        {'passage_ids':[foreign]},{'revision':document['revision']+1},
                        {'manifest_content_sha256':'0'*64}]:
            unavailable(bound,{**args,**changed})
        unavailable(BoundCanonicalRetrieval(store,tenant_id=other_tenant,principal_id=principal,authorized_sources=(source,)),args)
        unavailable(BoundCanonicalRetrieval(store,tenant_id=tenant,principal_id=principal,authorized_sources=()),args)

        # The source opener uses the same live grants as public MCP, in the
        # bounded SQL snapshot (the synthetic source fixture grants nothing).
        with store.connect() as connection:
            connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Source opening',now())", ('org:'+nonce,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company',%s)", (tenant,'org:'+nonce,'source-'+nonce))
            connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')", (tenant,principal))
            connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read')", (tenant,principal,source))
        locator={**document,'source_id':source,'passage_id':rows[0]['passage_id']}

        def open_unavailable(target=locator, cursor=None):
            try:
                bound.show(target,cursor=cursor)
            except ValueError:
                return
            raise AssertionError('ineligible source records were exposed')

        opened=bound.show(locator)
        assert opened['complete'] and opened['opened_receipts']==rows[0]['receipts']
        assert all(c['content_complete'] for c in opened['chunks'])
        page_limit=_encoded_result_size(opened)-256
        cursor=None
        restored=''
        for _ in range(20):
            page=bound.show(locator,cursor=cursor,page_bytes=page_limit)
            assert _encoded_result_size(page)<=page_limit
            for chunk in page['chunks']:
                assert chunk['content_start']==len(restored)
                restored+=chunk['text']
                assert chunk['byte_end']==len(restored.encode())
            if page['complete']:
                break
            cursor=page['next_cursor']
        else:
            raise AssertionError('source pagination did not terminate')
        assert restored==opened['chunks'][0]['text'] and cursor is not None
        for field,value in [('source_id',other_source),('revision',document['revision']+1),
                            ('manifest_content_sha256','0'*64)]:
            open_unavailable({**locator,field:value})
        for table in ('brain_access_grants','canonical_source_grants'):
            with store.connect() as connection:
                connection.execute(f'DELETE FROM {table} WHERE tenant_id=%s AND principal_id=%s',(tenant,principal))
            open_unavailable()
            with store.connect() as connection:
                if table=='brain_access_grants':
                    connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')",(tenant,principal))
                else:
                    connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read')",(tenant,principal,source))
        # Additional event chunks are part of the selection even when no stored
        # passage receipt points to them. Deletion must not appear complete.
        with store.connect() as connection:
            connection.execute("""INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
                SELECT tenant_id,source_id,%s,document_id,1,replace(receipt,'#item=0','#item=1'),'extra',%s
                FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND ordinal=0""",
                ('chk_'+uuid.uuid4().hex,hashlib.sha256(b'extra').hexdigest(),tenant,source))
        extra=bound.show(locator)
        assert len(extra['chunks'])==2 and extra['chunks'][1]['text']=='extra'
        open_unavailable(cursor=cursor)  # membership changed since first page
        with store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND ordinal=1',(tenant,source))
        open_unavailable()
        with store.connect() as connection:
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND ordinal=1',(tenant,source))

        with store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND source_id=%s',('recall://missing',tenant,source))
        open_unavailable()
        with store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND source_id=%s',(rows[0]['receipts'][0],tenant,source))
            connection.execute('UPDATE canonical_passage_documents SET source_document_sha256=%s WHERE tenant_id=%s AND source_id=%s',('0'*64,tenant,source))
        open_unavailable()
        with store.connect() as connection:
            connection.execute('''UPDATE canonical_passage_documents projected SET source_document_sha256=evidence.document_content_sha256
                FROM canonical_evidence_documents evidence WHERE projected.tenant_id=evidence.tenant_id
                AND projected.source_id=evidence.source_id AND projected.logical_document_id=evidence.logical_document_id
                AND projected.tenant_id=%s AND projected.source_id=%s''',(tenant,source))
            connection.execute("INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) VALUES (%s,%s,'one','forget')",(tenant,source))
        open_unavailable()
        with store.connect() as connection:
            connection.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',(tenant,source))
        assert bound.show(locator)['complete']

        # Exercise real liveness joins, then restore synthetic state for the
        # next independent mutation. A redirect must not rescue exact old pins.
        for table,assignment,restore in [('canonical_chunks','deleted_at=now()','deleted_at=NULL'),
                                         ('canonical_documents','deleted_at=now(),is_current=false','deleted_at=NULL,is_current=true'),
                                         ('canonical_documents','is_current=false','is_current=true')]:
            with store.connect() as connection:
                connection.execute(f'UPDATE {table} SET {assignment} WHERE tenant_id=%s AND source_id=%s',(tenant,source))
            unavailable(bound,args)
            open_unavailable()
            with store.connect() as connection:
                connection.execute(f'UPDATE {table} SET {restore} WHERE tenant_id=%s AND source_id=%s',(tenant,source))
            assert bound.passage_metadata(**args)['passage']['passage_id']==rows[0]['passage_id']

        with store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s AND source_id=%s',('f'*64,tenant,source))
        unavailable(bound,{**args,'cursor':first['next_cursor']})
        with store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s AND source_id=%s',(document['manifest_content_sha256'],tenant,source))
            connection.execute('''INSERT INTO canonical_events(
                tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,
                       kind,%s,revision+1,occurred_at,observed_at,true,'{}'::jsonb
                FROM canonical_events WHERE tenant_id=%s AND source_id=%s LIMIT 1''',
                ('evt_'+uuid.uuid4().hex,'f'*64,tenant,source))
        unavailable(bound,args)
        open_unavailable()
    store.close()
    print(json.dumps({'status':'pass','metadata_only':True,'exact_pin_paging':True,
                      'tenant_source_passage_isolation':True,'deleted_and_tombstoned_receipts_denied':True}))


if __name__=='__main__':
    main()
