#!/usr/bin/env python3
"""Fresh PostgreSQL proof of exact passage metadata authorization and pins."""
from __future__ import annotations

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
                              text='A café confirmed the tenant boundary and retained source receipts.',role='assistant',byte_start=0)
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

        # Exercise real liveness joins, then restore synthetic state for the
        # next independent mutation. A redirect must not rescue exact old pins.
        for table,assignment,restore in [('canonical_chunks','deleted_at=now()','deleted_at=NULL'),
                                         ('canonical_documents','deleted_at=now(),is_current=false','deleted_at=NULL,is_current=true'),
                                         ('canonical_documents','is_current=false','is_current=true')]:
            with store.connect() as connection:
                connection.execute(f'UPDATE {table} SET {assignment} WHERE tenant_id=%s AND source_id=%s',(tenant,source))
            unavailable(bound,args)
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
    store.close()
    print(json.dumps({'status':'pass','metadata_only':True,'exact_pin_paging':True,
                      'tenant_source_passage_isolation':True,'deleted_and_tombstoned_receipts_denied':True}))


if __name__=='__main__':
    main()
