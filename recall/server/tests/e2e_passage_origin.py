#!/usr/bin/env python3
"""Canonical origin annotations preserve publication, history, and native vector cost."""
import hashlib
import json
import unittest
from unittest.mock import patch
from e2e_handback_projection import HandbackPublication
from recall_server import passage_projection
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.projectors import canonical_json


class PassageOrigin(unittest.TestCase):
    setUp=HandbackPublication.setUp
    project=HandbackPublication.project
    projected=HandbackPublication.projected
    grant_and_open=HandbackPublication.grant_and_open

    def ingest(self,native,*,parent='original',message_id='message-one',turn_id='turn-one',text='Sapphire deployment completed and its receipt remains available.',second=0):
        value=dict(type='response_item',payload=dict(type='message',role='user',id=message_id,
            content=[dict(type='input_text',text=text)],internal_chat_message_metadata_passthrough=dict(turn_id=turn_id)))
        payload=canonical_json(value);occurred=f'2026-10-01T12:00:{second:02}Z'
        artifact=self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id=native,payload=payload,media_type='application/json',created_at=occurred)
        envelope=dict(schema_version=1,source_id=self.source,native_id=native,native_parent_id=parent,kind='transcript_record',occurred_at=occurred,observed_at=occurred,principal_id=self.principal,visibility='private',content_type='application/json',content=value,provenance=dict(connector_id='synthetic.origin',connector_schema_version=1,artifact_ref=artifact),content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant,principal_id=self.principal,connector_id='synthetic.origin',artifact_ref=artifact,envelope=envelope,text_redacted=payload.decode())
        return payload.decode()

    def requeue(self):
        with self.store.connect()as c:c.execute('''INSERT INTO canonical_passage_projection_queue(tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
            SELECT tenant_id,source_id,logical_document_id,revision,1,'backfill',clock_timestamp()
            FROM canonical_evidence_documents WHERE tenant_id=%s''',(self.tenant,))

    def search(self):
        return self.retrieval.search('Sapphire deployment',lexical_query='Sapphire deployment',since=None,until=None,limit=10)

    def test_annotation_only_reprojection_keeps_ids_and_has_zero_vendor_embedding_writes(self):
        self.store.search_plane='turbopuffer'
        captured=self.ingest('original-event')
        with patch.object(passage_projection,'_message_origin',return_value=None):self.project()
        self.writer.drain(tenant_id=self.tenant,max_months=10)
        before=self.projected();self.assertEqual(len(before),1);self.assertNotIn('origin',before[0]['spans'][0])
        opened=self.grant_and_open(before[0]);self.assertIn(captured,[r['text']for r in opened['chunks']])
        ns=self.client.namespace(self.settings.namespace(self.tenant));initial_writes=len(ns.writes)
        with self.store.connect()as c:checkpoint=c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint']
        self.requeue();report=self.project();after=self.projected()
        self.assertEqual(report['retained'],1);self.assertEqual(report['inserted'],0);self.assertEqual(report['deleted'],0)
        self.assertEqual(after[0]['passage_id'],before[0]['passage_id']);self.assertEqual(after[0]['embed_sha256'],before[0]['embed_sha256']);self.assertIn('origin',after[0]['spans'][0])
        with self.store.connect()as c:current=c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint']
        self.assertEqual(current,checkpoint)
        self.writer.drain(tenant_id=self.tenant,max_months=10)
        self.assertEqual(len(ns.writes),initial_writes)
        self.assertEqual(self.grant_and_open(after[0])['chunks'],opened['chunks'])
        print(dict(annotation_only_retained=1,changed_passage_ids=0,native_tp_writes=0,embedding_utf8_bytes=0))

    def test_canonical_origin_folds_copies_and_forget_removes_only_its_authorized_pointer(self):
        self.store.search_plane='turbopuffer'
        self.ingest('original-event');self.ingest('copied-event',parent='fork',second=1)
        self.project();self.writer.drain(tenant_id=self.tenant,max_months=10)
        self.grant_and_open(self.projected()[0])
        before=self.search();self.assertEqual(before['diagnostics']['authority_status'],'ok')
        self.assertEqual(before['diagnostics']['copied_ranges_folded'],1)
        self.assertEqual(len(before['results']),1)
        item=before['results'][0]['matching_ranges'][0];self.assertEqual(len(item['copied_evidence']),1)
        all_receipts=set(item['receipts'])|{r for copy in item['copied_evidence']for r in copy['receipts']};self.assertEqual(len(all_receipts),2)
        primary={**before['results'][0],**item};alternate=item['copied_evidence'][0]
        reader=BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,authorized_sources=(self.source,))
        keys=('source_id','logical_document_id','revision','manifest_content_sha256','passage_id')
        for pointer in (primary,alternate):
            opened=reader.show({k:pointer[k]for k in keys})
            self.assertTrue(opened['complete'])
            returned={chunk['native_id']for chunk in opened['chunks']}
            expected={r.split('/',3)[3].split('?')[0]for r in pointer['receipts']}
            self.assertEqual(returned,expected)
        wrong={k:alternate[k]for k in keys};wrong['logical_document_id']=primary['logical_document_id']
        with self.assertRaises(ValueError):reader.show(wrong)

        forgotten=next(r for r in all_receipts if '/original-event?'in r)
        request=dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,target_receipt=forgotten,mode='explicit_forget',reason='owner_requested',requested_at='2026-10-01T13:00:00Z',idempotency_key='forget-origin')
        self.assertEqual(self.plane.forget(request)['status'],'deleted')
        # The asynchronous vendor copy is still present; canonical authority
        # must remove it before grouping, without borrowing the survivor's proof.
        after=self.search();self.assertEqual(len(after['results']),1)
        self.assertNotIn(forgotten,json.dumps(after['results']))
        self.assertEqual(after['diagnostics']['copied_ranges_folded'],0)
        self.assertIn('/copied-event?',json.dumps(after['results']))

    def test_vendor_invented_origin_cannot_fold_independent_messages(self):
        self.store.search_plane='turbopuffer'
        self.ingest('first',message_id='one');self.ingest('second',parent='other',message_id='two',second=1)
        self.project();self.writer.drain(tenant_id=self.tenant,max_months=10)
        ns=self.client.namespace(self.settings.namespace(self.tenant));rows=list(ns.rows.values())
        self.assertEqual(len(rows),2);rows[1]['spans']=rows[0]['spans']
        result=self.search();self.assertEqual(len(result['results']),2)
        self.assertEqual(result['diagnostics']['copied_ranges_folded'],0)

    def test_old_immutable_append_prefix_keeps_ids_and_annotations(self):
        self.store.search_plane='turbopuffer'
        for i in range(4):self.ingest(f'prefix-{i}',message_id=f'origin-{i}',text=(f'Sapphire finding {i} '+('unchanged ' * 400)),second=i)
        with patch.object(passage_projection,'_message_origin',return_value=None):self.project()
        before=self.projected();self.assertGreater(len(before),1)
        self.ingest('new-tail',message_id='new-origin',text='Unique Sapphire continuation.',second=5)
        self.logical.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        candidate=self.passages._pending(tenant_id=self.tenant,limit=1)[0]
        prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNotNone(prepared.previous_checkpoint)
        oracle=self.passages._prepare(candidate,incremental=False)
        self.assertEqual([(p.passage_id,p.ordinal,p.receipts)for p in prepared.passages],[(p.passage_id,p.ordinal,p.receipts)for p in oracle.passages])
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')
        after={p['passage_id']:p for p in self.projected()}
        untouched=before[:prepared.previous_checkpoint['passage_ordinal']]
        self.assertTrue(untouched)
        for old in untouched:
            self.assertEqual(after[old['passage_id']]['spans'],old['spans'])
            self.assertEqual(after[old['passage_id']]['embed_sha256'],old['embed_sha256'])
        self.assertTrue(any('origin'in span for p in after.values()for span in p['spans']))

    def test_stale_annotation_preparation_cannot_overwrite_new_source(self):
        self.store.search_plane='turbopuffer';self.ingest('original-event')
        with patch.object(passage_projection,'_message_origin',return_value=None):self.project()
        before=self.projected();self.requeue();candidate=self.passages._pending(tenant_id=self.tenant,limit=1)[0]
        prepared=self.passages._prepare(candidate)
        self.ingest('original-event',text='Changed Sapphire evidence.',second=1)
        self.logical.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        self.assertEqual(self.passages._commit(prepared)['status'],'stale')
        self.assertEqual([p['spans']for p in self.projected()],[p['spans']for p in before])

    def test_next_append_after_enrichment_reuses_prefix_with_full_builder_parity(self):
        self.store.search_plane='turbopuffer'
        for i in range(4):self.ingest(f'prefix-{i}',message_id=f'origin-{i}',text=(f'Sapphire finding {i} '+('unchanged ' * 400)),second=i)
        with patch.object(passage_projection,'_message_origin',return_value=None):self.project()
        before=self.projected();self.requeue();self.project();enriched=self.projected()
        self.assertEqual([p['passage_id']for p in before],[p['passage_id']for p in enriched])
        for old,new in zip(before,enriched):
            self.assertEqual(old['spans'],[{k:v for k,v in span.items()if k!='origin'}for span in new['spans']])
        with self.store.connect()as c:checkpoint=c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint']
        self.ingest('new-tail',message_id='new-origin',text='Unique Sapphire continuation.',second=5)
        self.logical.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        candidate=self.passages._pending(tenant_id=self.tenant,limit=1)[0];prepared=self.passages._prepare(candidate,incremental=True)
        self.assertEqual(prepared.previous_checkpoint,checkpoint);self.assertGreater(checkpoint['passage_ordinal'],0)
        oracle=self.passages._prepare(candidate,incremental=False)
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')
        after=self.projected();self.assertEqual(len(after),len(oracle.passages))
        for actual,expected in zip(after,oracle.passages):
            self.assertEqual(actual['passage_id'],expected.passage_id)
            self.assertEqual(actual['text_redacted'],expected.text)
            self.assertEqual(actual['text_sha256'],expected.text_sha256)
            self.assertEqual(actual['spans'],json.loads(passage_projection.canonical_spans_json(expected.spans)))
        print(dict(enriched_next_append_reused_prefix=checkpoint['passage_ordinal'],full_builder_id_geometry_text_parity=True))


if __name__=='__main__':unittest.main(verbosity=2)
