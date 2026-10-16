#!/usr/bin/env python3
"""Typed passage repair retains captured history and existing publication fences."""
import hashlib
import time
import unittest
from unittest.mock import patch

from e2e_handback_projection import HandbackPublication
from recall_server import passage_projection
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.parent_membership_enrollment import enroll_parent_passages
from recall_server.projectors import canonical_json


class TypedPassageVisibility(HandbackPublication):
    def ingest_blocks(self, native, blocks, *, parent='session-one'):
        value={'type':'user','message':{'role':'user','content':blocks}}
        payload=canonical_json(value);occurred='2026-10-13T12:00:00Z'
        artifact=self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,
            native_id=native,payload=payload,media_type='application/json',created_at=occurred)
        envelope=dict(schema_version=1,source_id=self.source,native_id=native,
            native_parent_id=parent,kind='transcript_record',occurred_at=occurred,
            observed_at=occurred,principal_id=self.principal,visibility='private',
            content_type='application/json',content=value,
            provenance=dict(connector_id='synthetic.typed',connector_schema_version=1,artifact_ref=artifact),
            content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant,principal_id=self.principal,
            connector_id='synthetic.typed',artifact_ref=artifact,envelope=envelope,
            text_redacted=payload.decode())
        return payload.decode()

    def check_reprojection(self, *, mixed):
        blocks=[{'type':'tool_result','content':'deployment skill boilerplate'}]
        if mixed:blocks.insert(0,{'type':'text','text':'deployment finding remains visible'})
        raw=self.ingest_blocks('typed-event',blocks)
        self.ingest_blocks('unaffected-event',[{'type':'text','text':'Unchanged genuine user history.'}],parent='unaffected')
        native=passage_projection._visible_message
        # Publish the exact previous extraction contract through native owners.
        with patch.object(passage_projection,'_visible_message',side_effect=lambda text:native(text,include_tool_results=True)):
            self.project()
        self.writer.drain(tenant_id=self.tenant,max_months=20)
        rows=self.projected();old=next(row for row in rows if 'deployment' in row['text_redacted'])
        untouched=next(row for row in rows if 'Unchanged genuine' in row['text_redacted'])
        opened=self.grant_and_open(old)
        self.assertIn(raw,[chunk['text']for chunk in opened['chunks']])
        self.store.verified_singleton_receipts=True
        scope=dict(tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            native_parent_id='session-one',passage_ids=(old['passage_id'],),binding_epoch='typed-test',
            limits=dict(passages=32,receipt_references=1000,metadata_bytes=1024*1024))
        self.assertEqual(enroll_parent_passages(self.store,**scope,deadline_at=time.monotonic()+5)['status'],'enrolled')
        with self.store.connect()as c:
            before_generation=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s',(self.tenant,self.source,'session-one')).fetchone()['generation']
            certified=c.execute('SELECT membership_generation FROM canonical_passages WHERE tenant_id=%s AND passage_id=%s',(self.tenant,old['passage_id'])).fetchone()
        self.assertEqual(certified['membership_generation'],before_generation)
        # A code change does not silently queue the corpus under the same policy.
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant),0)
        self.assertEqual(self.project()['documents'],0)
        self.assertEqual(self.grant_and_open(old)['chunks'],opened['chunks'])
        with self.store.connect()as c:
            c.execute('''INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                SELECT tenant_id,source_id,logical_document_id,revision,1,'backfill',clock_timestamp()
                FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                  AND revision=%s AND manifest_content_sha256=%s''',
                (self.tenant,self.source,old['logical_document_id'],old['revision'],old['manifest_content_sha256']))
        namespace=self.client.namespace(self.settings.namespace(self.tenant))
        commit=self.passages._commit
        def before_commit(prepared):
            self.assertIn(old['passage_id'],namespace.rows)
            with self.store.connect()as c:
                live=self.retrieval._authorize_singletons(c,[(self.source,old['passage_id'])],time.monotonic()+5)
            self.assertEqual(len(live),1)
            return commit(prepared)
        with patch.object(self.passages,'_commit',side_effect=before_commit):
            report=self.project()
        self.assertEqual(report['documents'],1)
        self.assertGreater(report['deleted'],0)
        current=self.projected();ids={row['passage_id']for row in current}
        self.assertNotIn(old['passage_id'],ids)
        self.assertIn(untouched['passage_id'],ids)
        fresh=[row for row in current if row['logical_document_id']==old['logical_document_id']]
        self.assertEqual(len(fresh),int(mixed))
        if mixed:
            self.assertEqual(fresh[0]['text_redacted'],'deployment finding remains visible')
            self.assertIsNone(fresh[0]['membership_generation'])
        # Old vendor rows can outlive the transaction. Current catalog authority
        # refuses them before any external deletion ACK.
        self.assertIn(old['passage_id'],namespace.rows)
        with self.store.connect()as c:
            self.assertEqual(self.retrieval._authorize_singletons(c,[(self.source,old['passage_id'])],time.monotonic()+5),[])
            generation=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s',(self.tenant,self.source,'session-one')).fetchone()['generation']
        # Canonical receipt membership did not mutate. The old certificate row
        # is gone; replacement passages need their own exact publication proof.
        self.assertEqual(generation,before_generation)
        self.writer.drain(tenant_id=self.tenant,max_months=20)
        self.assertNotIn(old['passage_id'],namespace.rows)
        self.assertEqual(set(namespace.rows),ids)
        target=old['receipts'][0]
        history=BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,authorized_sources=(self.source,)).show(target)
        self.assertEqual(history['opened_receipts'],[target])
        self.assertIn(raw,[chunk['text']for chunk in history['chunks']])
        self.assertEqual(self.project()['documents'],0)

    def test_tool_only_parent_reprojects_to_zero_and_raw_history_remains(self):
        self.check_reprojection(mixed=False)

    def test_mixed_parent_reprojects_only_visible_text_with_old_vendor_fenced(self):
        self.check_reprojection(mixed=True)


if __name__=='__main__':
    suite=unittest.TestSuite(TypedPassageVisibility(name)for name in TypedPassageVisibility.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
