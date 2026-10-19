#!/usr/bin/env python3
"""Append projection reuses immutable prefix work without changing publication."""
import unittest
import time
from unittest.mock import patch
from e2e_handback_projection import HandbackPublication

class PassageAppendTail(HandbackPublication):
    def setUp(self):
        super().setUp()
        put=self.logical.projection.put_records
        def bounded_parts(**kwargs):return put(**dict(kwargs,part_bytes=4096))
        self.part_patch=patch.object(self.logical.projection,'put_records',side_effect=bounded_parts)
        self.part_patch.start();self.addCleanup(self.part_patch.stop)
    def seed(self):
        for i in range(20):self.ingest(f'event-{i:03}',text=('retained evidence '+str(i)+' ')*100,occurred=f'2026-10-13T12:00:{i:02}Z')
        self.project()
    def test_append_skips_archive_prefix_and_matches_full_owner(self):
        self.seed();before=self.projected()
        self.ingest('event-020',text='New precise finding remains searchable.',occurred='2026-10-13T12:00:20Z')
        self.logical.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        candidate=self.passages._pending(tenant_id=self.tenant,limit=1)[0]
        full_bytes=sum(r['size_bytes']for r in candidate.part_references);reads=[];read=self.passages.logical_projection.read_part
        def observed(reference,**kwargs):
            body=read(reference,**kwargs);reads.append(len(body));return body
        metadata_ms=[];state=self.passages._append_state;prefix=self.passages._append_prefix
        def state_timing(*args,**kwargs):
            started=time.monotonic();result=state(*args,**kwargs);metadata_ms.append((time.monotonic()-started)*1000);return result
        def prefix_timing(*args,**kwargs):
            if len(args)>2 or kwargs.get('connection') is not None:return prefix(*args,**kwargs)
            started=time.monotonic();result=prefix(*args,**kwargs);metadata_ms.append((time.monotonic()-started)*1000);return result
        with patch.object(self.passages.logical_projection,'read_part',side_effect=observed),patch.object(self.passages,'_append_state',side_effect=state_timing),patch.object(self.passages,'_append_prefix',side_effect=prefix_timing):
            actual=self.passages._prepare(candidate,incremental=True)
        self.assertLess(sum(reads),full_bytes/4,'append reread the complete immutable prefix')
        self.assertEqual(actual.archive_bytes_read,sum(reads));self.assertEqual(actual.archive_parts_read,len(reads))
        oracle=self.passages._prepare(candidate,incremental=False)
        prefix=actual.previous_checkpoint["passage_ordinal"]
        self.assertEqual(actual.passages[prefix:],oracle.passages[prefix:])
        self.assertEqual([(p.passage_id,p.ordinal,p.receipts)for p in actual.passages],[(p.passage_id,p.ordinal,p.receipts)for p in oracle.passages])
        self.assertEqual(actual.dense_message_count,oracle.dense_message_count)
        self.assertEqual(actual.dense_message_bytes,oracle.dense_message_bytes)
        self.assertEqual(self.passages._commit(actual)['status'],'committed')
        after=self.projected();self.assertEqual({p.passage_id for p in oracle.passages},{r['passage_id']for r in after})
        self.assertTrue({r['passage_id']for r in before[:-1]}<={r['passage_id']for r in after})
        print(dict(full_archive_bytes=full_bytes,actual_archive_bytes=sum(reads),parts_read=len(reads),prepare_metadata_queries=len(metadata_ms),prepare_metadata_ms=metadata_ms))

    def next_candidate(self):
        self.logical.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,upload_concurrency=1)
        return self.passages._pending(tenant_id=self.tenant,limit=1)[0]
    def append(self,native='event-020',text='New appended observation.',second=20):
        self.ingest(native,text=text,occurred=f'2026-10-13T12:00:{second:02}Z')
        return self.next_candidate()
    def assert_oracle(self,candidate,prepared):
        oracle=self.passages._prepare(candidate,incremental=False)
        offset=prepared.previous_checkpoint['passage_ordinal']if prepared.previous_checkpoint else 0
        self.assertEqual(prepared.passages[offset:],oracle.passages[offset:])
        self.assertEqual([(p.passage_id,p.ordinal,p.receipts)for p in prepared.passages],[(p.passage_id,p.ordinal,p.receipts)for p in oracle.passages])
        self.assertEqual((prepared.dense_message_count,prepared.dense_message_bytes),(oracle.dense_message_count,oracle.dense_message_bytes))
        return oracle
    def test_edit_policy_and_unknown_checkpoint_use_full_owner(self):
        from recall_server.passage_projection import PassagePolicy
        self.seed()
        self.ingest('event-005',text='Edited earlier source evidence.',occurred='2026-10-13T12:00:05Z')
        candidate=self.next_candidate();prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNone(prepared.previous_checkpoint);self.assert_oracle(candidate,prepared);self.passages._commit(prepared)
        candidate=self.append();self.passages.policy=PassagePolicy(target_tokens=64,overlap_tokens=8)
        prepared=self.passages._prepare(candidate,incremental=True);self.assertIsNone(prepared.previous_checkpoint);self.assert_oracle(candidate,prepared);self.passages._commit(prepared)
        with self.store.connect()as c:
            c.execute("UPDATE canonical_passage_documents SET append_checkpoint='{}'::jsonb WHERE tenant_id=%s",(self.tenant,))
        candidate=self.append('event-021',second=21);prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNone(prepared.previous_checkpoint);self.assert_oracle(candidate,prepared)
    def test_concurrent_append_cannot_ack_stale_preparation(self):
        self.seed();before=self.projected();candidate=self.append();prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNotNone(prepared.previous_checkpoint)
        newer=self.append('event-021',second=21)
        self.assertEqual(self.passages._commit(prepared)['status'],'stale')
        self.assertEqual([{k:v for k,v in r.items()if k!='manifest_content_sha256'}for r in self.projected()],[{k:v for k,v in r.items()if k!='manifest_content_sha256'}for r in before])
        latest=self.passages._prepare(newer,incremental=True);self.assert_oracle(newer,latest);self.assertEqual(self.passages._commit(latest)['status'],'committed')
    def test_checkpoint_and_publication_rollback_together(self):
        self.seed();candidate=self.append();prepared=self.passages._prepare(candidate,incremental=True);before=self.projected()
        with self.store.connect()as c:
            prior=c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint']
        with patch('recall_server.passage_index.enqueue_search_outbox',side_effect=RuntimeError('synthetic transaction failure')):
            with self.assertRaisesRegex(RuntimeError,'synthetic transaction'):self.passages._commit(prepared)
        with self.store.connect()as c:
            after=c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint']
        self.assertEqual(after,prior);self.assertEqual(self.projected(),before)
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')
    def test_explicit_forget_never_retains_forgotten_receipt(self):
        self.seed();receipt=next(r for p in self.projected()for r in p['receipts']if '/event-005?'in r)
        request=dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,target_receipt=receipt,mode='explicit_forget',reason='owner_requested',requested_at='2026-10-13T13:00:00Z',idempotency_key='forget-append-prefix')
        self.assertEqual(self.plane.forget(request)['status'],'deleted')
        self.project()
        self.assertNotIn(receipt,[r for p in self.projected()for r in p['receipts']])
        candidate=self.append();prepared=self.passages._prepare(candidate,incremental=True);self.assert_oracle(candidate,prepared);self.passages._commit(prepared)
        self.assertNotIn(receipt,[r for p in self.projected()for r in p['receipts']])
    def test_legacy_schema_uses_full_projection(self):
        self.seed()
        with self.store.connect()as c:c.execute('ALTER TABLE canonical_passage_documents DROP COLUMN append_checkpoint')
        try:
            candidate=self.append();prepared=self.passages._prepare(candidate,incremental=True)
            self.assertIsNone(prepared.previous_checkpoint);self.assertIsNone(prepared.append_checkpoint);self.assert_oracle(candidate,prepared)
            self.assertEqual(self.passages._commit(prepared)['status'],'committed')
        finally:
            with self.store.connect()as c:c.execute('ALTER TABLE canonical_passage_documents ADD COLUMN append_checkpoint jsonb')

    def test_typed_tool_and_complete_report_append_matches_full_owner(self):
        from e2e_typed_passage_visibility import TypedPassageVisibility
        self.seed()
        TypedPassageVisibility.ingest_blocks(self,'tool-only',[dict(type='tool_result',content='internal scaffolding '*200)])
        self.ingest('report-tail',report='Verified completed report. '*250,occurred='2026-10-13T12:01:00Z')
        self.project()
        self.assertNotIn('internal scaffolding',' '.join(r['text_redacted']for r in self.projected()))
        self.ingest('report-followup',text='Human-visible followup after complete report.',occurred='2026-10-13T12:02:00Z')
        candidate=self.next_candidate();prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNotNone(prepared.previous_checkpoint);oracle=self.assert_oracle(candidate,prepared)
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')
        self.assertEqual(len(self.projected()),len(oracle.passages))
    def test_empty_rebuild_clears_previous_checkpoint(self):
        from e2e_typed_passage_visibility import TypedPassageVisibility
        self.ingest('only-event',text='Initially searchable human text.')
        self.project()
        with self.store.connect()as c:
            self.assertIsNotNone(c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint'])
        TypedPassageVisibility.ingest_blocks(self,'only-event',[dict(type='tool_result',content='raw tool evidence remains captured')])
        self.project();self.assertEqual(self.projected(),[])
        with self.store.connect()as c:
            self.assertIsNone(c.execute('SELECT append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['append_checkpoint'])
    def test_current_part_corruption_cannot_publish_or_ack(self):
        from recall_server.logical_evidence import LogicalEvidenceError
        self.seed();candidate=self.append();before=self.rows()
        with patch.object(self.passages.logical_projection,'read_part',side_effect=LogicalEvidenceError('logical_evidence_corrupt')):
            with self.assertRaises(LogicalEvidenceError):self.passages._prepare(candidate,incremental=True)
        self.assertEqual(self.rows(),before)
        with self.store.connect()as c:
            self.assertEqual(c.execute('SELECT count(*)AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s',(self.tenant,)).fetchone()['n'],1)

    def test_successive_appends_rebase_checkpoint_without_drift(self):
        self.seed()
        for i in range(20,24):
            candidate=self.append(f'event-{i:03}',text=('New append '+str(i)+' ')*90,second=i)
            prepared=self.passages._prepare(candidate,incremental=True);self.assertIsNotNone(prepared.previous_checkpoint)
            self.assert_oracle(candidate,prepared);self.assertEqual(self.passages._commit(prepared)['status'],'committed')
    def test_same_visible_text_metadata_revision_refreshes_receipts(self):
        from e2e_retained_passage_receipts import RetainedPassageReceipts
        for i in range(20):RetainedPassageReceipts.ingest(self,1,native=f'event-{i:03}',text=('Stable visible text '+str(i)+' ')*100)
        self.project();before=self.rows();old_receipt=next(r for p in before for r in p['receipts']if '/event-005?'in r)
        RetainedPassageReceipts.ingest(self,2,native='event-005',text=('Stable visible text 5 ')*100)
        candidate=self.next_candidate();prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNone(prepared.previous_checkpoint);self.assert_oracle(candidate,prepared);self.passages._commit(prepared)
        after=self.rows();self.assertEqual([p['passage_id']for p in before],[p['passage_id']for p in after])
        self.assertNotIn(old_receipt,[r for p in after for r in p['receipts']])
    def test_segmented_message_across_parts_preserves_absolute_spans(self):
        self.seed()
        with patch('recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES',1024):
            self.ingest('long-event',text='Unicode café evidence '*500,occurred='2026-10-13T12:01:00Z')
            self.project()
            self.ingest('tail-event',text='A later finding.',occurred='2026-10-13T12:02:00Z')
            candidate=self.next_candidate()
        prepared=self.passages._prepare(candidate,incremental=True);self.assertIsNotNone(prepared.previous_checkpoint)
        self.assert_oracle(candidate,prepared)
        self.assertTrue(any(span.record_count>1 for p in prepared.passages if hasattr(p,'spans')for span in p.spans))
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')

    def test_invalid_segment_restart_falls_back_to_full_owner(self):
        self.seed()
        with patch('recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES',1024):
            self.ingest('long-event',text='Unicode café evidence '*500,occurred='2026-10-13T12:01:00Z');self.project()
            self.ingest('tail-event',text='Later finding.',occurred='2026-10-13T12:02:00Z');candidate=self.next_candidate()
        with self.store.connect()as c:
            c.execute("UPDATE canonical_passage_documents SET append_checkpoint=jsonb_set(append_checkpoint,'{record_ordinal}',to_jsonb((append_checkpoint->>'record_ordinal')::int+1)) WHERE tenant_id=%s",(self.tenant,))
        prepared=self.passages._prepare(candidate,incremental=True)
        self.assertIsNone(prepared.previous_checkpoint);self.assert_oracle(candidate,prepared)
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')

    def test_disabled_reuse_preserves_full_owner_and_updates_checkpoint(self):
        import os
        from recall_server.passage_index import CanonicalPassageProjector
        self.seed();candidate=self.append()
        with patch.dict(os.environ,{'RECALL_PASSAGE_APPEND_REUSE':'0'}):
            full=CanonicalPassageProjector(self.store,self.passages.logical_projection,policy=self.passages.policy,bound_tenant_id=self.tenant)
        reads=[];read=full.logical_projection.read_part
        def observed(reference,**kwargs):
            body=read(reference,**kwargs);reads.append(len(body));return body
        prepared_values=[];prepare=full._prepare
        def capture(*args,**kwargs):
            from dataclasses import replace
            prepared=prepare(*args,**kwargs)
            prepared_values.append(replace(prepared, passages=tuple(prepared.passages), _resources=None))
            return prepared
        with patch.object(full.logical_projection,'read_part',side_effect=observed),patch.object(full,'_prepare',side_effect=capture):
            report=full.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,concurrency=1)
        self.assertEqual(report['documents'],1);self.assertEqual(len(prepared_values),1);prepared=prepared_values[0]
        self.assertEqual(report['prepare_full_documents'],1);self.assertEqual(report['prepare_reused_documents'],0)
        self.assertEqual(report['archive_bytes_read'],sum(reads));self.assertEqual(report['archive_parts_read'],len(reads))
        self.assertIsNone(prepared.previous_checkpoint)
        self.assertEqual(sum(reads),sum(r['size_bytes']for r in candidate.part_references))
        self.assertIsNotNone(prepared.append_checkpoint);self.assert_oracle(candidate,prepared)
        with self.store.connect()as c:
            state=c.execute('SELECT revision,append_checkpoint FROM canonical_passage_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()
        self.assertEqual(state['revision'],state['append_checkpoint']['revision'])
        self.append('event-021',second=21);reused=[];prepare_reused=self.passages._prepare
        def capture_reused(*args,**kwargs):
            value=prepare_reused(*args,**kwargs);reused.append(value);return value
        with patch.object(self.passages,'_prepare',side_effect=capture_reused):
            report=self.passages.project_pending(tenant_id=self.tenant,batch_size=1,max_batches=1,concurrency=1)
        self.assertEqual(report['documents'],1);self.assertEqual(len(reused),1);self.assertIsNotNone(reused[0].previous_checkpoint)
        self.assertEqual(report['prepare_reused_documents'],1);self.assertEqual(report['prepare_full_documents'],0)
        self.assertEqual(report['archive_bytes_read'],reused[0].archive_bytes_read)

if __name__=='__main__':
    suite=unittest.TestSuite(PassageAppendTail(name)for name in PassageAppendTail.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
