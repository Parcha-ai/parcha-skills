"""Actual PG integration of contiguous discovery with existing retirement owners."""
from dataclasses import replace
import time
import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile
import json

import e2e_selective_retirement_experiment as original
from recall_server import chunk_retirement as owner


class WindowTests(unittest.TestCase):
    setUp = original.SelectiveExperiment.setUp
    state = original.SelectiveExperiment.state
    actual_writer_fixture = original.SelectiveExperiment.actual_writer_fixture

    def prepare(self, count=2):
        from recall_server.parent_chunk_window import prepare_window
        return prepare_window(self.store, self.archive, scope=self.scope,
            principal_id=self.principal, limits=replace(self.limits, batch_documents=count),
            deadline_at=time.monotonic()+20)

    def apply(self, window):
        return owner._retire_parent_batch(self.store, proof=window['proof'], rows=window['rows'],
            scope=self.scope, limits=self.limits, deadline_at=time.monotonic()+5,
            complete=False, remaining_bytes=self.limits.max_clear_bytes,
            owner_principal_id=self.principal, coverage=window['coverage'])

    def progress(self):
        with self.store.connect() as c:
            return owner._parent_progress(self.store, c, self.scope, time.monotonic()+5)

    def test_two_contiguous_windows_clear_only_four_documents(self):
        before=self.state()
        first=self.prepare(); self.assertEqual(first['coverage']['start_cursor'], -1)
        self.assertEqual(self.apply(first)['cleared_documents'], 2)
        second=self.prepare()
        self.assertEqual(second['coverage']['start_cursor'], first['coverage']['covered_through'])
        self.assertEqual(self.apply(second)['cleared_documents'], 2)
        after=self.state()
        changed={row['document_id'] for old,row in zip(before,after) if old != row}
        self.assertEqual(len(changed),4)
        self.assertEqual(self.progress()['status'],'partial')

    def test_stale_start_refuses_and_preserves_first_ack(self):
        window=self.prepare(); self.apply(window); after=self.state()
        with self.assertRaisesRegex(owner.ChunkRetirementError,'window_cursor'):
            self.apply(window)
        self.assertEqual(self.state(),after)

    def test_unchecked_gap_is_not_a_coverage_witness(self):
        window=self.prepare(); before=self.state()
        window['coverage']['events'][0]['first']+=1
        with self.assertRaisesRegex(owner.ChunkRetirementError,'window_coverage'):
            self.apply(window)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_eof_explicitly_hands_off_to_original_full_proof(self):
        from recall_server.parent_chunk_window import retire_window
        window=self.prepare(12); self.apply(window)
        self.assertEqual(self.progress()['status'],'partial')
        result=retire_window(self.store,self.archive,scope=self.scope,
            principal_id=self.principal,limits=self.limits,
            deadline_at=time.monotonic()+20,reviewed_plan=window['plan'])
        self.assertTrue(result['complete'])
        self.assertEqual(result['completion_proof'],'original_full_parent')
        self.assertEqual(self.progress()['status'],'complete')

    def structural(self, native, present=True):
        from psycopg.types.json import Jsonb
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET canonical_redacted=(canonical_redacted-'type')||%s WHERE tenant_id=%s AND native_id=%s",
                (Jsonb({'type':'token_count'} if present else {}),self.tenant,native))

    def test_excluded_only_window_advances_without_clear_and_next_window_works(self):
        for row in self.docs[:2]: self.structural(row['native_id'])
        before=self.state(); window=self.prepare()
        self.assertEqual(window['rows'],[])
        self.assertEqual(self.apply(window)['cleared_documents'],0)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],1)
        self.assertEqual(self.apply(self.prepare())['cleared_documents'],2)

    def test_excluded_witness_change_refuses_cursor_advance(self):
        self.structural(self.docs[0]['native_id']); window=self.prepare()
        self.structural(self.docs[0]['native_id'],False)
        before=self.state()
        with self.assertRaisesRegex(owner.ChunkRetirementError,'witness_changed'):
            self.apply(window)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_absent_current_witness_must_remain_absent(self):
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND native_id=%s',
                (self.tenant,self.docs[0]['native_id']))
        window=self.prepare()
        self.assertEqual(window['coverage']['reasons'][self.docs[0]['native_id']],'not_current')
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=true WHERE tenant_id=%s AND native_id=%s',
                (self.tenant,self.docs[0]['native_id']))
        with self.assertRaisesRegex(owner.ChunkRetirementError,'witness_changed'):self.apply(window)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_excluded_native_lock_is_required(self):
        native=self.docs[0]['native_id']; self.structural(native); window=self.prepare()
        with self.store.connect() as c,c.transaction():
            c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                (f'v2\x1f{self.tenant}\x1f{self.source}\x1f{native}',))
            with self.assertRaisesRegex(owner.ChunkRetirementError,'lock_busy'):self.apply(window)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_aggregate_discovery_plus_reread_budget_is_enforced(self):
        from recall_server.parent_chunk_window import prepare_window
        with self.store.connect() as c:
            part=c.execute('SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s ORDER BY part_ordinal LIMIT 1',
                (self.tenant,)).fetchone()['size_bytes']
        with self.assertRaisesRegex(owner.ChunkRetirementError,'archive_budget'):
            prepare_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
                limits=replace(self.limits,batch_documents=1,max_archive_bytes=part),deadline_at=time.monotonic()+10)
        self.assertEqual(sum(self.archive.reads.values()),0)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_source_revocation_after_discovery_refuses(self):
        window=self.prepare();before=self.state()
        with self.store.connect() as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s',(self.tenant,))
        with self.assertRaises(owner.ChunkRetirementError):self.apply(window)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_manifest_changed_after_discovery_refuses(self):
        window=self.prepare();before=self.state()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_evidence_documents SET document_content_sha256=repeat('0',64) WHERE tenant_id=%s",(self.tenant,))
        with self.assertRaisesRegex(owner.ChunkRetirementError,'parent_changed'):self.apply(window)
        self.assertEqual(self.state(),before)

    def test_real_body_restore_disables_old_window(self):
        window=self.prepare();self.apply(window);next_window=self.prepare()
        document=window['rows'][0]['document_id']
        scope=dict(tenant_id=self.tenant,source_id=self.source,document_ids=(document,))
        preview=owner.retire_current_chunks(self.store,self.archive,**scope,restore=True)
        owner.retire_current_chunks(self.store,self.archive,**scope,restore=True,apply=True,reviewed_plan=preview['plan'])
        before=self.state()
        with self.assertRaisesRegex(owner.ChunkRetirementError,'disabled'):self.apply(next_window)
        self.assertEqual(self.state(),before)
        self.assertFalse(self.progress()['enabled'])

    def test_actual_parent_forget_refuses_window(self):
        helper=self.actual_writer_fixture(include_parent=True);window=self.prepare()
        helper.plane.evidence_projector=helper.projector
        result=helper.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            target_receipt=f'recall://{self.source}/session?rev=1#item=0',mode='explicit_forget',
            reason='owner_requested',requested_at='2026-10-15T00:00:00Z',idempotency_key='window-parent-forget'))
        self.assertEqual(result['status'],'deleted');before=self.state()
        with self.assertRaises(Exception):self.apply(window)
        self.assertEqual(self.state(),before)

    def test_split_events_and_final_short_window(self):
        from e2e_archive_reprojection import fixture
        from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
        root=tempfile.TemporaryDirectory();self.addCleanup(root.cleanup)
        with patch.object(CanonicalLogicalEvidenceProjector,'_text_segments',staticmethod(lambda text:(text[:500],text[500:]))):
            tenant,source,archive,*_=fixture(self.store,Path(root.name),count=3)
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",(tenant,self.principal,source))
        self.scope=(tenant,source,'session');self.tenant,self.source=tenant,source;self.archive=archive
        owner.set_parent_retirement_enabled(self.store,tenant_id=tenant,source_id=source,native_parent_id='session',enabled=True)
        first=self.prepare();self.assertTrue(all(e['count']==2 for e in first['coverage']['events']))
        self.assertEqual(self.apply(first)['cleared_documents'],2)
        last=self.prepare();self.assertEqual(len(last['coverage']['events']),1)
        self.assertEqual(last['coverage']['start_cursor'],3)
        self.assertEqual(self.apply(last)['cleared_documents'],1)
        self.assertEqual(self.progress()['last_record_ordinal'],5)
        self.assertEqual(self.progress()['status'],'partial')

    def test_real_commit_then_lost_ack_preserves_partial_not_completion(self):
        window=self.prepare()
        def lost():
            self.apply(window)
            raise ConnectionError('synthetic after acknowledged real commit')
        with self.assertRaises(ConnectionError):lost()
        self.assertEqual(self.progress()['last_record_ordinal'],1)
        self.assertEqual(self.progress()['cumulative_cleared_documents'],2)
        with self.assertRaisesRegex(owner.ChunkRetirementError,'window_cursor'):self.apply(window)

    def test_hash_budget_selects_longest_prefix_instead_of_stalling(self):
        from recall_server.parent_chunk_window import prepare_window
        size=len(self.texts[self.docs[0]['native_id']].encode())
        window=prepare_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
            limits=replace(self.limits,batch_documents=4,hash_bytes=size+1),deadline_at=time.monotonic()+20)
        self.assertEqual(len(window['rows']),1)
        self.assertEqual(window['coverage']['covered_through'],0)
        self.assertEqual(self.apply(window)['cleared_documents'],1)

    def test_remaining_clear_budget_selects_one_complete_event(self):
        from recall_server.parent_chunk_window import prepare_window
        size=len(self.texts[self.docs[0]['native_id']].encode())
        window=prepare_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
            limits=self.limits,deadline_at=time.monotonic()+20,remaining_bytes=size)
        self.assertEqual(len(window['rows']),1)
        self.assertEqual(window['coverage']['covered_through'],0)

    def test_wrapper_preserves_five_second_write_bound_and_metadata_mode(self):
        from recall_server.parent_chunk_window import retire_window
        window=self.prepare();started=time.monotonic()
        self.store.verified_singleton_receipts=True;self.store.search_plane='turbopuffer'
        with patch.object(owner,'_retire_parent_batch',return_value=dict(cleared_documents=0)) as write:
            retire_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
                limits=self.limits,deadline_at=started+240,reviewed_plan=window['plan'],metadata_mode='singleton')
        self.assertEqual(write.call_args.kwargs['metadata_mode'],'singleton')
        self.assertLessEqual(write.call_args.kwargs['deadline_at'],time.monotonic()+5)
        self.assertGreater(write.call_args.kwargs['deadline_at'],started)

    def test_eof_handoff_preserves_claimed_epoch(self):
        from recall_server import parent_chunk_window as window_owner
        window=self.prepare(12);self.apply(window);eof=self.prepare()
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,
            native_parent_id='session',enabled=True)
        with patch.object(window_owner,'prepare_window',return_value=eof):
            with self.assertRaisesRegex(owner.ChunkRetirementError,'disabled'):
                window_owner.retire_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
                    limits=self.limits,deadline_at=time.monotonic()+20,reviewed_plan=eof['plan'])
        self.assertNotEqual(self.progress()['status'],'complete')

    def test_actual_locator_repair_invalidates_old_exclusion_witness(self):
        from recall_server.locator_backfill_plan import apply_parent
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND native_id=%s',
                (self.tenant,self.docs[0]['native_id']))
        window=self.prepare()
        self.assertEqual(window['coverage']['reasons'][self.docs[0]['native_id']],'unlocated')
        result=apply_parent(self.store,self.archive,tenant_id=self.tenant,source_id=self.source,native_parent_id='session')
        self.assertEqual(result['applied_documents'],1)
        before=self.state()
        with self.assertRaisesRegex(owner.ChunkRetirementError,'disabled'):self.apply(window)
        self.assertEqual(self.state(),before)

    def test_actual_native_forget_refuses_coverage(self):
        from recall_server.canonical import CanonicalPlane
        window=self.prepare()
        plane=CanonicalPlane(self.store,self.archive,evidence_projector=self.projector,chunk_body_archive=self.archive)
        result=plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,
            principal_id=self.principal,source_id=self.source,target_receipt=f"recall://{self.source}/{self.docs[0]['native_id']}?rev=1#item=0",
            mode='explicit_forget',reason='owner_requested',requested_at='2026-10-15T00:00:00Z',idempotency_key='window-native-forget'))
        self.assertEqual(result['status'],'deleted');before=self.state()
        with self.assertRaises(Exception):self.apply(window)
        self.assertEqual(self.state(),before)

    def test_parent_forget_fence_precedes_discovery_io(self):
        from recall_server.canonical import _identity_sha256
        with self.store.connect() as c:
            c.execute("""INSERT INTO forget_tombstones(tenant_id,source_id,target_identity_sha256,
                mode,reason,deleted_at,status,idempotency_key)
                VALUES(%s,%s,%s,'explicit_forget','owner_requested',now(),'deleting','window-parent-block')""",
                (*self.scope[:2],_identity_sha256(*self.scope)))
        with self.assertRaisesRegex(owner.ChunkRetirementError,'forgotten'):self.prepare()
        self.assertFalse(self.archive.reads)

    def test_same_parent_lock_is_required_even_when_not_an_event(self):
        window=self.prepare()
        with self.store.connect() as c,c.transaction():
            c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                (f'v2\x1f{self.tenant}\x1f{self.source}\x1fsession',))
            with self.assertRaisesRegex(owner.ChunkRetirementError,'lock_busy'):self.apply(window)

    def test_singleton_mode_actual_admission_is_preserved(self):
        from recall_server.parent_chunk_window import retire_window
        with self.store.connect() as c:c.execute((original.SERVER/'schema/080_singleton_receipt_profile.sql').read_text())
        self.actual_writer_fixture()
        self.store.verified_singleton_receipts=True;self.store.search_plane='turbopuffer'
        window=self.prepare(1)
        result=retire_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
            limits=replace(self.limits,batch_documents=1),deadline_at=time.monotonic()+20,
            reviewed_plan=window['plan'],metadata_mode='singleton')
        self.assertEqual(result['retired_metadata_chunks'],1)
        self.assertFalse(result['complete'])

    def test_stop_after_proof_does_not_write(self):
        from recall_server.parent_chunk_window import retire_window
        window=self.prepare();before=self.state()
        stop=iter((False,True))
        result=retire_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
            limits=self.limits,deadline_at=time.monotonic()+20,reviewed_plan=window['plan'],should_stop=lambda:next(stop))
        self.assertEqual(result['status'],'stopped');self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_chunk_and_result_limits_choose_prefix(self):
        from recall_server import parent_chunk_window as window_owner
        first=window_owner.prepare_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
            limits=replace(self.limits,batch_chunks=1),deadline_at=time.monotonic()+20)
        self.assertEqual(len(first['rows']),1)
        with patch.object(window_owner,'MAX_RESULT_BYTES',len(self.texts[self.docs[0]['native_id']].encode())+1):
            second=self.prepare(4)
        self.assertEqual(len(second['rows']),1)

    def test_individually_oversized_for_attempt_is_a_hold(self):
        from recall_server.parent_chunk_window import prepare_window
        with self.assertRaisesRegex(owner.ChunkRetirementError,'individual_budget_hold'):
            prepare_window(self.store,self.archive,scope=self.scope,principal_id=self.principal,
                limits=replace(self.limits,hash_bytes=1),deadline_at=time.monotonic()+20)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_shared_part_cost_across_three_windows_is_explicit(self):
        from e2e_archive_reprojection import fixture
        from e2e_logical_source_integrity import SmallPartProjection
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        root=tempfile.TemporaryDirectory();self.addCleanup(root.cleanup)
        def normal_parts(projection,**values):
            self.assertEqual(projection.store.active_connections,0)
            return LogicalEvidenceProjectionStore.put_records(projection,**values,part_bytes=1024**2)
        with patch.object(SmallPartProjection,'put_records',normal_parts):
            tenant,source,archive,*_=fixture(self.store,Path(root.name),count=192)
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",(tenant,self.principal,source))
            parts=c.execute('SELECT artifact_id,size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s',(tenant,)).fetchall()
        self.assertEqual(len(parts),1)
        self.scope=(tenant,source,'session');self.tenant,self.source=tenant,source;self.archive=archive
        owner.set_parent_retirement_enabled(self.store,tenant_id=tenant,source_id=source,native_parent_id='session',enabled=True)
        totals=dict(discovery_archive_bytes=0,proof_archive_bytes=0,discovery_archive_gets=0,proof_archive_gets=0)
        start=time.monotonic()
        for _ in range(3):
            window=self.prepare(64)
            self.assertEqual(self.apply(window)['cleared_documents'],64)
            for key in totals:totals[key]+=window['cost'][key]
        self.assertEqual(sum(archive.reads.values()),6)
        self.assertEqual(self.progress()['status'],'partial')
        self.assertEqual(self.progress()['last_record_ordinal'],191)
        print(json.dumps(dict(experiment='three_windows_shared_part',documents=192,window_documents=64,
            distinct_part_bytes=parts[0]['size_bytes'],total_reads=6,elapsed_seconds=time.monotonic()-start,**totals)))

    def test_actual_new_revision_after_proof_refuses(self):
        helper=self.actual_writer_fixture();window=self.prepare()
        helper.ingest('turn/a','Changed actual canonical revision')
        before=self.state()
        with self.assertRaises(owner.ChunkRetirementError):self.apply(window)
        self.assertEqual(self.state(),before)
        self.assertEqual(self.progress()['last_record_ordinal'],-1)

    def test_actual_publication_replaces_manifest_and_invalidates_window(self):
        helper=self.actual_writer_fixture();window=self.prepare()
        helper.ingest('turn/c','New actual canonical append')
        result=helper.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(result['failed'],0);self.assertEqual(result['documents'],1)
        before=self.state()
        with self.assertRaises(owner.ChunkRetirementError):self.apply(window)
        self.assertEqual(self.state(),before)

    def test_native_discovery_uses_index_with_off_parent_metadata(self):
        from recall_server.parent_chunk_window import _WITNESSES
        from psycopg.types.json import Jsonb
        with self.store.connect() as c:
            seed=c.execute('''SELECT to_jsonb(e) AS event,to_jsonb(d) AS document FROM canonical_events e
                JOIN canonical_documents d USING(tenant_id,source_id,event_id)
                WHERE d.tenant_id=%s ORDER BY d.native_id LIMIT 1''',(self.tenant,)).fetchone()
            event,document=seed['event'],seed['document']
            c.execute('''INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,
                artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted)
                SELECT %s,%s,'evt_'||md5('off-event-'||n),'off/native/'||n,'other',%s,%s,%s,%s,1,%s,%s,%s
                FROM generate_series(1,2000)n''',(*self.scope[:2],event['artifact_id'],event['job_id'],event['kind'],
                event['content_sha256'],event['occurred_at'],event['observed_at'],Jsonb(event['canonical_redacted'])))
            c.execute('''INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,
                content_sha256,revision,is_current,text_redacted,text_sha256)
                SELECT %s,%s,'doc_'||md5('off-doc-'||n),'evt_'||md5('off-event-'||n),%s,'off/native/'||n,%s,1,true,%s,%s
                FROM generate_series(1,2000)n''',(*self.scope[:2],document['artifact_id'],document['content_sha256'],
                document['text_redacted'],document['text_sha256']))
            for table in ('canonical_documents','canonical_events','raw_artifacts'):c.execute('ANALYZE '+table)
            c.execute("SET LOCAL statement_timeout='3s'")
            plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+_WITNESSES,
                (*self.scope[:2],[row['native_id'] for row in self.docs[:2]])).fetchone()['QUERY PLAN']
        nodes=[]
        def walk(value):
            if isinstance(value,dict):
                if 'Node Type' in value:nodes.append(value)
                for child in value.values():walk(child)
            elif isinstance(value,list):
                for child in value:walk(child)
        walk(plan)
        self.assertFalse(any(n.get('Relation Name')in('canonical_documents','canonical_events') and n['Node Type']=='Seq Scan' for n in nodes))
        self.assertTrue(any(n.get('Index Name')=='canonical_documents_one_current_idx' for n in nodes))
        for node in nodes:
            if node.get('Relation Name') in ('canonical_documents','canonical_events','raw_artifacts'):
                self.assertLessEqual(node['Actual Loops'],2)
                self.assertLessEqual(node['Actual Rows'],2)
        self.assertEqual(self.apply(self.prepare())['cleared_documents'],2)
        print(json.dumps(dict(experiment='window_native_lookup_plan',off_parent_documents=2000,
            nodes=[{key:n[key] for key in ('Node Type','Relation Name','Index Name','Actual Rows','Actual Loops') if key in n} for n in nodes])))


    def test_parentless_native_uses_original_self_parent_identity(self):
        from e2e_archive_reprojection import insert_record
        from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
        with self.store.connect() as c:
            insert_record(c,tenant=self.tenant,source=self.source,parent=None,native='self-parent',
                text='Standalone archived record',role='assistant',byte_start=999)
            mark_logical_evidence_dirty(c,tenant_id=self.tenant,source_id=self.source,native_ids=['self-parent'],reason='ingest')
        report=self.projector.project_pending(tenant_id=self.tenant,batch_size=2,max_batches=1,upload_concurrency=1)
        self.assertEqual(report['failed'],0)
        self.scope=(self.tenant,self.source,'self-parent')
        owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,native_parent_id='self-parent',enabled=True)
        result=self.apply(self.prepare())
        self.assertEqual(result['cleared_documents'],1)
        self.assertEqual(result['cleared_utf8_bytes'],len('Standalone archived record'))

if __name__=='__main__':
    unittest.main()
