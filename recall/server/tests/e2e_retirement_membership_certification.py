"""Retirement completion can certify current passages without replaying publication."""
from dataclasses import replace
import os
import time
import unittest
from unittest.mock import patch

from e2e_parent_membership_publication import ParentMembershipPublicationTests
from e2e_compact_canonical_bodies import DurableArchiveFixture
from recall_server import chunk_retirement, retirement_runner, parent_membership_enrollment as membership
from recall_server.parent_membership_read import ReadSettings
from recall_server.receipt_membership import lookup_current_search_membership


class RetirementMembershipTests(ParentMembershipPublicationTests):
    def setUp(self):
        with patch('e2e_receipt_membership.FilesystemArchiveStore', DurableArchiveFixture):
            super().setUp()
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        guard = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        guard.start(); self.addCleanup(guard.stop)
        chunk_retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=True)
        self.retrieval.parent_membership_read = ReadSettings(enabled=True, binding_epoch='synthetic-epoch-a')

    def run_retirement(self, *, enabled=True, limits=None, deadline=None):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        return retirement_runner.run_retirement(self.store, self.archive,
            scope=retirement_runner.RetirementScope(self.tenant, self.principal, (self.source,)),
            apply=True, limits=retirement_runner.RunnerLimits(max_parents=1),
            parent_limits=limits or chunk_retirement.ParentRetirementLimits(),
            metadata_mode='singleton', deadline_at=deadline or time.monotonic()+10,
            membership_certification=self.settings(enabled=enabled))

    def authority(self):
        with patch('recall_server.receipt_membership.lookup_current_search_membership', wraps=lookup_current_search_membership) as native:
            with self.store.connect() as c:
                rows=self.retrieval._authorize_singletons(c, [(self.source,p) for p in self.scope['passage_ids']], time.monotonic()+3, stages={})
        return len(rows), native.call_count

    def test_committed_retirement_certifies_without_republishing_or_holding_locks(self):
        self.enroll(); original=membership.certify_retired_parent; calls=[]
        def certify(*args, **kwargs):
            self.assertLessEqual(kwargs['deadline_at']-time.monotonic(),3)
            with self.store.connect() as c:
                self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s',(self.tenant,)).fetchone()['n'],0)
                c.execute("SET LOCAL lock_timeout='100ms'")
                c.execute('SELECT 1 FROM canonical_chunk_retirement_progress WHERE tenant_id=%s FOR UPDATE NOWAIT',(self.tenant,))
                c.execute('SELECT 1 FROM canonical_parent_authority WHERE tenant_id=%s FOR UPDATE NOWAIT',(self.tenant,))
                logical=c.execute('SELECT logical_document_id FROM canonical_evidence_documents WHERE tenant_id=%s',(self.tenant,)).fetchone()['logical_document_id']
                self.assertTrue(c.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0)) AS ok',('lossless-passages\x1f'+logical,)).fetchone()['ok'])
            calls.append(True); return original(*args,**kwargs)
        with patch.object(membership,'certify_retired_parent',side_effect=certify):result=self.run_retirement()
        self.assertEqual(len(calls),1);self.assertEqual(result['retired_metadata_chunks'],2)
        self.assertEqual(result['membership_certified_passages'],len(self.ids));self.assertEqual(self.authority(),(len(self.ids),0))
        with self.store.connect() as c:self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s',(self.tenant,)).fetchone()['n'],0)

    def test_disabled_makes_no_certification_call(self):
        with patch.object(membership,'certify_retired_parent',side_effect=AssertionError('disabled')) as hook:
            result=self.run_retirement(enabled=False)
        hook.assert_not_called();self.assertEqual(result['retired_metadata_chunks'],2)
        self.assertEqual(result['membership_certified_passages'],0);self.assertEqual(self.authority(),(len(self.ids),1))

    def test_hook_failure_preserves_retirement_ack_and_public_authority(self):
        with patch.object(membership,'certify_retired_parent',side_effect=TimeoutError('optional timeout')):
            result=self.run_retirement()
        self.assertFalse(result['commit_outcome_unknown']);self.assertEqual(result['retired_metadata_chunks'],2)
        self.assertEqual(result['completed_parent_proofs'],1);self.assertEqual(result['membership_failed_parents'],1)
        self.assertEqual(self.authority(),(len(self.ids),1))
        with self.store.connect() as c:self.assertEqual(c.execute('SELECT status FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',(self.tenant,)).fetchone()['status'],'complete')

    def test_complete_noop_tail_can_certify_prior_metadata_retirement(self):
        self.run_retirement(enabled=False)
        # All physical work is done, but a resumed ledger still needs its EOF proof.
        with self.store.connect() as c:c.execute("UPDATE canonical_chunk_retirement_progress SET status='partial' WHERE tenant_id=%s",(self.tenant,))
        result=self.run_retirement()
        self.assertEqual(result['retired_metadata_chunks'],0);self.assertEqual(result['completed_parent_proofs'],1)
        self.assertEqual(result['membership_certified_passages'],len(self.ids));self.assertEqual(self.authority(),(len(self.ids),0))

    def test_partial_window_keeps_native_fallback(self):
        self.ingest('turn/extra','Synthetic extra turn')
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.publisher(self.settings(enabled=False)).project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.refresh_scope()
        with patch.object(membership,'certify_retired_parent',side_effect=AssertionError('partial')) as hook:
            result=self.run_retirement(limits=replace(chunk_retirement.ParentRetirementLimits(),batch_documents=1,max_batches=1))
        self.assertEqual(result['partial_parents'],1);hook.assert_not_called();self.assertEqual(self.authority(),(len(self.scope['passage_ids']),1))

    def test_edit_during_post_retirement_capture_cannot_grant_stale_authority(self):
        original=membership._capture
        def capture(*args,**kwargs):
            result=original(*args,**kwargs);self.edit();return result
        with patch.object(membership,'_capture',side_effect=capture):result=self.run_retirement()
        self.assertEqual(result['retired_metadata_chunks'],2);self.assertEqual(result['membership_certified_passages'],0)
        self.assertEqual(self.compare()['disagreements'],0);self.assertEqual(self.compare()['certified_passages'],0)

    def test_expired_adapter_never_checks_out_or_writes(self):
        with patch.object(self.store,'connect',side_effect=AssertionError('expired')) as checkout:
            result=membership.certify_retired_parent(self.store,tenant_id=self.tenant,source_id=self.source,
                native_parent_id='session',logical_document_id='unused',principal_id=self.principal,
                settings=self.settings(),deadline_at=time.monotonic()-1)
        checkout.assert_not_called();self.assertEqual(result['status'],'deadline');self.assertEqual(result['certified_passages'],0)

    def test_lost_retirement_ack_never_runs_optional_certification(self):
        original=retirement_runner.retire_parent_windows
        def lost(*args,**kwargs):
            committed=original(*args,**kwargs)
            error=chunk_retirement.ChunkRetirementError('parent_retirement_unavailable')
            error.committed=committed
            raise error
        with patch.object(retirement_runner,'retire_parent_windows',side_effect=lost), patch.object(membership,'certify_retired_parent',side_effect=AssertionError('unknown')) as hook:
            result=self.run_retirement()
        hook.assert_not_called();self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['retired_metadata_chunks'],2);self.assertEqual(self.authority(),(len(self.ids),1))

    def test_grant_revoked_between_capture_and_certificate_denies(self):
        original=membership._capture
        def capture(*args,**kwargs):
            result=original(*args,**kwargs)
            with self.store.connect() as c:c.execute('DELETE FROM brain_access_grants WHERE tenant_id=%s',(self.tenant,))
            return result
        with patch.object(membership,'_capture',side_effect=capture):result=self.run_retirement()
        self.assertEqual(result['retired_metadata_chunks'],2);self.assertEqual(result['membership_certified_passages'],0)
        self.assertEqual(self.compare()['status'],'unauthorized')

    def test_forget_between_capture_and_certificate_never_accepts(self):
        original=membership._capture
        def capture(*args,**kwargs):
            captured=original(*args,**kwargs)
            self.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
                tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
                target_receipt=self.receipt,mode='explicit_forget',reason='owner_requested',
                requested_at='2026-10-16T00:00:00Z',idempotency_key='retirement-membership-forget'))
            return captured
        with patch.object(membership,'_capture',side_effect=capture):result=self.run_retirement()
        self.assertEqual(result['retired_metadata_chunks'],2);self.assertEqual(result['membership_certified_passages'],0)
        self.assertEqual(self.authority(),(0,1))

    def test_publication_manifest_change_during_capture_falls_back(self):
        original=membership._capture
        def capture(*args,**kwargs):
            captured=original(*args,**kwargs)
            with self.store.connect() as c:c.execute('UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s',('a'*64,self.tenant))
            return captured
        with patch.object(membership,'_capture',side_effect=capture):result=self.run_retirement()
        self.assertEqual(result['membership_certified_passages'],0);self.assertEqual(self.authority(),(len(self.ids),1))

    def test_later_certificate_page_failure_keeps_acknowledged_progress(self):
        self.ingest('turn/long','Synthetic evidence paragraph. '*3000)
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.publisher(self.settings(enabled=False)).project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.refresh_scope();self.assertGreater(len(self.scope['passage_ids']),1)
        original=membership.enroll_parent_passages;calls=[]
        def enroll(*args,**kwargs):
            calls.append(kwargs['passage_ids'])
            if len(calls)==2:raise TimeoutError('second certificate page unknown')
            return original(*args,**kwargs)
        with patch.dict(membership._CERTIFICATION_LIMITS,passages=1),patch.object(membership,'enroll_parent_passages',side_effect=enroll):result=self.run_retirement()
        self.assertEqual(len(calls),2);self.assertNotEqual(calls[0],calls[1])
        self.assertEqual(result['membership_certified_passages'],1);self.assertEqual(result['membership_failed_parents'],1)
        self.assertFalse(result['commit_outcome_unknown']);self.assertEqual(self.compare()['disagreements'],0)

    def test_metadata_refusal_keeps_whole_scope_on_native_fallback(self):
        with patch.dict(membership._CERTIFICATION_LIMITS,metadata_bytes=1):result=self.run_retirement()
        self.assertEqual(result['retired_metadata_chunks'],2);self.assertEqual(result['membership_certified_passages'],0)
        self.assertEqual(result['membership_deferred_parents'],1);self.assertEqual(self.authority(),(len(self.ids),1))

    def test_actual_maintenance_uses_existing_flag_and_logs_committed_metrics(self):
        from io import BytesIO
        import json
        from unittest.mock import Mock
        from recall_server import retirement_policy as policy
        from recall_server.retirement_maintenance import RetirementMaintenance
        from recall_server.retirement_supervisor import RetirementSupervisor
        scope=retirement_runner.RetirementScope(self.tenant,self.principal,(self.source,))
        policy.start_policy(self.store,scope=scope)
        while not policy.capture_baseline(self.store,scope=scope)['complete']:
            pass
        policy.set_policy_enabled(self.store,scope=scope,
            reviewed_plan=policy.policy_plan(self.store,scope=scope),enabled=True)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s",(self.tenant,))
        with patch.dict(os.environ,RECALL_PARENT_MEMBERSHIP_CERTIFICATION='1',
                        RECALL_PARENT_MEMBERSHIP_EPOCH='synthetic-epoch-a'):
            owner=RetirementMaintenance(self.store,self.archive,tenant_id=self.tenant,
                enabled=True,seconds=10,max_parents=1,metadata_mode='singleton')
            result=owner.cycle(busy=False)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['retired_metadata_chunks'],2)
        self.assertEqual(result['membership_certified_passages'],len(self.ids))
        self.assertEqual(self.authority(),(len(self.ids),0))
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT maintenance_intent FROM canonical_retirement_source_policy WHERE tenant_id=%s AND source_id=%s',(self.tenant,self.source)).fetchone()['maintenance_intent'])
        supervisor=RetirementSupervisor('postgresql://synthetic.invalid/brain',tenant_id=self.tenant,seconds=10)
        supervisor.child=Mock(returncode=0,stdout=BytesIO(json.dumps(result).encode()))
        supervisor.child.poll.return_value=0
        with self.assertLogs('recall_server.retirement_supervisor',level='INFO') as logs:
            observed=supervisor.cycle()
        self.assertEqual(observed['membership_certified_passages'],len(self.ids))
        for field in ('membership_certified_passages','membership_failed_parents',
                      'membership_deferred_parents','membership_elapsed_ms'):
            self.assertIn(f'{field}={result[field]}',logs.output[0])
        supervisor.close()

    def test_actual_retirement_child_store_supports_enrollment_and_checkout_deadline(self):
        from recall_server.retirement_process import RetirementStore
        with patch.dict(os.environ,RECALL_SEARCH_PLANE='turbopuffer',RECALL_VERIFIED_SINGLETON_RECEIPTS='1'):
            child=RetirementStore(os.environ['RECALL_DATABASE_URL'])
        original=self.store
        try:
            self.store=child
            result=self.run_retirement()
            self.assertEqual(result['retired_metadata_chunks'],2)
            self.assertEqual(result['membership_certified_passages'],len(self.ids))
            with child.connect():
                started=time.monotonic()
                outcome=membership.certify_retired_parent(child,tenant_id=self.tenant,source_id=self.source,
                    native_parent_id='session',logical_document_id='unused',principal_id=self.principal,
                    settings=self.settings(),deadline_at=started+.03)
                self.assertLess(time.monotonic()-started,1)
                self.assertEqual(outcome['status'],'failed');self.assertEqual(outcome['certified_passages'],0)
        finally:
            self.store=original
            child.close()
        self.assertEqual(self.authority(),(len(self.ids),0))

if __name__=='__main__':
    suite=unittest.TestSuite(RetirementMembershipTests(n) for n in RetirementMembershipTests.__dict__ if n.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
