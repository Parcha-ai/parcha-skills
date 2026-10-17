"""Actual runner claims and cumulative subset retirement budgets."""
from dataclasses import replace
import time
import unittest
from unittest.mock import patch
import e2e_selective_retirement_experiment as original
from recall_server import chunk_retirement as owner
from recall_server import retirement_runner as runner


class RunnerWindows(unittest.TestCase):
    setUp = original.SelectiveExperiment.setUp
    state = original.SelectiveExperiment.state

    def run_parent(self, runner_limits=None, should_stop=None, **changes):
        limits=replace(self.limits,batch_documents=2,**changes)
        self.archive.reads.clear()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s",(self.tenant,))
        return runner.run_retirement(self.store,self.archive,
            scope=runner.RetirementScope(self.tenant,self.principal,(self.source,)),
            apply=True,limits=runner_limits or runner.RunnerLimits(max_parents=1),parent_limits=limits,
            should_stop=should_stop,
            deadline_at=time.monotonic()+20)

    def test_existing_runner_two_windows_share_batch_cap(self):
        result=self.run_parent(max_batches=2)
        self.assertEqual(result['cleared_documents'],4)
        self.assertEqual(result['batches'],2)
        self.assertEqual(result['completed_parent_proofs'],0)
        self.assertEqual(result['archive_gets'],8)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(sum(bool(r['text_redacted'])for r in self.state()),8)

    def test_existing_runner_full_completion_only_at_eof(self):
        from recall_server.parent_chunk_proof import prove_parent_chunks
        from recall_server import parent_chunk_window as window_owner
        with patch('recall_server.parent_chunk_proof.prove_parent_chunks',wraps=prove_parent_chunks)as full, \
                patch.object(window_owner,'retire_window',wraps=window_owner.retire_window)as windows:
            # Six selected windows ACK the bodies; the unchanged EOF owner
            # requires its own remaining batch to certify completion.
            result=self.run_parent(max_batches=7)
        self.assertEqual(result['cleared_documents'],12)
        self.assertEqual(result['completed_parent_proofs'],1)
        self.assertEqual(result['batches'],7)
        self.assertEqual(full.call_count,1)
        self.assertEqual(result['archive_gets'],36)
        tail = windows.call_args.kwargs
        self.assertEqual(full.call_args.kwargs['limits'],tail['limits'])
        self.assertEqual(full.call_args.kwargs['deadline_at'],tail['deadline_at'])
        self.assertEqual(tail['limits'].max_batches,1)
        self.assertLess(tail['limits'].max_archive_bytes,runner.RunnerLimits().max_archive_bytes)
        self.assertLess(tail['limits'].max_clear_bytes,self.limits.max_clear_bytes)

    def test_exact_six_batches_ack_bodies_then_restart_completes(self):
        result = self.run_parent(max_batches=6)
        self.assertEqual(result['cleared_documents'], 12)
        self.assertEqual(result['batches'], 6)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        resumed = self.run_parent(max_batches=1)
        self.assertEqual(resumed['cleared_documents'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 1)

    def test_same_attempt_clear_budget_stops_after_first_window(self):
        with self.store.connect() as c:
            size=c.execute('SELECT sum(octet_length(text_redacted))::bigint AS bytes FROM canonical_chunks WHERE tenant_id=%s AND document_id=ANY(%s)',
                (self.tenant,[d['document_id']for d in self.docs[:2]])).fetchone()['bytes']
        result=self.run_parent(max_clear_bytes=size,max_batches=8)
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['cleared_utf8_bytes'],size)
        self.assertEqual(result['batches'],1)
        self.assertEqual(result['archive_gets'],4)

    def test_invocation_archive_meter_is_shared_across_windows(self):
        with self.store.connect() as c:
            parts=c.execute('SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s ORDER BY part_ordinal LIMIT 2',(self.tenant,)).fetchall()
        cap=2*sum(p['size_bytes']for p in parts)
        result=self.run_parent(runner_limits=runner.RunnerLimits(max_parents=1,max_archive_bytes=cap),max_batches=8)
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['archive_bytes'],cap)
        self.assertEqual(result['batches'],1)

    def test_stop_callback_retains_acknowledged_prefix(self):
        commit=owner._retire_parent_batch;stopped=False
        def after(*args,**kwargs):
            nonlocal stopped
            result=commit(*args,**kwargs);stopped=True;return result
        with patch.object(owner,'_retire_parent_batch',side_effect=after):
            result=self.run_parent(max_batches=8,should_stop=lambda:stopped)
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['batches'],1)
        self.assertEqual(result['completed_parent_proofs'],0)

    def test_new_epoch_between_windows_refuses_before_more_archive(self):
        commit=owner._retire_parent_batch
        def after(*args,**kwargs):
            result=commit(*args,**kwargs)
            owner.set_parent_retirement_enabled(self.store,tenant_id=self.tenant,source_id=self.source,
                native_parent_id='session',enabled=False)
            return result
        with patch.object(owner,'_retire_parent_batch',side_effect=after):
            result=self.run_parent(max_batches=8)
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['archive_gets'],4)
        self.assertEqual(result['errors'],{'parent_retirement_disabled':1})
        self.assertFalse(result['commit_outcome_unknown'])

    def test_second_real_commit_lost_ack_preserves_only_first_ack(self):
        commit=owner._retire_parent_batch;calls=0
        def after(*args,**kwargs):
            nonlocal calls
            result=commit(*args,**kwargs);calls+=1
            if calls==2:raise ConnectionError('synthetic lost COMMIT acknowledgement')
            return result
        with patch.object(owner,'_retire_parent_batch',side_effect=after):
            result=self.run_parent(max_batches=8)
        self.assertEqual(calls,2)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['batches'],1)
        self.assertEqual(sum(not r['text_redacted']for r in self.state()),4)
        self.assertEqual(result['archive_gets'],8)

    def test_unusable_clear_residue_is_partial_not_failed_and_avoids_next_window(self):
        with self.store.connect() as c:
            size=c.execute('SELECT sum(octet_length(text_redacted))::bigint AS bytes FROM canonical_chunks WHERE tenant_id=%s AND document_id=ANY(%s)',
                (self.tenant,[d['document_id']for d in self.docs[:3]])).fetchone()['bytes']
        result=self.run_parent(runner_limits=runner.RunnerLimits(max_parents=1,max_clear_bytes=size+1),max_clear_bytes=size+1,max_batches=8)
        self.assertEqual(result['cleared_documents'],3)
        self.assertEqual(result['cleared_utf8_bytes'],size)
        self.assertEqual(result['batches'],2)
        self.assertEqual(result['failed_parents'],0)
        self.assertEqual(result['errors'],{})
        self.assertEqual(result['status'],'clear_budget_limited')
        self.assertEqual(result['archive_gets'],7)

    def test_unusable_archive_residue_keeps_prefix_without_more_gets(self):
        with self.store.connect() as c:
            parts=c.execute('SELECT size_bytes FROM canonical_evidence_document_parts WHERE tenant_id=%s ORDER BY part_ordinal LIMIT 2',(self.tenant,)).fetchall()
        consumed=2*sum(p['size_bytes']for p in parts)
        result=self.run_parent(runner_limits=runner.RunnerLimits(max_parents=1,max_archive_bytes=consumed+1),max_batches=8)
        self.assertEqual(result['cleared_documents'],2)
        self.assertEqual(result['archive_bytes'],consumed)
        self.assertEqual(result['status'],'archive_budget_limited')
        self.assertEqual(result['archive_gets'],4)
        self.assertEqual(result['errors'],{})
        self.assertEqual(result['failed_parents'],0)

if __name__=='__main__':unittest.main()
