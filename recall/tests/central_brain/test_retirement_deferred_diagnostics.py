"""Deferred native cycles retain safe failure stages without changing outcomes."""
import json
import unittest
from unittest.mock import Mock, patch

import psycopg
from central_brain.test_retirement_claim_diagnostics import ClaimStore, PRIVATE, runner
from recall_server import retirement_maintenance as maintenance


class DeferredDiagnostics(unittest.TestCase):
    def test_expired_before_first_claim_preserves_zero_report_without_dispatch(self):
        owner = maintenance.RetirementMaintenance(Mock(), Mock(), enabled=True, seconds=10)
        original_zero = maintenance.RetirementMaintenance(Mock(), Mock(), enabled=False).cycle(busy=False)
        with (
            patch.object(maintenance.time, 'monotonic', side_effect=[100.0, 111.0]),
            patch.object(owner, '_claim_source') as claim,
            patch.object(maintenance, 'run_retirement') as retire,
        ):
            result = owner.cycle(busy=False)
        self.assertEqual(result, dict(original_zero, status='stopped'))
        self.assertEqual(owner.deadline, 110.0)
        claim.assert_not_called()
        retire.assert_not_called()
        owner.store.connect.assert_not_called()

    def test_first_parent_claim_keeps_phase_and_original_deferred_outcome(self):
        error = psycopg.errors.LockNotAvailable(PRIVATE)
        store = ClaimStore({'candidate_select': error})
        owner = maintenance.RetirementMaintenance(store, Mock(), enabled=True)
        source = dict(tenant_id='tenant', principal_id='principal', source_id='source', maintenance_epoch=1)
        with (
            patch.object(owner, '_claim_source', return_value=source),
            patch.object(owner, '_reconcile_intent', return_value={}),
            patch.object(owner, '_reconcile_publications', return_value=0),
            patch.object(maintenance, '_authorize'),
            patch.object(runner, '_authorize', side_effect=lambda *a: store.step('authorize')),
            patch.object(runner, 'retire_parent_windows') as mutation,
        ):
            result = owner.cycle(busy=False)
        self.assertEqual(result['status'], 'deferred')
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_utf8_bytes'], 0)
        self.assertEqual(result['maintenance_phase'], 'parent_claim')
        self.assertEqual(result['claim_phase'], 'candidate_select')
        self.assertEqual(result['error_class'], 'LockNotAvailable')
        self.assertEqual(result['sqlstate'], '55P03')
        self.assertNotIn(PRIVATE, json.dumps(result))
        self.assertIs(store.exits[-1][1], error)
        mutation.assert_not_called()


    def test_owner_stages_and_wrapped_sqlstate_remain_content_free(self):
        from contextlib import ExitStack
        from recall_server.db import SearchDeadlineExceeded
        from recall_server.chunk_retirement import ChunkRetirementError
        source = dict(tenant_id='tenant', principal_id='principal', source_id='source', maintenance_epoch=1)
        for stage in ('source_claim', 'source_authorize', 'intent_reconcile',
                      'publication_enroll', 'intent_finalize'):
            for error in (psycopg.errors.QueryCanceled(PRIVATE),
                          SearchDeadlineExceeded(PRIVATE),
                          ChunkRetirementError('retirement_owner_required')):
                with self.subTest(stage=stage, error=type(error).__name__):
                    if isinstance(error, SearchDeadlineExceeded):
                        error.__cause__ = psycopg.errors.QueryCanceled(PRIVATE)
                    owner = maintenance.RetirementMaintenance(ClaimStore(), Mock(), enabled=True)
                    with ExitStack() as stack:
                        mocks = {
                            'source_claim': stack.enter_context(patch.object(owner, '_claim_source', return_value=source)),
                            'source_authorize': stack.enter_context(patch.object(maintenance, '_authorize')),
                            'intent_reconcile': stack.enter_context(patch.object(owner, '_reconcile_intent', return_value={})),
                            'publication_enroll': stack.enter_context(patch.object(owner, '_reconcile_publications', return_value=0)),
                            'intent_finalize': stack.enter_context(patch.object(owner, '_execute')),
                        }
                        mocks[stage].side_effect = error
                        run = stack.enter_context(patch.object(maintenance, 'run_retirement', return_value=dict(
                            status='bounded', cleared_utf8_bytes=23, cleared_documents=1,
                            commit_outcome_unknown=False)))
                        result = owner.cycle(busy=False)
                    self.assertEqual(result['maintenance_phase'], stage)
                    self.assertIsNone(result['claim_phase'])
                    self.assertEqual(result['status'], 'owner_refused' if isinstance(error, ChunkRetirementError) else 'deferred')
                    self.assertEqual(result['sqlstate'], None if isinstance(error, ChunkRetirementError) else '57014')
                    self.assertEqual(result['cleared_utf8_bytes'], 23 if stage=='intent_finalize' else 0)
                    self.assertFalse(result['commit_outcome_unknown'])
                    self.assertNotIn(PRIVATE, json.dumps(result))
                    self.assertEqual(run.call_count, int(stage=='intent_finalize'))

    def test_first_claim_diagnostics_do_not_replace_exception(self):
        error = psycopg.errors.LockNotAvailable(PRIVATE)
        for phase in ('connect', 'connection_enter', 'authorize', 'candidate_select',
                      'epoch_update', 'transaction_exit', 'connection_exit'):
            with self.subTest(phase=phase):
                store = ClaimStore({phase: error}); details = {}
                with patch.object(runner, '_authorize', side_effect=lambda *a: store.step('authorize')):
                    with self.assertRaises(psycopg.errors.LockNotAvailable) as caught:
                        runner.run_retirement(store, Mock(), scope=runner.RetirementScope(
                            'tenant','principal',('source',)), apply=True, failure_diagnostic=details)
                self.assertIs(caught.exception, error)
                self.assertEqual(details, {'phase':phase})

    def test_unknown_error_type_or_sqlstate_is_not_emitted(self):
        class PrivateType(psycopg.Error):
            sqlstate = PRIVATE
        owner = maintenance.RetirementMaintenance(ClaimStore(), Mock(), enabled=True)
        with patch.object(owner, '_claim_source', side_effect=PrivateType(PRIVATE)):
            result=owner.cycle(busy=False)
        self.assertEqual(result['error_class'], 'other')
        self.assertIsNone(result['sqlstate'])
        self.assertNotIn(PRIVATE, json.dumps(result))


    def test_deferred_diagnostics_reach_existing_supervisor_log(self):
        from io import BytesIO
        from recall_server.retirement_supervisor import RetirementSupervisor
        report = dict(status='deferred', cleared_documents=0, cleared_utf8_bytes=0,
            commit_outcome_unknown=False, maintenance_phase='parent_claim',
            claim_phase='connection_enter', error_class='PoolTimeout', sqlstate=None)
        owner = RetirementSupervisor('private-dsn', launch=Mock())
        owner.child = Mock(returncode=0, stdout=BytesIO(json.dumps(report).encode()))
        owner.child.poll.return_value = 0
        with self.assertLogs('recall_server.retirement_supervisor', level='INFO') as logs:
            self.assertEqual(owner.cycle(), report)
        for field in ('maintenance_phase','claim_phase','error_class','sqlstate'):
            self.assertIn(f'{field}={report[field]}', logs.output[0])
        self.assertNotIn('private-dsn', logs.output[0])
        owner.launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
