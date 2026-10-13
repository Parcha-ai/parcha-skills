#!/usr/bin/env python3
"""Actual policy, parent ledger and crash recovery for the existing worker."""
# ruff: noqa: E402
from pathlib import Path
import sys
import os
import threading
import time
import unittest
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_policy as policy_tests
from recall_server import retirement_maintenance as maintenance
from recall_server import retirement_policy as policy
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import ChunkRetirementError, set_parent_retirement_enabled, retire_parent_chunks
from recall_server.parent_chunk_proof import parent_retirement_plan


class MaintenanceTests(unittest.TestCase):
    setUpClass = classmethod(policy_tests.PolicyTests.setUpClass.__func__)
    tearDownClass = classmethod(policy_tests.PolicyTests.tearDownClass.__func__)
    def setUp(self):
        policy_tests.PolicyTests.setUp(self)
        environment = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        environment.start()
        self.addCleanup(environment.stop)

    state = policy_tests.PolicyTests.state
    plan = policy_tests.PolicyTests.plan
    publish = policy_tests.PolicyTests.publish
    activate = policy_tests.PolicyTests.activate
    parent_state = policy_tests.PolicyTests.parent_state
    repair = policy_tests.PolicyTests.repair
    replace = policy_tests.PolicyTests.replace

    def worker(self, **kwargs):
        return maintenance.RetirementMaintenance(self.store, self.archive,
            tenant_id=self.tenant, enabled=True, **kwargs)

    def ready(self):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s", self.key[:2])
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01 00:00:00+00' WHERE tenant_id=%s AND source_id=%s", self.key[:2])

    def policy_state(self):
        with self.store.connect() as c:
            return c.execute('SELECT * FROM canonical_retirement_source_policy WHERE tenant_id=%s AND source_id=%s', self.key[:2]).fetchone()

    def test_no_policy_and_disabled_worker_do_no_archive_or_claim_work(self):
        before = self.state()
        self.assertEqual(self.worker().cycle(busy=False)['status'], 'no_ready_sources')
        self.activate()
        worker = maintenance.RetirementMaintenance(self.store, self.archive,
            tenant_id=self.tenant, enabled=False)
        self.assertEqual(worker.cycle(busy=False)['status'], 'disabled')
        self.assertEqual(self.state(), before)
        self.assertFalse(self.archive.reads)

    def test_disabled_policy_keeps_finite_consent_but_does_not_enroll_future(self):
        self.activate()
        policy.set_policy_enabled(self.store, scope=self.scope,
            reviewed_plan=policy.policy_plan(self.store, scope=self.scope), enabled=False)
        self.publish()
        self.ready()
        report = self.worker().cycle(busy=False)
        self.assertEqual(report['cleared_documents'], 2, report)
        self.assertIsNone(self.parent_state('future-parent'))
        self.assertIsNone(self.policy_state()['maintenance_intent'])

    def test_reconciliation_enrolls_a_missed_publication_and_preserves_holds(self):
        self.activate()
        with self.store.connect() as c, c.transaction():
            c.execute('SELECT 1 FROM canonical_retirement_source_policy WHERE tenant_id=%s AND source_id=%s FOR UPDATE', self.key[:2])
            self.publish()
        self.assertIsNone(self.parent_state('future-parent'))
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        report = self.worker().cycle(busy=True)
        self.assertEqual(report['enrolled_parents'], 1)
        self.assertTrue(self.parent_state('future-parent')['enabled'])
        self.assertFalse(self.state()['enabled'])
        self.assertEqual(self.state()['status'], 'disabled')
        self.assertEqual(report['cleared_documents'], 0)  # New consent retains cooldown.

    def test_revoked_owner_advances_scheduling_without_reads_or_clears(self):
        self.activate()
        self.ready()
        before = self.state()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        report = self.worker().cycle(busy=False)
        self.assertEqual(report['status'], 'owner_refused')
        self.assertEqual(self.state(), before)
        self.assertFalse(self.archive.reads)
        self.assertEqual(self.worker().cycle(busy=False)['status'], 'no_ready_sources')

    def test_crash_after_durable_intent_before_proof_resumes_without_approval(self):
        self.activate()
        self.ready()
        worker = self.worker()
        save = worker._record_intent
        def crash(*args, **kwargs):
            save(*args, **kwargs)
            raise SystemExit('synthetic worker crash')
        with patch.object(worker, '_record_intent', side_effect=crash):
            with self.assertRaises(SystemExit):
                worker.cycle(busy=False)
        self.assertIsNotNone(self.policy_state()['maintenance_intent'])
        self.assertFalse(self.archive.reads)
        self.ready()
        report = self.worker().cycle(busy=False)
        self.assertEqual(report['reconciled_intents'], 1)
        self.assertEqual(report['cleared_documents'], 2, report)
        self.assertIsNone(self.policy_state()['maintenance_intent'])

    def test_committed_but_lost_response_is_observed_then_safe_next_round(self):
        self.activate()
        self.ready()
        clear = runner.retire_parent_chunks
        def lost_response(*args, **kwargs):
            clear(*args, **kwargs)
            raise ChunkRetirementError('parent_retirement_unavailable')
        with patch.object(runner, 'retire_parent_chunks', side_effect=lost_response):
            first = self.worker().cycle(busy=False)
        self.assertTrue(first['commit_outcome_unknown'], first)
        self.assertIsNotNone(self.policy_state()['maintenance_intent'])
        self.assertEqual(self.state()['cumulative_cleared_documents'], 2)
        self.ready()
        self.archive.reads.clear()
        second = self.worker().cycle(busy=False)
        self.assertEqual(second['reconciled_intents'], 1)
        self.assertEqual(second['observed_previous_cleared_documents'], 2)
        self.assertEqual(second['cleared_documents'], 0)
        self.assertFalse(self.archive.reads)
        self.assertIsNone(self.policy_state()['maintenance_intent'])

    def crashed_candidate(self):
        worker = self.worker()
        saved = worker._record_intent
        captured = []
        def crash(source, candidate):
            saved(source, candidate)
            captured.append(candidate)
            raise SystemExit('crash before parent proof')
        with patch.object(worker, '_record_intent', side_effect=crash):
            with self.assertRaises(SystemExit):
                worker.cycle(busy=False)
        return captured[0]

    def old_clear(self, candidate):
        return retire_parent_chunks(self.store, self.archive, **self.target, apply=True,
            reviewed_plan=parent_retirement_plan(self.key, candidate['manifest']),
            required_scope_epoch=candidate['scope_epoch'], owner_principal_id=self.scope.principal_id,
            deadline_at=time.monotonic()+10)

    def test_resumed_claim_fences_old_proof_before_it_can_clear(self):
        self.activate()
        self.ready()
        candidate = self.crashed_candidate()
        self.ready()
        resumed = self.worker().cycle(busy=False)
        self.assertEqual(resumed['cleared_documents'], 2, resumed)
        after = self.state()
        with self.assertRaises(ChunkRetirementError):
            self.old_clear(candidate)
        self.assertEqual(self.state(), after)

    def test_old_clear_holding_commit_lock_defers_recovery_until_observable(self):
        self.activate()
        self.ready()
        candidate = self.crashed_candidate()
        self.ready()
        locked, release = threading.Event(), threading.Event()
        failures = []
        execute = self.store._execute_bounded
        def observe(c, sql, values, deadline):
            result = execute(c, sql, values, deadline)
            if (threading.current_thread().name == 'old-clear'
                    and 'cumulative_cleared_documents=cumulative_cleared_documents+' in sql):
                locked.set()
                if not release.wait(5):
                    raise AssertionError('test did not release old commit')
            return result
        def old():
            try:
                self.old_clear(candidate)
            except BaseException as error:
                failures.append(error)
        with patch.object(self.store, '_execute_bounded', side_effect=observe):
            thread = threading.Thread(target=old, name='old-clear')
            thread.start()
            try:
                self.assertTrue(locked.wait(5))
                report = self.worker().cycle(busy=False)
                self.assertEqual(report['status'], 'deferred', report)
                self.assertIsNotNone(self.policy_state()['maintenance_intent'])
            finally:
                release.set()
                thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertFalse(failures, failures)
        self.ready()
        report = self.worker().cycle(busy=False)
        self.assertEqual(report['reconciled_intents'], 1)
        self.assertEqual(report['observed_previous_cleared_documents'], 2)
        self.assertEqual(report['cleared_documents'], 0)

    def test_recovery_observes_recreated_catalog_and_keeps_disabled_intent(self):
        self.activate()
        self.ready()
        self.crashed_candidate()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        self.replace()
        self.ready()
        report = self.worker().cycle(busy=False)
        self.assertEqual(report['reconciled_intents'], 1)
        self.assertEqual(report['reconciled_state'], 'replaced')
        self.assertEqual(report['cleared_documents'], 0)
        self.assertFalse(self.archive.reads)
        self.assertEqual(self.state()['status'], 'disabled')

    def test_cursor_survives_restart_and_advances_past_deferred_parent(self):
        self.activate()
        self.publish('first-future-event', 'a-future-parent')
        self.publish('second-future-event', 'b-future-parent')
        with self.store.connect() as c:
            c.execute("DELETE FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id IN ('a-future-parent','b-future-parent')", self.key[:2])
        worker = self.worker(reconcile_parents=1)
        with patch.object(maintenance, 'enroll_published_parent', return_value='deferred'):
            first = worker.cycle(busy=True)
        self.assertEqual(first['enrolled_parents'], 0)
        self.assertEqual(self.policy_state()['maintenance_cursor'], 'a-future-parent')
        self.ready()
        second = self.worker(reconcile_parents=1).cycle(busy=True)
        self.assertEqual(second['enrolled_parents'], 1)
        self.assertIsNone(self.parent_state('a-future-parent'))
        self.assertTrue(self.parent_state('b-future-parent')['enabled'])

    def test_sources_advance_fairly_even_when_first_has_revoked_owner(self):
        self.activate()
        other = policy_tests.PolicyTests()
        other.store = self.store
        other.setUp()
        self.addCleanup(other.doCleanups)
        other.activate()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at=now()")
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01' WHERE tenant_id=%s AND source_id=%s", self.key[:2])
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-02' WHERE tenant_id=%s AND source_id=%s", other.key[:2])
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        worker = maintenance.RetirementMaintenance(self.store, self.archive, enabled=True)
        self.assertEqual(worker.cycle(busy=True)['status'], 'owner_refused')
        with patch.object(maintenance, 'run_retirement', return_value=dict(status='no_ready_parents', commit_outcome_unknown=False)) as run:
            worker.cycle(busy=True)
        self.assertEqual(run.call_args.kwargs['scope'], other.scope)


if __name__ == '__main__':
    unittest.main()
