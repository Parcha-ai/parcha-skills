#!/usr/bin/env python3
"""Actual policy, parent ledger and crash recovery for the existing worker."""
# ruff: noqa: E402
from pathlib import Path
import sys
import os
import threading
import subprocess
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_policy as policy_tests
from recall_server import retirement_maintenance as maintenance
from recall_server import retirement_policy as policy
from recall_server import retirement_runner as runner
from recall_server.retirement_supervisor import RetirementSupervisor
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

    def empty_source_first(self):
        from e2e_logical_evidence_projection import insert_source
        empty = self.source + ':empty'
        scope = runner.RetirementScope(self.tenant, self.scope.principal_id, (empty,))
        with self.store.connect() as c:
            insert_source(c, self.tenant, self.scope.principal_id, empty)
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
                      (self.tenant, self.scope.principal_id, empty))
        policy.start_policy(self.store, scope=scope)
        self.assertTrue(policy.capture_baseline(self.store, scope=scope)['complete'])
        policy.set_policy_enabled(self.store, scope=scope,
            reviewed_plan=policy.policy_plan(self.store, scope=scope), enabled=True)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01' WHERE tenant_id=%s AND source_id=%s", (self.tenant, empty))
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-02' WHERE tenant_id=%s AND source_id=%s", self.key[:2])
        return empty

    def test_empty_source_hands_remaining_round_to_live_source(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        before = self.state()
        self.archive.reads.clear()
        report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['cleared_documents'], 2, report)
        self.assertEqual(report['completed_parent_proofs'], 1, report)
        self.assertEqual(report['completed_noop_parents'], 0, report)
        self.assertFalse(report['commit_outcome_unknown'])
        self.assertGreater(sum(self.archive.reads.values()), 0)
        self.assertEqual(self.state()['cumulative_cleared_documents'],
                         before['cumulative_cleared_documents'] + 2)
        with self.store.connect() as c:
            self.assertEqual(c.execute("SELECT count(*) AS n FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s", (self.tenant, empty)).fetchone()['n'], 0)
            self.assertEqual(c.execute("SELECT count(*) AS n FROM canonical_retirement_source_policy WHERE tenant_id=%s AND maintenance_epoch=1 AND maintenance_intent IS NULL", (self.tenant,)).fetchone()['n'], 2)

    def source_epochs(self):
        with self.store.connect() as c:
            return {row['source_id']: row['maintenance_epoch'] for row in c.execute(
                'SELECT source_id,maintenance_epoch FROM canonical_retirement_source_policy WHERE tenant_id=%s',
                (self.tenant,)).fetchall()}

    def test_all_empty_sources_exit_once_even_if_schedule_is_rearmed(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        execute = self.store._execute_bounded

        def rearm_schedule(c, sql, args, deadline):
            result = execute(c, sql, args, deadline)
            if 'SET maintenance_intent=NULL' in sql:
                c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01' WHERE tenant_id=%s", (self.tenant,))
            return result

        self.archive.reads.clear()
        with patch.object(self.store, '_execute_bounded', side_effect=rearm_schedule):
            report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['status'], 'no_ready_parents', report)
        self.assertEqual(report['attempted_parents'], 0)
        self.assertEqual(report['cleared_documents'], 0)
        self.assertEqual(self.source_epochs(), {empty: 1, self.source: 1})
        self.assertFalse(self.archive.reads)

    def test_expired_remaining_budget_does_not_claim_next_source(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        clock = [time.monotonic()]
        start = clock[0]
        execute = self.store._execute_bounded

        def expire_after_empty(c, sql, args, deadline):
            result = execute(c, sql, args, deadline)
            if 'SET maintenance_intent=NULL' in sql:
                clock[0] = start + 5  # The original10s round reserves2.5s twice.
            return result

        owner = self.worker(seconds=10)
        with patch.object(maintenance, 'time', SimpleNamespace(monotonic=lambda: clock[0])), \
                patch.object(self.store, '_execute_bounded', side_effect=expire_after_empty):
            report = owner.cycle(busy=False)
        self.assertEqual(report['status'], 'no_ready_parents', report)
        self.assertEqual(owner.deadline, start + 10)
        self.assertEqual(self.source_epochs(), {empty: 1, self.source: 0})
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)

    def test_empty_source_claim_refusal_does_not_move_to_live_source(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        execute = self.store._execute_bounded

        def cancel_claim(c, sql, args, deadline):
            if sql.startswith('SELECT progress.source_id,progress.native_parent_id,'):
                return execute(c, 'SELECT pg_sleep(1)', (), time.monotonic()+0.03)
            return execute(c, sql, args, deadline)

        with patch.object(self.store, '_execute_bounded', side_effect=cancel_claim):
            report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['status'], 'deferred', report)
        self.assertEqual(report['claim_phase'], 'candidate_select')
        self.assertEqual(report['sqlstate'], '57014')
        self.assertEqual(self.source_epochs(), {empty: 1, self.source: 0})
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)

    def test_productive_first_source_keeps_original_round_boundary(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1969-01-01' WHERE tenant_id=%s AND source_id=%s", self.key[:2])
        report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['cleared_documents'], 2, report)
        self.assertEqual(report['completed_parent_proofs'], 1)
        self.assertEqual(self.source_epochs(), {empty: 0, self.source: 1})

    def test_later_publication_in_previously_empty_source_is_not_skipped(self):
        from e2e_logical_evidence_projection import insert_record
        from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        first = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(first['cleared_documents'], 2)
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=empty, parent='later-parent',
                          native='later-event', text='new native evidence', role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=empty,
                                       native_ids=['later-event'], reason='ingest')
        projected = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(projected['failed'], 0, projected)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01' WHERE tenant_id=%s AND source_id=%s", (self.tenant, empty))
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s", (self.tenant, empty))
        second = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(second['cleared_documents'], 1, second)
        self.assertEqual(second['completed_parent_proofs'], 1)
        self.assertEqual(self.source_epochs(), {empty: 2, self.source: 1})

    def test_next_source_rechecks_revoked_authority_before_retirement(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        execute = self.store._execute_bounded

        def revoke_after_empty(c, sql, args, deadline):
            result = execute(c, sql, args, deadline)
            if 'SET maintenance_intent=NULL' in sql and args[1] == empty:
                c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', self.key[:2])
            return result

        with patch.object(self.store, '_execute_bounded', side_effect=revoke_after_empty):
            report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['status'], 'owner_refused', report)
        self.assertEqual(self.source_epochs(), {empty: 1, self.source: 1})
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)
        self.assertFalse(report['commit_outcome_unknown'])

    def test_source_claim_refusal_does_not_attempt_any_parent(self):
        self.activate()
        self.ready()
        empty = self.empty_source_first()
        execute = self.store._execute_bounded

        def cancel_source(c, sql, args, deadline):
            if sql.startswith('SELECT * FROM canonical_retirement_source_policy'):
                return execute(c, 'SELECT pg_sleep(1)', (), time.monotonic()+0.03)
            return execute(c, sql, args, deadline)

        with patch.object(self.store, '_execute_bounded', side_effect=cancel_source):
            report = self.worker(seconds=10).cycle(busy=False)
        self.assertEqual(report['status'], 'deferred', report)
        self.assertEqual(report['maintenance_phase'], 'source_claim')
        self.assertEqual(report['sqlstate'], '57014')
        self.assertEqual(self.source_epochs(), {empty: 0, self.source: 0})
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)

    def test_first_parent_claim_sql_timeout_retains_safe_stage(self):
        self.activate()
        self.ready()
        worker = self.worker(seconds=10)
        original = self.store._execute_bounded
        timed_out = []

        def bounded(connection, sql, args, deadline):
            if sql.startswith('SELECT progress.source_id,progress.native_parent_id,'):
                timed_out.append(True)
                return original(connection, 'SELECT pg_sleep(1)', (), time.monotonic()+0.05)
            return original(connection, sql, args, deadline)

        with patch.object(self.store, '_execute_bounded', side_effect=bounded):
            result = worker.cycle(busy=False)
        self.assertEqual(timed_out, [True])
        self.assertEqual(result['status'], 'deferred')
        self.assertEqual(result['maintenance_phase'], 'parent_claim')
        self.assertEqual(result['claim_phase'], 'candidate_select')
        self.assertEqual(result['error_class'], 'SearchDeadlineExceeded')
        self.assertEqual(result['sqlstate'], '57014')
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_utf8_bytes'], 0)
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)
        self.assertIsNone(self.policy_state()['maintenance_intent'])

    def test_round_reserves_batch_commit_and_intent_finalization(self):
        self.activate()
        self.ready()
        worker = self.worker(seconds=180)
        # Exact budget arithmetic, while keeping native deadlines near the real clock.
        now = [float(int(time.monotonic()))]
        start = now[0]
        original_run = maintenance.run_retirement
        original_read = self.archive.read_raw
        def read_near_end(reference):
            value = original_read(reference)
            now[0] = start + 171
            return value
        def inspect_budget(*args, **kwargs):
            self.assertEqual(kwargs['deadline_at'], start + 175)
            self.assertFalse(kwargs['should_stop']())
            return original_run(*args, **kwargs)
        with patch.object(maintenance, 'time', SimpleNamespace(monotonic=lambda: now[0])), \
                patch.object(maintenance, 'run_retirement', side_effect=inspect_budget), \
                patch.object(self.archive, 'read_raw', side_effect=read_near_end):
            result = worker.cycle(busy=False)
        self.assertEqual(worker.deadline, start + 180)
        self.assertGreater(result['archive_bytes'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['status'], 'stopped')
        self.assertIsNone(self.policy_state()['maintenance_intent'])
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)

    def test_short_round_reserves_time_without_consuming_entire_budget(self):
        self.activate()
        self.ready()
        worker = self.worker(seconds=180)
        now = [time.monotonic()]
        start = now[0]
        original_run = maintenance.run_retirement
        def inspect_budget(*args, **kwargs):
            self.assertEqual(kwargs['deadline_at'], start + 3.75)
            self.assertFalse(kwargs['should_stop']())
            now[0] = start + 2.5
            self.assertTrue(kwargs['should_stop']())
            return original_run(*args, **kwargs)
        with patch.object(maintenance, 'time', SimpleNamespace(monotonic=lambda: now[0])), \
                patch.object(maintenance, 'run_retirement', side_effect=inspect_budget):
            result = worker.cycle(busy=True)
        self.assertEqual(worker.deadline, start + 5)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['status'], 'stopped')
        self.assertIsNone(self.policy_state()['maintenance_intent'])

    def test_catalog_budget_uses_exact_parts_and_preserves_proof(self):
        self.activate()
        self.ready()
        result = self.worker(catalog_archive_budget=True, seconds=60,
            max_archive_bytes=64*1024**3).cycle(busy=False)
        self.assertEqual(result['cleared_documents'], 2)
        self.assertEqual(result['failed_parents'], 0)
        self.assertTrue(self.state()['enabled'])

    def test_catalog_budget_refuses_replaced_parent_before_archive(self):
        self.activate()
        self.ready()
        original = runner._claim
        def claim_then_replace(*args, **kwargs):
            found = original(*args, **kwargs)
            if found is not None:
                self.replace()
            return found
        with patch.object(runner, '_claim', side_effect=claim_then_replace):
            result = self.worker(catalog_archive_budget=True, seconds=60,
                max_archive_bytes=64*1024**3).cycle(busy=False)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['archive_gets'], 0)
        self.assertFalse(self.state()['enabled'])

    def child(self, seconds=60, *, stop_after_archive_reads=0):
        root = Path(self.root.name)
        script = """import sys,time
from pathlib import Path
from recall_server import retirement_process as process, retirement_maintenance as maintenance
from types import SimpleNamespace
from recall_server.archive import FilesystemArchiveStore
root,objects=map(Path,sys.argv[1:3])
stop_after,budget=int(sys.argv[3]),float(sys.argv[4])
class PausedArchive:
    def __init__(self):
        self.archive=FilesystemArchiveStore(objects,namespace_key=b'r'*32)
        self.first=True
        self.reads=0
    def read_raw(self,reference):
        if self.first:
            self.first=False;(root/'proof-ready').touch()
            while not (root/'proof-release').exists():time.sleep(.005)
        value=self.archive.read_raw(reference)
        self.reads+=1
        if self.reads==stop_after:
            # Exhaust only the scheduler admission clock. The actual proof,
            # SQL deadlines, and archive reader remain unchanged.
            maintenance.time=SimpleNamespace(monotonic=lambda:time.monotonic()+budget)
        return value
process.build_evidence_archive_store=lambda **kw:PausedArchive()
process.main()
"""
        def launch(configuration):
            child = subprocess.Popen([sys.executable, '-c', script, str(root),
                str(self.archive.delegate.root), str(stop_after_archive_reads),
                str(seconds)], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env=dict(os.environ, PYTHONPATH=os.pathsep.join((
                    str(SERVER.parent), str(SERVER), os.environ.get('PYTHONPATH', '')))))
            return child
        supervisor = RetirementSupervisor(self.store.dsn, tenant_id=self.tenant,
            seconds=seconds, launch=launch)
        self.addCleanup(supervisor.close)
        self.assertEqual(supervisor.cycle(busy=True)['status'], 'running')
        deadline = time.monotonic()+15
        while not (root/'proof-ready').exists():
            self.assertIsNone(supervisor.child.poll(), 'retirement child exited before proof')
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        return supervisor

    def finish_child(self, supervisor):
        (Path(self.root.name)/'proof-release').touch()
        deadline = time.monotonic()+supervisor.seconds+10
        while supervisor.child.poll() is None:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        return supervisor.cycle(busy=True)

    def test_child_proof_holds_one_idle_connection_and_foreground_progresses(self):
        self.activate()
        self.ready()
        supervisor = self.child()
        with self.store.connect() as c:
            connections=c.execute("""SELECT state,xact_start FROM pg_stat_activity
                WHERE datname=current_database() AND application_name='recall-retirement'""").fetchall()
        self.assertEqual(len(connections),1)
        self.assertEqual(connections[0]['state'],'idle')
        self.assertIsNone(connections[0]['xact_start'])
        for _ in range(25):
            self.assertEqual(supervisor.cycle(busy=True)['status'],'running')
            with self.store.connect() as c:
                self.assertEqual(c.execute('SELECT 1 AS n').fetchone()['n'],1)
        report=self.finish_child(supervisor)
        self.assertEqual(report['cleared_documents'],2)
        self.assertGreater(report['peak_rss_kib'],0)

    def test_child_drains_more_than_ten_small_parents_in_one_round(self):
        self.activate()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        for number in range(12):
            self.publish(f'small-event-{number:02d}', f'small-parent-{number:02d}')
        self.ready()
        report = self.finish_child(self.child())
        self.assertFalse(report['commit_outcome_unknown'], report)
        self.assertEqual(report['failed_parents'], 0, report)
        self.assertEqual(report['attempted_parents'], 12, report)
        self.assertEqual(report['completed_parent_proofs'], 12, report)
        self.assertEqual(report['archive_gets'], 12, report)  # One original proof per small parent.
        self.assertEqual(report['cleared_documents'], 12, report)
        self.assertEqual(report['status'], 'no_ready_parents', report)
        self.assertLess(len(json.dumps(report)), 65536)
        for number in range(12):
            self.assertEqual(self.parent_state(f'small-parent-{number:02d}')['status'], 'complete')
        print(json.dumps(dict(measurement='small_parent_round', **report)), flush=True)

    def test_child_stops_admitting_small_parents_at_existing_budget(self):
        self.activate()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        for number in range(12):
            self.publish(f'budget-event-{number:02d}', f'budget-parent-{number:02d}')
        self.ready()
        report = self.finish_child(self.child(stop_after_archive_reads=11))
        self.assertFalse(report['commit_outcome_unknown'], report)
        self.assertEqual(report['status'], 'stopped', report)
        self.assertEqual(report['attempted_parents'], 11, report)
        self.assertEqual(report['cleared_documents'], 10, report)
        self.assertIsNone(self.policy_state()['maintenance_intent'])
        self.assertEqual(sum(self.parent_state(f'budget-parent-{number:02d}')['status']=='complete'
                             for number in range(12)),10)

    def test_child_owner_revocation_during_proof_cannot_clear(self):
        self.activate()
        self.ready()
        supervisor=self.child()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',self.key[:2])
        report=self.finish_child(supervisor)
        self.assertEqual(report['cleared_documents'],0)
        self.assertEqual(self.state()['cumulative_cleared_documents'],0)

    def test_child_catalog_replacement_during_proof_preserves_detached_hold(self):
        self.activate()
        self.ready()
        supervisor=self.child()
        self.replace()
        held=self.state()
        report=self.finish_child(supervisor)
        self.assertEqual(report['cleared_documents'],0)
        self.assertEqual(self.state(),held)
        self.assertFalse(held['enabled'])

    def test_killed_child_intent_is_reconciled_by_fresh_proof(self):
        self.activate()
        self.ready()
        supervisor=self.child()
        self.assertIsNotNone(self.policy_state()['maintenance_intent'])
        supervisor.child.kill()
        supervisor.child.wait(timeout=2)
        self.assertTrue(supervisor.cycle(busy=True)['commit_outcome_unknown'])
        self.ready()
        recovered=self.worker(catalog_archive_budget=True,seconds=60,
            max_archive_bytes=64*1024**3).cycle(busy=False)
        self.assertEqual(recovered['reconciled_intents'],1)
        self.assertEqual(recovered['cleared_documents'],2)

    def test_child_clears_parent_above_old_round_limit_with_one_proof(self):
        from e2e_logical_evidence_projection import insert_record
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty)
        self.activate()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        for ordinal in range(18):
            with self.store.connect() as c:
                insert_record(c, tenant=self.tenant, source=self.source,
                    parent='one-proof-parent', native=f'one-proof-{ordinal:04d}',
                    text=f'{ordinal:08d}'+'x'*(4*1024**2-8),
                    role='assistant', byte_start=ordinal*10)
        with self.store.connect() as c:
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                native_ids=['one-proof-0000'], reason='ingest')
        projector = CanonicalLogicalEvidenceProjector(self.store,
            LogicalEvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant,
            raw_archive=self.archive)
        published = projector.project_pending(tenant_id=self.tenant,
            batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(published['failed'], 0, published)
        self.ready()
        report = self.finish_child(self.child())
        self.assertFalse(report['commit_outcome_unknown'], report)
        self.assertEqual(report['failed_parents'], 0, report)
        self.assertEqual(report['cleared_utf8_bytes'], 72*1024**2)
        self.assertEqual(report['completed_parent_proofs'], 1)
        self.assertEqual(report['batches'], 9)  # Original eight-MiB transactions.
        self.assertLess(report['archive_bytes'], 74*1024**2)
        self.assertEqual(self.parent_state('one-proof-parent')['status'], 'complete')

    @unittest.skipUnless(os.environ.get('RECALL_LARGE_RETIREMENT_TEST') == '1',
                         'explicit 512MiB resource measurement')
    def test_512mib_parent_makes_bounded_progress_until_complete(self):
        from e2e_logical_evidence_projection import insert_record
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty)
        self.activate()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        started = time.monotonic()
        # Compressible synthetic SQL bodies; archive bytes and proof are real.
        # One record is generated at a time; no 512MiB Python collection.
        for ordinal in range(128):
            text = f'{ordinal:08d}' + 'x' * (4*1024**2-8)
            with self.store.connect() as c:
                insert_record(c, tenant=self.tenant, source=self.source,
                    parent='large-parent', native=f'large-{ordinal:04d}',
                    text=text, role='assistant', byte_start=ordinal*10)
        with self.store.connect() as c:
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                native_ids=['large-0000'], reason='ingest')
        projector = CanonicalLogicalEvidenceProjector(self.store,
            LogicalEvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant,
            raw_archive=self.archive)
        published = projector.project_pending(tenant_id=self.tenant,
            batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(published['failed'], 0, published)
        self.assertTrue(self.parent_state('large-parent')['enabled'])
        setup_seconds = time.monotonic()-started
        reports = []
        for round_number in range(9):
            self.ready()
            for name in ('proof-ready', 'proof-release'):
                (Path(self.root.name)/name).unlink(missing_ok=True)
            supervisor = self.child(seconds=180)
            # Actual foreground publication can commit while proof is outside SQL.
            if round_number == 0:
                foreground = self.publish('concurrent-small', 'concurrent-small-parent')
                self.assertEqual(foreground['failed'], 0, foreground)
            (Path(self.root.name)/'proof-release').touch()
            foreground_queries = 0
            foreground_latencies = []
            foreground_publication_seconds = None
            max_connections = 0
            deadline = time.monotonic()+190
            while supervisor.child.poll() is None:
                self.assertLess(time.monotonic(), deadline)
                foreground_started = time.monotonic()
                with self.store.connect() as c:
                    count = c.execute("""SELECT count(*) AS n FROM pg_stat_activity
                        WHERE datname=current_database()
                        AND application_name='recall-retirement'""").fetchone()['n']
                foreground_latencies.append((time.monotonic()-foreground_started)*1000)
                if round_number == 0 and foreground_queries == 0:
                    foreground_started = time.monotonic()
                    published = self.publish('during-proof', 'during-proof-parent')
                    self.assertEqual(published['failed'], 0, published)
                    foreground_publication_seconds = time.monotonic()-foreground_started
                max_connections = max(max_connections, count)
                foreground_queries += 1
                time.sleep(.05)
            report = supervisor.cycle(busy=True)
            print(json.dumps(dict(measurement='512mib_round', round=round_number, report=report)), flush=True)
            self.assertFalse(report['commit_outcome_unknown'], report)
            self.assertGreater(report['cleared_utf8_bytes'], 0, report)
            self.assertLessEqual(report['cleared_utf8_bytes'], 512*1024**2)
            self.assertGreater(report['archive_bytes'], 512*1024**2)
            self.assertLess(report['peak_rss_kib'], 512*1024)
            self.assertLessEqual(max_connections, 1)
            latency_p95 = sorted(foreground_latencies)[int((len(foreground_latencies)-1)*.95)]
            self.assertLess(latency_p95, 250)
            reports.append(dict(report, foreground_queries=foreground_queries,
                foreground_query_p95_ms=latency_p95,
                foreground_publication_seconds=foreground_publication_seconds,
                max_connections=max_connections))
            if self.parent_state('large-parent')['status'] == 'complete':
                break
        current = self.parent_state('large-parent')
        self.assertEqual(current['status'], 'complete', current)
        self.assertEqual(current['cumulative_cleared_utf8_bytes'], 512*1024**2)
        print(json.dumps(dict(measurement='512mib_retirement',
            setup_seconds=setup_seconds, rounds=reports)), flush=True)

    def test_no_policy_and_disabled_worker_do_no_archive_or_claim_work(self):
        before = self.state()
        self.assertEqual(self.worker().cycle(busy=False)['status'], 'no_ready_sources')
        self.activate()
        worker = maintenance.RetirementMaintenance(self.store, self.archive,
            tenant_id=self.tenant, enabled=False)
        self.assertEqual(worker.cycle(busy=False)['status'], 'disabled')
        self.assertEqual(self.state(), before)
        self.assertFalse(self.archive.reads)

    def test_missing_schema_is_inert_and_migration_replay_preserves_intent(self):
        self.activate()
        self.ready()
        self.crashed_candidate()
        before = self.policy_state()
        with self.store.connect() as c:
            c.execute((SERVER/'schema/078_retirement_maintenance.sql').read_text())
        self.assertEqual(self.policy_state(), before)
        with self.store.connect() as c:
            c.execute('DELETE FROM schema_migrations WHERE version=78')
        try:
            self.assertEqual(self.worker().cycle(busy=False)['status'], 'no_ready_sources')
            self.assertEqual(self.policy_state(), before)
        finally:
            with self.store.connect() as c:
                c.execute('INSERT INTO schema_migrations(version) VALUES(78)')
        self.assertFalse(self.archive.reads)

    def test_intent_transport_failure_stops_before_archive_and_next_round_recovers(self):
        self.activate()
        self.ready()
        worker = self.worker()
        with patch.object(worker, '_record_intent', side_effect=psycopg.OperationalError('synthetic')):
            first = worker.cycle(busy=False)
        self.assertEqual(first['status'], 'deferred')
        self.assertFalse(self.archive.reads)
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)
        self.ready()
        second = self.worker().cycle(busy=False)
        self.assertEqual(second['cleared_documents'], 2, second)

    def test_parent_above_archive_budget_remains_eligible_and_can_clear_with_sufficient_budget(self):
        self.activate()
        self.ready()
        first = self.worker(max_archive_bytes=1).cycle(busy=False)
        self.assertEqual(first['cleared_documents'], 0, first)
        self.assertEqual(first['failed_parents'], 1, first)
        self.assertFalse(self.archive.reads)
        self.assertTrue(self.state()['enabled'])
        self.assertEqual(self.state()['cumulative_cleared_documents'], 0)
        self.ready()
        second = self.worker().cycle(busy=False)
        self.assertEqual(second['cleared_documents'], 2, second)

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
        clear = runner.retire_parent_windows
        def lost_response(*args, **kwargs):
            clear(*args, **kwargs)
            raise ChunkRetirementError('parent_retirement_unavailable')
        with patch.object(runner, 'retire_parent_windows', side_effect=lost_response):
            first = self.worker().cycle(busy=False)
        self.assertTrue(first['commit_outcome_unknown'], first)
        intent = self.policy_state()['maintenance_intent']
        self.assertIsNotNone(intent)
        self.assertEqual(first['unknown_source_id'], self.key[1])
        self.assertEqual(first['unknown_native_parent_id'], intent['parent'])
        self.assertEqual(first['unknown_scope_epoch'], intent['scope_epoch'])
        self.assertNotIn('maintenance_epoch', first)
        self.assertNotIn('intent_acknowledged', first)
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
