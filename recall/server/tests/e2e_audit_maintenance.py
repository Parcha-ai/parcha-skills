#!/usr/bin/env python3
"""Existing retirement child drains only explicitly admitted success diagnostics."""
# ruff: noqa: E402
from contextlib import contextmanager
from pathlib import Path
import json
import os
import subprocess
import sys
import unittest
import time
from types import SimpleNamespace
from unittest.mock import patch


SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from e2e_retirement_maintenance import MaintenanceTests
from e2e_audit_retirement import AuditRetirementTests
from recall_server.audit_retirement import AuditRetirement
from recall_server import audit_maintenance, retirement_maintenance
from recall_server.archive import ArtifactReference
from recall_server.db import SearchDeadlineExceeded


class AuditMaintenanceTests(MaintenanceTests):
    add = AuditRetirementTests.add

    def admit(self):
        checkpoint = dict(version=1, cutoff='2026-10-13T00:00:00+00:00',
                          after=None, pending=None, exhausted=False, journal=None)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_retirement_source_policy SET audit_checkpoint=%s::jsonb '
                      'WHERE tenant_id=%s AND source_id=%s',
                      (json.dumps(checkpoint), self.tenant, self.source))
    def audit_ids(self):
        with self.store.connect() as c:
            return [r['audit_id'] for r in c.execute("SELECT audit_id FROM canonical_audit_events "
                "WHERE tenant_id=%s AND source_id=%s AND audit_id LIKE 'audit-maint:%%' ORDER BY audit_id",
                (self.tenant, self.source)).fetchall()]

    def test_admitted_audit_page_drains_while_body_retirement_progresses(self):
        self.activate()
        self.add('audit-maint:01')
        self.add('audit-maint:02', operation='forget.commit')
        self.add('audit-maint:03', status='failed')
        self.admit()
        self.ready()
        result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['cleared_documents'], 2, 'body work must still progress')
        self.assertNotIn('audit_error_class', result, result)
        with self.store.connect() as c:
            rows = c.execute("SELECT audit_id FROM canonical_audit_events WHERE tenant_id=%s "
                             "AND source_id=%s AND audit_id LIKE 'audit-maint:%%' ORDER BY audit_id",
                             (self.tenant, self.source)).fetchall()
        self.assertEqual([r['audit_id'] for r in rows], ['audit-maint:02', 'audit-maint:03'])
        self.assertEqual(result['audit_cleared_rows'], 1)
        self.assertIsNone(self.policy_state()['audit_checkpoint']['pending'])
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])

    def test_lost_clear_commit_is_observed_before_cursor_advance(self):
        self.activate()
        self.add('audit-maint:01')
        self.admit()
        self.ready()
        original = AuditRetirement.clear
        def lose_response(owner, receipt):
            original(owner, receipt)
            raise TimeoutError('synthetic lost response after actual COMMIT')
        with patch.object(AuditRetirement, 'clear', lose_response):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_status'], 'unknown')
        self.assertTrue(result['audit_commit_outcome_unknown'])
        self.assertEqual(result['audit_cleared_rows'], 0)
        saved = self.policy_state()['audit_checkpoint']
        self.assertIsNone(saved['after'])
        self.assertIsNotNone(saved['pending'])
        self.assertEqual(self.audit_ids(), [])
        journal = self.archive.read(ArtifactReference(**saved['pending']['journal']),
                                   tenant_id=self.tenant, source_id=self.source)
        self.assertEqual(json.loads(journal)['receipt'], saved['pending']['receipt'])
        self.ready()
        with patch.object(AuditRetirement, 'clear', side_effect=AssertionError('must not replay clear')):
            observed = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(observed['audit_reconciled_rows'], 1)
        self.assertEqual(observed['audit_cleared_rows'], 0)
        self.assertIsNone(self.policy_state()['audit_checkpoint']['pending'])
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])
        restored = AuditRetirement(self.store, self.archive).restore(saved['pending']['receipt'])
        self.assertEqual(restored, dict(status='restored', rows=1))
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])

    def test_disable_before_clear_preserves_pending_and_rows(self):
        self.activate()
        self.add('audit-maint:01')
        self.admit()
        self.ready()
        original = AuditRetirement.clear
        def disable(owner, receipt):
            with self.store.connect() as c:
                c.execute('UPDATE canonical_retirement_source_policy SET enabled=false,policy_epoch=policy_epoch+1 '
                          'WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
            return original(owner, receipt)
        with patch.object(AuditRetirement, 'clear', disable):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_cleared_rows'], 0)
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])
        self.assertIsNotNone(self.policy_state()['audit_checkpoint']['pending'])
        self.ready()
        disabled = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(disabled['audit_status'], 'disabled')

    def test_native_sql_uses_remaining_deadline(self):
        self.activate()
        self.add('audit-maint:01')
        original = self.store._execute_bounded
        def slow(connection, query, params, deadline_at):
            if 'SELECT audit_id FROM canonical_audit_events' in str(query):
                return original(connection, 'SELECT pg_sleep(1)', (), deadline_at)
            return original(connection, query, params, deadline_at)
        start = time.monotonic()
        owner = AuditRetirement(self.store, self.archive, deadline_at=start + .1)
        with patch.object(self.store, '_execute_bounded', slow):
            with self.assertRaises(SearchDeadlineExceeded):
                owner.archive_page(tenant_id=self.tenant, source_id=self.source,
                                   cutoff='2026-10-13T00:00:00+00:00', limit=5000)
        self.assertLess(time.monotonic() - start, .8)
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])

    def test_uncertain_journal_put_never_clears_without_readback(self):
        self.activate()
        self.add('audit-maint:01')
        self.admit()
        self.ready()
        put = AuditRetirement.put
        journals = []
        def lose_journal_response(owner, request):
            reference = put(owner, request)
            if request.media_type == 'application/json':
                journals.append(reference)
                raise TimeoutError('synthetic lost response after immutable journal PUT')
            return reference
        with patch.object(AuditRetirement, 'put', lose_journal_response), \
                patch.object(AuditRetirement, 'clear', side_effect=AssertionError('journal unacknowledged')):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_phase'], 'journal')
        self.assertFalse(result['audit_commit_outcome_unknown'])
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])
        self.assertIsNone(self.policy_state()['audit_checkpoint']['pending'])
        self.assertIsNone(self.policy_state()['audit_checkpoint']['after'])
        self.assertEqual(len(journals), 1)
        self.ready()
        result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_cleared_rows'], 1)
        self.assertEqual(ArtifactReference(**self.policy_state()['audit_checkpoint']['journal']), journals[0])

    def test_sparse_exception_page_advances_and_next_page_reaches_success(self):
        self.activate()
        self.admit()
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,created_at)
                SELECT %s,%s,'000-audit:'||lpad(n::text,5,'0'),'forget.commit','success','2026-10-12'
                FROM generate_series(1,5000)n""", (self.tenant,self.source))
        self.add('audit-maint:01')
        self.ready()
        result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_completed_pages'], 2)
        self.assertGreaterEqual(result['audit_scanned_rows'], 5001)
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])
        self.assertEqual(result['audit_cleared_rows'], 1)
        with self.store.connect() as c:
            count = c.execute("SELECT count(*) AS n FROM canonical_audit_events WHERE tenant_id=%s "
                              "AND source_id=%s AND audit_id LIKE '000-audit:%%'", self.key[:2]).fetchone()['n']
        self.assertEqual(count, 5000)

    def test_null_checkpoint_and_revoked_owner_never_archive_audits(self):
        self.activate()
        self.add('audit-maint:01')
        self.ready()
        with patch.object(AuditRetirement, 'archive_page', side_effect=AssertionError('not admitted')):
            self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])
        self.admit()
        self.ready()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        with patch.object(AuditRetirement, 'archive_page', side_effect=AssertionError('owner revoked')):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['status'], 'owner_refused')
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])

    def test_native_single_connection_child_runs_audit_and_body_owners(self):
        self.activate()
        self.add('audit-maint:01')
        self.admit()
        self.ready()
        script = """import sys
from contextlib import contextmanager
from pathlib import Path
from recall_server import retirement_process as process
from recall_server.archive import FilesystemArchiveStore
process.build_evidence_archive_store = lambda **kw: FilesystemArchiveStore(Path(sys.argv[1]), namespace_key=b'r'*32)
process.main()
"""
        completed = subprocess.run([sys.executable, '-c', script, str(self.archive.delegate.root)],
            input=json.dumps(dict(dsn=self.store.dsn, tenant_id=self.tenant, seconds=10)),
            capture_output=True, text=True, timeout=20,
            env=dict(os.environ, PYTHONPATH=os.pathsep.join((str(SERVER.parent), str(SERVER)))))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(completed.stdout)
        self.assertEqual(report['audit_cleared_rows'], 1, report)
        self.assertEqual(report['cleared_documents'], 2)
        self.assertFalse(report['audit_commit_outcome_unknown'])
        self.assertIsNone(self.policy_state()['audit_checkpoint']['pending'])

    def test_pages_share_one_time_slice_and_leave_body_budget(self):
        self.activate()
        self.admit()
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,created_at)
                SELECT %s,%s,'000-audit:'||lpad(n::text,5,'0'),'ingest.commit','success','2026-10-12'
                FROM generate_series(1,5001)n""", self.key[:2])
        self.ready()
        now = [time.monotonic()]
        started = now[0]
        original_archive = AuditRetirement.archive_page
        original_clear = AuditRetirement.clear
        original_body = retirement_maintenance.run_retirement
        deadlines = []
        def archive(owner, **kwargs):
            deadlines.append(owner.deadline_at)
            return original_archive(owner, **kwargs)
        def clear(owner, receipt):
            result = original_clear(owner, receipt)
            now[0] = started + 15.1
            return result
        def body(*args, **kwargs):
            self.assertEqual(kwargs['deadline_at'], started + 55)
            self.assertFalse(kwargs['should_stop']())
            return original_body(*args, **kwargs)
        clock = SimpleNamespace(monotonic=lambda: now[0])
        with patch.object(audit_maintenance, 'time', clock), \
                patch.object(retirement_maintenance, 'time', clock), \
                patch.object(AuditRetirement, 'archive_page', archive), \
                patch.object(AuditRetirement, 'clear', clear), \
                patch.object(retirement_maintenance, 'run_retirement', body):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(deadlines, [started + 15])
        self.assertEqual(result['audit_cleared_rows'], 5000)
        self.assertEqual(result['audit_status'], 'deferred')
        self.assertEqual(result['cleared_documents'], 2)
        self.assertIsNotNone(self.policy_state()['audit_checkpoint']['pending'])
        self.ready()
        next_round = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(next_round['audit_reconciled_rows'], 5000)
        self.assertEqual(next_round['audit_cleared_rows'], 1)
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])

    def test_lost_pending_commit_stops_before_clear_and_reconciles_next_round(self):
        self.activate()
        self.add('audit-maint:01')
        self.admit()
        self.ready()
        connect = self.store.connect
        execute = self.store._execute_bounded
        updates = []
        lost = []
        def track(connection, query, params, deadline_at):
            result = execute(connection, query, params, deadline_at)
            if 'SET audit_checkpoint=' in str(query):
                updates.append(True)
            return result
        @contextmanager
        def lose_after_commit(**kwargs):
            before = len(updates)
            with connect(**kwargs) as connection:
                yield connection
            if len(updates) > before and not lost:
                lost.append(True)
                raise TimeoutError('synthetic lost pending checkpoint COMMIT response')
        with patch.object(self.store, '_execute_bounded', track), \
                patch.object(self.store, 'connect', lose_after_commit), \
                patch.object(AuditRetirement, 'clear', side_effect=AssertionError('pending commit unknown')):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_phase'], 'intent')
        self.assertEqual(result['audit_completed_pages'], 0)
        self.assertIsNotNone(self.policy_state()['audit_checkpoint']['pending'])
        self.assertEqual(self.audit_ids(), ['audit-maint:01'])
        self.ready()
        result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_cleared_rows'], 1)
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])

    def test_oversized_checkpoint_refuses_before_archive(self):
        self.activate()
        self.admit()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET audit_checkpoint="
                      "jsonb_set(audit_checkpoint,'{after}',to_jsonb(%s::text)) "
                      "WHERE tenant_id=%s AND source_id=%s", ('x'*65536,*self.key[:2]))
        self.ready()
        with patch.object(AuditRetirement, 'archive_page', side_effect=AssertionError('oversized checkpoint')):
            result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_phase'], 'checkpoint')
        self.assertEqual(result['audit_status'], 'deferred')
        self.assertEqual(result['cleared_documents'], 2)

    def test_three_page_limit_keeps_exact_next_cursor(self):
        self.activate()
        self.admit()
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_audit_events
                (tenant_id,source_id,audit_id,operation,status,created_at)
                SELECT %s,%s,'000-audit:'||lpad(n::text,5,'0'),'ingest.commit','success','2026-10-12'
                FROM generate_series(1,15001)n""", self.key[:2])
        self.ready()
        result = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(result['audit_completed_pages'], 3)
        self.assertEqual(result['audit_cleared_rows'], 15000)
        self.assertEqual(result['audit_status'], 'page_budget')
        self.assertEqual(self.policy_state()['audit_checkpoint']['after'], '000-audit:15000')
        self.assertEqual(result['cleared_documents'], 2)
        self.ready()
        next_round = self.worker(seconds=60).cycle(busy=False)
        self.assertEqual(next_round['audit_cleared_rows'], 1)
        self.assertTrue(self.policy_state()['audit_checkpoint']['exhausted'])


if __name__ == '__main__':
    suite = unittest.TestSuite(AuditMaintenanceTests(name) for name in
        AuditMaintenanceTests.__dict__ if name.startswith('test_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
