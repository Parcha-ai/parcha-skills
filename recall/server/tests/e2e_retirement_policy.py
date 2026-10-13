#!/usr/bin/env python3
"""Real PostgreSQL: approved future admission preserves the frozen baseline."""
# ruff: noqa: E402
from pathlib import Path
import sys
import json
import time
import unittest
from unittest.mock import patch

import psycopg

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_detachment as detachment
from e2e_logical_evidence_projection import insert_record
from recall_server import retirement_policy as policy
from recall_server import retirement_runner as runner
from recall_server.chunk_retirement import ChunkRetirementError, set_parent_retirement_enabled
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty


class PolicyTests(unittest.TestCase):
    setUpClass = classmethod(detachment.DetachmentTests.setUpClass.__func__)
    tearDownClass = classmethod(detachment.DetachmentTests.tearDownClass.__func__)
    setUp = detachment.DetachmentTests.setUp
    state = detachment.DetachmentTests.state
    plan = detachment.DetachmentTests.plan
    replace = detachment.DetachmentTests.replace
    rebind = detachment.DetachmentTests.rebind

    def publish(self, native='future-event', parent='future-parent'):
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=self.source, parent=parent,
                          native=native, text='new future evidence', role='user', byte_start=100)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                                       native_ids=[native], reason='ingest')
        result = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(result['failed'], 0)
        return result

    def activate(self, **options):
        policy.start_policy(self.store, scope=self.scope, **options)
        while not policy.capture_baseline(self.store, scope=self.scope, limit=1)['complete']:
            pass
        policy.set_policy_enabled(self.store, scope=self.scope,
            reviewed_plan=policy.policy_plan(self.store, scope=self.scope), enabled=True)

    def parent_state(self, parent):
        with self.store.connect() as c:
            return c.execute("""SELECT * FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (*self.key[:2], parent)).fetchone()

    def repair(self, native):
        with self.store.connect() as c:
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                                       native_ids=[native], reason='ingest')
        report = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(report['failed'], 0)
        return report

    def test_no_policy_off_and_revoked_grant_do_not_block_publication(self):
        self.publish()
        self.assertIsNone(self.parent_state('future-parent'))
        self.activate()
        plan = policy.policy_plan(self.store, scope=self.scope)
        policy.set_policy_enabled(self.store, scope=self.scope, reviewed_plan=plan, enabled=False)
        self.publish('off-event', 'off-parent')
        self.assertIsNone(self.parent_state('off-parent'))
        policy.set_policy_enabled(self.store, scope=self.scope,
            reviewed_plan=policy.policy_plan(self.store, scope=self.scope), enabled=True)
        self.repair('off-event')
        self.assertTrue(self.parent_state('off-parent')['enabled'])
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        self.publish('revoked-event', 'revoked-parent')
        self.assertIsNone(self.parent_state('revoked-parent'))

    def test_partial_capture_cannot_activate_and_stale_review_refuses(self):
        before = policy.start_policy(self.store, scope=self.scope)
        with self.assertRaises(ChunkRetirementError):
            policy.set_policy_enabled(self.store, scope=self.scope, reviewed_plan=before, enabled=True)
        policy.capture_baseline(self.store, scope=self.scope)
        with self.assertRaises(ChunkRetirementError):
            policy.set_policy_enabled(self.store, scope=self.scope, reviewed_plan=before, enabled=True)
        self.assertEqual(policy.policy_plan(self.store, scope=self.scope)['enabled'], False)

    def test_held_rows_survive_capture_and_recreated_catalog(self):
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        before = self.state()
        self.activate()
        self.assertEqual(self.state(), before)
        self.replace()
        self.repair('event-0000')
        self.assertEqual(self.state()['status'], 'disabled')
        self.assertFalse(self.state()['enabled'])

    def test_locked_policy_defers_enrollment_but_publication_commits_then_repair_enrolls(self):
        self.activate()
        # Lock just policy metadata. The publication savepoint must release its
        # refusal without losing the new catalog or completed projection queue.
        with psycopg.connect(self.store.dsn) as c, c.transaction():
            c.execute('SELECT 1 FROM canonical_retirement_source_policy WHERE tenant_id=%s AND source_id=%s FOR UPDATE', self.key[:2])
            self.publish()
        self.assertIsNone(self.parent_state('future-parent'))
        self.repair('future-event')
        self.assertTrue(self.parent_state('future-parent')['enabled'])

    def test_pre_capture_transaction_committing_late_gets_permanent_baseline(self):
        with self.store.connect() as c, c.transaction():
            insert_record(c, tenant=self.tenant, source=self.source, parent='late-old-parent',
                          native='late-old-event', text='pre capture', role='user', byte_start=110)
            self.activate()
        self.repair('late-old-event')
        self.assertEqual(self.parent_state('late-old-parent')['status'], 'baseline')
        self.assertFalse(self.parent_state('late-old-parent')['enabled'])

    def test_explicit_baseline_binding_requires_review_and_never_overwrites_disable(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        self.activate()
        epoch = self.state()['scope_epoch']
        plan = self.plan()
        with self.assertRaises(ChunkRetirementError):
            runner.enroll_baseline_parent(self.store, scope=self.scope,
                reviewed_plan=plan, expected_scope_epoch=epoch + 1)
        runner.enroll_baseline_parent(self.store, scope=self.scope,
            reviewed_plan=plan, expected_scope_epoch=epoch)
        self.assertTrue(self.state()['enabled'])
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        with self.assertRaises(ChunkRetirementError):
            runner.enroll_baseline_parent(self.store, scope=self.scope,
                reviewed_plan=plan, expected_scope_epoch=self.state()['scope_epoch'])
        self.assertFalse(self.state()['enabled'])

    def test_parent_admitted_during_capture_is_baseline_before_seal(self):
        policy.start_policy(self.store, scope=self.scope)
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=self.source, parent='during-capture',
                          native='during-event', text='before seal', role='user', byte_start=110)
        self.activate()
        self.repair('during-event')
        self.assertEqual(self.parent_state('during-capture')['status'], 'baseline')

    def test_detached_rebind_requires_explicit_policy_consent_and_preserves_disabled(self):
        self.activate(allow_detached_rebind=True)
        self.replace()
        detached_epoch = self.state()['scope_epoch']
        self.repair('event-0000')
        self.assertTrue(self.state()['enabled'])
        self.assertEqual(self.state()['scope_epoch'], detached_epoch + 1)
        self.assertEqual(self.state()['last_record_ordinal'], -1)
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        self.replace()
        self.repair('event-0000')
        self.assertEqual(self.state()['status'], 'disabled')
        self.assertFalse(self.state()['enabled'])

    def test_future_only_policy_does_not_rebind_detached_old_consent(self):
        self.activate()
        self.replace()
        self.repair('event-0000')
        self.assertEqual(self.state()['status'], 'detached')
        self.assertFalse(self.state()['enabled'])

    def test_large_baseline_uses_indexed_successor_and_exact_membership(self):
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_events
                SELECT (jsonb_populate_record(NULL::canonical_events,
                    to_jsonb(template) || jsonb_build_object(
                        'event_id','evt_baseline_' || n,
                        'native_id','baseline-native-' || n,
                        'native_parent_id','bulk-' || lpad((n %% 12000)::text,5,'0'),
                        'canonical_redacted','{}'::jsonb))).*
                FROM (SELECT * FROM canonical_events WHERE tenant_id=%s AND source_id=%s LIMIT 1) template
                CROSS JOIN generate_series(1,24000) n""", self.key[:2])
            c.execute('ANALYZE canonical_events')
            explained = c.execute('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ' + policy._BASELINE_PAGE,
                policy._baseline_values(self.key[:2], None, 500)).fetchone()['QUERY PLAN'][0]
        def nodes(value):
            yield value
            for child in value.get('Plans', []):
                yield from nodes(child)
        event_access = [node for node in nodes(explained['Plan']) if node.get('Relation Name') == 'canonical_events']
        self.assertTrue(event_access)
        self.assertTrue(all('Index' in node['Node Type'] for node in event_access), event_access)
        policy.start_policy(self.store, scope=self.scope)
        started = time.monotonic()
        pages = 0
        while True:
            page = policy.capture_baseline(self.store, scope=self.scope, limit=500)
            pages += 1
            if page['complete']:
                break
        self.assertEqual(page['captured_parents'], 12001)
        self.assertEqual(pages, 25)
        with self.store.connect() as c:
            c.execute('ANALYZE canonical_chunk_retirement_progress')
            membership = c.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id='bulk-06000'", self.key[:2]).fetchone()['QUERY PLAN'][0]
        self.assertIn('Index', membership['Plan']['Node Type'])
        self.assertEqual(membership['Plan']['Actual Rows'], 1)
        print(json.dumps(dict(policy_baseline_parents=12001, capture_pages=pages,
            capture_seconds=round(time.monotonic()-started, 3),
            indexed_page_ms=explained['Execution Time'], membership_ms=membership['Execution Time'])))

    def test_capture_includes_catalog_only_parent_after_events_removed(self):
        with self.store.connect() as c:
            for table in ('canonical_chunks', 'canonical_documents', 'canonical_events', 'canonical_chunk_retirement_progress'):
                c.execute('DELETE FROM ' + table + ' WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        self.activate()
        self.assertIsNotNone(self.state())
        self.assertEqual(self.state()['status'], 'baseline')

    def test_policy_timeout_does_not_leak_into_publication_transaction(self):
        self.activate()
        with self.store.connect() as c, c.transaction():
            c.execute("SET LOCAL statement_timeout='1s'")
            self.assertEqual(policy.enroll_published_parent(self.store, c, self.key), 'represented')
            c.execute('SELECT pg_sleep(0.2)')
            self.assertEqual(c.execute("SHOW statement_timeout").fetchone()['statement_timeout'], '1s')

    def test_catalog_and_enrollment_roll_back_together(self):
        self.activate()
        original = policy.enroll_published_parent
        def stop_after_enrollment(*args):
            self.assertEqual(original(*args), 'enrolled')
            raise RuntimeError('synthetic publication rollback')
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=self.source, parent='rollback-parent',
                          native='rollback-event', text='retryable source body', role='user', byte_start=120)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                                       native_ids=['rollback-event'], reason='ingest')
        with patch.object(policy, 'enroll_published_parent', side_effect=stop_after_enrollment):
            with self.assertRaisesRegex(RuntimeError, 'synthetic publication rollback'):
                self.projector.project_pending(tenant_id=self.tenant,
                    batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertIsNone(self.parent_state('rollback-parent'))
        with self.store.connect() as c:
            self.assertIsNone(c.execute("SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id='rollback-parent'", self.key[:2]).fetchone())
        self.repair('rollback-event')
        self.assertTrue(self.parent_state('rollback-parent')['enabled'])

    def test_policy_migration_requires_detachment_and_replays_without_reset(self):
        self.activate()
        before = policy.policy_plan(self.store, scope=self.scope)
        migration = (SERVER / 'schema/076_retirement_source_policy.sql').read_text()
        with self.store.connect() as c:
            c.execute(migration)
        self.assertEqual(policy.policy_plan(self.store, scope=self.scope), before)
        with self.store.connect() as c:
            c.execute('DELETE FROM schema_migrations WHERE version=75')
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                with self.store.connect() as c:
                    c.execute(migration)
            with self.assertRaises(ChunkRetirementError):
                policy.start_policy(self.store, scope=self.scope)
        finally:
            with self.store.connect() as c:
                c.execute('INSERT INTO schema_migrations(version) VALUES(75)')
        self.assertEqual(policy.policy_plan(self.store, scope=self.scope), before)

    def test_missing_policy_marker_keeps_publication_running_and_mutation_refuses(self):
        self.activate()
        with self.store.connect() as c:
            c.execute('DELETE FROM schema_migrations WHERE version=76')
        try:
            self.publish()
            self.assertIsNone(self.parent_state('future-parent'))
            with self.assertRaises(ChunkRetirementError):
                policy.start_policy(self.store, scope=self.scope)
        finally:
            with self.store.connect() as c:
                c.execute('INSERT INTO schema_migrations(version) VALUES(76)')

    def test_capture_is_resumable_and_future_publication_enrolls_once(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s', self.key[:2])
        policy.start_policy(self.store, scope=self.scope)
        first = policy.capture_baseline(self.store, scope=self.scope, limit=1)
        self.assertFalse(first['complete'])
        self.assertEqual(self.state()['status'], 'baseline')
        self.assertFalse(self.state()['enabled'])
        second = policy.capture_baseline(self.store, scope=self.scope, limit=1)
        self.assertTrue(second['complete'])
        plan = policy.policy_plan(self.store, scope=self.scope)
        policy.set_policy_enabled(self.store, scope=self.scope, reviewed_plan=plan, enabled=True)
        self.publish()
        with self.store.connect() as c:
            row = c.execute('''SELECT * FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id='future-parent' ''', self.key[:2]).fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(row['enabled'])
        self.assertEqual(row['status'], 'pending')
        self.assertIsNotNone(row['logical_document_id'])
        self.assertEqual(self.state()['status'], 'baseline')


if __name__ == '__main__':
    unittest.main()
