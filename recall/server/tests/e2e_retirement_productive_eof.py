"""Productive preparation before large-parent archive retirement attempts."""
import json
import os
import uuid
import time
import unittest
from dataclasses import replace
from unittest.mock import patch
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import e2e_thinning_retirement_transition as retained
from recall_server import retirement_runner as runner, parent_chunk_window as window
from recall_server.db import BrainStore, SearchDeadlineExceeded

from recall_server import chunk_retirement as retirement
from recall_server.canonical_thinning import _thin_selected
from recall_server.parent_chunk_proof import read_parent_catalog
from recall_server.receipt_membership import lookup_receipt_metadata


class ProductiveEOF(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Keep synthetic parent rows and statistics out of later native fixtures.
        admin_dsn = os.environ['RECALL_DATABASE_URL']
        database = 'productive_retirement_' + uuid.uuid4().hex
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        def drop_database():
            with psycopg.connect(admin_dsn, autocommit=True) as admin:
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))
        cls.addClassCleanup(drop_database)
        cls.store = BrainStore(make_conninfo(admin_dsn, dbname=database))
        cls.addClassCleanup(cls.store.close)
        cls.store.migrate()

    setUp = retained.ThinningRetirement.setUp
    ledger = retained.ThinningRetirement.ledger

    def state(self):
        with self.store.connect() as c:
            docs = c.execute('''SELECT d.document_id,d.native_id,d.revision,d.text_sha256,
                d.body_location,d.chunk_receipt_profile,d.body_record_ordinal,d.body_record_count,
                octet_length(d.text_redacted) AS inline_bytes,
                count(ch.chunk_id) AS physical_chunks,
                coalesce(sum(octet_length(ch.text_redacted)),0) AS chunk_bytes
                FROM canonical_documents d LEFT JOIN canonical_chunks ch
                USING(tenant_id,source_id,document_id)
                WHERE d.tenant_id=%s AND d.source_id=%s AND d.is_current
                GROUP BY d.tenant_id,d.source_id,d.document_id ORDER BY d.native_id''',
                (self.tenant,self.source)).fetchall()
            catalog = c.execute('''SELECT revision,manifest_artifact_id,document_content_sha256,
                record_count,receipt_count,part_count FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id='session' ''',
                (self.tenant,self.source)).fetchone()
        return dict(ledger=self.ledger(),catalog=catalog,documents=docs)

    def run_observed(self,label,batches):
        # Original fixture only accelerates eligibility age; no cursor/epoch/proof edit.
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s",(self.tenant,))
        observed=dict(metadata_rows=0,stream_calls=0,archive_gets=0,archive_bytes=0)
        stream,read=psycopg.Cursor.stream,self.archive.read_raw
        def trace_stream(cursor,query,*args,**kwargs):
            target='SELECT selected.*,items.chunks FROM (' in str(query)
            if target: observed['stream_calls']+=1
            it=stream(cursor,query,*args,**kwargs)
            try:
                for row in it:
                    if target: observed['metadata_rows']+=1
                    yield row
            finally:it.close()
        def trace_read(*args,**kwargs):
            data=read(*args,**kwargs);observed['archive_gets']+=1;observed['archive_bytes']+=len(data);return data
        before=self.state();started=time.monotonic()
        with patch.object(psycopg.Cursor,'stream',trace_stream),patch.object(self.archive,'read_raw',trace_read),patch.object(retirement,'retire_parent_chunks',wraps=retirement.retire_parent_chunks) as full:
            result=runner.run_retirement(self.store,self.archive,
                scope=runner.RetirementScope(self.tenant,self.principal,(self.source,)),
                apply=True,limits=runner.RunnerLimits(max_parents=1),
                parent_limits=replace(retirement.ParentRetirementLimits(),batch_documents=256,max_batches=batches),
                metadata_mode='singleton',deadline_at=time.monotonic()+60)
        observed.update(elapsed_seconds=time.monotonic()-started,full_parent_calls=full.call_count)
        value=dict(before=before,after=self.state(),result=result,observed=observed)
        print(json.dumps(dict(phase=label,result=result,observed=observed),default=str),flush=True)
        return value

    def prepare_eof(self, count=160, mixed=True, advance=True):
        inputs = {}
        for n in range(count - 2):
            native = 'turn/extra-%03d' % n
            text = ('Native archived historical row %d ' % n) + (
                'multi chunk receipt evidence ' * 2000 if mixed and n < 4
                else 'bounded native receipt ' * 35)
            inputs[native] = text
            self.fixture.ingest(native, text)
        with self.store.connect() as c:
            for native, text in inputs.items():
                c.execute("UPDATE canonical_documents SET body_location='inline',text_redacted=%s WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND is_current",
                    (text, self.tenant, self.source, native))
        published = self.fixture.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        self.assertEqual(published['failed'], 0)
        with self.store.connect() as c:
            c.execute('''UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL
                WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)''',
                (self.tenant, self.source, ['turn/extra-%03d' % n for n in range(count-25, count-2)] if mixed else []))
            receipts = tuple(row['receipt'] for row in c.execute(
                'SELECT receipt FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s ORDER BY receipt',
                (self.tenant, self.source)))
        self.receipts = receipts
        self.membership = lookup_receipt_metadata(self.store, tenant_id=self.tenant,
            authorized_sources=(self.source,), receipts=receipts, allow_verified_singletons=True)
        self.assertEqual(len(self.membership), count + (8 if mixed else 0))
        initial = self.state()
        self.assertEqual(len(initial['documents']), count)
        self.assertEqual(sum(row['body_record_ordinal'] is None for row in initial['documents']), 23 if mixed else 0)
        self.assertEqual(sum(row['physical_chunks'] > 1 for row in initial['documents']), 4 if mixed else 0)
        if not advance:
            return initial
        # Establish the existing native EOF state without the new preparation.
        with patch.object(window, '_thin_before_parent', return_value=None):
            cleared = self.run_observed(self.id().split('.')[-1] + '-initial', 2)
        self.assertGreater(cleared['result']['cleared_utf8_bytes'], 0)
        self.assertEqual(cleared['after']['ledger']['status'], 'partial')
        self.assertEqual(cleared['after']['ledger']['last_record_ordinal'], initial['catalog']['record_count'] - 1)
        return cleared['after']

    def invoke_thin(self, *, bound=256, epoch=None):
        scope = self.tenant, self.source, 'session'
        with self.store.connect() as c:
            catalog = read_parent_catalog(self.store, c, scope, time.monotonic()+5)
        return window._thin_before_parent(self.store, scope=scope, principal_id=self.principal,
            limits=replace(retirement.ParentRetirementLimits(), batch_documents=bound),
            deadline_at=time.monotonic()+10,
            reviewed_plan=window.parent_retirement_plan(scope, catalog['manifest']),
            required_scope_epoch=self.ledger()['scope_epoch'] if epoch is None else epoch)

    def assert_receipts_preserved(self):
        self.assertEqual(lookup_receipt_metadata(self.store, tenant_id=self.tenant,
            authorized_sources=(self.source,), receipts=self.receipts,
            allow_verified_singletons=True), self.membership)

    def test_parent_thins_then_fresh_claim_reclaims_without_loop(self):
        before = self.prepare_eof()
        eof = self.run_observed('productive-eof', 1)
        self.assertEqual(eof['observed']['full_parent_calls'], 0)
        self.assertEqual(eof['result']['completed_parent_proofs'], 0)
        self.assertEqual(eof['result']['parent_thinned_documents'], 160)
        self.assertEqual(eof['result']['parent_thin_document_bytes_removed'], 361464)
        self.assertGreater(eof['result']['parent_thin_event_bytes_replaced'], 0)
        self.assertEqual(eof['after']['ledger']['last_record_ordinal'], -1)
        self.assertGreater(eof['after']['ledger']['scope_epoch'], before['ledger']['scope_epoch'])
        self.assert_receipts_preserved()
        reclaimed = self.run_observed('fresh-claim-after-thinning', 2)
        self.assertEqual(reclaimed['result']['retired_metadata_chunks'], 133)
        self.assertEqual(sum(row['physical_chunks'] > 0 for row in reclaimed['after']['documents']), 27)
        self.assertEqual(sum(row['body_record_ordinal'] is None for row in reclaimed['after']['documents']), 23)
        after = self.ledger()
        self.assertIsNone(self.invoke_thin())
        self.assertEqual(self.ledger(), after)
        finished = self.run_observed('already-thinned-original-eof', 1)
        self.assertEqual(finished['observed']['full_parent_calls'], 1)
        self.assertEqual(finished['result']['completed_parent_proofs'], 1)
        self.assertEqual(finished['result']['parent_thinned_documents'], 0)
        self.assertEqual(finished['after']['ledger']['status'], 'complete')
        self.assert_receipts_preserved()

    def test_577_rows_thin_without_archive_replay_then_original_traversal(self):
        self.prepare_eof(count=577, mixed=False, advance=False)
        for expected in (256, 256, 65):
            observed = self.run_observed('bounded-577-thinning', 1)
            self.assertEqual(observed['result']['parent_thinned_documents'], expected)
            self.assertEqual(observed['observed']['archive_gets'], 0)
            self.assertEqual(observed['observed']['stream_calls'], 0)
            self.assertEqual(observed['observed']['full_parent_calls'], 0)
            self.assertFalse(observed['result']['commit_outcome_unknown'])
            self.assert_receipts_preserved()
        retired = self.run_observed('577-original-traversal', 10)
        self.assertEqual(retired['result']['parent_thinned_documents'], 0)
        self.assertEqual(retired['result']['retired_metadata_chunks'], 577)
        self.assertEqual(retired['after']['ledger']['status'], 'complete')
        self.assertEqual(retired['result']['completed_parent_proofs'] + retired['result']['completed_noop_parents'], 1)
        self.assert_receipts_preserved()

    def test_bounded_nomination_moves_past_changed_rows(self):
        self.assertEqual(self.invoke_thin(bound=1)['parent_thinned_documents'], 1)
        self.assertEqual(self.invoke_thin(bound=1)['parent_thinned_documents'], 1)
        self.assertIsNone(self.invoke_thin(bound=1))

    def test_ineligible_inline_prefix_does_not_hide_productive_tail(self):
        self.prepare_eof(count=260, mixed=False, advance=False)
        with self.store.connect() as c:
            ordered = c.execute('''SELECT d.document_id,e.artifact_id FROM canonical_events e
                JOIN canonical_documents d USING(tenant_id,source_id,event_id)
                WHERE e.tenant_id=%s AND e.source_id=%s AND d.is_current
                ORDER BY e.source_ordinal,e.occurred_at,e.native_id''',
                (self.tenant,self.source)).fetchall()
            c.execute('''UPDATE canonical_chunks SET deleted_at=now()
                WHERE tenant_id=%s AND source_id=%s AND document_id=ANY(%s)''',
                (self.tenant,self.source,[r['document_id'] for r in ordered[:128]]))
            c.execute('''UPDATE raw_artifacts SET storage_backend='filesystem'
                WHERE tenant_id=%s AND source_id=%s AND artifact_id=ANY(%s)''',
                (self.tenant,self.source,[r['artifact_id'] for r in ordered[128:256]]))
        before = self.state()
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as reads:
            result = self.invoke_thin()
        reads.assert_not_called()
        self.assertIsNotNone(result)
        self.assertEqual(result['parent_thinned_documents'],4)
        after = self.state()
        original = {r['document_id']:r for r in before['documents']}
        current = {r['document_id']:r for r in after['documents']}
        for row in ordered[:256]:
            self.assertEqual(current[row['document_id']], original[row['document_id']])
        for row in ordered[256:]:
            self.assertEqual(current[row['document_id']]['body_location'],'chunks')
        self.assertIsNone(self.invoke_thin())
        print(json.dumps(dict(phase='ineligible-prefix',skipped_no_live_chunk=128,
            skipped_non_s3_raw=128,changed=4,next_changes=0)))

    def test_small_parent_keeps_original_first_chunk_progress(self):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        with patch.object(window, '_thin_before_parent', wraps=window._thin_before_parent) as preparation:
            result = runner.run_retirement(self.store, self.archive,
                scope=runner.RetirementScope(self.tenant,self.principal,(self.source,)),
                apply=True, limits=runner.RunnerLimits(max_parents=1), metadata_mode='singleton',
                parent_limits=replace(retirement.ParentRetirementLimits(),batch_documents=1,max_batches=1),
                deadline_at=time.monotonic()+20)
        preparation.assert_not_called()
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(result['parent_thinned_documents'], 0)

    def test_large_parent_runtime_guard_precedes_thinning(self):
        before = self.prepare_eof(advance=False)
        with patch.object(self.store, 'verified_singleton_receipts', False), \
                patch.object(window, '_thin_before_parent', wraps=window._thin_before_parent) as preparation:
            observed = self.run_observed('runtime-refusal-before-thinning', 1)
        preparation.assert_not_called()
        self.assertEqual(observed['observed']['archive_gets'], 0)
        self.assertEqual(observed['after']['documents'], before['documents'])
        self.assertEqual(observed['result']['parent_thinned_documents'], 0)
        self.assertIn('singleton_metadata_archive_runtime_required', observed['result']['errors'])

    def test_stale_epoch_and_revoked_owner_refuse(self):
        before = self.state()
        with self.assertRaises(retirement.ChunkRetirementError):
            self.invoke_thin(epoch=before['ledger']['scope_epoch']+1)
        self.assertEqual(self.state(), before)
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s',
                (self.tenant, self.principal))
        with self.assertRaises(retirement.ChunkRetirementError):
            self.invoke_thin()
        self.assertEqual(self.state(), before)

    def test_expired_attempt_precedes_thinning(self):
        from types import SimpleNamespace
        before = self.prepare_eof(advance=False)
        scope = self.tenant, self.source, 'session'
        deadline = time.monotonic()+20
        with self.store.connect() as c:
            catalog = read_parent_catalog(self.store, c, scope, deadline)
        # Model the original body-proof clock reaching the caller's deadline
        # after SQL admission, without changing the database's statement clock.
        with patch('recall_server.chunk_bodies.time', SimpleNamespace(monotonic=lambda: deadline+1)), \
                patch.object(window, '_thin_before_parent', wraps=window._thin_before_parent) as preparation:
            with self.assertRaises(retirement.ChunkRetirementError):
                window.retire_parent_windows(self.store, self.archive, tenant_id=self.tenant,
                    source_id=self.source, native_parent_id='session', apply=True,
                    reviewed_plan=window.parent_retirement_plan(scope, catalog['manifest']),
                    limits=retirement.ParentRetirementLimits(), deadline_at=deadline,
                    required_scope_epoch=before['ledger']['scope_epoch'],
                    owner_principal_id=self.principal, metadata_mode='singleton')
        preparation.assert_not_called()
        self.assertEqual(self.state(), before)

    def test_append_between_nomination_and_write_refuses(self):
        original = retirement.require_retirement_owner
        changed = False
        def authorize(*args, **kwargs):
            nonlocal changed
            if kwargs.get('lock', True) and not changed:
                changed = True
                self.fixture.ingest('turn/late', 'New revision invalidates the reviewed parent')
            return original(*args, **kwargs)
        with patch.object(retirement, 'require_retirement_owner', side_effect=authorize), \
                patch('recall_server.canonical_thinning._thin_selected', wraps=_thin_selected) as thin:
            with self.assertRaises(retirement.ChunkRetirementError):
                self.invoke_thin()
        self.assertTrue(changed)
        thin.assert_not_called()

    def test_forget_between_nomination_and_write_refuses(self):
        original = retirement.require_retirement_owner
        changed = False
        def authorize(*args, **kwargs):
            nonlocal changed
            if kwargs.get('lock', True) and not changed:
                changed = True
                result = self.fixture.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=self.receipts[0], mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-21T00:00:00Z', idempotency_key='eof-thin-forget'))
                self.assertEqual(result['status'], 'deleted')
            return original(*args, **kwargs)
        with patch.object(retirement, 'require_retirement_owner', side_effect=authorize), \
                patch('recall_server.canonical_thinning._thin_selected', wraps=_thin_selected) as thin:
            with self.assertRaises(retirement.ChunkRetirementError):
                self.invoke_thin()
        self.assertTrue(changed)
        thin.assert_not_called()

    def test_nomination_timeout_falls_back_without_writes(self):
        original = self.store._execute_bounded
        before = self.state()
        def execute(c, sql, *args, **kwargs):
            if 'ORDER BY source_ordinal,occurred_at,native_id OFFSET 0' in sql:
                raise SearchDeadlineExceeded()
            return original(c, sql, *args, **kwargs)
        with patch.object(self.store, '_execute_bounded', side_effect=execute):
            self.assertIsNone(self.invoke_thin())
        self.assertEqual(self.state(), before)

    def test_lost_thinning_ack_intent_reconciles_and_source_resumes(self):
        from recall_server import retirement_policy as policy
        from recall_server.retirement_maintenance import RetirementMaintenance
        self.prepare_eof()
        scope = runner.RetirementScope(self.tenant, self.principal, (self.source,))
        policy.start_policy(self.store, scope=scope)
        self.assertTrue(policy.capture_baseline(self.store, scope=scope)['complete'])
        policy.set_policy_enabled(self.store, scope=scope,
            reviewed_plan=policy.policy_plan(self.store, scope=scope), enabled=True)
        def ready():
            with self.store.connect() as c:
                c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
                c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1970-01-01' WHERE tenant_id=%s", (self.tenant,))
        def state():
            with self.store.connect() as c:
                return c.execute('SELECT * FROM canonical_retirement_source_policy WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.source)).fetchone()
        worker = RetirementMaintenance(self.store, self.archive, tenant_id=self.tenant,
            enabled=True, seconds=60, max_parents=1, metadata_mode='singleton')
        original = window._thin_before_parent
        def lost(*args, **kwargs):
            result = original(*args, **kwargs)
            self.assertEqual(result['parent_thinned_documents'], 160)
            raise ConnectionError('synthetic lost COMMIT acknowledgement')
        ready()
        with patch.object(window, '_thin_before_parent', side_effect=lost) as calls:
            first = worker.cycle(busy=False)
        self.assertEqual(calls.call_count, 1)
        self.assertTrue(first['commit_outcome_unknown'])
        self.assertEqual(first['parent_thinned_documents'], 0)
        intent = state()['maintenance_intent']
        self.assertIsNotNone(intent)
        self.assertGreater(self.ledger()['scope_epoch'], intent['scope_epoch'])
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)
        self.assertEqual(sum(row['inline_bytes'] for row in self.state()['documents']), 0)
        self.assert_receipts_preserved()
        ready()
        second = worker.cycle(busy=False)
        self.assertFalse(second['commit_outcome_unknown'])
        self.assertEqual(second['reconciled_intents'], 1)
        self.assertEqual(second['observed_previous_cleared_documents'], 0)
        self.assertEqual(second['parent_thinned_documents'], 0)
        self.assertEqual(second['retired_metadata_chunks'], 133)
        self.assertIsNone(state()['maintenance_intent'])
        self.assertEqual(self.ledger()['status'], 'complete')
        self.assert_receipts_preserved()
        print(json.dumps(dict(phase='lost-thin-ack-recovery', first=first, second=second), default=str))


if __name__ == '__main__':
    unittest.main()
