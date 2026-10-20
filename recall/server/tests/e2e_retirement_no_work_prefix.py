"""Current admitted singleton prefixes need no repeat archive proof to schedule a live tail."""
from dataclasses import replace
import json
import os
import time
import unittest
from unittest.mock import patch

from e2e_singleton_metadata_admission import SingletonMetadataAdmission
from recall_server import chunk_retirement as retirement, retirement_runner as runner


class NoWorkPrefix(SingletonMetadataAdmission):
    def setUp(self):
        super().setUp()
        env = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        env.start()
        self.addCleanup(env.stop)
        self.store.verified_singleton_receipts = True
        self.store.search_plane = 'turbopuffer'
        put = self.projector.projection.put_records
        small_parts = patch.object(self.projector.projection, 'put_records',
            side_effect=lambda **kwargs: put(**dict(kwargs, part_bytes=1024)))
        small_parts.start()
        self.addCleanup(small_parts.stop)
        self.receipts = [self.ingest('turn/'+name, name+' '+('native body ' * 70))['receipt']
                         for name in ('one', 'two')]
        self.publish()
        retirement.set_parent_retirement_enabled(self.store, tenant_id=self.tenant,
            source_id=self.source, native_parent_id='session', enabled=True)

    def publish(self):
        result = self.projector.project_pending(tenant_id=self.tenant, batch_size=10,
            max_batches=1, upload_concurrency=1)
        self.assertEqual(result['failed'], 0)

    def run_parent(self, batches):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        return runner.run_retirement(self.store, self.archive,
            scope=runner.RetirementScope(self.tenant, self.principal, (self.source,)),
            apply=True, limits=runner.RunnerLimits(max_parents=1),
            parent_limits=replace(retirement.ParentRetirementLimits(), batch_documents=1, max_batches=batches),
            metadata_mode='singleton', deadline_at=time.monotonic()+20)

    def ledger(self):
        with self.store.connect() as c:
            return c.execute('SELECT * FROM canonical_chunk_retirement_progress WHERE tenant_id=%s', (self.tenant,)).fetchone()

    def append_after_retired_prefix(self):
        first = self.run_parent(2)
        self.assertEqual(first['retired_metadata_chunks'], 2)
        with self.store.connect() as c:
            parts = c.execute('SELECT artifact_id FROM canonical_evidence_document_parts WHERE tenant_id=%s ORDER BY part_ordinal', (self.tenant,)).fetchall()
        self.assertEqual(len(parts), 2)
        tail = self.ingest('turn/zz-tail', 'tail '+('native body ' * 70))['receipt']
        self.publish()
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)
        return {p['artifact_id'] for p in parts}, tail

    def test_retired_prefix_after_append_avoids_archive_reads_and_proves_tail(self):
        prefix, tail = self.append_after_retired_prefix()
        before = self.ledger()
        with patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as reads, \
                patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(3)
        opened = [call.args[0]['artifact_id'] for call in reads.call_args_list]
        print(json.dumps({'result': result, 'prefix_reads': len(prefix.intersection(opened)),
                          'archive_gets': len(opened)}, default=str))
        self.assertFalse(prefix.intersection(opened), 'already-retired prefix archive reopened')
        self.assertEqual(result['cleared_documents'], 1)
        self.assertEqual(result['retired_metadata_chunks'], 1)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(full.call_count, 0)
        self.assertIsNone(self.baseline(tail))
        self.assertEqual(self.ledger()['cumulative_cleared_documents'], before['cumulative_cleared_documents']+1)
        self.assertIsNotNone(self.store.resolve(tail, tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def drain(self):
        self.assertEqual(self.run_parent(2)['retired_metadata_chunks'], 2)
        return self.ledger()

    def test_noop_maintenance_completion_retains_its_source_round(self):
        from e2e_retirement_maintenance import MaintenanceTests
        from recall_server import retirement_policy as policy
        from recall_server.retirement_maintenance import RetirementMaintenance
        self.drain()
        self.scope = runner.RetirementScope(self.tenant, self.principal, (self.source,))
        self.key = self.tenant, self.source, 'session'
        policy.start_policy(self.store, scope=self.scope)
        self.assertTrue(policy.capture_baseline(self.store, scope=self.scope)['complete'])
        policy.set_policy_enabled(self.store, scope=self.scope,
            reviewed_plan=policy.policy_plan(self.store, scope=self.scope), enabled=True)
        empty = MaintenanceTests.empty_source_first(self)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_retirement_source_policy SET maintenance_scheduled_at='1969-01-01' WHERE tenant_id=%s AND source_id=%s", self.key[:2])
            c.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        owner = RetirementMaintenance(self.store, self.archive, tenant_id=self.tenant,
            enabled=True, seconds=10, metadata_mode='singleton')
        # Exercise the existing window path with two records rather than the
        # production256-record threshold; all proof and completion owners run.
        limits = retirement.ParentRetirementLimits
        with patch.object(self.archive, 'read_raw', side_effect=AssertionError('drained archive reopened')), \
                patch('recall_server.retirement_maintenance.ParentRetirementLimits',
                      side_effect=lambda **kwargs: replace(limits(**kwargs), batch_documents=1)):
            result = owner.cycle(busy=False)
        self.assertEqual(result['completed_noop_parents'], 1, result)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['cleared_utf8_bytes'], 0)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(MaintenanceTests.source_epochs(self), {self.source: 1, empty: 0})

    def test_eof_is_scheduling_only_without_archive_or_repeated_attempt(self):
        before = self.drain()
        with patch.object(self.archive, 'read_raw', side_effect=AssertionError('drained archive reopened')) as reads, \
                patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 1)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['cleared_utf8_bytes'], 0)
        self.assertEqual(result['retired_metadata_chunks'], 0)
        reads.assert_not_called(); full.assert_not_called()
        after = self.ledger()
        self.assertEqual(after['status'], 'complete')
        for field in ('cumulative_cleared_documents', 'cumulative_cleared_chunks', 'cumulative_cleared_utf8_bytes'):
            self.assertEqual(after[field], before[field])
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)

    def restore_metadata(self):
        from recall_server.singleton_metadata_restore import restore_singleton_metadata
        with self.store.connect() as c:
            doc = c.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s AND is_current', (self.tenant, 'turn/one')).fetchone()['document_id']
        args = dict(tenant_id=self.tenant, source_id=self.source, principal_id=self.principal, document_ids=(doc,))
        preview = restore_singleton_metadata(self.store, self.archive, **args)
        return restore_singleton_metadata(self.store, self.archive, **args, apply=True, reviewed_plan=preview['plan'])

    def test_restored_physical_metadata_behind_cursor_keeps_original_eof_proof(self):
        self.drain()
        self.assertEqual(self.restore_metadata()['restored_documents'], 1)
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['completed_parent_proofs'], 1)

    def test_gap_overlap_unlocated_and_unadmitted_prefix_fall_back_without_advancing(self):
        from recall_server import parent_chunk_window as window
        self.append_after_retired_prefix()
        with self.store.connect() as c:
            docs = c.execute('SELECT document_id,body_record_ordinal,body_record_count,chunk_receipt_profile FROM canonical_documents WHERE tenant_id=%s AND is_current ORDER BY body_record_ordinal', (self.tenant,)).fetchall()
        changes = [('gap', docs[0]['document_id'], 'body_record_ordinal', 1),
                   ('overlap', docs[1]['document_id'], 'body_record_ordinal', 0),
                   ('unlocated', docs[0]['document_id'], 'body_record_ordinal', None),
                   ('profile_null', docs[0]['document_id'], 'chunk_receipt_profile', None)]
        for label, doc, field, value in changes:
            with self.subTest(label=label):
                with self.store.connect() as c:
                    extra = ',body_record_count=NULL' if label=='unlocated' else ''
                    c.execute(f'UPDATE canonical_documents SET {field}=%s{extra} WHERE tenant_id=%s AND document_id=%s', (value, self.tenant, doc))
                with patch.object(window, 'prepare_window', side_effect=AssertionError('original fallback reached')) as prepare:
                    result = self.run_parent(1)
                self.assertEqual(prepare.call_count, 1)
                self.assertEqual(result['completed_noop_parents'], 0)
                self.assertEqual(self.ledger()['last_record_ordinal'], -1)
                with self.store.connect() as c:
                    original = next(d[field] for d in docs if d['document_id']==doc)
                    c.execute(f'UPDATE canonical_documents SET {field}=%s,body_record_count=1 WHERE tenant_id=%s AND document_id=%s', (original, self.tenant, doc))

    def test_empty_deleted_nonzero_physical_row_prevents_prefix_skip(self):
        from recall_server import parent_chunk_window as window
        self.append_after_retired_prefix()
        self.restore_metadata()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET ordinal=7,deleted_at=now() WHERE tenant_id=%s AND document_id IN (SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s)", (self.tenant, self.tenant, 'turn/one'))
            c.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND native_id=%s', (self.tenant, 'turn/one'))
        with patch.object(window, 'prepare_window', side_effect=AssertionError('physical row requires original proof')) as prepare:
            result = self.run_parent(1)
        self.assertEqual(prepare.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)

    def test_locked_physical_tail_does_not_block_admitted_prefix(self):
        self.append_after_retired_prefix()
        with self.store.connect() as c, c.transaction():
            tail = c.execute('''SELECT document_id FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND native_id='turn/zz-tail'
                  AND is_current FOR UPDATE NOWAIT''', (self.tenant, self.source)).fetchone()
            self.assertIsNotNone(tail)
            with patch.object(self.archive, 'read_raw',
                              side_effect=AssertionError('admitted prefix reopened')):
                result = self.run_parent(1)
        self.assertFalse(result['commit_outcome_unknown'])
        self.assertEqual(result['errors'], {})
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(self.ledger()['last_record_ordinal'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')

    def test_append_between_no_work_check_and_cursor_write_refuses(self):
        self.drain()
        execute = self.store._execute_bounded
        appended = False
        def query(connection, sql, *args, **kwargs):
            nonlocal appended
            result = execute(connection, sql, *args, **kwargs)
            if 'END AS drained' in sql and not appended:
                appended = True
                self.ingest('turn/zz-tail', 'Concurrent fresh collector ACK')
            return result
        with patch.object(self.store, '_execute_bounded', side_effect=query):
            result = self.run_parent(1)
        self.assertTrue(appended)
        self.assertEqual(result['errors'], {'parent_retirement_window_pending': 1})
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(self.ledger()['status'], 'partial')

    def test_append_after_scheduling_completion_rearms_original_epoch(self):
        self.drain()
        before = self.ledger()
        self.assertEqual(self.run_parent(1)['completed_noop_parents'], 1)
        self.ingest('turn/zz-tail', 'Append after scheduling completion')
        self.publish()
        after = self.ledger()
        self.assertEqual(after['status'], 'pending')
        self.assertGreater(after['scope_epoch'], before['scope_epoch'])
        self.assertEqual(after['last_record_ordinal'], -1)
        self.assertEqual(self.run_parent(3)['retired_metadata_chunks'], 1)

    def test_restore_shared_catalog_lock_blocks_prefix_scheduler(self):
        from recall_server import singleton_metadata_restore as restore
        self.append_after_retired_prefix()
        observed = []
        original = restore.restore_outgoing
        def locked(*args, **kwargs):
            observed.append(self.run_parent(1))
            return original(*args, **kwargs)
        with patch.object(restore, 'restore_outgoing', side_effect=locked):
            self.restore_metadata()
        self.assertEqual(observed[0]['errors'], {'parent_retirement_lock_busy': 1})
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)

    def test_lost_completion_ack_is_unknown_without_claimed_proof(self):
        from contextlib import contextmanager
        import psycopg
        self.drain()
        transaction, execute = psycopg.Connection.transaction, self.store._execute_bounded
        armed = False
        def query(connection, sql, values, *args, **kwargs):
            nonlocal armed
            result = execute(connection, sql, values, *args, **kwargs)
            if 'SET manifest_artifact_id=%s,last_record_ordinal=%s,status=%s' in sql and values[2]=='complete':
                armed = True
            return result
        @contextmanager
        def lost_ack(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic lost scheduling acknowledgement')
        with patch.object(self.store, '_execute_bounded', side_effect=query), \
                patch.object(psycopg.Connection, 'transaction', lost_ack):
            result = self.run_parent(1)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(self.ledger()['status'], 'complete')
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)

    def before_advance(self, action):
        from recall_server import parent_chunk_window as window
        advance = window._advance_without_physical_work
        def wrapped(*args, **kwargs):
            action()
            return advance(*args, **kwargs)
        return patch.object(window, '_advance_without_physical_work', side_effect=wrapped)

    def test_revoked_owner_after_claim_cannot_advance(self):
        self.append_after_retired_prefix()
        def revoke():
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s', (self.tenant, self.principal))
        with self.before_advance(revoke):
            result = self.run_parent(1)
        self.assertEqual(result['errors'], {'retirement_owner_required': 1})
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)

    def test_disabled_new_epoch_after_claim_cannot_advance(self):
        self.append_after_retired_prefix()
        with self.before_advance(lambda: retirement.set_parent_retirement_enabled(self.store,
                tenant_id=self.tenant, source_id=self.source, native_parent_id='session', enabled=False)):
            result = self.run_parent(1)
        self.assertEqual(result['errors'], {'parent_retirement_disabled': 1})
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)
        self.assertEqual(self.ledger()['status'], 'disabled')

    def test_child_forget_after_claim_cannot_skip_old_locator(self):
        self.append_after_retired_prefix()
        def forget():
            result = self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                target_receipt=self.receipts[0], mode='explicit_forget', reason='owner_requested',
                requested_at='2026-10-17T00:00:00Z', idempotency_key='prefix-forget'))
            self.assertEqual(result['status'], 'deleted')
        with self.before_advance(forget):
            result = self.run_parent(1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')
        self.assertEqual(self.ledger()['last_record_ordinal'], -1)
        self.assertIsNone(self.store.resolve(self.receipts[0], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive))

    def test_inline_profile_null_no_chunks_is_not_no_work_proof(self):
        self.drain()
        # Preserve the observed counterexample shape: absent physical chunks are
        # not evidence that historical inline document/event bodies are retired.
        with self.store.connect() as c:
            c.execute("UPDATE canonical_documents SET chunk_receipt_profile=NULL,body_location='inline',text_redacted=%s WHERE tenant_id=%s", ('x'*115, self.tenant))
            c.execute("UPDATE canonical_events SET canonical_redacted=jsonb_build_object('text',%s::text),body_location='inline' WHERE tenant_id=%s", ('x'*1944, self.tenant))
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)
        self.assertNotEqual(self.ledger()['status'], 'complete')
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT min(octet_length(text_redacted)) AS n FROM canonical_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 115)
            self.assertEqual(c.execute('SELECT min(octet_length(canonical_redacted::text)) AS n FROM canonical_events WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 1956)

    def test_empty_current_parent_cannot_claim_scheduling_completion(self):
        self.drain()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s', (self.tenant,))
        with patch.object(retirement, 'retire_parent_chunks', wraps=retirement.retire_parent_chunks) as full:
            result = self.run_parent(1)
        self.assertEqual(full.call_count, 1)
        self.assertEqual(result['completed_noop_parents'], 0)

    def test_body_restore_disables_previously_completed_scheduler(self):
        self.drain()
        self.assertEqual(self.run_parent(1)['completed_noop_parents'], 1)
        self.restore_metadata()
        with self.store.connect() as c:
            doc = c.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s AND is_current', (self.tenant, 'turn/one')).fetchone()['document_id']
        args = dict(tenant_id=self.tenant, source_id=self.source, document_ids=(doc,), restore=True)
        preview = retirement.retire_current_chunks(self.store, self.archive, **args)
        restored = retirement.retire_current_chunks(self.store, self.archive, **args, apply=True, reviewed_plan=preview['plan'])
        self.assertGreater(restored['bytes'], 0)
        self.assertFalse(self.ledger()['enabled'])
        self.assertEqual(self.run_parent(1)['attempted_parents'], 0)


if __name__ == '__main__':
    suite = unittest.TestSuite(NoWorkPrefix(name) for name in NoWorkPrefix.__dict__ if name.startswith('test_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
