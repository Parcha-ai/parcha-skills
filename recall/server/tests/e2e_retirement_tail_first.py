"""Tail ACKs survive a real PostgreSQL full-capture timeout; EOF stays native."""
from dataclasses import replace
import json
import os
import time
import unittest
from unittest.mock import patch

import e2e_selective_retirement_experiment as fixture
from recall_server import chunk_retirement, parent_chunk_proof, retirement_runner


class TailFirst(unittest.TestCase):
    state = fixture.SelectiveExperiment.state

    def setUp(self):
        original = fixture.fixture
        with patch.object(fixture, 'fixture', side_effect=lambda store, root, count:
                          original(store, root, count=301)):
            fixture.SelectiveExperiment.setUp(self)

    def run_parent(self, *, max_batches=1000, seconds=20):
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s", (self.tenant,))
        if hasattr(self.archive, 'reads'):
            self.archive.reads.clear()
        return retirement_runner.run_retirement(
            self.store, self.archive,
            scope=retirement_runner.RetirementScope(self.tenant, self.principal, (self.source,)),
            apply=True, limits=retirement_runner.RunnerLimits(max_parents=1),
            parent_limits=replace(self.limits, batch_documents=100, max_batches=max_batches),
            deadline_at=time.monotonic()+seconds,
        )

    def progress(self):
        with self.store.connect() as connection:
            return chunk_retirement._parent_progress(self.store, connection, self.scope, time.monotonic()+5)

    def test_last_tail_ack_precedes_blocked_full_capture_and_restart_completes(self):
        prefix = self.run_parent(max_batches=3, seconds=60)
        self.assertEqual(prefix['cleared_documents'], 300)
        self.assertEqual(prefix['completed_parent_proofs'], 0)
        self.assertEqual(self.progress()['last_record_ordinal'], 299)
        original_capture = parent_chunk_proof._capture

        def blocked_capture(*args, **kwargs):
            # A separate real SQL transaction blocks the unchanged metadata
            # SELECT. Both old and new schedulers encounter this same failure.
            with self.store.connect() as blocker, blocker.transaction():
                blocker.execute('LOCK TABLE canonical_documents IN ACCESS EXCLUSIVE MODE')
                return original_capture(*args, **kwargs)

        with patch.object(parent_chunk_proof, '_capture', side_effect=blocked_capture):
            result = self.run_parent(seconds=1.5)
        print(json.dumps({'case': 'blocked_full_capture', **result}, default=str))
        # The original named-cursor QueryCanceled boundary is deliberately
        # conservative. This scheduler change must not reclassify it.
        self.assertEqual(result['errors'], {'parent_retirement_unavailable': 1})
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['error_class'], 'QueryCanceled')
        self.assertEqual(result['cleared_documents'], 1)
        self.assertGreater(result['cleared_utf8_bytes'], 0)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(self.progress()['status'], 'partial')
        self.assertEqual(self.progress()['last_record_ordinal'], 300)
        self.assertFalse(any(row['text_redacted'] for row in self.state()))
        resumed = self.run_parent()
        self.assertEqual(resumed['cleared_documents'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 1)
        self.assertEqual(self.progress()['status'], 'complete')


class NativeTail(unittest.TestCase):
    run_parent = TailFirst.run_parent
    progress = TailFirst.progress

    def setUp(self):
        import e2e_logical_append_capability as native
        from e2e_compact_canonical_bodies import DurableArchiveFixture
        from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
        from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore
        native.AppendCapability.setUp(self)
        self.archive = DurableArchiveFixture(self.archive.root, namespace_key=b'a' * 32)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=self.tenant, principal_id=self.principal)
        self.plane = CanonicalPlane(self.store, self.archive, chunk_body_archive=self.archive)
        self.logical = CanonicalLogicalEvidenceProjector(self.store,
            LogicalEvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant,
            raw_archive=self.archive)
        self.event = native.AppendCapability.event.__get__(self)
        self.ingest = native.AppendCapability.ingest.__get__(self)
        self.publish = native.AppendCapability.publish.__get__(self)
        self.plane.chunk_body_archive = self.archive
        self.plane.evidence_projector = CanonicalEvidenceProjector(
            self.store, EvidenceProjectionStore(self.archive))
        self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
            events=[self.event(f'event-{i:03}', i, 'Native retained body.') for i in range(101)])
        self.assertEqual(self.publish()['documents'], 1)
        self.scope = (self.tenant, self.source, self.parent)
        self.limits = chunk_retirement.ParentRetirementLimits()
        chunk_retirement.set_parent_retirement_enabled(self.store,
            tenant_id=self.tenant, source_id=self.source,
            native_parent_id=self.parent, enabled=True)
        environment = patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive')
        environment.start()
        self.addCleanup(environment.stop)

    def test_101_completes_in_same_five_second_attempt_with_production_batch_bound(self):
        started = time.monotonic()
        execute = self.store._execute_bounded
        with patch.object(self.store, '_execute_bounded', wraps=execute) as queries, \
                patch.object(self.archive, 'read_raw', wraps=self.archive.read_raw) as reads:
            result = self.run_parent(seconds=5)
        self.assertEqual(result['cleared_documents'], 101)
        self.assertEqual(result['completed_parent_proofs'], 1)
        self.assertEqual(self.progress()['status'], 'complete')
        print(json.dumps({'case': 'native101', 'elapsed_ms': (time.monotonic()-started)*1000,
            'sql_execute_calls': queries.call_count, 'actual_archive_reads': reads.call_count,
            **{key: result[key] for key in ('batches', 'archive_gets', 'archive_bytes',
                'archive_reused_gets', 'archive_reused_bytes',
                'cleared_utf8_bytes', 'metadata_rows', 'committed_sql_ms')}}))

    def test_exact_two_batch_limit_preserves_acks_and_defers_completion(self):
        result = self.run_parent(max_batches=2)
        self.assertEqual(result['batches'], 2)
        self.assertEqual(result['cleared_documents'], 101)
        self.assertEqual(result['completed_parent_proofs'], 0)
        self.assertEqual(self.progress()['status'], 'partial')
        resumed = self.run_parent(max_batches=2)
        self.assertEqual(resumed['cleared_documents'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 1)

    def after_tail(self, callback):
        self.assertEqual(self.run_parent(max_batches=1)['cleared_documents'], 100)
        commit = chunk_retirement._retire_parent_batch
        changed = False
        callback_errors = []
        def after(*args, **kwargs):
            nonlocal changed
            result = commit(*args, **kwargs)
            if kwargs.get('coverage') and not changed:
                changed = True
                try:
                    callback()
                except Exception as error:
                    callback_errors.append(error)
                    raise
            return result
        with patch.object(chunk_retirement, '_retire_parent_batch', side_effect=after):
            result = self.run_parent()
        self.assertTrue(changed)
        if callback_errors and not isinstance(callback_errors[0], ConnectionError):
            raise callback_errors[0]
        self.assertEqual(result['completed_parent_proofs'], 0)
        progress = self.progress()
        self.assertTrue(progress is None or progress['status'] != 'complete')
        return result

    def test_warm_native_append_after_tail_prevents_old_manifest_completion(self):
        result = self.after_tail(lambda: self.ingest('later', 200, 'New native body.'))
        self.assertEqual(result['cleared_documents'], 1)
        self.assertTrue(result['errors'])

    def test_warm_historical_edit_after_tail_prevents_old_manifest_completion(self):
        result = self.after_tail(lambda: self.ingest('event-000', 0, 'Changed native body.'))
        self.assertEqual(result['cleared_documents'], 1)
        self.assertTrue(result['errors'])

    def test_source_revocation_after_tail_prevents_completion(self):
        def revoke():
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s', (self.tenant,))
        result = self.after_tail(revoke)
        self.assertEqual(result['cleared_documents'], 1)
        self.assertTrue(result['errors'])

    def test_native_parent_forget_after_tail_prevents_completion(self):
        self.ingest(self.parent, 102, 'Parent identity body.')
        self.assertEqual(self.publish()['documents'], 1)
        def forget():
            result = self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                target_receipt=f'recall://{self.source}/{self.parent}?rev=1#item=0',
                mode='explicit_forget', reason='owner_requested', requested_at='2026-10-17T00:00:00Z',
                idempotency_key='tail-first-forget'))
            self.assertEqual(result['status'], 'deleted')
        self.assertEqual(self.after_tail(forget)['cleared_documents'], 2)

    def test_partial_native_forget_keeps_revocation_and_refuses_completion(self):
        from recall_server.canonical import CanonicalLifecycleError
        # Retain the alternate whole-parent deletion fence counterexample.
        # Production injects the per-event revision owner instead.
        self.plane.evidence_projector = self.logical
        def forget():
            # The unchanged delete owner cannot reconstruct these retired
            # surviving siblings. Its committed revocation still wins.
            with self.assertRaisesRegex(CanonicalLifecycleError, 'archive_delete_failed'):
                self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=f'recall://{self.source}/event-100?rev=1#item=0',
                    mode='explicit_forget', reason='owner_requested', requested_at='2026-10-17T00:00:00Z',
                    idempotency_key='tail-first-partial-forget'))
            with self.store.connect() as connection:
                rows = connection.execute('SELECT status FROM forget_tombstones WHERE tenant_id=%s',
                    (self.tenant,)).fetchall()
            self.assertEqual([row['status'] for row in rows], ['deleting'])
        result = self.after_tail(forget)
        self.assertEqual(result['cleared_documents'], 1)
        self.assertTrue(result['errors'])

    def test_app_wired_single_event_forget_after_retirement_rebuilds_survivors(self):
        from recall_server.chunk_bodies import read_archived_chunks
        self.assertEqual(self.run_parent()['completed_parent_proofs'], 1)
        result = self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=f'recall://{self.source}/event-100?rev=1#item=0',
            mode='explicit_forget', reason='owner_requested', requested_at='2026-10-17T00:00:00Z',
            idempotency_key='tail-first-app-forget'))
        self.assertEqual(result['status'], 'deleted')
        self.assertEqual(self.publish()['documents'], 1)
        with self.store.connect() as connection:
            documents = connection.execute('''SELECT document_id,native_id FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND is_current AND deleted_at IS NULL
                ORDER BY native_id''', (self.tenant, self.source)).fetchall()
        self.assertEqual(len(documents), 100)
        self.assertNotIn('event-100', [row['native_id'] for row in documents])
        bodies = read_archived_chunks(self.store, self.archive, tenant_id=self.tenant,
            source_ids=(self.source,), document_ids=tuple(row['document_id'] for row in documents),
            located_only=True, deadline_at=time.monotonic()+10)
        self.assertEqual(len(bodies), 100)
        self.assertTrue(all('Native retained body.' in ''.join(chunk['text_redacted'] for chunk in chunks)
            for chunks in bodies.values()))

    def test_actual_tail_commit_lost_response_retains_unknown_and_restart_uses_full_proof(self):
        def lose_response():
            raise ConnectionError('synthetic lost selected-tail commit response')
        result = self.after_tail(lose_response)
        self.assertTrue(result['commit_outcome_unknown'])
        self.assertEqual(result['cleared_documents'], 0)
        self.assertEqual(self.progress()['last_record_ordinal'], 100)
        resumed = self.run_parent()
        self.assertEqual(resumed['cleared_documents'], 0)
        self.assertEqual(resumed['completed_parent_proofs'], 1)


if __name__ == '__main__':
    unittest.main()
