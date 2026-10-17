"""A retired publication needs durable proof work even without an earlier queue."""
import json
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

from e2e_retirement_membership_certification import RetirementMembershipTests
from recall_server import membership_remainder
from recall_server import parent_membership_enrollment as membership
from recall_server import chunk_retirement, retirement_runner


class UnqueuedRetirementCertificationTests(RetirementMembershipTests):
    def pending_certificates(self):
        return membership_remainder.pending(self.store, tenant_id=self.tenant,
            limit=10, deadline_at=time.monotonic() + 3)

    def test_partial_certificate_after_retirement_survives_restart_without_prior_queue(self):
        self.ingest('turn/unqueued-tail', 'Synthetic evidence paragraph. ' * 3000)
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        self.publisher(self.settings(enabled=False)).project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        self.refresh_scope()
        self.assertGreater(len(self.scope['passage_ids']), 1)
        self.assertEqual(membership_remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3), [])
        original = membership.enroll_parent_passages
        calls = []

        def interrupt_second_page(*args, **kwargs):
            calls.append(tuple(kwargs['passage_ids']))
            if len(calls) == 2:
                raise membership.SearchDeadlineExceeded('synthetic certificate deadline')
            return original(*args, **kwargs)

        with (patch.dict(membership._CERTIFICATION_LIMITS, passages=1),
              patch.object(membership, 'enroll_parent_passages', side_effect=interrupt_second_page)):
            retired = self.run_retirement()
        self.assertEqual(retired['completed_parent_proofs'], 1)
        self.assertFalse(retired['commit_outcome_unknown'])
        self.assertEqual(retired['membership_certified_passages'], 1)
        self.assertEqual(self.run_retirement()['completed_parent_proofs'], 0)
        restarted = self.publisher()
        with patch.object(restarted.logical_projection, 'read_part',
                          side_effect=AssertionError('no archive replay for certification')):
            resumed = restarted.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        self.assertEqual(resumed['documents'], 0)
        compared = self.compare()
        print(json.dumps(dict(initial_ack=retired['membership_certified_passages'],
            next_cycle_ack=resumed['membership_certified_passages'],
            selected=compared['selected_passages'], certified=compared['certified_passages'],
            native_allowed=compared['native_allowed_passages'])), flush=True)
        self.assertEqual(compared['disagreements'], 0)
        self.assertEqual(compared['certified_passages'], len(self.scope['passage_ids']),
                         'completed retirement lost unqueued certification work')

    def test_lost_retirement_ack_keeps_durable_request_for_fresh_native_proof(self):
        self.assertEqual(self.pending_certificates(), [])
        original = retirement_runner.retire_parent_windows

        def lost_ack(*args, **kwargs):
            committed = original(*args, **kwargs)
            error = chunk_retirement.ChunkRetirementError('parent_retirement_unavailable')
            error.committed = committed
            raise error

        with patch.object(retirement_runner, 'retire_parent_windows', side_effect=lost_ack):
            retired = self.run_retirement()
        self.assertTrue(retired['commit_outcome_unknown'])
        self.assertEqual(retired['membership_certified_passages'], 0)
        self.assertEqual(len(self.pending_certificates()), 1)
        self.assertEqual(self.run_retirement()['completed_parent_proofs'], 0)
        self.publisher().project_pending(tenant_id=self.tenant, batch_size=10,
                                        max_batches=1, concurrency=1)
        self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.pending_certificates(), [])

    def test_lost_scheduling_ack_never_starts_retirement_mutation(self):
        retain = membership_remainder.retain_retirement

        def lost_ack(*args, **kwargs):
            row = retain(*args, **kwargs)
            self.assertIsNotNone(row)
            raise OSError('synthetic scheduling commit response lost')

        with (patch.object(membership_remainder, 'retain_retirement', side_effect=lost_ack),
              patch.object(retirement_runner, 'retire_parent_windows',
                           side_effect=AssertionError('no retirement after unknown scheduling ACK'))):
            retired = self.run_retirement()
        self.assertEqual(retired['cleared_utf8_bytes'], 0)
        self.assertFalse(retired['commit_outcome_unknown'])
        self.assertEqual(retired['errors'], {'retirement_certification_admission_unavailable': 1})
        self.assertEqual(len(self.pending_certificates()), 1)
        self.assertEqual(self.run_retirement()['completed_parent_proofs'], 1)
        self.publisher().project_pending(tenant_id=self.tenant, batch_size=10,
                                        max_batches=1, concurrency=1)
        self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.pending_certificates(), [])

    def test_new_publication_before_admission_wins_and_old_retirement_refuses(self):
        retain = membership_remainder.retain_retirement
        saved = []

        def publish_then_retain(*args, **kwargs):
            self.queue()
            with self.store.connect() as connection:
                saved.append(connection.execute('SELECT * FROM canonical_passage_projection_queue '
                    'WHERE tenant_id=%s', (self.tenant,)).fetchone())
            return retain(*args, **kwargs)

        with patch.object(membership_remainder, 'retain_retirement', side_effect=publish_then_retain):
            retired = self.run_retirement()
        self.assertEqual(retired['completed_parent_proofs'], 0)
        self.assertEqual(retired['retired_metadata_chunks'], 0)
        with self.store.connect() as connection:
            current = connection.execute('SELECT * FROM canonical_passage_projection_queue '
                'WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertEqual(current, saved[0])
        self.assertNotEqual(current['reason'], 'certification')

    def test_newer_publication_survives_full_certificate_cleanup(self):
        certify = membership.certify_retired_parent
        saved = []

        def publish_after_proof(*args, **kwargs):
            outcome = certify(*args, **kwargs)
            self.assertEqual(outcome['status'], 'enrolled')
            self.queue()
            with self.store.connect() as connection:
                saved.append(connection.execute('SELECT * FROM canonical_passage_projection_queue '
                    'WHERE tenant_id=%s', (self.tenant,)).fetchone())
            return outcome

        with patch.object(membership, 'certify_retired_parent', side_effect=publish_after_proof):
            retired = self.run_retirement()
        self.assertEqual(retired['completed_parent_proofs'], 1)
        with self.store.connect() as connection:
            current = connection.execute('SELECT * FROM canonical_passage_projection_queue '
                'WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertEqual(current, saved[0])
        self.assertNotEqual(current['reason'], 'certification')

    def test_partial_retirement_keeps_request_busy_until_original_completion(self):
        from dataclasses import replace
        retired = self.run_retirement(limits=replace(
            chunk_retirement.ParentRetirementLimits(), batch_documents=1, max_batches=1))
        self.assertEqual(retired['partial_parents'], 1)
        queued = self.pending_certificates()
        self.assertEqual(len(queued), 1)
        outcome = membership_remainder.resume(self.store, queued[0],
            settings=self.settings(), deadline_at=time.monotonic() + 3)
        self.assertEqual(outcome['status'], 'busy')
        self.assertEqual(outcome['certified_passages'], 0)
        self.assertEqual(len(self.pending_certificates()), 1)
        self.assertEqual(self.run_retirement()['completed_parent_proofs'], 1)
        self.publisher().project_pending(tenant_id=self.tenant, batch_size=10,
                                        max_batches=1, concurrency=1)
        self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.pending_certificates(), [])

    def test_concurrent_publication_queue_insert_wins_without_replacement(self):
        with self.store.connect() as connection:
            manifest = connection.execute('SELECT * FROM canonical_evidence_documents '
                'WHERE tenant_id=%s', (self.tenant,)).fetchone()
        query = membership._query
        entering_insert = threading.Event()
        worker_pid = []

        def observed_query(store, connection, sql, args, deadline):
            if 'INSERT INTO canonical_passage_projection_queue' in sql:
                worker_pid.append(connection.info.backend_pid)
                entering_insert.set()
            return query(store, connection, sql, args, deadline)

        with (ThreadPoolExecutor(max_workers=1) as executor,
              patch.object(membership, '_query', side_effect=observed_query)):
            with self.store.connect() as writer, writer.transaction():
                writer.execute('''INSERT INTO canonical_passage_projection_queue
                    (tenant_id,source_id,logical_document_id,revision,reason)
                    VALUES(%s,%s,%s,%s,'backfill')''',
                    (self.tenant, self.source, manifest['logical_document_id'], manifest['revision']))
                saved = writer.execute('SELECT * FROM canonical_passage_projection_queue '
                    'WHERE tenant_id=%s', (self.tenant,)).fetchone()
                future = executor.submit(membership_remainder.retain_retirement,
                    self.store, scope=dict(tenant_id=self.tenant, source_id=self.source,
                                           principal_id=self.principal),
                    manifest=manifest, settings=self.settings(), deadline_at=time.monotonic()+3)
                self.assertTrue(entering_insert.wait(1))
                blockers = []
                for _ in range(50):
                    blockers = writer.execute('SELECT pg_blocking_pids(%s) AS pids',
                        (worker_pid[0],)).fetchone()['pids']
                    if blockers:
                        break
                    time.sleep(.01)
                self.assertIn(writer.info.backend_pid, blockers)
            self.assertIsNone(future.result(timeout=3))
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT * FROM canonical_passage_projection_queue '
                'WHERE tenant_id=%s', (self.tenant,)).fetchone(), saved)

    def test_pre085_schema_keeps_original_completion_hook(self):
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_passage_projection_queue '
                               'DROP COLUMN certification_checkpoint')
        try:
            retired = self.run_retirement()
            self.assertEqual(retired['completed_parent_proofs'], 1)
            self.assertEqual(retired['membership_certified_passages'], len(self.ids))
            self.assertEqual(self.compare()['disagreements'], 0)
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_passage_projection_queue '
                                   'ADD COLUMN certification_checkpoint jsonb')

    def test_revoked_admission_does_not_clear_bodies(self):
        retain = membership_remainder.retain_retirement

        def revoke_before_request(*args, **kwargs):
            with self.store.connect() as connection:
                connection.execute('DELETE FROM brain_access_grants WHERE tenant_id=%s',
                                   (self.tenant,))
            return retain(*args, **kwargs)

        with patch.object(membership_remainder, 'retain_retirement', side_effect=revoke_before_request):
            retired = self.run_retirement()
        self.assertEqual(retired['retired_metadata_chunks'], 0)
        self.assertEqual(retired['completed_parent_proofs'], 0)
        self.assertEqual(retired['errors'], {'retirement_certification_admission_unavailable': 1})
        self.assertEqual(self.pending_certificates(), [])

    def test_append_to_completed_retirement_keeps_request_until_new_manifest_finishes(self):
        self.assertEqual(self.run_retirement(enabled=False)['completed_parent_proofs'], 1)
        self.queue()
        self.publisher(self.settings(enabled=False)).project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        self.refresh_scope()
        with self.store.connect() as connection:
            state = connection.execute('''SELECT progress.status,
                progress.manifest_artifact_id IS DISTINCT FROM evidence.manifest_artifact_id AS stale
                FROM canonical_chunk_retirement_progress progress
                JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,native_parent_id)
                WHERE progress.tenant_id=%s''', (self.tenant,)).fetchone()
        # Supported native append already rearms retirement; preserve that
        # scheduling fence rather than inventing an alternate completion state.
        self.assertEqual(state, {'status': 'pending', 'stale': True})
        retain = membership_remainder.retain_retirement

        def worker_between_admission_and_retirement(*args, **kwargs):
            row = retain(*args, **kwargs)
            self.assertIsNotNone(row)
            outcome = membership_remainder.resume(self.store, row,
                settings=self.settings(), deadline_at=time.monotonic()+3)
            self.assertEqual(outcome['status'], 'busy')
            self.assertEqual(outcome['certified_passages'], 0)
            return row

        with (patch.object(membership_remainder, 'retain_retirement',
                           side_effect=worker_between_admission_and_retirement),
              patch.object(membership, 'certify_retired_parent',
                           side_effect=TimeoutError('completion hook interrupted'))):
            retired = self.run_retirement()
        self.assertEqual(retired['completed_parent_proofs'], 1)
        self.assertEqual(len(self.pending_certificates()), 1)
        self.publisher().project_pending(tenant_id=self.tenant, batch_size=10,
                                        max_batches=1, concurrency=1)
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(self.pending_certificates(), [])


if __name__ == '__main__':
    suite = unittest.TestSuite(UnqueuedRetirementCertificationTests(name)
        for name in UnqueuedRetirementCertificationTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
