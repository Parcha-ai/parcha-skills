#!/usr/bin/env python3
"""Missing legacy generation metadata uses the same durable proof queue."""
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment
from recall_server.passage_index import CertificationBudget


class QueueInitializationTests(ParentMembershipPublicationTests):
    def pending(self):
        return remainder.pending(self.store, tenant_id=self.tenant, limit=20,
                                 deadline_at=time.monotonic() + 3)

    def publish_missing_generation(self, *, first=False):
        if first:
            self.source += ':first'
        self.queue()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                               (self.tenant, self.source))
        budget = CertificationBudget()
        with budget.lease():
            result = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1,
                max_batches=1, concurrency=1, certification_budget=budget)
        self.assertEqual(result['documents'], 1)
        return result

    def test_first_publication_survives_restart_before_generation_initialization(self):
        self.publish_missing_generation(first=True)
        pending = self.pending()
        self.assertEqual(len(pending), 1, 'committed publication lost optional proof work')
        self.assertIsNone(pending[0]['checkpoint']['authority_generation'])
        self.assertEqual(pending[0]['checkpoint']['after'], '')
        with patch.object(type(self.publisher().logical_projection), 'read_part',
                          side_effect=AssertionError('certification must not reread source bodies')):
            result = remainder.resume(self.store, pending[0], settings=self.settings(),
                                      deadline_at=time.monotonic() + 3)
        self.assertEqual(result['status'], 'enrolled')
        self.assertGreater(result['certified_passages'], 0)
        self.assertEqual(self.pending(), [])
        self.scope.update(source_id=self.source, passage_ids=())
        with self.store.connect() as connection:
            self.scope['passage_ids'] = tuple(row['passage_id'] for row in connection.execute(
                'SELECT passage_id FROM canonical_passages WHERE tenant_id=%s AND source_id=%s',
                (self.tenant, self.source)))
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(compared['disagreements'], 0)


    def resume(self, row=None):
        return remainder.resume(self.store, row or self.pending()[0],
                                settings=self.settings(), deadline_at=time.monotonic() + 3)

    def generation(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                                      (self.tenant, self.source)).fetchone()

    def test_legacy_reprojection_initializes_without_republishing(self):
        self.publish_missing_generation()
        self.assertIsNone(self.generation())
        result = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1,
                                                  max_batches=1, concurrency=1)
        self.assertEqual(result['documents'], 0)
        self.assertGreater(result['membership_certified_passages'], 0)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))

    def test_missing_generation_with_old_certificate_is_never_reset(self):
        self.publish_missing_generation()
        self.refresh_scope()
        # Model proof COMMIT before cursor ACK, followed by loss of authority
        # metadata. A null scheduling checkpoint is not permission to reset it.
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
            self.assertGreater(connection.execute('SELECT count(*) AS n FROM canonical_passages WHERE tenant_id=%s AND membership_generation IS NOT NULL',
                                                  (self.tenant,)).fetchone()['n'], 0)
        outcome = self.resume()
        self.assertEqual(outcome['status'], 'unknown-generation')
        self.assertEqual(outcome['certified_passages'], 0)
        self.assertIsNone(self.generation())
        self.assertEqual(self.pending(), [])
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'], 0)

    def test_revoked_owner_cannot_initialize_or_certify(self):
        self.publish_missing_generation()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s',
                               (self.tenant, self.principal))
        self.assertEqual(self.resume()['status'], 'unauthorized')
        self.assertIsNone(self.generation())
        self.assertEqual(self.pending(), [])
        self.assertEqual(self.compare()['status'], 'unauthorized')

    def test_revocation_after_initialization_is_rechecked_before_proof(self):
        self.publish_missing_generation()
        initialize = enrollment.initialize_published_parent_generation
        def revoke(*args, **kwargs):
            result = initialize(*args, **kwargs)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s',
                                   (self.tenant, self.principal))
            return result
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=revoke):
            result = self.resume()
        self.assertEqual(result['status'], 'unauthorized')
        self.assertEqual(result['certified_passages'], 0)
        self.assertEqual(self.compare()['status'], 'unauthorized')

    def test_initialized_publication_keeps_original_proof_without_initializer(self):
        self.queue()
        candidate = self.publisher()._pending(tenant_id=self.tenant, limit=1)[0]
        # Additive INSERTs need not create an authority epoch. Establish this
        # test's initialized precondition through the native metadata owner.
        self.assertEqual(enrollment.initialize_published_parent_generation(
            self.store, tenant_id=self.tenant, source_id=self.source,
            logical_document_id=candidate.logical_document_id,
            deadline_at=time.monotonic() + 3), 'initialized')
        self.assertGreater(self.generation()['generation'], 0)
        self.assertEqual(self.compare()['certified_passages'], 0)
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=AssertionError('normal path changed')):
            result = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1,
                                                      max_batches=1, concurrency=1)
        self.assertEqual(result['documents'], 1)
        self.assertGreater(result['membership_certified_passages'], 0)
        self.refresh_scope()
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], compared['native_allowed_passages'])
        self.assertEqual(compared['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(compared['disagreements'], 0)

    def test_normal_publication_initializes_additive_generation_then_proves(self):
        self.queue()
        before = self.generation()
        if self.additive_membership_generation:
            self.assertIsNone(before)
        initialize = enrollment.initialize_published_parent_generation
        with patch.object(enrollment, 'initialize_published_parent_generation', wraps=initialize) as initialized:
            result = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1,
                                                      max_batches=1, concurrency=1)
        self.assertEqual(initialized.call_count, int(before is None))
        self.assertEqual(result['documents'], 1)
        self.assertGreater(result['membership_certified_passages'], 0)
        self.assertEqual(result['membership_failed_documents'], 0)
        self.assertGreater(self.generation()['generation'], 0)
        self.refresh_scope()
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], compared['native_allowed_passages'])
        self.assertEqual(compared['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(compared['disagreements'], 0)

    def test_forget_before_archive_cleanup_keeps_native_refusal(self):
        from recall_server.canonical import CanonicalLifecycleError
        self.publish_missing_generation()
        self.refresh_scope()
        row = self.pending()[0]
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=self.receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-14T00:01:00Z', idempotency_key='null-generation-forget')
        with patch.object(self.archive, 'delete_internal_raw_bounded', side_effect=OSError('archive unavailable')):
            with self.assertRaisesRegex(CanonicalLifecycleError, 'archive_delete_failed'):
                self.plane.forget(request)
        self.assertEqual(self.resume(row)['certified_passages'], 0)
        self.assertEqual(self.compare()['certified_passages'], 0)
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_initializer_commit_lost_response_keeps_queue_for_fresh_proof(self):
        self.publish_missing_generation()
        row = self.pending()[0]
        initialize = enrollment.initialize_published_parent_generation
        def lost(*args, **kwargs):
            self.assertEqual(initialize(*args, **kwargs), 'initialized')
            raise OSError('lost initializer commit response')
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=lost):
            result = self.resume(row)
        self.assertEqual(result, dict(status='failed', certified_passages=0,
            failure=dict(error_class='OSError', sqlstate=None, source_id=row['source_id'],
                logical_document_id=row['logical_document_id'], native_parent_id=None)))
        self.assertEqual(self.pending(), [row])
        self.assertIsNotNone(self.generation())
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=AssertionError('already initialized')):
            self.assertEqual(self.resume()['status'], 'enrolled')
        self.refresh_scope()
        self.assertEqual(self.compare()['disagreements'], 0)
        self.assertEqual(self.compare()['certified_passages'], len(self.scope['passage_ids']))

    def test_initializer_never_holds_the_rotation_queue_lock(self):
        self.publish_missing_generation()
        row = self.pending()[0]
        ready, initialize_now, initialized, release = (threading.Event() for _ in range(4))
        initialize = enrollment.initialize_published_parent_generation
        def observed(*args, **kwargs):
            ready.set()
            self.assertTrue(initialize_now.wait(2))
            result = initialize(*args, **kwargs)
            initialized.set()
            self.assertTrue(release.wait(2))
            return result
        with ThreadPoolExecutor(max_workers=1) as pool:
            with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=observed):
                future = pool.submit(self.resume, row)
                try:
                    self.assertTrue(ready.wait(1))
                    with self.store.connect() as connection, connection.transaction():
                        connection.execute('SELECT 1 FROM canonical_passage_projection_queue WHERE tenant_id=%s FOR UPDATE NOWAIT', (self.tenant,))
                        initialize_now.set()
                        self.assertTrue(initialized.wait(1), 'initializer waited on a queue lock')
                        self.assertIsNotNone(self.generation())
                finally:
                    initialize_now.set()
                    release.set()
                self.assertEqual(future.result(timeout=3)['status'], 'enrolled')

    def test_new_source_publication_wins_after_initialization_before_cas(self):
        self.publish_missing_generation()
        old = self.pending()[0]
        initialize = enrollment.initialize_published_parent_generation
        def concurrent(*args, **kwargs):
            result = initialize(*args, **kwargs)
            self.ingest('turn/concurrent', 'A genuinely newer source publication')
            self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
            return result
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=concurrent):
            result = self.resume(old)
        self.assertEqual(result['status'], 'changed')
        self.assertEqual(result['certified_passages'], 0)
        with self.store.connect() as connection:
            queued = connection.execute('SELECT reason,generation FROM canonical_passage_projection_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertEqual(queued['reason'], 'logical-update')
        self.assertGreater(queued['generation'], old['generation'])
        self.assertEqual(self.publisher().project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)['documents'], 1)

    def test_null_generation_never_trusts_a_nonempty_prefix(self):
        self.publish_missing_generation()
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_passage_projection_queue SET certification_checkpoint=jsonb_set(certification_checkpoint,'{after}','\"psg_ffffffffffffffffffffffffffffffff\"'::jsonb) WHERE tenant_id=%s", (self.tenant,))
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=AssertionError('malformed checkpoint initialized')):
            self.assertEqual(self.resume()['status'], 'changed')
        self.assertIsNone(self.generation())
        self.assertEqual(self.pending(), [])

    def test_failed_initialization_rotates_without_claiming_a_prefix(self):
        self.publish_missing_generation()
        row = self.pending()[0]
        with self.store.connect() as connection:
            before = connection.execute('SELECT changed_at FROM canonical_passage_projection_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()['changed_at']
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=OSError('initializer unavailable')):
            result = self.resume(row)
        self.assertEqual(result, dict(status='failed', certified_passages=0,
            failure=dict(error_class='OSError', sqlstate=None, source_id=row['source_id'],
                logical_document_id=row['logical_document_id'], native_parent_id=None)))
        with self.store.connect() as connection:
            after = connection.execute('SELECT changed_at,certification_checkpoint FROM canonical_passage_projection_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertGreater(after['changed_at'], before)
        self.assertEqual(after['certification_checkpoint'], row['checkpoint'])
        self.assertEqual(self.resume()['status'], 'enrolled')

    def test_repeated_initializer_deadlines_do_not_starve_ready_parent(self):
        self.publish_missing_generation()
        blocked = self.pending()[0]
        self.publish_missing_generation(first=True)
        initialize = enrollment.initialize_published_parent_generation
        attempted = []
        def delayed(*args, **kwargs):
            attempted.append(kwargs['source_id'])
            if kwargs['source_id'] == blocked['source_id']:
                time.sleep(max(0, kwargs['deadline_at'] - time.monotonic()) + .01)
                raise TimeoutError('initializer deadline')
            return initialize(*args, **kwargs)
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=delayed):
            outcomes = [self.publisher().project_pending(tenant_id=self.tenant, batch_size=1,
                max_batches=1, concurrency=1) for _ in range(3)]
        self.assertEqual(attempted, [blocked['source_id'], self.source, blocked['source_id']])
        self.assertEqual([o['documents'] for o in outcomes], [0, 0, 0])
        self.assertGreater(outcomes[1]['membership_certified_passages'], 0)
        self.assertEqual(outcomes[0]['membership_certified_passages'], 0)
        self.assertEqual(outcomes[2]['membership_certified_passages'], 0)
        self.assertTrue(all(o['membership_elapsed_ms'] < 4000 for o in outcomes))
        self.assertEqual(self.pending(), [blocked])
        self.assertEqual(self.resume()['status'], 'enrolled')

    def test_expired_budget_keeps_initialization_pending(self):
        self.publish_missing_generation()
        before = self.pending()
        with patch.object(enrollment, 'initialize_published_parent_generation', side_effect=AssertionError('expired initializer')):
            result = remainder.resume(self.store, before[0], settings=self.settings(), deadline_at=time.monotonic() - 1)
        self.assertEqual(result['status'], 'deadline')
        self.assertEqual(self.pending(), before)
        self.assertIsNone(self.generation())

    def test_fresh_uninitialized_parent_cannot_preempt_older_durable_work(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        self.queue()
        budget = CertificationBudget()
        with budget.lease():
            self.publisher().project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1,
                                             concurrency=1, certification_budget=budget)
        old = self.pending()[0]
        self.source += ':fresh'
        self.queue()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
        certify = enrollment.certify_published_passages
        proved = []
        native_resume = remainder.resume
        def observed(*args, **kwargs):
            proved.append(kwargs['source_id'])
            return certify(*args, **kwargs)
        def slow(*args, **kwargs):
            result = native_resume(*args, **kwargs)
            time.sleep(max(0, kwargs['deadline_at'] - time.monotonic()) + .01)
            return result
        with patch.object(enrollment, 'certify_published_passages', side_effect=observed), patch.object(remainder, 'resume', side_effect=slow):
            outcome = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual(outcome['documents'], 1)
        self.assertEqual(proved, [old['source_id']])
        self.assertGreater(outcome['membership_certified_passages'], 0)
        self.assertLess(outcome['membership_elapsed_ms'], 3300)
        self.assertIsNone(self.generation())
        # A later cycle initializes and proves the new row without republishing.
        outcome = self.publisher().project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1)
        self.assertEqual(outcome['documents'], 0)
        self.assertGreater(outcome['membership_certified_passages'], 0)


if __name__ == '__main__':
    suite = unittest.TestSuite(QueueInitializationTests(name)
        for name in QueueInitializationTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
