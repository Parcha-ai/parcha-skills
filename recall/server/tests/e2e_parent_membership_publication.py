#!/usr/bin/env python3
"""Certification follows native publication and cannot turn it into a failure."""
import os
import time
import threading
from contextlib import contextmanager
import unittest
import weakref
from unittest.mock import patch

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


class ParentMembershipPublicationTests(ParentEnrollmentTests):
    def settings(self, *, enabled=True, epoch='synthetic-epoch-a'):
        return enrollment.CertificationSettings(enabled=enabled,binding_epoch=epoch)

    def publisher(self, settings=None):
        return CanonicalPassageProjector(self.store,LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,bound_tenant_id=self.tenant,
            membership_certification=settings or self.settings())

    def queue(self):
        self.ingest('turn/new-publication','Synthetic new published content')
        result=self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(result['failed'],0)

    def refresh_scope(self, epoch='synthetic-epoch-a'):
        with self.store.connect() as c:
            rows=c.execute('SELECT passage_id FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id',(self.tenant,)).fetchall()
        self.scope.update(passage_ids=tuple(row['passage_id'] for row in rows),binding_epoch=epoch)

    def test_successful_publication_certifies_after_releasing_body_and_locks(self):
        self.queue();publisher=self.publisher();native_prepare=publisher._prepare
        native_certify=enrollment.certify_published_passages;references=[]
        native_semaphore=threading.BoundedSemaphore;active=[]
        @contextmanager
        def tracked_slot(*args):
            with native_semaphore(*args):
                active.append(True)
                try:
                    yield
                finally:
                    active.pop()
        def prepare(candidate):
            prepared=native_prepare(candidate);references.append(weakref.ref(prepared));return prepared
        def certify(*args,**kwargs):
            self.assertEqual(active,[])
            self.assertTrue(references)
            self.assertIsNone(references[-1]())
            with self.store.connect() as c:
                lock=c.execute('SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0)) AS acquired',
                    ('lossless-passages\x1f'+kwargs['logical_document_id'],)).fetchone()
                self.assertTrue(lock['acquired'])
            return native_certify(*args,**kwargs)
        with patch.object(publisher,'_prepare',side_effect=prepare), patch.object(enrollment,'certify_published_passages',side_effect=certify), patch('recall_server.passage_index.threading.BoundedSemaphore',side_effect=tracked_slot):
            result=publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.assertEqual(result['documents'],1)
        self.assertGreater(result['membership_certified_passages'],0)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'],len(self.scope['passage_ids']))

    def test_interrupted_hook_keeps_publication_and_queue_committed(self):
        self.queue();publisher=self.publisher()
        with patch.object(enrollment,'certify_published_passages',side_effect=OSError('synthetic interruption')):
            result=publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.assertEqual(result['documents'],1)
        self.assertEqual(result['pending'],0)
        self.assertEqual(result['membership_certified_passages'],0)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assertEqual(self.enroll()['status'],'enrolled')
        self.assertEqual(self.compare()['certified_passages'],len(self.scope['passage_ids']))

    def test_disabled_hook_never_queries_certification(self):
        self.queue();publisher=self.publisher(self.settings(enabled=False))
        with patch.object(enrollment,'certify_published_passages',side_effect=AssertionError('disabled owner called')) as certify:
            result=publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        certify.assert_not_called()
        self.assertEqual(result['documents'],1)

    def test_edit_after_publication_before_proof_keeps_native_fallback(self):
        self.queue();publisher=self.publisher();native=enrollment.certify_published_passages
        def edited(*args,**kwargs):
            self.edit();return native(*args,**kwargs)
        with patch.object(enrollment,'certify_published_passages',side_effect=edited):
            result=publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.assertEqual(result['documents'],1)
        self.assertEqual(result['membership_certified_passages'],0)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assertEqual(self.compare()['disagreements'],0)

    def test_binding_epoch_rotation_invalidates_older_certificates(self):
        self.scope['binding_epoch']='synthetic-epoch-a'
        self.assertEqual(self.enroll()['status'],'enrolled')
        self.scope['binding_epoch']='synthetic-epoch-b'
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assertEqual(self.compare()['fallback_passages'],len(self.ids))
        self.assertEqual(self.enroll()['status'],'enrolled')
        self.assertEqual(self.compare()['certified_passages'],len(self.ids))

    def test_later_batch_deadline_retains_committed_certificate_count(self):
        self.ingest('turn/long-publication','Synthetic evidence paragraph. '*3000)
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        publisher=self.publisher();native=enrollment.enroll_parent_passages;calls=[]
        def interrupted(*args,**kwargs):
            calls.append(True)
            if len(calls)==2:
                raise enrollment.SearchDeadlineExceeded('synthetic second batch deadline')
            return native(*args,**kwargs)
        with patch.dict(enrollment._CERTIFICATION_LIMITS,passages=1), patch.object(enrollment,'enroll_parent_passages',side_effect=interrupted):
            result=publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,concurrency=1)
        self.assertEqual(len(calls),2)
        self.assertEqual(result['documents'],1)
        self.assertEqual(result['pending'],0)
        self.assertEqual(result['membership_certified_passages'],1)
        self.assertEqual(result['membership_failed_documents'],1)
        with self.store.connect() as c:
            certified=c.execute('SELECT count(*) AS n FROM canonical_passages WHERE tenant_id=%s AND membership_generation IS NOT NULL',(self.tenant,)).fetchone()['n']
        self.assertEqual(certified,1)

    def test_expired_enrollment_does_not_checkout_a_connection(self):
        with patch.object(self.store,'connect',side_effect=AssertionError('expired checkout')) as checkout:
            with self.assertRaises(enrollment.SearchDeadlineExceeded):
                enrollment.enroll_parent_passages(self.store,**self.scope,deadline_at=time.monotonic()-1)
        checkout.assert_not_called()

    def test_enabling_requires_explicit_nonempty_epoch(self):
        with self.assertRaises(ValueError):
            self.settings(epoch='')
        with patch.dict(os.environ,{'RECALL_PARENT_MEMBERSHIP_CERTIFICATION':'1','RECALL_PARENT_MEMBERSHIP_EPOCH':''}):
            with self.assertRaises(ValueError):
                enrollment.CertificationSettings.from_env()


if __name__=='__main__':
    suite=unittest.TestSuite(ParentMembershipPublicationTests(name) for name in ParentMembershipPublicationTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
