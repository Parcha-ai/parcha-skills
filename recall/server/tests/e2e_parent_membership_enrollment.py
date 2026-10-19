#!/usr/bin/env python3
"""Explicit stable-parent enrollment; certificate columns are test fixtures only."""
from concurrent.futures import ThreadPoolExecutor
import time
import unittest
from unittest.mock import patch

from e2e_singleton_search_authority import SingletonSearchTests
from recall_server import parent_membership_enrollment as enrollment


class ParentEnrollmentTests(SingletonSearchTests):
    def setUp(self):
        super().setUp()
        with self.store.connect() as c:
            c.execute('ALTER TABLE canonical_passages ADD COLUMN IF NOT EXISTS membership_generation bigint, ADD COLUMN IF NOT EXISTS membership_binding_sha256 text')
        self.prepare_search(mixed=True)
        with self.store.connect() as c:
            c.execute("INSERT INTO brain_organizations(organization_id,organization_kind,display_name) VALUES(%s,'company','Synthetic enrollment')",(self.tenant,))
            c.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','synthetic')",(self.tenant,self.tenant))
            c.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES(%s,%s,'owner') ON CONFLICT DO NOTHING",(self.tenant,self.principal))
        with self.store.connect() as c:
            self.ids=tuple(row['passage_id'] for row in c.execute('SELECT passage_id FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id',(self.tenant,)).fetchall())
        with self.store.connect() as c:
            self.additive_membership_generation = c.execute(
                'SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=92) AS enabled'
            ).fetchone()['enabled']
        self.scope=dict(tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
                        native_parent_id='session',passage_ids=self.ids,binding_epoch='synthetic-epoch-a',
                        limits=dict(passages=32,receipt_references=1000,metadata_bytes=1024*1024))

    def enroll(self):
        return enrollment.enroll_parent_passages(self.store,**self.scope,deadline_at=time.monotonic()+3)

    def compare(self):
        return enrollment.compare_parent_membership(self.store,**self.scope,deadline_at=time.monotonic()+3)

    def test_unknown_then_exact_enrollment_shadow_agrees(self):
        before=self.compare()
        self.assertEqual(before['certified_passages'],0)
        self.assertEqual(before['fallback_passages'],len(self.ids))
        self.assertEqual(self.enroll()['status'],'enrolled')
        after=self.compare()
        self.assertEqual(after['certified_passages'],len(self.ids))
        self.assertEqual(after['native_allowed_passages'],len(self.ids))
        self.assertEqual(after['disagreements'],0)

    def test_edit_invalidates_old_certificate(self):
        self.enroll()
        self.edit()
        result=self.compare()
        self.assertEqual(result['certified_passages'],0)
        self.assertEqual(result['disagreements'],0)

    def test_edit_between_capture_and_publication_cannot_stamp_new_generation(self):
        original=enrollment._capture
        def changed(*args,**kwargs):
            captured=original(*args,**kwargs)
            self.edit()
            return captured
        with patch.object(enrollment,'_capture',side_effect=changed):
            self.assertEqual(self.enroll()['status'],'changed')
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_receipt_mutation_under_same_passage_identity_falls_back(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_passages SET receipts=ARRAY[%s] WHERE tenant_id=%s',
                      ('recall://'+self.source+'/missing?rev=1#item=0',self.tenant))
        result=self.compare()
        self.assertEqual(result['certified_passages'],0)
        self.assertEqual(result['native_allowed_passages'],0)

    def test_foreign_actual_owner_is_not_enrolled(self):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET native_parent_id='other-parent' WHERE tenant_id=%s",(self.tenant,))
        self.assertEqual(self.enroll()['status'],'ownership-unproved')
        self.assertEqual(self.compare()['native_allowed_passages'],len(self.ids))
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_grant_revocation_denies_even_with_valid_certificate(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s',(self.tenant,self.principal))
        self.assertEqual(self.compare()['status'],'unauthorized')
        self.assertEqual(self.enroll()['status'],'unauthorized')

    def test_missing_requested_passage_never_partially_enrolls(self):
        self.scope['passage_ids']=(*self.ids,'psg_'+'0'*32)
        self.assertEqual(self.enroll()['status'],'incomplete')

    def test_body_and_locator_changes_do_not_invalidate(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s",(self.tenant,))
            c.execute("UPDATE canonical_documents SET text_redacted='',body_record_ordinal=0,body_record_count=1 WHERE tenant_id=%s",(self.tenant,))
        result=self.compare()
        self.assertEqual(result['certified_passages'],len(self.ids))
        self.assertEqual(result['disagreements'],0)

    def test_missing_generation_initializes_only_without_existing_certificates(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assertEqual(self.enroll()['status'],'enrolled')
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s',(self.tenant,))
        self.assertEqual(self.enroll()['status'],'unknown-generation')
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_manifest_change_without_canonical_edit_falls_back(self):
        self.enroll()
        with self.store.connect() as c:
            c.execute("UPDATE canonical_evidence_documents SET manifest_content_sha256=%s WHERE tenant_id=%s",('a'*64,self.tenant))
        result=self.compare()
        self.assertEqual(result['certified_passages'], len(self.ids) if self.additive_membership_generation else 0)
        self.assertEqual(result['native_allowed_passages'],len(self.ids))
        self.assertEqual(result['disagreements'],0)
        from e2e_parent_membership_fast_read import ParentMembershipFastReadTests
        from recall_server.parent_membership_read import ReadSettings
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.retrieval.parent_membership_read = ReadSettings(enabled=False)
            native = ParentMembershipFastReadTests.search(self)
            self.retrieval.parent_membership_read = ReadSettings(enabled=True,
                binding_epoch=self.scope['binding_epoch'])
            certified = ParentMembershipFastReadTests.search(self)
        self.assertTrue(native['results'])
        self.assertEqual(certified['results'],native['results'])
        self.assertEqual(certified['arms'],native['arms'])

    def test_live_grant_revoked_between_proof_and_publication(self):
        original=enrollment._capture
        def changed(*args,**kwargs):
            captured=original(*args,**kwargs)
            with self.store.connect() as c:
                c.execute('DELETE FROM brain_access_grants WHERE tenant_id=%s',(self.tenant,))
            return captured
        with patch.object(enrollment,'_capture',side_effect=changed):
            self.assertEqual(self.enroll()['status'],'unauthorized')

    def test_admitted_singletons_and_profile_error_keep_native_behavior(self):
        from recall_server.receipt_membership import ReceiptMembershipError
        for receipt in (self.receipt,self.physical_receipt):
            self.admit_fixture(self.baseline(receipt)['document_id'])
        self.assertEqual(self.enroll()['status'],'enrolled')
        result=self.compare()
        self.assertEqual(result['certified_passages'],len(self.ids))
        self.assertEqual(result['disagreements'],0)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s',(self.tenant,))
        with self.assertRaises(ReceiptMembershipError):
            self.compare()
        with self.assertRaises(ReceiptMembershipError):
            self.enroll()

    def test_forget_revocation_precedes_archive_cleanup_failure(self):
        from recall_server.canonical import CanonicalLifecycleError
        self.enroll()
        request=dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            target_receipt=self.receipt,mode='explicit_forget',reason='owner_requested',
            requested_at='2026-10-14T00:01:00Z',idempotency_key='enrollment-forget')
        with patch.object(self.archive,'delete_internal_raw_bounded',side_effect=OSError('synthetic archive failure')):
            with self.assertRaisesRegex(CanonicalLifecycleError,'archive_delete_failed'):
                self.plane.forget(request)
        result=self.compare()
        self.assertEqual(result['certified_passages'],0)
        self.assertEqual(result['disagreements'],0)

    def test_expired_deadline_does_not_publish(self):
        from recall_server.db import SearchDeadlineExceeded
        with self.assertRaises(SearchDeadlineExceeded):
            enrollment.enroll_parent_passages(self.store,**self.scope,deadline_at=time.monotonic()-1)
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_shadow_native_comparison_retains_one_snapshot(self):
        self.enroll()
        original=enrollment.lookup_current_search_membership
        def changed(*args,**kwargs):
            self.edit()
            return original(*args,**kwargs)
        with patch.object(enrollment,'lookup_current_search_membership',side_effect=changed):
            result=self.compare()
        self.assertEqual(result['certified_passages'],len(self.ids))
        self.assertEqual(result['native_allowed_passages'],len(self.ids))
        self.assertEqual(result['disagreements'],0)
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_enrollment_publication_does_not_wait_for_authority_row_lock(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with ThreadPoolExecutor(1) as pool:
            with self.store.connect() as c:
                c.execute('UPDATE canonical_parent_authority SET generation=generation+1 WHERE tenant_id=%s',(self.tenant,))
                pending=pool.submit(self.enroll)
                self.assertEqual(pending.result(2)['status'],'enrolled')
        # The writer commits afterward. The saved generation is the earlier
        # capture, so the just-published certificate is now a fallback.
        result=self.compare()
        self.assertEqual(result['certified_passages'],0)
        self.assertEqual(result['native_allowed_passages'],len(self.ids))
        self.assertEqual(result['disagreements'],0)

    def test_shadow_always_runs_original_verifier_even_when_certified(self):
        self.enroll()
        with patch.object(enrollment,'lookup_current_search_membership',wraps=enrollment.lookup_current_search_membership) as native:
            result=self.compare()
        self.assertEqual(result['certified_passages'],len(self.ids))
        self.assertEqual(native.call_count,1)
        self.assertEqual(set(native.call_args.kwargs['receipts']),{self.receipt,self.physical_receipt})

    def test_explicit_job_budget_refuses_whole_scope_without_partial_certificate(self):
        for key in ('receipt_references','metadata_bytes'):
            with self.subTest(budget=key):
                limits=dict(self.scope['limits']);limits[key]=1
                with patch.dict(self.scope,limits=limits):
                    result=self.enroll()
                    self.assertEqual(result,dict(status='budget-exceeded',enrolled_passages=0))
                    self.assertEqual(self.compare(),dict(status='budget-exceeded',compared_passages=0))
                self.assertEqual(self.compare()['certified_passages'],0)
        with patch.dict(self.scope,passage_ids=self.ids*33):
            self.assertEqual(self.enroll()['status'],'budget-exceeded')

    def test_actual_source_deletion_recreation_retains_generation_and_loses_certificate(self):
        from psycopg import sql
        self.enroll()
        with self.store.connect() as c:
            old=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                          (self.tenant,self.source)).fetchone()['generation']
            # Disposable test database only: exercise real FK ordering and normal
            # triggers, never disable them or use TRUNCATE to simulate cleanup.
            for table in ('canonical_evidence_documents','canonical_evidence_objects',
                          'canonical_chunks','canonical_documents','canonical_events',
                          'canonical_ingest_jobs','raw_artifacts','canonical_source_grants',
                          'canonical_audit_events','canonical_evidence_document_queue',
                          'canonical_evidence_cleanup_queue','canonical_sources'):
                c.execute(sql.SQL('DELETE FROM {} WHERE tenant_id=%s AND source_id=%s').format(sql.Identifier(table)),
                          (self.tenant,self.source))
        with self.store.connect() as c:
            retained=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                               (self.tenant,self.source)).fetchone()['generation']
            self.assertGreater(retained,old)
            c.execute('INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id) VALUES(%s,%s,%s)',
                      (self.tenant,self.source,self.principal))
            c.execute("INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,%s,'owner')",
                      (self.tenant,self.source,self.principal))
        self.ingest('turn/search','Synthetic recreated source')
        with self.store.connect() as c:
            current=c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s',
                              (self.tenant,self.source)).fetchone()['generation']
        if self.additive_membership_generation:
            self.assertEqual(current,retained)
        else:
            self.assertGreater(current,retained)
        self.assertEqual(self.compare()['status'],'incomplete')
        self.assertEqual(self.search()['results'],[])


if __name__=='__main__':
    suite=unittest.TestSuite(ParentEnrollmentTests(name) for name in ParentEnrollmentTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
