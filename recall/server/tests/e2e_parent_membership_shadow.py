#!/usr/bin/env python3
"""Reader comparison never replaces the native receipt membership decision."""
import time
import unittest
from unittest.mock import patch

from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server import parent_membership_shadow as shadow
from recall_server.receipt_membership import lookup_current_search_membership


class ParentMembershipShadowTests(ParentEnrollmentTests):
    def enable(self, epoch='synthetic-epoch-a'):
        self.retrieval.parent_membership_shadow=shadow.ShadowSettings(enabled=True,binding_epoch=epoch,serving_reserve_ms=100)
        self.store.verified_singleton_receipts=True

    def test_enrolled_comparison_retains_complete_native_proof_and_results(self):
        self.enroll();self.store.verified_singleton_receipts=True
        before=self.search();self.enable()
        with patch('recall_server.receipt_membership.lookup_current_search_membership',wraps=lookup_current_search_membership) as native:
            after=self.search()
        self.assertEqual(after['results'],before['results'])
        self.assertTrue(native.called)
        observed=after['diagnostics']['authority_stages']['parent_membership_shadow']
        self.assertEqual(observed['status'],'compared')
        self.assertEqual(observed['certified_passages'],len(self.ids))
        self.assertEqual(observed['disagreements'],0)
        self.assertGreater(observed['avoidable_receipts'],0)
        self.assertGreaterEqual(observed['elapsed_ms'],0)

    def test_disabled_shadow_never_calls_optional_owner(self):
        self.store.verified_singleton_receipts=True
        with patch.object(shadow,'observe_membership',side_effect=AssertionError('disabled observer')) as observer:
            result=self.search()
        observer.assert_not_called()
        self.assertTrue(result['results'])
        self.assertNotIn('parent_membership_shadow',result['diagnostics']['authority_stages'])

    def test_legacy_reader_explicitly_skips_certificate_comparison(self):
        self.enable();self.store.verified_singleton_receipts=False
        with patch.object(shadow,'observe_membership',side_effect=AssertionError('legacy observer')) as observer:
            result=self.search()
        observer.assert_not_called()
        self.assertTrue(result['results'])
        self.assertEqual(result['diagnostics']['authority_stages']['parent_membership_shadow']['status'],'requires-singleton')

    def test_shadow_requires_epoch_and_positive_serving_reserve(self):
        with self.assertRaises(ValueError):
            shadow.ShadowSettings(enabled=True)
        with self.assertRaises(ValueError):
            shadow.ShadowSettings(enabled=True,binding_epoch='synthetic',serving_reserve_ms=0)

    def test_wrong_epoch_is_fallback_with_identical_native_results(self):
        self.enroll();before=self.search();self.enable('synthetic-new-epoch')
        after=self.search();observed=after['diagnostics']['authority_stages']['parent_membership_shadow']
        self.assertEqual(after['results'],before['results'])
        self.assertEqual(observed['certified_passages'],0)
        self.assertEqual(observed['fallback_passages'],len(self.ids))
        self.assertEqual(observed['avoidable_receipts'],0)

    def test_deliberately_false_certificate_cannot_override_native_rejection(self):
        self.enroll();self.edit();self.enable()
        scope=enrollment._scope(**self.scope,deadline_at=time.monotonic()+3)
        captured=enrollment._capture(self.store,scope,time.monotonic()+3,prove_ownership=False)
        with self.store.connect() as c:
            for row in captured['rows']:
                c.execute('UPDATE canonical_passages SET membership_generation=%s,membership_binding_sha256=%s WHERE tenant_id=%s AND passage_id=%s',
                    (row['generation'],enrollment._binding(row,scope),self.tenant,row['passage_id']))
        after=self.search();observed=after['diagnostics']['authority_stages']['parent_membership_shadow']
        self.assertEqual(after['results'],[])
        self.assertGreater(observed['disagreements'],0)
        self.assertEqual(observed['native_allowed_passages'],0)

    def test_optional_sql_failure_is_contained_and_native_results_survive(self):
        self.enroll();self.store.verified_singleton_receipts=True;before=self.search();self.enable()
        native=self.store._execute_bounded
        def failed(connection,sql,values,deadline):
            if sql==shadow._METADATA_SQL:
                return connection.execute('SELECT * FROM deliberately_missing_shadow_table')
            return native(connection,sql,values,deadline)
        with patch.object(self.store,'_execute_bounded',side_effect=failed):
            after=self.search()
        self.assertEqual(after['results'],before['results'])
        self.assertEqual(after['diagnostics']['authority_stages']['parent_membership_shadow']['status'],'unavailable')
        self.assertEqual(after['diagnostics']['authority_status'],'ok')

    def test_optional_query_deadline_does_not_abort_native_transaction(self):
        self.enroll();self.store.verified_singleton_receipts=True;before=self.search();self.enable()
        self.retrieval.parent_membership_shadow=shadow.ShadowSettings(enabled=True,binding_epoch='synthetic-epoch-a',budget_ms=20,serving_reserve_ms=100)
        native=self.store._execute_bounded
        def delayed(connection,sql,values,deadline):
            if sql==shadow._METADATA_SQL:
                return native(connection,'SELECT pg_sleep(0.1)',(),deadline)
            return native(connection,sql,values,deadline)
        with patch.object(self.store,'_execute_bounded',side_effect=delayed):
            after=self.search()
        self.assertEqual(after['results'],before['results'])
        self.assertEqual(after['diagnostics']['authority_stages']['parent_membership_shadow']['status'],'deadline')
        self.assertEqual(after['diagnostics']['authority_status'],'ok')

    def test_serving_reserve_skips_optional_query(self):
        self.enroll();self.enable();stages={};native=self.store._execute_bounded
        self.retrieval.parent_membership_shadow=shadow.ShadowSettings(enabled=True,binding_epoch='synthetic-epoch-a')
        def guarded(connection,sql,values,deadline):
            self.assertNotEqual(sql,shadow._METADATA_SQL)
            return native(connection,sql,values,deadline)
        with self.store.connect() as c, patch.object(self.store,'_execute_bounded',side_effect=guarded):
            allowed=self.retrieval._authorize_singletons(c,[(self.source,p) for p in self.ids],time.monotonic()+0.5,stages=stages)
        self.assertEqual(len(allowed),len(self.ids))
        self.assertEqual(stages['parent_membership_shadow']['status'],'serving-reserve')

    def test_shadow_and_native_share_snapshot_across_concurrent_edit(self):
        # This snapshot race includes a synchronous writer transaction. Keep its
        # finite budget separate from the retained 300ms deadline regressions.
        self.enterContext(patch.object(self.store, 'search_deadline_ms', 2000))
        self.enroll();self.store.verified_singleton_receipts=True;before=self.search();self.enable()
        native=shadow.observe_membership
        def edited(*args,**kwargs):
            self.edit()
            return native(*args,**kwargs)
        with patch.object(shadow,'observe_membership',side_effect=edited):
            after=self.search()
        self.assertEqual(after['results'],before['results'])
        self.assertEqual(after['diagnostics']['authority_stages']['parent_membership_shadow']['disagreements'],0)
        self.assertEqual(self.search()['results'],[])


if __name__=='__main__':
    suite=unittest.TestSuite(ParentMembershipShadowTests(name) for name in ParentMembershipShadowTests.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
