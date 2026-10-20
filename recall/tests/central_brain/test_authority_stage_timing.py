"""Content-free serving timings preserve the existing authority boundary."""
from contextlib import contextmanager
import json
import math
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from psycopg_pool import PoolTimeout
from recall_server.db import SearchDeadlineExceeded
from tests.central_brain import test_turbopuffer_forget_fence as fixtures


class AuthorityStageTimingTests(unittest.TestCase):
    def test_existing_proof_operations_have_separate_numeric_timings(self):
        retrieval, ns, legs, results, _, row = fixtures.ForgetAuthorityTests().fixture()
        row['receipts'] = ['private-receipt-a', 'private-receipt-a', 'private-receipt-b']
        store = retrieval.store
        store.verified_singleton_receipts = True
        clock = [10.0]
        events = []

        class Connection:
            @contextmanager
            def transaction(self):
                events.append('transaction-enter')
                clock[0] += .02
                yield
                events.append('transaction-exit')
                clock[0] += .06

            def execute(self, statement):
                self_statement = 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
                if statement != self_statement:
                    raise AssertionError(statement)
                events.append('isolation')

        connection = Connection()

        @contextmanager
        def connect():
            events.append('checkout')
            clock[0] += .01
            yield connection
            events.append('checkin')
            clock[0] += .07

        def execute(conn, sql, values, deadline):
            self.assertIs(conn, connection)
            self.assertEqual(deadline, 100.0)
            self.assertEqual(values[-2:], ([row['source_id']], [row['passage_id']]))
            events.append('catalog')
            clock[0] += .03
            def fetchall():
                events.append('fetch')
                clock[0] += .04
                return [row]
            return SimpleNamespace(fetchall=fetchall)

        def membership(owner, **kwargs):
            self.assertIs(owner, store)
            self.assertIs(kwargs['connection'], connection)
            self.assertEqual(kwargs['deadline_at'], 100.0)
            self.assertEqual(kwargs['receipts'], ('private-receipt-a', 'private-receipt-b'))
            events.append('membership')
            clock[0] += .05
            return {(row['source_id'], receipt) for receipt in kwargs['receipts']}

        store.connect, store._execute_bounded_pipeline = connect, execute
        with patch('recall_server.turbopuffer_retrieval.time.monotonic', side_effect=lambda: clock[0]), \
                patch('recall_server.receipt_membership.lookup_current_search_membership', side_effect=membership):
            diagnostics = retrieval._authorize_ranges(results, legs, deadline_at=100.0)
        self.assertEqual(diagnostics['authority_status'], 'ok')
        self.assertEqual(len(results), 1)
        self.assertEqual(events, ['checkout', 'transaction-enter', 'isolation', 'catalog',
                                  'fetch', 'membership', 'transaction-exit', 'checkin'])
        self.assertEqual(diagnostics['authority_stages'], dict(
            path='singleton', checkout_ms=10.0, catalog_ms=70.0, receipt_prepare_ms=0.0,
            membership_ms=50.0, transaction_exit_ms=60.0, selected_passages=1,
            receipt_references=3, unique_receipts=2, allowed_receipts=2))
        self.assertEqual(diagnostics['authority_elapsed_ms'], 280.0)
        self.assertEqual(ns.queries, [])

    def test_failed_stages_remain_unobserved_and_never_disclose_vendor_body(self):
        for failure in ('checkout', 'catalog', 'membership', 'transaction-exit'):
            with self.subTest(failure=failure):
                retrieval, ns, legs, results, _, row = fixtures.ForgetAuthorityTests().fixture()
                store = retrieval.store
                store.verified_singleton_receipts = True
                events = []

                class Connection:
                    @contextmanager
                    def transaction(self):
                        events.append('transaction-enter')
                        try:
                            yield
                        finally:
                            events.append('transaction-exit')
                        if failure == 'transaction-exit':
                            raise RuntimeError('private-transaction-error')

                    def execute(self, statement):
                        events.append('isolation')

                @contextmanager
                def connect():
                    if failure == 'checkout':
                        raise PoolTimeout('private-checkout-error')
                    events.append('checkout')
                    try:
                        yield Connection()
                    finally:
                        events.append('checkin')

                def execute(*args):
                    if failure == 'catalog':
                        raise SearchDeadlineExceeded()
                    return SimpleNamespace(fetchall=lambda: [row])

                def membership(*args, **kwargs):
                    if failure == 'membership':
                        raise RuntimeError('private-membership-error')
                    return {(row['source_id'], receipt) for receipt in row['receipts']}

                store.connect, store._execute_bounded_pipeline = connect, execute
                with patch('recall_server.receipt_membership.lookup_current_search_membership', side_effect=membership):
                    diagnostics = retrieval._hydrate_ranges(results, legs, deadline_at=time.monotonic()+5)
                self.assertEqual(diagnostics['authority_status'],
                                 'deadline-exceeded' if failure == 'catalog' else 'unavailable')
                self.assertEqual(results, [])
                self.assertEqual(legs[0][2], [])
                self.assertEqual(ns.queries, [])
                stage = diagnostics['authority_stages']
                self.assertIsNone(stage['transaction_exit_ms'])
                if failure == 'checkout':
                    self.assertEqual(events, [])
                    self.assertIsNone(stage['checkout_ms'])
                else:
                    self.assertEqual(events.count('checkout'), 1)
                    self.assertEqual(events.count('checkin'), 1)
                if failure in ('checkout', 'catalog'):
                    self.assertIsNone(stage['catalog_ms'])
                    self.assertIsNone(stage['selected_passages'])
                if failure != 'transaction-exit':
                    self.assertIsNone(stage['membership_ms'])
                    self.assertIsNone(stage['allowed_receipts'])
                self.assertNotIn('private-', json.dumps(diagnostics))
                self.assertEqual(set(stage), {'path', 'checkout_ms', 'catalog_ms',
                    'receipt_prepare_ms', 'membership_ms', 'transaction_exit_ms',
                    'selected_passages', 'receipt_references', 'unique_receipts', 'allowed_receipts'})
                self.assertTrue(all(value is None or isinstance(value, (int, float))
                                    and math.isfinite(value) and value >= 0
                                    for key, value in stage.items() if key != 'path'))

    def test_legacy_empty_and_expired_paths_do_not_invent_substage_observations(self):
        for mode in ('legacy', 'empty', 'expired'):
            with self.subTest(mode=mode):
                retrieval, _, legs, results, calls, _ = fixtures.ForgetAuthorityTests().fixture()
                if mode == 'empty':
                    results.clear()
                diagnostics = retrieval._authorize_ranges(results, legs,
                    deadline_at=time.monotonic() + (-1 if mode == 'expired' else 5))
                stage = diagnostics['authority_stages']
                self.assertEqual(stage['path'], {'legacy': 'legacy', 'empty': 'empty',
                                                'expired': 'unobserved'}[mode])
                self.assertEqual(len(calls), int(mode == 'legacy'))
                self.assertTrue(all(value is None for key, value in stage.items()
                                    if key not in ('path', 'checkout_ms')))


if __name__ == '__main__':
    unittest.main()
