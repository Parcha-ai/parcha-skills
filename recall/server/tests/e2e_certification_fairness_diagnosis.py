#!/usr/bin/env python3
"""Distinguish native proof fairness from pre-rotation metadata cancellation."""
import json
import time
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from e2e_membership_remainder import MembershipRemainderTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server import membership_remainder as remainder
from recall_server.passage_index import CertificationBudget


class CertificationFairness(MembershipRemainderTests):
    def four_parents(self):
        self.interrupted_publication()
        head = self.queued()[0]
        ingest = self.plane.ingest_document
        for index in range(3):
            def independent(*, _index=index, **kwargs):
                kwargs['envelope']['native_parent_id'] = f'ready-{_index}'
                return ingest(**kwargs)
            with patch.object(self.plane, 'ingest_document', side_effect=independent):
                self.ingest(f'turn/ready-{index}', f'Synthetic complete ready evidence {index}.')
        projected = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        self.assertEqual(projected['failed'], 0)
        with patch.object(enrollment, 'enroll_parent_passages', side_effect=OSError('fixture hold')):
            self.run_cycle()
        queued = self.queued()
        self.assertEqual(len(queued), 4)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_passage_projection_queue SET changed_at=now()-interval '1 minute' "
                "WHERE tenant_id=%s AND logical_document_id=%s", (self.tenant, head['logical_document_id']))
        self.assertEqual(self.queued()[0]['logical_document_id'], head['logical_document_id'])
        return head, {r['logical_document_id'] for r in queued} - {head['logical_document_id']}

    def run_window(self):
        # Actual production allowance owner: exactly three seconds, never renewed inside a drain.
        budget = CertificationBudget()
        started = time.monotonic()
        result = self.publisher().certify_pending(tenant_id=self.tenant, batch_size=4,
            certification_budget=budget)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 4.0)
        return result, elapsed

    def test_native_proof_timeout_rotates_and_three_ready_parents_progress(self):
        head, ready = self.four_parents()
        original = enrollment.certify_published_passages
        attempted = []
        def timeout_head(store, **kwargs):
            attempted.append(kwargs['logical_document_id'])
            if kwargs['logical_document_id'] == head['logical_document_id']:
                with enrollment._connect(store, kwargs['deadline_at']) as connection:
                    store._execute_bounded(connection, 'SELECT pg_sleep(5)', (), kwargs['deadline_at'])
            return original(store, **kwargs)
        with patch.object(enrollment, 'certify_published_passages', side_effect=timeout_head):
            first, first_elapsed = self.run_window()
            self.assertEqual(first['membership_failures'][0]['sqlstate'], '57014')
            self.assertNotEqual(self.queued()[0]['logical_document_id'], head['logical_document_id'])
            second, second_elapsed = self.run_window()
        self.assertEqual({r['logical_document_id'] for r in self.queued()}, {head['logical_document_id']})
        self.assertTrue(ready.issubset(attempted))
        self.assertGreaterEqual(second['membership_certified_passages'], 3)
        print(json.dumps(dict(mechanism='proof_timeout_after_rotation', ready_completed=3,
            first=first, second=second, elapsed=[first_elapsed, second_elapsed])), flush=True)

    def test_current_query_timeout_rotates_without_proof_progress_and_allows_three_ready(self):
        head, ready = self.four_parents()
        original = self.store._execute_bounded_pipeline
        attempted = []
        def timeout_current(connection, statement, parameters, deadline):
            if statement == remainder.CURRENT:
                attempted.append(parameters[2])
                if parameters[2] == head['logical_document_id']:
                    return original(connection, 'SELECT pg_sleep(5)', (), deadline)
            return original(connection, statement, parameters, deadline)
        outcomes = []
        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=timeout_current):
            for window in range(2):
                outcome, elapsed = self.run_window()
                self.assertEqual(outcome['membership_failures'][0]['sqlstate'], '57014')
                self.assertEqual(outcome['membership_certified_passages'], 0 if window == 0 else 3)
                saved = next(row for row in self.queued() if row['logical_document_id'] == head['logical_document_id'])
                self.assertEqual(saved['checkpoint'], head['checkpoint'], 'cancellation acknowledged proof progress')
                outcomes.append(dict(result=outcome, elapsed=elapsed))
        self.assertTrue(ready.issubset(attempted))
        self.assertEqual({r['logical_document_id'] for r in self.queued()}, {head['logical_document_id']})
        recovery, _ = self.run_window()
        self.assertEqual(self.queued(), [])
        self.assertGreaterEqual(recovery['membership_certified_passages'], 1)
        print(json.dumps(dict(mechanism='current_query_timeout_before_rotation', ready_completed_during_fault=3,
            windows=outcomes, recovery_certified=recovery['membership_certified_passages'])), flush=True)


    def test_lost_rotation_ack_changes_no_proof_checkpoint_and_restart_recovers(self):
        self.interrupted_publication()
        before = self.queued()
        original = self.store._execute_bounded_pipeline
        connect = enrollment._connect
        rotated = []
        lost = []
        def observe_rotation(connection, statement, parameters, deadline):
            result = original(connection, statement, parameters, deadline)
            if 'SET changed_at=clock_timestamp()' in statement:
                rotated.append(True)
            return result
        @contextmanager
        def lose_committed_ack(store, deadline):
            with connect(store, deadline) as connection:
                yield connection
            if rotated and not lost:
                lost.append(True)
                raise OSError('synthetic lost scheduling commit acknowledgement')
        with patch.object(self.store, '_execute_bounded_pipeline', side_effect=observe_rotation), \
                patch.object(enrollment, '_connect', side_effect=lose_committed_ack):
            result, _ = self.run_window()
        self.assertEqual(lost, [True])
        self.assertEqual(result['membership_failed_documents'], 1)
        self.assertEqual(result['membership_certified_passages'], 0)
        self.assertEqual(self.queued(), before)
        self.assertEqual(self.compare()['certified_passages'], 0)
        resumed, _ = self.run_window()
        self.assertGreater(resumed['membership_certified_passages'], 0)
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.compare()['disagreements'], 0)

    def test_sql_null_checkpoint_preserves_original_terminal_cleanup(self):
        self.interrupted_publication()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passage_projection_queue '
                'SET certification_checkpoint=NULL WHERE tenant_id=%s', (self.tenant,))
        with patch.object(enrollment, 'certify_published_passages', side_effect=AssertionError('invalid proof')):
            result, _ = self.run_window()
        self.assertEqual(result['membership_certified_passages'], 0)
        self.assertEqual(result['membership_deferred_documents'], 1)
        self.assertEqual(self.queued(), [])


if __name__ == '__main__':
    suite = unittest.TestSuite(CertificationFairness(name) for name in CertificationFairness.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
