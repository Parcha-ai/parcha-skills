#!/usr/bin/env python3
"""A locked publication queue row cannot block ready certification siblings."""
import time
import unittest
from unittest.mock import patch

from e2e_certification_selection_batch import CertificationSelectionTests
from recall_server import membership_remainder as remainder
from recall_server.passage_index import CertificationBudget


class QueueLockProgress(CertificationSelectionTests):
    def check_locked_oldest(self, batch_size):
        parents = self.ready_parents()
        publisher = self.publisher()
        oldest = next(p for p in parents if p['source_id'] == self.source)
        native_ingest = self.plane.ingest_document

        def independent(**kwargs):
            kwargs['envelope']['native_parent_id'] = 'ready-same-source'
            return native_ingest(**kwargs)

        with patch.object(self.plane, 'ingest_document', side_effect=independent):
            self.ingest('turn/ready-sibling', 'Synthetic independent ready sibling evidence.')
        self.assertEqual(self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1,
            upload_concurrency=1)['failed'], 0)
        budget = CertificationBudget()
        with budget.lease():
            self.assertEqual(publisher.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1,
                concurrency=1, certification_budget=budget)['documents'], 1)
        with self.store.connect() as connection:
            self.ids = tuple(row['passage_id'] for row in connection.execute(
                """SELECT passage_id FROM canonical_passages
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                ORDER BY passage_id""", remainder.key(oldest)).fetchall())
            self.scope['passage_ids'] = self.ids
            connection.execute("""UPDATE canonical_passage_projection_queue
                SET changed_at=now()-interval '1 day'
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                remainder.key(oldest))

        def queued():
            with self.store.connect() as connection:
                return connection.execute("""SELECT * FROM canonical_passage_projection_queue
                    WHERE tenant_id=%s ORDER BY source_id,logical_document_id""",
                    (self.tenant,)).fetchall()

        before = next(row for row in queued() if remainder.key(row) == remainder.key(oldest))
        with self.store.connect() as blocker, blocker.transaction():
            blocker.execute("""SELECT 1 FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s FOR UPDATE""",
                remainder.key(oldest))
            # Three unchanged allowances cover the ready same-source parent and
            # both other sources even when the selector admits just one row.
            for _ in range(3):
                outcome = publisher.certify_pending(
                    tenant_id=self.tenant, batch_size=batch_size,
                    certification_budget=CertificationBudget())
                self.assertEqual(outcome['membership_failed_documents'], 0, outcome)
            self.assertEqual(queued(), [before],
                'Ready siblings must complete while the locked row stays byte-for-byte unchanged.')
            self.assertEqual(self.compare()['certified_passages'], 0)
        # Selection made no claim on the skipped row. It remains independently
        # resumable, and original native proof runs once the publisher releases it.
        outcome = publisher.certify_pending(
            tenant_id=self.tenant, batch_size=batch_size,
            certification_budget=CertificationBudget())
        self.assertEqual(outcome['membership_failed_documents'], 0, outcome)
        self.assertEqual(queued(), [])
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], len(self.ids))
        self.assertEqual(compared['disagreements'], 0)

    def test_ready_sibling_progresses_while_oldest_queue_row_is_locked(self):
        self.check_locked_oldest(10)

    def test_single_row_allowance_skips_locked_oldest_and_wraps(self):
        self.check_locked_oldest(1)


if __name__ == '__main__':
    suite = unittest.TestSuite(QueueLockProgress(name) for name in (
        'test_ready_sibling_progresses_while_oldest_queue_row_is_locked',
        'test_single_row_allowance_skips_locked_oldest_and_wraps',
    ))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
