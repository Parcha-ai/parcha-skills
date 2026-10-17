#!/usr/bin/env python3
"""Certification metadata selection does not inherit body preparation concurrency."""
import time
import unittest
from unittest.mock import patch

from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment
from recall_server.passage_index import CertificationBudget
from recall_server.projection_worker import run_projection_worker


class CertificationSelectionTests(ParentMembershipPublicationTests):
    def ready_parents(self):
        self.queue()
        original_source = self.source
        for suffix in ("second", "third"):
            self.source = original_source + ":" + suffix
            self.ingest("turn/ready", "Synthetic independent source evidence " + suffix)
        self.source = original_source
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1
        )
        # Publish all three through the native owner, retaining its durable work.
        # The held optional lease does not postpone source publication.
        budget = CertificationBudget()
        with budget.lease():
            published = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1,
                concurrency=2, certification_budget=budget,
            )
        self.assertEqual(published["documents"], 3)
        rows = remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        )
        self.assertEqual(len(rows), 3)
        return rows

    def worker_cycle(self, publisher, *, batch_size=100):
        class IdleLogical:
            def project_pending(self, **kwargs):
                return dict(status="complete", documents=0, records=0,
                            batches=0, pending=0, pruned=0, cleanup_failures=0)

        return run_projection_worker(
            IdleLogical(), publisher, tenant_id=self.tenant,
            logical_batch_size=100, passage_batch_size=batch_size, embedding_batch_size=1,
            max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=2,
            interval_seconds=30, once=True, skip_embedding=True,
        )

    def test_three_ready_parents_use_one_budget_with_two_body_slots(self):
        parents = self.ready_parents()
        publisher = self.publisher()
        native_resume = remainder.resume
        deadlines = []

        def resume(*args, **kwargs):
            deadlines.append(kwargs["deadline_at"])
            return native_resume(*args, **kwargs)

        with patch.object(remainder, "resume", side_effect=resume), patch.object(
            publisher.logical_projection, "read_part",
            side_effect=AssertionError("certification must not read source bodies"),
        ):
            outcome = self.worker_cycle(publisher)
        self.assertEqual(outcome["passage_documents"], 0)
        self.assertEqual(outcome["membership_failed_documents"], 0)
        remaining = remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        )
        self.assertEqual(remaining, [], "third ready parent lost unused certification budget")
        self.assertEqual(len(deadlines), 3)
        self.assertEqual(len(set(deadlines)), 1)
        for parent in parents:
            with self.store.connect() as connection:
                ids = tuple(row["passage_id"] for row in connection.execute(
                    "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s "
                    "AND source_id=%s AND logical_document_id=%s ORDER BY passage_id",
                    remainder.key(parent),
                ).fetchall())
            compared = enrollment.compare_parent_membership(
                self.store, **dict(self.scope, source_id=parent["source_id"], passage_ids=ids),
                deadline_at=time.monotonic() + 3,
            )
            self.assertEqual(compared["certified_passages"], len(ids))
            self.assertEqual(compared["native_allowed_passages"], len(ids))
            self.assertEqual(compared["disagreements"], 0)

    def test_two_busy_parents_do_not_hide_third_ready_parent(self):
        parents = self.ready_parents()
        original_source = self.source
        for parent in parents[:2]:
            self.source = parent["source_id"]
            self.ingest("turn/unprojected", "Synthetic pending source update")
        self.source = original_source
        outcome = self.worker_cycle(self.publisher())
        self.assertEqual(outcome["membership_deferred_documents"], 2)
        self.assertEqual(outcome["membership_failed_documents"], 0)
        self.assertGreater(outcome["membership_certified_passages"], 0)
        remaining = remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        )
        self.assertEqual({remainder.key(row) for row in remaining},
                         {remainder.key(row) for row in parents[:2]})
        self.assertTrue(all(row["checkpoint"]["after"] == "" for row in remaining))

    def test_smaller_configured_batch_remains_a_finite_selection(self):
        self.ready_parents()
        outcome = self.worker_cycle(self.publisher(), batch_size=1)
        self.assertGreater(outcome["membership_certified_passages"], 0)
        remaining = remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        )
        self.assertEqual(len(remaining), 2)

    def test_expired_first_proof_does_not_attempt_other_selected_parents(self):
        parents = self.ready_parents()
        original = enrollment.certify_published_passages
        attempted = []

        def expired(*args, **kwargs):
            attempted.append(kwargs["logical_document_id"])
            time.sleep(max(0, kwargs["deadline_at"] - time.monotonic()) + .01)
            return original(*args, **kwargs)

        with patch.object(enrollment, "certify_published_passages", side_effect=expired):
            outcome = self.worker_cycle(self.publisher())
        self.assertEqual(attempted, [parents[0]["logical_document_id"]])
        self.assertEqual(outcome["membership_certified_passages"], 0)
        self.assertEqual(outcome["membership_failed_documents"], 1)
        remaining = remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        )
        self.assertEqual(len(remaining), 3)
        self.assertEqual([remainder.key(row) for row in remaining[:2]],
                         [remainder.key(row) for row in parents[1:]])
        self.assertTrue(all(row["checkpoint"]["after"] == "" for row in remaining))


if __name__ == "__main__":
    suite = unittest.TestSuite(CertificationSelectionTests(name)
        for name in CertificationSelectionTests.__dict__ if name.startswith("test_"))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
