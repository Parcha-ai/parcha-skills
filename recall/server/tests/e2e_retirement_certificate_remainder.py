"""Completed retirement must leave optional proof work resumable."""
import json
import unittest
from unittest.mock import patch

from e2e_retirement_membership_certification import RetirementMembershipTests
from recall_server import chunk_retirement, membership_remainder
from recall_server import parent_membership_enrollment as membership
import time


class RetirementCertificateRemainderTests(RetirementMembershipTests):
    def test_completed_retirement_resumes_unfinished_certificate_tail(self):
        self.enroll()
        self.ingest("turn/long-tail", "Synthetic evidence paragraph. " * 3000)
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1,
            upload_concurrency=1,
        )
        publication = self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1,
        )
        self.assertEqual(publication["documents"], 1)
        self.refresh_scope()
        self.assertGreater(len(self.scope["passage_ids"]), 1)
        original = membership.enroll_parent_passages
        attempts = []

        def interrupted_after_committed_page(*args, **kwargs):
            attempts.append(tuple(kwargs["passage_ids"]))
            if len(attempts) == 2:
                raise TimeoutError("synthetic second certificate page interrupted")
            return original(*args, **kwargs)

        with (
            patch.dict(membership._CERTIFICATION_LIMITS, passages=1),
            patch.object(membership, "enroll_parent_passages",
                         side_effect=interrupted_after_committed_page),
        ):
            retired = self.run_retirement()
        self.assertEqual(retired["completed_parent_proofs"], 1)
        self.assertFalse(retired["commit_outcome_unknown"])
        self.assertEqual(retired["membership_certified_passages"], 1)
        self.assertEqual(retired["membership_failed_parents"], 1)
        # A complete unchanged retirement is no longer eligible for maintenance.
        again = self.run_retirement()
        self.assertEqual(again["completed_parent_proofs"], 0)
        restarted = self.publisher()
        with patch.object(restarted.logical_projection, "read_part",
                          side_effect=AssertionError("proof must not replay archives")):
            resumed = restarted.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1,
            )
        self.assertEqual(resumed["documents"], 0)
        compared = self.compare()
        print(json.dumps(dict(
            completed_retirement=retired["completed_parent_proofs"],
            initial_certificate_ack=retired["membership_certified_passages"],
            retirement_reselected=again["completed_parent_proofs"],
            next_cycle_certificate_ack=resumed["membership_certified_passages"],
            selected=compared["selected_passages"],
            certified=compared["certified_passages"],
            native_allowed=compared["native_allowed_passages"],
            disagreements=compared["disagreements"],
        )), flush=True)
        self.assertEqual(compared["disagreements"], 0)
        self.assertEqual(compared["native_allowed_passages"], len(self.scope["passage_ids"]))
        self.assertEqual(compared["certified_passages"], len(self.scope["passage_ids"]),
                         "completed retirement lost its optional certification tail")

    def test_retirement_during_proof_keeps_work_and_reproves_invalidated_prefix(self):
        self.enroll()
        chunk_retirement.set_parent_retirement_enabled(
            self.store, tenant_id=self.tenant, source_id=self.source,
            native_parent_id="session", enabled=False,
        )
        self.ingest("turn/racing-tail", "Synthetic evidence paragraph. " * 3000)
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1,
        )
        original = membership.enroll_parent_passages
        attempted = []

        def partial(*args, **kwargs):
            attempted.append(kwargs["passage_ids"])
            if len(attempted) == 2:
                raise TimeoutError("synthetic interrupted prefix")
            return original(*args, **kwargs)

        with (patch.dict(membership._CERTIFICATION_LIMITS, passages=1),
              patch.object(membership, "enroll_parent_passages", side_effect=partial)):
            published = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1,
            )
        self.assertEqual(published["membership_certified_passages"], 1)
        rows = membership_remainder.pending(
            self.store, tenant_id=self.tenant, limit=10, deadline_at=time.monotonic()+3,
        )
        self.assertEqual(rows[0]["checkpoint"]["after"], attempted[0][-1])
        capture = membership._capture

        def retire_after_capture(*args, **kwargs):
            captured = capture(*args, **kwargs)
            chunk_retirement.set_parent_retirement_enabled(
                self.store, tenant_id=self.tenant, source_id=self.source,
                native_parent_id="session", enabled=True,
            )
            self.assertEqual(self.run_retirement(enabled=False)["completed_parent_proofs"], 1)
            return captured

        with patch.object(membership, "_capture", side_effect=retire_after_capture):
            outcome = membership_remainder.resume(
                self.store, rows[0], settings=self.settings(), deadline_at=time.monotonic()+10,
            )
        self.assertEqual(outcome["status"], "changed")
        self.assertEqual(outcome["certified_passages"], 0)
        self.assertEqual(membership_remainder.pending(
            self.store, tenant_id=self.tenant, limit=10, deadline_at=time.monotonic()+3,
        ), rows)
        reproved = []

        def record(*args, **kwargs):
            reproved.extend(kwargs["passage_ids"])
            return original(*args, **kwargs)

        with patch.object(membership, "enroll_parent_passages", side_effect=record):
            resumed = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1,
            )
        self.assertEqual(reproved[0], attempted[0][0])
        self.refresh_scope()
        self.assertEqual(resumed["membership_certified_passages"], len(self.scope["passage_ids"]))
        compared = self.compare()
        self.assertEqual(compared["certified_passages"], len(self.scope["passage_ids"]))
        self.assertEqual(compared["disagreements"], 0)


if __name__ == "__main__":
    suite = unittest.TestSuite(RetirementCertificateRemainderTests(name)
        for name in RetirementCertificateRemainderTests.__dict__ if name.startswith("test_"))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
