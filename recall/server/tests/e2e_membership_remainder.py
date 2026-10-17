#!/usr/bin/env python3
"""A committed publication retains optional native certification work across restart."""

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import unittest
from unittest.mock import patch
from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import parent_membership_enrollment as enrollment
from recall_server import membership_remainder as remainder
from e2e_passage_receipt_repair import Repair


class MembershipRemainderTests(ParentMembershipPublicationTests):
    def test_native_query_cancel_retains_failure_scope_and_queue_for_restart(self):
        self.interrupted_publication()
        before = self.queued()

        def cancelled(*args, **kwargs):
            with self.store.connect() as connection:
                self.store._execute_bounded(connection, "SELECT pg_sleep(1)", (),
                    time.monotonic() + .01)

        with patch.object(enrollment, "enroll_parent_passages", side_effect=cancelled):
            result = self.run_cycle()
        self.assertEqual(result["membership_failed_documents"], 1)
        self.assertEqual(result["membership_certified_passages"], 0)
        self.assertEqual(result["membership_failures"], [dict(
            error_class="SearchDeadlineExceeded", sqlstate="57014", source_id=self.source,
            native_parent_id="session", logical_document_id=before[0]["logical_document_id"])])
        self.assertEqual(self.queued(), before)
        resumed = self.run_cycle()
        self.assertEqual(resumed["documents"], 0)
        self.assertEqual(resumed["membership_failures"], [])
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.compare()["disagreements"], 0)

    def test_queue_selection_failure_does_not_invent_parent_identity(self):
        self.interrupted_publication()
        before = self.queued()
        with patch.object(remainder, "pending", side_effect=OSError("private diagnostic text")):
            result = self.run_cycle()
        self.assertEqual(result["membership_failed_documents"], 1)
        self.assertEqual(result["membership_failures"], [dict(error_class="OSError",
            sqlstate=None, source_id=None, native_parent_id=None, logical_document_id=None)])
        self.assertEqual(self.queued(), before)
        self.assertNotIn("private diagnostic text", repr(result))

    def test_empty_early_queue_does_not_spend_proof_budget_during_source_work(self):
        from recall_server.projection_worker import run_projection_worker

        publisher = self.publisher()
        self.assertEqual(self.queued(), [])
        test = self

        class Logical:
            def project_pending(self, **kwargs):
                # The initial publication callback found no optional work.
                # Real source work can take longer than the proof allowance.
                time.sleep(10)
                test.queue()
                kwargs["on_progress"]()
                return dict(status="complete", documents=1, records=1,
                            batches=1, pending=0, pruned=0, cleanup_failures=0)

        outcome = run_projection_worker(
            Logical(), publisher, tenant_id=self.tenant,
            logical_batch_size=1, passage_batch_size=1, embedding_batch_size=1,
            max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
            interval_seconds=1, once=True, skip_embedding=True,
        )
        self.assertEqual(outcome["passage_documents"], 1)
        self.assertGreater(outcome["membership_certified_passages"], 0)
        self.assertEqual(self.queued(), [])
        self.refresh_scope()
        self.assertEqual(self.compare()["certified_passages"], len(self.scope["passage_ids"]))

    def test_busy_optional_lease_never_blocks_committed_source_publication(self):
        from recall_server.passage_index import CertificationBudget

        self.queue()
        budget = CertificationBudget()
        with budget.lease() as deadline:
            self.assertIsNotNone(deadline)
            outcome = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=1, max_batches=1,
                concurrency=1, certification_budget=budget)
            self.assertEqual(outcome["documents"], 1)
            self.assertEqual(outcome["membership_certified_passages"], 0)
            self.assertEqual(outcome["membership_failed_documents"], 0)
            self.assertEqual(len(self.queued()), 1)
            self.refresh_scope()
            current = self.compare()
            self.assertEqual(current["certified_passages"], 0)
            self.assertEqual(current["native_allowed_passages"], len(self.scope["passage_ids"]))
        resumed = self.run_cycle()
        self.assertEqual(resumed["documents"], 0)
        self.assertGreater(resumed["membership_certified_passages"], 0)
        self.assertEqual(self.queued(), [])

    def test_worker_callbacks_share_certification_window_and_keep_source_progress(self):
        from recall_server.projection_worker import run_projection_worker

        self.queue()
        publisher = self.publisher()
        deadlines = []
        test = self

        class Logical:
            def project_pending(self, **kwargs):
                for index in range(2):
                    test.ingest(f"turn/callback-{index}", f"Published callback {index}")
                    test.projector.project_pending(tenant_id=test.tenant, batch_size=10,
                                                   max_batches=1, upload_concurrency=1)
                    kwargs["on_progress"]()
                return dict(status="complete", documents=2, records=2, batches=1, pending=0, pruned=0, cleanup_failures=0)

        def exhaust_window(*args, **kwargs):
            deadlines.append(kwargs["deadline_at"])
            if len(deadlines) == 1:
                time.sleep(max(0, kwargs["deadline_at"] - time.monotonic()) + .02)
            return dict(status="deadline", certified_passages=0)

        with patch.object(remainder, "resume", side_effect=exhaust_window):
            outcome = run_projection_worker(
                Logical(), publisher, tenant_id=self.tenant,
                logical_batch_size=2, passage_batch_size=1, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=1, once=True, skip_embedding=True,
            )
        self.assertEqual(outcome["passage_documents"], 3)
        self.assertEqual(len(deadlines), 1, "later callbacks renewed the exhausted budget")
        self.assertEqual(len(self.queued()), 1)
        self.assertEqual(self.queued()[0]["checkpoint"]["after"], "")
        # A new cycle can finish native proof without republishing source bytes.
        resumed = self.run_cycle()
        self.assertEqual(resumed["documents"], 0)
        self.assertGreater(resumed["membership_certified_passages"], 0)
        self.refresh_scope()
        self.assertEqual(self.compare()["certified_passages"], len(self.scope["passage_ids"]))

    def test_restart_certifies_committed_publication_without_archive_republication(
        self,
    ):
        self.queue()
        with patch.object(
            enrollment,
            "enroll_parent_passages",
            side_effect=OSError("synthetic interruption after publication"),
        ):
            published = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1
            )
        self.assertEqual(published["documents"], 1)
        self.assertEqual(published["membership_failed_documents"], 1)
        self.refresh_scope()
        self.assertEqual(self.compare()["certified_passages"], 0)
        restarted = self.publisher()
        with patch.object(
            restarted.logical_projection,
            "read_part",
            side_effect=AssertionError("certification must not reread archives"),
        ):
            resumed = restarted.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1
            )
        self.assertEqual(resumed["documents"], 0)
        self.assertEqual(
            resumed["membership_certified_passages"], len(self.scope["passage_ids"])
        )
        compared = self.compare()
        self.assertEqual(compared["certified_passages"], len(self.scope["passage_ids"]))
        self.assertEqual(
            compared["native_allowed_passages"], len(self.scope["passage_ids"])
        )
        self.assertEqual(compared["disagreements"], 0)
        self.assertEqual(resumed["pending"], 0)

    def test_second_prepared_publisher_cannot_republish_optional_phase(self):
        self.queue()
        publisher = self.publisher()
        candidate = publisher._pending(tenant_id=self.tenant, limit=1)[0]
        prepared = publisher._prepare(candidate)
        self.assertEqual(publisher._commit(prepared)["status"], "committed")
        # A second worker may already hold the same prepared source generation.
        self.assertEqual(publisher._commit(prepared)["status"], "stale")
        self.assertEqual(self.queued()[0]["checkpoint"]["after"], "")

    def test_disabled_remainder_does_not_make_published_source_pending(self):
        from recall_server.cli import _storage_authority_audit
        from recall_server.canonical_retrieval import BoundCanonicalRetrieval

        self.queue()
        with patch.object(
            enrollment, "enroll_parent_passages", side_effect=OSError("interrupted")
        ):
            self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1
            )
        disabled = self.publisher(self.settings(enabled=False))
        with patch.object(
            disabled.logical_projection,
            "read_part",
            side_effect=AssertionError("archive replay"),
        ):
            outcome = disabled.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1
            )
        self.assertEqual(outcome["documents"], 0)
        self.assertEqual(outcome["pending"], 0)
        source = self.plane.source_status(
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source
        )
        self.assertEqual(source["passage_queue"], 0)
        self.assertEqual(
            _storage_authority_audit(self.store, self.tenant)["queues"]["passage"], 0
        )
        reader = BoundCanonicalRetrieval(
            self.store,
            tenant_id=self.tenant,
            principal_id=self.principal,
            authorized_sources=(self.source,),
        )
        with self.store.connect() as connection:
            connection.execute(
                "DELETE FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.assertEqual(
            reader._parquet_shards([self.source], since=None, until=None)[1], 0
        )
        self.refresh_scope()
        self.assertEqual(self.compare()["certified_passages"], 0)
        # Re-enabling the same durable owner still has the retained work.
        resumed = self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(
            resumed["membership_certified_passages"], len(self.scope["passage_ids"])
        )

    def run_cycle(self):
        return self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )

    def interrupted_publication(self):
        self.queue()
        with patch.object(
            enrollment, "enroll_parent_passages", side_effect=OSError("interrupt")
        ):
            self.run_cycle()
        self.refresh_scope()

    def queued(self):
        return remainder.pending(
            self.store,
            tenant_id=self.tenant,
            limit=10,
            deadline_at=time.monotonic() + 3,
        )

    def test_native_commit_lost_response_reproves_without_publication(self):
        self.interrupted_publication()
        native = enrollment.enroll_parent_passages

        def lost_response(*args, **kwargs):
            native(*args, **kwargs)
            raise OSError("lost response after committed native proof")

        before = self.queued()
        with patch.object(
            enrollment, "enroll_parent_passages", side_effect=lost_response
        ):
            first = self.run_cycle()
        self.assertEqual(first["membership_failed_documents"], 1)
        self.assertEqual(self.queued(), before)
        self.assertEqual(
            self.compare()["certified_passages"], len(self.scope["passage_ids"])
        )
        with patch.object(enrollment, "enroll_parent_passages", wraps=native) as prove:
            second = self.run_cycle()
        self.assertGreater(prove.call_count, 0)
        self.assertEqual(second["documents"], 0)
        self.assertEqual(
            second["membership_certified_passages"], len(self.scope["passage_ids"])
        )
        self.assertEqual(self.queued(), [])

    def test_committed_cursor_lost_response_resumes_after_acknowledged_ids(self):
        self.interrupted_publication()
        native = remainder.change_cursor

        def lost_response(*args, **kwargs):
            result = native(*args, **kwargs)
            if kwargs.get("after") is not None:
                raise OSError("lost response after cursor commit")
            return result

        with patch.object(remainder, "change_cursor", side_effect=lost_response):
            first = self.run_cycle()
        self.assertEqual(first["membership_failed_documents"], 1)
        self.assertEqual(
            self.queued()[0]["checkpoint"]["after"], max(self.scope["passage_ids"])
        )
        with patch.object(
            enrollment,
            "enroll_parent_passages",
            side_effect=AssertionError("already committed prefix"),
        ):
            second = self.run_cycle()
        self.assertEqual(second["membership_failed_documents"], 0)
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.compare()["disagreements"], 0)

    def test_new_publication_overrides_old_cursor_cas(self):
        self.interrupted_publication()
        old = self.queued()[0]
        self.ingest("turn/newer", "Synthetic subsequent content")
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1
        )
        self.assertFalse(
            remainder.change_cursor(
                self.store, old, old["checkpoint"], deadline_at=time.monotonic() + 3
            )
        )
        with self.store.connect() as c:
            row = c.execute(
                "SELECT reason,generation FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()
        self.assertEqual(row["reason"], "logical-update")
        self.assertGreater(row["generation"], old["generation"])
        outcome = self.run_cycle()
        self.assertEqual(outcome["documents"], 1)
        self.assertGreater(outcome["membership_certified_passages"], 0)

    def test_publication_racing_final_cursor_cleanup_is_reported_changed(self):
        self.interrupted_publication()
        row = self.queued()[0]
        remainder.change_cursor(
            self.store,
            row,
            row["checkpoint"],
            deadline_at=time.monotonic() + 3,
            after=max(self.scope["passage_ids"]),
        )
        row = self.queued()[0]
        native = remainder.change_cursor

        def superseded(*args, **kwargs):
            self.ingest("turn/newer-cleanup", "Synthetic newer publication")
            self.projector.project_pending(
                tenant_id=self.tenant,
                batch_size=10,
                max_batches=1,
                upload_concurrency=1,
            )
            return native(*args, **kwargs)

        with patch.object(remainder, "change_cursor", side_effect=superseded):
            outcome = remainder.resume(
                self.store,
                row,
                settings=self.settings(),
                deadline_at=time.monotonic() + 3,
            )
        self.assertEqual(outcome["status"], "changed")
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT reason FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["reason"],
                "logical-update",
            )

    def test_malformed_checkpoint_is_terminal_metadata_work(self):
        self.interrupted_publication()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_passage_projection_queue SET certification_checkpoint='[]'::jsonb WHERE tenant_id=%s",
                (self.tenant,),
            )
        with patch.object(
            enrollment,
            "enroll_parent_passages",
            side_effect=AssertionError("malformed scope"),
        ):
            result = self.run_cycle()
        self.assertEqual(result["membership_failed_documents"], 0)
        self.assertEqual(result["membership_deferred_documents"], 1)
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.compare()["certified_passages"], 0)

    def test_json_null_checkpoint_does_not_repeat_forever(self):
        self.interrupted_publication()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_passage_projection_queue SET certification_checkpoint='null'::jsonb WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.assertEqual(self.run_cycle()["membership_deferred_documents"], 1)
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.run_cycle()["membership_deferred_documents"], 0)

    def test_one_cycle_deadline_and_finite_selection_follow_publication(self):
        self.queue()
        native_pending = remainder.pending
        observed = []

        def pending(*args, **kwargs):
            rows = native_pending(*args, **kwargs)
            self.assertEqual(len(rows), 1)
            self.assertEqual(kwargs["limit"], 10)
            observed.append(kwargs["deadline_at"])
            return rows * 3

        def resume(*args, **kwargs):
            self.assertEqual(kwargs["deadline_at"], observed[0])
            observed.append(kwargs["deadline_at"])
            return dict(status="deadline", certified_passages=0)

        with (
            patch.object(remainder, "pending", side_effect=pending),
            patch.object(remainder, "resume", side_effect=resume),
        ):
            outcome = self.run_cycle()
        self.assertEqual(outcome["documents"], 1)
        self.assertEqual(len(observed), 4)
        self.assertEqual(len(set(observed)), 1)

    def test_active_retirement_rotates_without_archive_or_reproof(self):
        self.interrupted_publication()
        with self.store.connect() as c:
            c.execute(
                """INSERT INTO canonical_chunk_retirement_progress(
                tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
                FROM canonical_evidence_documents WHERE tenant_id=%s
                ON CONFLICT(tenant_id,source_id,native_parent_id)
                DO UPDATE SET enabled=true,status='pending'""",
                (self.tenant,),
            )
        before = self.queued()
        with self.store.connect() as c:
            changed_at = c.execute(
                "SELECT changed_at FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["changed_at"]
        with patch.object(
            enrollment,
            "enroll_parent_passages",
            side_effect=AssertionError("busy parent"),
        ):
            outcome = self.run_cycle()
            second = self.run_cycle()
        self.assertEqual(outcome["membership_deferred_documents"], 1)
        self.assertEqual(second["membership_deferred_documents"], 1)
        self.assertEqual(self.queued(), before)
        with self.store.connect() as c:
            self.assertGreater(c.execute(
                "SELECT changed_at FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["changed_at"], changed_at)

    def test_busy_parent_rotates_behind_independent_ready_parent(self):
        self.interrupted_publication()
        busy = self.queued()[0]
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_chunk_retirement_progress(
                tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
                FROM canonical_evidence_documents WHERE tenant_id=%s""", (self.tenant,))
        ingest = self.plane.ingest_document

        def independent(**kwargs):
            kwargs["envelope"]["native_parent_id"] = "independent-parent"
            return ingest(**kwargs)

        with patch.object(self.plane, "ingest_document", side_effect=independent):
            self.ingest("turn/independent", "Synthetic independent parent")
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1,
        )
        with patch.object(enrollment, "enroll_parent_passages", side_effect=TimeoutError("hold ready proof")):
            self.run_cycle()
        with self.store.connect() as c:
            c.execute("""UPDATE canonical_passage_projection_queue
                SET changed_at=now()-interval '1 minute'
                WHERE tenant_id=%s AND logical_document_id=%s""",
                (self.tenant, busy["logical_document_id"]))
        oldest = remainder.pending(self.store, tenant_id=self.tenant,
            limit=1, deadline_at=time.monotonic()+3)[0]
        self.assertEqual(oldest["logical_document_id"], busy["logical_document_id"])
        self.assertEqual(remainder.resume(self.store, oldest, settings=self.settings(),
            deadline_at=time.monotonic()+3)["status"], "busy")
        ready = remainder.pending(self.store, tenant_id=self.tenant,
            limit=1, deadline_at=time.monotonic()+3)[0]
        self.assertNotEqual(ready["logical_document_id"], busy["logical_document_id"])
        result = remainder.resume(self.store, ready, settings=self.settings(),
            deadline_at=time.monotonic()+3)
        self.assertEqual(result["status"], "enrolled")
        self.assertGreater(result["certified_passages"], 0)
        self.assertEqual(self.queued(), [busy])

    def test_concurrent_publication_wins_rotation_cas(self):
        self.interrupted_publication()
        original = self.queued()[0]
        query = enrollment._query
        interleaved = []

        def publish_newer():
            self.ingest("turn/rotation-race", "Synthetic newer publication")
            self.projector.project_pending(tenant_id=self.tenant,
                batch_size=10, max_batches=1, upload_concurrency=1)

        def before_rotation(store, connection, statement, parameters, deadline):
            if "SET changed_at=clock_timestamp(),certification_checkpoint=" in statement and not interleaved:
                interleaved.append(True)
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(publish_newer).result(timeout=10)
            return query(store, connection, statement, parameters, deadline)

        with patch.object(enrollment, "_query", side_effect=before_rotation):
            outcome = remainder.resume(self.store, original, settings=self.settings(),
                deadline_at=time.monotonic()+15)
        self.assertEqual(interleaved, [True])
        self.assertEqual(outcome["status"], "changed")
        self.assertEqual(outcome["certified_passages"], 0)
        with self.store.connect() as c:
            current = c.execute("""SELECT reason,generation,certification_checkpoint
                FROM canonical_passage_projection_queue WHERE tenant_id=%s""",
                (self.tenant,)).fetchone()
        self.assertEqual(current["reason"], "logical-update")
        self.assertGreater(current["generation"], original["generation"])
        self.assertEqual(self.run_cycle()["documents"], 1)

    def test_busy_proof_outcome_keeps_acknowledged_cursor(self):
        self.interrupted_publication()
        before = self.queued()
        with patch.object(enrollment, "certify_published_passages",
                          return_value=dict(status="busy", certified_passages=0)):
            outcome = self.run_cycle()
        self.assertEqual(outcome["membership_deferred_documents"], 1)
        self.assertEqual(self.queued(), before)
        self.assertGreater(self.run_cycle()["membership_certified_passages"], 0)
        self.assertEqual(self.queued(), [])

    def test_epoch_mismatch_is_not_rebased(self):
        self.interrupted_publication()
        with patch.object(enrollment, "enroll_parent_passages",
                          side_effect=AssertionError("foreign epoch")):
            outcome = remainder.resume(self.store, self.queued()[0],
                settings=self.settings(epoch="synthetic-epoch-b"), deadline_at=time.monotonic()+3)
        self.assertEqual(outcome["status"], "changed")
        self.assertEqual(self.queued(), [])

    def test_partial_batch_restart_preserves_committed_prefix(self):
        self.ingest("turn/long-publication", "Synthetic evidence paragraph. " * 3000)
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1
        )
        native = enrollment.enroll_parent_passages
        calls = []

        def interrupted(*args, **kwargs):
            calls.append(tuple(kwargs["passage_ids"]))
            if len(calls) == 2:
                raise OSError("second batch interrupted")
            return native(*args, **kwargs)

        with (
            patch.dict(enrollment._CERTIFICATION_LIMITS, passages=1),
            patch.object(enrollment, "enroll_parent_passages", side_effect=interrupted),
        ):
            first = self.run_cycle()
        self.assertEqual(first["membership_certified_passages"], 1)
        self.assertEqual(self.queued()[0]["checkpoint"]["after"], calls[0][0])
        subsequent = []

        def recorded(*args, **kwargs):
            subsequent.extend(kwargs["passage_ids"])
            return native(*args, **kwargs)

        with patch.object(enrollment, "enroll_parent_passages", side_effect=recorded):
            second = self.run_cycle()
        self.assertNotIn(calls[0][0], subsequent)
        self.assertIn(calls[1][0], subsequent)
        self.assertEqual(second["documents"], 0)
        self.assertEqual(self.queued(), [])

    def test_old_schema_keeps_original_optional_hook(self):
        def restore_schema():
            with self.store.connect() as c:
                c.execute(
                    (
                        Path(__file__).resolve().parents[1]
                        / "schema/085_membership_remainder.sql"
                    ).read_text()
                )

        self.addCleanup(restore_schema)
        with self.store.connect() as c:
            c.execute(
                "ALTER TABLE canonical_passage_projection_queue DROP COLUMN certification_checkpoint"
            )
            c.execute("DELETE FROM schema_migrations WHERE version=85")
        self.queue()
        result = self.run_cycle()
        self.assertEqual(result["documents"], 1)
        self.assertGreater(result["membership_certified_passages"], 0)
        self.assertEqual(self.queued(), [])

    def test_repeated_failure_rotates_queue_without_advancing_cursor(self):
        self.interrupted_publication()
        before = self.queued()
        with self.store.connect() as c:
            old = c.execute(
                "SELECT changed_at FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["changed_at"]
        with patch.object(
            enrollment,
            "enroll_parent_passages",
            side_effect=OSError("repeatable proof failure"),
        ):
            self.run_cycle()
        with self.store.connect() as c:
            new = c.execute(
                "SELECT changed_at FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["changed_at"]
        self.assertGreater(new, old)
        self.assertEqual(self.queued(), before)

    def test_live_source_grant_revocation_rejects_remainder(self):
        self.interrupted_publication()
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s",
                (self.tenant, self.principal),
            )
        result = self.run_cycle()
        self.assertEqual(result["membership_certified_passages"], 0)
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.compare()["status"], "unauthorized")

    def test_forget_remains_effective_before_archive_cleanup(self):
        from recall_server.canonical import CanonicalLifecycleError

        self.interrupted_publication()
        request = dict(
            contract="recall.forget-request.v1",
            schema_version=1,
            tenant_id=self.tenant,
            principal_id=self.principal,
            source_id=self.source,
            target_receipt=self.receipt,
            mode="explicit_forget",
            reason="owner_requested",
            requested_at="2026-10-14T00:01:00Z",
            idempotency_key="remainder-forget",
        )
        with patch.object(
            self.archive,
            "delete_internal_raw_bounded",
            side_effect=OSError("archive unavailable"),
        ):
            with self.assertRaisesRegex(
                CanonicalLifecycleError, "archive_delete_failed"
            ):
                self.plane.forget(request)
        result = self.run_cycle()
        self.assertEqual(result["membership_certified_passages"], 0)
        self.assertEqual(self.compare()["certified_passages"], 0)
        self.assertEqual(self.compare()["disagreements"], 0)


class RepairWithRemainder(Repair):
    def test_native_receipt_repair_supersedes_only_optional_phase(self):
        self.passages.membership_certification = enrollment.CertificationSettings(
            enabled=True, binding_epoch="synthetic-epoch-a"
        )
        with patch.object(
            enrollment, "enroll_parent_passages", side_effect=OSError("interruption")
        ):
            plan = self.company_admin_plan()
        with self.store.connect() as c:
            row = c.execute(
                "SELECT reason,generation FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()
        self.assertEqual(row["reason"], "certification")
        self.assertEqual(
            self.passages.requeue_receipt_repair(plan=plan, principal_id=self.admin)[
                "status"
            ],
            "queued",
        )
        with self.store.connect() as c:
            changed = c.execute(
                "SELECT reason,generation FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()
        self.assertEqual(changed["reason"], "backfill")
        self.assertGreater(changed["generation"], row["generation"])
        self.passages.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(self.rows()[0]["receipts"], self.correct[0]["receipts"])

    def test_empty_repair_and_backfill_override_optional_phase(self):
        self.passages.membership_certification = enrollment.CertificationSettings(
            enabled=True, binding_epoch="synthetic-epoch-a"
        )
        with patch.object(
            enrollment, "enroll_parent_passages", side_effect=OSError("interruption")
        ):
            self.company_admin_plan()
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_passages WHERE tenant_id=%s", (self.tenant,)
            )
            c.execute(
                "UPDATE canonical_passage_documents SET passage_count=0 WHERE tenant_id=%s",
                (self.tenant,),
            )
        result = self.passages.repair_empty(
            tenant_id=self.tenant, source_id=self.source, limit=10, apply=True
        )
        self.assertEqual(result["queued"], 1)
        self.passages.project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(self.rows()[0]["receipts"], self.correct[0]["receipts"])
        # A changed extraction policy similarly supersedes optional old work.
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO canonical_passage_projection_queue(tenant_id,source_id,logical_document_id,revision,reason) SELECT tenant_id,source_id,logical_document_id,revision,'certification' FROM canonical_passage_documents WHERE tenant_id=%s ON CONFLICT(tenant_id,source_id,logical_document_id) DO UPDATE SET reason='certification'",
                (self.tenant,),
            )
            c.execute(
                "UPDATE canonical_passage_documents SET policy_fingerprint=%s WHERE tenant_id=%s",
                ("f" * 64, self.tenant),
            )
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT reason FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["reason"],
                "backfill",
            )


if __name__ == "__main__":
    suite = unittest.TestSuite(
        cls(name)
        for cls in (MembershipRemainderTests, RepairWithRemainder)
        for name in cls.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
