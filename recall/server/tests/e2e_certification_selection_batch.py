#!/usr/bin/env python3
"""Certification metadata selection does not inherit body preparation concurrency."""
import time
import unittest
import json
import re
from contextlib import nullcontext
from pathlib import Path
import psycopg
from psycopg import sql
from unittest.mock import patch

from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment
from recall_server.passage_index import CertificationBudget
from recall_server.projection_worker import run_projection_worker


class CertificationSelectionTests(ParentMembershipPublicationTests):
    def test_expired_source_turn_leaves_unattempted_rows_and_advances_to_peers(self):
        parents = self.ready_parents()
        publisher = self.publisher()
        native = enrollment.certify_published_passages
        attempted = []
        def rows():
            with self.store.connect() as connection:
                return {row["source_id"]: row for row in connection.execute(
                    "SELECT * FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                    (self.tenant,)).fetchall()}
        before = rows()
        def expire_first(store, **kwargs):
            attempted.append(kwargs["source_id"])
            if kwargs["source_id"] == self.source:
                with enrollment._connect(store, kwargs["deadline_at"]) as connection:
                    store._execute_bounded(connection, "SELECT pg_sleep(5)", (), kwargs["deadline_at"])
            return native(store, **kwargs)
        with patch.object(enrollment, "certify_published_passages", side_effect=expire_first):
            first = publisher.certify_pending(tenant_id=self.tenant, batch_size=10,
                                               certification_budget=CertificationBudget())
            self.assertEqual(first["membership_certified_passages"], 0)
            self.assertEqual(first["membership_failures"][0]["sqlstate"], "57014")
            self.assertEqual(attempted, [self.source])
            after = rows()
            for parent in parents:
                if parent["source_id"] != self.source:
                    self.assertEqual(after[parent["source_id"]], before[parent["source_id"]])
            second = publisher.certify_pending(tenant_id=self.tenant, batch_size=10,
                                                certification_budget=CertificationBudget())
        self.assertEqual(second["membership_failed_documents"], 0)
        self.assertGreaterEqual(second["membership_certified_passages"], 2)
        self.assertEqual(set(rows()), {self.source})
        self.assertEqual(attempted.count(self.source), 1,
                         "unattempted selected sources incorrectly consumed their turns")

    def test_concurrent_source_index_is_idempotent_and_readiness_rejects_invalid_same_name(self):
        migration = (Path(__file__).resolve().parents[1] /
                     "schema/085a_membership_remainder_concurrent.sql").read_text()
        readiness = (Path(__file__).resolve().parents[1] /
                     "operations/certification_source_index_readiness.sql").read_text()
        name = "canonical_passage_certification_source_idx"
        with self.store.connect() as connection:
            versions = connection.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
            before = connection.execute("SELECT %s::regclass::oid AS oid", (name,)).fetchone()["oid"]
            for _ in range(2):
                self.store._migrate_concurrently(connection, migration)
                connection.execute(readiness)
            self.assertEqual(connection.execute("SELECT %s::regclass::oid AS oid", (name,)).fetchone()["oid"], before)
            self.assertEqual(connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version").fetchall(), versions)
            for flag in ("indisvalid", "indisready"):
                with self.subTest(flag=flag):
                    try:
                        connection.execute(sql.SQL("UPDATE pg_index SET {}=false WHERE indexrelid=%s::regclass").format(
                            sql.Identifier(flag)), (name,))
                        self.store._migrate_concurrently(connection, migration)
                        with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation,
                                                    "certification_source_index_unavailable"):
                            connection.execute(readiness)
                        connection.rollback()
                    finally:
                        connection.execute(sql.SQL("UPDATE pg_index SET {}=true WHERE indexrelid=%s::regclass").format(
                            sql.Identifier(flag)), (name,))
                        connection.commit()
            try:
                connection.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(name)))
                connection.execute(sql.SQL("CREATE INDEX {} ON canonical_passage_projection_queue "
                                           "(tenant_id,source_id,logical_document_id)").format(sql.Identifier(name)))
                self.store._migrate_concurrently(connection, migration)
                with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation,
                                            "certification_source_index_unavailable"):
                    connection.execute(readiness)
                connection.rollback()
            finally:
                connection.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(name)))
                self.store._migrate_concurrently(connection, migration)
                connection.execute(readiness)

    def test_source_turn_selection_does_not_scan_the_whole_bulk_queue(self):
        plans = []
        execute = self.store._execute_bounded_pipeline
        def measured(connection, statement, parameters, deadline):
            positions = iter(range(1, len(parameters)+1))
            prepared = re.sub(r"%s", lambda _: f"${next(positions)}", statement)
            execute(connection, "PREPARE source_turn AS " + prepared, (), deadline)
            try:
                command = sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE source_turn({})").format(
                    sql.SQL(",").join(sql.Literal(value) for value in parameters)).as_string(connection)
                plan = execute(connection, command, (), deadline).fetchone()["QUERY PLAN"][0]
                plans.append(plan)
            finally:
                execute(connection, "DEALLOCATE source_turn", (), deadline)
            return execute(connection, statement, parameters, deadline)
        def nodes(plan):
            yield plan
            for child in plan.get("Plans", ()):
                yield from nodes(child)
        with self.store.connect() as connection:
            self.assertIsNotNone(connection.execute(
                "SELECT to_regclass('canonical_passage_certification_source_idx') AS index"
            ).fetchone()["index"])
            connection.execute("""CREATE TEMP TABLE canonical_passage_projection_queue
                (LIKE public.canonical_passage_projection_queue INCLUDING ALL)""")
            try:
                connection.execute("""INSERT INTO canonical_passage_projection_queue
                    (tenant_id,source_id,logical_document_id,revision,reason,changed_at)
                    SELECT 'a','bulk',lpad(n::text,6,'0'),1,'certification',
                           now()-interval '1 day' FROM generate_series(1,73581)n""")
                connection.execute("""INSERT INTO canonical_passage_projection_queue
                    (tenant_id,source_id,logical_document_id,revision,reason)
                    VALUES ('a','peer-b','one',1,'certification'),
                           ('a','peer-c','one',1,'certification')""")
                connection.execute("ANALYZE canonical_passage_projection_queue")
                with patch.object(self.store, "connect", side_effect=lambda **_: nullcontext(connection)), \
                     patch.object(self.store, "_execute_bounded_pipeline", side_effect=measured):
                    for mode in ("force_custom_plan", "force_generic_plan"):
                        connection.execute("SET LOCAL plan_cache_mode=" + mode)
                        for cursor in (("a", "bulk"), ("a", "peer-c"), ("", "")):
                            rows = remainder.pending(self.store, tenant_id="a", limit=10,
                                after_source=cursor, deadline_at=time.monotonic()+3)
                            plans[-1]["mode"] = mode
                            plans[-1]["cursor"] = cursor
                            if cursor == ("a", "bulk"):
                                self.assertEqual({row["source_id"] for row in rows},
                                                 {"peer-b", "peer-c"})
                            else:
                                self.assertEqual(len(rows), 10)
                                self.assertEqual({row["source_id"] for row in rows}, {"bulk"})
            finally:
                connection.execute("DROP TABLE canonical_passage_projection_queue")
        for plan in plans:
            examined = sum((node.get("Actual Rows", 0)+node.get("Rows Removed by Filter", 0))
                           * node.get("Actual Loops", 1) for node in nodes(plan["Plan"])
                           if "Scan" in node["Node Type"])
            print(json.dumps(dict(source_turn_plan=plan, examined=examined)), flush=True)
            self.assertLess(examined, 2000, "source rotation traversed unrelated bulk debt")

    def test_bulk_source_timeouts_do_not_hide_ready_sources_across_allowances(self):
        parents = self.ready_parents()
        bulk_source = self.source
        peers = [row for row in parents if row["source_id"] != bulk_source]
        publisher = self.publisher()
        native_ingest = self.plane.ingest_document
        native_certify = enrollment.certify_published_passages
        attempted = []

        def add_bulk(index):
            def independent(**kwargs):
                kwargs["envelope"]["native_parent_id"] = f"bulk-{index}"
                return native_ingest(**kwargs)
            with patch.object(self.plane, "ingest_document", side_effect=independent):
                self.ingest(f"turn/bulk-{index}", f"Synthetic bulk evidence {index}.")
            self.assertEqual(self.projector.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1,
                upload_concurrency=1)["failed"], 0)
            held = CertificationBudget()
            with held.lease():
                result = publisher.project_pending(
                    tenant_id=self.tenant, batch_size=10, max_batches=1,
                    concurrency=1, certification_budget=held)
            self.assertEqual(result["documents"], 1)

        def slow_bulk(store, **kwargs):
            attempted.append((kwargs["source_id"], kwargs["logical_document_id"]))
            if kwargs["source_id"] == bulk_source:
                with enrollment._connect(store, kwargs["deadline_at"]) as connection:
                    store._execute_bounded(
                        connection, "SELECT pg_sleep(5)", (), kwargs["deadline_at"])
            return native_certify(store, **kwargs)

        for index in range(3):
            add_bulk(index)
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_passage_projection_queue SET changed_at=now()-interval '1 hour' "
                "WHERE tenant_id=%s AND source_id=%s", (self.tenant, bulk_source))
        with patch.object(enrollment, "certify_published_passages", side_effect=slow_bulk):
            timeouts = 0
            for turn in range(3):
                add_bulk(3 + turn)
                result = publisher.certify_pending(
                    tenant_id=self.tenant, batch_size=10,
                    certification_budget=CertificationBudget())
                for failure in result["membership_failures"]:
                    self.assertEqual(failure["sqlstate"], "57014")
                    timeouts += 1
        self.assertGreaterEqual(timeouts, 2)
        remaining = remainder.pending(
            self.store, tenant_id=self.tenant, limit=20,
            deadline_at=time.monotonic() + 3)
        self.assertTrue(remaining, "bulk debt must remain queued")
        self.assertTrue(all(row["source_id"] == bulk_source for row in remaining),
                        "ready sources waited for the bulk source to exhaust")
        for parent in peers:
            self.assertIn((parent["source_id"], parent["logical_document_id"]), attempted)
            with self.store.connect() as connection:
                ids = tuple(row["passage_id"] for row in connection.execute(
                    "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s "
                    "AND source_id=%s AND logical_document_id=%s ORDER BY passage_id",
                    remainder.key(parent)).fetchall())
            compared = enrollment.compare_parent_membership(
                self.store, **dict(self.scope, source_id=parent["source_id"], passage_ids=ids),
                deadline_at=time.monotonic() + 3)
            self.assertEqual(compared["certified_passages"], len(ids))
            self.assertEqual(compared["disagreements"], 0)

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
            if self.additive_membership_generation:
                # On092 a pending additive ingest is safe to certify. Hold an
                # actual parent maintenance intent to exercise busy selection.
                with self.store.connect() as connection:
                    connection.execute("""INSERT INTO canonical_retirement_source_policy(
                        tenant_id,source_id,principal_id,maintenance_intent)
                        SELECT tenant_id,source_id,%s,
                               jsonb_build_object('parent',native_parent_id)
                        FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                        (self.principal, *remainder.key(parent)))
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

    def test_worker_preserves_configured_batch_above_body_projection_maximum(self):
        self.ready_parents()
        outcome = self.worker_cycle(self.publisher(), batch_size=1001)
        self.assertEqual(outcome["membership_failed_documents"], 0)
        self.assertGreater(outcome["membership_certified_passages"], 0)
        self.assertEqual(remainder.pending(
            self.store, tenant_id=self.tenant, limit=10,
            deadline_at=time.monotonic() + 3,
        ), [])

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
