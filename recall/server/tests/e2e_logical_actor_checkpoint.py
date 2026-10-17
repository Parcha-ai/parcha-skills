#!/usr/bin/env python3
"""Native actor publication checkpoint boundaries."""

import json
import os
from pathlib import Path
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import patch

import psycopg
from psycopg.types.json import Jsonb

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
]
import e2e_logical_append_capability as fixtures  # noqa: E402
from recall_server import logical_actor_checkpoint as actor_owner  # noqa: E402


class ActorCheckpoint(unittest.TestCase):
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    queue = fixtures.AppendCapability.queue

    def setUp(self):
        self.environment = patch.dict(os.environ, RECALL_LOGICAL_ACTOR_APPEND_REUSE="1")
        self.environment.start()
        self.addCleanup(self.environment.stop)
        fixtures.AppendCapability.setUp(self)

    def checkpoint(self):
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT to_jsonb(d) AS metadata FROM canonical_evidence_documents d WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            ).fetchone()
        return row["metadata"].get("actor_checkpoint")

    def test_complete_publication_creates_bounded_event_only_checkpoint(self):
        self.ingest("first", 10)
        self.publish()
        marker = self.checkpoint()
        self.assertIsNotNone(marker)
        self.assertEqual(marker["source_ordinal"], 10)
        self.assertEqual(marker["actors"], [])

    def actor(self, letter):
        actor = "actor_" + letter * 32
        with self.store.connect() as connection:
            connection.execute(
                "INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES(%s,%s,'human','Synthetic actor')",
                (self.tenant, actor),
            )
        return actor

    def attach(self, connection, native, actor):
        connection.execute(
            "INSERT INTO canonical_event_actors(tenant_id,source_id,event_id,actor_id,relation) SELECT tenant_id,source_id,event_id,%s,'author' FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s ON CONFLICT DO NOTHING",
            (actor, self.tenant, self.source, native),
        )

    def published(self):
        with self.store.connect() as connection:
            return {
                (row["actor_id"], row["relation"])
                for row in connection.execute(
                    "SELECT actor_id,relation FROM canonical_evidence_document_actors WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
            }

    def attributed_event(self, native, ordinal):
        import hashlib
        from recall_server.projectors import canonical_json

        event = self.event(native, ordinal)
        content = {
            "type": "event_msg",
            "payload": {"type": "user_message", "message": "Synthetic user message"},
        }
        payload = canonical_json(content)
        artifact = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=payload,
            media_type="application/json",
            created_at=event["occurred_at"],
        )
        event.update(
            content=content, content_sha256=hashlib.sha256(payload).hexdigest()
        )
        event["provenance"].update(harness="codex", artifact_ref=artifact)
        return event

    def ingest_attributed(self, event):
        return self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.append-capability",
            artifact_ref=event["provenance"]["artifact_ref"],
            envelope=event,
            text_redacted=event["content"]["payload"]["message"],
        )

    def bind_actor_fixture(self):
        actor = self.actor("e")
        with self.store.connect() as connection:
            connection.execute(
                "INSERT INTO canonical_source_actor_bindings(tenant_id,source_id,actor_id,relation) VALUES(%s,%s,%s,'contributor')",
                (self.tenant, self.source, actor),
            )
        return actor

    catalog = fixtures.AppendCapability.catalog
    safe = fixtures.AppendCapability.safe

    def dirty(self):
        with self.store.connect() as connection:
            self.store._queue_actor_projection(
                connection, tenant_id=self.tenant, source_ids=[self.source]
            )

    @contextmanager
    def actor_queries(self):
        calls = []
        original = psycopg.Connection.execute

        def capture(connection, query, params=None, **kwargs):
            if query == actor_owner.EVENT_ACTOR_SQL:
                calls.append(params)
            return original(connection, query, params, **kwargs)

        with patch.object(psycopg.Connection, "execute", capture):
            yield calls

    def assert_original_union(self):
        with self.store.connect() as connection:
            actual = self.published()
            with connection.transaction():
                connection.execute("SAVEPOINT actor_oracle")
                row = connection.execute(
                    "SELECT logical_document_id,revision FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                ).fetchone()
                connection.execute(
                    "DELETE FROM canonical_evidence_document_actors WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
                connection.execute(
                    actor_owner.FULL_ACTOR_SQL,
                    (
                        self.tenant,
                        self.source,
                        row["logical_document_id"],
                        row["revision"],
                        self.tenant,
                        self.source,
                        self.parent,
                        None,
                        None,
                        self.tenant,
                        self.source,
                    ),
                )
                expected = {
                    (r["actor_id"], r["relation"])
                    for r in connection.execute(
                        "SELECT actor_id,relation FROM canonical_evidence_document_actors WHERE tenant_id=%s AND source_id=%s",
                        (self.tenant, self.source),
                    )
                }
                connection.execute("ROLLBACK TO SAVEPOINT actor_oracle")
        self.assertEqual(actual, expected)

    def test_scalar_and_batch_append_match_original_full_union(self):
        self.ingest("first", 1)
        self.publish()
        actor = self.bind_actor_fixture()
        with self.actor_queries() as calls:
            self.ingest_attributed(self.attributed_event("tail", 2))
            self.publish()
        self.assertEqual(calls[0][3], 1)
        self.assertIn((actor, "author"), self.published())
        tail = [
            self.attributed_event("batch-a", 3),
            self.attributed_event("batch-b", 4),
        ]
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=tail
        )
        with self.actor_queries() as calls:
            self.publish()
        self.assertEqual(calls[0][3], 2)
        self.assert_original_union()

    def test_duplicate_historical_attribution_forces_full_event_read(self):
        events = [self.attributed_event("old-a", 1), self.attributed_event("old-b", 2)]
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=events
        )
        self.publish()
        actor = self.bind_actor_fixture()
        self.assertEqual(self.ingest_attributed(events[0])["duplicate_events"], 1)
        with self.actor_queries() as calls:
            self.publish()
        self.assertIsNone(calls[0][3])
        self.assertIn((actor, "author"), self.published())
        self.assertEqual(
            self.plane.ingest_batch(
                tenant_id=self.tenant, principal_id=self.principal, events=events
            )["duplicate_events"],
            2,
        )
        with self.actor_queries() as calls:
            self.publish()
        self.assertIsNone(calls[0][3])
        self.assert_original_union()

    def test_source_binding_removal_does_not_carry_mixed_published_set(self):
        self.ingest("first", 1)
        actor = self.bind_actor_fixture()
        self.publish()
        self.assertIn((actor, "contributor"), self.published())
        # Deliberate raw SQL retains the existing authority counterexample.
        # Bindings are fresh at publication even without a new actor marker.
        with self.store.connect() as connection:
            connection.execute(
                "DELETE FROM canonical_source_actor_bindings WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            )
        self.ingest("second", 2)
        self.publish()
        self.assertNotIn((actor, "contributor"), self.published())
        self.assert_original_union()

    def test_old_identical_body_writer_ack_clears_marker(self):
        self.ingest("visible", 1)
        self.ingest("excluded", 2, record_type="token_count")
        self.publish()
        before = self.catalog()
        actor = self.actor("a")
        with self.store.connect() as connection:
            self.attach(connection, "excluded", actor)
            self.store._queue_actor_projection(
                connection, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.logical.actor_append_enabled = False
        self.assertEqual(self.publish()["repaired"], 1)
        self.assertEqual(before, self.catalog())
        self.assertIsNone(self.checkpoint())
        self.logical.actor_append_enabled = True
        self.ingest("tail", 3)
        with self.actor_queries() as calls:
            self.publish()
        self.assertIsNone(calls[0][3])
        self.assertIn((actor, "author"), self.published())
        self.assert_original_union()

    def test_enabled_identical_body_repair_refreshes_historical_structural_actor(self):
        self.ingest("visible", 1)
        self.ingest("excluded", 2, record_type="token_count")
        self.publish()
        before = self.catalog()
        actor = self.actor("b")
        with self.store.connect() as connection:
            self.attach(connection, "excluded", actor)
            self.store._queue_actor_projection(
                connection, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.assertEqual(self.publish()["repaired"], 1)
        self.assertEqual(before, self.catalog())
        self.assertIn((actor, "author"), self.published())
        self.assertIn([actor, "author"], self.checkpoint()["actors"])
        self.assert_original_union()

    def test_forget_invalidates_prefix_and_removes_event_actor(self):
        self.ingest("visible", 1)
        self.ingest("kept", 2)
        actor = self.actor("c")
        with self.store.connect() as connection:
            self.attach(connection, "visible", actor)
        self.publish()
        result = self.plane.forget(
            dict(
                contract="recall.forget-request.v1",
                schema_version=1,
                tenant_id=self.tenant,
                principal_id=self.principal,
                source_id=self.source,
                target_receipt=f"recall://{self.source}/visible?rev=1#item=0",
                mode="explicit_forget",
                reason="owner_requested",
                requested_at="2026-10-14T10:00:00Z",
                idempotency_key="actor-checkpoint-forget",
            )
        )
        self.assertEqual(result["status"], "deleted")
        self.assertEqual(self.queue()["reason"], "forget")
        with self.actor_queries() as calls:
            self.publish()
        self.assertIsNone(calls[0][3])
        self.assertEqual(self.published(), set())
        self.assert_original_union()

    def test_oversized_and_malformed_markers_fall_back_without_truncation(self):
        self.ingest("first", 1)
        self.publish()
        for changes in (
            {"version": 2},
            {"actors": [["x" * 256, "author"]]},
            {"actors": [["x", "author"]] * 65},
            {"actors": [[str(i).zfill(255), "author"] for i in range(64)]},
        ):
            marker = dict(self.checkpoint(), **changes)
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE canonical_evidence_documents SET actor_checkpoint=%s WHERE tenant_id=%s AND source_id=%s",
                    (Jsonb(marker), self.tenant, self.source),
                )
            ordinal = self.catalog()["append_source_ordinal"] + 1
            self.ingest(f"tail-{ordinal}", ordinal)
            with self.actor_queries() as calls:
                self.publish()
            self.assertIsNone(calls[0][3])
        for number in range(65):
            self.ingest(f"many-{number}", 100 + number)
        with self.store.connect() as connection:
            for number in range(65):
                actor = "actor_" + f"{number:032x}"
                connection.execute(
                    "INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES(%s,%s,'human','Synthetic')",
                    (self.tenant, actor),
                )
                self.attach(connection, f"many-{number}", actor)
            self.store._queue_actor_projection(
                connection, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.publish()
        self.assertIsNone(self.checkpoint())
        self.assertEqual(len(self.published()), 65)
        self.assert_original_union()

    def test_schema_absent_and_feature_disabled_preserve_full_owner(self):
        self.ingest("first", 1)
        with self.store.connect() as connection:
            connection.execute(
                "DROP TRIGGER logical_actor_checkpoint_ack ON canonical_evidence_document_queue"
            )
            connection.execute("DELETE FROM schema_migrations WHERE version=87")
            connection.execute(
                "ALTER TABLE canonical_evidence_documents DROP COLUMN actor_checkpoint"
            )
        try:
            with self.actor_queries() as calls:
                self.publish()
            self.assertEqual(calls, [])
        finally:
            with self.store.connect() as connection:
                connection.execute(
                    "ALTER TABLE canonical_evidence_documents ADD COLUMN actor_checkpoint jsonb"
                )
                connection.execute("INSERT INTO schema_migrations(version) VALUES(87)")
                connection.execute(
                    "CREATE TRIGGER logical_actor_checkpoint_ack AFTER DELETE ON canonical_evidence_document_queue FOR EACH ROW EXECUTE FUNCTION invalidate_logical_actor_checkpoint_on_ack()"
                )
        self.logical.actor_append_enabled = False
        self.ingest("second", 2)
        with self.actor_queries() as calls:
            self.publish()
        self.assertEqual(calls, [])
        self.assertIsNone(self.checkpoint())
        self.assert_original_union()

    def test_ack_rollback_preserves_checkpoint(self):
        self.ingest("first", 1)
        self.publish()
        before = self.checkpoint()
        self.dirty()
        with self.store.connect() as connection:
            connection.execute("SAVEPOINT before_ack")
            connection.execute(
                "DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT actor_checkpoint FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                ).fetchone()["actor_checkpoint"]
            )
            connection.execute("ROLLBACK TO SAVEPOINT before_ack")
        self.assertEqual(self.checkpoint(), before)
        self.assertIsNotNone(self.queue())

    def test_source_recreation_has_no_old_marker(self):
        from psycopg import sql

        self.ingest("first", 1)
        actor = self.actor("d")
        with self.store.connect() as connection:
            self.attach(connection, "first", actor)
        self.publish()
        with self.store.connect() as connection:
            for table in (
                "canonical_evidence_documents",
                "canonical_evidence_objects",
                "canonical_chunks",
                "canonical_documents",
                "canonical_events",
                "canonical_ingest_jobs",
                "raw_artifacts",
                "canonical_source_grants",
                "canonical_audit_events",
                "canonical_evidence_document_queue",
                "canonical_evidence_cleanup_queue",
                "canonical_sources",
            ):
                connection.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE tenant_id=%s AND source_id=%s"
                    ).format(sql.Identifier(table)),
                    (self.tenant, self.source),
                )
            connection.execute(
                "INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id) VALUES(%s,%s,%s)",
                (self.tenant, self.source, self.principal),
            )
            connection.execute(
                "INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,%s,'owner')",
                (self.tenant, self.source, self.principal),
            )
        self.ingest("fresh", 2)
        self.publish()
        self.assertEqual(self.published(), set())
        self.assertEqual(self.checkpoint()["actors"], [])

    def test_concurrent_new_queue_prevents_stale_actor_commit(self):
        self.ingest("first", 1)
        self.publish()
        self.ingest("second", 2)
        (candidate,) = self.logical._pending(tenant_id=self.tenant, limit=1)
        (upload,) = self.logical._prepare_batch_and_upload((candidate,))
        self.addCleanup(upload.body_locators.close)
        old = self.checkpoint()
        self.ingest("third", 3)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertEqual(self.checkpoint(), old)
        self.assertIsNotNone(self.queue())
        self.publish()
        self.assertEqual(self.checkpoint()["source_ordinal"], 3)
        self.assert_original_union()

    def test_later_append_keeps_queue_and_marker_at_captured_frontier(self):
        self.ingest("first", 1)
        self.publish()
        self.logical.append_enabled = True
        self.ingest("second", 2)
        (candidate,) = self.logical._pending(tenant_id=self.tenant, limit=1)
        (upload,) = self.logical._prepare_batch_and_upload((candidate,))
        self.addCleanup(upload.body_locators.close)
        self.ingest("third", 3)
        with self.actor_queries() as calls:
            self.assertEqual(
                self.logical._commit_upload(candidate, upload), "committed"
            )
        self.assertEqual(calls[0][3:], (1, 1, 2, 2))
        self.assertEqual(self.checkpoint()["source_ordinal"], 2)
        self.assertIsNotNone(self.queue())
        self.publish()
        self.assertEqual(self.checkpoint()["source_ordinal"], 3)
        self.assertIsNone(self.queue())

    def test_native_duplicate_waits_for_publication_then_invalidates_checkpoint(self):
        old = self.attributed_event("first", 1)
        self.ingest_attributed(old)
        self.publish()
        self.bind_actor_fixture()
        self.ingest("tail", 2)
        (candidate,) = self.logical._pending(tenant_id=self.tenant, limit=1)
        (upload,) = self.logical._prepare_batch_and_upload((candidate,))
        self.addCleanup(upload.body_locators.close)
        locked, release, attempted = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        original = self.logical._publish_body_locators
        original_execute = psycopg.Connection.execute
        mutation_state = {}

        def pause(*args, **kwargs):
            result = original(*args, **kwargs)
            locked.set()
            if not release.wait(5):
                raise AssertionError("publication barrier timeout")
            return result

        def capture(connection, query, params=None, **kwargs):
            if threading.get_ident() == mutation_state.get("thread"):
                mutation_state["pid"] = connection.info.backend_pid
                attempted.set()
            return original_execute(connection, query, params, **kwargs)

        def duplicate():
            mutation_state["thread"] = threading.get_ident()
            return self.ingest_attributed(old)

        with ThreadPoolExecutor(max_workers=2) as pool:
            with (
                patch.object(self.logical, "_publish_body_locators", pause),
                patch.object(psycopg.Connection, "execute", capture),
            ):
                publication = pool.submit(
                    self.logical._commit_upload, candidate, upload
                )
                self.assertTrue(locked.wait(5))
                mutation = pool.submit(duplicate)
                try:
                    self.assertTrue(attempted.wait(5))
                    import time

                    waiting = False
                    until = time.monotonic() + 2
                    with self.store.connect() as observer:
                        while time.monotonic() < until:
                            observer.execute("SELECT pg_stat_clear_snapshot()")
                            row = observer.execute(
                                "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                                (mutation_state["pid"],),
                            ).fetchone()
                            if row and row["wait_event_type"] == "Lock":
                                waiting = True
                                break
                            time.sleep(0.01)
                    self.assertTrue(
                        waiting,
                        "native duplicate must actually wait behind publication",
                    )
                finally:
                    release.set()
                self.assertEqual(publication.result(timeout=10), "committed")
                self.assertEqual(mutation.result(timeout=10)["duplicate_events"], 1)
        self.assertIsNotNone(self.queue())
        self.assertFalse(self.safe())
        with self.actor_queries() as calls:
            self.publish()
        self.assertIsNone(calls[0][3])
        self.assert_original_union()

    def test_suffix_plan_reads_only_new_events_and_full_initialization_matches_oracle(
        self,
    ):
        import time

        self.ingest("register", 0)
        actor = self.actor("f")
        batch = [self.event(f"event-{number}", number + 1) for number in range(128)]
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=batch
        )
        with self.store.connect() as connection:
            for number in range(128):
                self.attach(connection, f"event-{number}", actor)
        started = time.monotonic()
        with self.actor_queries() as initial:
            self.publish()
        initial_ms = (time.monotonic() - started) * 1000
        self.assertEqual(initial[0][3], None)
        self.assert_original_union()
        self.ingest("tail", 129)
        with self.store.connect() as connection:
            self.attach(connection, "tail", actor)
        started = time.monotonic()
        with self.actor_queries() as suffix:
            self.publish()
        suffix_ms = (time.monotonic() - started) * 1000
        self.assertEqual(suffix[0][3], 128)
        self.assert_original_union()
        plans = []
        with self.store.connect() as connection:
            connection.execute("ANALYZE canonical_events")
            connection.execute("ANALYZE canonical_event_actors")
            for params in (initial[0], suffix[0]):
                plan = connection.execute(
                    "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) "
                    + actor_owner.EVENT_ACTOR_SQL,
                    params,
                ).fetchone()["QUERY PLAN"][0]

                def event_rows(node):
                    return (
                        node.get("Actual Rows", 0) * node.get("Actual Loops", 0)
                        if node.get("Relation Name") == "canonical_events"
                        else 0
                    ) + sum(event_rows(child) for child in node.get("Plans", []))

                plans.append(
                    dict(
                        event_rows=event_rows(plan["Plan"]),
                        execution_ms=plan["Execution Time"],
                        shared_hits=plan["Plan"].get("Shared Hit Blocks", 0),
                    )
                )
        self.assertGreaterEqual(plans[0]["event_rows"], 128)
        print(json.dumps(dict(actor_plans=plans)))
        self.assertEqual(plans[1]["event_rows"], 1)
        print(
            json.dumps(
                dict(
                    actor_checkpoint_local_cost=dict(
                        initial=plans[0],
                        suffix=plans[1],
                        initial_publication_ms=initial_ms,
                        suffix_publication_ms=suffix_ms,
                    )
                ),
                sort_keys=True,
            )
        )

    def test_raw_historical_sql_requires_explicit_native_dirtying(self):
        self.ingest("first", 1)
        self.publish()
        actor = self.actor("a")
        with self.store.connect() as connection:
            self.attach(connection, "first", actor)
        self.ingest("tail", 2)
        self.publish()
        self.assertNotIn(
            (actor, "author"),
            self.published(),
            "unannounced raw SQL is outside the supported mutation contract",
        )
        self.dirty()
        self.publish()
        self.assertIn((actor, "author"), self.published())
        self.assert_original_union()

    def test_multiple_parents_in_one_native_batch_keep_separate_event_sets(self):
        self.ingest("register", 0)
        left_actor, right_actor = self.actor("a"), self.actor("b")
        self.parent = "session-one"
        left = self.event("left", 1)
        self.parent = "session-two"
        right = self.event("right", 1)
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[left, right]
        )
        with self.store.connect() as connection:
            self.attach(connection, "left", left_actor)
            self.attach(connection, "right", right_actor)
        self.assertEqual(self.publish()["documents"], 2)
        with self.store.connect() as connection:
            markers = {
                r["native_parent_id"]: r["actor_checkpoint"]
                for r in connection.execute(
                    "SELECT native_parent_id,actor_checkpoint FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
            }
        self.assertEqual(markers["session-one"]["actors"], [[left_actor, "author"]])
        self.assertEqual(markers["session-two"]["actors"], [[right_actor, "author"]])
        tail = []
        for parent in ("session-one", "session-two"):
            self.parent = parent
            tail.append(self.event(parent + "-tail", 2))
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=tail
        )
        self.assertEqual(self.publish()["documents"], 2)
        with self.store.connect() as connection:
            actual = {
                r["native_parent_id"]: r["actor_checkpoint"]
                for r in connection.execute(
                    "SELECT native_parent_id,actor_checkpoint FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
            }
        for parent in markers:
            self.assertEqual(actual[parent]["actors"], markers[parent]["actors"])
            self.assertEqual(actual[parent]["source_ordinal"], 2)

    def test_flag_defaults_off_and_rejects_non_boolean_strings(self):
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )
        from recall_server.logical_evidence import LogicalEvidenceError

        with patch.dict(os.environ, {}, clear=True):
            projector = CanonicalLogicalEvidenceProjector(
                self.store, self.logical.projection
            )
            self.assertFalse(projector.actor_append_enabled)
        for value in ("true", "", "2"):
            with patch.dict(os.environ, RECALL_LOGICAL_ACTOR_APPEND_REUSE=value):
                with self.assertRaisesRegex(
                    LogicalEvidenceError, "logical_actor_append_flag_invalid"
                ):
                    CanonicalLogicalEvidenceProjector(
                        self.store, self.logical.projection
                    )

    def test_checkpoint_write_cost_and_existing_authority_generation_unchanged(self):
        self.ingest("first", 1)
        self.publish()
        marker = self.checkpoint()
        self.dirty()
        with self.store.connect() as connection:
            before = connection.execute(
                "SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                (self.tenant, self.source, self.parent),
            ).fetchone()
            start = connection.execute(
                "SELECT pg_current_wal_insert_lsn() AS lsn"
            ).fetchone()["lsn"]
            connection.execute("SAVEPOINT optional_writes")
            ack_plan = connection.execute(
                "EXPLAIN(ANALYZE,BUFFERS,WAL,FORMAT JSON) DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                (self.tenant, self.source, self.parent),
            ).fetchone()["QUERY PLAN"][0]
            update_plan = connection.execute(
                "EXPLAIN(ANALYZE,BUFFERS,WAL,FORMAT JSON) UPDATE canonical_evidence_documents SET actor_checkpoint=%s WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                (Jsonb(marker), self.tenant, self.source, self.parent),
            ).fetchone()["QUERY PLAN"][0]
            wal = connection.execute(
                "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s::pg_lsn) AS bytes",
                (start,),
            ).fetchone()["bytes"]
            after = connection.execute(
                "SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                (self.tenant, self.source, self.parent),
            ).fetchone()
            self.assertEqual(before, after)
            connection.execute("ROLLBACK TO SAVEPOINT optional_writes")
        print(
            json.dumps(
                dict(
                    actor_checkpoint_write_cost=dict(
                        checkpoint_bytes=len(json.dumps(marker).encode()),
                        ack_with_trigger_ms=ack_plan["Execution Time"],
                        marker_update_ms=update_plan["Execution Time"],
                        wal_bytes_including_queue_ack=int(wal),
                    )
                )
            )
        )
        self.assertEqual(self.checkpoint(), marker)
        self.assertIsNotNone(self.queue())


if __name__ == "__main__":
    unittest.main()
