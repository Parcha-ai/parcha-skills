#!/usr/bin/env python3
"""Deleted body maintenance preserves canonical authority and lifecycle behavior."""

# ruff: noqa: E402
from pathlib import Path
from contextlib import contextmanager
import hashlib
import json
import os
from unittest.mock import patch

import psycopg
import sys
import unittest

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_historical_bundle_reader as fixture
from recall_server.deleted_chunk_bodies import (
    clear_deleted_chunk_bodies,
    discover_deleted_chunk_bodies,
)
from recall_server.chunk_retirement import ChunkRetirementError, retire_current_chunks
from recall_server.canonical import CanonicalLifecycleError
from recall_server.projectors import canonical_json


class DeletedChunkBodyTests(unittest.TestCase):
    setUpClass = classmethod(fixture.HistoricalBundleReaderTests.setUpClass.__func__)
    tearDownClass = classmethod(
        fixture.HistoricalBundleReaderTests.tearDownClass.__func__
    )
    setUp = fixture.HistoricalBundleReaderTests.setUp
    event = fixture.HistoricalBundleReaderTests.event
    write = fixture.HistoricalBundleReaderTests.write
    resolve = fixture.HistoricalBundleReaderTests.resolve
    forget = fixture.HistoricalBundleReaderTests.forget
    project = fixture.HistoricalBundleReaderTests.project

    def tombstone(self, native="turn"):
        value = self.event(native=native, revision=99)
        content = {"target_native_id": native}
        value.update(
            kind="tombstone",
            content=content,
            content_sha256=hashlib.sha256(canonical_json(content)).hexdigest(),
        )
        value["provenance"]["artifact_ref"] = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=canonical_json(content),
            media_type="application/json",
            created_at="2026-10-14T00:00:00Z",
        )
        return value

    def restore_prevention_fixture(self, rows):
        # Reproduce the production backlog written before tombstones cleared
        # text. This fixture restores only the strict already-deleted cohort.
        with self.store.connect() as c:
            for row in rows:
                c.execute(
                    """UPDATE canonical_chunks chunk SET text_redacted=%s
                    FROM canonical_documents document
                    WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND chunk.chunk_id=%s
                      AND document.tenant_id=chunk.tenant_id AND document.source_id=chunk.source_id
                      AND document.document_id=chunk.document_id
                      AND chunk.deleted_at IS NOT NULL AND document.deleted_at IS NOT NULL
                      AND NOT document.is_current""",
                    (row["text_redacted"], self.tenant, self.source, row["chunk_id"]),
                )

    def write_legacy_tombstone(self, event=None):
        rows = self.rows()
        result = self.write(event or self.tombstone())
        self.restore_prevention_fixture(rows)
        return result

    def rows(self):
        with self.store.connect() as c:
            return c.execute(
                """SELECT c.*,d.is_current,d.deleted_at AS document_deleted,
                d.revision FROM canonical_chunks c JOIN canonical_documents d
                USING(tenant_id,source_id,document_id)
                WHERE c.tenant_id=%s AND c.source_id=%s ORDER BY c.chunk_id""",
                (self.tenant, self.source),
            ).fetchall()

    def clear(self, **kwargs):
        return clear_deleted_chunk_bodies(
            self.store,
            tenant_id=self.tenant,
            source_id=self.source,
            principal_id=kwargs.pop("principal_id", self.principal),
            chunk_ids=kwargs.pop(
                "chunk_ids", tuple(r["chunk_id"] for r in self.rows())
            ),
            **kwargs,
        )

    def test_deleted_text_clears_without_touching_identity_hash_or_live_history(self):
        self.write(self.event())
        self.write(self.event(revision=2))
        self.write(self.event(native="live"))
        self.write_legacy_tombstone()
        before = self.rows()
        eligible = [
            r
            for r in before
            if r["deleted_at"] and r["document_deleted"] and not r["is_current"]
        ]
        self.assertTrue(eligible)
        with unittest.mock.patch.object(
            self.archive,
            "read_raw",
            side_effect=AssertionError("no archive proof for deleted body"),
        ):
            result = self.clear(apply=True)
        self.assertEqual(result["cleared_chunks"], len(eligible))
        self.assertEqual(
            result["cleared_utf8_bytes"],
            sum(len(r["text_redacted"].encode()) for r in eligible),
        )
        after = self.rows()
        allowed = {r["chunk_id"] for r in eligible}
        # The fixture retains a generated search_vector; production retired it.
        # Its derived value follows text, unlike identity/hash metadata.
        for old, new in zip(before, after, strict=True):
            self.assertEqual(
                new["text_redacted"],
                "" if old["chunk_id"] in allowed else old["text_redacted"],
            )
            self.assertEqual(
                {
                    k: v
                    for k, v in old.items()
                    if k not in ("text_redacted", "search_vector")
                },
                {
                    k: v
                    for k, v in new.items()
                    if k not in ("text_redacted", "search_vector")
                },
            )
        self.assertIsNone(self.resolve())

    def deleted_fixture(self):
        original = self.event()
        result = self.write(original)
        self.write_legacy_tombstone()
        return original, result

    def test_dry_run_and_repeated_clear_are_exact_and_idempotent(self):
        self.deleted_fixture()
        before = self.rows()
        plan = self.clear()
        self.assertGreater(plan["eligible_utf8_bytes"], 0)
        self.assertEqual(plan["cleared_chunks"], 0)
        self.assertEqual(before, self.rows())
        applied = self.clear(apply=True)
        self.assertEqual(applied["cleared_utf8_bytes"], plan["eligible_utf8_bytes"])
        repeated = self.clear(apply=True)
        self.assertEqual(repeated["cleared_chunks"], 0)
        self.assertEqual(repeated["deleted_chunks"], plan["deleted_chunks"])
        self.assertEqual(repeated["physical_bytes_reclaimed"], 0)

    def test_same_hash_replay_after_tombstone_does_not_restore_erased_text(self):
        original, first = self.deleted_fixture()
        self.clear(apply=True)
        before = self.rows()
        replay = self.write(original)
        self.assertEqual(replay["inserted"], 0)
        self.assertEqual(replay["duplicate_events"], 1)
        self.assertEqual(first["receipts"], replay["receipts"])
        self.assertEqual(before, self.rows())
        self.assertIsNone(self.resolve())

    def test_new_revision_after_tombstone_uses_new_body_and_keeps_deleted_rows_empty(
        self,
    ):
        self.deleted_fixture()
        self.clear(apply=True)
        deleted = self.rows()
        self.write(self.event(revision=3))
        latest = self.resolve(revision=3)
        self.assertTrue(latest["items"])
        self.assertIn("revision3", latest["items"][0]["text_redacted"])
        self.assertIsNone(self.resolve(revision=1))
        after = {r["chunk_id"]: r for r in self.rows()}
        for row in deleted:
            self.assertEqual(after[row["chunk_id"]], row)
        self.assertEqual(self.clear(apply=True)["cleared_chunks"], 0)

    def test_ordinary_historical_resolve_and_deleted_restore_fence_remain(self):
        self.write(self.event())
        original = self.resolve()
        self.write(self.event(revision=2))
        self.assertEqual(self.clear(apply=True)["cleared_chunks"], 0)
        self.assertEqual(self.resolve(), original)
        self.write_legacy_tombstone()
        self.clear(apply=True)
        deleted_docs = tuple(
            {r["document_id"] for r in self.rows() if r["document_deleted"]}
        )
        with patch.object(
            self.archive,
            "read_raw",
            side_effect=AssertionError("deleted restore must not read archive"),
        ):
            with self.assertRaisesRegex(ChunkRetirementError, "target_ineligible"):
                retire_current_chunks(
                    self.store,
                    self.archive,
                    tenant_id=self.tenant,
                    source_id=self.source,
                    document_ids=deleted_docs,
                    restore=True,
                )
        self.assertIsNone(self.resolve(revision=2))

    def test_forget_retry_after_archive_failure_needs_metadata_not_deleted_text(self):
        self.write(self.event())
        with patch.object(
            self.archive, "delete", side_effect=RuntimeError("provider failure")
        ):
            with self.assertRaisesRegex(
                CanonicalLifecycleError, "archive_delete_failed"
            ):
                self.forget()
        self.assertGreater(self.clear(apply=True)["cleared_chunks"], 0)
        result = self.forget()
        self.assertEqual(result["status"], "deleted")
        self.assertEqual(self.rows(), [])
        self.assertIsNone(self.resolve())

    def test_queued_projection_after_clear_ignores_deleted_body_and_preserves_live_session(
        self,
    ):
        self.write(self.event())
        self.write(self.event(native="live"))
        self.project()
        self.write_legacy_tombstone()
        self.clear(apply=True)
        self.project()
        live = self.store.resolve(
            f"recall://{self.source}/live?rev=1#item=0",
            tenant_id=self.tenant,
            authorized_sources=(self.source,),
            chunk_body_archive=self.archive,
        )
        self.assertTrue(live["items"])
        self.assertIsNone(self.resolve())
        with self.store.connect() as c:
            remaining = c.execute(
                "SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            ).fetchall()
        self.assertEqual(remaining, [{"native_parent_id": "live"}])

    def test_revoked_or_wrong_scope_refuses_clear_and_keeps_bodies(self):
        self.deleted_fixture()
        before = self.rows()
        for scope in (
            {"tenant_id": self.tenant + "-wrong", "source_id": self.source},
            {"tenant_id": self.tenant, "source_id": self.source + "-wrong"},
        ):
            with self.assertRaisesRegex(
                ChunkRetirementError, "retirement_owner_required"
            ):
                clear_deleted_chunk_bodies(
                    self.store,
                    **scope,
                    principal_id=self.principal,
                    chunk_ids=tuple(r["chunk_id"] for r in before),
                    apply=True,
                )
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            )
        for apply in (False, True):
            with self.assertRaisesRegex(
                ChunkRetirementError, "retirement_owner_required"
            ):
                self.clear(apply=apply)
        self.assertEqual(before, self.rows())

    def test_native_writer_lock_refuses_entire_exact_batch(self):
        self.deleted_fixture()
        before = self.rows()
        with psycopg.connect(self.store.dsn) as c, c.transaction():
            c.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"v2\x1f{self.tenant}\x1f{self.source}\x1fturn",),
            )
            with self.assertRaisesRegex(
                ChunkRetirementError, "deleted_chunk_lock_busy"
            ):
                self.clear(apply=True)
        self.assertEqual(before, self.rows())

    def multiple_deleted_natives(self):
        for native in ("z-last", "a-first", "m-middle"):
            self.write(self.event(native=native))
            self.write(self.event(native=native, revision=2))
            self.write_legacy_tombstone(self.tombstone(native))

    def test_native_locks_use_one_ordered_query_for_distinct_natives(self):
        self.multiple_deleted_natives()
        original = self.store._execute_bounded
        locks = []

        def measured(connection, sql, args, deadline):
            if "pg_try_advisory_xact_lock" in sql:
                locks.append((sql, args))
            return original(connection, sql, args, deadline)

        with patch.object(self.store, "_execute_bounded", side_effect=measured):
            result = self.clear(apply=True)
        self.assertEqual(result["cleared_chunks"], 3)
        self.assertEqual(len(locks), 1)
        self.assertIn("WITH ORDINALITY", locks[0][0])
        self.assertEqual(
            locks[0][1][0],
            [
                f"v2\x1f{self.tenant}\x1f{self.source}\x1f{native}"
                for native in ("a-first", "m-middle", "z-last")
            ],
        )

    def test_any_contended_native_rolls_back_all_keys_and_releases_other_locks(self):
        self.multiple_deleted_natives()
        before = self.rows()
        for blocked in ("a-first", "m-middle", "z-last"):
            with (
                self.subTest(blocked=blocked),
                psycopg.connect(self.store.dsn) as blocker,
            ):
                key = f"v2\x1f{self.tenant}\x1f{self.source}\x1f{blocked}"
                blocker.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (key,)
                )
                with self.assertRaisesRegex(
                    ChunkRetirementError, "deleted_chunk_lock_busy"
                ):
                    self.clear(apply=True)
                self.assertEqual(self.rows(), before)
                with psycopg.connect(self.store.dsn) as observer:
                    for native in ("a-first", "m-middle", "z-last"):
                        key = f"v2\x1f{self.tenant}\x1f{self.source}\x1f{native}"
                        got = observer.execute(
                            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))",
                            (key,),
                        ).fetchone()[0]
                        self.assertEqual(got, native != blocked)
        self.assertEqual(self.clear(apply=True)["cleared_chunks"], 3)

    def test_lost_commit_ack_returns_no_success_and_exact_retry_observes_empty(self):
        self.deleted_fixture()
        actual = self.store

        class UnknownCommit:
            _execute_bounded = staticmethod(actual._execute_bounded)
            _set_statement_deadline = staticmethod(actual._set_statement_deadline)

            @contextmanager
            def connect(self):
                with actual.connect() as connection:
                    yield connection
                raise RuntimeError("commit acknowledgment lost")

        returned = None
        with self.assertRaisesRegex(RuntimeError, "acknowledgment lost"):
            returned = clear_deleted_chunk_bodies(
                UnknownCommit(),
                tenant_id=self.tenant,
                source_id=self.source,
                principal_id=self.principal,
                chunk_ids=tuple(r["chunk_id"] for r in self.rows()),
                apply=True,
            )
        self.assertIsNone(returned)
        reconciled = self.clear()
        self.assertEqual(reconciled["eligible_utf8_bytes"], 0)
        self.assertGreater(reconciled["deleted_chunks"], 0)
        self.assertEqual(self.clear(apply=True)["cleared_chunks"], 0)

    def test_scalar_tombstone_and_linked_children_clear_without_erasing_survivor(self):
        self.write(self.event())
        child = self.event(native="child")
        child["native_parent_id"] = "turn"
        self.write(child)
        self.write(self.event(native="survivor"))
        before_tombstone = self.rows()
        tombstone = self.tombstone()
        self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.history",
            artifact_ref=tombstone["provenance"]["artifact_ref"],
            envelope=tombstone,
            text_redacted="",
        )
        self.restore_prevention_fixture(before_tombstone)
        result = self.clear(apply=True)
        self.assertEqual(result["cleared_chunks"], 2)
        rows = self.rows()
        self.assertEqual(sum(bool(row["text_redacted"]) for row in rows), 1)
        live = self.store.resolve(
            f"recall://{self.source}/survivor?rev=1#item=0",
            tenant_id=self.tenant,
            authorized_sources=(self.source,),
        )
        self.assertTrue(live["items"])
        child = self.store.resolve(
            f"recall://{self.source}/child?rev=1#item=0",
            tenant_id=self.tenant,
            authorized_sources=(self.source,),
        )
        self.assertFalse(child and child["items"])

    def test_company_admin_uses_own_exact_read_grant_without_changing_source_owner(
        self,
    ):
        self.deleted_fixture()
        admin = "principal:company-maintenance"
        organization = "organization:" + self.tenant
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO brain_principals VALUES(%s,%s,now())", (self.tenant, admin)
            )
            c.execute(
                "INSERT INTO brain_organizations VALUES(%s,'company','Maintenance test',now())",
                (organization,),
            )
            c.execute(
                "INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','maintenance')",
                (self.tenant, organization),
            )
            c.execute(
                "INSERT INTO brain_memberships VALUES(%s,%s,'admin',now())",
                (organization, admin),
            )
            c.execute(
                "INSERT INTO brain_access_grants VALUES(%s,%s,'admin',now())",
                (self.tenant, admin),
            )
            c.execute(
                "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'read',now())",
                (self.tenant, admin, self.source),
            )
            before = c.execute(
                "SELECT * FROM canonical_source_grants WHERE tenant_id=%s ORDER BY principal_id",
                (self.tenant,),
            ).fetchall()
        self.assertGreater(
            self.clear(principal_id=admin, apply=True)["cleared_chunks"], 0
        )
        with self.store.connect() as c:
            after = c.execute(
                "SELECT * FROM canonical_source_grants WHERE tenant_id=%s ORDER BY principal_id",
                (self.tenant,),
            ).fetchall()
            owner = c.execute(
                "SELECT owner_principal_id FROM canonical_sources WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            ).fetchone()
            c.execute(
                "DELETE FROM brain_memberships WHERE organization_id=%s AND principal_id=%s",
                (organization, admin),
            )
        self.assertEqual(before, after)
        self.assertEqual(owner["owner_principal_id"], self.principal)
        with self.assertRaisesRegex(ChunkRetirementError, "retirement_owner_required"):
            self.clear(principal_id=admin, apply=True)

    def test_authority_grant_stays_locked_between_snapshot_and_clear(self):
        self.deleted_fixture()
        actual = self.store
        observed = []
        owner = self

        class ObservedStore:
            connect = actual.connect
            _set_statement_deadline = staticmethod(actual._set_statement_deadline)

            @staticmethod
            def _execute_bounded(connection, sql, args, deadline):
                result = actual._execute_bounded(connection, sql, args, deadline)
                if (
                    "FROM canonical_chunks chunk JOIN canonical_documents" in sql
                    and not observed
                ):
                    with psycopg.connect(actual.dsn) as contender:
                        with owner.assertRaises(psycopg.errors.LockNotAvailable):
                            with contender.transaction():
                                contender.execute("SET LOCAL lock_timeout='50ms'")
                                contender.execute(
                                    "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                                    (owner.tenant, owner.source),
                                )
                    observed.append("revocation_blocked_by_authority_lock")
                return result

        result = clear_deleted_chunk_bodies(
            ObservedStore(),
            tenant_id=self.tenant,
            source_id=self.source,
            principal_id=self.principal,
            chunk_ids=tuple(r["chunk_id"] for r in self.rows()),
            apply=True,
        )
        self.assertGreater(result["cleared_chunks"], 0)
        self.assertEqual(observed, ["revocation_blocked_by_authority_lock"])

    def test_foreign_or_live_keys_inside_authorized_source_are_not_cleared(self):
        self.deleted_fixture()
        self.write(self.event(native="live"))
        before = self.rows()
        live = next(r for r in before if r["is_current"])
        result = self.clear(apply=True, chunk_ids=(live["chunk_id"], "chk_unknown"))
        self.assertEqual(result["cleared_chunks"], 0)
        self.assertEqual(before, self.rows())

    def test_slack_terminal_tombstone_keeps_reingest_suppressed_after_clear(self):
        native = "slack:C123:123.456"
        original = self.event(native=native)
        original["provenance"]["connector_id"] = "slack.messages"
        self.write(original)
        deleted = self.tombstone(native)
        deleted["provenance"]["connector_id"] = "slack.messages"
        self.write_legacy_tombstone(deleted)
        self.clear(apply=True)
        replacement = self.event(native=native, revision=3)
        replacement["provenance"]["connector_id"] = "slack.messages"
        result = self.write(replacement)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["suppressed_deleted_events"], 1)
        self.assertTrue(
            all(not r["text_redacted"] and not r["is_current"] for r in self.rows())
        )

    def test_new_scalar_and_batch_tombstones_clear_only_current_deleted_body(self):
        for scalar in (True, False):
            with self.subTest(scalar=scalar):
                native = "parent-" + str(scalar)
                child = "child-" + str(scalar)
                for revision in (1, 2):
                    self.write(self.event(native=native, revision=revision))
                    value = self.event(native=child, revision=revision)
                    value["native_parent_id"] = native
                    self.write(value)
                before = self.rows()
                with self.store.connect() as c:
                    event_metadata = c.execute(
                        "SELECT event_id,content_sha256,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY event_id",
                        (self.tenant, self.source),
                    ).fetchall()
                tombstone = self.tombstone(native)
                if scalar:
                    self.plane.ingest_document(
                        tenant_id=self.tenant,
                        principal_id=self.principal,
                        connector_id="synthetic.history",
                        artifact_ref=tombstone["provenance"]["artifact_ref"],
                        envelope=tombstone,
                        text_redacted="",
                    )
                else:
                    self.write(tombstone)
                after = {r["chunk_id"]: r for r in self.rows()}
                cleared = 0
                preserved_history = 0
                for old in before:
                    row = after[old["chunk_id"]]
                    if old["is_current"] and row["document_deleted"]:
                        self.assertEqual(row["text_redacted"], "")
                        cleared += 1
                    else:
                        self.assertEqual(row["text_redacted"], old["text_redacted"])
                        preserved_history += int(
                            old["revision"] == 1 and row["document_deleted"] is None
                        )
                    for field in (
                        "chunk_id",
                        "document_id",
                        "receipt",
                        "ordinal",
                        "text_sha256",
                    ):
                        self.assertEqual(row[field], old[field])
                self.assertEqual(cleared, 2)
                self.assertGreaterEqual(preserved_history, 2)
                with self.store.connect() as c:
                    events = c.execute(
                        "SELECT event_id,content_sha256,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY event_id",
                        (self.tenant, self.source),
                    ).fetchall()
                self.assertTrue(all(row in events for row in event_metadata))

    def test_discovery_advances_over_live_and_empty_pages_with_frozen_source_bound(
        self,
    ):
        for native in ("one", "two", "three", "four", "live"):
            self.write(self.event(native=native))
            if native != "live":
                self.write_legacy_tombstone(self.tombstone(native))
        all_rows = self.rows()
        first_deleted = next(r for r in all_rows if r["document_deleted"])
        self.clear(apply=True, chunk_ids=(first_deleted["chunk_id"],))
        expected = {
            r["chunk_id"]
            for r in self.rows()
            if r["document_deleted"] and r["text_redacted"]
        }
        seen = set()
        after = None
        through = None
        scanned = 0
        pages = 0
        while True:
            page = discover_deleted_chunk_bodies(
                self.store,
                tenant_id=self.tenant,
                source_id=self.source,
                principal_id=self.principal,
                after_chunk_id=after,
                through_chunk_id=through,
                limit=1,
            )
            self.assertLessEqual(page["scanned_keys"], 1)
            self.assertFalse(seen.intersection(page["candidate_chunk_ids"]))
            seen.update(page["candidate_chunk_ids"])
            scanned += page["scanned_keys"]
            pages += 1
            if through is not None:
                self.assertEqual(page["through_chunk_id"], through)
            if page["pass_complete"]:
                break
            self.assertNotEqual(page["after_chunk_id"], after)
            after, through = page["after_chunk_id"], page["through_chunk_id"]
            self.assertLess(pages, 10)
        self.assertEqual(seen, expected)
        self.assertEqual(scanned, len(all_rows))
        self.assertEqual(pages, len(all_rows))
        self.assertTrue(through)

    def test_discovery_uses_existing_source_key_index_on_populated_relation(self):
        self.deleted_fixture()
        document = self.rows()[0]["document_id"]
        with self.store.connect() as c:
            c.execute(
                """INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,
                ordinal,receipt,text_redacted,text_sha256,deleted_at)
                SELECT %s,%s,'chk_'||lpad(to_hex(n),32,'0'),%s,10000+n,
                    'recall://plan/test?rev=1#item='||n,'x',%s,now()
                FROM generate_series(1,5000) n""",
                (self.tenant, self.source, document, hashlib.sha256(b"x").hexdigest()),
            )
            c.execute("ANALYZE canonical_chunks")
            c.execute("ANALYZE canonical_documents")
        actual = self.store
        plans = []

        class ExplainedStore:
            connect = actual.connect
            _set_statement_deadline = staticmethod(actual._set_statement_deadline)

            @staticmethod
            def _execute_bounded(connection, sql, args, deadline):
                if (
                    "SELECT chunk_id FROM canonical_chunks" in sql
                    or "FROM canonical_chunks chunk JOIN canonical_documents" in sql
                ):
                    plan = actual._execute_bounded(
                        connection,
                        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + sql,
                        args,
                        deadline,
                    ).fetchone()["QUERY PLAN"][0]
                    plans.append(dict(sql=sql, plan=plan))
                return actual._execute_bounded(connection, sql, args, deadline)

        first = discover_deleted_chunk_bodies(
            ExplainedStore(),
            tenant_id=self.tenant,
            source_id=self.source,
            principal_id=self.principal,
            limit=50,
        )
        second = discover_deleted_chunk_bodies(
            ExplainedStore(),
            tenant_id=self.tenant,
            source_id=self.source,
            principal_id=self.principal,
            limit=50,
            after_chunk_id=first["after_chunk_id"],
            through_chunk_id=first["through_chunk_id"],
        )
        self.assertEqual(first["scanned_keys"], 50)
        self.assertEqual(second["scanned_keys"], 50)
        self.assertEqual(first["eligible_utf8_bytes"], 50)
        self.assertEqual(second["eligible_utf8_bytes"], 50)
        self.assertFalse(
            set(first["candidate_chunk_ids"]).intersection(
                second["candidate_chunk_ids"]
            )
        )
        self.assertEqual(len(plans), 5)

        def nodes(plan):
            yield plan
            for child in plan.get("Plans", []):
                yield from nodes(child)

        for item in plans:
            if "SELECT chunk_id FROM canonical_chunks" in item["sql"]:
                scans = [
                    node
                    for node in nodes(item["plan"]["Plan"])
                    if node.get("Relation Name") == "canonical_chunks"
                ]
                self.assertEqual(len(scans), 1)
                self.assertIn(scans[0]["Node Type"], ("Index Scan", "Index Only Scan"))
                self.assertEqual(scans[0]["Index Name"], "canonical_chunks_pkey")
                self.assertIn("tenant_id", scans[0]["Index Cond"])
                self.assertIn("source_id", scans[0]["Index Cond"])
        if os.environ.get("RECALL_DISCOVERY_PLAN_PATH"):
            Path(os.environ["RECALL_DISCOVERY_PLAN_PATH"]).write_text(
                json.dumps(plans, indent=2, default=str) + "\n"
            )

    def test_discovery_requires_current_source_authority_and_frozen_cursor(self):
        self.deleted_fixture()
        with self.assertRaisesRegex(
            ChunkRetirementError, "deleted_chunk_scope_invalid"
        ):
            discover_deleted_chunk_bodies(
                self.store,
                tenant_id=self.tenant,
                source_id=self.source,
                principal_id=self.principal,
                after_chunk_id=self.rows()[0]["chunk_id"],
            )
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            )
        with self.assertRaisesRegex(ChunkRetirementError, "retirement_owner_required"):
            discover_deleted_chunk_bodies(
                self.store,
                tenant_id=self.tenant,
                source_id=self.source,
                principal_id=self.principal,
            )


if __name__ == "__main__":
    unittest.main()
