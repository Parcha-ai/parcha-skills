#!/usr/bin/env python3
"""Deleted document cleanup preserves receipts and ordinary readable history."""

# ruff: noqa: E402
from pathlib import Path
from contextlib import contextmanager
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_deleted_chunk_bodies as chunk_tests
from recall_server.deleted_chunk_bodies import (
    clear_deleted_chunk_bodies,
    discover_deleted_chunk_bodies,
)


class DeletedDocumentBodyTests(chunk_tests.DeletedChunkBodyTests):
    def clear(self, **kwargs):
        kwargs.setdefault("include_document_bodies", True)
        before = self.documents()
        result = super().clear(**kwargs)
        if kwargs.get("apply") and kwargs["include_document_bodies"]:
            keys = set(
                kwargs.get("chunk_ids", (row["chunk_id"] for row in self.rows()))
            )
            affected = {
                row["document_id"]
                for row in self.rows()
                if row["chunk_id"] in keys
                and row["deleted_at"]
                and row["document_deleted"]
                and not row["is_current"]
            }
            for old, new in zip(before, self.documents(), strict=True):
                self.assertEqual(
                    new["text_redacted"],
                    "" if old["document_id"] in affected else old["text_redacted"],
                )
                self.assertEqual(
                    {k: v for k, v in old.items() if k != "text_redacted"},
                    {k: v for k, v in new.items() if k != "text_redacted"},
                )
        return result

    def write_legacy_tombstone(self, event=None):
        documents = self.documents()
        result = super().write_legacy_tombstone(event)
        with self.store.connect() as connection:
            for row in documents:
                connection.execute(
                    """UPDATE canonical_documents SET text_redacted=%s
                    WHERE tenant_id=%s AND source_id=%s AND document_id=%s
                      AND deleted_at IS NOT NULL AND NOT is_current""",
                    (
                        row["text_redacted"],
                        self.tenant,
                        self.source,
                        row["document_id"],
                    ),
                )
        return result

    def documents(self):
        with self.store.connect() as connection:
            return connection.execute(
                "SELECT * FROM canonical_documents WHERE tenant_id=%s AND source_id=%s ORDER BY document_id",
                (self.tenant, self.source),
            ).fetchall()

    def test_document_body_clear_is_explicit_and_preserves_metadata_and_history(self):
        self.write(self.event())
        self.write(self.event(revision=2))
        self.write(self.event(native="live"))
        self.write_legacy_tombstone()
        before = self.documents()
        deleted = [row for row in before if row["deleted_at"] and not row["is_current"]]
        self.assertEqual(len(deleted), 1)
        self.assertTrue(deleted[0]["text_redacted"])
        expected_bytes = len(deleted[0]["text_redacted"].encode())
        self.clear(apply=True, include_document_bodies=False)
        self.assertEqual(before, self.documents())
        plan = self.clear(include_document_bodies=True)
        self.assertEqual(plan["eligible_document_utf8_bytes"], expected_bytes)
        self.assertEqual(plan["cleared_documents"], 0)
        self.assertEqual(before, self.documents())
        result = self.clear(apply=True, include_document_bodies=True)
        self.assertEqual(result["cleared_documents"], 1)
        self.assertEqual(result["cleared_document_utf8_bytes"], expected_bytes)
        self.assertEqual(result["cleared_utf8_bytes"], 0)
        for old, new in zip(before, self.documents(), strict=True):
            self.assertEqual(
                new["text_redacted"], "" if old in deleted else old["text_redacted"]
            )
            self.assertEqual(
                {k: v for k, v in old.items() if k != "text_redacted"},
                {k: v for k, v in new.items() if k != "text_redacted"},
            )
        self.assertIsNone(self.resolve())
        repeated = self.clear(apply=True, include_document_bodies=True)
        self.assertEqual(repeated["cleared_documents"], 0)
        self.assertEqual(repeated["physical_bytes_reclaimed"], 0)

    def test_scalar_and_batch_tombstones_clear_document_bodies_and_preserve_history(
        self,
    ):
        for scalar in (True, False):
            with self.subTest(scalar=scalar):
                native, child = "parent-" + str(scalar), "child-" + str(scalar)
                for revision in (1, 2):
                    self.write(self.event(native=native, revision=revision))
                    value = self.event(native=child, revision=revision)
                    value["native_parent_id"] = native
                    self.write(value)
                before = self.documents()
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
                after = {row["document_id"]: row for row in self.documents()}
                newly_deleted = 0
                for old in before:
                    new = after[old["document_id"]]
                    deleted = old["is_current"] and new["deleted_at"] is not None
                    self.assertEqual(
                        new["text_redacted"], "" if deleted else old["text_redacted"]
                    )
                    for key in old:
                        if key not in ("is_current", "deleted_at", "text_redacted"):
                            self.assertEqual(old[key], new[key])
                    newly_deleted += int(deleted)
                self.assertEqual(newly_deleted, 2)

    def test_discovery_finds_document_only_bodies_and_counts_each_document_once(self):
        self.deleted_fixture()
        self.clear(apply=True, include_document_bodies=False)
        row = self.rows()[0]
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,
                    ordinal,receipt,text_redacted,text_sha256,deleted_at)
                VALUES (%s,%s,'chk_document_duplicate',%s,999,'recall://duplicate#item=999','',%s,now())""",
                (self.tenant, self.source, row["document_id"], row["text_sha256"]),
            )
        scope = dict(
            tenant_id=self.tenant, source_id=self.source, principal_id=self.principal
        )
        old_page = discover_deleted_chunk_bodies(self.store, **scope)
        self.assertEqual(old_page["candidate_chunk_ids"], ())
        page = discover_deleted_chunk_bodies(
            self.store, **scope, include_document_bodies=True
        )
        self.assertEqual(
            set(page["candidate_chunk_ids"]), {r["chunk_id"] for r in self.rows()}
        )
        expected = len(self.documents()[0]["text_redacted"].encode())
        self.assertEqual(page["eligible_document_utf8_bytes"], expected)
        result = self.clear(apply=True, include_document_bodies=True)
        self.assertEqual(result["cleared_documents"], 1)
        self.assertEqual(result["cleared_document_utf8_bytes"], expected)

    def test_document_clear_unknown_commit_preserves_unknown_outcome_and_replay(self):
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
                chunk_ids=tuple(row["chunk_id"] for row in self.rows()),
                apply=True,
                include_document_bodies=True,
            )
        self.assertIsNone(returned)
        self.assertTrue(all(not row["text_redacted"] for row in self.documents()))
        self.assertEqual(self.clear()["eligible_document_utf8_bytes"], 0)
        self.assertEqual(self.clear(apply=True)["cleared_documents"], 0)

    def test_failure_before_document_update_rolls_back_chunk_clear_too(self):
        self.deleted_fixture()
        before_chunks, before_documents = self.rows(), self.documents()
        actual = self.store

        class FailedUpdate:
            connect = actual.connect
            _set_statement_deadline = staticmethod(actual._set_statement_deadline)

            @staticmethod
            def _execute_bounded(connection, sql, args, deadline):
                if "UPDATE canonical_documents SET text_redacted=''" in sql:
                    raise TimeoutError("document update deadline")
                return actual._execute_bounded(connection, sql, args, deadline)

        with self.assertRaisesRegex(TimeoutError, "document update deadline"):
            clear_deleted_chunk_bodies(
                FailedUpdate(),
                tenant_id=self.tenant,
                source_id=self.source,
                principal_id=self.principal,
                chunk_ids=tuple(row["chunk_id"] for row in before_chunks),
                apply=True,
                include_document_bodies=True,
            )
        self.assertEqual(self.rows(), before_chunks)
        self.assertEqual(self.documents(), before_documents)


if __name__ == "__main__":
    unittest.main(verbosity=2)
