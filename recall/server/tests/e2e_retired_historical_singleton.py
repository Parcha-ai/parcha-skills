#!/usr/bin/env python3
"""Ordinary revisions preserve historical reads without rebuilding retired rows."""
import json
import os
import hashlib
from pathlib import Path
from contextlib import contextmanager

import psycopg
import time
import unittest
from unittest.mock import patch

from e2e_singleton_metadata_admission import SingletonMetadataAdmission
from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore


class RetiredHistoricalSingleton(SingletonMetadataAdmission):
    def setUp(self):
        super().setUp()
        self.plane.evidence_projector = CanonicalEvidenceProjector(
            self.store, EvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant,
        )

    def retired_revision(self):
        original = self.prepare()
        preview = self.admit(original)
        admitted = self.admit(original, apply=True, reviewed_plan=preview["plan"])
        self.assertEqual(admitted["retired_chunks"], 1)
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})
        self.enterContext(patch.object(self.store, "verified_singleton_receipts", True))
        self.enterContext(patch.object(self.store, "retain_archived_singletons", True))
        return original

    def archive_revision(self):
        original = self.retired_revision()
        before = self.store.resolve(
            original["receipt"], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive,
        )
        changed = self.ingest("turn/admit", "New independently captured revision β")
        self.assertEqual(changed["revision"], 2)
        return original, before, changed

    def resolve_old(self, original):
        return self.store.resolve(
            original["receipt"], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive,
        )

    def test_revision_keeps_verified_historical_singleton_retired(self):
        original = self.retired_revision()
        with patch.object(self.store, "verified_singleton_receipts", True):
            before = self.store.resolve(
                original["receipt"], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive,
            )
            self.assertEqual(before, self.public_response)
            changed = self.ingest("turn/admit", "New independently captured revision β")
            self.assertEqual(changed["revision"], 2)
            historical = self.store.resolve(
                original["receipt"], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive,
            )
            self.assertEqual(historical, before)
            self.assertIsNotNone(self.store.resolve(
                changed["receipt"], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive,
            ))
        with self.store.connect() as connection:
            old = connection.execute(
                """SELECT document.is_current,document.chunk_receipt_profile,
                   (SELECT count(*) FROM canonical_chunks chunk
                     WHERE chunk.tenant_id=document.tenant_id
                       AND chunk.source_id=document.source_id
                       AND chunk.document_id=document.document_id) AS chunks,
                   EXISTS(SELECT 1 FROM canonical_evidence_objects evidence
                     WHERE evidence.tenant_id=document.tenant_id
                       AND evidence.source_id=document.source_id
                       AND evidence.document_id=document.document_id) AS archived
                   FROM canonical_documents document
                   WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                (self.tenant, self.source, original["document_id"]),
            ).fetchone()
        print("ordinary_revision_storage=" + json.dumps(old), flush=True)
        self.assertFalse(old["is_current"])
        self.assertTrue(old["archived"])
        self.assertEqual(old["chunks"], 0)
        self.assertEqual(old["chunk_receipt_profile"], 1)

    def test_wrong_historical_revision_hash_or_receipt_count_refuses(self):
        from recall_server.receipt_membership import ReceiptMembershipError
        original, before, _ = self.archive_revision()
        for column, changed in (
            ("revision", 2), ("text_sha256", "0" * 64), ("receipt_count", 2),
        ):
            with self.subTest(column=column):
                with self.store.connect() as connection:
                    connection.execute(
                        f"""UPDATE canonical_evidence_objects SET {column}=%s
                            WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                        (changed, self.tenant, self.source, original["document_id"]),
                    )
                try:
                    with self.assertRaises(ReceiptMembershipError):
                        self.resolve_old(original)
                finally:
                    prior = {"revision": 1, "text_sha256": original["text_sha256"],
                             "receipt_count": 1}
                    with self.store.connect() as connection:
                        connection.execute(
                            f"""UPDATE canonical_evidence_objects SET {column}=%s
                                WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                            (prior[column], self.tenant, self.source, original["document_id"]),
                        )
                self.assertEqual(self.resolve_old(original), before)

    def test_missing_historical_artifact_does_not_restore_or_admit_body(self):
        from recall_server.chunk_bodies import ChunkBodyError
        original, _, _ = self.archive_revision()
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s AND document_id=%s",
                (self.tenant, original["document_id"]),
            ).fetchone()
        self.archive.delete_raw(self.plane.evidence_projector._reference(row))
        with self.assertRaises(ChunkBodyError):
            self.resolve_old(original)
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})

    def test_current_search_never_admits_the_historical_receipt(self):
        from recall_server.receipt_membership import lookup_current_search_membership
        original, _, changed = self.archive_revision()
        with self.store.connect() as connection, connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            allowed = lookup_current_search_membership(
                self.store, tenant_id=self.tenant, authorized_sources=(self.source,),
                receipts=(original["receipt"], changed["receipt"]),
                connection=connection, deadline_at=time.monotonic() + 3,
            )
        self.assertEqual(allowed, {(self.source, changed["receipt"])})

    def test_missing_compatibility_marker_rolls_back_the_revision(self):
        from recall_server.canonical import CanonicalLifecycleError
        original = self.retired_revision()
        before = self.resolve_old(original)
        with self.store.connect() as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version=95")
        try:
            with self.assertRaises(CanonicalLifecycleError):
                self.ingest("turn/admit", "A revision without the reader floor")
            self.assertEqual(self.resolve_old(original), before)
            self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})
            with self.store.connect() as connection:
                self.assertEqual(connection.execute(
                    "SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"], 1)
                self.assertEqual(connection.execute(
                    "SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"], 0)
        finally:
            with self.store.connect() as connection:
                connection.execute("INSERT INTO schema_migrations(version) VALUES(95)")

    def test_partial_forget_revokes_then_resumes_without_physical_rows(self):
        from recall_server.canonical import CanonicalPlane, CanonicalLifecycleError
        original, _, changed = self.archive_revision()
        request = dict(
            contract="recall.forget-request.v1", schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=original["receipt"], mode="explicit_forget",
            reason="owner_requested", requested_at="2026-10-20T00:00:00Z",
            idempotency_key="retired-historical-singleton-forget",
        )
        ownerless = CanonicalPlane(self.store, self.archive)
        with self.assertRaises(CanonicalLifecycleError):
            ownerless.forget(request)
        hidden = self.resolve_old(original)
        self.assertTrue(hidden is None or hidden["items"] == [])
        hidden_new = self.store.resolve(
            changed["receipt"], tenant_id=self.tenant, authorized_sources=(self.source,),
            chunk_body_archive=self.archive,
        )
        self.assertTrue(hidden_new is None or hidden_new["items"] == [])
        self.plane.forget(request)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["n"], 0)
        self.assertIsNone(self.resolve_old(original))

    def test_chunk_appearing_after_staging_refuses_revision(self):
        from recall_server.canonical import CanonicalLifecycleError
        from recall_server.receipt_membership import ReceiptMembershipError
        original = self.retired_revision()
        put = self.plane.evidence_projector.projection.put

        def put_then_insert(**kwargs):
            reference = put(**kwargs)
            with self.store.connect() as connection:
                connection.execute(
                    """INSERT INTO canonical_chunks(
                       tenant_id,source_id,chunk_id,document_id,ordinal,
                       receipt,text_redacted,text_sha256,created_at)
                       SELECT tenant_id,source_id,%s,document_id,0,%s,'',
                              text_sha256,created_at FROM canonical_documents
                        WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                    (original["chunk_id"], original["receipt"], self.tenant,
                     self.source, original["document_id"]),
                )
            return reference

        with patch.object(self.plane.evidence_projector.projection, "put", put_then_insert):
            with self.assertRaises((CanonicalLifecycleError, ReceiptMembershipError)):
                self.ingest("turn/admit", "A revision after physical metadata appeared")
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["n"], 1)


    def test_enabled_writer_without_outgoing_archive_keeps_original_restore(self):
        original = self.retired_revision()
        before = self.resolve_old(original)
        self.plane.evidence_projector = None
        self.ingest("turn/admit", "Fallback writer retains physical historical metadata")
        self.assertEqual(self.state(original), {"chunk_receipt_profile": None, "chunks": 1})
        self.assertEqual(self.resolve_old(original), before)

    def test_owner_grant_revoked_after_staging_refuses_revision(self):
        from recall_server.canonical import CanonicalLifecycleError, REGISTRATION_CACHE
        original = self.retired_revision()
        put = self.plane.evidence_projector.projection.put

        def revoke_after_upload(**kwargs):
            reference = put(**kwargs)
            with self.store.connect() as connection:
                connection.execute(
                    "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
            REGISTRATION_CACHE.invalidate_tenant(self.tenant)
            return reference

        with patch.object(self.plane.evidence_projector.projection, "put", revoke_after_upload):
            with self.assertRaises(CanonicalLifecycleError):
                self.ingest("turn/admit", "Revoked source cannot publish history")
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["revision"], 1)

    def test_parent_forget_during_staging_refuses_revision(self):
        from recall_server.canonical import CanonicalLifecycleError
        original = self.retired_revision()
        put = self.plane.evidence_projector.projection.put

        def forget_after_upload(**kwargs):
            reference = put(**kwargs)
            with self.store.connect() as connection:
                connection.execute("""INSERT INTO forget_tombstones(
                    tenant_id,source_id,target_identity_sha256,mode,reason,deleted_at,status)
                    VALUES (%s,%s,%s,'explicit_forget','owner_requested',now(),'deleting')""",
                    (self.tenant, self.source, hashlib.sha256(
                        '\x1f'.join((self.tenant, self.source, 'session')).encode()).hexdigest()))
            return reference

        with patch.object(self.plane.evidence_projector.projection, "put", forget_after_upload):
            with self.assertRaises(CanonicalLifecycleError):
                self.ingest("turn/admit", "Pending Forget blocks archive admission")
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["n"], 0)

    def test_later_tombstone_revokes_historical_receipt(self):
        original, _, _ = self.archive_revision()
        self.ingest("turn/admit", "", tombstone=True)
        hidden = self.resolve_old(original)
        self.assertTrue(hidden is None or hidden["items"] == [])
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})

    def test_actual_commit_lost_ack_keeps_history_owned(self):
        original = self.retired_revision()
        before = self.resolve_old(original)
        admit = self.plane.evidence_projector.admit_outgoing
        transaction = psycopg.Connection.transaction
        armed = False

        def admitted(*args, **kwargs):
            nonlocal armed
            result = admit(*args, **kwargs)
            armed = True
            return result

        @contextmanager
        def lost_ack(connection, *args, **kwargs):
            nonlocal armed
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                armed = False
                raise psycopg.OperationalError("synthetic lost commit acknowledgement")

        with patch.object(self.plane.evidence_projector, "admit_outgoing", admitted), \
                patch.object(psycopg.Connection, "transaction", lost_ack):
            with self.assertRaisesRegex(psycopg.OperationalError, "lost commit acknowledgement"):
                self.ingest("turn/admit", "Durable revision with no acknowledged result")
        self.projector.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.resolve_old(original), before)
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["revision"], 2)

    def test_marker_addition_does_not_rewrite_documents_or_chunks(self):
        original = self.retired_revision()
        schema = (Path(__file__).resolve().parents[1] / "schema" /
                  "095_historical_singleton_receipts.sql").read_text()
        query = """SELECT oid,relfilenode FROM pg_class WHERE oid IN
                   ('canonical_documents'::regclass,'canonical_chunks'::regclass)
                   ORDER BY oid"""
        with self.store.connect() as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version=95")
            before = connection.execute(query).fetchall()
            row_before = connection.execute(
                "SELECT xmin::text AS xmin,* FROM canonical_documents WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall()
            connection.execute(schema)
            self.assertEqual(connection.execute(query).fetchall(), before)
            self.assertEqual(connection.execute(
                "SELECT xmin::text AS xmin,* FROM canonical_documents WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall(), row_before)
        self.assertEqual(self.state(original), {"chunk_receipt_profile": 1, "chunks": 0})

    def test_writer_is_default_off_and_requires_existing_singleton_reader(self):
        from recall_server.db import BrainStore
        with patch.dict(os.environ, {"RECALL_RETAIN_ARCHIVED_SINGLETONS": "0"}):
            disabled = BrainStore(self.store.dsn)
            self.addCleanup(disabled.close)
            self.assertFalse(disabled.retain_archived_singletons)
        with patch.dict(os.environ, {"RECALL_RETAIN_ARCHIVED_SINGLETONS": "1"}):
            with self.assertRaises(ValueError):
                BrainStore(self.store.dsn, verified_singleton_receipts=False)
            enabled = BrainStore(self.store.dsn, verified_singleton_receipts=True)
            self.addCleanup(enabled.close)
            self.assertTrue(enabled.retain_archived_singletons)
        with patch.dict(os.environ, {"RECALL_RETAIN_ARCHIVED_SINGLETONS": "true"}):
            with self.assertRaises(ValueError):
                BrainStore(self.store.dsn, verified_singleton_receipts=True)


if __name__ == "__main__":
    suite = unittest.TestSuite([
        RetiredHistoricalSingleton(name)
        for name in RetiredHistoricalSingleton.__dict__
        if name.startswith("test_")
    ])
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
