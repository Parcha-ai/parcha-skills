#!/usr/bin/env python3
"""Exact historical bodies survive logical cleanup without SQL restoration."""

# ruff: noqa: E402
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.chunk_bodies import ChunkBodyError
from recall_server.db import BrainStore
from recall_server.evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceProjectionStore,
)
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json


class HistoricalBundleReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = os.environ["RECALL_DATABASE_URL"]
        cls.database = "recall_historical_owner_" + uuid.uuid4().hex
        settings = conninfo_to_dict(cls.admin)
        settings["dbname"] = cls.database
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{cls.database}"')
        cls.store = BrainStore(make_conninfo(**settings))
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'DROP DATABASE "{cls.database}"')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="recall-historical-owner-")
        self.addCleanup(self.temporary.cleanup)
        self.tenant = "tenant:history-" + uuid.uuid4().hex[:8]
        self.source, self.principal = "source:history", "principal:history"
        self.archive = FilesystemArchiveStore(
            Path(self.temporary.name), namespace_key=b"h" * 32
        )
        self.evidence = CanonicalEvidenceProjector(
            self.store,
            EvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant,
        )
        self.logical = CanonicalLogicalEvidenceProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant,
            raw_archive=self.archive,
        )
        self.plane = CanonicalPlane(
            self.store,
            self.archive,
            evidence_projector=self.evidence,
            chunk_body_archive=self.archive,
        )
        self.gateway = CanonicalArchiveGateway(
            self.store, self.archive, tenant_id=self.tenant, principal_id=self.principal
        )

    def event(self, native="turn", revision=1):
        content = {
            "text": (f"{native} revision{revision}: α 🧠\n" * 100),
            "role": "user",
        }
        reference = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=canonical_json(content),
            media_type="application/json",
            created_at="2026-10-13T00:00:00Z",
        )
        return dict(
            schema_version=1,
            source_id=self.source,
            native_id=native,
            native_parent_id=native,
            kind="connector_record",
            occurred_at="2026-10-13T00:00:00Z",
            observed_at="2026-10-13T00:00:00Z",
            principal_id=self.principal,
            visibility="private",
            content_type="application/json",
            content=content,
            provenance={"connector_id": "synthetic.history", "artifact_ref": reference},
            content_sha256=hashlib.sha256(canonical_json(content)).hexdigest(),
        )

    def write(self, event):
        return self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[event]
        )

    def project(self):
        return self.logical.project_pending(
            tenant_id=self.tenant, batch_size=100, max_batches=10, upload_concurrency=1
        )

    def resolve(self, revision=1, tenant=None, sources=None):
        return self.store.resolve(
            f"recall://{self.source}/turn?rev={revision}#item=0",
            tenant_id=tenant or self.tenant,
            authorized_sources=sources or (self.source,),
            chunk_body_archive=self.archive,
        )

    def clear(self, revision=1):
        # Simulate an already proof-cleared row; original clearer tests remain unchanged.
        with self.store.connect() as c, c.transaction():
            c.execute(
                """UPDATE canonical_chunks chunk SET text_redacted=''
                FROM canonical_documents document WHERE chunk.tenant_id=document.tenant_id
                AND chunk.source_id=document.source_id AND chunk.document_id=document.document_id
                AND document.tenant_id=%s AND document.source_id=%s AND document.revision=%s""",
                (self.tenant, self.source, revision),
            )
            c.execute(
                "UPDATE canonical_documents SET text_redacted='',body_location='chunks' WHERE tenant_id=%s AND source_id=%s AND revision=%s",
                (self.tenant, self.source, revision),
            )

    def test_exact_historical_bundle_reader_survives_logical_cleanup(self):
        self.write(self.event())
        baseline = self.resolve()
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=100, max_batches=10, upload_concurrency=1
        )
        self.project()
        self.write(self.event(revision=2))
        self.project()
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.clear()
        self.assertEqual(self.resolve(), baseline)

    def forget(self):
        return self.plane.forget(
            dict(
                contract="recall.forget-request.v1",
                schema_version=1,
                tenant_id=self.tenant,
                principal_id=self.principal,
                source_id=self.source,
                target_receipt=f"recall://{self.source}/turn?rev=1#item=0",
                mode="explicit_forget",
                reason="owner_requested",
                requested_at="2026-10-13T00:01:00Z",
                idempotency_key="history-forget",
            )
        )

    def references(self):
        with self.store.connect() as c:
            return c.execute(
                "SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall()

    def historical(self, *, inline=False):
        self.write(self.event())
        baseline = self.resolve()
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=100, max_batches=10, upload_concurrency=1
        )
        self.write(self.event(revision=2))
        if not inline:
            self.clear()
        return baseline

    def test_valid_inline_history_skips_archive_but_still_checks_deletion(self):
        baseline = self.historical(inline=True)
        with patch.object(
            self.archive,
            "read_raw",
            side_effect=AssertionError("inline history must not load archive"),
        ):
            self.assertEqual(self.resolve(), baseline)
        original = self.store._execute_bounded
        calls = []

        def execute(c, sql, args, deadline):
            result = original(c, sql, args, deadline)
            if (
                "SELECT chunk.source_id,chunk.document_id,chunk.ordinal,chunk.receipt"
                in sql
                and not calls
            ):
                calls.append(True)
                rows = result.fetchall()
                self.forget()
                return SimpleNamespace(fetchall=lambda: rows)
            return result

        with patch.object(self.store, "_execute_bounded", side_effect=execute):
            with self.assertRaises(ChunkBodyError):
                self.resolve()

    def test_forget_during_archived_read_refuses_loaded_bytes(self):
        self.historical()
        original = self.archive.read_raw

        def read(reference):
            payload = original(reference)
            self.forget()
            return payload

        with patch.object(self.archive, "read_raw", side_effect=read):
            with self.assertRaises(ChunkBodyError):
                self.resolve()

    def test_cross_tenant_and_source_read_no_archive(self):
        self.historical()
        with patch.object(
            self.archive, "read_raw", side_effect=AssertionError("unauthorized read")
        ):
            self.assertIsNone(self.resolve(tenant="tenant:other"))
            self.assertIsNone(self.resolve(sources=("source:other",)))

    def test_corrupt_archive_without_valid_inline_body_fails_closed(self):
        self.historical()
        with patch.object(self.archive, "read_raw", return_value=b"wrong"):
            with self.assertRaises(ChunkBodyError):
                self.resolve()

    def test_changed_catalog_after_read_fails_closed(self):
        self.historical()
        original = self.archive.read_raw

        def read(reference):
            payload = original(reference)
            with self.store.connect() as c:
                c.execute(
                    "UPDATE canonical_evidence_objects SET receipt_count=receipt_count+1 WHERE tenant_id=%s",
                    (self.tenant,),
                )
            return payload

        with patch.object(self.archive, "read_raw", side_effect=read):
            with self.assertRaises(ChunkBodyError):
                self.resolve()

    def test_archive_body_read_holds_no_database_connection(self):
        from contextlib import contextmanager

        self.historical()
        connect, read = self.store.connect, self.archive.read_raw
        active = []

        @contextmanager
        def connection():
            with connect() as c:
                active.append(True)
                try:
                    yield c
                finally:
                    active.pop()

        def read_without_lease(reference):
            self.assertFalse(active)
            return read(reference)

        with (
            patch.object(self.store, "connect", connection),
            patch.object(self.archive, "read_raw", side_effect=read_without_lease),
        ):
            self.assertTrue(self.resolve()["items"])

    def test_catalog_and_bundle_cannot_relabel_canonical_revision(self):
        from dataclasses import replace
        from recall_server.evidence_projection import EvidenceBundle

        self.historical()
        reference = self.references()[0]
        payload = self.archive.read_raw(
            CanonicalEvidenceProjector._reference(reference)
        )
        relabeled = replace(EvidenceBundle.decode(payload), revision=2).encode()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_evidence_objects SET revision=2 WHERE tenant_id=%s",
                (self.tenant,),
            )
        with patch.object(self.archive, "read_raw", return_value=relabeled):
            with self.assertRaises(ChunkBodyError):
                self.resolve()


if __name__ == "__main__":
    unittest.main()
