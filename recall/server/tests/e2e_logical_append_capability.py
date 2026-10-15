#!/usr/bin/env python3
"""Append capability is advisory, atomic, and invalidated by any unknown writer."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

from psycopg import Connection

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402


class AppendCapability(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore(os.environ["RECALL_DATABASE_URL"])
        self.addCleanup(self.store.close)
        self.store.migrate()
        nonce = uuid.uuid4().hex
        self.tenant, self.source, self.principal = (
            f"{kind}:append:{nonce}" for kind in ("tenant", "source", "principal")
        )
        self.parent = "session-one"
        directory = tempfile.TemporaryDirectory(prefix="recall-append-capability-")
        self.addCleanup(directory.cleanup)
        self.archive = FilesystemArchiveStore(
            Path(directory.name), namespace_key=b"a" * 32
        )
        self.gateway = CanonicalArchiveGateway(
            self.store, self.archive, tenant_id=self.tenant, principal_id=self.principal
        )
        self.plane = CanonicalPlane(self.store, self.archive)
        self.logical = CanonicalLogicalEvidenceProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant,
            raw_archive=self.archive,
        )

    def event(
        self,
        native,
        ordinal,
        text="An exact archived body.",
        *,
        kind="connector_record",
        record_type=None,
    ):
        occurred = "2026-10-14T10:00:00Z"
        content = (
            {"target_native_id": native}
            if kind == "tombstone"
            else {"text": text, "role": "user"}
        )
        if record_type is not None:
            content["type"] = record_type
        payload = canonical_json(content)
        artifact = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=payload,
            media_type="application/json",
            created_at=occurred,
        )
        envelope = dict(
            schema_version=1,
            source_id=self.source,
            native_id=native,
            native_parent_id=self.parent,
            kind=kind,
            occurred_at=occurred,
            observed_at=occurred,
            principal_id=self.principal,
            visibility="private",
            content_type="application/json",
            content=content,
            provenance=dict(
                connector_id="synthetic.append-capability",
                connector_schema_version=1,
                artifact_ref=artifact,
                byte_start=ordinal,
            ),
            content_sha256=hashlib.sha256(payload).hexdigest(),
        )
        return envelope

    def ingest(self, native, ordinal, text="An exact archived body.", **kwargs):
        envelope = self.event(native, ordinal, text, **kwargs)
        return self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.append-capability",
            artifact_ref=envelope["provenance"]["artifact_ref"],
            envelope=envelope,
            text_redacted="" if envelope["kind"] == "tombstone" else text,
        )

    def publish(self):
        return self.logical.project_pending(
            tenant_id=self.tenant, batch_size=2, max_batches=1, upload_concurrency=1
        )

    def catalog(self):
        with self.store.connect() as c:
            return c.execute(
                """SELECT manifest_artifact_id,append_source_ordinal,append_manifest_id
                FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s""",
                (self.tenant, self.source),
            ).fetchone()

    def queue(self):
        with self.store.connect() as c:
            return c.execute(
                """SELECT generation,reason,append_generation,append_base_manifest_id
                FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s""",
                (self.tenant, self.source),
            ).fetchone()

    def test_published_frontier_classifies_only_new_ordered_append(self):
        self.ingest("first", 10)
        self.assertEqual(self.publish()["documents"], 1)
        catalog = self.catalog()
        self.assertEqual(catalog["append_source_ordinal"], 10)
        self.ingest("second", 20)
        queued = self.queue()
        self.assertEqual(queued["append_generation"], queued["generation"])
        self.assertEqual(
            queued["append_base_manifest_id"], catalog["manifest_artifact_id"]
        )
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["append_source_ordinal"], 20)

    def seed(self):
        self.ingest("first", 10)
        self.assertEqual(self.publish()["documents"], 1)
        return self.catalog()

    def safe(self):
        queued = self.queue()
        catalog = self.catalog()
        return bool(
            queued
            and catalog
            and queued["append_generation"] == queued["generation"]
            and queued["append_base_manifest_id"] == catalog["manifest_artifact_id"]
            and catalog["append_manifest_id"] == catalog["manifest_artifact_id"]
        )

    def test_edit_late_tied_and_null_arrivals_require_full_publication(self):
        self.seed()
        for native, ordinal, text in [
            ("first", 10, "Changed body."),
            ("late", 5, "Late body."),
            ("tied", 10, "Tied body."),
            ("unknown", None, "Null ordinal."),
        ]:
            with self.subTest(native=native):
                self.ingest(native, ordinal, text)
                self.assertFalse(self.safe())
                self.publish()
        self.assertIsNone(self.catalog()["append_source_ordinal"])
        self.ingest("later", 50)
        self.assertFalse(self.safe())

    def test_legacy_writer_poison_survives_next_new_writer(self):
        self.seed()
        self.ingest("second", 20)
        self.assertTrue(self.safe())
        with self.store.connect() as c:
            c.execute(
                """UPDATE canonical_evidence_document_queue
                SET generation=generation+1,changed_at=clock_timestamp()
                WHERE tenant_id=%s AND source_id=%s""",
                (self.tenant, self.source),
            )
        self.assertFalse(self.safe())
        self.ingest("third", 30)
        self.assertFalse(self.safe())
        self.publish()
        self.ingest("fourth", 40)
        self.assertTrue(self.safe())

    def test_same_manifest_authority_requeue_poison_then_full_repair(self):
        catalog = self.seed()
        with self.store.connect() as c:
            self.store._queue_actor_projection(
                c, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.assertFalse(self.safe())
        self.assertEqual(
            self.catalog()["manifest_artifact_id"], catalog["manifest_artifact_id"]
        )
        result = self.publish()
        self.assertEqual(result["repaired"], 1)
        self.assertEqual(
            self.catalog()["manifest_artifact_id"], catalog["manifest_artifact_id"]
        )
        self.ingest("second", 20)
        self.assertTrue(self.safe())
        with self.store.connect() as c:
            self.store._queue_actor_projection(
                c, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.ingest("third", 30)
        self.assertFalse(self.safe())

    def test_old_publisher_cannot_advance_a_manifest_bound_frontier(self):
        original = self.seed()
        self.ingest("second", 20)
        with patch(
            "recall_server.logical_append_capability.record_frontier", return_value=None
        ):
            self.assertEqual(self.publish()["documents"], 1)
        current = self.catalog()
        self.assertNotEqual(
            current["manifest_artifact_id"], original["manifest_artifact_id"]
        )
        self.assertEqual(
            current["append_manifest_id"], original["manifest_artifact_id"]
        )
        self.ingest("third", 30)
        self.assertFalse(self.safe())

    def test_batch_proof_is_atomic_and_a_prior_identity_forces_full(self):
        self.seed()
        result = self.plane.ingest_batch(
            tenant_id=self.tenant,
            principal_id=self.principal,
            events=[self.event("second", 20), self.event("third", 30)],
        )
        self.assertEqual(result["inserted"], 2)
        self.assertTrue(self.safe())
        self.publish()
        self.plane.ingest_batch(
            tenant_id=self.tenant,
            principal_id=self.principal,
            events=[
                self.event("first", 10, "Changed original."),
                self.event("fourth", 40),
            ],
        )
        self.assertFalse(self.safe())

    def test_structural_tail_is_in_the_frontier(self):
        self.ingest("first", 10)
        self.ingest("hidden", 30, record_type="token_count")
        result = self.publish()
        self.assertEqual(result["records"], 1)
        self.assertEqual(self.catalog()["append_source_ordinal"], 30)
        self.ingest("second", 20)
        self.assertFalse(self.safe())

    def test_tombstone_and_same_hash_replay_do_not_create_append_evidence(self):
        self.seed()
        self.ingest("second", 20)
        before = self.queue()
        self.assertEqual(self.ingest("second", 20)["inserted"], 0)
        self.assertEqual(self.queue(), before)
        self.ingest("first", 30, kind="tombstone")
        self.assertFalse(self.safe())

    def test_schema_absent_keeps_existing_full_ingest_and_publication(self):
        with self.store.connect() as c:
            c.execute("DELETE FROM schema_migrations WHERE version=79")
            c.execute(
                "ALTER TABLE canonical_evidence_documents DROP COLUMN append_source_ordinal,DROP COLUMN append_manifest_id"
            )
            c.execute(
                "ALTER TABLE canonical_evidence_document_queue DROP COLUMN append_generation,DROP COLUMN append_base_manifest_id"
            )
        standalone_probes = []
        dispatch_probes = []
        original_execute = Connection.execute

        def observe(connection, query, *args, **kwargs):
            normalized = " ".join(str(query).split())
            if (
                normalized.startswith("SELECT")
                and "schema_migrations WHERE version=79" in normalized
            ):
                if ("FROM canonical_evidence_document_queue queue" in normalized
                        and "WHERE queue.tenant_id=%s AND queue.source_id=%s AND queue.native_parent_id=%s" in normalized):
                    dispatch_probes.append(normalized)
                else:
                    standalone_probes.append(normalized)
            return original_execute(connection, query, *args, **kwargs)

        try:
            with patch.object(Connection, "execute", observe):
                self.ingest("first", 10)
                self.assertEqual(self.publish()["documents"], 1)
                self.ingest("second", 20)
                self.assertEqual(self.publish()["documents"], 1)
            self.assertEqual(len(dispatch_probes), 2 if self.logical.append_enabled else 0)
            self.assertEqual(
                standalone_probes,
                [],
                "optional capability must reuse existing SQL round trips",
            )
        finally:
            self.store.migrate()

    def test_rollback_cannot_leave_an_append_annotation(self):
        self.seed()
        event = self.event("second", 20)
        with self.plane.prepare_history(
            tenant_id=self.tenant, principal_id=self.principal, events=[event]
        ) as history:
            with self.assertRaisesRegex(RuntimeError, "deliberate rollback"):
                with self.store.connect() as c, c.transaction():
                    self.plane.ingest_document(
                        tenant_id=self.tenant,
                        principal_id=self.principal,
                        connector_id="synthetic.append-capability",
                        artifact_ref=event["provenance"]["artifact_ref"],
                        envelope=event,
                        text_redacted=event["content"]["text"],
                        _connection=c,
                        _history=history,
                    )
                    row = c.execute(
                        "SELECT append_generation,generation FROM canonical_evidence_document_queue WHERE tenant_id=%s",
                        (self.tenant,),
                    ).fetchone()
                    self.assertEqual(row["append_generation"], row["generation"])
                    raise RuntimeError("deliberate rollback")
        self.assertIsNone(self.queue())
        self.assertEqual(self.catalog()["append_source_ordinal"], 10)

    def test_transaction_started_before_publication_uses_current_manifest(self):
        self.ingest("first", 10)
        event = self.event("second", 20)
        with self.plane.prepare_history(
            tenant_id=self.tenant, principal_id=self.principal, events=[event]
        ) as history:
            with self.store.connect() as c, c.transaction():
                c.execute("SELECT 1")
                self.assertEqual(self.publish()["documents"], 1)
                manifest = self.catalog()["manifest_artifact_id"]
                self.plane.ingest_document(
                    tenant_id=self.tenant,
                    principal_id=self.principal,
                    connector_id="synthetic.append-capability",
                    artifact_ref=event["provenance"]["artifact_ref"],
                    envelope=event,
                    text_redacted=event["content"]["text"],
                    _connection=c,
                    _history=history,
                )
        self.assertTrue(self.safe())
        self.assertEqual(self.queue()["append_base_manifest_id"], manifest)

    def test_stale_prepared_publication_cannot_advance_frontier(self):
        original = self.seed()
        self.ingest("second", 20)
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.ingest("third", 30)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertEqual(self.catalog(), original)
        self.assertTrue(self.safe())
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["append_source_ordinal"], 30)

    def test_same_manifest_recreated_catalog_and_queue_reject_stale_candidate(self):
        original = self.seed()
        self.ingest("second", 20)
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        with self.store.connect() as c, c.transaction():
            c.execute(
                "CREATE TEMP TABLE saved_catalog ON COMMIT DROP AS SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s",
                (self.tenant,),
            )
            c.execute(
                "DELETE FROM canonical_evidence_documents WHERE tenant_id=%s",
                (self.tenant,),
            )
            c.execute(
                "INSERT INTO canonical_evidence_documents SELECT * FROM saved_catalog"
            )
            c.execute(
                "DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s",
                (self.tenant,),
            )
            c.execute(
                """INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,generation,reason,changed_at)
                VALUES(%s,%s,%s,%s,'ingest',clock_timestamp())""",
                (self.tenant, self.source, self.parent, candidate.generation),
            )
        self.assertEqual(self.catalog(), original)
        self.assertFalse(self.safe())
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertEqual(self.catalog(), original)


if __name__ == "__main__":
    unittest.main()
