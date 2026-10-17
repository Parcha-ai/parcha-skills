"""Supported writers atomically name every changed scan document."""

import hashlib
import unittest
from unittest.mock import patch
from psycopg import sql
from recall_server.control import ControlPlane, SecretBox
from e2e_parent_membership_enrollment import ParentEnrollmentTests
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.parquet_scan import CanonicalParquetScanProjector
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy, DEFAULT_PASSAGE_POLICY


class DirtyCompleteness(ParentEnrollmentTests):
    def test_passage_only_reprojection_names_dirty_document(self):
        self.store.verified_singleton_receipts = True
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        first = scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.assertGreater(first["shards"], 0)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_parquet_scan_dirty_documents WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )
        # Supported policy backfill, without changing source or logical history.
        projector = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=PassagePolicy(target_tokens=4, overlap_tokens=0),
            bound_tenant_id=self.tenant,
        )
        self.assertEqual(projector.seed_backfill(tenant_id=self.tenant), 1)
        self.assertEqual(
            projector.project_pending(
                tenant_id=self.tenant, batch_size=4, max_batches=1
            )["documents"],
            1,
        )
        candidate = scan._pending(tenant_id=self.tenant, limit=1)[0]
        documents = scan._documents(candidate)
        catalog = scan._catalog(candidate)
        self.assertEqual(
            catalog.dirty, frozenset(row["logical_document_id"] for row in documents)
        )
        mode, victims, rewrite, dirty = scan._plan(
            candidate, documents, catalog, allow_compaction=False
        )
        self.assertEqual(mode, "delta")
        self.assertGreater(len(victims), 0)
        self.assertEqual(rewrite, {row["logical_document_id"] for row in documents})
        self.assertEqual(dirty, rewrite)
        print(
            {
                "counterexample": "native_passage_only_reprojection",
                "dirty_rows": len(catalog.dirty),
                "native_fingerprint_dirty": len(dirty),
                "victim_fragments": len(victims),
                "all_changed_documents_named": rewrite <= set(catalog.dirty),
            },
            flush=True,
        )

    def test_actor_rename_names_changed_fingerprint_before_logical_rebuild(self):
        self.store.verified_singleton_receipts = True
        args = dict(
            tenant_id=self.tenant,
            employee_key="synthetic-employee",
            source_ids=[self.source],
        )
        self.store.bind_coding_sources_to_employee(
            **args, display_name="Synthetic original"
        )
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, upload_concurrency=1
        )
        passages = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        )
        passages.project_pending(tenant_id=self.tenant, batch_size=4, max_batches=1)
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        candidate = scan._pending(tenant_id=self.tenant, limit=1)[0]
        self.assertGreater(
            scan.project_pending(
                tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
            )["shards"],
            0,
        )
        before = scan._documents(candidate)
        result = self.store.bind_coding_sources_to_employee(
            **args, display_name="Synthetic renamed"
        )
        after = scan._documents(candidate)
        catalog = scan._catalog(candidate)
        self.assertEqual(len(before), len(after))
        self.assertTrue(
            any(
                scan._fingerprint(a) != scan._fingerprint(b)
                for a, b in zip(before, after)
            )
        )
        self.assertEqual(
            catalog.dirty, frozenset(row["logical_document_id"] for row in after)
        )
        self.assertGreater(result["queued_documents"], 0)
        mode, victims, rewrite, dirty = scan._plan(
            candidate, after, catalog, allow_compaction=False
        )
        self.assertEqual(mode, "delta")
        self.assertGreater(len(rewrite), 0)
        print(
            {
                "counterexample": "native_actor_rename_before_logical_rebuild",
                "dirty_rows": len(catalog.dirty),
                "fingerprint_stale_documents": len(dirty),
                "queued_logical_documents": result["queued_documents"],
                "all_changed_documents_named": rewrite <= set(catalog.dirty),
            },
            flush=True,
        )

    def actor_fixture(self):
        self.store.verified_singleton_receipts = True
        self.actor_args = dict(
            tenant_id=self.tenant,
            employee_key="synthetic-employee",
            source_ids=[self.source],
        )
        actor = self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic original"
        )["actor_id"]
        self.projector.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, upload_concurrency=1
        )
        CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        ).project_pending(tenant_id=self.tenant, batch_size=4, max_batches=1)
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        return actor

    def test_actor_rename_marks_all_sources_documents_and_months_once(self):
        actor = self.actor_fixture()
        other = self.source + ":sibling"
        with self.store.connect() as c:
            # Additional catalog fixtures share this actor; no archive reads are claimed.
            def copy(table, overrides, where, params):
                columns = [
                    row["attname"]
                    for row in c.execute(
                        "SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 "
                        "AND NOT attisdropped AND attgenerated='' ORDER BY attnum",
                        (table,),
                    ).fetchall()
                ]
                values = [
                    sql.Placeholder() if name in overrides else sql.Identifier(name)
                    for name in columns
                ]
                c.execute(
                    sql.SQL(
                        "INSERT INTO {} ({}) SELECT {} FROM {} WHERE "
                        + where
                        + " LIMIT 1"
                    ).format(
                        sql.Identifier(table),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                        sql.SQL(",").join(values),
                        sql.Identifier(table),
                    ),
                    tuple(overrides[name] for name in columns if name in overrides)
                    + params,
                )

            copy(
                "canonical_sources",
                {"source_id": other},
                "tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            )
            expected = set()
            for n, source in enumerate((self.source, other)):
                digest = hashlib.md5(("synthetic-clone-" + str(n)).encode()).hexdigest()
                logical = "ldoc_" + digest
                copy(
                    "canonical_evidence_documents",
                    dict(
                        source_id=source,
                        logical_document_id=logical,
                        native_parent_id="synthetic-clone-" + str(n),
                        evidence_id="evd_" + digest,
                        manifest_version_id="synthetic-clone-" + str(n),
                        first_occurred_at="2026-09-01T00:00:00Z",
                        last_occurred_at="2026-10-01T00:00:00Z",
                    ),
                    "tenant_id=%s AND source_id=%s",
                    (self.tenant, self.source),
                )
                copy(
                    "canonical_evidence_document_actors",
                    dict(source_id=source, logical_document_id=logical),
                    "tenant_id=%s AND source_id=%s AND actor_id=%s",
                    (self.tenant, self.source, actor),
                )
                expected.update(
                    (source, logical, month) for month in ("2026-09-01", "2026-10-01")
                )
            original = c.execute(
                "SELECT logical_document_id FROM canonical_evidence_documents WHERE tenant_id=%s "
                "AND native_parent_id='session'",
                (self.tenant,),
            ).fetchone()["logical_document_id"]
            expected.add((self.source, original, "2026-10-01"))
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic renamed"
        )
        with self.store.connect() as c:
            rows = c.execute(
                "SELECT source_id,logical_document_id,bucket_start FROM canonical_parquet_scan_dirty_documents "
                "WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall()
            self.assertEqual(
                {
                    (
                        r["source_id"],
                        r["logical_document_id"],
                        r["bucket_start"].isoformat(),
                    )
                    for r in rows
                },
                expected,
            )
            queues = c.execute(
                "SELECT source_id,bucket_start,generation FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall()
            self.assertEqual(len(queues), 4)
            self.assertTrue(all(row["generation"] == 1 for row in queues))

    def test_unchanged_name_does_not_touch_scan_queue(self):
        self.actor_fixture()
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic original"
        )
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )

    def test_actor_name_and_dirty_admission_roll_back_together(self):
        actor = self.actor_fixture()
        with patch.object(
            self.store,
            "_queue_actor_scan",
            side_effect=RuntimeError("synthetic rollback"),
        ):
            with self.assertRaisesRegex(RuntimeError, "synthetic rollback"):
                self.store.bind_coding_sources_to_employee(
                    **self.actor_args, display_name="Synthetic renamed"
                )
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT display_name FROM brain_actors WHERE tenant_id=%s AND actor_id=%s",
                    (self.tenant, actor),
                ).fetchone()["display_name"],
                "Synthetic original",
            )
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )

    def test_actor_rename_rejects_prepared_scan_and_native_retry_keeps_name(self):
        self.actor_fixture()
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic first rename"
        )
        candidate = scan._pending(tenant_id=self.tenant, limit=1)[0]
        upload = scan._build(candidate, allow_compaction=False)
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic final rename"
        )
        self.assertEqual(scan._commit(candidate, upload), "stale")
        self.assertEqual(len(scan._pending(tenant_id=self.tenant, limit=1)), 1)
        self.assertGreater(
            scan.project_pending(
                tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
            )["shards"],
            0,
        )
        documents = scan._documents(candidate)
        self.assertTrue(
            all(
                any(
                    link["display_name"] == "Synthetic final rename"
                    for link in row["actor_links"]
                )
                for row in documents
            )
        )
        self.assertFalse(scan._catalog(candidate).dirty)

    def test_passage_and_dirty_admission_roll_back_together(self):
        self.store.verified_singleton_receipts = True
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        projector = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=PassagePolicy(target_tokens=4, overlap_tokens=0),
            bound_tenant_id=self.tenant,
        )
        self.assertEqual(projector.seed_backfill(tenant_id=self.tenant), 1)

        def state():
            with self.store.connect() as c:
                passages = c.execute(
                    "SELECT * FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id",
                    (self.tenant,),
                ).fetchall()
                pointer = c.execute(
                    "SELECT * FROM canonical_passage_documents WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchall()
                return passages, pointer

        before = state()
        candidate = projector._pending(tenant_id=self.tenant, limit=1)[0]
        prepared = projector._prepare(candidate)
        with patch.object(
            CanonicalLogicalEvidenceProjector,
            "_queue_parquet_scan",
            side_effect=RuntimeError("synthetic rollback"),
        ):
            with self.assertRaisesRegex(RuntimeError, "synthetic rollback"):
                projector._commit(prepared)
        self.assertEqual(state(), before)
        self.assertEqual(len(projector._pending(tenant_id=self.tenant, limit=1)), 1)
        self.assertFalse(scan._pending(tenant_id=self.tenant, limit=1))
        self.assertEqual(projector._commit(prepared)["status"], "committed")
        self.assertTrue(scan._pending(tenant_id=self.tenant, limit=1))

    def test_invitation_actor_rename_queues_linked_documents(self):
        actor = self.actor_fixture()
        issuer = "https://synthetic.example.invalid"
        subject = "synthetic-invited-user"
        principal = (
            "principal:human:"
            + hashlib.sha256(f"{issuer}\0{subject}".encode()).hexdigest()[:32]
        )
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO brain_memberships(organization_id,principal_id,role) VALUES(%s,%s,'owner')",
                (self.tenant, self.principal),
            )
            c.execute(
                "INSERT INTO brain_principals(tenant_id,principal_id) VALUES(%s,%s)",
                (self.tenant, principal),
            )
            c.execute(
                "INSERT INTO brain_actor_principals(tenant_id,actor_id,principal_id) VALUES(%s,%s,%s)",
                (self.tenant, actor, principal),
            )
        control = ControlPlane(self.store, SecretBox(b"e" * 32), {})
        email = "invited@example.invalid"
        invitation = control.create_brain_invitation(
            principal_id=self.principal,
            tenant_id=self.tenant,
            email=email,
            display_name="Synthetic invited name",
            role="member",
        )
        result = self.store.accept_external_invitation(
            issuer=issuer,
            subject=subject,
            email=email,
            email_index=control.invitation_email_index(email),
            scopes=["read"],
            audience="https://recall.example.invalid/mcp",
            tenant_id=self.tenant,
            invitation_id=invitation["id"],
        )
        self.assertIsNotNone(result)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT display_name FROM brain_actors WHERE tenant_id=%s AND actor_id=%s",
                    (self.tenant, actor),
                ).fetchone()["display_name"],
                "Synthetic invited name",
            )
            self.assertGreater(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_parquet_scan_dirty_documents WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )


if __name__ == "__main__":
    suite = unittest.TestSuite(
        DirtyCompleteness(name)
        for name in DirtyCompleteness.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
