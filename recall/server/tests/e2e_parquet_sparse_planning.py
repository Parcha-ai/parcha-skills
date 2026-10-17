"""Sparse metadata planning must match the native full-month build."""

import hashlib
from contextlib import contextmanager
import json
import os
from pathlib import Path
import uuid

import psycopg
from psycopg import sql
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from e2e_receipt_membership import ReceiptMembershipTests
from e2e_parquet_scan_fragments import live_parts, reference
from recall_server import parquet_scan
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projectors import canonical_json


class SparsePlanning(ReceiptMembershipTests):
    def setUp(self):
        super().setUp()
        with self.store.connect() as c:
            # Each test starts with the actual optional migration, including after
            # the explicit old-schema compatibility test drops the column.
            migration = (
                Path(__file__).resolve().parents[1]
                / "schema"
                / "088_parquet_dirty_plan_epoch.sql"
            )
            c.execute(migration.read_text())
        self.epoch = "synthetic-complete-writers-v1"
        self.parts = patch.object(parquet_scan, "FRAGMENT_TARGET_BYTES", 1)
        self.parts.start()
        self.addCleanup(self.parts.stop)
        self.passages = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        )
        for parent in ("alpha", "beta", "gamma"):
            self.ingest_parent(parent, parent + "/one", "Synthetic original " + parent)
        self.actor_args = dict(
            tenant_id=self.tenant,
            employee_key="synthetic-sparse-owner",
            source_ids=[self.source],
        )
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic sparse actor"
        )
        self.project()
        self.scan = self.new_scan()
        self.scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        with self.store.connect() as c:
            self.ids = {
                r["native_parent_id"]: r["logical_document_id"]
                for r in c.execute(
                    "SELECT native_parent_id,logical_document_id FROM canonical_evidence_documents WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchall()
            }

    def ingest_parent(self, parent, native, text, *, tombstone=False):
        content = (
            {"target_native_id": native}
            if tombstone
            else {"text": text, "role": "user"}
        )
        payload = canonical_json(content)
        ref = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=native,
            payload=payload,
            media_type="application/json",
            created_at="2026-10-14T00:00:00Z",
        )
        envelope = dict(
            schema_version=1,
            source_id=self.source,
            native_id=native,
            native_parent_id=parent,
            kind="tombstone" if tombstone else "connector_record",
            occurred_at="2026-10-14T00:00:00Z",
            observed_at="2026-10-14T00:00:00Z",
            principal_id=self.principal,
            visibility="private",
            content_type="application/json",
            content=content,
            provenance=dict(connector_id="synthetic.membership", artifact_ref=ref),
            content_sha256=hashlib.sha256(payload).hexdigest(),
        )
        return self.plane.ingest_document(
            tenant_id=self.tenant,
            principal_id=self.principal,
            connector_id="synthetic.membership",
            artifact_ref=ref,
            envelope=envelope,
            text_redacted=text,
        )

    def project(self):
        self.assertEqual(
            self.projector.project_pending(
                tenant_id=self.tenant,
                batch_size=20,
                max_batches=1,
                upload_concurrency=1,
            )["failed"],
            0,
        )
        self.passages.project_pending(
            tenant_id=self.tenant, batch_size=20, max_batches=1, concurrency=1
        )

    def new_scan(self, *, enabled=True, epoch=None):
        with patch.dict(
            os.environ,
            {
                "RECALL_PARQUET_SPARSE_PLAN": "1" if enabled else "0",
                "RECALL_PARQUET_DIRTY_EPOCH": epoch or self.epoch,
            },
        ):
            return parquet_scan.CanonicalParquetScanProjector(
                self.store, LogicalEvidenceProjectionStore(self.archive)
            )

    def candidate(self):
        return self.scan._pending(tenant_id=self.tenant, limit=1)[0]

    def rows(self, upload):
        parts = {
            (r["dataset"], int(r["shard_index"])): reference(r)
            for r in live_parts(self.store, self.tenant, self.source)
        }
        for key in upload.removed:
            parts.pop(key)
        parts.update(upload.references)
        result = {dataset: [] for dataset in parquet_scan.SCAN_DATASETS}
        for (dataset, _), ref in parts.items():
            result[dataset].extend(
                pq.read_table(pa.BufferReader(self.archive.read_raw(ref))).to_pylist()
            )
        return {
            key: sorted((json.dumps(row, sort_keys=True, default=str) for row in value))
            for key, value in result.items()
        }

    def assert_oracle(self, candidate=None):
        candidate = candidate or self.candidate()
        full = self.new_scan(enabled=False)._build(candidate, allow_compaction=False)
        sparse = self.scan._build(candidate, allow_compaction=False)
        self.assertEqual(self.rows(sparse), self.rows(full))
        self.assertEqual(sparse.removed, full.removed)
        return sparse

    def test_first_full_commit_enrolls_existing_shards(self):
        with self.store.connect() as c:
            markers = {
                r["dirty_plan_epoch"]
                for r in c.execute(
                    "SELECT dirty_plan_epoch FROM canonical_parquet_scan_shards WHERE tenant_id=%s",
                    (self.tenant,),
                )
            }
        self.assertEqual(markers, {self.scan.sparse_plan_epoch})

    def test_document_projection_preserves_native_build_without_unused_metadata(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic projection change")
        self.project()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_evidence_documents SET actor_checkpoint=%s::jsonb "
                "WHERE tenant_id=%s",
                (json.dumps({"synthetic_unused_payload": "x" * 12_000}), self.tenant),
            )
        candidate = self.candidate()
        captured = []
        connect = self.store.connect

        class Capture:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, parameters):
                captured.append((statement, parameters))
                return self.connection.execute(statement, parameters)

        @contextmanager
        def capture():
            with connect() as c:
                yield Capture(c)

        with patch.object(self.store, "connect", capture):
            selected = self.scan._documents(candidate)
        self.assertEqual(len(captured), 1)
        statement, parameters = captured[0]
        # The original broad projection, with exactly the native joins, filters
        # and parameters, supplies the independent value/output comparison.
        broad_statement = "SELECT document.*, pointer.policy_fingerprint" + statement.split(
            "pointer.policy_fingerprint", 1
        )[1]
        with connect() as c:
            broad = c.execute(broad_statement, parameters).fetchall()
        fields = {
            "tenant_id", "source_id", "logical_document_id", "native_parent_id",
            "revision", "document_content_sha256", "part_count",
            "passage_policy_fingerprint", "passage_source_sha256",
            "passage_count", "actor_links",
        }
        self.assertTrue(broad)
        self.assertTrue(all(row["manifest_object_key"] and row["actor_checkpoint"] for row in broad))
        self.assertEqual(selected, [{key: row[key] for key in fields} for row in broad])
        selected_bytes = len(json.dumps(selected, sort_keys=True, default=str).encode())
        broad_bytes = len(json.dumps(broad, sort_keys=True, default=str).encode())
        print(json.dumps({"synthetic_metadata_rows": len(broad),
                          "broad_json_bytes": broad_bytes,
                          "selected_json_bytes": selected_bytes}))
        self.assertLess(selected_bytes, broad_bytes - 12_000 * len(broad))
        self.assertEqual(
            [self.scan._fingerprint(row) for row in selected],
            [self.scan._fingerprint(row) for row in broad],
        )
        upload = self.scan._build(candidate, allow_compaction=False)

        def broad_documents(candidate, *, document_ids=None):
            return [dict(row) for row in broad
                    if document_ids is None or row["logical_document_id"] in document_ids]

        with patch.object(self.scan, "_documents", broad_documents):
            original = self.scan._build(candidate, allow_compaction=False)
        self.assertEqual(self.rows(upload), self.rows(original))
        self.assertEqual(upload.removed, original.removed)
        self.assertEqual(upload.members, original.members)
        self.assertEqual(upload.generation_sha256, original.generation_sha256)
        self.assertEqual(self.scan._commit(candidate, upload), "committed")

    def test_changed_document_reads_only_affected_metadata_with_full_oracle_parity(
        self,
    ):
        self.ingest_parent("alpha", "alpha/one", "Synthetic changed alpha")
        self.project()
        candidate = self.candidate()
        with patch.object(self.scan, "_documents", wraps=self.scan._documents) as docs:
            upload = self.scan._build(candidate, allow_compaction=False)
        self.assertTrue(docs.call_args_list)
        self.assertTrue(
            all(
                call.kwargs.get("document_ids") is not None
                for call in docs.call_args_list
            )
        )
        self.assertEqual(
            set(docs.call_args_list[-1].kwargs["document_ids"]), {self.ids["alpha"]}
        )
        full = self.new_scan(enabled=False)._build(candidate, allow_compaction=False)
        self.assertEqual(self.rows(upload), self.rows(full))
        self.assertEqual(upload.removed, full.removed)

    def test_removed_only_document_does_not_drop_unrelated_survivors(self):
        self.ingest_parent("alpha", "alpha/one", "", tombstone=True)
        self.project()
        upload = self.assert_oracle()
        rows = self.rows(upload)
        self.assertEqual(
            {json.loads(r)["logical_document_id"] for r in rows["documents"]},
            {self.ids["beta"], self.ids["gamma"]},
        )

    def test_shared_victim_siblings_survive_all_four_datasets(self):
        self.parts.stop()
        self.ingest_parent(
            "alpha", "alpha/one", "Synthetic full rebuild before sharing"
        )
        self.project()
        self.scan.seed_backfill(tenant_id=self.tenant)
        self.scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        with self.store.connect() as c:
            self.assertGreater(
                c.execute(
                    "SELECT max(members) AS maximum FROM ("
                    "SELECT count(*) AS members FROM canonical_parquet_scan_fragment_documents "
                    "WHERE tenant_id=%s GROUP BY dataset,shard_index) grouped",
                    (self.tenant,),
                ).fetchone()["maximum"],
                1,
            )
        self.ingest_parent("alpha", "alpha/one", "Synthetic changed shared alpha")
        self.project()
        upload = self.assert_oracle()
        self.assertTrue(
            all(self.rows(upload)[name] for name in parquet_scan.SCAN_DATASETS)
        )
        self.assertEqual(
            {
                json.loads(r)["logical_document_id"]
                for r in self.rows(upload)["documents"]
            },
            set(self.ids.values()),
        )

    def test_epoch_rotation_and_compaction_keep_full_metadata_fallback(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic changed alpha")
        self.project()
        candidate = self.candidate()
        for scan, compact in (
            (self.new_scan(epoch="synthetic-after-old-writer"), False),
            (self.scan, True),
        ):
            with (
                self.subTest(compaction=compact),
                patch.object(scan, "_documents", wraps=scan._documents) as docs,
            ):
                scan._build(candidate, allow_compaction=compact)
            self.assertTrue(
                any(
                    call.kwargs.get("document_ids") is None
                    for call in docs.call_args_list
                )
            )

    def test_new_generation_rejects_prepared_build_without_stamping(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic first edit")
        self.project()
        scan = self.new_scan(epoch="synthetic-next-epoch")
        candidate = self.candidate()
        upload = scan._build(candidate, allow_compaction=False)
        self.ingest_parent("alpha", "alpha/one", "Synthetic second edit")
        self.project()
        self.assertEqual(scan._commit(candidate, upload), "stale")
        with self.store.connect() as c:
            self.assertFalse(
                c.execute(
                    "SELECT 1 FROM canonical_parquet_scan_shards WHERE tenant_id=%s AND dirty_plan_epoch=%s",
                    (self.tenant, scan.sparse_plan_epoch),
                ).fetchone()
            )

    def test_clean_unknown_month_enrolls_without_rewriting_objects(self):
        before = live_parts(self.store, self.tenant, self.source)
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_parquet_scan_shards SET dirty_plan_epoch=NULL WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        with patch.object(self.scan, "_documents", wraps=self.scan._documents) as docs:
            upload = self.scan._build(candidate, allow_compaction=False)
        self.assertEqual(upload.mode, "reuse")
        self.assertFalse(upload.created)
        self.assertTrue(
            any(call.kwargs.get("document_ids") is None for call in docs.call_args_list)
        )
        self.assertEqual(self.scan._commit(candidate, upload), "committed")
        after = live_parts(self.store, self.tenant, self.source)
        self.assertEqual(
            [(r["artifact_id"], r["version_id"]) for r in before],
            [(r["artifact_id"], r["version_id"]) for r in after],
        )
        self.assertTrue(
            all(r["dirty_plan_epoch"] == self.scan.sparse_plan_epoch for r in after)
        )

    def test_named_noop_reuses_and_new_document_is_not_lost(self):
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )

        before = live_parts(self.store, self.tenant, self.source)
        with self.store.connect() as c:
            doc = c.execute(
                "SELECT first_occurred_at,last_occurred_at FROM canonical_evidence_documents "
                "WHERE tenant_id=%s AND logical_document_id=%s",
                (self.tenant, self.ids["alpha"]),
            ).fetchone()
            CanonicalLogicalEvidenceProjector._queue_parquet_scan(
                c,
                tenant_id=self.tenant,
                source_id=self.source,
                logical_document_id=self.ids["alpha"],
                ranges=((doc["first_occurred_at"], doc["last_occurred_at"]),),
                reason="logical-update",
            )
        upload = self.assert_oracle()
        self.assertEqual(upload.mode, "reuse")
        self.assertEqual(self.scan._commit(self.candidate(), upload), "committed")
        self.assertEqual(
            [r["artifact_id"] for r in before],
            [
                r["artifact_id"]
                for r in live_parts(self.store, self.tenant, self.source)
            ],
        )
        self.ingest_parent("delta", "delta/one", "Synthetic newly added parent")
        self.project()
        self.assertEqual(len(self.rows(self.assert_oracle())["documents"]), 4)

    def test_partial_tombstone_and_actor_rename_match_full_oracle(self):
        self.ingest_parent("alpha", "alpha/two", "Synthetic retained second record")
        self.project()
        self.scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.ingest_parent("alpha", "alpha/one", "", tombstone=True)
        self.project()
        upload = self.assert_oracle()
        self.assertEqual(self.scan._commit(self.candidate(), upload), "committed")
        self.store.bind_coding_sources_to_employee(
            **self.actor_args, display_name="Synthetic renamed actor"
        )
        changed = self.assert_oracle()
        self.assertTrue(
            any(
                "Synthetic renamed actor" in row for row in self.rows(changed)["actors"]
            )
        )

    def test_untracked_old_writer_change_requires_new_epoch_full_oracle(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic old writer interval")
        self.project()
        with self.store.connect() as c:
            # Simulate the old supported writer's month-only invalidation.
            c.execute(
                "DELETE FROM canonical_parquet_scan_dirty_documents WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.scan = self.new_scan(epoch="synthetic-post-rollback-epoch")
        candidate = self.candidate()
        with patch.object(self.scan, "_documents", wraps=self.scan._documents) as docs:
            upload = self.scan._build(candidate, allow_compaction=False)
        self.assertTrue(
            any(call.kwargs.get("document_ids") is None for call in docs.call_args_list)
        )
        self.assertEqual(
            self.rows(upload),
            self.rows(
                self.new_scan(enabled=False)._build(candidate, allow_compaction=False)
            ),
        )
        self.assertEqual(self.scan._commit(candidate, upload), "committed")
        self.assertTrue(
            all(
                r["dirty_plan_epoch"] == self.scan.sparse_plan_epoch
                for r in live_parts(self.store, self.tenant, self.source)
            )
        )

    def test_unexpected_changed_sibling_uses_full_oracle(self):
        self.parts.stop()
        self.ingest_parent(
            "alpha", "alpha/one", "Synthetic full rebuild before sharing"
        )
        self.project()
        self.scan.seed_backfill(tenant_id=self.tenant)
        self.scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        with self.store.connect() as c:
            self.assertGreater(
                c.execute(
                    "SELECT max(members) AS maximum FROM ("
                    "SELECT count(*) AS members FROM canonical_parquet_scan_fragment_documents "
                    "WHERE tenant_id=%s GROUP BY dataset,shard_index) grouped",
                    (self.tenant,),
                ).fetchone()["maximum"],
                1,
            )
        self.ingest_parent("alpha", "alpha/one", "Synthetic tracked edit")
        self.ingest_parent("beta", "beta/one", "Synthetic untracked sibling edit")
        self.project()
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_parquet_scan_dirty_documents "
                "WHERE tenant_id=%s AND logical_document_id=%s",
                (self.tenant, self.ids["beta"]),
            )
        candidate = self.candidate()
        with patch.object(self.scan, "_documents", wraps=self.scan._documents) as docs:
            upload = self.scan._build(candidate, allow_compaction=False)
        self.assertTrue(
            any(call.kwargs.get("document_ids") is None for call in docs.call_args_list)
        )
        full = self.new_scan(enabled=False)._build(candidate, allow_compaction=False)
        self.assertEqual(self.rows(upload), self.rows(full))
        self.assertEqual(upload.removed, full.removed)

    def test_last_document_removal_matches_full_oracle(self):
        for parent in ("alpha", "beta", "gamma"):
            self.ingest_parent(parent, parent + "/one", "", tombstone=True)
        self.project()
        upload = self.assert_oracle()
        self.assertTrue(all(not rows for rows in self.rows(upload).values()))
        self.assertEqual(self.scan._commit(self.candidate(), upload), "committed")
        self.assertFalse(live_parts(self.store, self.tenant, self.source))

    def test_partial_enrollment_cannot_admit_sparse_plan(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic edit after partial marker")
        self.project()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_parquet_scan_shards SET dirty_plan_epoch=NULL "
                "WHERE tenant_id=%s AND dataset='actors'",
                (self.tenant,),
            )
        candidate = self.candidate()
        with patch.object(self.scan, "_documents", wraps=self.scan._documents) as docs:
            upload = self.scan._build(candidate, allow_compaction=False)
        self.assertTrue(
            all(call.kwargs.get("document_ids") is None for call in docs.call_args_list)
        )
        full = self.new_scan(enabled=False)._build(candidate, allow_compaction=False)
        self.assertEqual(self.rows(upload), self.rows(full))

    def test_marker_membership_and_dirty_cleanup_commit_atomically(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic rollback edit")
        self.project()
        scan = self.new_scan(epoch="synthetic-atomic-next-epoch")
        candidate = self.candidate()
        upload = scan._build(candidate, allow_compaction=False)
        before = live_parts(self.store, self.tenant, self.source)
        catalog = scan._catalog(candidate)
        with self.store.connect() as c:
            # Use a normal fixture function: the production commit uses another pooled connection.
            c.execute(
                "CREATE OR REPLACE FUNCTION public.fixture_refuse_sparse_finish() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic_commit_failure'; END $$"
            )
            c.execute(
                "CREATE TRIGGER fixture_sparse_finish BEFORE DELETE ON canonical_parquet_scan_queue "
                "FOR EACH ROW EXECUTE FUNCTION public.fixture_refuse_sparse_finish()"
            )
        try:
            with self.assertRaisesRegex(Exception, "synthetic_commit_failure"):
                scan._commit(candidate, upload)
        finally:
            with self.store.connect() as c:
                c.execute(
                    "DROP TRIGGER fixture_sparse_finish ON canonical_parquet_scan_queue"
                )
                c.execute("DROP FUNCTION public.fixture_refuse_sparse_finish()")
        self.assertEqual(live_parts(self.store, self.tenant, self.source), before)
        self.assertEqual(scan._catalog(candidate), catalog)
        self.assertEqual(self.candidate(), candidate)
        self.assertEqual(scan._commit(candidate, upload), "committed")
        self.assertTrue(
            all(
                row["dirty_plan_epoch"] == scan.sparse_plan_epoch
                for row in live_parts(self.store, self.tenant, self.source)
            )
        )

    def test_disabled_path_requires_no_optional_schema(self):
        self.ingest_parent("alpha", "alpha/one", "Synthetic compatible old-schema edit")
        self.project()
        with self.store.connect() as c:
            c.execute(
                "ALTER TABLE canonical_parquet_scan_shards DROP COLUMN dirty_plan_epoch"
            )
        scan = self.new_scan(enabled=False)
        candidate = self.candidate()
        upload = scan._build(candidate, allow_compaction=False)
        self.assertIsNone(upload.dirty_plan_epoch)
        self.assertEqual(len(self.rows(upload)["documents"]), 3)
        self.assertEqual(scan._commit(candidate, upload), "committed")

    def test_explicit_forget_matches_full_oracle_and_keeps_other_parents(self):
        with self.store.connect() as c:
            receipt = c.execute(
                "SELECT chunk.receipt FROM canonical_chunks chunk JOIN canonical_documents document "
                "USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.native_id=%s",
                (self.tenant, "alpha/one"),
            ).fetchone()["receipt"]
        result = self.plane.forget(
            dict(
                contract="recall.forget-request.v1",
                schema_version=1,
                tenant_id=self.tenant,
                principal_id=self.principal,
                source_id=self.source,
                target_receipt=receipt,
                mode="explicit_forget",
                reason="owner_requested",
                requested_at="2026-10-15T00:00:00Z",
                idempotency_key="synthetic-sparse-forget",
            )
        )
        self.assertEqual(result["status"], "deleted")
        self.project()
        upload = self.assert_oracle()
        self.assertEqual(
            {
                json.loads(row)["logical_document_id"]
                for row in self.rows(upload)["documents"]
            },
            {self.ids["beta"], self.ids["gamma"]},
        )


class SparseSchema(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(
            os.environ["RECALL_DATABASE_URL"], autocommit=True
        )
        self.addCleanup(self.connection.close)
        self.schema = "synthetic_sparse_" + uuid.uuid4().hex
        self.connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema))
        )
        self.addCleanup(self.drop_schema)
        self.connection.execute(
            sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema))
        )
        self.connection.execute(
            "CREATE TABLE schema_migrations(version integer PRIMARY KEY)"
        )
        self.connection.execute("INSERT INTO schema_migrations VALUES (61)")
        self.connection.execute(
            "CREATE TABLE canonical_parquet_scan_shards(id integer PRIMARY KEY)"
        )
        self.migration = (
            Path(__file__).resolve().parents[1]
            / "schema"
            / "088_parquet_dirty_plan_epoch.sql"
        ).read_text()

    def drop_schema(self):
        self.connection.execute("ROLLBACK")
        self.connection.execute(
            sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema))
        )

    def test_additive_migration_is_nullable_without_default_and_preserves_existing_epoch(
        self,
    ):
        self.connection.execute(self.migration)
        self.connection.execute(
            "INSERT INTO canonical_parquet_scan_shards VALUES (1,'synthetic-existing-epoch')"
        )
        self.connection.execute(self.migration)
        self.assertEqual(
            self.connection.execute(
                "SELECT dirty_plan_epoch FROM canonical_parquet_scan_shards WHERE id=1"
            ).fetchone(),
            ("synthetic-existing-epoch",),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT atttypid='text'::regtype,attnotnull,atthasdef FROM pg_attribute WHERE attrelid='canonical_parquet_scan_shards'::regclass AND attname='dirty_plan_epoch'"
            ).fetchone(),
            (True, False, False),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM schema_migrations WHERE version=88"
            ).fetchone(),
            (1,),
        )

    def test_missing_fragment_prerequisite_rolls_back_without_marker_or_column(self):
        self.connection.execute("DELETE FROM schema_migrations WHERE version=61")
        with self.assertRaisesRegex(
            psycopg.Error, "parquet_dirty_plan_requires_fragment_catalog"
        ):
            self.connection.execute(self.migration)
        self.connection.execute("ROLLBACK")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM schema_migrations WHERE version=88"
            ).fetchone(),
            (0,),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM pg_attribute WHERE attrelid='canonical_parquet_scan_shards'::regclass AND attname='dirty_plan_epoch' AND NOT attisdropped"
            ).fetchone(),
            (0,),
        )

    def test_wrong_existing_type_default_or_nullability_is_refused(self):
        for definition in (
            "varchar",
            "text NOT NULL",
            "text DEFAULT 'synthetic-default'",
        ):
            with self.subTest(definition=definition):
                self.connection.execute(
                    "ALTER TABLE canonical_parquet_scan_shards ADD COLUMN dirty_plan_epoch "
                    + definition
                )
                with self.assertRaisesRegex(
                    psycopg.Error, "parquet_dirty_plan_column_invalid"
                ):
                    self.connection.execute(self.migration)
                self.connection.execute("ROLLBACK")
                self.assertEqual(
                    self.connection.execute(
                        "SELECT count(*) FROM schema_migrations WHERE version=88"
                    ).fetchone(),
                    (0,),
                )
                self.connection.execute(
                    "ALTER TABLE canonical_parquet_scan_shards DROP COLUMN dirty_plan_epoch"
                )


if __name__ == "__main__":
    suite = unittest.TestSuite(
        SparsePlanning(name)
        for name in SparsePlanning.__dict__
        if name.startswith("test_")
    )
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(SparseSchema))
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
