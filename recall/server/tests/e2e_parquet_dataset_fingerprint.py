"""Native witnesses for passage-only scan invalidation."""

import json
from unittest.mock import patch
from e2e_passage_origin import PassageOrigin
from e2e_legacy_tool_migration import LegacyToolMigration
from e2e_parquet_sparse_planning import SparsePlanning
from recall_server.parquet_scan import ParquetScanError
from e2e_parquet_dirty_completeness import DirtyCompleteness
from e2e_parquet_scan_fragments import live_parts, read_dataset
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.parquet_scan import CanonicalParquetScanProjector
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy


class DatasetFingerprintWitness(DirtyCompleteness):
    def test_passage_only_reprojection_preserves_raw_datasets(self):
        self.store.verified_singleton_receipts = True
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        args = dict(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.assertGreater(scan.project_pending(**args)["shards"], 0)
        before_parts = live_parts(self.store, self.tenant, self.source)
        datasets = ("records", "documents", "actors")
        before = {
            name: read_dataset(self.archive, before_parts, name) for name in datasets
        }
        before_passages = read_dataset(self.archive, before_parts, "passages")
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
        with (
            patch.object(
                scan, "_project_document", wraps=scan._project_document
            ) as raw,
            patch.object(
                self.archive, "read_raw", wraps=self.archive.read_raw
            ) as reads,
        ):
            outcome = scan.project_pending(**args)
        self.assertTrue(reads.call_args_list)
        self.assertTrue(
            all(
                call.args[0]["media_type"] == "application/vnd.apache.parquet"
                for call in reads.call_args_list
            )
        )
        self.assertGreater(outcome["shards"], 0)
        after_parts = live_parts(self.store, self.tenant, self.source)
        after = {
            name: read_dataset(self.archive, after_parts, name) for name in datasets
        }
        self.assertEqual(before, after)
        self.assertNotEqual(
            before_passages, read_dataset(self.archive, after_parts, "passages")
        )
        old_keys = {p["object_key"] for p in before_parts if p["dataset"] in datasets}
        new_keys = {p["object_key"] for p in after_parts if p["dataset"] in datasets}
        print(
            json.dumps(
                dict(
                    witness="passage_only_raw_reprojection",
                    raw_projection_calls=raw.call_count,
                    identical_raw_rows={
                        name: len(rows) for name, rows in after.items()
                    },
                    old_raw_fragments=len(old_keys),
                    new_raw_fragments=len(new_keys),
                    retained_raw_fragments=len(old_keys & new_keys),
                )
            ),
            flush=True,
        )
        self.assertEqual(
            raw.call_count, 0, "unchanged raw datasets must not be reprojected"
        )


class SameCountExtractionWitness(LegacyToolMigration):
    def test_same_count_native_tool_enrollment_refreshes_scan_rows(self):
        self.seed(historical=True)
        old = self.projected()
        self.assertEqual(len(old), 1)
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        args = dict(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.assertGreater(scan.project_pending(**args)["shards"], 0)
        row = self.document()
        result = self.passages.request_extraction_version(
            source_id=self.source,
            logical_document_id=row["logical_document_id"],
            revision=row["revision"],
            source_document_sha256=row["source_document_sha256"],
            extraction_version="tool_groups_v1",
            principal_id=self.principal,
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(self.project()["documents"], 1)
        current = self.projected()
        self.assertEqual(len(current), 1)
        self.assertNotEqual(old[0]["passage_id"], current[0]["passage_id"])
        self.assertEqual(old[0]["text_redacted"], current[0]["text_redacted"])
        # A subsequent logical no-op cannot erase the actual passage commit's dirt.
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )

        with self.store.connect() as connection:
            native = connection.execute(
                "SELECT first_occurred_at,last_occurred_at FROM canonical_evidence_documents WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()
            CanonicalLogicalEvidenceProjector._queue_parquet_scan(
                connection,
                tenant_id=self.tenant,
                source_id=self.source,
                logical_document_id=row["logical_document_id"],
                ranges=((native["first_occurred_at"], native["last_occurred_at"]),),
                reason="logical-update",
            )
        candidate = scan._pending(tenant_id=self.tenant, limit=1)[0]
        documents = scan._documents(candidate)
        catalog = scan._catalog(candidate)
        mode, victims, rewrite, dirty = scan._plan(
            candidate, documents, catalog, allow_compaction=False
        )
        print(
            json.dumps(
                dict(
                    witness="same_count_native_extraction_enrollment",
                    old_count=len(old),
                    current_count=len(current),
                    passage_identity_changed=True,
                    scan_plan=mode,
                    dirty_documents=len(dirty),
                    rewritten_documents=len(rewrite),
                )
            ),
            flush=True,
        )
        scan.project_pending(**args)
        rows = read_dataset(
            self.archive, live_parts(self.store, self.tenant, self.source), "passages"
        )
        self.assertEqual(
            {p["passage_id"] for p in rows},
            {p["passage_id"] for p in current},
            "scan must publish the current native extraction identity",
        )


class SharedDatasetWitness(SparsePlanning):
    def test_shared_fragments_keep_passage_only_raw_and_rebuild_changed_sibling(self):
        self.parts.stop()
        self.scan.seed_backfill(tenant_id=self.tenant)
        self.assertGreater(
            self.scan.project_pending(
                tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
            )["shards"],
            0,
        )
        self.ingest_parent(
            "alpha", "alpha/one", "Synthetic actual raw change for alpha"
        )
        self.project()
        projector = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=PassagePolicy(target_tokens=4, overlap_tokens=0),
            bound_tenant_id=self.tenant,
        )
        projector.seed_backfill(tenant_id=self.tenant)
        candidate = next(
            row
            for row in projector._pending(tenant_id=self.tenant, limit=10)
            if row.logical_document_id == self.ids["beta"]
        )
        prepared = projector._prepare(candidate)
        try:
            self.assertEqual(projector._commit(prepared)["status"], "committed")
        finally:
            prepared.close()
        candidate = self.candidate()
        with patch.object(
            self.scan, "_project_document", wraps=self.scan._project_document
        ) as raw:
            result = self.scan._build(candidate, allow_compaction=False)
        self.assertEqual(
            {call.args[1]["logical_document_id"] for call in raw.call_args_list},
            {self.ids["alpha"]},
        )
        oracle = self.scan._build(candidate, _rebuild=True)
        self.assertEqual(self.rows(result), self.rows(oracle))
        self.assertEqual(self.scan._commit(candidate, result), "committed")
        # A subsequent named logical no-op must use the refreshed member identity.
        from recall_server.logical_evidence_projection import (
            CanonicalLogicalEvidenceProjector,
        )

        with self.store.connect() as connection:
            native = connection.execute(
                "SELECT first_occurred_at,last_occurred_at FROM canonical_evidence_documents WHERE tenant_id=%s AND logical_document_id=%s",
                (self.tenant, self.ids["beta"]),
            ).fetchone()
            CanonicalLogicalEvidenceProjector._queue_parquet_scan(
                connection,
                tenant_id=self.tenant,
                source_id=self.source,
                logical_document_id=self.ids["beta"],
                ranges=((native["first_occurred_at"], native["last_occurred_at"]),),
                reason="logical-update",
            )
        self.assertEqual(self.assert_oracle().mode, "reuse")

    def passage_only_build(self):
        projector = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=PassagePolicy(target_tokens=4, overlap_tokens=0),
            bound_tenant_id=self.tenant,
        )
        projector.seed_backfill(tenant_id=self.tenant)
        selected = next(
            row
            for row in projector._pending(tenant_id=self.tenant, limit=10)
            if row.logical_document_id == self.ids["beta"]
        )
        prepared = projector._prepare(selected)
        try:
            self.assertEqual(projector._commit(prepared)["status"], "committed")
        finally:
            prepared.close()
        candidate = self.candidate()
        result = self.scan._build(candidate, allow_compaction=False)
        self.assertTrue(result.retained_members)
        return candidate, result

    def member_rows(self):
        with self.store.connect() as connection:
            return connection.execute(
                "SELECT * FROM canonical_parquet_scan_fragment_documents WHERE tenant_id=%s ORDER BY dataset,shard_index,logical_document_id",
                (self.tenant,),
            ).fetchall()

    def test_retained_member_compare_and_swap_rolls_back_catalog(self):
        candidate, result = self.passage_only_build()
        identity, pairs = next(iter(result.retained_members.items()))
        old, new = pairs[0]
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_parquet_scan_fragment_documents SET generation_sha256=%s WHERE tenant_id=%s AND dataset=%s AND shard_index=%s AND logical_document_id=%s",
                ("0" * 64, self.tenant, *identity, old.logical_document_id),
            )
        before_members = self.member_rows()
        before_parts = live_parts(self.store, self.tenant, self.source)
        with self.assertRaises(ParquetScanError):
            self.scan._commit(candidate, result)
        self.assertEqual(self.member_rows(), before_members)
        self.assertEqual(live_parts(self.store, self.tenant, self.source), before_parts)
        self.assertEqual(self.candidate(), candidate)
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_parquet_scan_fragment_documents SET generation_sha256=%s WHERE tenant_id=%s AND dataset=%s AND shard_index=%s AND logical_document_id=%s",
                (
                    old.generation_sha256,
                    self.tenant,
                    *identity,
                    old.logical_document_id,
                ),
            )
        self.assertEqual(self.scan._commit(candidate, result), "committed")

    def test_later_logical_revision_refuses_retained_member_refresh(self):
        candidate, result = self.passage_only_build()
        self.ingest_parent(
            "beta", "beta/one", "A later source revision supersedes prepared raw reuse"
        )
        self.project()
        before_members = self.member_rows()
        before_parts = live_parts(self.store, self.tenant, self.source)
        self.assertEqual(self.scan._commit(candidate, result), "stale")
        self.assertEqual(self.member_rows(), before_members)
        self.assertEqual(live_parts(self.store, self.tenant, self.source), before_parts)
        self.assertGreater(self.candidate().generation, candidate.generation)
        self.assertEqual(
            self.rows(self.assert_oracle()),
            self.rows(self.scan._build(self.candidate(), _rebuild=True)),
        )

    def test_unavailable_old_document_fragment_rebuilds_only_that_parent(self):
        from recall_server.archive import ArchiveNotFound

        candidate, unused = self.passage_only_build()
        catalog = self.scan._catalog(candidate)
        identity = next(
            identity
            for identity, members in catalog.members.items()
            if identity[0] == "documents"
            and any(m.logical_document_id == self.ids["beta"] for m in members)
        )
        artifact_id = catalog.shards[identity]["artifact_id"]
        original = self.archive.read_raw

        def read(reference):
            if reference["artifact_id"] == artifact_id:
                raise ArchiveNotFound("synthetic missing document metadata fragment")
            return original(reference)

        with (
            patch.object(self.archive, "read_raw", side_effect=read),
            patch.object(
                self.scan, "_project_document", wraps=self.scan._project_document
            ) as raw,
        ):
            result = self.scan._build(candidate, allow_compaction=False)
        self.assertEqual(result.mode, "delta")
        self.assertEqual(
            {call.args[1]["logical_document_id"] for call in raw.call_args_list},
            {self.ids["beta"]},
        )
        self.assertEqual(
            self.rows(result), self.rows(self.scan._build(candidate, _rebuild=True))
        )

    def test_forget_after_raw_equality_proof_refuses_prepared_commit(self):
        candidate, result = self.passage_only_build()
        with self.store.connect() as connection:
            receipt = connection.execute(
                "SELECT chunk.receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.native_id=%s",
                (self.tenant, "beta/one"),
            ).fetchone()["receipt"]
        self.assertEqual(
            self.plane.forget(
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
                    idempotency_key="raw-reuse-forget",
                )
            )["status"],
            "deleted",
        )
        self.project()
        members = self.member_rows()
        self.assertEqual(self.scan._commit(candidate, result), "stale")
        self.assertEqual(self.member_rows(), members)
        current = self.assert_oracle()
        self.assertNotIn(
            self.ids["beta"],
            {
                json.loads(row)["logical_document_id"]
                for row in self.rows(current)["documents"]
            },
        )

    def test_actual_unchanged_passage_commit_keeps_scan_reuse(self):
        from e2e_passage_origin import PassageOrigin

        before = live_parts(self.store, self.tenant, self.source)
        PassageOrigin.requeue(self)
        outcome = self.passages.project_pending(
            tenant_id=self.tenant, batch_size=20, max_batches=1, concurrency=1
        )
        self.assertEqual(outcome["documents"], 3)
        with patch.object(
            self.scan, "_project_document", wraps=self.scan._project_document
        ) as raw:
            result = self.scan._build(self.candidate(), allow_compaction=False)
        self.assertEqual(result.mode, "reuse")
        self.assertEqual(raw.call_count, 0)
        self.assertEqual(self.scan._commit(self.candidate(), result), "committed")
        self.assertEqual(live_parts(self.store, self.tenant, self.source), before)


class RetainedMetadataWitness(PassageOrigin):
    def test_same_id_native_span_repair_forces_passage_refresh(self):
        from recall_server import passage_projection

        self.ingest("same-id-origin")
        with patch.object(passage_projection, "_message_origin", return_value=None):
            self.assertEqual(self.project()["documents"], 1)
        before = self.projected()
        self.assertNotIn("origin", before[0]["spans"][0])
        scan = CanonicalParquetScanProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive)
        )
        args = dict(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.assertGreater(scan.project_pending(**args)["shards"], 0)
        self.requeue()
        self.assertEqual(self.project()["documents"], 1)
        after = self.projected()
        self.assertEqual(before[0]["passage_id"], after[0]["passage_id"])
        self.assertEqual(before[0]["text_redacted"], after[0]["text_redacted"])
        self.assertIn("origin", after[0]["spans"][0])
        candidate = scan._pending(tenant_id=self.tenant, limit=1)[0]
        catalog = scan._catalog(candidate)
        self.assertEqual(
            catalog.forced_documents, frozenset({after[0]["logical_document_id"]})
        )
        with patch.object(
            scan, "_project_document", wraps=scan._project_document
        ) as raw:
            prepared = scan._build(candidate, allow_compaction=False)
        self.assertEqual(prepared.mode, "delta")
        self.assertEqual(raw.call_count, 0)
        self.assertEqual(scan._commit(candidate, prepared), "committed")
        self.assertEqual(
            {
                row["passage_id"]
                for row in read_dataset(
                    self.archive,
                    live_parts(self.store, self.tenant, self.source),
                    "passages",
                )
            },
            {row["passage_id"] for row in after},
        )
