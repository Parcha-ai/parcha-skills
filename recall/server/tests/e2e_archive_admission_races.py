#!/usr/bin/env python3
"""An upload receipt alone cannot authorize a dangling catalogue reference."""

# ruff: noqa: E402
from contextlib import contextmanager
from pathlib import Path
import threading
import sys
import unittest
import json
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_archive_reference_lifetime as fixtures
from recall_server.archive import ArchiveNotFound
from recall_server.parquet_scan import CanonicalParquetScanProjector


class ArchiveAdmissionTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.ArchiveReferenceTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveReferenceTests.tearDownClass.__func__)
    setUp = fixtures.ArchiveReferenceTests.setUp
    event = fixtures.ArchiveReferenceTests.event
    write = fixtures.ArchiveReferenceTests.write
    references = fixtures.ArchiveReferenceTests.references

    def test_forget_deletes_raw_and_bundle_without_reading_object_bodies(self):
        self.write(self.event())
        self.evidence.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        with patch.object(self.archive, "read", side_effect=AssertionError("body read under lock")):
            result = self.plane.forget(dict(
                contract="recall.forget-request.v1", schema_version=1,
                tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                target_receipt=f"recall://{self.source}/turn?rev=1#item=0",
                mode="explicit_forget", reason="owner_requested",
                requested_at="2026-10-13T00:00:00Z", idempotency_key="forget-no-body",
            ))
        self.assertEqual(result["status"], "deleted")
        self.assertEqual(self.references(), [])

    def remove_unregistered(self, references):
        self.logical._schedule_cleanup(tuple(references))
        result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertGreater(result["deleted"], 0)

    def test_bundle_forget_keeps_an_object_still_owned_by_raw_archive(self):
        self.write(self.event())
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        reference = self.evidence._reference(self.references()[0])
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO raw_artifacts SELECT * FROM jsonb_populate_record(NULL::raw_artifacts,%s::jsonb)",
                (json.dumps(dict(reference, state="live")),),
            )
        before = self.archive.read_raw(reference)
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.assertEqual(
            self.evidence.delete_native_ids(
                tenant_id=self.tenant, source_id=self.source, native_ids=["turn"]
            ),
            1,
        )
        self.assertEqual(self.references(), [])
        self.assertEqual(self.archive.read_raw(reference), before)

    def test_raw_forget_keeps_an_object_still_owned_by_another_bundle(self):
        self.write(self.event())
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        reference = self.evidence._reference(self.references()[0])
        event = self.event("other")
        event["provenance"]["artifact_ref"] = reference
        self.write(event)
        before = self.archive.read_raw(reference)
        result = self.plane.forget(
            dict(
                contract="recall.forget-request.v1",
                schema_version=1,
                tenant_id=self.tenant,
                principal_id=self.principal,
                source_id=self.source,
                target_receipt=f"recall://{self.source}/other?rev=1#item=0",
                mode="explicit_forget",
                reason="owner_requested",
                requested_at="2026-10-13T00:00:00Z",
                idempotency_key="forget-other",
            )
        )
        self.assertEqual(result["status"], "deleted")
        self.assertEqual(self.archive.read_raw(reference), before)

    def test_bundle_publication_rechecks_native_forget_after_upload(self):
        self.write(self.event())
        original = self.evidence.projection.put

        def upload_then_forget(**kwargs):
            reference = original(**kwargs)
            with self.store.connect() as c:
                c.execute(
                    "UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s",
                    (self.tenant,),
                )
            return reference

        with patch.object(
            self.evidence.projection, "put", side_effect=upload_then_forget
        ):
            with self.assertRaisesRegex(Exception, "evidence_publication_stale"):
                self.evidence.project_pending(
                    tenant_id=self.tenant,
                    batch_size=1,
                    max_batches=1,
                    upload_concurrency=1,
                )
        self.assertEqual(self.references(), [])

    def test_bundle_delete_refuses_without_committed_forget(self):
        self.write(self.event())
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        reference = self.evidence._reference(self.references()[0])
        with self.assertRaisesRegex(Exception, "evidence_forget_not_started"):
            self.evidence.delete_native_ids(
                tenant_id=self.tenant, source_id=self.source, native_ids=["turn"]
            )
        self.assertTrue(self.archive.read_raw(reference))

    def test_admission_deduplicates_new_references_and_reuses_live_without_head(self):
        from recall_server.archive_ownership import admit_references

        event = self.event()
        reference = event["provenance"]["artifact_ref"]
        with patch.object(
            self.archive, "verify_raw_bounded", wraps=self.archive.verify_raw_bounded
        ) as verify:
            with self.store.connect() as c:
                self.assertEqual(
                    admit_references(c, self.archive, [reference] * 100), 1
                )
            self.assertEqual(verify.call_count, 1)
        self.write(event)
        with patch.object(
            self.archive,
            "verify_raw_bounded",
            side_effect=AssertionError("unexpected HEAD"),
        ):
            with self.store.connect() as c:
                self.assertEqual(
                    admit_references(c, self.archive, [reference] * 100), 0
                )

    def test_admission_does_not_trust_deleted_bundle_or_deleting_raw(self):
        from recall_server.archive_ownership import admit_references

        event = self.event()
        self.write(event)
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        reference = self.evidence._reference(self.references()[0])
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s",
                (self.tenant,),
            )
            c.execute(
                "UPDATE raw_artifacts SET state='deleting' WHERE tenant_id=%s",
                (self.tenant,),
            )
        for value in [reference, event["provenance"]["artifact_ref"]]:
            self.archive.delete_raw(value)
            with self.store.connect() as c, self.assertRaises(ArchiveNotFound):
                admit_references(c, self.archive, [value])

    def test_all_admissions_hold_artifact_guard_through_head(self):
        for owner in ("raw", "bundle", "logical", "parquet"):
            with self.subTest(owner=owner):
                event = self.event("guard-" + owner)
                if owner != "raw":
                    self.write(event)
                if owner == "parquet":
                    self.logical.project_pending(
                        tenant_id=self.tenant, batch_size=10, max_batches=1
                    )
                original = self.archive.verify_raw_bounded
                lock = threading.Lock()
                observed = []

                def verify(reference, *, deadline_at):
                    with lock:
                        if not observed:
                            self.logical._schedule_cleanup((reference,))
                            result = self.logical.drain_cleanup(tenant_id=self.tenant)
                            self.assertEqual(result["deleted"], 0)
                            observed.append(reference)
                    original(reference, deadline_at=deadline_at)

                with patch.object(
                    self.archive, "verify_raw_bounded", side_effect=verify
                ):
                    if owner == "raw":
                        self.write(event)
                    elif owner == "bundle":
                        self.evidence.project_pending(
                            tenant_id=self.tenant, batch_size=10, max_batches=1
                        )
                    elif owner == "logical":
                        self.logical.project_pending(
                            tenant_id=self.tenant, batch_size=10, max_batches=1
                        )
                    else:
                        CanonicalParquetScanProjector(
                            self.store, self.logical.projection
                        ).project_pending(
                            tenant_id=self.tenant, batch_size=10, max_batches=1
                        )
                self.assertTrue(observed)
                self.logical.drain_cleanup(tenant_id=self.tenant)
                for reference in observed:
                    self.assertTrue(self.archive.read_raw(reference))

    def test_bundle_delete_rollback_does_not_make_missing_object_trusted(self):
        from recall_server.archive_ownership import admit_references

        self.write(self.event())
        self.evidence.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1
        )
        reference = self.evidence._reference(self.references()[0])
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s",
                (self.tenant,),
            )
        original = self.store.connect

        @contextmanager
        def fail_commit():
            with original() as c:
                yield c
                raise RuntimeError("synthetic lost commit")

        with patch.object(self.store, "connect", side_effect=fail_commit):
            with self.assertRaisesRegex(RuntimeError, "synthetic lost commit"):
                self.evidence.delete_native_ids(
                    tenant_id=self.tenant, source_id=self.source, native_ids=["turn"]
                )
        self.assertEqual(len(self.references()), 1)
        with self.store.connect() as c, self.assertRaises(ArchiveNotFound):
            admit_references(c, self.archive, [reference])
        self.assertEqual(
            self.evidence.delete_native_ids(
                tenant_id=self.tenant, source_id=self.source, native_ids=["turn"]
            ),
            1,
        )
        self.assertEqual(self.references(), [])

    def test_raw_admission_refuses_object_deleted_after_upload(self):
        event = self.event()
        self.remove_unregistered([event["provenance"]["artifact_ref"]])
        with self.assertRaises(ArchiveNotFound):
            self.write(event)

    def test_bundle_admission_refuses_object_deleted_after_upload(self):
        self.write(self.event())
        original = self.evidence.projection.put

        def upload_then_delete(**kwargs):
            reference = original(**kwargs)
            self.remove_unregistered([reference])
            return reference

        with patch.object(
            self.evidence.projection, "put", side_effect=upload_then_delete
        ):
            try:
                self.evidence.project_pending(
                    tenant_id=self.tenant,
                    batch_size=1,
                    max_batches=1,
                    upload_concurrency=1,
                )
            except ArchiveNotFound:
                pass
        self.assertEqual(
            self.references(), [], "bundle catalogue references a deleted upload"
        )

    def test_logical_admission_refuses_objects_deleted_after_upload(self):
        self.write(self.event())
        original = self.logical._commit

        def delete_then_commit(candidate, upload):
            self.remove_unregistered(upload.cleanup_references)
            return original(candidate, upload)

        with patch.object(self.logical, "_commit", side_effect=delete_then_commit):
            with self.assertRaises(ArchiveNotFound):
                self.logical.project_pending(
                    tenant_id=self.tenant,
                    batch_size=1,
                    max_batches=1,
                    upload_concurrency=1,
                )
        with self.store.connect() as c:
            count = c.execute(
                "SELECT count(*) AS n FROM canonical_evidence_documents WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["n"]
        self.assertEqual(count, 0, "logical catalogue references deleted uploads")

    def test_failed_logical_upload_does_not_delete_a_concurrent_winners_part(self):
        from recall_server.logical_evidence import PART_MEDIA_TYPE, MANIFEST_MEDIA_TYPE
        self.write(self.event())
        original = self.archive.put_raw
        shared = []

        def loser_put(**kwargs):
            if kwargs["media_type"] == MANIFEST_MEDIA_TYPE and shared:
                raise RuntimeError("synthetic manifest upload failure")
            reference = original(**kwargs)
            if kwargs["media_type"] == PART_MEDIA_TYPE and not shared:
                shared.append(reference)
                with patch.object(self.archive, "put_raw", side_effect=original):
                    winner = self.logical.project_pending(tenant_id=self.tenant,
                        batch_size=1, max_batches=1, upload_concurrency=1)
                self.assertEqual(winner["documents"], 1)
            return reference

        with patch.object(self.archive, "put_raw", side_effect=loser_put):
            loser = self.logical.project_pending(tenant_id=self.tenant,
                batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(loser["failed"], 1)
        self.assertEqual(len(shared), 1)
        self.archive.verify_raw(shared[0])
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.archive.verify_raw(shared[0])

    def test_parquet_admission_refuses_objects_deleted_after_upload(self):
        self.write(self.event())
        self.logical.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        scan = CanonicalParquetScanProjector(self.store, self.logical.projection)
        original = scan._commit

        def delete_then_commit(candidate, upload):
            self.remove_unregistered(upload.references.values())
            return original(candidate, upload)

        with patch.object(scan, "_commit", side_effect=delete_then_commit):
            try:
                scan.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
            except ArchiveNotFound:
                pass
        with self.store.connect() as c:
            count = c.execute(
                "SELECT count(*) AS n FROM canonical_parquet_scan_shards WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["n"]
        self.assertEqual(count, 0, "Parquet catalogue references deleted uploads")


if __name__ == "__main__":
    unittest.main()
