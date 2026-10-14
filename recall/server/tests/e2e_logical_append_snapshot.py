#!/usr/bin/env python3
"""A captured append may publish while a later append remains queued."""

from pathlib import Path
import sys
import unittest
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
from unittest.mock import patch

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
]
import e2e_logical_append_reader as fixtures  # noqa: E402


class AppendSnapshot(unittest.TestCase):
    setUp = fixtures.AppendReader.setUp
    event = fixtures.AppendReader.event
    ingest = fixtures.AppendReader.ingest
    publish = fixtures.AppendReader.publish

    def catalog(self):
        with self.store.connect() as c:
            return c.execute(
                "SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            ).fetchone()

    def queue(self):
        with self.store.connect() as c:
            return c.execute(
                "SELECT * FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source),
            ).fetchone()

    safe = fixtures.AppendReader.safe
    prepare = fixtures.AppendReader.prepare

    def test_captured_suffix_commits_and_rebases_only_the_newer_queue(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.logical.append_enabled = False
        full = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.addCleanup(full.body_locators.close)
        self.logical.append_enabled = True
        self.assertEqual(upload.manifest.encode(), full.manifest.encode())
        self.assertEqual(upload.part_references, full.part_references)
        self.ingest("later", 11)
        queued = self.queue()
        self.assertGreater(queued["generation"], candidate.generation)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        current, remaining = self.catalog(), self.queue()
        self.assertEqual(current["record_count"], 4)
        self.assertEqual(current["append_source_ordinal"], 10)
        self.assertEqual(current["source_updated_at"], candidate.source_updated_at)
        for field in (
            "generation",
            "changed_at",
            "first_queued_at",
            "attempts",
            "notification_queued_at",
        ):
            self.assertEqual(remaining[field], queued[field])
        self.assertEqual(remaining["append_generation"], queued["generation"])
        self.assertEqual(
            remaining["append_base_manifest_id"], current["manifest_artifact_id"]
        )
        seen = []
        original = self.logical._record_stream

        def capture(rows, **kwargs):
            def observe():
                for row in rows:
                    seen.append(row["native_id"])
                    yield row

            return original(observe(), **kwargs)

        with patch.object(self.logical, "_record_stream", capture):
            self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(seen, ["later"])
        self.assertEqual(self.catalog()["record_count"], 5)
        self.assertIsNone(self.queue())

    def assert_late_arrival_refused(self, ordinal, record_type=None):
        from recall_server.logical_evidence import LogicalEvidenceError

        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        original = self.catalog()
        self.ingest(
            "late-backfill",
            ordinal,
            **({"record_type": record_type} if record_type else {}),
        )
        self.assertTrue(self.safe())
        with self.assertRaisesRegex(
            LogicalEvidenceError, "logical_evidence_source_changed"
        ):
            self.logical._commit_upload(candidate, upload)
        self.assertEqual(self.catalog(), original)
        self.assertIsNotNone(self.queue())
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["record_count"], 4 if record_type else 5)

    def test_later_arrival_inside_captured_ordinal_window_cannot_be_skipped(self):
        self.assert_late_arrival_refused(5)

    def test_later_arrival_at_captured_ordinal_cannot_be_skipped(self):
        self.assert_late_arrival_refused(10)

    def test_later_excluded_structural_arrival_cannot_be_skipped(self):
        self.assert_late_arrival_refused(5, "token_count")

    def test_structural_snapshot_keeps_newer_visible_append_queued(self):
        self.ingest("visible", 1)
        self.publish()
        before = self.catalog()
        self.logical.append_enabled = True
        self.ingest("structural", 2, record_type="token_count")
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 3)
        queued = self.queue()
        self.assertEqual(self.logical._commit_upload(candidate, upload), "repaired")
        current = self.catalog()
        self.assertEqual(
            current["manifest_artifact_id"], before["manifest_artifact_id"]
        )
        self.assertEqual(current["append_source_ordinal"], 2)
        self.assertEqual(self.queue()["generation"], queued["generation"])
        self.assertEqual(self.publish()["records"], 2)
        self.assertIsNone(self.queue())

    def test_append_during_verified_prefix_read_does_not_discard_captured_suffix(self):
        candidate, prepared = self.prepare()
        prepared.body_locators.close()
        original = self.archive.read_raw
        triggered = []

        def read(reference):
            payload = original(reference)
            if not triggered and reference["media_type"].endswith("part+jsonl"):
                triggered.append(True)
                self.ingest("later", 11)
            return payload

        with (
            patch.object(self.archive, "read_raw", read),
            patch(
                "recall_server.logical_evidence_projection.PROJECTION_PROGRESS_INTERVAL_SECONDS",
                0,
            ),
        ):
            result = self.publish()
        self.assertEqual(triggered, [True])
        self.assertEqual(result["documents"], 1)
        self.assertEqual(self.catalog()["append_source_ordinal"], 10)
        self.assertIsNotNone(self.queue())

    def test_edited_captured_record_still_refuses_after_later_append(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 11)
        self.ingest("new", 12, "changed captured body")
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertEqual(self.catalog()["record_count"], 3)

    def test_tombstone_still_refuses_after_later_append(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 11)
        self.ingest("new", 12, kind="tombstone")
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")

    def test_actor_poison_cannot_be_revived_by_more_appends(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        with self.store.connect() as c:
            self.store._queue_actor_projection(
                c, tenant_id=self.tenant, source_ids=[self.source]
            )
        self.ingest("later", 11)
        self.assertIsNone(self.queue()["append_generation"])
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")

    def test_catalog_identity_change_refuses_even_with_same_manifest(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 11)
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_evidence_documents SET created_at=created_at+interval '1 second' WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")

    def test_same_generation_changed_timestamp_is_not_an_append_chain(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_evidence_document_queue SET changed_at=clock_timestamp() WHERE tenant_id=%s",
                (self.tenant,),
            )
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")

    def test_competing_publisher_cannot_overwrite_the_new_snapshot_base(self):
        candidate, first = self.prepare()
        second = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.addCleanup(first.body_locators.close)
        self.addCleanup(second.body_locators.close)
        self.ingest("later", 11)
        self.assertEqual(self.logical._commit_upload(candidate, first), "committed")
        catalog, queued = self.catalog(), self.queue()
        self.assertEqual(self.logical._commit_upload(candidate, second), "adopted")
        self.assertEqual(self.catalog(), catalog)
        self.assertEqual(self.queue(), queued)
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.logical.projection.read_manifest(
            first.manifest_reference, tenant_id=self.tenant, source_id=self.source
        )

    def test_lost_actual_commit_ack_retains_newer_queue_and_archive(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 11)
        queued = self.queue()
        commit = self.logical._commit

        def lost(*args, **kwargs):
            status = commit(*args, **kwargs)
            self.assertEqual(status, "committed")
            raise psycopg.OperationalError("synthetic lost commit acknowledgment")

        with patch.object(self.logical, "_commit", lost):
            with self.assertRaises(psycopg.OperationalError):
                self.logical._commit_upload(candidate, upload)
        self.assertEqual(self.queue()["generation"], queued["generation"])
        self.assertEqual(self.catalog()["append_source_ordinal"], 10)
        before_reconcile = self.queue()
        self.assertEqual(self.logical._commit_upload(candidate, upload), "adopted")
        self.assertEqual(self.queue(), before_reconcile)
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.logical.projection.read_manifest(
            upload.manifest_reference, tenant_id=self.tenant, source_id=self.source
        )
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["record_count"], 5)

    def test_revoked_reader_cannot_serve_materialized_snapshot(self):
        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        self.ingest("later", 11)
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO brain_organizations VALUES(%s,'company','Snapshot test',now())",
                ("organization:" + self.tenant,),
            )
            c.execute(
                "INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','snapshot')",
                (self.tenant, "organization:" + self.tenant),
            )
            c.execute(
                "INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES(%s,%s,'owner') ON CONFLICT DO NOTHING",
                (self.tenant, self.principal),
            )
        self.assertIn(
            self.source,
            self.store.authorized_canonical_source_ids(self.tenant, self.principal),
        )
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s",
                (self.tenant, self.source, self.principal),
            )
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        authorized = tuple(
            self.store.authorized_canonical_source_ids(self.tenant, self.principal)
        )
        self.assertNotIn(self.source, authorized)
        self.assertIsNone(
            self.store.resolve(
                f"recall://{self.source}/new?rev=1#item=0",
                tenant_id=self.tenant,
                authorized_sources=authorized,
                chunk_body_archive=self.archive,
            )
        )

    def wait_for_lock(self, pid):
        end = time.monotonic() + 5
        while time.monotonic() < end:
            with self.store.connect() as c:
                row = c.execute(
                    "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (pid,)
                ).fetchone()
            if row and row["wait_event_type"] == "Lock":
                return
            time.sleep(0.005)
        self.fail("expected real PostgreSQL queue-lock contention")

    def test_append_waiting_behind_publication_reclassifies_against_new_manifest(self):
        from recall_server import canonical

        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        ready = threading.Event()
        writer_pid = []
        original_dirty = canonical.mark_logical_evidence_dirty
        original_finish = self.logical._finish_queue
        futures = []

        def dirty(connection, **kwargs):
            writer_pid.append(connection.info.backend_pid)
            ready.set()
            return original_dirty(connection, **kwargs)

        with ThreadPoolExecutor(max_workers=1) as pool:

            def finish(*args, **kwargs):
                futures.append(pool.submit(self.ingest, "later", 11))
                self.assertTrue(ready.wait(5))
                self.wait_for_lock(writer_pid[0])
                return original_finish(*args, **kwargs)

            with (
                patch.object(canonical, "mark_logical_evidence_dirty", dirty),
                patch.object(self.logical, "_finish_queue", finish),
            ):
                self.assertEqual(
                    self.logical._commit_upload(candidate, upload), "committed"
                )
                futures[0].result(timeout=5)
        self.assertEqual(self.catalog()["append_source_ordinal"], 10)
        self.assertTrue(self.safe())
        self.assertEqual(
            self.queue()["append_base_manifest_id"],
            self.catalog()["manifest_artifact_id"],
        )
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["record_count"], 5)

    def test_publication_waiting_behind_append_retains_its_committed_generation(self):
        from recall_server import canonical, archive_ownership

        candidate, upload = self.prepare()
        self.addCleanup(upload.body_locators.close)
        writer_ready, allow_writer, publisher_ready = (
            threading.Event() for _ in range(3)
        )
        publisher_pid = []
        original_dirty = canonical.mark_logical_evidence_dirty
        original_admit = archive_ownership.admit_references

        def dirty(connection, **kwargs):
            result = original_dirty(connection, **kwargs)
            writer_ready.set()
            if not allow_writer.wait(5):
                raise RuntimeError("test release missing")
            return result

        def admit(connection, *args, **kwargs):
            result = original_admit(connection, *args, **kwargs)
            publisher_pid.append(connection.info.backend_pid)
            publisher_ready.set()
            return result

        with ThreadPoolExecutor(max_workers=2) as pool:
            with patch.object(canonical, "mark_logical_evidence_dirty", dirty):
                writer = pool.submit(self.ingest, "later", 11)
                try:
                    self.assertTrue(writer_ready.wait(5))
                    with patch.object(archive_ownership, "admit_references", admit):
                        publisher = pool.submit(
                            self.logical._commit_upload, candidate, upload
                        )
                        self.assertTrue(publisher_ready.wait(5))
                        self.wait_for_lock(publisher_pid[-1])
                        allow_writer.set()
                        writer.result(timeout=5)
                        self.assertEqual(publisher.result(timeout=5), "committed")
                finally:
                    allow_writer.set()
        self.assertEqual(self.catalog()["append_source_ordinal"], 10)
        self.assertGreater(self.queue()["generation"], candidate.generation)
        self.assertTrue(self.safe())
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["record_count"], 5)


if __name__ == "__main__":
    unittest.main()
