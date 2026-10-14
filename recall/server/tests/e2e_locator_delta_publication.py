#!/usr/bin/env python3
"""Real PostgreSQL: unchanged locator prefixes do not become commit-time work."""

from pathlib import Path
import sys
import unittest
import hashlib
import time

from unittest.mock import patch

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
]
import e2e_archive_reference_lifetime as fixtures  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402
from recall_server.streaming_locators import publish_parent_locators  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceError  # noqa: E402
from recall_server.logical_evidence_projection import _LocatorSpool  # noqa: E402


class LocatorDeltaTests(unittest.TestCase):
    setUpClass = classmethod(fixtures.ArchiveReferenceTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveReferenceTests.tearDownClass.__func__)
    setUp = fixtures.ArchiveReferenceTests.setUp
    event = fixtures.ArchiveReferenceTests.event
    write = fixtures.ArchiveReferenceTests.write

    def append(self, natives):
        events = []
        for native in natives:
            event = self.event(native)
            event["native_parent_id"] = "parent"
            events.append(event)
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=events
        )

    def locations(self):
        with self.store.connect() as c:
            return c.execute(
                """SELECT document_id,native_id,body_record_ordinal,body_record_count
                FROM canonical_documents WHERE tenant_id=%s AND source_id=%s
                AND is_current AND deleted_at IS NULL ORDER BY native_id""",
                (self.tenant, self.source),
            ).fetchall()

    def prepare_append(self):
        self.append([f"turn-{n:03d}" for n in range(12)])
        first = self.logical.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1
        )
        self.assertEqual(first["documents"], 1)
        before = self.locations()
        self.assertTrue(all(row["body_record_ordinal"] is not None for row in before))
        self.append(["turn-999"])
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        return before, candidate, upload

    def test_append_spools_only_new_locator_work(self):
        before, candidate, upload = self.prepare_append()
        try:
            self.assertEqual(
                len(list(upload.body_locators)),
                1,
                "unchanged prefix must not be re-enumerated in the catalog commit",
            )
            self.assertEqual(
                self.logical._commit_upload(candidate, upload), "committed"
            )
            self.assertEqual(self.locations()[:12], before)
            self.assertIsNotNone(self.locations()[-1]["body_record_ordinal"])
        finally:
            upload.body_locators.close()

    def test_prepared_commit_does_not_scan_parent_membership_again(self):
        before, candidate, upload = self.prepare_append()
        publish = self.logical._publish_body_locators
        statements = []

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def execute(self, statement, *args, **kwargs):
                statements.append(str(statement))
                if "canonical_events" in str(statement):
                    raise AssertionError(
                        "prepared catalog commit re-enumerates parent events"
                    )
                return self.connection.execute(statement, *args, **kwargs)

        def observed(connection, candidate, locators):
            return publish(Connection(connection), candidate, locators)

        try:
            with patch.object(
                self.logical, "_publish_body_locators", side_effect=observed
            ):
                self.assertEqual(
                    self.logical._commit_upload(candidate, upload), "committed"
                )
            self.assertEqual(self.locations()[:12], before)
            self.assertTrue(any("UPDATE canonical_documents" in s for s in statements))
        finally:
            upload.body_locators.close()

    def write_special(self):
        event = self.event("turn-000")
        event["native_parent_id"] = "parent"
        event["content"]["type"] = "special-skip"
        event["provenance"]["artifact_ref"] = self.gateway.put_raw(
            tenant_id=self.tenant,
            source_id=self.source,
            native_id=event["native_id"],
            payload=canonical_json(event["content"]),
            media_type="application/json",
            created_at="2026-10-13T00:00:00Z",
        )
        event["content_sha256"] = hashlib.sha256(
            canonical_json(event["content"])
        ).hexdigest()
        self.write(event)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        return self.locations()[0]

    def test_excluded_existing_document_gets_explicit_null_reset(self):
        old = self.write_special()
        self.assertIsNotNone(old["body_record_ordinal"])
        self.logical.excluded_structural_types = ("special-skip",)
        self.append(["turn-999"])
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.assertEqual(len(list(upload.body_locators)), 2)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        final = self.locations()
        self.assertEqual(
            (final[0]["body_record_ordinal"], final[0]["body_record_count"]),
            (None, None),
        )
        self.assertIsNotNone(final[1]["body_record_ordinal"])

    def test_actual_null_repair_between_prepare_and_commit_is_overridden_by_reset(self):
        old = self.write_special()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND source_id=%s AND document_id=%s",
                (self.tenant, self.source, old["document_id"]),
            )
        self.logical.excluded_structural_types = ("special-skip",)
        self.append(["turn-999"])
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.assertEqual(len(list(upload.body_locators)), 2)
        repaired = publish_parent_locators(
            self.store,
            self.archive,
            tenant_id=self.tenant,
            source_id=self.source,
            native_parent_id="parent",
            owner_principal_id=self.principal,
            apply=True,
            deadline_at=time.monotonic() + 10,
        )
        self.assertEqual(repaired["published_documents"], 1)
        self.assertIsNotNone(self.locations()[0]["body_record_ordinal"])
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        self.assertIsNone(self.locations()[0]["body_record_ordinal"])

    def test_new_ingest_generation_keeps_prepared_delta_uncommitted(self):
        before, candidate, upload = self.prepare_append()
        self.append(["turn-998"])
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        after = self.locations()
        self.assertEqual(after[:12], before)
        self.assertTrue(all(row["body_record_ordinal"] is None for row in after[12:]))

    def test_changed_document_lock_refuses_without_partial_publication(self):
        before, candidate, upload = self.prepare_append()
        new = self.locations()[-1]
        with self.store.connect() as other, other.transaction():
            other.execute(
                "SELECT 1 FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND document_id=%s FOR UPDATE",
                (self.tenant, self.source, new["document_id"]),
            )
            self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertEqual(self.locations()[:12], before)
        self.assertIsNone(self.locations()[-1]["body_record_ordinal"])

    def test_changed_identity_without_queue_change_fails_closed(self):
        before, candidate, upload = self.prepare_append()
        new = self.locations()[-1]
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_documents SET text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND document_id=%s",
                ("f" * 64, self.tenant, self.source, new["document_id"]),
            )
        with self.assertRaisesRegex(
            LogicalEvidenceError, "logical_evidence_source_changed"
        ):
            self.logical._commit_upload(candidate, upload)
        self.assertIsNone(self.locations()[-1]["body_record_ordinal"])

    def test_unchanged_rebuild_does_not_create_locator_work(self):
        self.append(["turn-001", "turn-002"])
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        before = self.locations()
        self.logical.seed_backfill(
            tenant_id=self.tenant, source_id=self.source, include_existing=True
        )
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.assertEqual(list(upload.body_locators), [])
        self.assertEqual(self.logical._commit_upload(candidate, upload), "repaired")
        self.assertEqual(self.locations(), before)

    def test_old_upload_shape_keeps_full_reset_behavior(self):
        self.append(["turn-001", "turn-002"])
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        before = self.locations()
        self.logical.seed_backfill(
            tenant_id=self.tenant, source_id=self.source, include_existing=True
        )
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        spool = _LocatorSpool()
        try:
            spool.append((before[0]["document_id"], 0, 1))
            with self.store.connect() as c, c.transaction():
                self.assertEqual(
                    self.logical._publish_body_locators(c, candidate, spool), 1
                )
            self.assertIsNotNone(self.locations()[0]["body_record_ordinal"])
            self.assertIsNone(self.locations()[1]["body_record_ordinal"])
        finally:
            spool.close()

    def test_concurrent_preparations_preserve_first_committed_catalog(self):
        before, candidate, upload = self.prepare_append()
        second = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        current = self.locations()
        self.assertEqual(self.logical._commit_upload(candidate, second), "adopted")
        self.assertEqual(self.locations(), current)


if __name__ == "__main__":
    unittest.main()
