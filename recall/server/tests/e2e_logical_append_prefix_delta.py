#!/usr/bin/env python3
"""Append only mutates suffix locators and preserves archive-proven prefix repairs."""

import time
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
]
import e2e_logical_append_capability as fixtures  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402


class LocatorCost(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    catalog = fixtures.AppendCapability.catalog

    def test_append_delta_contains_only_suffix_with_sparse_excluded_prefix(self):
        events = [
            self.event(
                f"prefix-{i:05}", i + 1, record_type="token_count" if i < 200 else None
            )
            for i in range(1000)
        ]
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=events
        )
        self.assertEqual(self.publish()["records"], 800)
        with self.store.connect() as connection:
            connection.execute("ANALYZE canonical_documents")
            connection.execute("ANALYZE canonical_events")
        self.logical.append_enabled = True
        original_delta = CanonicalLogicalEvidenceProjector._publish_locator_delta
        observations = []

        def delta(connection, candidate, locators, **kwargs):
            rows = list(locators)
            report = dict(
                count=locators.count,
                retained_null=sum(r[2].startswith("prefix-") for r in rows),
                suffix=sum(r[2].startswith("suffix-") for r in rows),
                desired_null=sum(r[-2:] == (None, None) for r in rows),
            )
            started = time.monotonic()
            report["changed"] = original_delta(
                connection, candidate, locators, **kwargs
            )
            report["delta_wall_ms"] = (time.monotonic() - started) * 1000
            observations.append(report)
            return report["changed"]

        for round_no, count in enumerate((3, 1)):
            self.plane.ingest_batch(
                tenant_id=self.tenant,
                principal_id=self.principal,
                events=[
                    self.event(f"suffix-{round_no}-{i}", 1001 + round_no * 10 + i)
                    for i in range(count)
                ],
            )
            candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
            upload = self.logical._prepare_batch_and_upload((candidate,))[0]
            try:
                self.assertIsNotNone(upload.append_base_manifest_id)
                self.assertEqual(upload.input_sql_rows, count)
                with patch.object(
                    CanonicalLogicalEvidenceProjector,
                    "_publish_locator_delta",
                    staticmethod(delta),
                ):
                    self.assertEqual(
                        self.logical._commit_upload(candidate, upload), "committed"
                    )
            finally:
                upload.body_locators.close()
        print(dict(observations=observations), flush=True)
        self.assertEqual(
            [
                (x["count"], x["retained_null"], x["suffix"], x["changed"])
                for x in observations
            ],
            [(3, 0, 3, 3), (1, 0, 1, 1)],
        )


class RepairInterleaving(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    catalog = fixtures.AppendCapability.catalog

    def prepare_missing_locator(self):
        self.ingest("old", 1)
        self.ingest("excluded", 2, record_type="token_count")
        self.assertEqual(self.publish()["documents"], 1)
        # Existing legacy NULL state is accepted by both published repair owners.
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND native_id='old'",
                (self.tenant,),
            )
        self.ingest("new", 3)
        self.logical.append_enabled = True
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.addCleanup(upload.body_locators.close)
        return candidate, upload

    def positions(self):
        with self.store.connect() as connection:
            return connection.execute(
                "SELECT native_id,body_record_ordinal,body_record_count FROM canonical_documents WHERE tenant_id=%s ORDER BY native_id",
                (self.tenant,),
            ).fetchall()

    def scope(self):
        return dict(
            tenant_id=self.tenant, source_id=self.source, native_parent_id=self.parent
        )

    def assert_repaired(self):
        self.assertEqual(
            self.positions(),
            [
                dict(
                    native_id="excluded",
                    body_record_ordinal=None,
                    body_record_count=None,
                ),
                dict(native_id="new", body_record_ordinal=1, body_record_count=1),
                dict(native_id="old", body_record_ordinal=0, body_record_count=1),
            ],
        )

    def test_backfill_proven_prefix_repair_survives_append(self):
        from recall_server.locator_backfill_plan import apply_parent

        candidate, upload = self.prepare_missing_locator()
        repaired = apply_parent(self.store, self.archive, **self.scope())
        self.assertEqual(repaired["applied_documents"], 1)
        self.assertEqual(repaired["excluded"]["structural"], 1)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        self.assert_repaired()

    def test_streaming_proven_prefix_repair_survives_append(self):
        from recall_server.streaming_locators import publish_parent_locators

        candidate, upload = self.prepare_missing_locator()
        repaired = publish_parent_locators(
            self.store,
            self.archive,
            **self.scope(),
            owner_principal_id=self.principal,
            apply=True,
        )
        self.assertEqual(repaired["published_documents"], 1)
        self.assertEqual(repaired["excluded"]["structural"], 1)
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")
        self.assert_repaired()

    def test_streaming_proof_of_old_catalog_refuses_after_append_commits(self):
        from recall_server import streaming_locators

        candidate, upload = self.prepare_missing_locator()
        original = streaming_locators._publish_batch

        def append_first(*args, **kwargs):
            self.assertEqual(
                self.logical._commit_upload(candidate, upload), "committed"
            )
            return original(*args, **kwargs)

        with patch.object(streaming_locators, "_publish_batch", append_first):
            with self.assertRaisesRegex(
                streaming_locators.LocatorPublicationError,
                "locator_publication_parent_changed",
            ):
                streaming_locators.publish_parent_locators(
                    self.store,
                    self.archive,
                    **self.scope(),
                    owner_principal_id=self.principal,
                    apply=True,
                )
        self.assertIsNone(self.positions()[2]["body_record_ordinal"])
        # A fresh proof can fill the unchanged exact prefix against the new catalog.
        streaming_locators.publish_parent_locators(
            self.store,
            self.archive,
            **self.scope(),
            owner_principal_id=self.principal,
            apply=True,
        )
        self.assert_repaired()

    def test_catalog_lock_excludes_concurrent_streaming_locator_mutation(self):
        from recall_server import streaming_locators

        candidate, upload = self.prepare_missing_locator()
        with self.store.connect() as connection, connection.transaction():
            connection.execute(
                "SELECT 1 FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s FOR UPDATE",
                (self.tenant, self.source),
            )
            with self.assertRaises(streaming_locators.LocatorPublicationError):
                streaming_locators.publish_parent_locators(
                    self.store,
                    self.archive,
                    **self.scope(),
                    owner_principal_id=self.principal,
                    apply=True,
                )
        self.assertIsNone(self.positions()[2]["body_record_ordinal"])
        self.assertEqual(self.logical._commit_upload(candidate, upload), "committed")


if __name__ == "__main__":
    unittest.main()
