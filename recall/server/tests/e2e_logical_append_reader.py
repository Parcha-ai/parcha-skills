#!/usr/bin/env python3
"""The append reader must match full publication while avoiding old SQL bodies."""

from pathlib import Path
from contextlib import contextmanager
import os
import sys
import unittest
from unittest.mock import patch

import psycopg

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
]
import e2e_logical_append_capability as fixtures  # noqa: E402


class AppendReader(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    catalog = fixtures.AppendCapability.catalog
    queue = fixtures.AppendCapability.queue
    safe = fixtures.AppendCapability.safe

    def test_full_and_append_outputs_match_without_old_body_hydration(self):
        put = self.logical.projection.put_records

        def small_parts(**kwargs):
            return put(part_bytes=2300, **kwargs)

        with patch.object(self.logical.projection, "put_records", small_parts):
            for index in range(7):
                self.ingest(f"old-{index}", index + 1, "old body " + "x" * 700)
            self.ingest("old-excluded", 8, record_type="token_count")
            self.assertEqual(self.publish()["documents"], 1)
            self.ingest("new-1", 10, "new body " + "y" * 800)
            self.ingest("new-2", 10, "new body " + "z" * 600)
            self.assertTrue(self.safe())
            candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
            statements = []
            original_execute = psycopg.ServerCursor.execute

            def capture(cursor, query, params=None, **kwargs):
                if cursor.name == "logical_evidence_batch_stream":
                    statements.append((query, params))
                return original_execute(cursor, query, params, **kwargs)

            with patch.object(psycopg.ServerCursor, "execute", capture):
                full = self.logical._prepare_batch_and_upload((candidate,))[0]
            observed = []
            records = self.logical._record_stream

            def observe(rows, **kwargs):
                def inputs():
                    for row in rows:
                        observed.append(row["native_id"])
                        yield row

                return records(inputs(), **kwargs)

            self.logical.append_enabled = True
            with (
                patch.object(self.logical, "_record_stream", observe),
                patch.object(psycopg.ServerCursor, "execute", capture),
            ):
                append = self.logical._prepare_batch_and_upload((candidate,))[0]
            try:
                self.assertEqual(
                    observed,
                    ["new-1", "new-2"],
                    "append hydrated old PostgreSQL bodies",
                )

                def chunk_loops(node):
                    return (
                        node.get("Actual Loops", 0)
                        if node.get("Relation Name") == "canonical_chunks"
                        else 0
                    ) + sum(chunk_loops(child) for child in node.get("Plans", []))

                loops = []
                with self.store.connect() as connection:
                    for query, values in statements:
                        plan = connection.execute(
                            "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, values
                        ).fetchone()["QUERY PLAN"][0]
                        loops.append(chunk_loops(plan["Plan"]))
                self.assertEqual(loops, [10, 2])
                print(
                    {
                        "full_chunk_lookup_loops": loops[0],
                        "append_chunk_lookup_loops": loops[1],
                        "old_sql_bodies_returned": 0,
                    }
                )
                self.assertEqual(append.prepared, full.prepared)
                self.assertEqual(append.manifest.encode(), full.manifest.encode())
                self.assertEqual(append.part_references, full.part_references)
                self.assertEqual(append.manifest_reference, full.manifest_reference)
                self.assertEqual(list(append.body_locators), list(full.body_locators))
                self.assertEqual(
                    self.logical._commit_upload(candidate, append), "committed"
                )
                self.assertEqual(self.catalog()["append_source_ordinal"], 10)
            finally:
                full.body_locators.close()
                append.body_locators.close()

    def prepare(self, *, structural=False):
        for index in range(3):
            self.ingest(f"old-{index}", index + 1, "old exact body")
        if structural:
            self.ingest("excluded", 4, record_type="token_count")
        self.assertEqual(self.publish()["documents"], 1)
        self.ingest("new", 10, "new exact body")
        self.logical.append_enabled = True
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        return candidate, self.logical._prepare_batch_and_upload((candidate,))[0]

    def test_multisegment_suffix_keeps_exact_ordinals_and_receipts(self):
        with patch("recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES", 128):
            self.ingest("old", 1, "old multilingual α body " * 50)
            self.assertEqual(self.publish()["documents"], 1)
            self.ingest("new", 2, "new multilingual β body " * 60)
            candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
            full = self.logical._prepare_batch_and_upload((candidate,))[0]
            self.logical.append_enabled = True
            appended = self.logical._prepare_batch_and_upload((candidate,))[0]
            try:
                self.assertGreater(full.prepared.record_count, 2)
                self.assertEqual(full.manifest.encode(), appended.manifest.encode())
                self.assertEqual(full.part_references, appended.part_references)
                self.assertEqual(list(full.body_locators), list(appended.body_locators))
                self.assertEqual(
                    self.logical._commit_upload(candidate, appended), "committed"
                )
            finally:
                full.body_locators.close()
                appended.body_locators.close()

    def test_removed_optional_schema_marker_forces_full_even_with_stored_binding(self):
        candidate, upload = self.prepare()
        upload.body_locators.close()
        with self.store.connect() as connection:
            connection.execute("DELETE FROM schema_migrations WHERE version=79")
        seen = []
        original = self.logical._record_stream

        def records(rows, **kwargs):
            def tracked():
                for row in rows:
                    seen.append(row["native_id"])
                    yield row

            return original(tracked(), **kwargs)

        with patch.object(self.logical, "_record_stream", records):
            upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        upload.body_locators.close()
        self.assertEqual(seen, ["old-0", "old-1", "old-2", "new"])

    def test_structural_only_append_advances_frontier_without_changing_content(self):
        self.ingest("visible", 1)
        self.assertEqual(self.publish()["documents"], 1)
        before = self.catalog()
        self.logical.append_enabled = True
        self.ingest("structural", 2, record_type="token_count")
        self.assertTrue(self.safe())
        self.publish()
        after = self.catalog()
        self.assertEqual(after["manifest_artifact_id"], before["manifest_artifact_id"])
        self.assertEqual(after["append_source_ordinal"], 2)
        self.assertIsNone(self.queue())
        self.ingest("new", 3)
        self.assertTrue(self.safe())
        self.assertEqual(self.publish()["records"], 2)

    def test_custom_retention_prefix_cannot_opt_into_default_append(self):
        from recall_server.logical_evidence_projection import (
            DEFAULT_EXCLUDED_STRUCTURAL_TYPES,
        )

        self.logical.excluded_structural_types = ("custom_hidden",)
        self.ingest("visible", 1)
        self.ingest("custom", 2, record_type="custom_hidden")
        self.assertEqual(self.publish()["documents"], 1)
        self.assertIsNone(self.catalog()["append_manifest_id"])
        self.logical.excluded_structural_types = DEFAULT_EXCLUDED_STRUCTURAL_TYPES
        self.logical.append_enabled = True
        self.ingest("new", 3)
        self.assertFalse(self.safe())
        self.assertEqual(self.publish()["records"], 3)

    def test_prefix_body_reads_hold_no_database_lease(self):
        candidate, upload = self.prepare()
        upload.body_locators.close()
        original_connect = self.store.connect
        active = 0
        body_reads = []

        @contextmanager
        def connect():
            nonlocal active
            with original_connect() as connection:
                active += 1
                try:
                    yield connection
                finally:
                    active -= 1

        read_part = self.logical.projection.read_part

        def read(*args, **kwargs):
            body_reads.append(active)
            self.assertEqual(active, 0)
            return read_part(*args, **kwargs)

        with (
            patch.object(self.store, "connect", connect),
            patch.object(self.logical.projection, "read_part", read),
        ):
            upload = self.logical._prepare_batch_and_upload(
                (candidate,), require_current_queue=True
            )[0]
        upload.body_locators.close()
        self.assertTrue(body_reads)

    def test_missing_prefix_fails_without_republishing_from_sql(self):
        from recall_server.logical_evidence import LogicalEvidenceError

        candidate, prepared = self.prepare()
        prepared.body_locators.close()
        original = self.catalog()["manifest_artifact_id"]
        with patch.object(
            self.logical.projection,
            "read_part",
            side_effect=LogicalEvidenceError("logical_evidence_not_found"),
        ):
            with self.assertRaisesRegex(LogicalEvidenceError, "not_found"):
                self.logical._prepare_batch_and_upload((candidate,))
        self.assertEqual(self.catalog()["manifest_artifact_id"], original)
        self.assertIsNotNone(self.queue())

    def test_catalog_replacement_without_queue_change_refuses_prefix_commit(self):
        candidate, upload = self.prepare()
        try:
            with self.store.connect() as c:
                c.execute(
                    "UPDATE canonical_evidence_documents SET manifest_artifact_id=%s WHERE tenant_id=%s",
                    ("art_" + "a" * 32, self.tenant),
                )
            self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        finally:
            upload.body_locators.close()

    def test_append_during_archive_read_preserves_stale_generation_fence(self):
        candidate, upload = self.prepare()
        upload.body_locators.close()
        read = self.logical.projection.read_part
        injected = []

        def append_during_read(*args, **kwargs):
            payload = read(*args, **kwargs)
            if not injected:
                injected.append(True)
                self.ingest("newer", 11, "newer exact body")
            return payload

        with patch.object(self.logical.projection, "read_part", append_during_read):
            upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        try:
            self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
            self.assertEqual(self.catalog()["append_source_ordinal"], 3)
        finally:
            upload.body_locators.close()

    def test_excluded_prefix_null_guard_retains_original_reset_behavior(self):
        candidate, upload = self.prepare(structural=True)
        try:
            with self.store.connect() as c:
                c.execute(
                    "UPDATE canonical_documents SET body_record_ordinal=0,body_record_count=1 WHERE tenant_id=%s AND native_id='excluded'",
                    (self.tenant,),
                )
            self.assertEqual(
                self.logical._commit_upload(candidate, upload), "committed"
            )
            with self.store.connect() as c:
                row = c.execute(
                    "SELECT body_record_ordinal,body_record_count FROM canonical_documents WHERE tenant_id=%s AND native_id='excluded'",
                    (self.tenant,),
                ).fetchone()
            self.assertEqual(
                row, dict(body_record_ordinal=None, body_record_count=None)
            )
        finally:
            upload.body_locators.close()

    def test_lost_commit_ack_preserves_unknown_and_current_archive_objects(self):
        candidate, upload = self.prepare()
        commit = self.logical._commit

        def lost_ack(*args, **kwargs):
            commit(*args, **kwargs)
            raise psycopg.OperationalError("synthetic lost commit acknowledgment")

        with patch.object(self.logical, "_commit", lost_ack):
            with self.assertRaises(psycopg.OperationalError):
                self.logical._commit_upload(candidate, upload)
        self.assertIsNone(self.queue())
        self.assertEqual(
            self.catalog()["manifest_artifact_id"],
            upload.manifest_reference["artifact_id"],
        )
        self.logical.drain_cleanup(tenant_id=self.tenant)
        for reference in upload.all_references:
            self.archive.verify_raw(reference)

    def test_forget_after_preparation_cannot_republish_prefix(self):
        candidate, upload = self.prepare()
        self.plane.forget(
            dict(
                contract="recall.forget-request.v1",
                schema_version=1,
                tenant_id=self.tenant,
                principal_id=self.principal,
                source_id=self.source,
                target_receipt=f"recall://{self.source}/old-0?rev=1#item=0",
                mode="explicit_forget",
                reason="owner_requested",
                requested_at="2026-10-14T10:00:00Z",
                idempotency_key="forget-append-prefix",
            )
        )
        self.assertEqual(self.logical._commit_upload(candidate, upload), "stale")
        self.assertFalse(self.safe())

    def test_retired_prefix_stays_cold_and_matches_full_recovered_output(self):
        from recall_server.chunk_retirement import (
            retire_parent_chunks,
            set_parent_retirement_enabled,
        )

        for index in range(3):
            self.ingest(f"old-{index}", index + 1, "exact cold prefix body")
        self.assertEqual(self.publish()["documents"], 1)
        scope = dict(
            tenant_id=self.tenant, source_id=self.source, native_parent_id=self.parent
        )
        preview = retire_parent_chunks(self.store, self.archive, **scope)
        set_parent_retirement_enabled(self.store, **scope, enabled=True)
        with patch.dict(os.environ, {"RECALL_CHUNK_BODY_READS": "archive"}):
            retired = retire_parent_chunks(
                self.store,
                self.archive,
                **scope,
                apply=True,
                reviewed_plan=preview["plan"],
            )
        self.assertEqual(retired["cleared_documents"], 3)
        self.ingest("new", 10, "new body")
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        full = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.logical.append_enabled = True
        appended = self.logical._prepare_batch_and_upload((candidate,))[0]
        try:
            self.assertEqual(full.manifest.encode(), appended.manifest.encode())
            self.assertEqual(full.part_references, appended.part_references)
            self.assertEqual(
                self.logical._commit_upload(candidate, appended), "committed"
            )
            with self.store.connect() as c:
                still_cold = c.execute(
                    "SELECT count(*) AS n FROM canonical_chunks c JOIN canonical_documents d ON d.tenant_id=c.tenant_id AND d.source_id=c.source_id AND d.document_id=c.document_id WHERE c.tenant_id=%s AND d.native_id LIKE 'old-%%' AND c.text_redacted=''",
                    (self.tenant,),
                ).fetchone()["n"]
            self.assertEqual(still_cold, 3)
        finally:
            full.body_locators.close()
            appended.body_locators.close()


@unittest.skipUnless(
    os.environ.get("RECALL_TEST_APPEND_LARGE") == "1", "opt-in 10k-parent measurement"
)
class LargeAppendReader(unittest.TestCase):
    setUp = AppendReader.setUp
    event = AppendReader.event
    publish = AppendReader.publish

    def test_ten_thousand_prefix_rows_stay_out_of_append_sql(self):
        import hashlib
        import resource
        import time

        seeded = time.monotonic()
        for start in range(0, 10_000, 1000):
            events = []
            for index in range(start, start + 1000):
                text = "".join(
                    hashlib.sha256(f"{index}:{part}".encode()).hexdigest()
                    for part in range(32)
                )
                events.append(self.event(f"old-{index:05d}", index + 1, text))
            self.plane.ingest_batch(
                tenant_id=self.tenant, principal_id=self.principal, events=events
            )
        self.assertEqual(self.publish()["records"], 10_000)
        self.plane.ingest_batch(
            tenant_id=self.tenant,
            principal_id=self.principal,
            events=[
                self.event(f"new-{index}", 10_001 + index, "New exact body " * 100)
                for index in range(10)
            ],
        )
        seed_seconds = time.monotonic() - seeded
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        statements, actual = [], []
        original_execute = psycopg.ServerCursor.execute
        original_records = self.logical._record_stream

        def execute(cursor, query, params=None, **kwargs):
            if cursor.name == "logical_evidence_batch_stream":
                statements.append((query, params))
            return original_execute(cursor, query, params, **kwargs)

        def records(rows, **kwargs):
            def inputs():
                for row in rows:
                    actual[-1]["rows"] += 1
                    actual[-1]["bytes"] += len(row["event_text"].encode())
                    actual[-1]["old_rows"] += int(row["native_id"].startswith("old-"))
                    yield row

            return original_records(inputs(), **kwargs)

        uploads, times = [], []
        try:
            for enabled in (False, True):
                self.logical.append_enabled = enabled
                actual.append(dict(rows=0, bytes=0, old_rows=0))
                started = time.monotonic()
                with (
                    patch.object(psycopg.ServerCursor, "execute", execute),
                    patch.object(self.logical, "_record_stream", records),
                ):
                    uploads.append(
                        self.logical._prepare_batch_and_upload((candidate,))[0]
                    )
                times.append(time.monotonic() - started)
            full, appended = uploads
            self.assertEqual([row["rows"] for row in actual], [10_010, 10])
            self.assertEqual([row["old_rows"] for row in actual], [10_000, 0])
            self.assertEqual(full.prepared, appended.prepared)
            self.assertEqual(full.manifest.encode(), appended.manifest.encode())
            self.assertEqual(full.part_references, appended.part_references)
            self.assertEqual(full.manifest_reference, appended.manifest_reference)
            self.assertEqual(list(full.body_locators), list(appended.body_locators))

            def loops(node):
                return (
                    node.get("Actual Loops", 0)
                    if node.get("Relation Name") == "canonical_chunks"
                    else 0
                ) + sum(loops(child) for child in node.get("Plans", []))

            actual_loops = []
            with self.store.connect() as connection:
                for query, params in statements:
                    plan = connection.execute(
                        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, params
                    ).fetchone()["QUERY PLAN"][0]
                    actual_loops.append(loops(plan["Plan"]))
            self.assertEqual(actual_loops, [10_010, 10])
            self.assertEqual(
                self.logical._commit_upload(candidate, appended), "committed"
            )
            print(
                dict(
                    seed_seconds=round(seed_seconds, 3),
                    full_seconds=round(times[0], 3),
                    append_seconds=round(times[1], 3),
                    full_sql=actual[0],
                    append_sql=actual[1],
                    chunk_lookup_loops=actual_loops,
                    archive_document_bytes=sum(
                        ref["size_bytes"] for ref in full.part_references
                    ),
                    process_peak_rss_kib=resource.getrusage(
                        resource.RUSAGE_SELF
                    ).ru_maxrss,
                )
            )
        finally:
            for upload in uploads:
                upload.body_locators.close()


class AppendReaderCapabilityMatrix(fixtures.AppendCapability):
    def setUp(self):
        super().setUp()
        self.logical.append_enabled = True


if __name__ == "__main__":
    unittest.main()
