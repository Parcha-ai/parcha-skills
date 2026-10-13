#!/usr/bin/env python3
"""Real queue races during private recovery/encoding, before any upload."""

from pathlib import Path
from types import SimpleNamespace
import tempfile
import sqlite3
import unittest
from unittest.mock import patch

import psycopg

from e2e_logical_early_supersession import EarlySupersession
from e2e_logical_evidence_projection import insert_record, archive_object_count
import recall_server.logical_evidence_projection as module
from recall_server.logical_archive_bodies import ArchivedBodyLookup


class PeriodicSupersession(EarlySupersession):
    def setUp(self):
        super().setUp()
        with self.store.connect() as c:
            for n in range(1, 12):
                insert_record(
                    c,
                    tenant=self.tenant,
                    source=self.sources["large"],
                    parent="large",
                    native=f"large-{n}",
                    text=f"synthetic record {n} " + "bounded body " * 160,
                    role="user",
                    byte_start=200 + n,
                )
        self.clock = [0.0]

    def observe_temps(self):
        opened = []
        original = tempfile.TemporaryFile

        def create(*args, **kwargs):
            value = original(*args, **kwargs)
            opened.append(value)
            return value

        return opened, patch.object(module.tempfile, "TemporaryFile", create)

    def assert_retryable(self, result):
        self.assertEqual(
            (result["documents"], result["source_races"], result["failed"]), (1, 1, 0)
        )
        self.assertNotIn("large", self.uploaded)
        with self.store.connect() as c:
            row = c.execute(
                "SELECT generation,attempts,next_attempt_at FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.sources["large"]),
            ).fetchone()
        self.assertEqual(
            (row["generation"], row["attempts"], row["next_attempt_at"]), (2, 0, None)
        )

    def encoding(self, kind):
        stream = self.logical._record_stream
        encoded = []

        def tracked(rows, **kwargs):
            for record in stream(rows, **kwargs):
                if getattr(self.owners, "parent", "") == "large":
                    encoded.append(record.event_native_id)
                    if len(encoded) == 2:
                        self.mutate(kind)
                        self.clock[0] = (
                            module.PROJECTION_PROGRESS_INTERVAL_SECONDS + 0.1
                        )
                yield record

        opened, temp_patch = self.observe_temps()
        with (
            patch.object(
                module, "time", SimpleNamespace(monotonic=lambda: self.clock[0])
            ),
            temp_patch,
            patch.object(self.logical, "_record_stream", tracked),
        ):
            result = self.project()
        self.assertLessEqual(
            len(encoded), 2, "obsolete encoding consumed the remaining source rows"
        )
        self.assert_retryable(result)
        self.assertTrue(opened)
        self.assertTrue(
            all(value.closed for value in opened), "private spool handle leaked"
        )
        self.assertEqual(
            archive_object_count(self.archive_root),
            2,
            "obsolete parent created archive objects",
        )

    def test_append_during_encoding_stops_before_remaining_rows(self):
        self.encoding("append")

    def test_forget_during_encoding_stops_before_remaining_rows(self):
        self.encoding("forget")

    def recovery(self, kind):
        # Seed a real immutable multi-part large parent; small remains pending.
        original_put = self.projection.put_records
        with patch.object(
            self.projection,
            "put_records",
            lambda **kwargs: original_put(**dict(kwargs, part_bytes=4096)),
        ):
            candidate = next(
                c
                for c in self.logical._pending(limit=2, tenant_id=self.tenant)
                if c.native_parent_id == "large"
            )
            (upload,) = self.logical._prepare_batch_and_upload((candidate,))
            self.assertEqual(
                self.logical._commit_upload(candidate, upload), "committed"
            )
        with self.store.connect() as c:
            parts = c.execute(
                "SELECT count(*) AS n FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.sources["large"]),
            ).fetchone()["n"]
            self.assertGreater(parts, 2)
            c.execute(
                "UPDATE canonical_documents SET text_redacted='' WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.sources["large"]),
            )
            c.execute(
                "UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.sources["large"]),
            )
            module.mark_logical_evidence_dirty(
                c,
                tenant_id=self.tenant,
                source_id=self.sources["large"],
                native_ids=["large"],
                reason="ingest",
            )
        initial_objects = archive_object_count(self.archive_root)
        self.uploaded.clear()
        reads, lookups = [], []
        read_part = self.projection.read_part

        def tracked_read(reference, **kwargs):
            payload = read_part(reference, **kwargs)
            if kwargs["source_id"] == self.sources["large"]:
                reads.append(reference["artifact_id"])
                if len(reads) == 1:
                    self.mutate(kind)
                    self.clock[0] = module.PROJECTION_PROGRESS_INTERVAL_SECONDS + 0.1
            return payload

        outer = self

        class Lookup(ArchivedBodyLookup):
            def __init__(inner):
                super().__init__()
                inner.closed_checked = False
                lookups.append(inner)

            def close(inner):
                super().close()
                # Verify SQLite closure on its creating worker thread.
                with outer.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
                    inner.index.execute("SELECT 1")
                inner.closed_checked = True

        opened, temp_patch = self.observe_temps()
        with (
            patch.object(
                module, "time", SimpleNamespace(monotonic=lambda: self.clock[0])
            ),
            temp_patch,
            patch.object(module, "ArchivedBodyLookup", Lookup),
            patch.object(self.projection, "read_part", tracked_read),
        ):
            result = self.project()
        self.assertEqual(
            len(reads), 1, "obsolete recovery fetched remaining immutable parts"
        )
        self.assert_retryable(result)
        self.assertTrue(all(value.closed for value in opened))
        self.assertEqual(len(lookups), 1)
        self.assertTrue(lookups[0].bodies.closed)
        self.assertFalse(Path(lookups[0].directory.name).exists())
        self.assertTrue(lookups[0].closed_checked)
        self.assertEqual(archive_object_count(self.archive_root), initial_objects + 2)

    def test_append_during_recovery_stops_before_remaining_parts(self):
        self.recovery("append")

    def test_forget_during_recovery_stops_before_remaining_parts(self):
        self.recovery("forget")

    def test_unchanged_many_records_do_not_query_per_record(self):
        checks = []
        original = self.logical._check_candidate_current

        def check(candidate):
            checks.append(candidate.native_parent_id)
            original(candidate)

        with (
            patch.object(module, "time", SimpleNamespace(monotonic=lambda: 0.0)),
            patch.object(self.logical, "_check_candidate_current", check),
        ):
            result = self.project()
        self.assertEqual((result["documents"], result["failed"]), (2, 0))
        self.assertEqual(checks.count("large"), 2)
        self.assertEqual(checks.count("small"), 2)

    def test_unchanged_work_rearms_one_check_after_elapsed_interval(self):
        checks = []
        original = self.logical._check_candidate_current
        stream = self.logical._record_stream

        def check(candidate):
            checks.append(candidate.native_parent_id)
            original(candidate)

        def advance(rows, **kwargs):
            for record in stream(rows, **kwargs):
                if getattr(self.owners, "parent", "") == "large":
                    self.clock[0] = module.PROJECTION_PROGRESS_INTERVAL_SECONDS + 0.1
                yield record

        with (
            patch.object(
                module, "time", SimpleNamespace(monotonic=lambda: self.clock[0])
            ),
            patch.object(self.logical, "_check_candidate_current", check),
            patch.object(self.logical, "_record_stream", advance),
        ):
            result = self.project()
        self.assertEqual((result["documents"], result["failed"]), (2, 0))
        self.assertEqual(
            checks.count("large"),
            3,
            "interval check must rearm instead of querying each record",
        )

    def test_direct_preparation_keeps_no_queue_check_contract(self):
        candidates = tuple(self.logical._pending(limit=2, tenant_id=self.tenant))
        with patch.object(
            self.logical,
            "_check_candidate_current",
            side_effect=AssertionError(
                "direct preparation must not inspect worker queue"
            ),
        ):
            uploads = self.logical._prepare_batch_and_upload(candidates)
        self.assertTrue(all(upload is not None for upload in uploads))
        self.logical._schedule_upload_cleanup(uploads)

    def test_periodic_database_error_remains_failure(self):
        original = self.logical._check_candidate_current
        checks = []

        def check(candidate):
            checks.append(candidate.native_parent_id)
            if candidate.native_parent_id == "large" and self.clock[0] > 0:
                raise psycopg.OperationalError("synthetic queue check unavailable")
            original(candidate)

        stream = self.logical._record_stream

        def advance(rows, **kwargs):
            for record in stream(rows, **kwargs):
                if getattr(self.owners, "parent", "") == "large":
                    self.clock[0] = module.PROJECTION_PROGRESS_INTERVAL_SECONDS + 0.1
                yield record

        with (
            patch.object(
                module, "time", SimpleNamespace(monotonic=lambda: self.clock[0])
            ),
            patch.object(self.logical, "_check_candidate_current", check),
            patch.object(self.logical, "_record_stream", advance),
        ):
            result = self.project()
        self.assertEqual(
            (result["documents"], result["source_races"], result["failed"]), (1, 0, 1)
        )
        self.assertNotIn("large", self.uploaded)
        with self.store.connect() as c:
            row = c.execute(
                "SELECT generation,attempts,next_attempt_at,last_error_code FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.sources["large"]),
            ).fetchone()
        self.assertEqual(
            (row["generation"], row["attempts"], row["last_error_code"]),
            (1, 1, "OperationalError"),
        )
        self.assertIsNotNone(row["next_attempt_at"])


if __name__ == "__main__":
    names = [name for name in PeriodicSupersession.__dict__ if name.startswith("test_")]
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(PeriodicSupersession(name) for name in names)
    )
    raise SystemExit(not result.wasSuccessful())
