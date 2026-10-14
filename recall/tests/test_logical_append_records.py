"""Committed prefix reuse must preserve the existing byte-level publication contract."""

from dataclasses import replace
import unittest
import time
import resource
from unittest.mock import patch

from test_logical_part_uploads import _ConcurrentArchive, _records
from server.recall_server.logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
)


class LogicalAppendRecordsTests(unittest.TestCase):
    def setUp(self):
        self.archive = _ConcurrentArchive()
        self.projection = LogicalEvidenceProjectionStore(self.archive)
        self.scope = dict(
            tenant_id="tenant:append",
            source_id="source:parallel",
            native_parent_id="parent",
            part_bytes=2300,
        )

    def publish(self, records, revision, **kwargs):
        return self.projection.put_records(
            **self.scope, revision=revision, records=records, **kwargs
        )

    def test_multipart_prefix_and_tail_match_full_without_reencoding_old_records(self):
        records = tuple(
            replace(
                record,
                occurred_at=(
                    "2026-07-28T00:00:00Z"
                    if record.ordinal % 2
                    else "2026-07-27T00:00:00Z"
                ),
            )
            for record in _records(9)
        )
        prefix = self.publish(records[:5], 1)
        self.assertGreater(len(prefix.part_references), 1)
        full = self.publish(records, 2)
        encoded = []
        original = LogicalEvidenceRecord.encode

        def suffix_only(record, **kwargs):
            self.assertGreaterEqual(
                record.ordinal, 5, "prefix records were decoded and reencoded"
            )
            encoded.append(record.ordinal)
            return original(record, **kwargs)

        with patch.object(LogicalEvidenceRecord, "encode", suffix_only):
            appended = self.publish(records[5:], 2, prefix=prefix)
        self.assertEqual(encoded, [5, 6, 7, 8])
        self.assertEqual(appended.prepared, full.prepared)
        self.assertEqual(appended.manifest.encode(), full.manifest.encode())
        self.assertEqual(appended.part_references, full.part_references)
        self.assertEqual(appended.manifest_reference, full.manifest_reference)
        borrowed = {r["artifact_id"] for r in prefix.part_references[:-1]}
        self.assertTrue(
            borrowed.isdisjoint({r["artifact_id"] for r in appended.cleanup_references})
        )

    def test_empty_visible_suffix_preserves_identical_document_and_receipts(self):
        records = _records(5)
        prefix = self.publish(records, 1)
        full = self.publish(records, 2)
        appended = self.publish((), 2, prefix=prefix)
        self.assertEqual(appended.manifest.encode(), full.manifest.encode())
        self.assertEqual(appended.prepared, full.prepared)
        self.assertEqual(appended.part_references, full.part_references)

    def test_corrupt_prefix_fails_before_any_new_archive_upload(self):
        records = _records(7)
        prefix = self.publish(records[:5], 1)
        reference = prefix.part_references[0]
        self.archive.objects[reference["artifact_id"]] = b"corrupt\n"
        before = self.archive.put_calls
        with self.assertRaisesRegex(LogicalEvidenceError, "corrupt"):
            self.publish(records[5:], 2, prefix=prefix)
        self.assertEqual(self.archive.put_calls, before)

    def test_prefix_checkpoint_stops_before_any_upload(self):
        records = _records(8)
        prefix = self.publish(records[:5], 1)
        before = self.archive.put_calls
        checks = []

        def stopped():
            checks.append(True)
            if len(checks) == 2:
                raise TimeoutError("synthetic owner cancellation")

        with self.assertRaises(TimeoutError):
            self.publish(records[5:], 2, prefix=prefix, prefix_checkpoint=stopped)
        self.assertEqual(len(checks), 2)
        self.assertEqual(self.archive.put_calls, before)

    def test_changed_part_budget_refuses_incompatible_prefix_boundaries(self):
        records = _records(8)
        prefix = self.publish(records[:5], 1)
        for new_budget in (1024, 10000):
            with self.subTest(part_bytes=new_budget):
                with self.assertRaisesRegex(LogicalEvidenceError, "prefix_invalid"):
                    self.projection.put_records(
                        **dict(self.scope, part_bytes=new_budget),
                        revision=2,
                        records=records[5:],
                        prefix=prefix,
                    )

    def test_wrong_prefix_scope_and_metadata_fail_closed(self):
        records = _records(6)
        prefix = self.publish(records[:5], 1)
        variants = [
            replace(
                prefix,
                manifest=replace(
                    prefix.manifest, logical_document_id="ldoc_" + "a" * 32
                ),
            ),
            replace(
                prefix, manifest=replace(prefix.manifest, native_parent_sha256="a" * 64)
            ),
            replace(
                prefix, prepared=replace(prefix.prepared, source_id="source:other")
            ),
            replace(
                prefix,
                prepared=replace(
                    prefix.prepared, first_occurred_at="2020-01-01T00:00:00Z"
                ),
            ),
            replace(prefix, prepared=replace(prefix.prepared, receipt_count=123)),
        ]
        before = self.archive.put_calls
        for invalid in variants:
            with self.assertRaisesRegex(LogicalEvidenceError, "prefix_invalid"):
                self.publish(records[5:], 2, prefix=invalid)
        self.assertEqual(self.archive.put_calls, before)


class LogicalAppendLargePrefixTests(unittest.TestCase):
    def test_one_hundred_thousand_record_prefix_matches_full_bytes(self):
        archive = _ConcurrentArchive()
        projection = LogicalEvidenceProjectionStore(archive)
        records = _records(100_010)
        scope = dict(
            tenant_id="tenant:append",
            source_id="source:parallel",
            native_parent_id="large-parent",
        )
        prefix = projection.put_records(
            **scope, revision=1, records=iter(records[:100_000])
        )
        start = time.monotonic()
        full = projection.put_records(**scope, revision=2, records=iter(records))
        full_seconds = time.monotonic() - start
        start = time.monotonic()
        original = LogicalEvidenceRecord.encode

        def suffix_only(record, **kwargs):
            self.assertGreaterEqual(record.ordinal, 100_000)
            return original(record, **kwargs)

        with patch.object(LogicalEvidenceRecord, "encode", suffix_only):
            appended = projection.put_records(
                **scope, revision=2, records=iter(records[100_000:]), prefix=prefix
            )
        append_seconds = time.monotonic() - start
        self.assertEqual(appended.prepared, full.prepared)
        self.assertEqual(appended.manifest.encode(), full.manifest.encode())
        self.assertEqual(appended.part_references, full.part_references)
        self.assertEqual(appended.manifest_reference, full.manifest_reference)
        print(
            dict(
                prefix_records=100_000,
                suffix_records=10,
                prefix_bytes=sum(ref["size_bytes"] for ref in prefix.part_references),
                prefix_parts=len(prefix.part_references),
                full_seconds=round(full_seconds, 3),
                append_seconds=round(append_seconds, 3),
                process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            )
        )
