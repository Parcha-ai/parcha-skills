from __future__ import annotations

import hashlib
import threading
import time
import unittest
from typing import Any

from server.recall_server.logical_evidence import (
    LogicalEvidenceError,
    DEFAULT_PART_BYTES,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
)
from server.recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
)


class _ConcurrentArchive:
    def __init__(self, *, fail_suffix: str | None = None) -> None:
        self.fail_suffix = fail_suffix
        self.active = 0
        self.maximum_active = 0
        self.put_calls = 0
        self.lock = threading.Lock()
        self.objects: dict[str, bytes] = {}

    def put_raw(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_id: str,
        payload: bytes,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]:
        with self.lock:
            self.active += 1
            self.put_calls += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            time.sleep(0.01)
            if self.fail_suffix and native_id.endswith(self.fail_suffix):
                raise RuntimeError("synthetic upload failure")
            digest = hashlib.sha256(
                native_id.encode() + b"\0" + payload
            ).hexdigest()
            reference = {
                "contract": "recall.artifact-ref.v1",
                "schema_version": 1,
                "tenant_id": tenant_id,
                "source_id": source_id,
                "artifact_id": "art_" + digest[:32],
                "storage_backend": "filesystem",
                "object_key": f"objects/{digest[:2]}/{digest}",
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "media_type": media_type,
                "encryption": "filesystem-owner-only",
                "version_id": "fs-" + digest[:32],
                "created_at": created_at,
            }
            with self.lock:
                self.objects[reference["artifact_id"]] = payload
            return reference
        finally:
            with self.lock:
                self.active -= 1

    def delete_raw(self, value: dict[str, Any]) -> bool:
        with self.lock:
            return self.objects.pop(value["artifact_id"], None) is not None

    def read_raw(self, value: dict[str, Any]) -> bytes:
        with self.lock:
            return self.objects[value["artifact_id"]]


def _records(count: int) -> tuple[LogicalEvidenceRecord, ...]:
    source = "source:parallel"
    return tuple(
        LogicalEvidenceRecord(
            ordinal=ordinal,
            event_native_id=f"event:parallel:{ordinal}",
            event_kind="transcript_record",
            occurred_at="2026-07-27T00:00:00Z",
            roles=("assistant",),
            receipts=(
                f"recall://{source}/event-parallel-{ordinal}"
                f"?rev=1#item=0",
            ),
            segment_ordinal=0,
            segment_count=1,
            text="x" * 700,
        )
        for ordinal in range(count)
    )


class LogicalPartUploadTests(unittest.TestCase):
    def test_backfill_rejects_an_invalid_exact_source_before_database_access(
        self,
    ) -> None:
        projector = CanonicalLogicalEvidenceProjector(None, None)

        with self.assertRaisesRegex(
            LogicalEvidenceError,
            "logical_evidence_rebuild_invalid",
        ):
            projector.seed_backfill(source_id="not a source")

    def test_projection_preserves_receipts_from_the_ingested_revision(self) -> None:
        projector = CanonicalLogicalEvidenceProjector(None, None)
        receipt = "recall://source:parallel/event-existing?rev=1#item=0"
        records = tuple(
            projector._record_stream(
                [
                    {
                        "event_text": "x" * 25_000,
                        "document_revision": 1,
                        "fallback_type_values": [],
                        "fallback_role_values": ["assistant"],
                        "raw_media_type": "application/json",
                        "source_id": "source:parallel",
                        "native_id": "event:existing",
                        "kind": "transcript_record",
                        "occurred_at": "2026-07-27T00:00:00Z",
                        "chunk_count": 1,
                        "chunk_receipts": [receipt],
                    }
                ]
            )
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].receipts, (receipt,))

    def test_default_part_size_is_bounded_for_interactive_scans(self) -> None:
        self.assertEqual(DEFAULT_PART_BYTES, 4 * 1024 * 1024)

    def test_indivisible_large_record_gets_its_own_bounded_part(self) -> None:
        archive = _ConcurrentArchive()
        projection = LogicalEvidenceProjectionStore(archive)
        source = "source:parallel"
        records = tuple(
            LogicalEvidenceRecord(
                ordinal=ordinal,
                event_native_id=f"event:large:{ordinal}",
                event_kind="transcript_record",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("assistant",),
                receipts=(
                    f"recall://{source}/event-large-{ordinal}"
                    f"?rev=1#item=0",
                ),
                segment_ordinal=0,
                segment_count=1,
                text=text,
            )
            for ordinal, text in enumerate(
                ("small", "x" * (DEFAULT_PART_BYTES + 1_024), "small")
            )
        )

        upload = projection.put_records(
            tenant_id="tenant:parallel",
            source_id=source,
            native_parent_id="session:large",
            revision=1,
            records=records,
        )

        sizes = [
            reference["size_bytes"]
            for reference in upload.part_references
        ]
        self.assertEqual(len(sizes), 3)
        self.assertLessEqual(sizes[0], DEFAULT_PART_BYTES)
        self.assertGreater(sizes[1], DEFAULT_PART_BYTES)
        self.assertLessEqual(sizes[2], DEFAULT_PART_BYTES)

    def test_parts_upload_concurrently_and_keep_manifest_order(self) -> None:
        archive = _ConcurrentArchive()
        projection = LogicalEvidenceProjectionStore(
            archive,
            part_upload_concurrency=4,
        )

        first = projection.put_records(
            tenant_id="tenant:parallel",
            source_id="source:parallel",
            native_parent_id="session:parallel",
            revision=1,
            records=iter(_records(40)),
            part_bytes=1_024,
        )
        calls_after_first = archive.put_calls
        second = projection.put_records(
            tenant_id="tenant:parallel",
            source_id="source:parallel",
            native_parent_id="session:parallel",
            revision=2,
            records=iter(_records(40)),
            part_bytes=1_024,
            existing_part_references=first.part_references,
        )

        self.assertGreaterEqual(archive.maximum_active, 2)
        self.assertEqual(
            [part.ordinal for part in first.prepared.parts],
            list(range(len(first.prepared.parts))),
        )
        self.assertEqual(
            [value["artifact_id"] for value in first.part_references],
            [value["artifact_id"] for value in second.part_references],
        )
        self.assertNotEqual(
            first.manifest_reference["artifact_id"],
            second.manifest_reference["artifact_id"],
        )
        self.assertEqual(archive.put_calls, calls_after_first + 1)

    def test_failed_parallel_part_upload_removes_completed_objects(self) -> None:
        archive = _ConcurrentArchive(fail_suffix=":3")
        projection = LogicalEvidenceProjectionStore(
            archive,
            part_upload_concurrency=4,
        )

        with self.assertRaisesRegex(RuntimeError, "synthetic upload failure"):
            projection.put_records(
                tenant_id="tenant:parallel",
                source_id="source:parallel",
                native_parent_id="session:parallel",
                revision=1,
                records=iter(_records(20)),
                part_bytes=1_024,
            )

        self.assertEqual(archive.objects, {})


if __name__ == "__main__":
    unittest.main()
