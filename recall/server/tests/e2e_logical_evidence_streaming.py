#!/usr/bin/env python3
"""Bounded-memory throughput proof for multi-gigabyte logical documents."""

from __future__ import annotations

import hashlib
import json
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
)


class MeasuringArchive:
    def __init__(self) -> None:
        self.calls = 0
        self.total_bytes = 0
        self.maximum_payload_bytes = 0
        self.references: dict[str, bytes] = {}

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
        digest = hashlib.sha256(payload).hexdigest()
        self.calls += 1
        self.total_bytes += len(payload)
        self.maximum_payload_bytes = max(
            self.maximum_payload_bytes,
            len(payload),
        )
        self.references[digest] = b"present"
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": tenant_id,
            "source_id": source_id,
            "artifact_id": "art_" + digest[:32],
            "storage_backend": "filesystem",
            "object_key": f"objects/{digest[:2]}/{digest}",
            "content_sha256": digest,
            "size_bytes": len(payload),
            "media_type": media_type,
            "encryption": "filesystem-owner-only",
            "version_id": "fs-" + digest[:32],
            "created_at": created_at,
        }

    def read_raw(self, value: dict[str, Any]) -> bytes:
        raise AssertionError("streaming proof must not read uploaded parts")

    def delete_raw(self, value: dict[str, Any]) -> bool:
        return self.references.pop(value["content_sha256"], None) is not None


def records(count: int):
    source = "source:streaming"
    text = "0123456789abcdef" * 32
    for ordinal in range(count):
        yield LogicalEvidenceRecord(
            ordinal=ordinal,
            event_native_id=f"event:streaming:{ordinal}",
            event_kind="transcript_record",
            occurred_at="2026-07-27T00:00:00Z",
            roles=("assistant",) if ordinal % 2 else ("user",),
            receipts=(
                f"recall://{source}/event-streaming-{ordinal}"
                f"?rev=1#item={ordinal}",
            ),
            segment_ordinal=0,
            segment_count=1,
            text=f"{text}:{ordinal}",
        )


def main() -> None:
    count = 100_000
    part_bytes = 4 * 1024 * 1024
    archive = MeasuringArchive()
    projection = LogicalEvidenceProjectionStore(
        archive,
        part_upload_concurrency=4,
    )
    tracemalloc.start()
    started = time.monotonic()
    upload = projection.put_records(
        tenant_id="tenant:streaming",
        source_id="source:streaming",
        native_parent_id="session:streaming",
        revision=1,
        records=records(count),
        part_bytes=part_bytes,
    )
    duration = time.monotonic() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rate = count / duration
    retained_payload_bytes = sum(
        len(part.payload) for part in upload.prepared.parts
    )
    assert upload.prepared.record_count == count
    assert upload.prepared.receipt_count == count
    assert len(upload.prepared.parts) > 10
    assert archive.maximum_payload_bytes <= part_bytes
    assert retained_payload_bytes == 0
    assert peak < 64 * 1024 * 1024
    assert rate >= 1_000
    print(
        json.dumps(
            {
                "status": "pass",
                "records": count,
                "objects": len(upload.all_references),
                "input_megabytes": round(
                    archive.total_bytes / 1024 / 1024,
                    2,
                ),
                "duration_seconds": round(duration, 3),
                "records_per_second": round(rate, 1),
                "peak_memory_megabytes": round(peak / 1024 / 1024, 2),
                "maximum_part_megabytes": round(
                    archive.maximum_payload_bytes / 1024 / 1024,
                    2,
                ),
                "retained_part_payload_bytes": retained_payload_bytes,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
