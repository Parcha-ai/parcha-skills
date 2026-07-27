#!/usr/bin/env python3
"""Content-safe proof for exact, bounded logical evidence documents."""

from __future__ import annotations

import hashlib
import gzip
import json
import sys
import tempfile
import uuid
from pathlib import Path


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
    LogicalEvidenceRecord,
    prepare_logical_document,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)


def expect_error(code: str, operation) -> None:
    try:
        operation()
    except LogicalEvidenceError as error:
        assert str(error) == code
    else:
        raise AssertionError(f"operation did not fail with {code}")


def main() -> None:
    nonce = uuid.uuid4().hex
    tenant = f"tenant:synthetic:{nonce}"
    source = f"source:synthetic:{nonce}"
    native_parent = f"session:synthetic:{nonce}"
    records = tuple(
        LogicalEvidenceRecord(
            ordinal=ordinal,
            event_native_id=f"event:synthetic:{nonce}:{ordinal}",
            event_kind="transcript_record",
            occurred_at=(
                "2026-07-27T23:00:00-05:00"
                if ordinal == 0
                else "2026-07-28T01:00:00Z"
            ),
            roles=("assistant",) if ordinal % 2 else ("user",),
            receipts=tuple(
                (
                    f"recall://{source}/event-synthetic-{nonce}-{ordinal}"
                    f"?rev=1#item={item}"
                )
                for item in range(2 if ordinal == 0 else 1)
            ),
            segment_ordinal=0,
            segment_count=1,
            text=(
                json.dumps(
                    {"message": "synthetic native JSON content"},
                    separators=(",", ":"),
                    sort_keys=True,
                )
                if ordinal == 11
                else (
                    f"synthetic logical evidence line {ordinal} "
                    + "0123456789abcdef" * 20
                )
            ),
        )
        for ordinal in range(12)
    )
    prepared = prepare_logical_document(
        tenant_id=tenant,
        source_id=source,
        native_parent_id=native_parent,
        revision=1,
        records=iter(records),
        part_bytes=1_024,
    )
    assert len(prepared.parts) > 1
    assert prepared.record_count == len(records)
    assert prepared.receipt_count == len(records) + 1
    assert prepared.first_occurred_at == "2026-07-28T01:00:00Z"
    assert prepared.last_occurred_at == "2026-07-27T23:00:00-05:00"
    assert [location.ordinal for location in prepared.record_locations] == [
        0,
        *range(len(records)),
    ]

    with tempfile.TemporaryDirectory(prefix="recall-logical-evidence-") as value:
        archive = FilesystemArchiveStore(
            Path(value) / "archive",
            namespace_key=b"synthetic-logical-evidence-key-32",
        )
        store = LogicalEvidenceProjectionStore(archive)
        upload = store.put(prepared)
        manifest = store.read_manifest(
            upload.manifest_reference,
            tenant_id=tenant,
            source_id=source,
        )
        assert manifest["record_count"] == len(records)
        assert manifest["receipt_count"] == len(records) + 1
        assert len(manifest["parts"]) == len(prepared.parts)
        manifest_payload = archive.read_raw(upload.manifest_reference)
        assert tenant.encode() not in manifest_payload
        assert source.encode() not in manifest_payload
        assert native_parent.encode() not in manifest_payload

        decoded: list[dict] = []
        for reference in upload.part_references:
            payload = store.read_part(
                reference,
                tenant_id=tenant,
                source_id=source,
            )
            decoded.extend(json.loads(line) for line in payload.splitlines())
        assert [row["ordinal"] for row in decoded] == list(range(len(records)))
        assert [row["receipts"] for row in decoded] == [
            list(record.receipts) for record in records
        ]
        decoded_texts = [
            (
                json.dumps(
                    row["content"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                if "content" in row
                else row["text"]
            )
            for row in decoded
        ]
        assert decoded_texts == [record.text for record in records]
        assert decoded[-1]["content"] == {
            "message": "synthetic native JSON content"
        }
        reconstructed = b"".join(part.payload for part in prepared.parts)
        assert hashlib.sha256(reconstructed).hexdigest() == (
            prepared.document_content_sha256
        )

        expect_error(
            "logical_evidence_not_found",
            lambda: store.read_part(
                upload.part_references[0],
                tenant_id="tenant:other",
                source_id=source,
            ),
        )
        object_data = (
            archive.root / upload.part_references[0]["object_key"] / "data"
        )
        object_data.write_bytes(b"corrupt")
        expect_error(
            "logical_evidence_corrupt",
            lambda: store.read_part(
                upload.part_references[0],
                tenant_id=tenant,
                source_id=source,
            ),
        )
        object_data.write_bytes(prepared.parts[0].payload)
        assert store.delete(upload) == len(upload.all_references)
        assert store.delete(upload) == 0

        segmented_receipt = (
            f"recall://{source}/event-segmented-{nonce}?rev=1#item=0"
        )
        segmented = prepare_logical_document(
            tenant_id=tenant,
            source_id=source,
            native_parent_id=f"session:segmented:{nonce}",
            revision=1,
            records=iter(
                (
                    LogicalEvidenceRecord(
                        ordinal=0,
                        event_native_id=f"event:segmented:{nonce}",
                        event_kind="transcript_record",
                        occurred_at="2026-07-28T01:00:00Z",
                        roles=("tool",),
                        receipts=(segmented_receipt,),
                        segment_ordinal=0,
                        segment_count=2,
                        text="a" * 1_200,
                    ),
                    LogicalEvidenceRecord(
                        ordinal=1,
                        event_native_id=f"event:segmented:{nonce}",
                        event_kind="transcript_record",
                        occurred_at="2026-07-28T01:00:00Z",
                        roles=("tool",),
                        receipts=(),
                        segment_ordinal=1,
                        segment_count=2,
                        text="b" * 1_200,
                    ),
                )
            ),
            part_bytes=2_048,
        )
        assert segmented.record_count == 2
        assert segmented.receipt_count == 1
        assert [part.receipt_count for part in segmented.parts] == [1, 0]
        segmented_upload = store.put(segmented)
        assert segmented_upload.manifest.record_count == 2
        assert segmented_upload.manifest.receipt_count == 1
        assert store.delete(segmented_upload) == len(
            segmented_upload.all_references
        )

        full_text = json.dumps(
            {"payload": "x" * (9 * 1024 * 1024)},
            separators=(",", ":"),
            sort_keys=True,
        )
        compressed = gzip.compress(full_text.encode(), mtime=0)
        full_reference = archive.put_raw(
            tenant_id=tenant,
            source_id=source,
            native_id=f"event:oversized:{nonce}:full",
            payload=compressed,
            media_type="application/vnd.recall.oversized-record+gzip",
            created_at="2026-07-28T01:00:00Z",
        )
        row = {
            "tenant_id": tenant,
            "source_id": source,
            "native_id": f"event:oversized:{nonce}",
            "kind": "transcript_record",
            "occurred_at": "2026-07-28T01:00:00Z",
            "canonical_redacted": {
                "content": {
                    "contract": "recall.oversized-projection.v1",
                    "full_record_available": True,
                    "archive_encoding": "gzip",
                    "full_size_bytes": len(full_text.encode()),
                    "full_content_sha256": hashlib.sha256(
                        full_text.encode()
                    ).hexdigest(),
                },
                "role": "assistant",
            },
            "chunk_ordinal": 1,
            **{
                "raw_" + key: full_reference[key]
                for key in (
                    "artifact_id",
                    "storage_backend",
                    "object_key",
                    "content_sha256",
                    "size_bytes",
                    "media_type",
                    "encryption",
                    "version_id",
                    "created_at",
                )
            },
        }
        projector = CanonicalLogicalEvidenceProjector(
            None,
            store,
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        oversized_receipts = [
            f"recall://{source}/event-oversized-{nonce}?rev=1#item={item}"
            for item in range(2)
        ]
        restored = list(
            projector._event_records(
                row,
                chunks=["bounded-head", "bounded-tail"],
                receipts=oversized_receipts,
                start_ordinal=0,
            )
        )
        assert len(restored) == 1
        assert "".join(record.text for record in restored) == full_text
        assert restored[0].receipts == tuple(oversized_receipts)
        assert [record.segment_ordinal for record in restored] == [0]
        assert restored[0].segment_count == 1

    print(
        json.dumps(
            {
                "status": "pass",
                "records": len(records),
                "parts": len(prepared.parts),
                "exact_receipts": len(records) + 1,
                "exact_texts": len(records),
                "tenant_escape_reads": 0,
                "plaintext_manifest_identities": 0,
                "corruption_detected": True,
                "oversized_full_bytes": len(full_text.encode()),
                "oversized_segments": len(restored),
                "zero_receipt_continuation_parts": 1,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
