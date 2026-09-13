#!/usr/bin/env python3
"""PostgreSQL E2E for oversized transcript records: poison, quarantine, repair.

Reproduces the production failure of 2026-09-13: an oversized record whose
canonical pointer declares a ``full_size_bytes``/``full_content_sha256`` that
does not describe the archived gzip. The logical projector must fail that
group closed (``logical_evidence_full_record_corrupt``) and back it off, the
ingest boundary must refuse a pointer that already contradicts its artifact,
and ``repair_oversized_records`` must re-derive the pointer from the archived
bytes, reset the queue backoff and let the projection finish with the exact
full record restored.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import (  # noqa: E402
    CanonicalArchiveGateway,
    CanonicalLifecycleError,
    CanonicalPlane,
)
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.oversized_repair import repair_oversized_records  # noqa: E402

OVERSIZED = "application/vnd.recall.oversized-record+gzip"


def canonical_json(value) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def oversized_envelope(
    *,
    source_id: str,
    principal_id: str,
    parent: str,
    native_id: str,
    occurred_at: str,
    artifact: dict,
    full_payload: bytes,
    compressed: bytes,
    byte_start: int,
    declared_size: int | None = None,
    declared_sha256: str | None = None,
    archive_size_bytes: int | None = None,
) -> dict:
    rendered = full_payload.decode()
    content = {
        "contract": "recall.oversized-projection.v1",
        "schema_version": 1,
        "_recall_collector_generation": 0,
        "content_fidelity": "head_tail",
        "full_record_available": True,
        "full_content_sha256": (
            hashlib.sha256(full_payload).hexdigest()
            if declared_sha256 is None
            else declared_sha256
        ),
        "full_size_bytes": len(full_payload) if declared_size is None else declared_size,
        "archive_encoding": "gzip",
        "archive_size_bytes": (
            len(compressed) if archive_size_bytes is None else archive_size_bytes
        ),
        "head": rendered[:64],
        "tail": rendered[-64:],
    }
    return {
        "schema_version": 1,
        "source_id": source_id,
        "native_id": native_id,
        "native_parent_id": parent,
        "kind": "transcript_record",
        "occurred_at": occurred_at,
        "observed_at": "2026-09-13T05:10:00Z",
        "principal_id": principal_id,
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
        "provenance": {
            "harness": "codex",
            "connector_id": "codex.jsonl",
            "connector_schema_version": 1,
            "collector_version": 1,
            "privacy_policy_version": "recall-privacy-v2",
            "original_path": "/synthetic/rollout.jsonl",
            "byte_start": byte_start,
            "byte_end": byte_start + len(full_payload) + 1,
            "artifact_ref": artifact,
        },
    }


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:oversized:{nonce}"
    principal = "principal:owner"
    source = f"codex:linux:oversized-{nonce[:8]}"
    parent = f"codex-session-{nonce[:24]}"

    with tempfile.TemporaryDirectory(prefix="recall-oversized-") as value:
        archive = FilesystemArchiveStore(
            Path(value) / "archive",
            namespace_key=b"synthetic-oversized-records-key-32",
        )
        gateway = CanonicalArchiveGateway(
            store, archive, tenant_id=tenant, principal_id=principal,
        )
        projection = LogicalEvidenceProjectionStore(archive)
        projector = CanonicalLogicalEvidenceProjector(
            store,
            projection,
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        plane = CanonicalPlane(store, archive, evidence_projector=projector)

        def archived(native_id: str, record: dict) -> tuple[dict, bytes, bytes]:
            full_payload = canonical_json(record)
            compressed = gzip.compress(full_payload, compresslevel=6, mtime=0)
            artifact = gateway.put_raw(
                tenant_id=tenant,
                source_id=source,
                native_id=native_id + ":full",
                payload=compressed,
                media_type=OVERSIZED,
                created_at=record["timestamp"],
            )
            return artifact, full_payload, compressed

        # Record 0: an ordinary inline record so the group has a healthy neighbour.
        inline_content = {
            "type": "user",
            "timestamp": "2026-09-11T05:59:00Z",
            "message": {"content": "start of the compacted session"},
        }
        inline_artifact = gateway.put_raw(
            tenant_id=tenant,
            source_id=source,
            native_id=f"{parent}-0",
            payload=canonical_json(inline_content),
            media_type="application/x-ndjson",
            created_at="2026-09-11T05:59:00Z",
        )
        inline_event = {
            "schema_version": 1,
            "source_id": source,
            "native_id": f"{parent}-0000000000000000",
            "native_parent_id": parent,
            "kind": "transcript_record",
            "occurred_at": "2026-09-11T05:59:00Z",
            "observed_at": "2026-09-13T05:10:00Z",
            "principal_id": principal,
            "visibility": "private",
            "content_type": "application/json",
            "content": inline_content,
            "content_sha256": hashlib.sha256(canonical_json(inline_content)).hexdigest(),
            "provenance": {
                "harness": "codex",
                "connector_id": "codex.jsonl",
                "connector_schema_version": 1,
                "byte_start": 0,
                "byte_end": 100,
                "artifact_ref": inline_artifact,
            },
        }

        # Record 1: the production shape. Declared size and digest disagree
        # with the archived gzip (here: stale values from other bytes).
        poisoned_record = {
            "type": "compacted",
            "timestamp": "2026-09-11T06:00:05.087Z",
            "payload": {"summary": "compacted history " + "ñ" * 40_000},
        }
        poisoned_native = f"{parent}-000000000016704f"
        poisoned_artifact, poisoned_payload, poisoned_compressed = archived(
            poisoned_native, poisoned_record,
        )
        stale_bytes = canonical_json({**poisoned_record, "payload": {"summary": "older"}})
        poisoned_event = oversized_envelope(
            source_id=source,
            principal_id=principal,
            parent=parent,
            native_id=poisoned_native,
            occurred_at="2026-09-11T06:00:05.087Z",
            artifact=poisoned_artifact,
            full_payload=poisoned_payload,
            compressed=poisoned_compressed,
            byte_start=1_470_543,
            declared_size=len(stale_bytes),
            declared_sha256=hashlib.sha256(stale_bytes).hexdigest(),
        )

        # Record 2: a healthy oversized record in the same group.
        healthy_record = {
            "type": "compacted",
            "timestamp": "2026-09-11T06:57:14.980Z",
            "payload": {"summary": "later compaction " + "y" * 30_000},
        }
        healthy_native = f"{parent}-00000000009b8d29"
        healthy_artifact, healthy_payload, healthy_compressed = archived(
            healthy_native, healthy_record,
        )
        healthy_event = oversized_envelope(
            source_id=source,
            principal_id=principal,
            parent=parent,
            native_id=healthy_native,
            occurred_at="2026-09-11T06:57:14.980Z",
            artifact=healthy_artifact,
            full_payload=healthy_payload,
            compressed=healthy_compressed,
            byte_start=10_194_217,
        )

        acknowledgement = plane.ingest_batch(
            tenant_id=tenant,
            principal_id=principal,
            events=[inline_event, poisoned_event, healthy_event],
        )
        assert acknowledgement["status"] == "committed", acknowledgement
        assert acknowledgement["inserted"] == 3, acknowledgement

        # The ingest boundary refuses a pointer that contradicts its artifact
        # in a way the server can see without reading the object.
        contradicting = oversized_envelope(
            source_id=source,
            principal_id=principal,
            parent=parent,
            native_id=f"{parent}-00000000011eee44",
            occurred_at="2026-09-11T08:39:21.581Z",
            artifact=healthy_artifact,
            full_payload=healthy_payload,
            compressed=healthy_compressed,
            byte_start=18_804_292,
            archive_size_bytes=len(healthy_compressed) + 1,
        )
        for events in ([contradicting], [inline_event, contradicting]):
            try:
                plane.ingest_batch(
                    tenant_id=tenant, principal_id=principal, events=events,
                )
            except CanonicalLifecycleError as error:
                assert error.error_code == "canonical_oversized_pointer_invalid", error
            else:
                raise AssertionError("contradicting oversized pointer was accepted")
        with store.connect() as connection:
            count = connection.execute(
                """SELECT count(*)::integer AS n FROM canonical_events
                   WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchone()["n"]
        assert count == 3, count

        # The poisoned group fails closed and is backed off, never crashing.
        first = projector.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=2, upload_concurrency=1,
        )
        assert first["documents"] == 0, first
        with store.connect() as connection:
            queue = connection.execute(
                """SELECT attempts,next_attempt_at,last_error_code
                     FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (tenant, source, parent),
            ).fetchone()
        assert queue is not None
        assert queue["attempts"] == 1, dict(queue)
        assert queue["next_attempt_at"] is not None
        assert queue["last_error_code"] == "logical_evidence_full_record_corrupt", dict(queue)

        # Dry run: counts only, nothing written.
        dry = repair_oversized_records(
            store,
            archive,
            tenant_id=tenant,
            source_id=source,
            native_parent_id=parent,
            dry_run=True,
        )
        assert dry == {
            "events": 2,
            "consistent": 1,
            "repaired": 1,
            "record_mismatch": 0,
            "unreadable": 0,
            "dry_run": True,
            "written": 0,
            "requeued": False,
        }, dry
        with store.connect() as connection:
            still = connection.execute(
                """SELECT canonical_redacted #>> '{content,full_content_sha256}' AS digest,
                          (canonical_redacted #> '{content,full_size_bytes}')::bigint AS size
                     FROM canonical_events
                    WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                (tenant, source, poisoned_native),
            ).fetchone()
        assert still["digest"] == hashlib.sha256(stale_bytes).hexdigest()
        assert still["size"] == len(stale_bytes)

        # Real repair: pointer re-derived from the archive, backoff reset.
        repaired = repair_oversized_records(
            store,
            archive,
            tenant_id=tenant,
            source_id=source,
            native_parent_id=parent,
            dry_run=False,
        )
        assert repaired["repaired"] == 1 and repaired["written"] == 1, repaired
        assert repaired["requeued"] is True, repaired
        with store.connect() as connection:
            fixed = connection.execute(
                """SELECT canonical_redacted->'content' AS content
                     FROM canonical_events
                    WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                (tenant, source, poisoned_native),
            ).fetchone()["content"]
            queue = connection.execute(
                """SELECT attempts,next_attempt_at,last_error_code,generation
                     FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (tenant, source, parent),
            ).fetchone()
        assert fixed["full_size_bytes"] == len(poisoned_payload), fixed["full_size_bytes"]
        assert fixed["full_content_sha256"] == hashlib.sha256(poisoned_payload).hexdigest()
        assert fixed["head"] == poisoned_payload.decode()[:64]
        assert fixed["_recall_collector_generation"] == 0
        assert queue["attempts"] == 0 and queue["next_attempt_at"] is None
        assert queue["last_error_code"] is None
        assert queue["generation"] >= 2, dict(queue)

        # Repairing a healthy group again is a no-op that still reports counts.
        again = repair_oversized_records(
            store,
            archive,
            tenant_id=tenant,
            source_id=source,
            native_parent_id=parent,
            dry_run=False,
        )
        assert again["consistent"] == 2 and again["repaired"] == 0, again
        assert again["written"] == 0 and again["requeued"] is True, again

        # The projection now completes and restores the exact full record.
        second = projector.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=2, upload_concurrency=1,
        )
        assert second["documents"] == 1, second
        assert second["records"] == 3, second
        receipt = f"recall://{source}/{poisoned_native}?rev=1#item=0"
        targets = projector.targets_for_receipts(
            tenant_id=tenant,
            source_ids=(source,),
            receipts=(receipt,),
            limit=10,
        )
        assert len(targets) == 1, targets
        rows = [
            json.loads(line)
            for line in projection.read_part(
                targets[0]["reference"], tenant_id=tenant, source_id=source,
            ).splitlines()
        ]
        restored = [row for row in rows if row.get("event_native_id") == poisoned_native]
        assert restored, [row.get("event_native_id") for row in rows]
        row = restored[-1]
        if "content" in row:
            restored_text = canonical_json(row["content"])
        else:
            restored_text = row["text"].encode()
        assert restored_text == poisoned_payload, row.keys()
        with store.connect() as connection:
            remaining = connection.execute(
                """SELECT count(*)::integer AS n
                     FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchone()["n"]
        assert remaining == 0, remaining

        # A group whose artifact belongs to a different record is reported and
        # never stitched together: the timestamp inside the archived record
        # must agree with the event.
        other_parent = f"codex-session-{uuid.uuid4().hex[:24]}"
        foreign_native = f"{other_parent}-0000000000000010"
        foreign_artifact, foreign_payload, foreign_compressed = archived(
            foreign_native, {**healthy_record, "timestamp": "2026-09-12T17:36:00.652Z"},
        )
        foreign_event = oversized_envelope(
            source_id=source,
            principal_id=principal,
            parent=other_parent,
            native_id=foreign_native,
            occurred_at="2026-09-11T20:07:32.326Z",
            artifact=foreign_artifact,
            full_payload=foreign_payload,
            compressed=foreign_compressed,
            byte_start=16,
            declared_size=1,
        )
        plane.ingest_batch(
            tenant_id=tenant, principal_id=principal, events=[foreign_event],
        )
        blocked = repair_oversized_records(
            store,
            archive,
            tenant_id=tenant,
            source_id=source,
            native_parent_id=other_parent,
            dry_run=False,
        )
        assert blocked["record_mismatch"] == 1 and blocked["written"] == 0, blocked
        assert blocked["requeued"] is False, blocked

    print(json.dumps({"status": "ok", "oversized_records_e2e": True}))


if __name__ == "__main__":
    main()
