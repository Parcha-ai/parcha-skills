#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.db import BrainStore
from recall_server.projectors import canonical_json


def event(
    *,
    source_id: str,
    principal_id: str,
    native_id: str,
    content: dict,
    artifact: dict,
    created_at: str,
) -> dict:
    return {
        "schema_version": 1,
        "source_id": source_id,
        "native_id": native_id,
        "native_parent_id": native_id,
        "kind": "transcript_record",
        "occurred_at": created_at,
        "observed_at": created_at,
        "principal_id": principal_id,
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "provenance": {
            "connector_id": "synthetic.bulk",
            "connector_schema_version": 1,
            "artifact_ref": artifact,
            "artifact_member": {
                "contract": "recall.artifact-member.v1",
                "schema_version": 1,
                "ordinal": int(native_id.rsplit(":", 1)[1]),
                "native_id": native_id,
                "content_sha256": hashlib.sha256(
                    canonical_json(content)
                ).hexdigest(),
                "byte_start": 0,
                "byte_end": 0,
                "manifest_sha256": artifact["content_sha256"],
            },
        },
        "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
    }


def forget(
    *,
    plane: CanonicalPlane,
    tenant_id: str,
    principal_id: str,
    source_id: str,
    receipt: str,
    suffix: str,
) -> dict:
    return plane.forget({
        "contract": "recall.forget-request.v1",
        "schema_version": 1,
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "source_id": source_id,
        "target_receipt": receipt,
        "mode": "explicit_forget",
        "reason": "owner_requested",
        "requested_at": "2026-07-24T07:15:00Z",
        "idempotency_key": "forget-bulk-" + suffix,
    })


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant_id = f"tenant:bulk:{nonce}"
    principal_id = f"principal:bulk:{nonce}"
    source_id = f"source:bulk:{nonce}"
    created_at = "2026-07-24T07:00:00Z"
    canary = "must-never-enter-bulk-manifest"

    with tempfile.TemporaryDirectory() as temporary:
        archive = FilesystemArchiveStore(
            Path(temporary) / "archive",
            namespace_key=b"b" * 32,
        )
        gateway = CanonicalArchiveGateway(
            store,
            archive,
            tenant_id=tenant_id,
            principal_id=principal_id,
        )
        manifest = canonical_json({
            "contract": "recall.bulk-manifest.v1",
            "schema_version": 1,
            "record_count": 2,
            "records": [
                {"native_id": "native:bulk:0", "content_sha256": "a" * 64},
                {"native_id": "native:bulk:1", "content_sha256": "b" * 64},
            ],
        })
        if canary.encode() in manifest:
            raise RuntimeError("synthetic content crossed into manifest")
        artifact = gateway.put_raw(
            tenant_id=tenant_id,
            source_id=source_id,
            native_id="bulk-" + hashlib.sha256(manifest).hexdigest(),
            payload=manifest,
            media_type="application/vnd.recall.bulk-manifest+json",
            created_at=created_at,
        )
        envelopes = [
            event(
                source_id=source_id,
                principal_id=principal_id,
                native_id=f"native:bulk:{index}",
                content={"text": f"safe record {index}"},
                artifact=artifact,
                created_at=created_at,
            )
            for index in range(2)
        ]
        plane = CanonicalPlane(store, archive)
        first = plane.ingest_batch(
            tenant_id=tenant_id,
            principal_id=principal_id,
            events=envelopes,
        )
        replay = plane.ingest_batch(
            tenant_id=tenant_id,
            principal_id=principal_id,
            events=envelopes,
        )
        if first["inserted"] != 2 or not replay["replay"]:
            raise RuntimeError("bulk replay was not idempotent")
        with store.connect() as connection:
            row = connection.execute(
                """SELECT count(DISTINCT artifact_id) AS artifacts,
                          count(DISTINCT content_sha256) AS content_versions
                   FROM canonical_events
                   WHERE tenant_id=%s AND source_id=%s""",
                (tenant_id, source_id),
            ).fetchone()
        if tuple(row.values()) != (1, 2):
            raise RuntimeError("event identity incorrectly inherited bundle identity")

        first_forget = forget(
            plane=plane,
            tenant_id=tenant_id,
            principal_id=principal_id,
            source_id=source_id,
            receipt=first["receipts"][0],
            suffix=nonce + "-0",
        )
        if first_forget["raw_deleted"] != 0:
            raise RuntimeError("shared manifest was deleted while still referenced")
        if not (archive.root / artifact["object_key"] / "data").exists():
            raise RuntimeError("shared manifest disappeared before final reference")

        second_forget = forget(
            plane=plane,
            tenant_id=tenant_id,
            principal_id=principal_id,
            source_id=source_id,
            receipt=first["receipts"][1],
            suffix=nonce + "-1",
        )
        if second_forget["raw_deleted"] != 1:
            raise RuntimeError("final shared manifest reference was not collected")
        with store.connect() as connection:
            counts = connection.execute(
                """SELECT
                     (SELECT count(*) FROM raw_artifacts
                       WHERE tenant_id=%s AND source_id=%s) AS artifacts,
                     (SELECT count(*) FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s) AS events,
                     (SELECT count(*) FROM canonical_documents
                       WHERE tenant_id=%s AND source_id=%s) AS documents""",
                (
                    tenant_id, source_id,
                    tenant_id, source_id,
                    tenant_id, source_id,
                ),
            ).fetchone()
        if tuple(counts.values()) != (0, 0, 0):
            raise RuntimeError("bulk lifecycle left authoritative content behind")

    store.close()
    print(json.dumps({
        "status": "pass",
        "events": 2,
        "archive_objects": 1,
        "duplicate_events_on_replay": 2,
        "shared_delete_raw_count": 0,
        "final_delete_raw_count": 1,
        "manifest_content_leaks": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
