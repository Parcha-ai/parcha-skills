#!/usr/bin/env python3
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

from psycopg.pq import TransactionStatus

SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore
from recall_server import canonical
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


def tombstone_jit_checks(store, archive, tenant_id, principal_id, source_id):
    """Real public ingest, cyclic historical lineage and pooled-setting isolation."""
    source_id += ":jit"
    created = "2026-07-24T07:00:00Z"
    gateway = CanonicalArchiveGateway(
        store, archive, tenant_id=tenant_id, principal_id=principal_id)
    artifact = gateway.put_raw(
        tenant_id=tenant_id, source_id=source_id, native_id="jit-fixture",
        payload=b"synthetic lineage fixture", media_type="application/json",
        created_at=created)
    plane = CanonicalPlane(store, archive)
    ids = [f"native:jit:{i}" for i in range(10, 15)]

    def envelope(index, parent, version=1, tombstone=False):
        value = event(source_id=source_id, principal_id=principal_id,
                      native_id=ids[index],
                      content=({"target_native_id": ids[index]} if tombstone
                               else {"text": f"safe version {version}"}),
                      artifact=artifact, created_at=created)
        value["native_parent_id"] = ids[parent]
        if tombstone:
            value["kind"] = "tombstone"
        return value

    statements, resolutions, restored = [], [], []
    connect = store.connect

    class ObservedConnection:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def execute(self, query, *args, **kwargs):
            if isinstance(query, str):
                statements.append(query)
                if query.startswith("WITH RECURSIVE roots"):
                    assert self.connection.execute("SHOW jit").fetchone()["jit"] == "off"
                    rows = self.connection.execute(query, *args, **kwargs).fetchall()
                    resolutions.append({row["native_id"] for row in rows})
                    class Result:
                        def fetchall(self):
                            return rows
                    return Result()
            return self.connection.execute(query, *args, **kwargs)

        @contextmanager
        def transaction(self):
            try:
                with self.connection.transaction():
                    yield
            finally:
                assert self.connection.info.transaction_status == TransactionStatus.IDLE
                assert self.connection.execute("SHOW jit").fetchone()["jit"] == "on"
                self.connection.rollback()  # End only the SHOW observation transaction.
                restored.append(True)

    @contextmanager
    def observed_connect():
        with connect() as connection:
            connection.execute("SET jit = on")
            connection.commit()
            yield ObservedConnection(connection)

    def ingest(events):
        with patch.object(store, "connect", observed_connect):
            return plane.ingest_batch(tenant_id=tenant_id, principal_id=principal_id,
                                      events=events)

    # A-B-C-A cycle; D has an old parent B and a newer parent E.
    ingest([envelope(i, parent) for i, parent in enumerate((2, 0, 1, 1, 4))])
    ingest([envelope(3, 4, version=2)])
    assert not any("SET LOCAL" in sql for sql in statements)
    tombstone = envelope(0, 2, version=3, tombstone=True)
    statements.clear()
    before_restored = len(restored)
    with patch.object(canonical, "MAX_LINKED_IDENTITIES", 2):
        try:
            ingest([tombstone])
        except canonical.CanonicalLifecycleError as error:
            assert str(error) == "canonical_lineage_limit"
        else:
            raise AssertionError("cyclic descendant overflow was accepted")
    assert len(restored) > before_restored
    with connect() as connection:
        assert connection.execute(
            "SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s "
            "AND source_id=%s AND is_tombstone", (tenant_id, source_id)
        ).fetchone()["n"] == 0
        assert connection.execute(
            "SELECT count(*) AS n FROM canonical_documents WHERE tenant_id=%s "
            "AND source_id=%s AND is_current", (tenant_id, source_id)
        ).fetchone()["n"] == 5
    before_restored = len(restored)
    ack = ingest([tombstone])
    assert ack["inserted"] == 1 and len(restored) > before_restored
    assert resolutions[-1] == set(ids[:4])
    with connect() as connection:
        remaining = connection.execute(
            "SELECT native_id FROM canonical_documents WHERE tenant_id=%s "
            "AND source_id=%s AND is_current", (tenant_id, source_id)
        ).fetchall()
        assert {row["native_id"] for row in remaining} == {ids[4]}
    statements.clear()
    assert ingest([tombstone])["replay"] is True
    assert not any("SET LOCAL" in sql for sql in statements)


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
        live_status = plane.source_status(
            tenant_id=tenant_id,
            principal_id=principal_id,
            source_id=source_id,
        )
        if (
            live_status["live_events"] != 2
            or live_status["current_documents"] != 2
            or live_status["missing_current_documents"] != 0
            or live_status["empty_current_documents"] != 0
            or live_status["logical_documents_expected"] != 2
        ):
            raise RuntimeError("source status did not report live document parity")

        tombstones = []
        for index in range(2):
            native_id = f"native:bulk:{index}"
            tombstone = event(
                source_id=source_id,
                principal_id=principal_id,
                native_id=native_id,
                content={"target_native_id": native_id},
                artifact=artifact,
                created_at="2026-07-24T07:10:00Z",
            )
            tombstone["kind"] = "tombstone"
            tombstones.append(tombstone)
        tombstone_first = plane.ingest_batch(
            tenant_id=tenant_id,
            principal_id=principal_id,
            events=tombstones,
        )
        tombstone_replay = plane.ingest_batch(
            tenant_id=tenant_id,
            principal_id=principal_id,
            events=tombstones,
        )
        if tombstone_first["inserted"] != 2 or not tombstone_replay["replay"]:
            raise RuntimeError("bulk tombstone replay was not idempotent")
        with store.connect() as connection:
            lifecycle = connection.execute(
                """SELECT
                     (SELECT count(*) FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                         AND is_tombstone) AS tombstones,
                     (SELECT count(*) FROM canonical_documents
                       WHERE tenant_id=%s AND source_id=%s
                         AND is_current) AS current_documents,
                     (SELECT count(*) FROM canonical_chunks
                       WHERE tenant_id=%s AND source_id=%s
                         AND deleted_at IS NULL) AS current_chunks""",
                (
                    tenant_id, source_id,
                    tenant_id, source_id,
                    tenant_id, source_id,
                ),
            ).fetchone()
        if tuple(lifecycle.values()) != (2, 0, 0):
            raise RuntimeError("bulk tombstones left authoritative content current")
        deleted_status = plane.source_status(
            tenant_id=tenant_id,
            principal_id=principal_id,
            source_id=source_id,
        )
        if (
            deleted_status["live_events"] != 0
            or deleted_status["current_documents"] != 0
            or deleted_status["missing_current_documents"] != 0
            or deleted_status["stale_current_documents"] != 0
        ):
            raise RuntimeError("source status did not report tombstone parity")

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

        tombstone_jit_checks(store, archive, tenant_id, principal_id, source_id)

    store.close()
    print(json.dumps({
        "status": "pass",
        "events": 2,
        "tombstones": 2,
        "archive_objects": 1,
        "duplicate_events_on_replay": 2,
        "shared_delete_raw_count": 0,
        "final_delete_raw_count": 1,
        "manifest_content_leaks": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
