#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg

SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import (
    CanonicalArchiveGateway,
    CanonicalLifecycleError,
    CanonicalPlane,
)
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy
from recall_server.projectors import canonical_json


class LoseDeleteAcknowledgementOnce:
    def __init__(self, archive: FilesystemArchiveStore):
        self.archive = archive
        self.lose_once = True

    def put_raw(self, **kwargs):
        return self.archive.put_raw(**kwargs)

    def delete_raw(self, reference):
        removed = self.archive.delete_raw(reference)
        if self.lose_once:
            self.lose_once = False
            raise OSError("synthetic lost archive acknowledgement")
        return removed


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(dsn)
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant_id = f"tenant:lifecycle:{nonce}"
    principal_id = f"principal:lifecycle:{nonce}"
    source_id = f"source:lifecycle:{nonce}"
    native_id = "native:lifecycle"
    raw_canary = "raw-secret-canary-lifecycle-91"
    redacted_text = "safe lifecycle context"
    created_at = "2026-07-19T12:00:00Z"

    with tempfile.TemporaryDirectory() as temporary:
        archive_store = FilesystemArchiveStore(
            Path(temporary) / "archive",
            namespace_key=b"l" * 32,
        )
        unreliable_archive = LoseDeleteAcknowledgementOnce(archive_store)
        gateway = CanonicalArchiveGateway(
            store,
            unreliable_archive,
            tenant_id=tenant_id,
            principal_id=principal_id,
        )
        raw_payload = json.dumps({
            "native_id": native_id,
            "content": {"text": raw_canary},
        }, sort_keys=True, separators=(",", ":")).encode()
        artifact = gateway.put_raw(
            tenant_id=tenant_id,
            source_id=source_id,
            native_id=native_id,
            payload=raw_payload,
            media_type="application/json",
            created_at=created_at,
        )
        content = {"text": redacted_text, "role": "user"}
        event = {
            "schema_version": 1,
            "source_id": source_id,
            "native_id": native_id,
            "native_parent_id": native_id,
            "kind": "connector_record",
            "occurred_at": created_at,
            "observed_at": created_at,
            "principal_id": principal_id,
            "visibility": "private",
            "content_type": "application/json",
            "content": content,
            "provenance": {
                "connector_id": "synthetic.lifecycle",
                "connector_schema_version": 1,
                "artifact_ref": artifact,
            },
            "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
        }
        logical = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(archive_store),
            bound_tenant_id=tenant_id,
            raw_archive=archive_store,
        )
        plane = CanonicalPlane(
            store,
            unreliable_archive,
            evidence_projector=logical,
        )
        first = plane.ingest_document(
            tenant_id=tenant_id,
            principal_id=principal_id,
            connector_id="synthetic.lifecycle",
            artifact_ref=artifact,
            envelope=event,
            text_redacted=redacted_text,
        )
        replay = plane.ingest_document(
            tenant_id=tenant_id,
            principal_id=principal_id,
            connector_id="synthetic.lifecycle",
            artifact_ref=artifact,
            envelope=event,
            text_redacted=redacted_text,
        )
        if first["inserted"] != 1 or replay["replay"] is not True:
            raise RuntimeError("canonical ingest replay contract failed")
        with store.connect() as connection:
            raw_leaks = connection.execute(
                """SELECT
                     (SELECT count(*) FROM canonical_events
                       WHERE tenant_id=%s AND canonical_redacted::text LIKE %s)
                   + (SELECT count(*) FROM canonical_documents
                       WHERE tenant_id=%s AND text_redacted LIKE %s)
                   + (SELECT count(*) FROM canonical_chunks
                       WHERE tenant_id=%s AND text_redacted LIKE %s) AS count""",
                (
                    tenant_id, f"%{raw_canary}%",
                    tenant_id, f"%{raw_canary}%",
                    tenant_id, f"%{raw_canary}%",
                ),
            ).fetchone()["count"]
        if raw_leaks:
            raise RuntimeError("raw archive bytes crossed into canonical projections")

        # H1-T3: project the logical document and its passages so forget is
        # exercised against the stable-id passage plane too.
        # Ingest already queued the group; seeding is idempotent here.
        logical.seed_backfill(tenant_id=tenant_id)
        logical_result = logical.project_pending(
            tenant_id=tenant_id,
            batch_size=10,
            max_batches=1,
            upload_concurrency=1,
        )
        if logical_result["documents"] != 1:
            raise RuntimeError("logical projection did not build the document")
        passages = CanonicalPassageProjector(
            store,
            LogicalEvidenceProjectionStore(archive_store),
            policy=PassagePolicy(target_tokens=4, overlap_tokens=1),
            bound_tenant_id=tenant_id,
        )
        passage_result = passages.project_pending(
            tenant_id=tenant_id,
            batch_size=10,
            max_batches=1,
            concurrency=1,
        )
        if passage_result["documents"] != 1 or passage_result["inserted"] < 1:
            raise RuntimeError(f"passage projection did not build passages: {passage_result}")
        with store.connect() as connection:
            forgotten_receipts = sorted({
                receipt
                for row in connection.execute(
                    """SELECT receipts FROM canonical_passages
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant_id, source_id),
                ).fetchall()
                for receipt in row["receipts"]
            })
        if not forgotten_receipts:
            raise RuntimeError("passages carry no receipts")
        passages_before_forget = passage_result["inserted"]
        forget = {
            "contract": "recall.forget-request.v1",
            "schema_version": 1,
            "tenant_id": tenant_id,
            "principal_id": principal_id,
            "source_id": source_id,
            "target_receipt": first["receipt"],
            "mode": "explicit_forget",
            "reason": "owner_requested",
            "requested_at": "2026-07-19T12:05:00Z",
            "idempotency_key": "forget-lifecycle-" + nonce,
        }
        try:
            plane.forget(forget)
        except CanonicalLifecycleError as error:
            if error.error_code != "archive_delete_failed":
                raise
        else:
            raise RuntimeError("lost archive acknowledgement did not interrupt forget")

        with store.connect() as connection:
            state = connection.execute(
                """SELECT
                     (SELECT count(*) FROM canonical_chunks chunk
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       WHERE document.tenant_id=%s AND document.source_id=%s
                         AND document.native_id=%s AND chunk.deleted_at IS NULL) AS live_chunks,
                     (SELECT status FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s) AS forget_status,
                     (SELECT state FROM raw_artifacts
                       WHERE tenant_id=%s AND source_id=%s) AS artifact_state""",
                (
                    tenant_id, source_id, native_id,
                    tenant_id, source_id,
                    tenant_id, source_id,
                ),
            ).fetchone()
        if tuple(state.values()) != (0, "deleting", "deleting"):
            raise RuntimeError("forget crash fence did not fail closed")
        if (archive_store.root / artifact["object_key"] / "data").exists():
            raise RuntimeError("raw object survived completed archive deletion")
        try:
            gateway.put_raw(
                tenant_id=tenant_id,
                source_id=source_id,
                native_id=native_id,
                payload=raw_payload,
                media_type="application/json",
                created_at=created_at,
            )
        except CanonicalLifecycleError as error:
            if error.error_code != "archive_identity_forgotten":
                raise
        else:
            raise RuntimeError("archive gateway resurrected a forgotten identity")

        completed = plane.forget(forget)
        replayed_forget = plane.forget(forget)
        if completed["raw_deleted"] != 1 or replayed_forget["replay"] is not True:
            raise RuntimeError("forget retry contract failed")
        with store.connect() as connection:
            counts = connection.execute(
                """SELECT
                     (SELECT count(*) FROM raw_artifacts
                       WHERE tenant_id=%s AND source_id=%s) AS artifacts,
                     (SELECT count(*) FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s) AS events,
                     (SELECT count(*) FROM canonical_documents
                       WHERE tenant_id=%s AND source_id=%s) AS documents,
                     (SELECT count(*) FROM canonical_chunks
                       WHERE tenant_id=%s AND source_id=%s) AS chunks,
                     (SELECT count(*) FROM forget_tombstones
                       WHERE tenant_id=%s AND source_id=%s AND status='deleted') AS tombstones""",
                (
                    tenant_id, source_id, tenant_id, source_id,
                    tenant_id, source_id, tenant_id, source_id,
                    tenant_id, source_id,
                ),
            ).fetchone()
            if tuple(counts.values()) != (0, 0, 0, 0, 1):
                raise RuntimeError("authoritative forget left canonical evidence behind")
            passage_counts = connection.execute(
                """SELECT
                     (SELECT count(*) FROM canonical_evidence_documents
                       WHERE tenant_id=%s AND source_id=%s) AS evidence,
                     (SELECT count(*) FROM canonical_passage_documents
                       WHERE tenant_id=%s AND source_id=%s) AS passage_documents,
                     (SELECT count(*) FROM canonical_passages
                       WHERE tenant_id=%s AND source_id=%s) AS passages,
                     (SELECT count(*) FROM canonical_passage_embeddings
                       WHERE tenant_id=%s AND source_id=%s) AS embeddings,
                     (SELECT count(*) FROM canonical_passage_actors
                       WHERE tenant_id=%s AND source_id=%s) AS actors,
                     (SELECT count(*) FROM canonical_passages
                       WHERE tenant_id=%s AND receipts && %s::text[])
                       AS forgotten_receipt_overlap""",
                (
                    tenant_id, source_id, tenant_id, source_id,
                    tenant_id, source_id, tenant_id, source_id,
                    tenant_id, source_id, tenant_id, forgotten_receipts,
                ),
            ).fetchone()
            if any(passage_counts.values()):
                raise RuntimeError(
                    f"forget left passage-plane rows behind: {dict(passage_counts)}"
                )
            try:
                with connection.transaction():
                    connection.execute(
                        """INSERT INTO canonical_events(
                               tenant_id,source_id,event_id,native_id,artifact_id,job_id,
                               kind,content_sha256,revision,occurred_at,observed_at,
                               canonical_redacted
                           ) VALUES (%s,%s,%s,%s,'art_missing','job_missing','document',
                                     %s,2,now(),now(),'{}'::jsonb)""",
                        (
                            tenant_id, source_id, "evt_resurrection", native_id,
                            hashlib.sha256(b"resurrection").hexdigest(),
                        ),
                    )
            except psycopg.Error as error:
                if error.sqlstate != "23514":
                    raise
            else:
                raise RuntimeError("database trigger accepted forgotten identity")
        if raw_canary in json.dumps({
            "first": first,
            "completed": completed,
            "replayed_forget": replayed_forget,
        }):
            raise RuntimeError("lifecycle result leaked raw content")
    store.close()
    print(json.dumps({
        "status": "pass",
        "archive_before_canonical": True,
        "ingest_replay": True,
        "forget_crash_fenced": True,
        "forget_retry": True,
        "raw_versions_remaining": 0,
        "derived_rows_remaining": 0,
        "passages_before_forget": passages_before_forget,
        "passages_after_forget": 0,
        "forgotten_receipt_overlap": 0,
        "resurrection_rejections": 2,
        "public_payload_canary_leaks": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
