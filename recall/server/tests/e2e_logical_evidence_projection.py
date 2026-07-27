#!/usr/bin/env python3
"""PostgreSQL E2E for exact source-level evidence projection."""

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
from recall_server.canonical import canonical_text_chunks  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def insert_source(connection, tenant: str, principal: str, source: str) -> None:
    connection.execute(
        "INSERT INTO brain_tenants(tenant_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (tenant,),
    )
    connection.execute(
        """INSERT INTO brain_principals(tenant_id,principal_id)
           VALUES (%s,%s) ON CONFLICT DO NOTHING""",
        (tenant, principal),
    )
    connection.execute(
        """INSERT INTO canonical_sources(
               tenant_id,source_id,owner_principal_id
           ) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
        (tenant, source, principal),
    )


def insert_record(
    connection,
    *,
    tenant: str,
    source: str,
    parent: str,
    native: str,
    text: str,
    role: str,
    byte_start: int,
    raw_reference: dict[str, object] | None = None,
    canonical_content: dict[str, object] | None = None,
) -> str:
    identity = digest(f"{tenant}\0{source}\0{native}\0{text}")
    artifact = "art_" + identity[:32]
    job = "job_" + identity[:32]
    event = "evt_" + identity[:32]
    document = "doc_" + identity[:32]
    object_digest = digest("raw\0" + identity)
    if raw_reference is None:
        raw_reference = {
            "artifact_id": artifact,
            "storage_backend": "filesystem",
            "object_key": f"objects/{object_digest[:2]}/{object_digest}",
            "content_sha256": digest(text),
            "size_bytes": len(text.encode()),
            "media_type": "application/json",
            "encryption": "filesystem-owner-only",
            "version_id": "fs-" + identity,
        }
    connection.execute(
        """INSERT INTO raw_artifacts(
               tenant_id,source_id,artifact_id,storage_backend,object_key,
               content_sha256,size_bytes,media_type,encryption,version_id
           ) VALUES (
               %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
           )""",
        (
            tenant,
            source,
            raw_reference["artifact_id"],
            raw_reference["storage_backend"],
            raw_reference["object_key"],
            raw_reference["content_sha256"],
            raw_reference["size_bytes"],
            raw_reference["media_type"],
            raw_reference["encryption"],
            raw_reference["version_id"],
        ),
    )
    connection.execute(
        """INSERT INTO canonical_ingest_jobs(
               tenant_id,source_id,job_id,connector_id,mode,status
           ) VALUES (
               %s,%s,%s,'connector.synthetic','incremental','committed'
           )""",
        (tenant, source, job),
    )
    envelope = {
        "content": canonical_content or {
            "message": {"role": role, "text": text},
            "type": role,
        },
        "type": role,
        "provenance": {"byte_start": byte_start, "byte_end": byte_start + 10},
    }
    connection.execute(
        """INSERT INTO canonical_events(
               tenant_id,source_id,event_id,native_id,native_parent_id,
               artifact_id,job_id,kind,content_sha256,revision,occurred_at,
               observed_at,canonical_redacted
           ) VALUES (
               %s,%s,%s,%s,%s,%s,%s,'transcript_record',%s,1,
               '2026-07-27T00:00:00Z','2026-07-27T00:00:01Z',%s
           )""",
        (
            tenant,
            source,
            event,
            native,
            parent,
            raw_reference["artifact_id"],
            job,
            digest(text),
            json.dumps(envelope),
        ),
    )
    connection.execute(
        """INSERT INTO canonical_documents(
               tenant_id,source_id,document_id,event_id,artifact_id,native_id,
               content_sha256,revision,is_current,text_redacted,text_sha256
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,1,true,%s,%s)""",
        (
            tenant,
            source,
            document,
            event,
            raw_reference["artifact_id"],
            native,
            digest(text),
            text,
            digest(text),
        ),
    )
    receipts = []
    for ordinal, chunk_text in enumerate(canonical_text_chunks(text)):
        chunk_identity = digest(f"{identity}\0{ordinal}")
        receipt = f"recall://{source}/{native}?rev=1#item={ordinal}"
        connection.execute(
            """INSERT INTO canonical_chunks(
                   tenant_id,source_id,chunk_id,document_id,ordinal,receipt,
                   text_redacted,text_sha256
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                tenant,
                source,
                "chk_" + chunk_identity[:32],
                document,
                ordinal,
                receipt,
                chunk_text,
                digest(chunk_text),
            ),
        )
        receipts.append(receipt)
    return receipts[0]


def archive_object_count(root: Path) -> int:
    return sum(1 for path in root.rglob("data") if path.is_file())


class FlakyDeleteArchive:
    def __init__(self, delegate: FilesystemArchiveStore) -> None:
        self.delegate = delegate
        self.failures_remaining = 0

    def put_raw(self, **values):
        return self.delegate.put_raw(**values)

    def read_raw(self, value):
        return self.delegate.read_raw(value)

    def delete_raw(self, value):
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise RuntimeError("synthetic delete failure")
        return self.delegate.delete_raw(value)


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:e2e:{nonce}"
    other_tenant = f"tenant:e2e:{nonce}:other"
    principal = f"principal:e2e:{nonce}"
    claude = f"claude:e2e:{nonce}"
    codex = f"codex:e2e:{nonce}"
    claude_parent = f"claude-session-{nonce}"
    codex_parent = f"codex-session-{nonce}"
    texts = {
        "claude-0": "synthetic claude first",
        "claude-1": "synthetic claude second",
        "claude-2": "synthetic claude third " + "multichunk " * 3_000,
        "codex-0": "synthetic codex first",
        "codex-1": "synthetic codex second",
    }
    receipts: dict[str, str] = {}
    with store.connect() as connection:
        insert_source(connection, tenant, principal, claude)
        insert_source(connection, tenant, principal, codex)
        receipts["claude-2"] = insert_record(
            connection,
            tenant=tenant,
            source=claude,
            parent=claude_parent,
            native=f"claude-record-{nonce}-2",
            text=texts["claude-2"],
            role="assistant",
            byte_start=20,
        )
        receipts["claude-0"] = insert_record(
            connection,
            tenant=tenant,
            source=claude,
            parent=claude_parent,
            native=f"claude-record-{nonce}-0",
            text=texts["claude-0"],
            role="user",
            byte_start=0,
        )
        receipts["claude-1"] = insert_record(
            connection,
            tenant=tenant,
            source=claude,
            parent=claude_parent,
            native=f"claude-record-{nonce}-1",
            text=texts["claude-1"],
            role="assistant",
            byte_start=10,
        )
        for ordinal in range(2):
            key = f"codex-{ordinal}"
            receipts[key] = insert_record(
                connection,
                tenant=tenant,
                source=codex,
                parent=codex_parent,
                native=f"codex-record-{nonce}-{ordinal}",
                text=texts[key],
                role="user" if ordinal == 0 else "assistant",
                byte_start=ordinal * 10,
            )

    with tempfile.TemporaryDirectory(prefix="recall-logical-projector-") as value:
        archive_root = Path(value) / "archive"
        archive = FlakyDeleteArchive(
            FilesystemArchiveStore(
                archive_root,
                namespace_key=b"synthetic-logical-projector-key-32",
            ),
        )
        oversized_text = json.dumps(
            {
                "type": "assistant",
                "message": {
                    "role": "assistant",
                    "content": "complete oversized source record " + "x" * 40_000,
                },
            },
            separators=(",", ":"),
        )
        oversized_payload = gzip.compress(oversized_text.encode())
        oversized_reference = archive.put_raw(
            tenant_id=tenant,
            source_id=codex,
            native_id=f"codex-record-{nonce}-oversized:full",
            payload=oversized_payload,
            media_type="application/vnd.recall.oversized-record+gzip",
            created_at="2026-07-27T00:00:01Z",
        )
        with store.connect() as connection:
            receipts["codex-oversized"] = insert_record(
                connection,
                tenant=tenant,
                source=codex,
                parent=codex_parent,
                native=f"codex-record-{nonce}-oversized",
                text="bounded oversized projection",
                role="assistant",
                byte_start=20,
                raw_reference=oversized_reference,
                canonical_content={
                    "contract": "recall.oversized-projection.v1",
                    "archive_encoding": "gzip",
                    "full_record_available": True,
                    "full_size_bytes": len(oversized_text.encode()),
                    "full_content_sha256": digest(oversized_text),
                },
            )
        projection = LogicalEvidenceProjectionStore(archive)
        projector = CanonicalLogicalEvidenceProjector(
            store,
            projection,
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        assert projector.seed_backfill(tenant_id=tenant) == 2
        first = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=2,
            upload_concurrency=2,
        )
        assert first["documents"] == 2
        assert first["records"] == 6
        assert first["receipts"] == 7
        assert first["objects"] == 4
        assert first["cleanup_failures"] == 0
        assert archive_object_count(archive_root) == 5
        assert projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
        )["documents"] == 0

        targets = projector.targets_for_receipts(
            tenant_id=tenant,
            source_ids=(claude,),
            receipts=(receipts["claude-1"],),
            limit=10,
        )
        assert len(targets) == 1
        payload = projection.read_part(
            targets[0]["reference"],
            tenant_id=tenant,
            source_id=claude,
        )
        rows = [json.loads(line) for line in payload.splitlines()]
        assert [row["text"] for row in rows] == [
            texts["claude-0"],
            texts["claude-1"],
            texts["claude-2"],
        ]
        assert [row["roles"] for row in rows] == [
            ["user"],
            ["assistant"],
            ["assistant"],
        ]
        assert targets[0]["receipts"] == (receipts["claude-1"],)
        oversized_targets = projector.targets_for_receipts(
            tenant_id=tenant,
            source_ids=(codex,),
            receipts=(receipts["codex-oversized"],),
            limit=10,
        )
        oversized_rows = [
            json.loads(line)
            for line in projection.read_part(
                oversized_targets[0]["reference"],
                tenant_id=tenant,
                source_id=codex,
            ).splitlines()
        ]
        assert oversized_rows[-1]["text"] == oversized_text

        with store.connect() as connection:
            receipts["claude-3"] = insert_record(
                connection,
                tenant=tenant,
                source=claude,
                parent=claude_parent,
                native=f"claude-record-{nonce}-3",
                text="synthetic claude fourth",
                role="user",
                byte_start=30,
            )
            mark_logical_evidence_dirty(
                connection,
                tenant_id=tenant,
                source_id=claude,
                native_ids=[f"claude-record-{nonce}-3"],
                reason="ingest",
            )
        archive.failures_remaining = 1
        revised = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
            upload_concurrency=2,
        )
        assert revised["documents"] == 1
        assert revised["records"] == 4
        assert revised["old_objects_deleted"] == 1
        assert revised["cleanup_failures"] == 1
        assert revised["cleanup_pending"] == 1
        assert archive_object_count(archive_root) == 6
        cleanup = projector.drain_cleanup(tenant_id=tenant)
        assert cleanup["completed"] == 1
        assert cleanup["deleted"] == 1
        assert cleanup["failures"] == 0
        assert cleanup["pending"] == 0
        assert archive_object_count(archive_root) == 5
        with store.connect() as connection:
            state = connection.execute(
                """SELECT revision,record_count,part_count
                     FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, claude),
            ).fetchone()
        assert state == {"revision": 2, "record_count": 4, "part_count": 1}

        forgotten_native = f"claude-record-{nonce}-1"
        with store.connect() as connection:
            connection.execute(
                """UPDATE canonical_chunks chunk SET deleted_at=now()
                     FROM canonical_documents document
                    WHERE chunk.tenant_id=document.tenant_id
                      AND chunk.source_id=document.source_id
                      AND chunk.document_id=document.document_id
                      AND document.tenant_id=%s AND document.source_id=%s
                      AND document.native_id=%s""",
                (tenant, claude, forgotten_native),
            )
            connection.execute(
                """UPDATE canonical_documents
                      SET is_current=false,deleted_at=now()
                    WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                (tenant, claude, forgotten_native),
            )
        assert projector.delete_native_ids(
            tenant_id=tenant,
            source_id=claude,
            native_ids=[forgotten_native],
        ) == 2
        assert archive_object_count(archive_root) == 3
        assert projector.targets_for_receipts(
            tenant_id=tenant,
            source_ids=(claude,),
            receipts=(receipts["claude-1"],),
            limit=10,
        ) == []
        rebuilt = projector.project_pending(
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
        )
        assert rebuilt["documents"] == 1
        assert rebuilt["records"] == 3
        assert archive_object_count(archive_root) == 5

        try:
            projector.project_pending(tenant_id=other_tenant)
        except Exception as error:
            assert str(error) == "logical_evidence_tenant_not_configured"
        else:
            raise AssertionError("bound projector accepted another tenant")

    with store.connect() as connection:
        counts = connection.execute(
            """SELECT
                   (SELECT count(*) FROM canonical_evidence_documents
                     WHERE tenant_id=%s) AS documents,
                   (SELECT count(*)
                      FROM canonical_evidence_documents
                     WHERE tenant_id=%s) AS receipt_documents,
                   (SELECT COALESCE(sum(receipt_count),0)
                      FROM canonical_evidence_documents
                     WHERE tenant_id=%s) AS receipts""",
            (tenant, tenant, tenant),
        ).fetchone()
    assert counts == {
        "documents": 2,
        "receipt_documents": 2,
        "receipts": 7,
    }
    print(
        json.dumps(
            {
                "status": "pass",
                "logical_documents": 2,
                "source_families": 2,
                "exact_receipts": 7,
                "oversized_sql_restorations": 1,
                "idempotent_reprojects": 0,
                "revision_replacements": 1,
                "durable_cleanup_retries": 1,
                "forgotten_receipt_hits": 0,
                "tenant_escape_writes": 0,
                "orphan_objects": 0,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
