#!/usr/bin/env python3
"""PostgreSQL E2E for fail-closed canonical body thinning."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
RECALL = SERVER.parent
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(RECALL))

from recall_server.canonical_text import canonical_text_chunks  # noqa: E402
from recall_server.canonical_thinning import thin_canonical_bodies  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def insert_document(
    connection,
    *,
    tenant: str,
    principal: str,
    source: str,
    suffix: str,
    text: str,
    corrupt_chunks: bool = False,
) -> tuple[str, str]:
    native = f"native:{suffix}"
    parent = f"session:{suffix}"
    content_hash = digest(f"event:{suffix}:{text}")
    artifact = "art_" + digest(f"artifact:{suffix}")[:32]
    job = "job_" + digest(f"job:{suffix}")[:32]
    event = "evt_" + digest(f"event:{suffix}")[:32]
    document = "doc_" + digest(f"document:{suffix}")[:32]
    object_hash = digest(f"object:{suffix}")
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
        """INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id)
           VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""",
        (tenant, source, principal),
    )
    connection.execute(
        """INSERT INTO raw_artifacts(
               tenant_id,source_id,artifact_id,storage_backend,object_key,
               content_sha256,size_bytes,media_type,encryption,version_id
           ) VALUES (%s,%s,%s,'s3',%s,%s,%s,'application/json','sse-s3',%s)""",
        (
            tenant,
            source,
            artifact,
            f"objects/{object_hash[:2]}/{object_hash}",
            object_hash,
            len(text.encode()),
            f"version:{suffix}",
        ),
    )
    connection.execute(
        """INSERT INTO canonical_ingest_jobs(
               tenant_id,source_id,job_id,connector_id,mode,status
           ) VALUES (%s,%s,%s,'connector.e2e','backfill','committed')""",
        (tenant, source, job),
    )
    envelope = {
        "role": "assistant",
        "type": "message",
        "provenance": {"cwd": "/workspace/project", "branch": "main"},
        "content": {
            "type": "assistant",
            "message": {"role": "assistant", "text": text},
            "large_duplicate": text,
        },
    }
    connection.execute(
        """INSERT INTO canonical_events(
               tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,
               job_id,kind,content_sha256,revision,occurred_at,observed_at,
               canonical_redacted
           ) VALUES (%s,%s,%s,%s,%s,%s,%s,'transcript_record',%s,1,
                     now(),now(),%s)""",
        (
            tenant,
            source,
            event,
            native,
            parent,
            artifact,
            job,
            content_hash,
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
            artifact,
            native,
            content_hash,
            text,
            digest(text),
        ),
    )
    for ordinal, chunk_text in enumerate(canonical_text_chunks(text)):
        if corrupt_chunks and ordinal == 0:
            chunk_text += " corrupt"
        chunk_hash = digest(f"{document}:{ordinal}")
        connection.execute(
            """INSERT INTO canonical_chunks(
                   tenant_id,source_id,chunk_id,document_id,ordinal,receipt,
                   text_redacted,text_sha256
               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                tenant,
                source,
                "chk_" + chunk_hash[:32],
                document,
                ordinal,
                f"recall://{source}/{native}?rev=1#item={ordinal}",
                chunk_text,
                digest(chunk_text),
            ),
        )
    manifest_hash = digest(f"manifest:{suffix}")
    connection.execute(
        """INSERT INTO canonical_evidence_documents(
               tenant_id,source_id,logical_document_id,native_parent_id,revision,
               evidence_id,manifest_artifact_id,manifest_storage_backend,
               manifest_object_key,manifest_content_sha256,manifest_size_bytes,
               manifest_media_type,manifest_encryption,manifest_version_id,
               document_content_sha256,record_count,receipt_count,part_count,
               first_occurred_at,last_occurred_at,source_updated_at
           ) VALUES (%s,%s,%s,%s,1,%s,%s,'s3',%s,%s,1,
                     'application/vnd.recall.logical-document-manifest+json',
                     'sse-s3',%s,%s,1,1,1,now(),now(),now())""",
        (
            tenant,
            source,
            "ldoc_" + digest(f"logical:{suffix}")[:32],
            parent,
            "evd_" + digest(f"evidence:{suffix}")[:32],
            "art_" + manifest_hash[:32],
            f"objects/{manifest_hash[:2]}/{manifest_hash}",
            manifest_hash,
            f"manifest-version:{suffix}",
            digest(text),
        ),
    )
    return event, document


def main() -> None:
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_canonical_thin_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    test_dsn = make_conninfo(**settings)
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')

    store = BrainStore(test_dsn)
    try:
        store.migrate()
        nonce = uuid.uuid4().hex
        tenant = f"tenant:e2e:{nonce}"
        principal = f"principal:e2e:{nonce}"
        source = f"codex:e2e:{nonce}"
        text = ("complete canonical body " * 2_000).strip()
        with store.connect() as connection:
            good_event, good_document = insert_document(
                connection,
                tenant=tenant,
                principal=principal,
                source=source,
                suffix="good",
                text=text,
            )
            _bad_event, bad_document = insert_document(
                connection,
                tenant=tenant,
                principal=principal,
                source=source,
                suffix="bad",
                text=text,
                corrupt_chunks=True,
            )

        report = thin_canonical_bodies(
            store,
            tenant_id=tenant,
            batch_size=10,
            max_batches=1,
        )
        assert report["documents"] == 1
        assert report["events"] == 1
        assert report["document_bytes_removed"] == len(text.encode())
        assert report["event_bytes_replaced"] > len(text.encode())

        with store.connect() as connection:
            good = connection.execute(
                """SELECT document.text_redacted,document.text_sha256,
                          document.body_location AS document_body_location,
                          event.canonical_redacted,
                          event.body_location AS event_body_location
                     FROM canonical_documents document
                     JOIN canonical_events event USING(tenant_id,source_id,event_id)
                    WHERE document.tenant_id=%s AND document.source_id=%s
                      AND document.document_id=%s""",
                (tenant, source, good_document),
            ).fetchone()
            bad = connection.execute(
                """SELECT text_redacted,body_location
                     FROM canonical_documents
                    WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                (tenant, source, bad_document),
            ).fetchone()
            rebuilt = connection.execute(
                """SELECT string_agg(text_redacted,'' ORDER BY ordinal) AS text
                     FROM canonical_chunks
                    WHERE tenant_id=%s AND source_id=%s AND document_id=%s""",
                (tenant, source, good_document),
            ).fetchone()["text"]
        assert good["text_redacted"] == ""
        assert good["text_sha256"] == digest(text)
        assert good["document_body_location"] == "chunks"
        assert good["event_body_location"] == "raw"
        assert good["canonical_redacted"]["provenance"] == {
            "cwd": "/workspace/project",
            "branch": "main",
        }
        assert good["canonical_redacted"]["content"]["type"] == "assistant"
        assert (
            good["canonical_redacted"]["content"]["message"]["role"]
            == "assistant"
        )
        assert "large_duplicate" not in good["canonical_redacted"]["content"]
        assert rebuilt == text
        assert bad["text_redacted"] == text
        assert bad["body_location"] == "inline"

        # A later session update must still be able to rebuild the immutable
        # logical document from the one retained chunk body copy.
        with store.connect() as connection:
            connection.execute(
                """DELETE FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s
                      AND native_parent_id='session:good'""",
                (tenant, source),
            )
            connection.execute(
                """INSERT INTO canonical_evidence_document_queue(
                       tenant_id,source_id,native_parent_id,reason
                   ) VALUES (%s,%s,'session:good','backfill')""",
                (tenant, source),
            )
        with tempfile.TemporaryDirectory() as archive_root:
            archive = FilesystemArchiveStore(
                root=Path(archive_root), namespace_key=b"t" * 32,
            )
            projection = LogicalEvidenceProjectionStore(archive)
            projected = CanonicalLogicalEvidenceProjector(
                store,
                projection,
                bound_tenant_id=tenant,
                raw_archive=archive,
            ).project_pending(
                tenant_id=tenant,
                batch_size=1,
                max_batches=1,
                upload_concurrency=1,
            )
            assert projected["documents"] == 1
            with store.connect() as connection:
                part = connection.execute(
                    """SELECT part.*
                         FROM canonical_evidence_document_parts part
                         JOIN canonical_evidence_documents document
                           USING(tenant_id,source_id,logical_document_id,revision)
                        WHERE document.tenant_id=%s AND document.source_id=%s
                          AND document.native_parent_id='session:good'
                        ORDER BY part.part_ordinal LIMIT 1""",
                    (tenant, source),
                ).fetchone()
            payload = projection.read_part(
                CanonicalLogicalEvidenceProjector._reference(dict(part)),
                tenant_id=tenant,
                source_id=source,
            )
            assert text in payload.decode()
        print(json.dumps({
            "status": "pass",
            "lossless_body_rebuilt": True,
            "logical_reprojection_after_thinning": True,
            "corrupt_chunks_refused": True,
            "object_backed_metadata_preserved": True,
            "event_id": good_event,
        }, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
