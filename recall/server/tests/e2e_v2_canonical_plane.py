#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg

SERVER = Path(__file__).resolve().parents[1]
RECALL = SERVER.parent
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(RECALL))

from recall_server.db import BrainStore
from recall_server.cli import _storage_authority_audit


def expect_sqlstate(connection, sql: str, parameters: tuple, expected: str) -> None:
    try:
        with connection.transaction():
            connection.execute(sql, parameters)
    except psycopg.Error as error:
        if error.sqlstate != expected:
            raise
    else:
        raise RuntimeError("canonical plane accepted an invalid write")


def insert_source(connection, tenant: str, principal: str, source: str) -> None:
    connection.execute(
        "INSERT INTO brain_tenants(tenant_id) VALUES (%s)",
        (tenant,),
    )
    connection.execute(
        "INSERT INTO brain_principals(tenant_id,principal_id) VALUES (%s,%s)",
        (tenant, principal),
    )
    connection.execute(
        """INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id)
           VALUES (%s,%s,%s)""",
        (tenant, source, principal),
    )


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(dsn)
    store.migrate()
    empty_audit = _storage_authority_audit(
        store,
        "tenant:e2e:authority-audit-empty",
    )
    assert empty_audit["status"] == "ok"
    assert empty_audit["canonical_documents"]["total"] == 0
    nonce = uuid.uuid4().hex
    tenant_a = f"tenant:e2e:{nonce}:a"
    tenant_b = f"tenant:e2e:{nonce}:b"
    principal = f"principal:e2e:{nonce}"
    source = f"source:e2e:{nonce}"
    content_sha256 = hashlib.sha256(b"synthetic canonical payload").hexdigest()
    text_sha256 = hashlib.sha256(b"synthetic canonical text").hexdigest()
    identity_sha256 = hashlib.sha256(b"synthetic deleted identity").hexdigest()
    artifacts = {
        tenant_a: "art_" + nonce + "a",
        tenant_b: "art_" + nonce + "b",
    }
    job = "job_" + nonce
    event = "evt_" + nonce
    document = "doc_" + nonce
    chunk = "chk_" + nonce
    object_keys = {
        tenant: "objects/" + hashlib.sha256(tenant.encode()).hexdigest()[:2]
        + "/" + hashlib.sha256((tenant + nonce).encode()).hexdigest()
        for tenant in (tenant_a, tenant_b)
    }

    with psycopg.connect(dsn) as connection:
        with connection.transaction(force_rollback=True):
            insert_source(connection, tenant_a, principal, source)
            insert_source(connection, tenant_b, principal, source)
            for tenant in (tenant_a, tenant_b):
                artifact = artifacts[tenant]
                connection.execute(
                    """INSERT INTO canonical_ingest_jobs(
                           tenant_id,source_id,job_id,connector_id,mode,status
                       ) VALUES (%s,%s,%s,%s,'incremental','committed')""",
                    (tenant, source, job, "connector.synthetic"),
                )
                connection.execute(
                    """INSERT INTO raw_artifacts(
                           tenant_id,source_id,artifact_id,storage_backend,object_key,
                           content_sha256,size_bytes,media_type,encryption,version_id
                       ) VALUES (%s,%s,%s,'s3',%s,%s,27,'application/json','sse-s3',%s)""",
                    (
                        tenant,
                        source,
                        artifact,
                        object_keys[tenant],
                        content_sha256,
                        "version-1",
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_events(
                           tenant_id,source_id,event_id,native_id,artifact_id,job_id,kind,
                           content_sha256,revision,occurred_at,observed_at,canonical_redacted
                       ) VALUES (%s,%s,%s,'native:synthetic',%s,%s,'document',%s,1,
                                 '2026-07-19T00:00:00Z','2026-07-19T00:00:01Z',%s)""",
                    (
                        tenant, source, event, artifact, job, content_sha256,
                        json.dumps({
                            "kind": "document.v1",
                            "content_fidelity": "complete",
                        }),
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_documents(
                           tenant_id,source_id,document_id,event_id,artifact_id,native_id,
                           content_sha256,revision,is_current,text_redacted,text_sha256
                       ) VALUES (%s,%s,%s,%s,%s,'native:synthetic',%s,1,true,
                                 'synthetic canonical text',%s)""",
                    (
                        tenant, source, document, event, artifact,
                        content_sha256, text_sha256,
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_chunks(
                           tenant_id,source_id,chunk_id,document_id,ordinal,receipt,
                           text_redacted,text_sha256
                       ) VALUES (%s,%s,%s,%s,0,%s,'synthetic canonical text',%s)""",
                    (
                        tenant, source, chunk, document,
                        f"recall://{source}/native-synthetic?rev=1#item=0",
                        text_sha256,
                    ),
                )
                connection.execute(
                    """INSERT INTO forget_tombstones(
                           tenant_id,source_id,target_identity_sha256,mode,reason,
                           deleted_at,status,completed_at
                       ) VALUES (%s,%s,%s,'explicit_forget','owner_requested',
                                 now(),'deleted',now())""",
                    (tenant, source, identity_sha256),
                )
                connection.execute(
                    """INSERT INTO receipt_redirects(
                           tenant_id,source_id,old_receipt,new_receipt,reason
                       ) VALUES (%s,%s,%s,%s,'v2_migration')""",
                    (
                        tenant,
                        source,
                        f"recall://{source}/legacy?rev=1",
                        f"recall://{source}/native-synthetic?rev=1",
                    ),
                )
                connection.execute(
                    """INSERT INTO canonical_audit_events(
                           tenant_id,source_id,audit_id,operation,status,item_count
                       ) VALUES (%s,%s,%s,'ingest.commit','success',1)""",
                    (tenant, source, "audit_" + nonce),
                )

            expect_sqlstate(
                connection,
                """INSERT INTO canonical_events(
                       tenant_id,source_id,event_id,native_id,artifact_id,job_id,kind,
                       content_sha256,revision,occurred_at,observed_at,canonical_redacted
                   ) VALUES (%s,%s,%s,'native:escape',%s,%s,'document',%s,1,
                             now(),now(),'{}'::jsonb)""",
                (
                    tenant_a, source, "evt_cross_tenant",
                    artifacts[tenant_b], job, content_sha256,
                ),
                "23503",
            )
            expect_sqlstate(
                connection,
                """INSERT INTO canonical_events(
                       tenant_id,source_id,event_id,native_id,artifact_id,job_id,kind,
                       content_sha256,revision,occurred_at,observed_at,canonical_redacted
                   ) VALUES (%s,%s,'evt_duplicate','native:synthetic',%s,%s,'document',
                             %s,1,now(),now(),'{}'::jsonb)""",
                (tenant_a, source, artifacts[tenant_a], job, content_sha256),
                "23505",
            )
            expect_sqlstate(
                connection,
                """UPDATE raw_artifacts SET state='deleted'
                   WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s""",
                (tenant_a, source, artifacts[tenant_a]),
                "23514",
            )
            expect_sqlstate(
                connection,
                """INSERT INTO canonical_audit_events(
                       tenant_id,source_id,audit_id,operation,status,duration_ms
                   ) VALUES (%s,%s,'audit_nonfinite','ingest.commit','success','NaN')""",
                (tenant_a, source),
                "23514",
            )
            counts = connection.execute(
                """SELECT
                     (SELECT count(*) FROM canonical_events WHERE source_id=%s) AS events,
                     (SELECT count(*) FROM raw_artifacts WHERE source_id=%s) AS artifacts,
                     (SELECT count(*) FROM forget_tombstones WHERE source_id=%s) AS tombstones,
                     (SELECT count(*) FROM receipt_redirects WHERE source_id=%s) AS redirects""",
                (source, source, source, source),
            ).fetchone()
            if tuple(counts) != (2, 2, 2, 2):
                raise RuntimeError("canonical tenant isolation count mismatch")

    print(json.dumps({
        "status": "pass",
        "tenants": 2,
        "canonical_events": 2,
        "artifact_references": 2,
        "tombstones": 2,
        "cross_tenant_rejections": 1,
        "duplicate_rejections": 1,
        "invalid_delete_state_rejections": 1,
        "nonfinite_audit_rejections": 1,
        "receipt_redirects": 2,
        "rolled_back": True,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
