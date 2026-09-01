#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
RECALL = SERVER.parent
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(RECALL))

from recall_server.cli import (  # noqa: E402
    _EMPTY_LEGACY_RELATIONS,
    _discard_covered_legacy_storage,
)
from recall_server.db import BrainStore  # noqa: E402


def main() -> None:
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_legacy_cut_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    test_dsn = make_conninfo(**settings)

    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')

    store = BrainStore(test_dsn)
    try:
        store.migrate()
        source = "source:e2e:legacy-cut"
        native = "native:e2e:legacy-cut"
        content_hash = hashlib.sha256(b"legacy cut payload").hexdigest()
        batch = uuid.uuid4()
        tenant = "tenant:e2e:legacy-cut"
        principal = "principal:e2e:legacy-cut"
        artifact = "art_e2e_legacy_cut"
        job = "job_e2e_legacy_cut"
        event = "evt_e2e_legacy_cut"
        object_key = f"objects/{content_hash[:2]}/{content_hash}"

        with store.connect() as connection:
            connection.execute(
                "INSERT INTO sources(id,principal_id) VALUES (%s,%s)",
                (source, principal),
            )
            connection.execute(
                """INSERT INTO ingest_batches(
                       id,idempotency_key,request_sha256,status,acknowledgement
                   ) VALUES (%s,%s,%s,'committed','{}'::jsonb)""",
                (batch, "legacy-cut", content_hash),
            )
            connection.execute(
                """INSERT INTO source_events(
                       source_id,native_id,kind,occurred_at,observed_at,principal_id,
                       visibility,content_type,content_sha256,revision,envelope,batch_id
                   ) VALUES (%s,%s,'document',now(),now(),%s,'private',
                             'application/json',%s,1,'{}'::jsonb,%s)""",
                (source, native, principal, content_hash, batch),
            )

        try:
            _discard_covered_legacy_storage(store)
        except ValueError as error:
            assert str(error) == "legacy source events are not fully S3-canonical-covered"
        else:
            raise RuntimeError("uncovered legacy row was discarded")

        with store.connect() as connection:
            connection.execute(
                "INSERT INTO brain_tenants(tenant_id) VALUES (%s)",
                (tenant,),
            )
            connection.execute(
                """INSERT INTO brain_principals(tenant_id,principal_id)
                   VALUES (%s,%s)""",
                (tenant, principal),
            )
            connection.execute(
                """INSERT INTO canonical_sources(
                       tenant_id,source_id,owner_principal_id
                   ) VALUES (%s,%s,%s)""",
                (tenant, source, principal),
            )
            connection.execute(
                """INSERT INTO raw_artifacts(
                       tenant_id,source_id,artifact_id,storage_backend,object_key,
                       content_sha256,size_bytes,media_type,encryption,version_id
                   ) VALUES (%s,%s,%s,'s3',%s,%s,18,'application/json',
                             'sse-s3','version-1')""",
                (tenant, source, artifact, object_key, content_hash),
            )
            connection.execute(
                """INSERT INTO canonical_ingest_jobs(
                       tenant_id,source_id,job_id,connector_id,mode,status
                   ) VALUES (%s,%s,%s,'connector.e2e','backfill','committed')""",
                (tenant, source, job),
            )
            connection.execute(
                """INSERT INTO canonical_events(
                       tenant_id,source_id,event_id,native_id,artifact_id,job_id,kind,
                       content_sha256,revision,occurred_at,observed_at,canonical_redacted
                   ) VALUES (%s,%s,%s,%s,%s,%s,'document',%s,1,now(),now(),
                             '{}'::jsonb)""",
                (tenant, source, event, native, artifact, job, content_hash),
            )

        report = _discard_covered_legacy_storage(store)
        assert report["coverage"] == {"total": 1, "canonical_covered": 1}
        with store.connect() as connection:
            counts = {
                relation: int(
                    connection.execute(
                        f'SELECT count(*) AS count FROM public."{relation}"'
                    ).fetchone()["count"]
                )
                for relation in _EMPTY_LEGACY_RELATIONS
            }
            canonical_count = int(
                connection.execute(
                    "SELECT count(*) AS count FROM canonical_events"
                ).fetchone()["count"]
            )
        assert set(counts.values()) == {0}
        assert canonical_count == 1
        assert "content" not in json.dumps(report)
        print(json.dumps({
            "status": "pass",
            "uncovered_refused": True,
            "covered_discarded": True,
            "canonical_preserved": True,
        }, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
