#!/usr/bin/env python3
"""Real PG/archive Slack projection and scoped empty-document repair; fake search API."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]

from connectors.slack_source import normalize_slack_message  # noqa: E402
from recall_server.actor_attribution import ActorIdentityIndex  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.control import SecretBox  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.parquet_scan import CanonicalParquetScanProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402
from recall_server.turbopuffer_plane import build_client, turbopuffer_settings_from_env  # noqa: E402
from recall_server.turbopuffer_projection import drain_search_outbox  # noqa: E402


def main():
    dsn = os.environ["RECALL_DATABASE_URL"]
    store = BrainStore(dsn)
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant, source, principal = (
        f"{kind}:slack-visibility:{nonce}" for kind in ("tenant", "source", "principal")
    )
    text = "The gateway preserved the tenant boundary during the archive failure."
    record = normalize_slack_message(
        workspace_id="T123",
        channel_id="C123",
        value={
            "ts": "1784332800.000100",
            "user": "U111",
            "text": text,
        },
    )
    with tempfile.TemporaryDirectory(prefix="recall-slack-visible-") as tmp:
        archive = FilesystemArchiveStore(Path(tmp) / "archive", namespace_key=b"s" * 32)
        gateway = CanonicalArchiveGateway(
            store, archive, tenant_id=tenant, principal_id=principal
        )
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive
        )
        plane = CanonicalPlane(
            store,
            archive,
            actor_identity_index=ActorIdentityIndex(SecretBox(b"i" * 32).blind_index),
        )
        artifact = gateway.put_raw(
            tenant_id=tenant,
            source_id=source,
            native_id=record.native_id,
            payload=canonical_json(record.content),
            media_type="application/json",
            created_at=record.occurred_at,
        )
        envelope = {
            "schema_version": 1,
            "source_id": source,
            "native_id": record.native_id,
            "native_parent_id": record.native_parent_id,
            "kind": "connector_record",
            "occurred_at": record.occurred_at,
            "observed_at": record.occurred_at,
            "principal_id": principal,
            "visibility": "private",
            "content_type": "application/json",
            "content": record.content,
            "provenance": {
                "connector_id": "slack.messages",
                "connector_schema_version": 2,
                "artifact_ref": artifact,
            },
            "content_sha256": hashlib.sha256(
                canonical_json(record.content)
            ).hexdigest(),
        }
        ingested = plane.ingest_document(
            tenant_id=tenant,
            principal_id=principal,
            connector_id="slack.messages",
            artifact_ref=artifact,
            envelope=envelope,
            text_redacted=canonical_json(record.content).decode(),
        )
        assert ingested["inserted"] == 1
        with store.connect() as con:
            assert (
                con.execute(
                    "SELECT count(*) AS n FROM canonical_event_actors WHERE tenant_id=%s",
                    (tenant,),
                ).fetchone()["n"]
                == 0
            )
        logical.seed_backfill(tenant_id=tenant)
        built = logical.project_pending(
            tenant_id=tenant, batch_size=2, max_batches=1, upload_concurrency=1
        )
        assert built["documents"] == 1, built
        passages = CanonicalPassageProjector(
            store, logical_store, policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=tenant
        )
        # Persist exactly the old failure: a valid logical document with no eligible messages.
        with patch("recall_server.passage_index.visible_messages", return_value=()):
            old = passages.project_pending(
                tenant_id=tenant, batch_size=2, max_batches=1, concurrency=1
            )
        assert old["documents"] == 1 and old["passages"] == 0, old
        assert passages.seed_backfill(tenant_id=tenant) == 0
        with store.connect() as con:
            before = dict(
                con.execute(
                    "SELECT logical_document_id,revision,source_document_sha256,policy_fingerprint FROM canonical_passage_documents WHERE tenant_id=%s",
                    (tenant,),
                ).fetchone()
            )
            # Narrow repair fixture only: no production command or global policy invalidation.
            queued = con.execute(
                """INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                SELECT evidence.tenant_id,evidence.source_id,evidence.logical_document_id,
                       evidence.revision,1,'backfill',clock_timestamp()
                  FROM canonical_evidence_documents evidence
                  JOIN canonical_passage_documents projected USING(tenant_id,source_id,logical_document_id)
                 WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                   AND evidence.logical_document_id=%s AND projected.passage_count=0
                   AND projected.revision=evidence.revision
                   AND projected.source_document_sha256=evidence.document_content_sha256
                ON CONFLICT(tenant_id,source_id,logical_document_id) DO NOTHING""",
                (tenant, source, before["logical_document_id"]),
            )
            assert queued.rowcount == 1
        repaired = passages.project_pending(
            tenant_id=tenant, batch_size=2, max_batches=1, concurrency=1
        )
        assert repaired["documents"] == 1 and repaired["passages"] == 1, repaired
        with store.connect() as con:
            after = dict(
                con.execute(
                    "SELECT logical_document_id,revision,source_document_sha256,policy_fingerprint FROM canonical_passage_documents WHERE tenant_id=%s",
                    (tenant,),
                ).fetchone()
            )
            assert after == before
            rows = con.execute(
                "SELECT text_redacted,receipts FROM canonical_passages WHERE tenant_id=%s",
                (tenant,),
            ).fetchall()
            assert len(rows) == 1 and rows[0]["text_redacted"] == text
            assert ingested["receipt"] in rows[0]["receipts"]
            assert (
                con.execute(
                    "SELECT count(*) AS n FROM canonical_passage_actors WHERE tenant_id=%s",
                    (tenant,),
                ).fetchone()["n"]
                == 0
            )
        scan = CanonicalParquetScanProjector(store, logical_store)
        scan.seed_backfill(tenant_id=tenant)
        scan_result = scan.project_pending(
            tenant_id=tenant, batch_size=2, max_batches=1
        )
        assert scan_result["shards"] == 1, scan_result
        with patch.dict(
            os.environ,
            {
                "RECALL_TPUF_API_KEY": "synthetic",
                "RECALL_TPUF_CLIENT_FACTORY": "tests.central_brain.fake_turbopuffer:factory",
                "RECALL_TPUF_FAKE_STATE": str(Path(tmp) / "fake-search.json"),
                "RECALL_SEARCH_PLANE": "turbopuffer",
            },
        ):
            settings = turbopuffer_settings_from_env(required=True)
            drained = drain_search_outbox(
                store,
                settings,
                tenant_id=tenant,
                max_months=2,
                client=build_client(settings),
            )
            assert drained["rows"] == 1 and drained["failed"] == 0, drained
            reader = BrainStore(dsn)
            try:
                bound = BoundCanonicalRetrieval(
                    reader,
                    tenant_id=tenant,
                    principal_id=principal,
                    authorized_sources=(source,),
                    passage_policy=DEFAULT_PASSAGE_POLICY,
                )
                found = bound.passage_hints(
                    "gateway tenant boundary archive failure", limit=5
                )
                assert any(
                    hit["logical_document_id"] == before["logical_document_id"]
                    for hit in found["results"]
                ), found
            finally:
                reader.close()
        idle = passages.project_pending(
            tenant_id=tenant, batch_size=2, max_batches=1, concurrency=1
        )
        assert idle["documents"] == idle["passages"] == 0, idle
    store.close()
    print(
        json.dumps(
            {
                "unmapped_slack_searchable": True,
                "actor_links_invented": 0,
                "old_empty_seed_skipped": True,
                "targeted_repair_passages": 1,
                "logical_revision_and_policy_unchanged": True,
                "idle_rewrites": 0,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
