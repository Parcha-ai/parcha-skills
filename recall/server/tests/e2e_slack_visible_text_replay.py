#!/usr/bin/env python3
"""Real canonical replay recovers omitted Slack prose at the same native identity."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]

from connectors.slack_source import normalize_slack_message  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402


def main():
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant, source, principal = (
        f"{kind}:slack-replay:{nonce}" for kind in ("tenant", "source", "principal")
    )
    message = {
        "ts": "1784332800.000100",
        "user": "U111",
        "text": "",
        "attachments": [
            {
                "pretext": "Gateway release blocked",
                "fields": [
                    {"title": "Cause", "value": "Archive permission regression"}
                ],
            }
        ],
    }
    recovered = normalize_slack_message(
        workspace_id="T123", channel_id="C123", value=message
    )
    old_content = {**recovered.content, "text": ""}
    with tempfile.TemporaryDirectory(prefix="recall-slack-replay-") as tmp:
        archive = FilesystemArchiveStore(Path(tmp), namespace_key=b"s" * 32)
        gateway = CanonicalArchiveGateway(
            store, archive, tenant_id=tenant, principal_id=principal
        )
        plane = CanonicalPlane(store, archive)
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive
        )
        passages = CanonicalPassageProjector(
            store, logical_store, policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=tenant
        )

        def ingest(content):
            payload = canonical_json(content)
            artifact = gateway.put_raw(
                tenant_id=tenant,
                source_id=source,
                native_id=recovered.native_id,
                payload=payload,
                media_type="application/json",
                created_at=recovered.occurred_at,
            )
            envelope = {
                "schema_version": 1,
                "source_id": source,
                "native_id": recovered.native_id,
                "native_parent_id": recovered.native_parent_id,
                "kind": "connector_record",
                "occurred_at": recovered.occurred_at,
                "observed_at": recovered.occurred_at,
                "principal_id": principal,
                "visibility": "private",
                "content_type": "application/json",
                "content": content,
                "provenance": {
                    "connector_id": "slack.messages",
                    "connector_schema_version": 2,
                    "artifact_ref": artifact,
                },
                "content_sha256": hashlib.sha256(payload).hexdigest(),
            }
            return plane.ingest_document(
                tenant_id=tenant,
                principal_id=principal,
                connector_id="slack.messages",
                artifact_ref=artifact,
                envelope=envelope,
                text_redacted=payload.decode(),
            )

        def project():
            logical.project_pending(
                tenant_id=tenant, batch_size=2, max_batches=1, upload_concurrency=1
            )
            return passages.project_pending(
                tenant_id=tenant, batch_size=2, max_batches=1, concurrency=1
            )

        assert ingest(old_content)["inserted"] == 1
        assert project()["passages"] == 0
        with store.connect() as con:
            before = con.execute(
                "SELECT logical_document_id,revision FROM canonical_evidence_documents WHERE tenant_id=%s",
                (tenant,),
            ).fetchone()
        assert ingest(recovered.content)["inserted"] == 1
        result = project()
        assert result["passages"] == 1, result
        with store.connect() as con:
            after = con.execute(
                "SELECT logical_document_id,revision FROM canonical_evidence_documents WHERE tenant_id=%s",
                (tenant,),
            ).fetchone()
            assert after["logical_document_id"] == before["logical_document_id"]
            assert after["revision"] > before["revision"]
            rows = con.execute(
                "SELECT text_redacted FROM canonical_passages WHERE tenant_id=%s",
                (tenant,),
            ).fetchall()
            assert [row["text_redacted"] for row in rows] == [
                "Gateway release blocked\nCause: Archive permission regression"
            ]
            assert (
                con.execute(
                    "SELECT count(*) AS n FROM canonical_documents WHERE tenant_id=%s AND is_current",
                    (tenant,),
                ).fetchone()["n"]
                == 1
            )
        assert ingest(recovered.content)["inserted"] == 0
        assert project()["documents"] == 0
    store.close()
    print(
        json.dumps(
            {
                "same_native_identity": True,
                "same_logical_document": True,
                "recovered_passages": 1,
                "repeat_replay_inserts": 0,
            }
        )
    )


if __name__ == "__main__":
    main()
