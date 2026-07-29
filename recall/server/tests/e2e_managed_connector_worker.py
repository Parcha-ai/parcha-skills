#!/usr/bin/env python3
"""Fresh-PostgreSQL E2E for web-controlled managed remote ingestion."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "recall"), str(ROOT / "recall/server")]

from recall_server.archive import FilesystemArchiveStore
from recall_server.control import SecretBox
from recall_server.db import BrainStore
from recall_server.managed_worker import ManagedConnectorWorker


OWNER = "principal:owner:managed-worker-e2e"
PERSONAL = "tenant:personal:managed-worker-e2e"
COMPANY = "tenant:company:managed-worker-e2e"
COMPANY_SOURCE = "google-gmail:company:managed-worker-e2e"
PERSONAL_SOURCE = "google-gmail:personal:managed-worker-e2e"
SECRET = "synthetic-managed-worker-refresh-secret"


class FakeSemanticRuntime:
    dimensions = 512
    model = "synthetic-managed-worker-embedding"
    fingerprint = "synthetic-managed-worker-runtime"

    def embed_documents(self, texts):
        return [[1.0] + [0.0] * 511 for _text in texts]

    def embed_query(self, _query):
        return [1.0] + [0.0] * 511


class GmailRail:
    def run(self, operation, params):
        if operation == "gmail.messages.list":
            if params.get("pageToken") == "page-2":
                return {"messages": [{"id": "managed-message-2"}]}
            return {
                "messages": [{"id": "managed-message-1"}],
                "nextPageToken": "page-2",
            }
        if operation == "gmail.messages.get":
            message_id = params["id"]
            if message_id == "managed-message-1":
                payload = {
                    "mimeType": "multipart/alternative",
                    "headers": [
                        {"name": "From", "value": "teammate@example.invalid"},
                        {"name": "To", "value": "owner@example.invalid"},
                        {"name": "Subject", "value": "managed company launch marker"},
                    ],
                    "body": {"size": 0},
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "filename": "",
                            "headers": [],
                            "body": {
                                "attachmentId": "managed-body-1",
                                "size": 82,
                            },
                        },
                        {
                            "mimeType": "text/html",
                            "filename": "",
                            "headers": [],
                            "body": {"data": base64.urlsafe_b64encode(
                                b"<p>duplicate html alternative</p>"
                            ).decode().rstrip("=")},
                        },
                    ],
                }
            else:
                body = base64.urlsafe_b64encode(
                    b"<html><body><h1>managed html body marker</h1>"
                    b"<p>api_key=synthetic-managed-content-secret</p>"
                    b"<script>hidden tracker</script></body></html>"
                ).decode().rstrip("=")
                payload = {
                    "mimeType": "text/html",
                    "headers": [
                        {"name": "From", "value": "teammate@example.invalid"},
                        {"name": "To", "value": "owner@example.invalid"},
                        {"name": "Subject", "value": "managed html message"},
                    ],
                    "body": {"data": body},
                }
            return {
                "id": message_id,
                "threadId": "managed-thread-1",
                "historyId": "700",
                "internalDate": "1784505600000",
                "payload": payload,
            }
        if operation == "gmail.messages.attachments.get":
            assert params == {
                "userId": "me",
                "messageId": "managed-message-1",
                "id": "managed-body-1",
            }
            body = (
                b"managed external complete body marker "
                b"api_key=synthetic-managed-content-secret"
            )
            return {
                "size": len(body),
                "data": base64.urlsafe_b64encode(body).decode().rstrip("="),
            }
        if operation == "gmail.history.list":
            return {"history": [], "historyId": "700"}
        raise AssertionError("unexpected synthetic Google operation")

    def export_document(self, *, file_id, mime_type="text/plain"):
        raise AssertionError("Gmail fixture does not export documents")


def provision(store):
    store.provision_brain(
        organization_id="org:personal:managed-worker-e2e",
        organization_kind="personal",
        display_name="Synthetic Personal",
        tenant_id=PERSONAL,
        brain_kind="personal",
        slug="personal-managed-worker",
        owner_principal_id=OWNER,
    )
    store.provision_brain(
        organization_id="org:company:managed-worker-e2e",
        organization_kind="company",
        display_name="Synthetic Company",
        tenant_id=COMPANY,
        brain_kind="company",
        slug="company-managed-worker",
        owner_principal_id=OWNER,
    )


def connection(store, box):
    connection_id = uuid.uuid4()
    credentials = {
        "type": "authorized_user",
        "client_id": "synthetic-client-id",
        "client_secret": "synthetic-client-secret",
        "refresh_token": SECRET,
        "token_uri": "https://oauth2.googleapis.com/token",
        "access_token": "synthetic-access-token",
        "token_type": "Bearer",
    }
    encrypted = box.seal(
        credentials,
        purpose=f"provider-connection:{connection_id}",
    )
    assert SECRET.encode() not in encrypted
    with store.connect() as database:
        database.execute(
            """INSERT INTO provider_connections(
                   id,principal_id,provider,subject_id,status,granted_scopes,
                   encrypted_credentials,encryption_key_id,expires_at
               ) VALUES (%s,%s,'google','synthetic-subject','connected',
                         ARRAY['https://www.googleapis.com/auth/gmail.readonly'],
                         %s,%s,%s)""",
            (
                connection_id,
                OWNER,
                encrypted,
                box.key_id,
                datetime.now(timezone.utc) + timedelta(hours=1),
            ),
        )
    return connection_id


def install(
    store,
    connection_id,
    *,
    tenant_id,
    source_id,
    state,
):
    installation_id = uuid.uuid4()
    with store.connect() as database:
        database.execute(
            """INSERT INTO connector_installations(
                   id,tenant_id,principal_id,connector_id,source_id,
                   connection_id,execution,state,privacy_mode,selectors
               ) VALUES (%s,%s,%s,'google.gmail',%s,%s,'remote_worker',
                         %s,'scrub',%s::jsonb)""",
            (
                installation_id,
                tenant_id,
                OWNER,
                source_id,
                connection_id,
                state,
                json.dumps(
                    {
                        "own_addresses": ["owner@example.invalid"],
                    }
                ),
            ),
        )
    return installation_id


def force_due(store, installation_id):
    with store.connect() as database:
        database.execute(
            """UPDATE connector_installations
               SET run_after=now()-interval '1 second',
                   lease_owner=NULL,lease_expires_at=NULL
               WHERE id=%s""",
            (installation_id,),
        )


def main():
    store = BrainStore(
        os.environ["RECALL_DATABASE_URL"],
        semantic_runtime=FakeSemanticRuntime(),
    )
    store.migrate()
    with store.connect() as database:
        database.execute(
            """TRUNCATE brain_tenants,brain_organizations,
                        provider_connections,control_audit_events
               RESTART IDENTITY CASCADE"""
        )
    provision(store)
    box = SecretBox(b"w" * 32)
    connection_id = connection(store, box)
    company_installation = install(
        store,
        connection_id,
        tenant_id=COMPANY,
        source_id=COMPANY_SOURCE,
        state="enabled",
    )
    install(
        store,
        connection_id,
        tenant_id=PERSONAL,
        source_id=PERSONAL_SOURCE,
        state="paused",
    )

    with tempfile.TemporaryDirectory(
        prefix="recall-managed-worker-e2e-"
    ) as directory:
        root = Path(directory)
        os.chmod(root, 0o700)
        archive = FilesystemArchiveStore(
            root / "archive",
            namespace_key=b"a" * 32,
        )
        worker = ManagedConnectorWorker(
            store,
            archive,
            box,
            state_root=root / "state",
            worker_id="worker:managed-e2e",
            remote_rails={"google.gmail": GmailRail()},
            interval_seconds=60,
            embedding_max_batches=3,
        )
        embedding_calls = []
        embed_pending = worker.retrieval.embed_pending

        def observed_embed_pending(**kwargs):
            embedding_calls.append(dict(kwargs))
            return embed_pending(**kwargs)

        worker.retrieval.embed_pending = observed_embed_pending
        first = worker.run_once()
        assert first == {
            "schema_version": 1,
            "status": "committed",
            "processed": 1,
            "committed": 1,
            "failed": 0,
            "acked": 1,
            "staged": 1,
            "embedded": 1,
            "has_more": True,
        }
        assert embedding_calls[0].get("tenant_id") is None
        assert embedding_calls[0]["max_batches"] == 3
        assert SECRET not in json.dumps(first)

        with store.connect() as database:
            due_in = database.execute(
                """SELECT extract(epoch FROM (run_after-now())) AS seconds
                   FROM connector_installations WHERE id=%s""",
                (company_installation,),
            ).fetchone()["seconds"]
        assert 0 <= float(due_in) <= 2

        force_due(store, company_installation)
        second = worker.run_once()
        assert second["status"] == "committed"
        assert second["acked"] == 1 and second["has_more"] is False

        idle = worker.run_once()
        assert idle["status"] == "idle" and idle["processed"] == 0

        with store.connect() as database:
            by_tenant = database.execute(
                """SELECT tenant_id,count(*) AS count
                   FROM canonical_events
                   GROUP BY tenant_id ORDER BY tenant_id"""
            ).fetchall()
            embeddings = database.execute(
                """SELECT tenant_id,count(*) AS count
                   FROM canonical_chunk_embeddings
                   GROUP BY tenant_id ORDER BY tenant_id"""
            ).fetchall()
            route = database.execute(
                """SELECT last_success_at,last_error_code,failure_count,
                          lease_owner,lease_expires_at
                   FROM connector_installations WHERE id=%s""",
                (company_installation,),
            ).fetchone()
            canonical = [
                row["canonical_redacted"]
                for row in database.execute(
                    """SELECT canonical_redacted
                       FROM canonical_events
                       WHERE tenant_id=%s AND source_id=%s
                       ORDER BY native_id""",
                    (COMPANY, COMPANY_SOURCE),
                ).fetchall()
            ]
            rendered = json.dumps(canonical)
            searchable = "\n".join(
                row["text_redacted"]
                for row in database.execute(
                    """SELECT text_redacted FROM canonical_chunks
                       WHERE tenant_id=%s AND source_id=%s
                         AND deleted_at IS NULL""",
                    (COMPANY, COMPANY_SOURCE),
                ).fetchall()
            )
        assert [(row["tenant_id"], row["count"]) for row in by_tenant] == [
            (COMPANY, 2)
        ]
        assert [(row["tenant_id"], row["count"]) for row in embeddings] == [
            (COMPANY, 2)
        ]
        assert (
            route["last_success_at"] is not None
            and route["last_error_code"] is None
            and route["failure_count"] == 0
            and route["lease_owner"] is None
            and route["lease_expires_at"] is None
        )
        assert "synthetic-managed-content-secret" not in rendered
        assert "managed external complete body marker" in searchable
        assert "managed html body marker" in searchable
        assert "duplicate html alternative" not in searchable
        assert all(
            event["content"]["content_fidelity"] == "complete"
            for event in canonical
        )

        force_due(store, company_installation)
        replay = worker.run_once()
        assert replay["status"] == "committed"
        with store.connect() as database:
            assert database.execute(
                """SELECT count(*) AS count FROM canonical_events
                   WHERE tenant_id=%s AND source_id=%s""",
                (COMPANY, COMPANY_SOURCE),
            ).fetchone()["count"] == 2

        with store.connect() as database:
            database.execute(
                """UPDATE connector_installations
                   SET state='paused',run_after=now()-interval '1 second'
                   WHERE id=%s""",
                (company_installation,),
            )
        paused = worker.run_once()
        assert paused["status"] == "idle"

    print(
        json.dumps(
            {
                "status": "pass",
                "managed_jobs_committed": 3,
                "canonical_company_events": 2,
                "canonical_personal_events": 0,
                "canonical_company_embeddings": 2,
                "duplicate_events_after_replay": 0,
                "paused_jobs_executed": 0,
                "plaintext_provider_credentials_at_rest": 0,
                "rendered_secret_canaries": 0,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
