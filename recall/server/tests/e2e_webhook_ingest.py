#!/usr/bin/env python3
"""Fresh-PostgreSQL proof that v1 writes land on the canonical plane only (H1-T6).

Covers the generic webhook, the Slack Events webhook, and ``/v1/ingest/batches``:

* rows appear in ``canonical_events`` and ``raw_artifacts``, never in
  ``source_events`` / ``items`` / ``chunks``;
* receipts still resolve through ``/v1/receipts/resolve``;
* an exact replay is idempotent (``duplicate_events=1``, ``replay=true``, same
  receipt);
* the four legacy read routes answer ``410 Gone`` by default and ``200`` with
  ``RECALL_LEGACY_READS=1``;
* ``RECALL_LEGACY_WRITES=1`` restores the dual-write rollback path.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import sys
import tempfile
import threading
import time
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "recall"))
sys.path.insert(0, str(ROOT / "recall/server"))

from recall_server.app import Handler  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalPlane  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.legacy_plane import (  # noqa: E402
    LEGACY_DROPPABLE_TABLES,
    LEGACY_READ_REPLACEMENTS,
    LegacyIngestBridge,
)
from recall_server.projectors import canonical_json  # noqa: E402

TENANT = "tenant:e2e:webhook"
OWNER = "synthetic-owner"
WEBHOOK_SOURCE = "synthetic:webhook"
SLACK_SOURCE = "synthetic:slack"
SLACK_WORKSPACE = "T0E2E"
SLACK_SECRET = "synthetic-slack-signing-secret"
COLLECTOR_SOURCE = "codex:e2e-collector"
LEGACY_TRUNCATE = (
    "TRUNCATE collector_credentials,session_export_cursors,chunks,items,"
    "sessions,projection_watermarks,source_events,ingest_batches,"
    "source_grants,sources,dead_letters,audit_events,"
    "connector_installations,provider_connections,brain_access_grants,"
    "brain_memberships,brain_spaces,brain_organizations,"
    "canonical_chunks,canonical_documents,canonical_events,"
    "canonical_ingest_jobs,raw_artifacts,canonical_source_grants,"
    "canonical_sources,brain_principals,brain_tenants "
    "RESTART IDENTITY CASCADE"
)
ENV_KEYS = (
    "RECALL_AUTH_REQUIRED",
    "RECALL_HTTP_PROFILE",
    "RECALL_TRUST_TAILSCALE_HEADERS",
    "RECALL_SLACK_SIGNING_SECRET",
    "RECALL_LEGACY_WRITES",
    "RECALL_LEGACY_READS",
    "RECALL_LEGACY_INGEST_TENANT_ID",
)


def body(*, text: str = "synthetic cobalt webhook", deleted: bool = False) -> dict:
    record = {"kind": "communication_message.v1"}
    if not deleted:
        record.update({
            "content_fidelity": "complete",
            "conversation_id": "synthetic-conversation",
            "direction": "inbound",
            "message_id": "synthetic-event",
            "text": text,
        })
    return {
        "schema_version": 1,
        "event_id": "synthetic-event",
        "parent_id": "synthetic-conversation",
        "occurred_at": "2026-07-18T20:00:00Z",
        "record": record,
        "deleted": deleted,
    }


def legacy_envelope(native_id: str, text: str) -> dict:
    content = {"role": "user", "text": text}
    return {
        "schema_version": 1,
        "source_id": COLLECTOR_SOURCE,
        "native_id": native_id,
        "native_parent_id": "session-e2e",
        "kind": "message",
        "occurred_at": "2026-07-12T20:00:00Z",
        "observed_at": "2026-07-12T20:00:02Z",
        "principal_id": OWNER,
        "visibility": "private",
        "content_type": "application/json",
        "content": content,
        "provenance": {
            "harness": "codex",
            "original_path": "/evidence/codex-e2e/session-e2e.jsonl",
            "cwd": "/workspace/recall-e2e",
            "branch": "test/legacy-retire",
        },
        "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
    }


def slack_event(text: str, ts: str = "1721332800.000100") -> bytes:
    return json.dumps({
        "type": "event_callback",
        "event_id": f"Ev{ts.replace('.', '')}",
        "team_id": SLACK_WORKSPACE,
        "event": {
            "type": "message",
            "channel": "C0E2E",
            "user": "U0E2E",
            "ts": ts,
            "text": text,
        },
    }, sort_keys=True).encode()


def request(
    server,
    method: str,
    path: str,
    *,
    token: str | None = None,
    payload: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict | None]:
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=10,
    )
    sent = {"Content-Type": "application/json"}
    if token is not None:
        sent["Authorization"] = f"Bearer {token}"
    if payload is not None:
        sent["Content-Length"] = str(len(payload))
    sent.update(headers or {})
    connection.request(method, path, body=payload, headers=sent)
    response = connection.getresponse()
    raw = response.read()
    connection.close()
    return response.status, (json.loads(raw) if raw else None)


def post_json(server, path: str, token: str | None, value: dict, **headers):
    return request(
        server, "POST", path, token=token,
        payload=json.dumps(value).encode(), headers=headers,
    )


def post_slack(server, raw: bytes, *, retry: str | None = None):
    timestamp = str(int(time.time()))
    signature = "v0=" + hmac.new(
        SLACK_SECRET.encode(), b"v0:" + timestamp.encode() + b":" + raw,
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "X-Slack-Request-Timestamp": timestamp,
        "X-Slack-Signature": signature,
    }
    if retry is not None:
        headers["X-Slack-Retry-Num"] = retry
    return request(
        server, "POST", "/webhooks/v1/slack", payload=raw, headers=headers,
    )


def resolve(server, token: str, receipt: str):
    return request(
        server, "GET", f"/v1/receipts/resolve?receipt={quote(receipt, safe='')}",
        token=token,
    )


def count(connection, sql: str, *params) -> int:
    return int(connection.execute(sql, params).fetchone()["count"])


def seed_slack_route(store: BrainStore) -> None:
    organization = "org:e2e:webhook"
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO brain_tenants(tenant_id) VALUES (%s)", (TENANT,),
        )
        connection.execute(
            """INSERT INTO brain_organizations(
                   organization_id,organization_kind,display_name
               ) VALUES (%s,'personal','E2E Webhook')""",
            (organization,),
        )
        connection.execute(
            """INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug)
               VALUES (%s,%s,'personal','e2e-webhook')""",
            (TENANT, organization),
        )
        connection.execute(
            "INSERT INTO brain_principals(tenant_id,principal_id) VALUES (%s,%s)",
            (TENANT, OWNER),
        )
        connection.execute(
            """INSERT INTO brain_access_grants(tenant_id,principal_id,permission)
               VALUES (%s,%s,'owner')""",
            (TENANT, OWNER),
        )
        connection_id = uuid.uuid4()
        connection.execute(
            """INSERT INTO provider_connections(
                   id,principal_id,provider,subject_id,status,granted_scopes,
                   encrypted_credentials,encryption_key_id
               ) VALUES (%s,%s,'slack',%s,'connected',ARRAY['channels:history'],
                         %s,'e2e-key')""",
            (connection_id, OWNER, SLACK_WORKSPACE, b"synthetic"),
        )
        connection.execute(
            """INSERT INTO connector_installations(
                   id,tenant_id,principal_id,connector_id,source_id,connection_id,
                   execution,state,privacy_mode
               ) VALUES (%s,%s,%s,'slack.messages',%s,%s,'remote_worker',
                         'enabled','scrub')""",
            (uuid.uuid4(), TENANT, OWNER, SLACK_SOURCE, connection_id),
        )


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    with store.connect() as connection:
        connection.execute(LEGACY_TRUNCATE)
    seed_slack_route(store)
    webhook = store.create_collector_token(
        "synthetic-webhook", WEBHOOK_SOURCE, ["webhook"],
        principal_id=OWNER, webhook_privacy_mode="scrub",
    )
    read_only = store.create_collector_token(
        "synthetic-read-only", None, ["read"], principal_id=OWNER,
    )
    collector = store.create_collector_token(
        "synthetic-collector", COLLECTOR_SOURCE, ["write"],
        tenant_id=TENANT, principal_id=OWNER,
    )
    previous = {key: os.environ.get(key) for key in ENV_KEYS}
    for key in ("RECALL_LEGACY_WRITES", "RECALL_LEGACY_READS"):
        os.environ.pop(key, None)
    # Private (tailnet) profile: it exposes /v1/receipts/resolve, the legacy
    # read routes, and /v1/ingest/batches, which public-edge hides with 404.
    os.environ.pop("RECALL_HTTP_PROFILE", None)
    os.environ.update({
        "RECALL_AUTH_REQUIRED": "1",
        "RECALL_TRUST_TAILSCALE_HEADERS": "0",
        "RECALL_SLACK_SIGNING_SECRET": SLACK_SECRET,
        "RECALL_LEGACY_INGEST_TENANT_ID": TENANT,
    })
    temporary = tempfile.TemporaryDirectory()
    archive = FilesystemArchiveStore(
        Path(temporary.name) / "archive", namespace_key=b"w" * 32,
    )
    Handler.store = store
    Handler.archive_store = archive
    Handler.canonical_plane = CanonicalPlane(store, archive)
    store.legacy_ingest_bridge = LegacyIngestBridge(
        store, Handler.canonical_plane, archive,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    canary = "password=synthetic-webhook-secret"
    evidence: dict[str, object] = {"status": "pass"}
    try:
        # --- generic webhook ------------------------------------------------
        first_status, first = post_json(
            server, "/webhooks/v1/events", webhook["token"],
            body(text=f"safe {canary} safe"),
        )
        replay_status, replay = post_json(
            server, "/webhooks/v1/events", webhook["token"],
            body(text=f"safe {canary} safe"),
        )
        changed_status, changed = post_json(
            server, "/webhooks/v1/events", webhook["token"],
            body(text="synthetic cobalt changed"),
        )
        denied_status, _ = post_json(
            server, "/webhooks/v1/events", read_only["token"], body(),
        )
        assert (first_status, replay_status, changed_status) == (201, 200, 201), (
            first_status, replay_status, changed_status, first, replay, changed,
        )
        assert denied_status == 401
        assert first["receipt"] == replay["receipt"]
        assert first["duplicate_events"] == 0 and replay["duplicate_events"] == 1
        assert replay["replay"] is True and first["replay"] is False
        assert changed["receipt"].split("#", 1)[0].endswith("?rev=2"), changed

        resolve_status, resolved = resolve(server, read_only["token"], first["receipt"])
        assert resolve_status == 200, (resolve_status, resolved)
        assert resolved["event"]["source_id"] == WEBHOOK_SOURCE
        assert resolved["event"]["native_id"] == "synthetic-event"
        assert resolved["event"]["revision"] == 1
        assert resolved["event"]["provenance"]["connector_id"] == "custom.webhook"
        assert resolved["items"] and resolved["items"][0]["receipt"] == first["receipt"]
        assert canary not in json.dumps(resolved)

        deleted_status, deleted = post_json(
            server, "/webhooks/v1/events", webhook["token"], body(deleted=True),
        )
        assert deleted_status == 201, (deleted_status, deleted)
        assert deleted["receipt"].split("#", 1)[0].endswith("?rev=3"), deleted
        gone_status, _ = resolve(server, read_only["token"], first["receipt"])
        assert gone_status == 404, gone_status

        # --- Slack Events webhook ------------------------------------------
        slack_raw = slack_event(f"slack {canary} slack")
        slack_status, slack = post_slack(server, slack_raw)
        slack_replay_status, slack_replay = post_slack(server, slack_raw, retry="1")
        assert slack_status == 200 and slack == {
            "status": "accepted", "routes": 1, "replays": 0, "duplicate_events": 0,
        }, (slack_status, slack)
        assert slack_replay_status == 200 and slack_replay == {
            "status": "accepted", "routes": 1, "replays": 1, "duplicate_events": 1,
        }, (slack_replay_status, slack_replay)

        # --- /v1/ingest/batches --------------------------------------------
        batch = {"events": [legacy_envelope("session-e2e:turn-1", "quartz decision")]}
        batch_status, ack = post_json(
            server, "/v1/ingest/batches", collector["token"], batch,
            **{"Idempotency-Key": "batch-e2e-1"},
        )
        batch_replay_status, ack_replay = post_json(
            server, "/v1/ingest/batches", collector["token"], batch,
            **{"Idempotency-Key": "batch-e2e-1"},
        )
        assert batch_status == 201 and ack["inserted"] == 1, (batch_status, ack)
        assert ack["duplicate_events"] == 0 and ack["replay"] is False
        assert batch_replay_status == 200 and ack_replay["replay"] is True
        assert ack_replay["duplicate_events"] == 1 and ack_replay["inserted"] == 0
        assert ack_replay["receipts"] == ack["receipts"]
        batch_resolve_status, batch_resolved = resolve(
            server, read_only["token"], ack["receipts"][0],
        )
        assert batch_resolve_status == 200, (batch_resolve_status, batch_resolved)
        assert batch_resolved["event"]["provenance"]["connector_id"] == "legacy.codex"
        assert batch_resolved["event"]["provenance"]["harness"] == "codex"
        mismatch = {"events": [{**batch["events"][0], "source_id": "attacker:source"}]}
        mismatch_status, _ = post_json(
            server, "/v1/ingest/batches", collector["token"], mismatch,
            **{"Idempotency-Key": "batch-e2e-mismatch"},
        )
        assert mismatch_status == 403

        # --- MCP capture / forget (BrainStore.capture) ---------------------
        capture_principal = {
            "source_id": "synthetic:owner:capture",
            "principal_id": OWNER,
            "capture_origin": "grep-agent",
            "tenant_id": TENANT,
        }
        capture_arguments = {
            "schema_version": 1,
            "title": "Synthetic capture",
            "body": f"keep {canary} keep",
            "occurred_at": "2026-07-18T21:00:00Z",
            "tags": ["synthetic"],
            "provenance": {"uri": "manual://synthetic"},
        }
        captured = store.capture(capture_principal, capture_arguments)
        captured_replay = store.capture(capture_principal, capture_arguments)
        assert captured["status"] == "committed" and captured["replay"] is False
        assert captured_replay["replay"] is True
        assert captured["receipt"] == captured_replay["receipt"]
        assert captured["receipt"].endswith("?rev=1#item=0"), captured
        capture_resolve_status, capture_resolved = resolve(
            server, read_only["token"], captured["receipt"],
        )
        assert capture_resolve_status == 200, (capture_resolve_status, capture_resolved)
        assert capture_resolved["event"]["kind"] == "capture"
        assert canary not in json.dumps(capture_resolved)
        forgotten = store.forget_capture(capture_principal, captured["receipt"])
        assert forgotten["status"] == "committed", forgotten
        assert forgotten["receipt"].split("#", 1)[0].endswith("?rev=2"), forgotten
        forgotten_status, _ = resolve(server, read_only["token"], captured["receipt"])
        assert forgotten_status == 404, forgotten_status

        # --- storage assertions --------------------------------------------
        with store.connect() as connection:
            canonical_events = count(
                connection,
                "SELECT count(*) AS count FROM canonical_events WHERE tenant_id=%s",
                TENANT,
            )
            canonical_by_source = {
                row["source_id"]: int(row["count"])
                for row in connection.execute(
                    """SELECT source_id,count(*) AS count FROM canonical_events
                       WHERE tenant_id=%s GROUP BY source_id""",
                    (TENANT,),
                ).fetchall()
            }
            raw_artifacts = count(
                connection,
                "SELECT count(*) AS count FROM raw_artifacts WHERE tenant_id=%s",
                TENANT,
            )
            legacy_counts = {
                table: count(connection, f'SELECT count(*) AS count FROM public."{table}"')
                for table in ("source_events", "items", "chunks", "ingest_batches", "sources")
            }
            canary_rows = count(
                connection,
                """SELECT count(*) AS count FROM canonical_events
                   WHERE canonical_redacted::text LIKE %s""",
                f"%{canary}%",
            ) + count(
                connection,
                "SELECT count(*) AS count FROM canonical_chunks WHERE text_redacted LIKE %s",
                f"%{canary}%",
            )
        # webhook: rev1, rev2, tombstone rev3; slack: 1; batch: 1;
        # capture: rev1 + forget tombstone rev2
        assert canonical_by_source == {
            WEBHOOK_SOURCE: 3, SLACK_SOURCE: 1, COLLECTOR_SOURCE: 1,
            "synthetic:owner:capture": 2,
        }, canonical_by_source
        assert canonical_events == 7
        # one raw artifact per distinct raw body: webhook 3, slack 1, batch 1,
        # capture 1, forget 1
        assert raw_artifacts == 7, raw_artifacts
        assert set(legacy_counts.values()) == {0}, legacy_counts
        assert canary_rows == 0

        # --- legacy reads: 410 by default, 200 with the flag ----------------
        read_bodies = {
            "/v1/search": {"query": "quartz decision", "limit": 5},
            "/v1/show": {"target": "/evidence/codex-e2e/session-e2e.jsonl", "tail": 5},
            "/v1/related": {"cwd": "/workspace/recall-e2e", "limit": 5},
            "/v1/session-export": {
                "target": "/evidence/codex-e2e/session-e2e.jsonl", "limit": 10,
            },
        }
        retired = {}
        for path, value in read_bodies.items():
            status, payload = post_json(server, path, read_only["token"], value)
            retired[path] = (status, payload)
        assert all(status == 410 for status, _ in retired.values()), retired
        for path, (_, payload) in retired.items():
            assert payload == {
                "error": "gone",
                "code": "legacy_plane_retired",
                "replacement": LEGACY_READ_REPLACEMENTS[path],
            }, payload

        # Rollback path: dual-write restores the v1 projection so the four
        # routes can answer 200 with RECALL_LEGACY_READS=1.
        os.environ["RECALL_LEGACY_WRITES"] = "1"
        os.environ["RECALL_LEGACY_READS"] = "1"
        dual_status, dual_ack = post_json(
            server, "/v1/ingest/batches", collector["token"], batch,
            **{"Idempotency-Key": "batch-e2e-dual"},
        )
        assert dual_status == 201 and dual_ack["inserted"] == 1, (dual_status, dual_ack)
        assert "batch_id" in dual_ack
        restored = {}
        for path, value in read_bodies.items():
            status, payload = post_json(server, path, read_only["token"], value)
            restored[path] = status
        assert set(restored.values()) == {200}, restored
        with store.connect() as connection:
            dual_legacy = count(connection, "SELECT count(*) AS count FROM source_events")
            dual_canonical = count(
                connection,
                """SELECT count(*) AS count FROM canonical_events
                   WHERE tenant_id=%s AND source_id=%s""",
                TENANT, COLLECTOR_SOURCE,
            )
        assert dual_legacy == 1 and dual_canonical == 1, (dual_legacy, dual_canonical)
        os.environ["RECALL_LEGACY_WRITES"] = "0"
        os.environ["RECALL_LEGACY_READS"] = "0"

        # Revocation still fences the webhook.
        assert store.revoke_collector_token("synthetic-webhook")
        revoked_status, _ = post_json(
            server, "/webhooks/v1/events", webhook["token"], body(),
        )
        assert revoked_status == 401
        evidence.update({
            "canonical_events": canonical_events,
            "canonical_events_by_source": canonical_by_source,
            "raw_artifacts": raw_artifacts,
            "legacy_rows_default_mode": legacy_counts,
            "webhook_replay_duplicate_events": replay["duplicate_events"],
            "slack_replay_duplicate_events": slack_replay["duplicate_events"],
            "batch_replay_duplicate_events": ack_replay["duplicate_events"],
            "receipts_resolved": 3,
            "capture_replay": captured_replay["replay"],
            "capture_forgotten_status": forgotten_status,
            "tombstoned_receipt_status": gone_status,
            "legacy_reads_default": {
                path: status for path, (status, _) in retired.items()
            },
            "legacy_reads_flag_on": restored,
            "dual_write_source_events": dual_legacy,
            "privacy_canary_rows": canary_rows,
            "revoked_status": revoked_status,
            "droppable_tables": list(LEGACY_DROPPABLE_TABLES),
        })
        print(json.dumps(evidence, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        Handler.archive_store = None
        Handler.canonical_plane = None
        store.legacy_ingest_bridge = None
        temporary.cleanup()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        with store.connect() as connection:
            connection.execute(LEGACY_TRUNCATE)
        store.close()


if __name__ == "__main__":
    main()
