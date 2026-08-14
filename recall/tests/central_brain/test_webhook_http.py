from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest import mock

from recall_server.app import Handler, validate_http_profile


def webhook_body(
    *,
    text: str = "Synthetic webhook evidence",
    event_id: str = "event-1",
    deleted: bool = False,
) -> dict:
    record = {"kind": "communication_message.v1"}
    if not deleted:
        record.update({
            "content_fidelity": "complete",
            "conversation_id": "conversation-1",
            "direction": "inbound",
            "message_id": event_id,
            "text": text,
        })
    return {
        "schema_version": 1,
        "event_id": event_id,
        "parent_id": "conversation-1",
        "occurred_at": "2026-07-18T20:00:00Z",
        "record": record,
        "deleted": deleted,
    }


class FakeStore:
    def __init__(self, privacy_mode: str = "scrub") -> None:
        self.calls: list[tuple] = []
        self.acknowledged: dict[str, dict] = {}
        self.privacy_mode = privacy_mode

    def authenticate_bearer(self, token: str, scope: str) -> dict | None:
        self.calls.append(("authenticate", scope))
        if token == "synthetic-read-token" and scope == "read":
            return {
                "name": "synthetic-read",
                "source_id": None,
                "principal_id": "synthetic-owner",
                "capture_origin": None,
                "webhook_privacy_mode": None,
                "scopes": ["read"],
            }
        if token != "synthetic-webhook-token" or scope != "webhook":
            return None
        return {
            "name": "synthetic-webhook",
            "source_id": "synthetic:webhook",
            "principal_id": "synthetic-owner",
            "capture_origin": None,
            "webhook_privacy_mode": self.privacy_mode,
            "scopes": ["webhook"],
        }

    def ingest(self, idempotency_key: str, events: list[dict]) -> tuple[dict, bool]:
        self.calls.append(("ingest", idempotency_key, events))
        if idempotency_key in self.acknowledged:
            return self.acknowledged[idempotency_key], True
        acknowledgement = {
            "status": "committed",
            "receipts": [
                f"recall://synthetic:webhook/{events[0]['native_id']}?rev=1"
            ],
        }
        self.acknowledged[idempotency_key] = acknowledgement
        return acknowledgement, False

    def authorized_source_ids(self, principal_id: str) -> list[str]:
        self.calls.append(("authorized_sources", principal_id))
        return ["synthetic:webhook"]

    def readiness(self) -> dict:
        self.calls.append(("readiness",))
        return {"status": "ready"}


class WebhookServer:
    def __init__(self, store: FakeStore) -> None:
        Handler.store = store
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        method: str,
        path: str,
        *,
        body: dict | None = None,
        token: str | None = "synthetic-webhook-token",
        content_type: str = "application/json",
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[int, bytes]:
        payload = json.dumps(body).encode() if body is not None else None
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if payload is not None:
            headers["Content-Type"] = content_type
            headers["Content-Length"] = str(len(payload))
        if path == "/mcp":
            headers["Accept"] = "application/json, text/event-stream"
            headers["MCP-Protocol-Version"] = "2025-11-25"
        headers.update(extra_headers or {})
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2
        )
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        connection.close()
        return response.status, raw


class WebhookHttpContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = FakeStore()
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_HTTP_PROFILE": "public-edge",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()

    def test_public_edge_profile_requires_auth_and_forbids_identity_headers(self):
        validate_http_profile()
        with mock.patch.dict(os.environ, {"RECALL_AUTH_REQUIRED": "0"}):
            with self.assertRaisesRegex(RuntimeError, "requires authentication"):
                validate_http_profile()
        with mock.patch.dict(os.environ, {"RECALL_TRUST_TAILSCALE_HEADERS": "1"}):
            with self.assertRaisesRegex(RuntimeError, "forbids trusted identity"):
                validate_http_profile()

    def test_configured_slack_webhook_is_public_in_mcp_profile(self):
        slack = {
            "RECALL_HTTP_PROFILE": "public-mcp",
            "RECALL_SLACK_CLIENT_ID": "synthetic-client",
            "RECALL_SLACK_CLIENT_SECRET": "synthetic-client-secret",
            "RECALL_SLACK_REDIRECT_URI": (
                "https://recall.example/admin/oauth/callback/slack"
            ),
            "RECALL_SLACK_SIGNING_SECRET": "synthetic-signing-secret",
        }
        challenge = {"type": "url_verification", "challenge": "synthetic-challenge"}
        body = json.dumps(challenge).encode()
        timestamp = str(int(time.time()))
        signature = "v0=" + hmac.new(
            slack["RECALL_SLACK_SIGNING_SECRET"].encode(),
            f"v0:{timestamp}:".encode() + body,
            hashlib.sha256,
        ).hexdigest()
        with mock.patch.dict(os.environ, slack, clear=False):
            with WebhookServer(self.store) as server:
                status, raw = server.request(
                    "POST",
                    "/webhooks/v1/slack",
                    body=challenge,
                    token=None,
                    extra_headers={
                        "X-Slack-Request-Timestamp": timestamp,
                        "X-Slack-Signature": signature,
                    },
                )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw), {"challenge": "synthetic-challenge"})

    def test_valid_webhook_is_source_principal_provenance_and_privacy_bound(self):
        canary = "password=synthetic-webhook-canary"
        with WebhookServer(self.store) as server:
            status, raw = server.request(
                "POST",
                "/webhooks/v1/events",
                body=webhook_body(text=f"safe {canary} safe"),
            )
        self.assertEqual(status, 201)
        result = json.loads(raw)
        self.assertEqual(result["status"], "committed")
        self.assertFalse(result["replay"])
        self.assertNotIn(canary, raw.decode())
        ingest = next(call for call in self.store.calls if call[0] == "ingest")
        event = ingest[2][0]
        self.assertEqual(event["source_id"], "synthetic:webhook")
        self.assertEqual(event["principal_id"], "synthetic-owner")
        self.assertEqual(event["visibility"], "private")
        self.assertEqual(event["provenance"], {
            "connector_id": "custom.webhook",
            "connector_schema_version": 2,
            "uri": "connector://custom-webhook",
        })
        self.assertNotIn(canary, json.dumps(event))
        self.assertIn("[REDACTED:credential]", event["content"]["text"])

    def test_public_edge_retains_authenticated_mcp(self):
        with WebhookServer(self.store) as server:
            status, raw = server.request(
                "POST",
                "/mcp",
                body={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "synthetic", "version": "1"},
                    },
                },
                token="synthetic-read-token",
            )
        self.assertEqual(status, 200)
        self.assertEqual(
            json.loads(raw)["result"]["protocolVersion"],
            "2025-11-25",
        )

    def test_replay_change_and_explicit_tombstone_have_stable_identity(self):
        with WebhookServer(self.store) as server:
            first_status, first_raw = server.request(
                "POST", "/webhooks/v1/events", body=webhook_body(),
            )
            replay_status, replay_raw = server.request(
                "POST", "/webhooks/v1/events", body=webhook_body(),
            )
            changed_status, _ = server.request(
                "POST",
                "/webhooks/v1/events",
                body=webhook_body(text="Synthetic changed evidence"),
            )
            deleted_status, _ = server.request(
                "POST",
                "/webhooks/v1/events",
                body=webhook_body(deleted=True),
            )
        self.assertEqual(
            (first_status, replay_status, changed_status, deleted_status),
            (201, 200, 201, 201),
        )
        self.assertFalse(json.loads(first_raw)["replay"])
        self.assertTrue(json.loads(replay_raw)["replay"])
        ingests = [call for call in self.store.calls if call[0] == "ingest"]
        self.assertEqual(ingests[0][1], ingests[1][1])
        self.assertNotEqual(ingests[0][1], ingests[2][1])
        self.assertEqual(ingests[3][2][0]["kind"], "tombstone")
        self.assertEqual(
            ingests[3][2][0]["content"],
            {"target_native_id": "event-1"},
        )

    def test_drop_policy_returns_no_receipt_and_performs_no_mutation(self):
        self.store = FakeStore(privacy_mode="drop")
        with WebhookServer(self.store) as server:
            status, raw = server.request(
                "POST",
                "/webhooks/v1/events",
                body=webhook_body(text="password=synthetic-drop-canary"),
            )
        self.assertEqual(status, 202)
        result = json.loads(raw)
        self.assertEqual(result["status"], "privacy_filtered")
        self.assertIsNone(result["receipt"])
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))
        self.assertNotIn("synthetic-drop-canary", raw.decode())

    def test_auth_shape_content_type_and_route_fail_before_mutation(self):
        invalid = {**webhook_body(), "source_id": "attacker:source"}
        with WebhookServer(self.store) as server:
            cases = (
                server.request(
                    "POST", "/webhooks/v1/events", body=webhook_body(), token=None,
                ),
                server.request(
                    "POST",
                    "/webhooks/v1/events",
                    body=webhook_body(),
                    token="synthetic-read-token",
                ),
                server.request(
                    "POST", "/webhooks/v1/events", body=invalid,
                ),
                server.request(
                    "POST",
                    "/webhooks/v1/events",
                    body=webhook_body(),
                    content_type="text/plain",
                ),
                server.request(
                    "POST", "/v1/ingest/batches", body={"events": []},
                ),
                server.request("GET", "/metrics"),
            )
        self.assertEqual([status for status, _ in cases], [401, 401, 400, 415, 404, 404])
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))
        rendered = b"".join(raw for _, raw in cases).decode()
        self.assertNotIn("attacker:source", rendered)


if __name__ == "__main__":
    unittest.main()
