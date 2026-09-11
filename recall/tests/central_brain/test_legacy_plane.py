"""H1-T6: legacy v1 plane retirement flags, 410 handler, and canonical bridge."""

from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from recall_server import legacy_plane
from recall_server.app import Handler, validate_http_profile
from recall_server.canonical import CanonicalLifecycleError
from recall_server.legacy_plane import (
    DEFAULT_LEGACY_INGEST_TENANT,
    LEGACY_DROPPABLE_TABLES,
    LEGACY_READ_REPLACEMENTS,
    CanonicalPlaneUnavailable,
    LegacyFlagError,
    LegacyIngestBridge,
    legacy_connector_id,
    legacy_ingest_tenant_id,
    legacy_reads_enabled,
    legacy_retired_response,
    legacy_writes_enabled,
    parse_flag,
    validate_legacy_flags,
)

from .test_webhook_http import FakeStore, WebhookServer, webhook_body


class LegacyFlagParsingTest(unittest.TestCase):
    def test_flags_default_to_retired(self) -> None:
        self.assertFalse(legacy_writes_enabled({}))
        self.assertFalse(legacy_reads_enabled({}))

    def test_flags_accept_common_boolean_spellings(self) -> None:
        for value in ("1", "true", "YES", " on "):
            with self.subTest(value=value):
                self.assertTrue(legacy_writes_enabled({"RECALL_LEGACY_WRITES": value}))
                self.assertTrue(legacy_reads_enabled({"RECALL_LEGACY_READS": value}))
        for value in ("0", "false", "No", "off", ""):
            with self.subTest(value=value):
                self.assertFalse(legacy_writes_enabled({"RECALL_LEGACY_WRITES": value}))
                self.assertFalse(legacy_reads_enabled({"RECALL_LEGACY_READS": value}))

    def test_unparseable_flag_is_a_startup_error(self) -> None:
        with self.assertRaises(LegacyFlagError):
            parse_flag("RECALL_LEGACY_WRITES", environment={"RECALL_LEGACY_WRITES": "maybe"})
        with self.assertRaisesRegex(RuntimeError, "RECALL_LEGACY_READS must be 0 or 1"):
            validate_legacy_flags({"RECALL_LEGACY_READS": "2"})
        with mock.patch.dict(
            os.environ,
            {"RECALL_HTTP_PROFILE": "", "RECALL_LEGACY_WRITES": "sometimes"},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "RECALL_LEGACY_WRITES"):
                validate_http_profile()

    def test_process_environment_is_the_default_source(self) -> None:
        with mock.patch.dict(os.environ, {"RECALL_LEGACY_WRITES": "1"}, clear=False):
            self.assertTrue(legacy_writes_enabled())
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(legacy_writes_enabled())
            self.assertFalse(legacy_reads_enabled())

    def test_ingest_tenant_prefers_bound_credential_then_env_then_default(self) -> None:
        self.assertEqual(
            legacy_ingest_tenant_id({"tenant_id": "tenant:bound"}, {"RECALL_LEGACY_INGEST_TENANT_ID": "tenant:env"}),
            "tenant:bound",
        )
        self.assertEqual(
            legacy_ingest_tenant_id({"tenant_id": None}, {"RECALL_LEGACY_INGEST_TENANT_ID": " tenant:env "}),
            "tenant:env",
        )
        self.assertEqual(legacy_ingest_tenant_id(None, {}), DEFAULT_LEGACY_INGEST_TENANT)

    def test_connector_id_derivation_for_v1_envelopes(self) -> None:
        self.assertEqual(
            legacy_connector_id({"provenance": {"connector_id": "custom.webhook"}}),
            "custom.webhook",
        )
        self.assertEqual(legacy_connector_id({"provenance": {"harness": "codex"}}), "legacy.codex")
        self.assertEqual(legacy_connector_id({}), "legacy.ingest")

    def test_droppable_table_inventory_matches_the_decision(self) -> None:
        expected = {
            "sources", "source_grants", "source_events", "items", "chunks", "entities",
            "item_embeddings", "sessions", "turn_embeddings", "turn_embedding_items",
            "turn_embedding_projection_watermarks", "projection_watermarks",
            "projection_backfills", "ingest_batches", "embedding_projection_watermarks",
        }
        self.assertEqual(set(LEGACY_DROPPABLE_TABLES), expected)
        self.assertEqual(list(LEGACY_DROPPABLE_TABLES), sorted(LEGACY_DROPPABLE_TABLES))


class LegacyRetiredResponseTest(unittest.TestCase):
    def test_every_legacy_read_route_names_its_replacement(self) -> None:
        self.assertEqual(
            LEGACY_READ_REPLACEMENTS,
            {
                "/v1/search": "recall_search",
                "/v1/show": "recall_show",
                "/v1/related": "recall_related",
                "/v1/session-export": "recall_session_context",
            },
        )
        for path, replacement in LEGACY_READ_REPLACEMENTS.items():
            self.assertEqual(
                legacy_retired_response(path),
                {"error": "gone", "code": "legacy_plane_retired", "replacement": replacement},
            )
        with self.assertRaises(KeyError):
            legacy_retired_response("/v1/receipts/resolve")


class LegacyReadRouteHttpTest(unittest.TestCase):
    """The 410 gate answers before authentication and touches no store method."""

    def setUp(self) -> None:
        self.store = FakeStore()
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_HTTP_PROFILE": "",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
            },
            clear=False,
        )
        self.environment.start()
        os.environ.pop("RECALL_LEGACY_READS", None)
        os.environ.pop("RECALL_LEGACY_WRITES", None)

    def tearDown(self) -> None:
        self.environment.stop()

    def test_legacy_reads_return_410_with_replacement_by_default(self) -> None:
        with WebhookServer(self.store) as server:
            results = {
                path: server.request("POST", path, body={"query": "x"}, token="synthetic-read-token")
                for path in LEGACY_READ_REPLACEMENTS
            }
            unauthenticated = server.request("POST", "/v1/search", body={"query": "x"}, token=None)
        for path, (status, raw) in results.items():
            with self.subTest(path=path):
                self.assertEqual(status, 410)
                self.assertEqual(
                    json.loads(raw),
                    {
                        "error": "gone",
                        "code": "legacy_plane_retired",
                        "replacement": LEGACY_READ_REPLACEMENTS[path],
                    },
                )
        self.assertEqual(unauthenticated[0], 410)
        self.assertFalse(any(call[0] == "authenticate" for call in self.store.calls))

    def test_legacy_reads_flag_restores_the_v1_routes(self) -> None:
        self.store.search = lambda query, filters, limit, authorized_source: {
            "results": [], "query": query, "limit": limit,
        }
        with mock.patch.dict(os.environ, {"RECALL_LEGACY_READS": "1"}):
            with WebhookServer(self.store) as server:
                status, raw = server.request(
                    "POST", "/v1/search", body={"query": "x", "limit": 3},
                    token="synthetic-read-token",
                )
                denied, _ = server.request("POST", "/v1/search", body={"query": "x"}, token=None)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw), {"results": [], "query": "x", "limit": 3})
        self.assertEqual(denied, 401)

    def test_receipt_resolve_is_not_gated(self) -> None:
        self.store.resolve = lambda receipt, authorized_source=None: {"event": {"receipt": receipt}, "items": []}
        with WebhookServer(self.store) as server:
            status, raw = server.request(
                "GET", "/v1/receipts/resolve?receipt=recall%3A%2F%2Fs%2Fn%3Frev%3D1",
                token="synthetic-read-token",
            )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)["event"]["receipt"], "recall://s/n?rev=1")


class FakeArchive:
    storage_backend = "filesystem"

    def __init__(self) -> None:
        self.payloads: list[bytes] = []

    def put_raw(self, *, tenant_id, source_id, native_id, payload, media_type, created_at):
        self.payloads.append(payload)
        digest = "ab" * 32
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": tenant_id,
            "source_id": source_id,
            "artifact_id": "art_" + digest[:32],
            "storage_backend": "filesystem",
            "object_key": f"objects/{digest[:2]}/{digest}",
            "content_sha256": digest,
            "size_bytes": len(payload),
            "media_type": media_type,
            "encryption": "filesystem-managed",
            "version_id": "v1",
            "created_at": created_at,
        }


class FakeCanonicalPlane:
    def __init__(self, store) -> None:
        self.store = store
        self.documents: list[dict] = []
        self.seen: dict[tuple, int] = {}

    def ingest_document(self, *, tenant_id, principal_id, connector_id, artifact_ref, envelope, text_redacted, _connection=None):
        self.documents.append({
            "tenant_id": tenant_id,
            "principal_id": principal_id,
            "connector_id": connector_id,
            "artifact_ref": artifact_ref,
            "envelope": envelope,
            "text_redacted": text_redacted,
            "connection": _connection,
        })
        identity = (tenant_id, envelope["source_id"], envelope["native_id"], envelope["content_sha256"])
        receipt = f"recall://{envelope['source_id']}/{envelope['native_id']}?rev=1#item=0"
        if identity in self.seen:
            return {"status": "committed", "inserted": 0, "duplicate_events": 1, "revision": 1, "receipt": receipt, "replay": True}
        self.seen[identity] = 1
        return {"status": "committed", "inserted": 1, "duplicate_events": 0, "revision": 1, "receipt": receipt, "replay": False}


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


class _Connection:
    def transaction(self):
        return _Transaction()


class _ConnectionContext:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_exc):
        return False


class CanonicalFakeStore(FakeStore):
    def __init__(self, privacy_mode: str = "scrub") -> None:
        super().__init__(privacy_mode)
        self.connection = _Connection()

    def connect(self):
        self.calls.append(("connect",))
        return _ConnectionContext(self.connection)


def gateway_passthrough(store, archive, *, tenant_id, principal_id):
    class _Gateway:
        def put_raw(self, **kwargs):
            return archive.put_raw(**kwargs)

    return _Gateway()


class LegacyIngestBridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = CanonicalFakeStore()
        self.archive = FakeArchive()
        self.plane = FakeCanonicalPlane(self.store)
        self.gateway = mock.patch.object(legacy_plane, "CanonicalArchiveGateway", gateway_passthrough)
        self.gateway.start()
        self.environment = mock.patch.dict(os.environ, {}, clear=False)
        self.environment.start()
        os.environ.pop("RECALL_LEGACY_WRITES", None)
        os.environ.pop("RECALL_LEGACY_INGEST_TENANT_ID", None)

    def tearDown(self) -> None:
        self.gateway.stop()
        self.environment.stop()

    def envelope(self, text: str = "safe text") -> dict:
        from recall_server.webhooks import build_webhook_event

        prepared = build_webhook_event(
            webhook_body(text=text),
            {
                "source_id": "synthetic:webhook",
                "principal_id": "synthetic-owner",
                "webhook_privacy_mode": "scrub",
                "connector_id": "custom.webhook",
            },
        )
        return prepared.event

    def test_retired_default_archives_raw_body_and_writes_canonical_only(self) -> None:
        bridge = LegacyIngestBridge(self.store, self.plane, self.archive, environment={"RECALL_LEGACY_INGEST_TENANT_ID": "tenant:unit"})
        event = self.envelope()
        raw = b'{"raw": "body"}'
        ack, replay = bridge.ingest("webhook-v1-key", [event], principal={"tenant_id": None}, raw_payload=raw)
        self.assertFalse(replay)
        self.assertEqual(ack["inserted"], 1)
        self.assertEqual(ack["duplicate_events"], 0)
        self.assertEqual(ack["receipts"], ["recall://synthetic:webhook/event-1?rev=1#item=0"])
        self.assertEqual(self.archive.payloads, [raw])
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))
        document = self.plane.documents[0]
        self.assertEqual(document["tenant_id"], "tenant:unit")
        self.assertEqual(document["principal_id"], "synthetic-owner")
        self.assertEqual(document["connector_id"], "custom.webhook")
        self.assertEqual(document["envelope"]["provenance"]["artifact_ref"], document["artifact_ref"])
        self.assertEqual(document["text_redacted"], "safe text")
        self.assertIs(document["connection"], self.store.connection)
        # The caller's envelope is not mutated by the canonical projection.
        self.assertNotIn("artifact_ref", event["provenance"])

    def test_replay_reports_one_duplicate_event_and_the_same_receipt(self) -> None:
        bridge = LegacyIngestBridge(self.store, self.plane, self.archive)
        event = self.envelope()
        first, first_replay = bridge.ingest("k", [event], principal=None, raw_payload=b"{}")
        second, second_replay = bridge.ingest("k", [event], principal=None, raw_payload=b"{}")
        self.assertEqual((first_replay, second_replay), (False, True))
        self.assertEqual(second["duplicate_events"], 1)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(second["receipts"], first["receipts"])
        self.assertEqual(self.plane.documents[0]["tenant_id"], DEFAULT_LEGACY_INGEST_TENANT)

    def test_batch_envelopes_archive_their_canonical_json_and_derive_connector(self) -> None:
        bridge = LegacyIngestBridge(self.store, self.plane, self.archive)
        import hashlib

        from recall_server.projectors import canonical_json

        content = {"role": "user", "text": "quartz"}
        event = {
            "schema_version": 1,
            "source_id": "codex:unit",
            "native_id": "s:turn-1",
            "native_parent_id": "s",
            "kind": "message",
            "occurred_at": "2026-07-12T20:00:00Z",
            "observed_at": "2026-07-12T20:00:02Z",
            "principal_id": "owner",
            "visibility": "private",
            "content_type": "application/json",
            "content": content,
            "provenance": {"harness": "codex"},
            "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
        }
        ack, _ = bridge.ingest("batch-1", [event], principal={"tenant_id": "tenant:bound"})
        self.assertEqual(ack["inserted"], 1)
        self.assertEqual(self.archive.payloads, [canonical_json(event)])
        document = self.plane.documents[0]
        self.assertEqual(document["connector_id"], "legacy.codex")
        self.assertEqual(document["tenant_id"], "tenant:bound")
        self.assertEqual(document["text_redacted"], json.dumps(content, sort_keys=True, separators=(",", ":")))

    def test_missing_canonical_plane_fails_closed(self) -> None:
        bridge = LegacyIngestBridge(self.store, None, None)
        with self.assertRaises(CanonicalPlaneUnavailable):
            bridge.ingest("k", [self.envelope()], principal=None, raw_payload=b"{}")
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))
        self.assertEqual(self.archive.payloads, [])

    def test_invalid_idempotency_key_and_empty_batch_are_rejected(self) -> None:
        bridge = LegacyIngestBridge(self.store, self.plane, self.archive)
        with self.assertRaisesRegex(ValueError, "idempotency"):
            bridge.ingest("", [self.envelope()], principal=None)
        with self.assertRaisesRegex(ValueError, "empty ingest batch"):
            bridge.ingest("k", [], principal=None)

    def test_rollback_flag_dual_writes_and_returns_the_v1_acknowledgement(self) -> None:
        bridge = LegacyIngestBridge(self.store, self.plane, self.archive, environment={"RECALL_LEGACY_WRITES": "1"})
        ack, replay = bridge.ingest("legacy-key", [self.envelope()], principal=None, raw_payload=b"{}")
        self.assertFalse(replay)
        self.assertEqual(ack["receipts"], ["recall://synthetic:webhook/event-1?rev=1"])
        self.assertEqual(len(self.plane.documents), 1)
        self.assertTrue(any(call[0] == "ingest" and call[1] == "legacy-key" for call in self.store.calls))

    def test_rollback_flag_without_canonical_plane_is_legacy_only(self) -> None:
        bridge = LegacyIngestBridge(self.store, None, None, environment={"RECALL_LEGACY_WRITES": "1"})
        ack, _ = bridge.ingest("legacy-key", [self.envelope()], principal=None)
        self.assertEqual(ack["status"], "committed")
        self.assertTrue(any(call[0] == "ingest" for call in self.store.calls))


class CaptureBridgeTest(unittest.TestCase):
    """MCP recall_capture / recall_forget write through the bridge, never BrainStore.ingest."""

    principal = {
        "source_id": "synthetic:capture",
        "principal_id": "synthetic-owner",
        "capture_origin": "synthetic-agent",
        "tenant_id": "tenant:unit",
    }
    arguments = {
        "schema_version": 1,
        "title": "Synthetic canonical receipt",
        "body": "synthetic receipt evidence",
        "occurred_at": "2026-07-18T02:00:00Z",
        "tags": ["synthetic"],
        "provenance": {"uri": "manual://synthetic"},
    }

    def setUp(self) -> None:
        from recall_server.db import BrainStore

        self.store = BrainStore("postgresql://synthetic.invalid/recall")
        self.store.ingest = mock.MagicMock(side_effect=AssertionError("legacy ingest must not run"))
        self.environment = mock.patch.dict(os.environ, {}, clear=False)
        self.environment.start()
        os.environ.pop("RECALL_LEGACY_WRITES", None)

    def tearDown(self) -> None:
        self.environment.stop()

    def test_capture_uses_the_attached_bridge_and_keeps_item_receipts(self) -> None:
        bridge = mock.MagicMock()
        bridge.ingest.return_value = (
            {"status": "committed", "receipts": ["recall://synthetic:capture/capture_x?rev=1#item=0"], "duplicate_events": 0},
            False,
        )
        self.store.legacy_ingest_bridge = bridge
        result = self.store.capture(self.principal, self.arguments)
        self.assertEqual(result["receipt"], "recall://synthetic:capture/capture_x?rev=1#item=0")
        self.assertFalse(result["replay"])
        call = bridge.ingest.call_args
        self.assertTrue(call.args[0].startswith("mcp-capture-v1-"))
        self.assertEqual(call.args[1][0]["kind"], "capture")
        self.assertEqual(call.kwargs["principal"], self.principal)
        self.assertEqual(call.kwargs["connector_id"], "mcp.capture")
        self.store.ingest.assert_not_called()

    def test_capture_without_bridge_or_plane_fails_closed(self) -> None:
        self.store.legacy_ingest_bridge = None
        with self.assertRaises(CanonicalPlaneUnavailable):
            self.store.capture(self.principal, self.arguments)
        self.store.ingest.assert_not_called()


class WebhookCanonicalHttpTest(unittest.TestCase):
    """HTTP contract in the retired default: 201/200 with duplicate_events, 503 without a plane."""

    def setUp(self) -> None:
        self.store = CanonicalFakeStore()
        self.archive = FakeArchive()
        self.plane = FakeCanonicalPlane(self.store)
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_HTTP_PROFILE": "public-edge",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
                "RECALL_LEGACY_INGEST_TENANT_ID": "tenant:unit",
            },
            clear=False,
        )
        self.environment.start()
        os.environ.pop("RECALL_LEGACY_WRITES", None)
        self.gateway = mock.patch.object(legacy_plane, "CanonicalArchiveGateway", gateway_passthrough)
        self.gateway.start()

    def tearDown(self) -> None:
        self.gateway.stop()
        self.environment.stop()
        Handler.archive_store = None
        Handler.canonical_plane = None

    def test_webhook_commits_canonically_and_replays_with_duplicate_counter(self) -> None:
        Handler.archive_store = self.archive
        Handler.canonical_plane = self.plane
        canary = "password=synthetic-canonical-canary"
        with WebhookServer(self.store) as server:
            first_status, first_raw = server.request("POST", "/webhooks/v1/events", body=webhook_body(text=f"safe {canary} safe"))
            replay_status, replay_raw = server.request("POST", "/webhooks/v1/events", body=webhook_body(text=f"safe {canary} safe"))
        first = json.loads(first_raw)
        replay = json.loads(replay_raw)
        self.assertEqual((first_status, replay_status), (201, 200))
        self.assertEqual((first["replay"], first["duplicate_events"]), (False, 0))
        self.assertEqual((replay["replay"], replay["duplicate_events"]), (True, 1))
        self.assertEqual(first["receipt"], replay["receipt"])
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))
        self.assertEqual(len(self.plane.documents), 2)
        self.assertNotIn(canary, self.plane.documents[0]["text_redacted"])
        self.assertIn("[REDACTED:credential]", self.plane.documents[0]["text_redacted"])
        # The raw request body is what gets archived.
        self.assertEqual(len(self.archive.payloads), 2)
        self.assertIn(canary.encode(), self.archive.payloads[0])

    def test_webhook_without_canonical_plane_is_503_not_a_legacy_write(self) -> None:
        Handler.archive_store = None
        Handler.canonical_plane = None
        with WebhookServer(self.store) as server:
            status, raw = server.request("POST", "/webhooks/v1/events", body=webhook_body())
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(raw), {"error": "canonical plane unavailable"})
        self.assertFalse(any(call[0] == "ingest" for call in self.store.calls))

    def test_canonical_lifecycle_errors_map_to_http_status(self) -> None:
        cases = {
            "canonical_identity_forgotten": 409,
            "archive_identity_forgotten": 409,
            "canonical_authority_forbidden": 403,
            "canonical_lineage_invalid": 403,
            "archive_authority_forbidden": 403,
            "canonical_contract_invalid": 400,
        }
        for code, expected in cases.items():
            with self.subTest(code=code):
                self.assertEqual(Handler.legacy_write_error_status(CanonicalLifecycleError(code)), expected)

    def test_forgotten_identity_is_409_over_http(self) -> None:
        class ForgottenPlane(FakeCanonicalPlane):
            def ingest_document(self, **kwargs):
                raise CanonicalLifecycleError("canonical_identity_forgotten")

        Handler.archive_store = self.archive
        Handler.canonical_plane = ForgottenPlane(self.store)
        with WebhookServer(self.store) as server:
            status, raw = server.request("POST", "/webhooks/v1/events", body=webhook_body())
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(raw), {"error": "canonical_identity_forgotten"})


if __name__ == "__main__":
    unittest.main()
