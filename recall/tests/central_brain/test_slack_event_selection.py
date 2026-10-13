"""Signed Slack callbacks obey each source's channel selection before ingest."""

import hashlib
import hmac
import json
import os
import time
import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from central_brain.test_webhook_http import FakeStore, WebhookServer
from recall_server.app import Handler


class RouteStore(FakeStore):
    def __init__(self, routes):
        super().__init__()
        self.routes = routes
        self.route_reads = 0

    @contextmanager
    def connect(self):
        self.route_reads += 1
        yield self

    def execute(self, query, params):
        assert params == ("T123",)
        return self

    def fetchall(self):
        return self.routes


def route(name, channels):
    return {
        "tenant_id": "tenant:synthetic",
        "principal_id": f"principal:{name}",
        "source_id": f"source:{name}",
        "privacy_mode": "scrub",
        "selectors": {"channel_ids": channels},
    }


class SlackEventSelectionTest(unittest.TestCase):
    def request(self, event, routes, *, signature_valid=True):
        store = RouteStore(routes)
        bridge = Mock()
        bridge.ingest.return_value = (
            {"status": "committed", "duplicate_events": 0},
            False,
        )
        value = {
            "type": "event_callback",
            "team_id": "T123",
            "event_id": "Ev123",
            "event": event,
        }
        timestamp = str(int(time.time()))
        signature = (
            "v0="
            + hmac.new(
                b"synthetic-secret",
                f"v0:{timestamp}:".encode() + json.dumps(value).encode(),
                hashlib.sha256,
            ).hexdigest()
        )
        with (
            patch.dict(
                os.environ,
                {
                    "RECALL_AUTH_REQUIRED": "1",
                    "RECALL_HTTP_PROFILE": "public-edge",
                    "RECALL_TRUST_TAILSCALE_HEADERS": "0",
                    "RECALL_SLACK_SIGNING_SECRET": "synthetic-secret",
                    "RECALL_SLACK_CLIENT_ID": "synthetic-client",
                    "RECALL_SLACK_CLIENT_SECRET": "synthetic-client-secret",
                    "RECALL_SLACK_REDIRECT_URI": "https://recall.example/admin/oauth/callback/slack",
                },
            ),
            patch.object(Handler, "legacy_ingest", return_value=bridge),
        ):
            with WebhookServer(store) as server:
                status, raw = server.request(
                    "POST",
                    "/webhooks/v1/slack",
                    body=value,
                    token=None,
                    extra_headers={
                        "X-Slack-Request-Timestamp": timestamp,
                        "X-Slack-Signature": signature
                        if signature_valid
                        else "v0=wrong",
                    },
                )
        return status, json.loads(raw), store, bridge

    def message(self, **extra):
        return {
            "type": "message",
            "channel": "C123",
            "channel_type": "channel",
            "user": "U111",
            "ts": "1784332800.000100",
            "text": "Synthetic selected evidence",
            **extra,
        }

    def test_routes_only_to_matching_selection_and_preserves_authority(self):
        status, result, _, bridge = self.request(
            self.message(),
            [
                route("selected", ["C123"]),
                route("excluded", ["C999"]),
                route("all", []),
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["routes"], 2)
        self.assertEqual(
            [call.args[1][0]["source_id"] for call in bridge.ingest.call_args_list],
            ["source:selected", "source:all"],
        )
        for call in bridge.ingest.call_args_list:
            event = call.args[1][0]
            self.assertEqual(
                event["principal_id"], call.kwargs["principal"]["principal_id"]
            )
            self.assertEqual(event["native_id"], "slack:T123:C123:1784332800.000100")
            self.assertEqual(event["visibility"], "private")

    def test_explicit_nonpublic_callbacks_do_not_read_routes_or_ingest(self):
        for kind in ("group", "im", "mpim", "unknown"):
            with self.subTest(kind=kind):
                status, result, store, bridge = self.request(
                    self.message(channel_type=kind), [route("all", [])]
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["routes"], 0)
                self.assertEqual(store.route_reads, 0)
                bridge.ingest.assert_not_called()

    def test_missing_classification_public_edit_delete_remain_compatible(self):
        for event in (
            self.message(),
            self.message(
                subtype="message_changed",
                message={"ts": "1784332800.000100", "text": "Edited", "user": "U111"},
            ),
            self.message(subtype="message_deleted", deleted_ts="1784332800.000100"),
        ):
            event.pop("channel_type")
            with self.subTest(subtype=event.get("subtype")):
                status, result, _, bridge = self.request(
                    event, [route("selected", ["C123"]), route("excluded", ["C999"])]
                )
                self.assertEqual(status, 200)
                self.assertEqual(result["routes"], 1)
                body = bridge.ingest.call_args.args[1][0]
                self.assertEqual(body["native_id"], "slack:T123:C123:1784332800.000100")
                self.assertEqual(
                    body["kind"],
                    "tombstone"
                    if event.get("subtype") == "message_deleted"
                    else "connector_record",
                )

    def test_invalid_signature_still_rejected_before_scope_reads(self):
        status, _, store, bridge = self.request(
            self.message(channel_type="im"), [route("all", [])], signature_valid=False
        )
        self.assertEqual(status, 400)
        self.assertEqual(store.route_reads, 0)
        bridge.ingest.assert_not_called()

    def test_malformed_selection_does_not_become_all_channels(self):
        for selectors in (
            None,
            [],
            {"channel_ids": "C123"},
            {"channel_ids": None},
            {"channel_ids": [123]},
        ):
            with self.subTest(selectors=selectors):
                chosen = route("broken", [])
                chosen["selectors"] = selectors
                status, result, _, bridge = self.request(self.message(), [chosen])
                self.assertEqual(status, 200)
                self.assertEqual(result["routes"], 0)
                bridge.ingest.assert_not_called()
