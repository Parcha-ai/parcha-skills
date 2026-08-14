from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import unittest
import zipfile
from collections import defaultdict, deque
from pathlib import Path

from connectors.kit import decode_page_wire
from connectors.portable_archives import SlackArchiveConnector
from connectors.registry import definition
from connectors.slack_events import slack_event_to_webhook
from connectors.source_plugin import (
    SourcePluginManifest,
    WireSourceConnector,
    pull_source_plugin_wire,
)
from connectors.work_apis import SlackMessagesConnector
from connectors.slack_workspace import SlackWorkspaceConnector
from recall_server.webhooks import build_webhook_event


class Rail:
    def __init__(self):
        self.responses = defaultdict(deque)
        self.calls = []

    def add(self, operation, *responses):
        self.responses[operation].extend(responses)

    def request(self, operation, **parameters):
        self.calls.append((operation, parameters))
        if not self.responses[operation]:
            raise AssertionError(f"unexpected operation: {operation}")
        return self.responses[operation].popleft()

    def download_binary(self, url, *, maximum_bytes=64 * 1024 * 1024):
        self.calls.append(("binary.download", {"url": url, "maximum": maximum_bytes}))
        return b"full searchable attachment", "text/plain"


def slack_response(messages, *, cursor=""):
    return {
        "ok": True,
        "messages": messages,
        "has_more": bool(cursor),
        "response_metadata": {"next_cursor": cursor},
    }


class SlackSourcePluginTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_reference_plugin_crosses_only_closed_manifest_and_page_wire(self):
        rail = Rail()
        rail.add("messages.history", slack_response([{
            "type": "message", "ts": "1784332800.000100",
            "user": "U111", "text": "Synthetic source-plugin record",
        }]))
        connector = SlackMessagesConnector(
            rail=rail, source_id="synthetic:slack:plugin",
            workspace_id="T123", channel_id="C123",
        )
        connector.manifest = SourcePluginManifest(
            definition=definition("slack.messages"),
            operations=("backfill", "event", "reconcile"),
        )
        wire = pull_source_plugin_wire(connector, None)
        self.assertEqual(decode_page_wire(wire).records[0].native_id,
                         "slack:T123:C123:1784332800.000100")
        hosted = WireSourceConnector(
            manifest=connector.manifest,
            source_id="synthetic:slack:wire",
            transport=lambda _cursor: wire,
        )
        self.assertEqual(hosted.pull(None), decode_page_wire(wire))
        public = connector.manifest.to_public()
        self.assertEqual(SourcePluginManifest.from_mapping(public), connector.manifest)
        self.assertEqual(public["execution"], "out_of_process")

    def test_archive_and_live_api_converge_on_exact_message_and_actor_ids(self):
        archive = self.root / "slack.zip"
        message = {
            "client_msg_id": "ignored-for-identity",
            "ts": "1784332800.000100", "user": "U111", "text": "Same message",
        }
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("channels.json", json.dumps([{"id": "C123", "name": "general"}]))
            output.writestr("users.json", json.dumps([{
                "id": "U111", "name": "employee",
                "profile": {"real_name": "Employee One", "email": "one@example.test"},
            }]))
            output.writestr("general/2026-07-18.json", json.dumps([message]))
        archived = SlackArchiveConnector(
            path=archive, source_id="portable:slack:test", archive_id="T123",
        ).pull(None)
        archived_message = next(
            record for record in archived.records
            if record.content["kind"] == "communication_message.v1"
        )
        rail = Rail()
        rail.add("messages.history", slack_response([message]))
        live = SlackMessagesConnector(
            rail=rail, source_id="synthetic:slack:live",
            workspace_id="T123", channel_id="C123",
        ).pull(None).records[0]
        self.assertEqual(archived_message.native_id, live.native_id)
        self.assertEqual(archived_message.content["author_id"], live.content["author_id"])
        self.assertEqual(archived_message.content["conversation_id"], live.content["conversation_id"])

    def test_history_drains_threads_then_reconciles_users_with_email(self):
        root = {
            "type": "message", "ts": "1784332800.000100", "user": "U111",
            "text": "Root", "reply_count": 1,
        }
        reply = {
            "type": "message", "ts": "1784332801.000200", "thread_ts": root["ts"],
            "user": "U222", "text": "Reply",
        }
        rail = Rail()
        rail.add("messages.history", slack_response([root]))
        rail.add("messages.replies", slack_response([root, reply]))
        rail.add("users.list", {
            "ok": True,
            "members": [{
                "id": "U111", "name": "employee",
                "profile": {"real_name": "Employee One", "email": "one@example.test"},
            }],
            "response_metadata": {"next_cursor": ""},
        })
        connector = SlackMessagesConnector(
            rail=rail, source_id="synthetic:slack:complete",
            workspace_id="T123", channel_id="C123", owner_user_ids=("U111",),
        )
        history = connector.pull(None)
        self.assertTrue(history.has_more)
        thread = connector.pull(history.next_cursor)
        self.assertFalse(thread.has_more)
        self.assertIn("slack:T123:C123:1784332801.000200",
                      {record.native_id for record in thread.records})
        users = connector.pull(thread.next_cursor)
        self.assertTrue(users.has_more)
        self.assertEqual(
            {record.content["identifier_type"] for record in users.records},
            {"email", "slack_user_id"},
        )
        self.assertEqual(
            next(record for record in users.records
                 if record.content["identifier_type"] == "slack_user_id").content["role"],
            "self",
        )

    def test_signed_events_flow_into_generic_webhook_and_preserve_identity(self):
        secret = "synthetic-signing-secret"
        now = 1784332802
        value = {
            "type": "event_callback", "team_id": "T123", "event_id": "Ev123",
            "event": {
                "type": "message", "channel": "C123", "user": "U111",
                "ts": "1784332800.000100", "text": "Live event",
            },
        }
        body = json.dumps(value, separators=(",", ":")).encode()
        signature = "v0=" + hmac.new(
            secret.encode(), f"v0:{now}:".encode() + body, hashlib.sha256,
        ).hexdigest()
        adapted = slack_event_to_webhook(
            body=body, timestamp=str(now), signature=signature,
            signing_secret=secret, expected_workspace_id="T123", now=now,
        )
        prepared = build_webhook_event(adapted.webhook, {
            "source_id": "synthetic:slack:webhook", "principal_id": "principal-test",
            "webhook_privacy_mode": "scrub",
        })
        self.assertEqual(prepared.event["native_id"],
                         "slack:T123:C123:1784332800.000100")
        self.assertEqual(prepared.event["content"]["author_id"], "slack:T123:U111")

        tampered = body.replace(b"Live", b"Fake")
        with self.assertRaisesRegex(Exception, "signature is invalid"):
            slack_event_to_webhook(
                body=tampered, timestamp=str(now), signature=signature,
                signing_secret=secret, now=now,
            )

    def test_workspace_plugin_discovers_and_scans_every_accessible_channel(self):
        rail = Rail()
        rail.add("channels.list", {
            "ok": True,
            "channels": [
                {"id": "C111", "is_private": False, "is_member": False},
                {"id": "G222", "is_private": True, "is_member": True},
                {"id": "G333", "is_private": True, "is_member": False},
            ],
            "response_metadata": {"next_cursor": ""},
        })
        rail.add("users.list", {
            "ok": True, "members": [], "response_metadata": {"next_cursor": ""},
        })
        rail.add("channels.join", {"ok": True, "channel": {"id": "C111"}})
        rail.add("messages.history", slack_response([{
            "type": "message", "ts": "1784332800.000100",
            "user": "U111", "text": "Public channel",
        }]), slack_response([{
            "type": "message", "ts": "1784332801.000100",
            "user": "U222", "text": "Private channel",
        }]))
        connector = SlackWorkspaceConnector(
            rail=rail, source_id="synthetic:slack:workspace",
            workspace_id="T123",
        )
        cursor = None
        records = []
        for _ in range(8):
            page = connector.pull(cursor)
            records.extend(page.records)
            cursor = page.next_cursor
            if not page.has_more:
                break
        self.assertEqual(
            {record.content["text"] for record in records
             if record.content["kind"] == "communication_message.v1"},
            {"Public channel", "Private channel"},
        )
        self.assertEqual(
            [call[1]["query"]["channel"] for call in rail.calls
             if call[0] == "channels.join"],
            ["C111"],
        )
        history_calls = [call for call in rail.calls if call[0] == "messages.history"]
        self.assertEqual({call[1]["query"]["channel"] for call in history_calls},
                         {"C111", "G222"})
        self.assertTrue(all("latest" in call[1]["query"] for call in history_calls))

    def test_workspace_plugin_streams_more_channels_than_fit_in_one_cursor(self):
        rail = Rail()
        channels = [
            {"id": f"C{index:010d}", "is_private": False, "is_member": False}
            for index in range(140)
        ]
        rail.add("users.list", {
            "ok": True, "members": [], "response_metadata": {"next_cursor": ""},
        })
        rail.add(
            "channels.list",
            {
                "ok": True, "channels": channels[:50],
                "response_metadata": {"next_cursor": "channels-2"},
            },
            {
                "ok": True, "channels": channels[50:100],
                "response_metadata": {"next_cursor": "channels-3"},
            },
            {
                "ok": True, "channels": channels[100:],
                "response_metadata": {"next_cursor": ""},
            },
        )
        rail.add("channels.join", *(
            {"ok": True, "channel": {"id": channel["id"]}}
            for channel in channels
        ))
        rail.add("messages.history", *(slack_response([]) for _channel in channels))
        connector = SlackWorkspaceConnector(
            rail=rail, source_id="synthetic:slack:large-workspace",
            workspace_id="T123",
        )

        cursor = None
        cursor_sizes = []
        for _ in range(400):
            page = connector.pull(cursor)
            cursor = page.next_cursor
            cursor_sizes.append(len(cursor.encode()))
            if not page.has_more:
                break
        else:
            self.fail("workspace backfill did not finish")

        history_channels = [
            call[1]["query"]["channel"] for call in rail.calls
            if call[0] == "messages.history"
        ]
        self.assertEqual(history_channels, [channel["id"] for channel in channels])
        self.assertEqual(len(set(history_channels)), 140)
        self.assertEqual(
            [call[1]["query"].get("cursor") for call in rail.calls
             if call[0] == "channels.list"],
            [None, "channels-2", "channels-3"],
        )
        self.assertLessEqual(max(cursor_sizes), 4096)

    def test_workspace_plugin_migrates_v1_cursor_by_replaying_its_time_window(self):
        rail = Rail()
        rail.add("users.list", {
            "ok": True, "members": [], "response_metadata": {"next_cursor": ""},
        })
        legacy = json.dumps({
            "v": 1,
            "phase": "discover",
            "page": "legacy-page",
            "channels": ["C123:0"],
            "channel_index": 0,
            "threads": [],
            "thread_index": 0,
            "thread_page": None,
            "watermark": "2026-08-01T00:00:00Z",
            "upper": "2026-08-02T00:00:00Z",
            "cycle": 3,
        })
        connector = SlackWorkspaceConnector(
            rail=rail, source_id="synthetic:slack:migrate", workspace_id="T123",
        )

        page = connector.pull(legacy)
        migrated = json.loads(page.next_cursor)

        self.assertTrue(page.has_more)
        self.assertEqual(migrated["v"], 2)
        self.assertEqual(migrated["phase"], "discover")
        self.assertEqual(migrated["watermark"], "2026-08-01T00:00:00Z")
        self.assertEqual(migrated["upper"], "2026-08-02T00:00:00Z")
        self.assertEqual(migrated["cycle"], 3)
        self.assertEqual(migrated["channels"], [])

    def test_workspace_plugin_archives_full_attachment_and_projects_its_text(self):
        rail = Rail()
        rail.add("users.list", {
            "ok": True, "members": [], "response_metadata": {"next_cursor": ""},
        })
        rail.add("channels.join", {"ok": True, "channel": {"id": "C123"}})
        rail.add("messages.history", slack_response([{
            "type": "message", "ts": "1784332800.000100",
            "user": "U111", "text": "See attachment",
            "files": [{
                "id": "F123", "name": "decision.txt", "mimetype": "text/plain",
                "user": "U111",
                "url_private_download": (
                    "https://files.slack.com/files-pri/T123-F123/decision.txt"
                ),
            }],
        }]))
        connector = SlackWorkspaceConnector(
            rail=rail, source_id="synthetic:slack:files",
            workspace_id="T123", channel_ids=("C123",),
        )
        users = connector.pull(None)
        page = connector.pull(users.next_cursor)
        document = next(
            record for record in page.records if record.content["kind"] == "document.v1"
        )
        message = next(
            record for record in page.records
            if record.content["kind"] == "communication_message.v1"
        )
        self.assertEqual(message.content["content_fidelity"], "complete")
        self.assertNotIn("content_omissions", message.content)
        self.assertEqual(document.native_id, "slack-file:T123:F123")
        self.assertEqual(document.archive_payload, b"full searchable attachment")
        self.assertEqual(document.archive_media_type, "text/plain")
        self.assertEqual(document.content["text"], "full searchable attachment")
        self.assertEqual(document.native_parent_id,
                         "slack:T123:C123:1784332800.000100")


if __name__ == "__main__":
    unittest.main()
