from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from client.cli import parser
from connectors.portable_archives import (
    NotionArchiveConnector,
    SlackArchiveConnector,
    XArchiveConnector,
)
from connectors.registry import definition
from connectors.sdk import ConnectorContractError, ConnectorRunner
from privacy.policy import PrivacyPolicy


class Brain:
    def __init__(self):
        self.events = {}

    def ingest(self, events):
        inserted = 0
        duplicates = 0
        receipts = []
        for event in events:
            key = (event["native_id"], event["content_sha256"])
            if key in self.events:
                duplicates += 1
            else:
                inserted += 1
                self.events[key] = event
            receipts.append(
                f"recall://{event['source_id']}/{event['native_id']}?rev=1"
            )
        return {
            "status": "committed",
            "inserted": inserted,
            "duplicate_events": duplicates,
            "receipts": receipts,
            "replay": bool(duplicates),
        }


class PortableServiceArchiveTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def _zip(self, name: str, members: dict[str, str]) -> Path:
        path = self.root / name
        with zipfile.ZipFile(path, "w") as archive:
            for member, value in members.items():
                archive.writestr(member, value)
        return path

    def test_registry_contract_is_closed_and_source_local(self):
        expected = {
            "portable.slack": (
                ("communication_message.v1", "contact_identity.v1"),
                "slack-archive-sync",
            ),
            "portable.notion": ("document.v1", "notion-archive-sync"),
            "portable.x": ("social_post.v1", "x-archive-sync"),
        }
        for connector_id, (kind, command) in expected.items():
            item = definition(connector_id)
            self.assertEqual(item.execution_placement, "source_local")
            self.assertEqual(item.acquisition_modes, ("import",))
            self.assertEqual(item.auth.kind, "selected_export")
            self.assertEqual(
                item.record_kinds, kind if isinstance(kind, tuple) else (kind,),
            )
            self.assertEqual(item.command, command)
            self.assertEqual(
                item.selection_fields,
                ("archive_id", "owner_identifiers", "removed_native_ids"),
            )
        args = parser().parse_args([
            "slack-archive-sync", "--endpoint", "https://brain.example.invalid",
            "--source-id", "portable:slack:test", "--keychain-service", "synthetic",
            "--keychain-account", "portable:slack:test",
            "--input", "/synthetic/slack.zip", "--archive-id", "T123",
            "--spool", "/synthetic/slack.db",
        ])
        self.assertEqual(args.privacy_mode, "scrub")
        self.assertEqual(args.visibility, "private")

    def test_slack_export_has_stable_identity_edits_and_no_absence_delete(self):
        archive = self._zip("slack.zip", {
            "general/2026-01-02.json": json.dumps([{
                "client_msg_id": "msg-1",
                "ts": "1767312000.000001",
                "user": "U111",
                "text": "synthetic first",
            }]),
            "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
            "users.json": "[]",
        })
        connector = SlackArchiveConnector(
            path=archive, source_id="portable:slack:test",
            archive_id="T123", owner_identifiers=("UOWNER",),
        )
        first = connector.pull(None)
        self.assertEqual(len(first.records), 1)
        record = first.records[0]
        self.assertEqual(record.content["text"], "synthetic first")
        self.assertEqual(record.content["direction"], "inbound")
        native_id = record.native_id

        self._zip("slack.zip", {
            "general/2026-01-02.json": json.dumps([{
                "client_msg_id": "msg-1",
                "ts": "1767312000.000001",
                "user": "U111",
                "text": "synthetic edited",
            }]),
            "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
        })
        changed = connector.pull(first.next_cursor)
        self.assertEqual(changed.records[0].native_id, native_id)
        self.assertEqual(changed.records[0].content["text"], "synthetic edited")

        empty = self._zip("empty-slack.zip", {
            "general/2026-01-03.json": "[]",
            "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
        })
        missing = SlackArchiveConnector(
            path=empty, source_id="portable:slack:test",
            archive_id="T123",
        ).pull(None)
        self.assertEqual(missing.records, ())

    def test_notion_and_x_exports_project_bounded_typed_records(self):
        notion = self._zip("notion.zip", {
            "Team/Plan abc123.md": "# Synthetic plan\n\nA bounded note.",
            "Team/Table def456.csv": "Name,Status\nSynthetic,Open\n",
        })
        notion_page = NotionArchiveConnector(
            path=notion, source_id="portable:notion:test", archive_id="space-test",
        ).pull(None)
        self.assertEqual(len(notion_page.records), 2)
        self.assertEqual(
            {record.content["mime_type"] for record in notion_page.records},
            {"text/csv", "text/markdown"},
        )

        x_archive = self._zip("x.zip", {
            "data/tweets.js": (
                'window.YTD.tweets.part0 = [{"tweet":'
                '{"id_str":"12345","full_text":"synthetic post",'
                '"created_at":"Wed Jan 02 03:04:05 +0000 2026",'
                '"favorite_count":"2","retweet_count":"3"}}]'
            ),
        })
        x_page = XArchiveConnector(
            path=x_archive, source_id="portable:x:test", archive_id="account-test",
            owner_identifiers=("owner-test",),
        ).pull(None)
        self.assertEqual(len(x_page.records), 1)
        self.assertEqual(x_page.records[0].content["post_id"], "12345")
        self.assertEqual(x_page.records[0].content["stream_type"], "own")

    def test_archive_traversal_symlink_malformed_and_expansion_fail_closed(self):
        traversal = self._zip("traversal.zip", {"../escape.md": "no"})
        with self.assertRaisesRegex(ConnectorContractError, "archive_member_invalid"):
            NotionArchiveConnector(
                path=traversal, source_id="portable:notion:test",
                archive_id="space-test",
            ).pull(None)

        symlink = self.root / "symlink.zip"
        with zipfile.ZipFile(symlink, "w") as archive:
            info = zipfile.ZipInfo("linked.md")
            info.external_attr = 0o120777 << 16
            archive.writestr(info, "no")
        with self.assertRaisesRegex(ConnectorContractError, "archive_member_invalid"):
            NotionArchiveConnector(
                path=symlink, source_id="portable:notion:test",
                archive_id="space-test",
            ).pull(None)

        malformed = self._zip("malformed.zip", {"general/2026-01-02.json": "{"})
        with self.assertRaisesRegex(ConnectorContractError, "slack_json_invalid"):
            SlackArchiveConnector(
                path=malformed, source_id="portable:slack:test",
                archive_id="workspace-test",
            ).pull(None)

        expansion = self.root / "expansion.zip"
        with zipfile.ZipFile(
            expansion, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            archive.writestr("Team/huge.md", "0" * 2_000_000)
        with self.assertRaisesRegex(ConnectorContractError, "archive_member_invalid"):
            NotionArchiveConnector(
                path=expansion, source_id="portable:notion:test",
                archive_id="space-test",
            ).pull(None)

    def test_explicit_removal_overrides_live_record(self):
        archive = self._zip("x-remove.zip", {
            "data/tweets.js": (
                'window.YTD.tweets.part0 = '
                '[{"tweet":{"id_str":"9","full_text":"synthetic"}}]'
            ),
        })
        live = XArchiveConnector(
            path=archive, source_id="portable:x:test", archive_id="account-test",
        ).pull(None).records[0]
        removed = XArchiveConnector(
            path=archive, source_id="portable:x:test", archive_id="account-test",
            removed_native_ids=(live.native_id,),
        ).pull(None).records[0]
        self.assertTrue(removed.deleted)

    def test_privacy_precedes_spool_and_brain_for_all_three_archives(self):
        canary = "synthetic-portable-archive-private-canary"
        archives = (
            (
                SlackArchiveConnector,
                self._zip("private-slack.zip", {
                    "channels.json": json.dumps([{"id": "C123", "name": "general"}]),
                    "general/2026-01-02.json": json.dumps([{
                        "client_msg_id": "private-1",
                        "ts": "1767312000.000001",
                        "user": "U111",
                        "text": f"api_key={canary}",
                    }]),
                }),
                "portable:slack:private",
            ),
            (
                NotionArchiveConnector,
                self._zip("private-notion.zip", {
                    "Private abc123.md": f"api_key={canary}",
                }),
                "portable:notion:private",
            ),
            (
                XArchiveConnector,
                self._zip("private-x.zip", {
                    "data/tweets.js": (
                        'window.YTD.tweets.part0 = [{"tweet":'
                        f'{{"id_str":"private-1","full_text":"api_key={canary}"}}'
                        "}]"
                    ),
                }),
                "portable:x:private",
            ),
        )
        for index, (connector_type, archive, source_id) in enumerate(archives):
            brain = Brain()
            spool = self.root / f"state-{index}.db"
            runner = ConnectorRunner(
                connector=connector_type(
                    path=archive, source_id=source_id,
                    archive_id=("T123" if connector_type is SlackArchiveConnector else "archive-test"),
                ),
                brain=brain,
                spool_path=spool,
                privacy=PrivacyPolicy(mode="scrub"),
            )
            try:
                first = runner.run_once()
                second = runner.run_once()
            finally:
                runner.close()
            self.assertEqual(first["acked"], 1)
            self.assertEqual(second["acked"], 0)
            self.assertNotIn(canary, spool.read_bytes().decode(errors="ignore"))
            self.assertNotIn(canary, json.dumps(list(brain.events.values())))
