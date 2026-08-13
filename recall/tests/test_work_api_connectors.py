from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections import defaultdict, deque
from pathlib import Path

from connectors.sdk import (
    ConnectorRunError,
    ConnectorRunner,
    ConnectorUpstreamError,
)
from connectors.work_apis import (
    GitHubActivityConnector,
    LinearActivityConnector,
    NotionWorkspaceConnector,
    SlackMessagesConnector,
    github_rail,
    linear_rail,
    notion_rail,
    slack_rail,
)
from privacy.policy import PrivacyPolicy
from tests.test_connector_sdk import FakeBrain


class FakeRail:
    def __init__(self):
        self.responses = defaultdict(deque)
        self.calls = []

    def add(self, operation, *values):
        self.responses[operation].extend(values)

    def request(self, operation, **parameters):
        self.calls.append((operation, copy.deepcopy(parameters)))
        if not self.responses[operation]:
            raise AssertionError(f"unexpected synthetic operation {operation}")
        value = self.responses[operation].popleft()
        if isinstance(value, Exception):
            raise value
        return value


def github_issue(number, *, updated="2026-07-18T01:00:00Z", body="Synthetic issue"):
    return {
        "number": number,
        "title": f"Synthetic issue {number}",
        "body": body,
        "state": "open",
        "created_at": "2026-07-17T01:00:00Z",
        "updated_at": updated,
        "html_url": f"https://github.example.invalid/o/r/issues/{number}",
        "user": {"login": "synthetic-user"},
        "labels": [{"name": "synthetic-label"}],
    }


class ProviderRailFactoryTest(unittest.TestCase):
    def test_origins_operations_queries_headers_and_graphql_are_code_owned(self):
        authority = Path("/synthetic/private/authority")
        rails = {
            "github": github_rail(authority_path=authority),
            "linear": linear_rail(authority_path=authority),
            "slack": slack_rail(authority_path=authority),
            "notion": notion_rail(authority_path=authority),
        }
        self.assertEqual(rails["github"].origin, "https://api.github.com")
        self.assertEqual(
            rails["github"].fixed_headers,
            {"X-GitHub-Api-Version": "2022-11-28"},
        )
        self.assertEqual(rails["linear"].origin, "https://api.linear.app")
        self.assertIn(
            "query RecallIssues",
            rails["linear"].operations["issues.list"].fixed_json["query"],
        )
        self.assertEqual(rails["slack"].origin, "https://slack.com")
        self.assertEqual(rails["notion"].origin, "https://api.notion.com")
        self.assertEqual(
            rails["notion"].fixed_headers,
            {"Notion-Version": "2026-03-11"},
        )
        for rail in rails.values():
            self.assertEqual(rail.authority_path, authority)


class GitHubConnectorTest(unittest.TestCase):
    def test_paginates_selected_repository_and_replays_watermark_inclusively(self):
        rail = FakeRail()
        rail.add(
            "issues.list",
            [github_issue(1), github_issue(2, updated="2026-07-18T02:00:00Z")],
            [github_issue(3, updated="2026-07-18T03:00:00Z")],
            [],
        )
        connector = GitHubActivityConnector(
            rail=rail,
            source_id="synthetic:github:activity",
            owner="synthetic-org",
            repository="synthetic-repo",
            page_size=2,
        )
        first = connector.pull(None)
        self.assertTrue(first.has_more)
        second = connector.pull(first.next_cursor)
        self.assertFalse(second.has_more)
        self.assertEqual(second.records[0].content["name"], "Synthetic issue 3")
        third = connector.pull(second.next_cursor)
        self.assertFalse(third.has_more)
        self.assertEqual(
            rail.calls[0][1]["path"],
            {"owner": "synthetic-org", "repo": "synthetic-repo"},
        )
        self.assertNotIn("since", rail.calls[0][1]["query"])
        self.assertEqual(
            rail.calls[2][1]["query"]["since"],
            "2026-07-18T03:00:00Z",
        )
        self.assertFalse(any(record.deleted for record in third.records))


class LinearConnectorTest(unittest.TestCase):
    def test_graphql_cursor_edit_revision_and_error_envelope(self):
        rail = FakeRail()
        rail.add("issues.list", {
            "data": {
                "issues": {
                    "nodes": [{
                        "id": "linear-1",
                        "identifier": "SYN-1",
                        "title": "Synthetic Linear issue",
                        "description": "First revision",
                        "url": "https://linear.example.invalid/issue/SYN-1",
                        "createdAt": "2026-07-18T01:00:00Z",
                        "updatedAt": "2026-07-18T02:00:00Z",
                        "state": {"name": "Started"},
                        "assignee": {"id": "person-1"},
                        "labels": {"nodes": [{"name": "synthetic"}]},
                    }],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            },
        }, {
            "errors": [{"message": "private provider detail"}],
        })
        connector = LinearActivityConnector(
            rail=rail,
            source_id="synthetic:linear:activity",
            team_id="team-synthetic",
        )
        page = connector.pull(None)
        self.assertEqual(page.records[0].content["document_id"], "linear:linear-1")
        variables = rail.calls[0][1]["json_body"]["variables"]
        self.assertEqual(variables["team_id"], "team-synthetic")
        with self.assertRaisesRegex(
            ConnectorUpstreamError,
            "^connector_upstream_error$",
        ) as raised:
            connector.pull(page.next_cursor)
        self.assertNotIn("private provider detail", str(raised.exception))


class SlackConnectorTest(unittest.TestCase):
    def test_opaque_pagination_edit_and_explicit_delete_only(self):
        rail = FakeRail()
        rail.add("messages.history", {
            "ok": True,
            "messages": [{
                "type": "message",
                "ts": "1784332800.000100",
                "user": "U1",
                "text": "Synthetic Slack message",
            }, {
                "type": "message",
                "subtype": "message_changed",
                "ts": "1784332801.000200",
                "message": {
                    "ts": "1784332800.000100",
                    "user": "U1",
                    "text": "Edited synthetic Slack message",
                    "edited": {"ts": "1784332801.000200"},
                },
            }, {
                "type": "message",
                "subtype": "message_deleted",
                "ts": "1784332802.000300",
                "deleted_ts": "1784332799.000050",
            }],
            "has_more": True,
            "response_metadata": {"next_cursor": "opaque-page-2"},
        }, {
            "ok": True,
            "messages": [],
            "has_more": False,
            "response_metadata": {"next_cursor": ""},
        })
        connector = SlackMessagesConnector(
            rail=rail,
            source_id="synthetic:slack:messages",
            workspace_id="T123",
            channel_id="C123",
        )
        first = connector.pull(None)
        self.assertTrue(first.has_more)
        self.assertEqual(
            [(record.native_id, record.deleted) for record in first.records],
            [
                ("slack:T123:C123:1784332800.000100", False),
                ("slack:T123:C123:1784332799.000050", True),
            ],
        )
        self.assertEqual(first.records[0].content["edited_at"], "2026-07-18T00:00:01.000200Z")
        second = connector.pull(first.next_cursor)
        self.assertFalse(second.has_more)
        self.assertEqual(
            rail.calls[1][1]["query"]["cursor"],
            "opaque-page-2",
        )

    def test_invalid_opaque_cursor_reconciles_from_acknowledged_watermark(self):
        rail = FakeRail()
        rail.add(
            "messages.history",
            {
                "ok": False,
                "error": "invalid_cursor",
            },
            {
                "ok": True,
                "messages": [],
                "has_more": False,
                "response_metadata": {"next_cursor": ""},
            },
        )
        connector = SlackMessagesConnector(
            rail=rail,
            source_id="synthetic:slack:messages",
            workspace_id="T123",
            channel_id="C123",
        )
        cursor = json.dumps({
            "v": 1,
            "page": "expired-page",
            "watermark": "2026-07-18T00:00:00Z",
            "max_seen": "2026-07-18T00:00:00Z",
            "cycle": 1,
        })
        page = connector.pull(cursor)
        self.assertFalse(page.has_more)
        self.assertEqual(rail.calls[0][1]["query"]["cursor"], "expired-page")
        self.assertNotIn("cursor", rail.calls[1][1]["query"])
        self.assertEqual(
            rail.calls[1][1]["query"]["oldest"],
            "1784332800.000000",
        )


class NotionConnectorTest(unittest.TestCase):
    def test_search_pagination_and_only_in_trash_is_a_tombstone(self):
        rail = FakeRail()
        rail.add("search.list", {
            "object": "list",
            "results": [{
                "object": "page",
                "id": "page-1",
                "url": "https://notion.example.invalid/page-1",
                "created_time": "2026-07-18T01:00:00Z",
                "last_edited_time": "2026-07-18T02:00:00Z",
                "in_trash": False,
                "properties": {
                    "Name": {
                        "type": "title",
                        "title": [{"plain_text": "Synthetic Notion page"}],
                    },
                },
            }, {
                "object": "page",
                "id": "page-2",
                "url": "https://notion.example.invalid/page-2",
                "created_time": "2026-07-18T01:00:00Z",
                "last_edited_time": "2026-07-18T03:00:00Z",
                "in_trash": True,
                "properties": {},
            }],
            "has_more": True,
            "next_cursor": "notion-page-2",
        }, {
            "object": "list",
            "results": [],
            "has_more": False,
            "next_cursor": None,
        })
        connector = NotionWorkspaceConnector(
            rail=rail,
            source_id="synthetic:notion:workspace",
            page_size=2,
        )
        first = connector.pull(None)
        self.assertEqual(
            [(record.native_id, record.deleted) for record in first.records],
            [("notion:page-1", False), ("notion:page-2", True)],
        )
        self.assertEqual(first.records[0].content["name"], "Synthetic Notion page")
        second = connector.pull(first.next_cursor)
        self.assertFalse(second.has_more)
        self.assertEqual(
            rail.calls[1][1]["json_body"]["start_cursor"],
            "notion-page-2",
        )


class WorkApiRunnerTest(unittest.TestCase):
    def test_all_four_sources_keep_cursor_behind_ack_and_scrub_before_spool(self):
        cases = []
        github = FakeRail()
        github.add("issues.list", [github_issue(1, body="api_key=private-canary")])
        cases.append(GitHubActivityConnector(
            rail=github,
            source_id="synthetic:github:runner",
            owner="synthetic-org",
            repository="synthetic-repo",
        ))

        linear = FakeRail()
        linear.add("issues.list", {"data": {"issues": {
            "nodes": [{
                "id": "l1", "identifier": "SYN-1", "title": "Synthetic",
                "description": "api_key=private-canary",
                "createdAt": "2026-07-18T01:00:00Z",
                "updatedAt": "2026-07-18T02:00:00Z",
                "state": {"name": "Open"}, "labels": {"nodes": []},
            }],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }}})
        cases.append(LinearActivityConnector(
            rail=linear,
            source_id="synthetic:linear:runner",
            team_id="team-synthetic",
        ))

        slack = FakeRail()
        slack.add("messages.history", {
            "ok": True,
            "messages": [{
                "type": "message", "ts": "1784332800.000100",
                "user": "U1", "text": "api_key=private-canary",
            }],
            "has_more": False,
            "response_metadata": {"next_cursor": ""},
        })
        cases.append(SlackMessagesConnector(
            rail=slack,
            source_id="synthetic:slack:runner",
            workspace_id="T123",
            channel_id="C123",
        ))

        notion = FakeRail()
        notion.add("search.list", {
            "object": "list",
            "results": [{
                "object": "page", "id": "page-1",
                "created_time": "2026-07-18T01:00:00Z",
                "last_edited_time": "2026-07-18T02:00:00Z",
                "in_trash": False,
                "properties": {"Name": {"type": "title", "title": [
                    {"plain_text": "api_key=private-canary"},
                ]}},
            }],
            "has_more": False, "next_cursor": None,
        })
        cases.append(NotionWorkspaceConnector(
            rail=notion,
            source_id="synthetic:notion:runner",
        ))

        for connector in cases:
            with self.subTest(connector=connector.connector_id):
                brain = FakeBrain()
                brain.fail_after_commit = True
                with tempfile.TemporaryDirectory() as directory:
                    spool = Path(directory) / "spool.db"
                    runner = ConnectorRunner(
                        connector=connector,
                        brain=brain,
                        spool_path=spool,
                        privacy=PrivacyPolicy(mode="scrub"),
                    )
                    with self.assertRaisesRegex(ConnectorRunError, "brain_unavailable"):
                        runner.run_once()
                    self.assertFalse(runner.doctor()["checkpointed"])
                    self.assertEqual(runner.doctor()["pending"], 1)
                    self.assertNotIn("private-canary", spool.read_bytes().decode(errors="ignore"))
                    self.assertEqual(runner.run_once()["replayed"], 1)
                    self.assertTrue(runner.doctor()["checkpointed"])
                    rendered = json.dumps(tuple(brain.events.values()), sort_keys=True)
                    self.assertNotIn("private-canary", rendered)
                    runner.close()


if __name__ == "__main__":
    unittest.main()
