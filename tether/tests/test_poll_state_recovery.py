from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import logging
import os
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"

TEAM = "T11111111"
CHANNEL = "D11111111"


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class PollClient:
    def __init__(self, responder):
        self.responder = responder
        self.requests: list[dict[str, object]] = []

    async def conversations_replies(self, **kwargs):
        self.requests.append(dict(kwargs))
        return self.responder(dict(kwargs))

    async def users_info(self, *, user):
        return {
            "ok": True,
            "user": {
                "id": user,
                "is_bot": False,
                "deleted": False,
            },
        }


class PollAdapter:
    def __init__(self, client: PollClient):
        self.client = client
        self.handled: list[dict[str, object]] = []

    def _get_client(self, _channel, team_id=None):
        if team_id != TEAM:
            raise RuntimeError("unexpected workspace")
        return self.client

    async def _handle_slack_message(self, event):
        self.handled.append(dict(event))


class PollStateRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "HOME": str(self.home),
                "HERMES_HOME": str(self.home / ".hermes"),
                "XDG_DATA_HOME": str(self.home / ".local" / "share"),
                "XDG_CONFIG_HOME": str(self.home / ".config"),
                "TETHER_REPLY_RECOVERY_HOURS": "24",
                "TETHER_REPLY_POLL_BATCH": "10",
                "TETHER_REPLY_POLL_MAX_PAGES": "25",
            },
            clear=False,
        )
        self.env_patch.start()
        self.runtime = load_module(
            f"bridge_runtime_poll_recovery_{id(self)}",
            RUNTIME_PATH,
        )
        self.db_path = self.home / ".hermes" / "bridges.db"
        self.store = self.runtime.Store(self.db_path)
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)
        self.env_patch.stop()
        self.temp.cleanup()

    def load_plugin(self, store=None):
        sys.modules["bridge_runtime"] = self.runtime
        for name in (
            "tether_routing",
            "tether_hermes_compat",
            "tether_slack_protocol",
        ):
            sys.modules.pop(name, None)
        plugin = load_module(
            f"tether_poll_recovery_{id(self)}_{len(sys.modules)}",
            PLUGIN_PATH,
        )
        plugin.store = store or self.store
        plugin.state.store = plugin.store
        return plugin

    def mark(self, *threads: str) -> None:
        for thread_ts in threads:
            self.store.mark_participation(TEAM, CHANNEL, thread_ts)

    def poll(self, plugin, client: PollClient, adapter_type=PollAdapter) -> int:
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=CURRENT_TIMESTAMP
                """
            )
        return asyncio.run(
            plugin._poll_recent_replies(adapter_type(client))
        )

    def table_rows(self, table: str) -> list[tuple]:
        allowed = {
            "slack_reply_poll_state",
            "slack_reply_poll_rotation",
            "slack_reply_poll_scheduler",
        }
        if table not in allowed:
            raise ValueError("unsupported test table")
        with self.store.connect() as database:
            return [
                tuple(row)
                for row in database.execute(
                    f"SELECT * FROM {table} ORDER BY 1,2,3"
                ).fetchall()
            ]

    def test_restart_resumes_cursor_page_oldest_and_page_count(self):
        thread = "1785000000.000001"
        self.mark(thread)
        first_client = PollClient(
            lambda _request: {
                "ok": True,
                "messages": [
                    {
                        "ts": "1785000001.000001",
                        "thread_ts": thread,
                        "text": "recover after restart",
                        "user": "U11111111",
                    }
                ],
                "response_metadata": {"next_cursor": "cursor-page-2"},
            }
        )
        first_plugin = self.load_plugin()

        self.assertEqual(self.poll(first_plugin, first_client), 0)
        first_request = first_client.requests[0]
        persisted = self.store.reply_poll_page_state(
            TEAM,
            CHANNEL,
            thread,
        )
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.next_cursor, "cursor-page-2")
        self.assertEqual(persisted.pages_seen, 1)
        self.assertEqual(persisted.page_oldest, first_request["oldest"])
        self.assertEqual(
            tuple(message["ts"] for message in persisted.pending_messages),
            ("1785000001.000001",),
        )

        restarted_store = self.runtime.Store(self.db_path)
        restarted_plugin = self.load_plugin(restarted_store)
        second_client = PollClient(
            lambda _request: {
                "ok": True,
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            }
        )

        self.assertEqual(self.poll(restarted_plugin, second_client), 0)
        second_request = second_client.requests[0]
        self.assertEqual(second_request["cursor"], "cursor-page-2")
        self.assertEqual(second_request["oldest"], first_request["oldest"])
        self.assertIsNone(
            restarted_store.reply_poll_page_state(TEAM, CHANNEL, thread)
        )

    def test_schema_12_adds_durable_pending_message_buffer(self):
        legacy_path = self.home / ".hermes" / "legacy-schema-12.db"
        self.runtime.Store(legacy_path)
        with contextlib.closing(sqlite3.connect(legacy_path)) as database, database:
            database.execute(
                "ALTER TABLE slack_reply_poll_state "
                "DROP COLUMN pending_messages_json"
            )
            database.execute("PRAGMA user_version=12")

        migrated = self.runtime.Store(legacy_path)
        with migrated.connect() as database:
            columns = {
                row["name"]
                for row in database.execute(
                    "PRAGMA table_info(slack_reply_poll_state)"
                )
            }
            version = int(
                database.execute("PRAGMA user_version").fetchone()[0]
            )
        self.assertIn("pending_messages_json", columns)
        self.assertEqual(version, 15)

    def test_corrupt_pending_message_buffer_fails_closed(self):
        thread = "1785000000.000001"
        self.store.save_reply_poll_page_state(
            TEAM,
            CHANNEL,
            thread,
            next_cursor="cursor-page-2",
            seen_cursors=("cursor-page-2",),
            pages_seen=1,
            page_oldest="1780000000.000000",
            pending_messages=(
                {
                    "ts": "1785000001.000001",
                    "thread_ts": thread,
                    "text": "continue",
                    "user": "U11111111",
                },
            ),
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reply_poll_state
                SET pending_messages_json='{"not":"a list"}'
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                (TEAM, CHANNEL, thread),
            )
        with self.assertRaisesRegex(
            RuntimeError,
            "stored Slack reply poll state is invalid",
        ):
            self.store.reply_poll_page_state(TEAM, CHANNEL, thread)

    def test_final_page_dispatch_failure_keeps_buffered_cursor_for_retry(self):
        thread = "1785000000.000001"
        self.mark(thread)

        def respond(request):
            if request.get("cursor"):
                return {
                    "ok": True,
                    "messages": [
                        {
                            "ts": "1785000002.000001",
                            "thread_ts": thread,
                            "text": "final page",
                            "user": "U11111111",
                        }
                    ],
                    "response_metadata": {"next_cursor": ""},
                }
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1785000001.000001",
                        "thread_ts": thread,
                        "text": "buffer this page",
                        "user": "U11111111",
                    }
                ],
                "response_metadata": {"next_cursor": "cursor-page-2"},
            }

        client = PollClient(respond)
        plugin = self.load_plugin()

        async def admitted(_adapter, _event):
            return types.SimpleNamespace(
                action=plugin.routing.RouteAction.HERMES,
            )

        class FailingAdapter(PollAdapter):
            async def _handle_slack_message(self, _event):
                raise RuntimeError("synthetic dispatch crash")

        with mock.patch.object(
            plugin,
            "_route_slack_event",
            side_effect=admitted,
        ):
            self.poll(plugin, client)
            with self.assertRaisesRegex(RuntimeError, "synthetic dispatch crash"):
                self.poll(plugin, client, FailingAdapter)
        persisted = self.store.reply_poll_page_state(
            TEAM,
            CHANNEL,
            thread,
        )
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.next_cursor, "cursor-page-2")
        self.assertEqual(
            tuple(message["ts"] for message in persisted.pending_messages),
            ("1785000001.000001",),
        )

        with mock.patch.object(
            plugin,
            "_route_slack_event",
            side_effect=admitted,
        ):
            self.poll(plugin, client)
        self.assertIsNone(
            self.store.reply_poll_page_state(TEAM, CHANNEL, thread)
        )
        self.assertEqual(
            [request.get("cursor") for request in client.requests],
            [None, "cursor-page-2", "cursor-page-2"],
        )

    def test_round_robin_survives_target_removal_and_insertion(self):
        threads = [
            "1785000000.000001",
            "1785000000.000002",
            "1785000000.000003",
        ]
        self.mark(*threads)
        client = PollClient(
            lambda _request: {
                "ok": True,
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            }
        )
        plugin = self.load_plugin()

        self.poll(plugin, client)
        self.poll(plugin, client)
        with self.store.connect() as database:
            database.execute(
                """
                DELETE FROM thread_participation
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                (TEAM, CHANNEL, threads[1]),
            )
        inserted = "1784000000.000001"
        self.mark(inserted)
        self.poll(plugin, client)
        self.poll(plugin, client)
        self.poll(plugin, client)

        self.assertEqual(
            [request["ts"] for request in client.requests],
            [threads[0], threads[1], threads[2], inserted, threads[0]],
        )
        self.assertEqual(len(client.requests), 5)

    def test_invalid_and_repeated_cursors_do_not_starve_other_threads(self):
        repeated = "1785000000.000001"
        invalid = "1785000000.000002"
        healthy = "1785000000.000003"
        self.mark(repeated, invalid, healthy)

        def respond(request):
            thread_ts = request["ts"]
            if thread_ts == repeated:
                return {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {
                        "next_cursor": "repeat-me",
                    },
                }
            if thread_ts == invalid:
                return {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": "   "},
                }
            return {
                "ok": True,
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            }

        client = PollClient(respond)
        plugin = self.load_plugin()

        self.poll(plugin, client)
        with self.assertRaisesRegex(RuntimeError, "every Slack thread poll failed"):
            self.poll(plugin, client)
        self.poll(plugin, client)
        with self.assertRaisesRegex(RuntimeError, "every Slack thread poll failed"):
            self.poll(plugin, client)
        with self.assertRaisesRegex(RuntimeError, "every Slack thread poll failed"):
            self.poll(plugin, client)
        self.poll(plugin, client)

        self.assertEqual(
            [request["ts"] for request in client.requests],
            [repeated, invalid, healthy, repeated, invalid, healthy],
        )
        repeated_requests = [
            request for request in client.requests
            if request["ts"] == repeated
        ]
        self.assertNotIn("cursor", repeated_requests[0])
        self.assertEqual(repeated_requests[1]["cursor"], "repeat-me")
        persisted = self.store.reply_poll_page_state(
            TEAM,
            CHANNEL,
            repeated,
        )
        self.assertIsNotNone(persisted)
        self.assertEqual(persisted.next_cursor, "repeat-me")
        self.assertEqual(persisted.pages_seen, 1)
        self.assertIsNone(
            self.store.reply_poll_page_state(TEAM, CHANNEL, invalid)
        )

    def test_disappeared_targets_prune_page_and_rotation_state(self):
        stale = "1785000000.000001"
        current = "1785000000.000002"
        self.mark(stale, current)
        for thread_ts in (stale, current):
            self.store.save_reply_poll_page_state(
                TEAM,
                CHANNEL,
                thread_ts,
                next_cursor=f"cursor-{thread_ts}",
                seen_cursors=(f"cursor-{thread_ts}",),
                pages_seen=1,
                page_oldest="1780000000.000000",
            )
        with self.store.connect() as database:
            database.execute(
                """
                DELETE FROM thread_participation
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                (TEAM, CHANNEL, stale),
            )
        plugin = self.load_plugin()
        client = PollClient(
            lambda _request: {
                "ok": True,
                "messages": [],
                "response_metadata": {"next_cursor": "cursor-current-next"},
            }
        )

        self.poll(plugin, client)
        self.assertIsNone(
            self.store.reply_poll_page_state(TEAM, CHANNEL, stale)
        )
        self.assertIsNotNone(
            self.store.reply_poll_page_state(TEAM, CHANNEL, current)
        )

        with self.store.connect() as database:
            database.execute(
                "DELETE FROM thread_participation WHERE team_id=?",
                (TEAM,),
            )
        self.poll(plugin, PollClient(lambda _request: self.fail("no poll expected")))

        self.assertEqual(self.table_rows("slack_reply_poll_state"), [])
        self.assertEqual(self.table_rows("slack_reply_poll_rotation"), [])
        self.assertEqual(self.table_rows("slack_reply_poll_scheduler"), [])

    def test_reply_poller_and_reconciliation_share_one_workspace_budget(self):
        thread = "1785000000.000001"
        self.mark(thread)
        plugin = self.load_plugin()
        first_client = PollClient(
            lambda _request: {
                "ok": True,
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            }
        )

        self.assertEqual(
            asyncio.run(
                plugin._poll_recent_replies(PollAdapter(first_client))
            ),
            0,
        )
        self.assertEqual(len(first_client.requests), 1)

        key = self.store.reconciliation_key(
            target_kind="reply",
            team_id=TEAM,
            channel_id=CHANNEL,
            thread_ts=thread,
            target_id="client-message-id",
        )
        self.store.ensure_reconciliation(
            reconciliation_key=key,
            team_id=TEAM,
            method="conversations.replies",
            channel_id=CHANNEL,
            thread_ts=thread,
            target_kind="reply",
            target_id="client-message-id",
        )
        self.assertEqual(
            self.store.claim_reconciliation_page(key)["status"],
            "waiting",
        )

    def test_multi_page_thread_persists_complete_ownership_before_dispatch(self):
        thread = "1785000000.000001"
        bot_user = "U99999999"
        human_user = "U11111111"
        bridge_id = "brg_1234567890abcdef"
        self.mark(thread)

        def respond(request):
            if not request.get("cursor"):
                return {
                    "ok": True,
                    "messages": [
                        {
                            "ts": thread,
                            "text": "",
                            "user": bot_user,
                            "bot_id": "B99999999",
                            "metadata": {
                                "event_type": "tether_root",
                                "event_payload": {"bridge_id": bridge_id},
                            },
                        },
                        {
                            "ts": "1785000000.500001",
                            "thread_ts": thread,
                            "text": "early reply must survive pagination",
                            "user": human_user,
                        },
                    ],
                    "response_metadata": {"next_cursor": "page-2"},
                }
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1785000001.000001",
                        "thread_ts": thread,
                        "text": "continue without a mention",
                        "user": human_user,
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }

        client = PollClient(respond)
        plugin = self.load_plugin()
        observed = []

        class InspectingAdapter(PollAdapter):
            async def _handle_slack_message(self, event):
                await plugin._route_slack_event(self, event)
                self.handled.append(dict(event))

        async def inspect_snapshot(_adapter, event):
            key = (TEAM, CHANNEL, thread)
            observed.append(
                (
                    event["ts"],
                    plugin.state.thread_bot_participants.get(key),
                    plugin.state.thread_root_bridges.get(key),
                )
            )
            return types.SimpleNamespace(
                action=plugin.routing.RouteAction.SILENT,
            )

        with mock.patch.object(
            plugin,
            "_route_slack_event",
            side_effect=inspect_snapshot,
        ):
            self.poll(plugin, client, InspectingAdapter)
            persisted = self.store.reply_poll_page_state(
                TEAM,
                CHANNEL,
                thread,
            )
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.bot_user_ids, (bot_user,))
            self.assertEqual(persisted.root_bridge_id, bridge_id)
            self.assertEqual(
                tuple(
                    message["ts"]
                    for message in persisted.pending_messages
                ),
                ("1785000000.500001",),
            )
            self.poll(plugin, client, InspectingAdapter)

        self.assertEqual(
            [item[0] for item in observed],
            ["1785000000.500001", "1785000001.000001"],
        )
        for _message_ts, participants, root in observed:
            self.assertEqual(participants[1], frozenset({bot_user}))
            self.assertEqual(root[1], bridge_id)
        self.assertIsNone(
            self.store.reply_poll_page_state(TEAM, CHANNEL, thread)
        )


if __name__ == "__main__":
    unittest.main()
