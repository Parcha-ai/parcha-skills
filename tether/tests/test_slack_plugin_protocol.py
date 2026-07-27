from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import sys
import tempfile
import time
import types
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"

TEAM_A = "T11111111"
TEAM_B = "T22222222"
CHANNEL = "C11111111"
THREAD = "1785000000.000001"
HUMAN = "UHUMAN001"
LOCAL = "UBOTAAAA1"


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class SlackPluginProtocolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        runtime_name = f"bridge_runtime_slack_protocol_{id(self)}"
        self.runtime = load_module(runtime_name, RUNTIME_PATH)
        sys.modules["bridge_runtime"] = self.runtime
        for name in (
            "tether_routing",
            "tether_hermes_compat",
            "tether_slack_protocol",
        ):
            sys.modules.pop(name, None)
        plugin_name = f"tether_slack_plugin_protocol_{id(self)}"
        self.plugin = load_module(plugin_name, PLUGIN_PATH)
        self.plugin.store = self.runtime.Store(
            pathlib.Path(self.temp.name) / "bridges.db"
        )
        self.plugin.state.store = self.plugin.store

    def tearDown(self):
        self.temp.cleanup()

    def test_mutation_gate_ignores_metadata_only_edits(self):
        previous = {
            "type": "message",
            "user": HUMAN,
            "text": "same text",
            "blocks": [],
            "attachments": [],
            "files": [],
            "ts": "1785000001.000001",
            "thread_ts": THREAD,
        }
        current = dict(previous)
        current["metadata"] = {
            "event_type": "tether_reply",
            "event_payload": {"bridge_id": "brg_123"},
        }
        event = {
            "type": "message",
            "subtype": "message_changed",
            "channel": CHANNEL,
            "event_ts": "1785000002.000001",
            "message": current,
            "previous_message": previous,
        }

        self.assertFalse(
            self.plugin._normalize_slack_mutation(
                event,
                {"team_id": TEAM_A},
            )
        )
        self.assertEqual(event["_tether_mutation_disposition"], "ignore")
        self.assertEqual(event["_tether_mutation_reason"], "metadata_only_edit")
        self.assertNotIn("_tether_mutation", event)

    def test_minimal_delete_is_not_given_invented_identity(self):
        event = {
            "type": "message",
            "subtype": "message_deleted",
            "channel": CHANNEL,
            "deleted_ts": "1785000001.000001",
            "event_ts": "1785000002.000001",
        }

        self.assertFalse(
            self.plugin._normalize_slack_mutation(
                event,
                {"team_id": TEAM_A},
            )
        )
        self.assertEqual(event["_tether_mutation_disposition"], "invalid")
        self.assertEqual(
            event["_tether_mutation_reason"],
            "mutation_identity_unresolved",
        )
        self.assertNotIn("user", event)
        self.assertNotIn("thread_ts", event)
        self.assertEqual(event["subtype"], "message_deleted")

    def test_minimal_delete_uses_exact_durable_original_identity(self):
        target_ts = "1785000001.000001"
        self.plugin.store.claim_thread_ingress(
            f"slack:{TEAM_A}:{CHANNEL}:{target_ts}",
            TEAM_A,
            CHANNEL,
            THREAD,
            route_action="hermes",
            writer_id="hermes",
            payload={
                "text": "original",
                "user": HUMAN,
                "message_ts": target_ts,
                "event_thread_ts": THREAD,
            },
        )
        event = {
            "type": "message",
            "subtype": "message_deleted",
            "channel": CHANNEL,
            "deleted_ts": target_ts,
            "event_ts": "1785000002.000001",
        }

        self.assertTrue(
            self.plugin._normalize_slack_mutation(
                event,
                {"team_id": TEAM_A},
            )
        )
        self.assertEqual(event["user"], HUMAN)
        self.assertEqual(event["thread_ts"], THREAD)
        self.assertEqual(
            event["_tether_mutation"],
            {
                "kind": "delete",
                "target_ts": target_ts,
                "replacement_text": "",
            },
        )

    def test_complete_edit_uses_canonical_identity_and_replacement(self):
        event = {
            "type": "message",
            "subtype": "message_changed",
            "channel": CHANNEL,
            "event_ts": "1785000002.000001",
            "message": {
                "type": "message",
                "user": HUMAN,
                "text": "new instruction",
                "blocks": [],
                "ts": "1785000001.000001",
                "thread_ts": THREAD,
            },
            "previous_message": {
                "type": "message",
                "user": HUMAN,
                "text": "old instruction",
                "blocks": [],
                "ts": "1785000001.000001",
                "thread_ts": THREAD,
            },
        }

        self.assertTrue(
            self.plugin._normalize_slack_mutation(
                event,
                {"team_id": TEAM_A, "event_id": "Ev123"},
            )
        )
        self.assertEqual(event["user"], HUMAN)
        self.assertEqual(event["thread_ts"], THREAD)
        self.assertEqual(event["ts"], "1785000002.000001")
        self.assertIn("new instruction", event["text"])
        self.assertEqual(
            event["_tether_mutation"],
            {
                "kind": "edit",
                "target_ts": "1785000001.000001",
                "replacement_text": "new instruction",
            },
        )

    def test_async_api_call_coordinates_retry_after_by_workspace_and_method(self):
        calls = 0

        class Response:
            status_code = 429
            headers = {"Retry-After": "7"}

        class RateLimited(Exception):
            response = Response()

        class Coordinator:
            def __init__(self):
                self.waited = []
                self.recorded = []

            async def wait_async(self, key):
                self.waited.append(key)

            def record_429(self, key, headers):
                self.recorded.append((key, dict(headers)))

        coordinator = Coordinator()
        self.plugin.state.slack_retry_after = coordinator

        async def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RateLimited()
            return {"ok": True}

        result = asyncio.run(
            self.plugin._slack_api_call(
                TEAM_A,
                "conversations.replies",
                operation,
            )
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(calls, 2)
        self.assertEqual(len(coordinator.waited), 2)
        self.assertEqual(coordinator.waited[0].workspace_id, TEAM_A)
        self.assertEqual(
            coordinator.waited[0].method,
            "conversations.replies",
        )
        self.assertEqual(
            coordinator.recorded,
            [(coordinator.waited[0], {"Retry-After": "7"})],
        )

    def _routing_adapter(self, client):
        return types.SimpleNamespace(
            _bot_user_id=LOCAL,
            _bot_id="BLOCAL001",
            _team_bot_user_ids={TEAM_A: LOCAL},
            _team_bot_ids={TEAM_A: "BLOCAL001"},
            _channel_team={CHANNEL: TEAM_A},
            _tether_user_kinds={(TEAM_A, HUMAN): "human"},
            _get_client=lambda _channel, team_id=None: client,
            config=types.SimpleNamespace(extra={}),
        )

    def _participating_store(self):
        class ParticipatingStore:
            def find(self, *_args):
                return None

            def recent_participating_threads(self, **_kwargs):
                return [(TEAM_A, CHANNEL, THREAD, time.time())]

        return ParticipatingStore()

    def test_explicit_bot_mention_skips_participant_history(self):
        class Client:
            def __init__(self):
                self.reply_calls = 0

            async def conversations_replies(self, **_kwargs):
                self.reply_calls += 1
                raise AssertionError("explicit mention fetched thread history")

        client = Client()
        adapter = self._routing_adapter(client)
        self.plugin.store = self._participating_store()
        self.plugin.state.store = self.plugin.store
        self.plugin.effective_allowed_users = lambda: {HUMAN}
        event = {
            "type": "message",
            "channel": CHANNEL,
            "team": TEAM_A,
            "channel_type": "channel",
            "thread_ts": THREAD,
            "ts": "1785000003.000001",
            "user": HUMAN,
            "text": f"<@{LOCAL}> investigate",
        }

        decision = asyncio.run(
            self.plugin._route_slack_event(adapter, event)
        )

        self.assertIsNotNone(decision)
        self.assertEqual(
            decision.action,
            self.plugin.routing.RouteAction.HERMES,
        )
        self.assertEqual(decision.reason, "self_explicitly_targeted")
        self.assertEqual(client.reply_calls, 0)

    def test_ambient_routing_fails_closed_on_incomplete_history(self):
        class Client:
            def __init__(self):
                self.requests = []

            async def conversations_replies(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "ok": True,
                    "messages": [
                        {
                            "ts": THREAD,
                            "user": LOCAL,
                            "bot_id": "BLOCAL001",
                            "text": "root",
                        }
                    ],
                    "response_metadata": {"next_cursor": "page-two"},
                }

        client = Client()
        adapter = self._routing_adapter(client)
        self.plugin.store = self._participating_store()
        self.plugin.state.store = self.plugin.store
        self.plugin.effective_allowed_users = lambda: {HUMAN}
        event = {
            "type": "message",
            "channel": CHANNEL,
            "team": TEAM_A,
            "channel_type": "channel",
            "thread_ts": THREAD,
            "ts": "1785000003.000002",
            "user": HUMAN,
            "text": "ambient follow-up",
        }

        decision = asyncio.run(
            self.plugin._route_slack_event(adapter, event)
        )

        self.assertIsNotNone(decision)
        self.assertEqual(
            decision.action,
            self.plugin.routing.RouteAction.SILENT,
        )
        self.assertEqual(decision.reason, "not_confidently_addressed")
        self.assertEqual(len(client.requests), 1)
        self.assertEqual(client.requests[0]["limit"], 15)
        self.assertNotIn("cursor", client.requests[0])

    def test_malformed_cursor_fails_closed_without_followup_request(self):
        class Client:
            def __init__(self):
                self.requests = []

            async def conversations_replies(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {"next_cursor": "   "},
                }

        client = Client()
        adapter = self._routing_adapter(client)

        participants = asyncio.run(
            self.plugin._thread_bot_users(
                adapter,
                TEAM_A,
                CHANNEL,
                THREAD,
                LOCAL,
            )
        )

        self.assertIsNone(participants)
        self.assertEqual(len(client.requests), 1)
        self.assertNotIn(
            (TEAM_A, CHANNEL, THREAD),
            self.plugin.state.thread_bot_participants,
        )

    def test_recovery_poll_fetches_one_small_page_per_workspace(self):
        bridges = [
            types.SimpleNamespace(
                bridge_id="brg_a1",
                team_id=TEAM_A,
                channel_id="D11111111",
                thread_ts="1785000100.000001",
            ),
            types.SimpleNamespace(
                bridge_id="brg_a2",
                team_id=TEAM_A,
                channel_id="D11111112",
                thread_ts="1785000100.000002",
            ),
            types.SimpleNamespace(
                bridge_id="brg_b1",
                team_id=TEAM_B,
                channel_id="D22222221",
                thread_ts="1785000100.000003",
            ),
        ]

        class PollStore:
            def __init__(self):
                self.pages = {}

            def active_bridges(self):
                return bridges

            def recent_participating_threads(self, **_kwargs):
                return []

            def claim_slack_read_budget(self, *_args):
                return True

            def select_reply_poll_targets(self, targets, **_kwargs):
                selected = {}
                for target in targets:
                    selected.setdefault(target[0], target)
                return list(selected.values())

            def reply_poll_page_state(self, *key):
                return self.pages.get(key)

            def save_reply_poll_page_state(self, *key, **values):
                self.pages[key] = types.SimpleNamespace(**values)

            def clear_reply_poll_page_state(self, *key):
                self.pages.pop(key, None)

        class Client:
            def __init__(self):
                self.requests = []

            async def conversations_replies(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "ok": True,
                    "messages": [],
                    "response_metadata": {
                        "next_cursor": f"next-{kwargs['ts']}",
                    },
                }

        clients = {TEAM_A: Client(), TEAM_B: Client()}

        class Adapter:
            _channel_team = {
                bridge.channel_id: bridge.team_id for bridge in bridges
            }

            def _get_client(self, channel, team_id=None):
                return clients[team_id]

        poll_store = PollStore()
        self.plugin.store = poll_store
        self.plugin.state.store = self.plugin.store
        recovered = asyncio.run(
            self.plugin._poll_recent_replies(Adapter())
        )

        self.assertEqual(recovered, 0)
        self.assertEqual(len(clients[TEAM_A].requests), 1)
        self.assertEqual(len(clients[TEAM_B].requests), 1)
        for request in (
            clients[TEAM_A].requests[0],
            clients[TEAM_B].requests[0],
        ):
            self.assertEqual(request["limit"], 15)
            self.assertTrue(request["include_all_metadata"])
            self.assertNotIn("cursor", request)
        self.assertEqual(len(poll_store.pages), 2)


if __name__ == "__main__":
    unittest.main()
