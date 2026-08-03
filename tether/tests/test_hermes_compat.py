from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"
NOTIFIER_PATH = ROOT / "skills" / "tether" / "scripts" / "tether_notify.py"

# Semantic target: NousResearch/hermes-agent 0.19.0 at this audited commit.
HERMES_SHA = "b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5"

TEAM_PRIMARY = "TPRIMARY1"
TEAM_TARGET = "TTARGET01"
CHANNEL = "CSHARED01"
GROUP_CHANNEL = "GSHARED01"
THREAD = "1785000000.000001"
MESSAGE_TS = "1785000001.000001"
HUMAN = "UHUMAN001"
LOCAL_BOT = "ULOCAL001"
LOCAL_APP = "BLOCAL001"
UNLABELED_BOT = "UUNLABEL1"
MENTIONED_BOT = "UMENTION1"


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class WorkspaceClient:
    def __init__(
        self,
        *,
        conversation: dict | None = None,
        users: dict[str, bool] | None = None,
        replies: list[dict] | None = None,
    ):
        self.conversation = conversation or {"is_channel": True}
        self.users = users or {}
        self.replies = replies or []

    async def conversations_info(self, **_kwargs):
        return {"channel": dict(self.conversation)}

    async def users_info(self, *, user):
        return {"user": {"id": user, "is_bot": self.users.get(user, False)}}

    async def conversations_replies(self, **_kwargs):
        return {
            "messages": list(self.replies),
            "response_metadata": {"next_cursor": ""},
        }

    async def conversations_history(self, **_kwargs):
        return {"ok": True, "messages": []}

    async def conversations_join(self, **_kwargs):
        return {"ok": True}


class AmbiguousWorkspaceAdapter:
    """Model Hermes 0.19.0's primary-client fallback for ambiguous channels."""

    def __init__(self, primary: WorkspaceClient, target: WorkspaceClient):
        self.primary = primary
        self.target = target
        self.client_calls: list[tuple[str, str | None]] = []
        self._bot_user_id = LOCAL_BOT
        self._bot_id = LOCAL_APP
        self._team_bot_user_ids = {TEAM_TARGET: LOCAL_BOT}
        self._team_bot_ids = {TEAM_TARGET: LOCAL_APP}
        # Hermes removes ambiguous channel mappings. The event team is therefore
        # the only safe workspace selector.
        self._channel_team = {}
        self._bot_message_ts = set()
        self._reacting_message_ids = set()
        self.config = types.SimpleNamespace(extra={})

    def _get_client(self, channel_id, team_id=None):
        self.client_calls.append((channel_id, team_id))
        return self.target if team_id == TEAM_TARGET else self.primary

    async def _resolve_user_is_bot(self, user_id, chat_id="", team_id=""):
        client = self._get_client(chat_id, team_id=team_id or None)
        result = await client.users_info(user=user_id)
        return bool(result["user"]["is_bot"])

    async def _handle_slack_message(self, event, payload=None):
        return event


class HermesCompatibilityTest(unittest.TestCase):
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
                "SLACK_ALLOWED_USERS": "",
                "GATEWAY_ALLOWED_USERS": "",
                "TETHER_ALLOWED_BOT_USERS": "",
                "TETHER_ALLOWED_BOT_IDS": "",
            },
            clear=False,
        )
        self.env_patch.start()

        self.runtime_name = f"bridge_runtime_hermes_compat_{id(self)}"
        self.runtime = load_module(self.runtime_name, RUNTIME_PATH)
        sys.modules["bridge_runtime"] = self.runtime
        sys.modules.pop("tether_routing", None)
        self.plugin_name = f"tether_plugin_hermes_compat_{id(self)}"
        self.plugin = load_module(self.plugin_name, PLUGIN_PATH)
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        self.plugin.state.ready = True
        installed_runtime = self.home / ".local" / "share" / "tether"
        installed_runtime.mkdir(parents=True)
        shutil.copy2(RUNTIME_PATH, installed_runtime / "bridge_runtime.py")
        shutil.copy2(
            ROOT / "runtime" / "security.py",
            installed_runtime / "security.py",
        )
        shutil.copy2(
            ROOT / "runtime" / "hermes_compat.py",
            installed_runtime / "hermes_compat.py",
        )
        shutil.copy2(
            ROOT / "runtime" / "routing.py",
            installed_runtime / "routing.py",
        )
        shutil.copy2(
            ROOT / "runtime" / "slack_protocol.py",
            installed_runtime / "slack_protocol.py",
        )
        self.notifier_name = f"tether_notifier_hermes_compat_{id(self)}"
        self.notifier = load_module(self.notifier_name, NOTIFIER_PATH)

        config = self.home / ".config" / "tether" / "config.toml"
        config.parent.mkdir(parents=True)
        config.write_text(
            'allowed_users = ["UHUMAN001", "UUNLABEL1"]\n',
            encoding="utf-8",
        )

    def tearDown(self):
        sys.modules.pop("bridge_runtime", None)
        sys.modules.pop("tether_routing", None)
        sys.modules.pop(self.plugin_name, None)
        sys.modules.pop(self.notifier_name, None)
        sys.modules.pop(self.runtime_name, None)
        self.env_patch.stop()
        self.temp.cleanup()

    def adapter(
        self,
        *,
        primary: WorkspaceClient | None = None,
        target: WorkspaceClient | None = None,
    ) -> AmbiguousWorkspaceAdapter:
        return AmbiguousWorkspaceAdapter(
            primary or WorkspaceClient(),
            target or WorkspaceClient(),
        )

    def slack_event(self, **overrides):
        event = {
            "team": TEAM_TARGET,
            "channel": CHANNEL,
            "channel_type": "channel",
            "ts": MESSAGE_TS,
            "text": f"<@{LOCAL_BOT}> investigate",
            "user": HUMAN,
        }
        event.update(overrides)
        return event

    def gateway_event(self):
        class Platform:
            value = "slack"

        platform = Platform()
        decision = self.plugin.routing.RoutingDecision(
            action=self.plugin.routing.RouteAction.HERMES,
            reason="active_hermes_binding",
            message_identity=self.plugin.routing.MessageIdentity(
                TEAM_TARGET,
                CHANNEL,
                MESSAGE_TS,
            ),
            writer_id="bridge:synthetic",
            bridge_id="brg_synthetic",
        )
        source = types.SimpleNamespace(
            platform=platform,
            thread_id=THREAD,
            scope_id=TEAM_TARGET,
            guild_id=TEAM_TARGET,
            chat_id=CHANNEL,
            user_id=HUMAN,
            message_id=MESSAGE_TS,
            is_bot=False,
        )
        event = types.SimpleNamespace(
            source=source,
            message_id=MESSAGE_TS,
            text="continue",
            raw_message={self.plugin.ROUTING_DECISION_KEY: decision},
        )
        adapter = types.SimpleNamespace(_reacting_message_ids=set())
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        return event, gateway

    def test_registered_pre_gateway_hook_fails_closed_when_store_raises(self):
        class HermesLikeHooks:
            def __init__(self):
                self.callbacks = []
                self._manager = self
                self._hooks = {"pre_gateway_dispatch": self.callbacks}

            def register_hook(self, name, callback):
                self.asserted_name = name
                self._hooks.setdefault(name, []).append(callback)

            def invoke_hook(self, **kwargs):
                # Hermes 0.19.0 catches callback exceptions and omits their
                # result, which would otherwise let normal dispatch continue.
                results = []
                for callback in self.callbacks:
                    try:
                        result = callback(**kwargs)
                    except Exception:
                        continue
                    if result is not None:
                        results.append(result)
                return results

        hooks = HermesLikeHooks()
        self.plugin.state.broker = None
        with mock.patch.dict(
            os.environ,
            {"SLACK_BOT_TOKEN": "synthetic-test-token"},
            clear=False,
        ), mock.patch.object(
            self.plugin,
            "start_broker",
            return_value=object(),
        ), mock.patch.object(
            self.plugin,
            "load_config",
        ), mock.patch.object(
            self.plugin,
            "_install_slack_bridge_prefilter",
        ), mock.patch.object(
            self.plugin,
            "_validate_hermes_compatibility",
            return_value="0.19.0",
        ), mock.patch.object(
            self.plugin.store,
            "requeue_processing",
        ), mock.patch.object(
            self.plugin.store,
            "queued_bridge_ids",
            return_value=[],
        ):
            self.plugin.register(hooks)

        event, gateway = self.gateway_event()
        with mock.patch.object(
            self.plugin.store,
            "find",
            side_effect=RuntimeError(
                "synthetic store failure api_key='synthetic-secret-value'"
            ),
        ), mock.patch.object(self.plugin.log, "error") as logged:
            results = hooks.invoke_hook(event=event, gateway=gateway)

        self.assertEqual(hooks.asserted_name, "pre_gateway_dispatch")
        logged.assert_called_once_with(
            "Tether gateway routing failed closed (%s)",
            "RuntimeError",
        )
        self.assertNotIn(
            "synthetic-secret-value",
            repr(logged.call_args),
        )
        self.assertEqual(
            results,
            [
                {
                    "action": "skip",
                    "reason": "tether-routing-internal-error",
                }
            ],
        )

    def test_conversation_lookup_uses_event_workspace(self):
        adapter = self.adapter(
            primary=WorkspaceClient(conversation={"is_im": True}),
            target=WorkspaceClient(conversation={"is_channel": True}),
        )

        kind = asyncio.run(
            self.plugin._conversation_kind(
                adapter,
                {},
                GROUP_CHANNEL,
                TEAM_TARGET,
            )
        )

        self.assertEqual(
            adapter.client_calls,
            [(GROUP_CHANNEL, TEAM_TARGET)],
            "ambiguous channel lookup must not fall back to the primary workspace",
        )
        self.assertEqual(kind, self.plugin.routing.ConversationKind.CHANNEL)

    def test_mention_identity_lookup_uses_event_workspace(self):
        adapter = self.adapter(
            primary=WorkspaceClient(users={MENTIONED_BOT: False}),
            target=WorkspaceClient(users={MENTIONED_BOT: True}),
        )
        policy = self.plugin._routing_policy(adapter, TEAM_TARGET)

        mentioned, bots, humans, unresolved = asyncio.run(
            self.plugin._classify_mentions(
                adapter,
                {"text": f"<@{MENTIONED_BOT}>"},
                TEAM_TARGET,
                CHANNEL,
                policy,
            )
        )

        self.assertEqual(adapter.client_calls, [(CHANNEL, TEAM_TARGET)])
        self.assertEqual(mentioned, frozenset({MENTIONED_BOT}))
        self.assertEqual(bots, frozenset({MENTIONED_BOT}))
        self.assertEqual(humans, frozenset())
        self.assertEqual(unresolved, frozenset())

    def test_thread_history_lookup_uses_event_workspace(self):
        adapter = self.adapter(
            primary=WorkspaceClient(replies=[]),
            target=WorkspaceClient(
                replies=[
                    {
                        "ts": "1785000000.000002",
                        "user": MENTIONED_BOT,
                        "bot_id": "BMENTION1",
                    }
                ]
            ),
        )

        participants = asyncio.run(
            self.plugin._thread_bot_users(
                adapter,
                TEAM_TARGET,
                CHANNEL,
                THREAD,
                LOCAL_BOT,
            )
        )

        self.assertEqual(adapter.client_calls, [(CHANNEL, TEAM_TARGET)])
        self.assertEqual(
            participants,
            frozenset({MENTIONED_BOT}),
        )

    def test_reply_poller_uses_bound_bridge_workspace(self):
        bridge = self.plugin.store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": "compat-run", "cwd": "/tmp/project"},
                "owner_user_id": "*",
                "team_id": TEAM_TARGET,
                "channel_id": CHANNEL,
                "idempotency_key": "hermes-compat-poller",
            }
        )
        self.plugin.store.bind(bridge.bridge_id, THREAD)
        adapter = self.adapter()

        asyncio.run(self.plugin._poll_recent_replies(adapter))

        self.assertEqual(adapter.client_calls, [(CHANNEL, TEAM_TARGET)])

    def test_block_kit_only_user_mention_is_classified(self):
        adapter = self.adapter()
        policy = self.plugin._routing_policy(adapter, TEAM_TARGET)
        event = {
            "text": "",
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {"type": "user", "user_id": LOCAL_BOT}
                            ],
                        }
                    ],
                }
            ],
        }

        mentioned, bots, humans, unresolved = asyncio.run(
            self.plugin._classify_mentions(
                adapter,
                event,
                TEAM_TARGET,
                CHANNEL,
                policy,
            )
        )

        self.assertEqual(mentioned, frozenset({LOCAL_BOT}))
        self.assertEqual(bots, frozenset({LOCAL_BOT}))
        self.assertEqual(humans, frozenset())
        self.assertEqual(unresolved, frozenset())

    def assert_untrusted_bot_origin(self, **event_fields):
        adapter = self.adapter(
            target=WorkspaceClient(users={UNLABELED_BOT: True})
        )
        event = self.slack_event(user=UNLABELED_BOT, **event_fields)

        decision = asyncio.run(
            self.plugin._route_slack_event(adapter, event)
        )

        self.assertEqual(decision.action, self.plugin.routing.RouteAction.SILENT)
        self.assertEqual(decision.reason, "untrusted_peer_bot")

    def test_bot_profile_marks_bot_origin(self):
        self.assert_untrusted_bot_origin(
            bot_profile={"id": "BPROFILE1"},
        )

    def test_app_id_without_client_message_id_marks_bot_origin(self):
        self.assert_untrusted_bot_origin(
            app_id="AAPP00001",
        )

    def test_user_profile_is_bot_marks_bot_origin(self):
        self.assert_untrusted_bot_origin(
            user_profile={"is_bot": True},
        )

    def test_unlabeled_bot_user_id_is_resolved_before_routing(self):
        self.assert_untrusted_bot_origin()

    def test_reaction_suppression_uses_workspace_marker_and_client(self):
        class Platform:
            value = "slack"

        platform = Platform()

        class Adapter:
            def __init__(self):
                self._reacting_message_ids = {(TEAM_TARGET, MESSAGE_TS)}
                self.removed = []

            async def _remove_reaction(
                self,
                channel,
                message_ts,
                reaction,
                team_id="",
            ):
                self.removed.append(
                    (channel, message_ts, reaction, team_id)
                )

        adapter = Adapter()
        source = types.SimpleNamespace(
            platform=platform,
            scope_id=TEAM_TARGET,
            guild_id=TEAM_TARGET,
            chat_id=CHANNEL,
            message_id=MESSAGE_TS,
        )
        event = types.SimpleNamespace(
            source=source,
            message_id=MESSAGE_TS,
        )
        gateway = types.SimpleNamespace(adapters={platform: adapter})

        async def exercise():
            self.plugin._suppress_bridge_reaction(event, gateway)
            await asyncio.sleep(0)

        asyncio.run(exercise())

        self.assertEqual(adapter._reacting_message_ids, set())
        self.assertEqual(
            adapter.removed,
            [(CHANNEL, MESSAGE_TS, "eyes", TEAM_TARGET)],
        )

    def test_setup_uses_hermes_mentions_mode_for_peer_bots(self):
        completed = types.SimpleNamespace(returncode=0)
        snapshot = {
            "config": {
                "slack.allow_bots": (False, ""),
                "display.busy_ack_enabled": (False, ""),
            },
            "config_mutations": [],
        }
        with mock.patch.object(
            self.notifier,
            "_run_hermes",
            return_value=completed,
        ) as run:
            result = self.notifier._configure_peer_agents(
                "/usr/bin/hermes",
                snapshot,
            )

        self.assertEqual(result, 0)
        self.assertEqual(
            snapshot["config_mutations"],
            ["slack.allow_bots", "display.busy_ack_enabled"],
        )
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                [
                    "/usr/bin/hermes",
                    "config",
                    "set",
                    "slack.allow_bots",
                    "mentions",
                ],
                [
                    "/usr/bin/hermes",
                    "config",
                    "set",
                    "display.busy_ack_enabled",
                    "false",
                ],
            ],
        )

    def test_setup_rejects_malformed_rollback_snapshot(self):
        with self.assertRaisesRegex(RuntimeError, "invalid setup snapshot"):
            self.notifier._configure_peer_agents(
                "/usr/bin/hermes",
                {
                    "config": [],
                    "config_mutations": [],
                },
            )


if __name__ == "__main__":
    unittest.main()
