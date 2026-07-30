from __future__ import annotations

import asyncio
import datetime
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"

TEAM = "T12345678"
CHANNEL = "C12345678"
THREAD = "1785000000.000001"
HUMAN = "UHUMAN001"
OTHER_HUMAN = "UHUMAN002"
LOCAL = "UBOTAAAA1"
LOCAL_APP = "BBOTAAAA1"
PEER = "UBOTBBBB2"
PEER_APP = "BBOTBBBB2"
OTHER_BOT = "UBOTCCCC3"


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeClient:
    def __init__(self):
        self.user_types = {
            HUMAN: False,
            OTHER_HUMAN: False,
            LOCAL: True,
            PEER: True,
            OTHER_BOT: True,
        }
        self.thread_messages = {}

    async def users_info(self, *, user):
        if user not in self.user_types:
            raise RuntimeError("identity unavailable")
        return {"user": {"id": user, "is_bot": self.user_types[user]}}

    async def conversations_info(self, **_kwargs):
        return {"channel": {"is_channel": True}}

    async def conversations_replies(self, *, ts, **_kwargs):
        return {"messages": list(self.thread_messages.get(ts, []))}

    async def conversations_history(self, **_kwargs):
        return {"ok": True, "messages": []}

    async def conversations_join(self, **_kwargs):
        return {"ok": True}


class RoutingPluginIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        env = {
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_DATA_HOME": str(self.home / ".local" / "share"),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
            "SLACK_ALLOWED_USERS": "",
            "GATEWAY_ALLOWED_USERS": "",
            "TETHER_ALLOWED_BOT_USERS": PEER,
            "TETHER_ALLOWED_BOT_IDS": PEER_APP,
        }
        self.env_patch = mock.patch.dict(os.environ, env, clear=False)
        self.env_patch.start()

        self.runtime_name = f"bridge_runtime_routing_plugin_{id(self)}"
        self.runtime = load_module(self.runtime_name, RUNTIME_PATH)
        sys.modules["bridge_runtime"] = self.runtime
        sys.modules.pop("tether_routing", None)
        self.plugin_name = f"tether_routing_plugin_{id(self)}"
        self.plugin = load_module(self.plugin_name, PLUGIN_PATH)
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        self.plugin.state.ready = True

        config = self.home / ".config" / "tether" / "config.toml"
        config.parent.mkdir(parents=True)
        config.write_text(f'allowed_users = ["{HUMAN}"]\n', encoding="utf-8")

        self.client = FakeClient()
        client = self.client
        test_case = self

        class SlackAdapter:
            _tether_prefilter = False

            def __init__(self):
                self._bot_user_id = LOCAL
                self._bot_id = LOCAL_APP
                self._team_bot_user_ids = {TEAM: LOCAL}
                self._team_bot_ids = {TEAM: LOCAL_APP}
                self._channel_team = {CHANNEL: TEAM}
                self._bot_message_ts = set()
                self._reacting_message_ids = set()
                self.config = types.SimpleNamespace(extra={})
                self.handled = []
                self.sent = []

            def _get_client(self, _channel, team_id=None):
                return client

            async def connect(self):
                return True

            async def send(self, channel, content, metadata=None):
                self.sent.append((channel, content, metadata))
                return {"ok": True}

            async def edit_message(
                self,
                chat_id,
                message_id,
                content,
                *,
                finalize=False,
                metadata=None,
            ):
                self.sent.append(
                    ("edit", chat_id, message_id, content, finalize, metadata)
                )
                return {"ok": True}

            def _metadata_team_id(self, metadata):
                return str((metadata or {}).get("team_id") or "")

            async def _ensure_dm_conversation(self, chat_id, team_id=None):
                if str(chat_id).startswith(("U", "W")):
                    self._channel_team["D12345678"] = str(team_id or TEAM)
                    return "D12345678"
                return chat_id

            def _is_ignored_channel(self, channel_id):
                return str(channel_id) in set(
                    self.config.extra.get("ignored_channels", [])
                )

            def _pop_slash_context(self, chat_id, team_id=""):
                return None

            def _resolve_thread_ts(self, reply_to=None, metadata=None):
                return str(
                    (metadata or {}).get("thread_id")
                    or (metadata or {}).get("thread_ts")
                    or reply_to
                    or ""
                )

            def _maybe_blocks(self, content):
                return None

            def format_message(self, content):
                return content

            @staticmethod
            def truncate_message(content, max_length=4096):
                return [content[:max_length]]

            async def _handle_slack_message(self, event, payload=None):
                if event.get("_tether_polled"):
                    gateway_event, gateway = test_case.gateway_event(event)
                    test_case.plugin._pre_gateway_dispatch(
                        event=gateway_event,
                        gateway=gateway,
                    )
                if event.get("_tether_test_reply"):
                    await self.send(
                        str(event["channel"]),
                        str(event["_tether_test_reply"]),
                        reply_to=str(event["ts"]),
                        metadata={
                            "thread_id": str(
                                event.get("thread_ts") or event["ts"]
                            ),
                            "team_id": str(event.get("team") or ""),
                        },
                    )
                self.handled.append((event, payload))
                return event

        modules = {
            "hermes_plugins": types.ModuleType("hermes_plugins"),
            "hermes_plugins.slack_platform": types.ModuleType(
                "hermes_plugins.slack_platform"
            ),
            "hermes_plugins.slack_platform.adapter": types.ModuleType(
                "hermes_plugins.slack_platform.adapter"
            ),
        }
        modules["hermes_plugins.slack_platform.adapter"].SlackAdapter = SlackAdapter
        self.modules_patch = mock.patch.dict(sys.modules, modules)
        self.modules_patch.start()
        self.plugin._ensure_reply_poller = lambda _adapter: None
        self.plugin._install_slack_bridge_prefilter()
        self.adapter = SlackAdapter()

    def tearDown(self):
        self.modules_patch.stop()
        sys.modules.pop("bridge_runtime", None)
        sys.modules.pop("tether_routing", None)
        sys.modules.pop(self.plugin_name, None)
        sys.modules.pop(self.runtime_name, None)
        self.env_patch.stop()
        self.temp.cleanup()

    def raw_event(
        self,
        *,
        ts="1785000001.000001",
        text="continue",
        user=HUMAN,
        bot_id="",
        channel=CHANNEL,
        team=TEAM,
        thread_ts=THREAD,
        channel_type="channel",
    ):
        event = {
            "ts": ts,
            "text": text,
            "user": user,
            "channel": channel,
            "channel_type": channel_type,
        }
        if team:
            event["team"] = team
        if thread_ts:
            event["thread_ts"] = thread_ts
        if bot_id:
            event.update({"bot_id": bot_id, "subtype": "bot_message"})
        return event

    def make_bridge(self, source_kind="headless_run"):
        if source_kind == "headless_run":
            source = {"run_id": "run-1", "cwd": "/tmp/project"}
        else:
            source = {"session_id": "session-1", "cwd": "/tmp/project"}
        bridge = self.plugin.store.create(
            {
                "source_kind": source_kind,
                "source": source,
                "owner_user_id": "*",
                "team_id": TEAM,
                "channel_id": CHANNEL,
                "idempotency_key": f"{source_kind}-1",
            }
        )
        bridge = self.plugin.store.bind(bridge.bridge_id, THREAD)
        self.client.thread_messages[THREAD] = [{
            "ts": THREAD,
            "user": LOCAL,
            "bot_id": LOCAL_APP,
            "text": "root",
            "metadata": {
                "event_type": "tether_root",
                "event_payload": {"bridge_id": bridge.bridge_id},
            },
        }]
        return bridge

    def make_owned_root_bridge(self, source_kind="headless_run"):
        bridge = self.make_bridge(source_kind)
        self.plugin.store.reserve_root(
            bridge.bridge_id,
            "root notification",
            "",
        )
        claimed = self.plugin.store.claim_root(bridge.bridge_id)
        self.assertEqual(claimed["status"], "claimed")
        self.assertTrue(
            self.plugin.store.record_root_post(
                bridge.bridge_id,
                claimed["lease_id"],
                THREAD,
            )
        )
        return self.plugin.store.get(bridge.bridge_id)

    def test_native_slack_send_redacts_before_adapter_egress(self):
        provider_key = "sk-" + "S" * 32
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000002.000001",
        ) as post:
            result = asyncio.run(
                self.adapter.send(
                    CHANNEL,
                    f"investigation complete; api_key={provider_key}",
                    metadata={"thread_id": THREAD},
                )
            )

        self.assertTrue(result["success"])
        self.assertEqual(self.adapter.sent, [])
        post.assert_called_once()
        sent_text = post.call_args.args[2]
        self.assertNotIn(provider_key, sent_text)
        self.assertIn("[REDACTED", sent_text)
        self.assertEqual(post.call_args.kwargs["options"], {"mrkdwn": True})

    def test_typing_cleanup_failure_does_not_fail_a_durable_send(self):
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        self.adapter.stop_typing = mock.AsyncMock(
            side_effect=RuntimeError("typing API unavailable"),
        )
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000002.000002",
        ) as post:
            result = asyncio.run(
                self.adapter.send(
                    CHANNEL,
                    "Delivery already succeeded.",
                    metadata={"team_id": TEAM, "thread_id": THREAD},
                )
            )

        self.assertTrue(result["success"])
        post.assert_called_once()
        self.adapter.stop_typing.assert_awaited_once()

    def test_auxiliary_hermes_slack_text_is_redacted(self):
        captured = []

        class SlackAdapter:
            _tether_prefilter = False

            async def connect(self):
                return True

            async def send(self, chat_id, content, reply_to=None, metadata=None):
                return {"ok": True}

            async def _handle_slack_message(self, event, payload=None):
                return event

            async def send_private_notice(
                self,
                chat_id,
                user_id,
                content,
                reply_to=None,
                metadata=None,
            ):
                captured.append(("private", content))
                return {"ok": True}

            async def send_exec_approval(
                self,
                chat_id,
                command,
                session_key,
                description="dangerous command",
                metadata=None,
            ):
                captured.append(("approval", command, description))
                return {"ok": True}

            async def send_slash_confirm(
                self,
                chat_id,
                title,
                message,
                session_key,
                confirm_id,
                metadata=None,
            ):
                captured.append(("confirm", title, message))
                return {"ok": True}

            async def send_clarify(
                self,
                chat_id,
                question,
                choices,
                clarify_id,
                session_key,
                metadata=None,
            ):
                captured.append(("clarify", question, choices))
                return {"ok": True}

        live_module = sys.modules[
            "hermes_plugins.slack_platform.adapter"
        ]
        provider_key = "sk-" + "R" * 32
        with mock.patch.object(
            live_module,
            "SlackAdapter",
            SlackAdapter,
        ), mock.patch.object(
            self.plugin,
            "_ensure_reply_poller",
        ):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            asyncio.run(
                adapter.send_private_notice(
                    CHANNEL,
                    HUMAN,
                    f"token={provider_key}",
                )
            )
            asyncio.run(
                adapter.send_exec_approval(
                    CHANNEL,
                    f"curl -H 'Bearer {provider_key}'",
                    "session",
                    description=f"secret={provider_key}",
                )
            )
            asyncio.run(
                adapter.send_slash_confirm(
                    CHANNEL,
                    f"title {provider_key}",
                    f"message {provider_key}",
                    "session",
                    "confirm",
                )
            )
            asyncio.run(
                adapter.send_clarify(
                    CHANNEL,
                    f"question {provider_key}",
                    [f"choice {provider_key}"],
                    "clarify",
                    "session",
                )
            )

        serialized = json.dumps(captured)
        self.assertNotIn(provider_key, serialized)
        self.assertIn("[REDACTED", serialized)

    def test_hermes_updates_are_redacted_and_durable(self):
        provider_key = "sk-" + "E" * 32
        target = "1785000002.000100"
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_update",
            return_value=target,
        ) as update:
            result = asyncio.run(
                self.adapter.edit_message(
                    CHANNEL,
                    target,
                    f"api_key={provider_key}",
                    finalize=True,
                    metadata={"team_id": TEAM, "thread_id": THREAD},
                )
            )

        self.assertTrue(result["success"])
        self.assertEqual(self.adapter.sent, [])
        update.assert_called_once()
        self.assertNotIn(provider_key, update.call_args.args[3])
        self.assertIn("[REDACTED", update.call_args.args[3])
        with self.plugin.store.connect() as database:
            row = database.execute(
                """
                SELECT operation,target_message_ts,state,message_ts
                FROM slack_messages
                """
            ).fetchone()
        self.assertEqual(
            tuple(row),
            ("update", target, "sent", target),
        )

    def test_identical_hermes_sends_in_one_turn_are_not_collapsed(self):
        event_id = f"slack:{TEAM}:{CHANNEL}:1785000002.000110"
        claim = self.plugin.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            THREAD,
            writer_id="hermes:test",
        )
        self.assertTrue(
            self.plugin.store.mark_thread_ingress_dispatched(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        )
        context = {
            "claim": (
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            ),
            "failed": "",
            "sequence": 0,
        }
        token = self.plugin._HERMES_EGRESS_CONTEXT.set(context)
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        try:
            with mock.patch.object(
                broker,
                "_ensure_channel_membership",
            ), mock.patch.object(
                self.runtime,
                "slack_post",
                side_effect=[
                    "1785000002.000111",
                    "1785000002.000112",
                ],
            ) as post:
                for _ in range(2):
                    result = asyncio.run(
                        self.adapter.send(
                            CHANNEL,
                            "Same useful update",
                            metadata={
                                "team_id": TEAM,
                                "thread_id": THREAD,
                            },
                        )
                    )
                    self.assertTrue(result["success"])
        finally:
            self.plugin._HERMES_EGRESS_CONTEXT.reset(token)

        self.assertEqual(post.call_count, 2)
        with self.plugin.store.connect() as database:
            groups = database.execute(
                """
                SELECT DISTINCT egress_group_id FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchall()
        self.assertEqual(len(groups), 2)

    def test_hermes_send_resolves_user_target_to_dm(self):
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000002.000120",
        ) as post:
            result = asyncio.run(
                self.adapter.send(
                    HUMAN,
                    "Direct message",
                    metadata={"team_id": TEAM},
                )
            )

        self.assertTrue(result["success"])
        self.assertEqual(post.call_args.args[1], "D12345678")

    def test_hermes_send_honors_ignored_channels(self):
        self.adapter.config.extra["ignored_channels"] = [CHANNEL]
        result = asyncio.run(
            self.adapter.send(
                CHANNEL,
                "Do not send",
                metadata={"team_id": TEAM},
            )
        )
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "ignored_channel")

    def test_hermes_send_rechecks_ignored_channel_after_dm_resolution(self):
        self.adapter.config.extra["ignored_channels"] = ["D12345678"]
        result = asyncio.run(
            self.adapter.send(
                HUMAN,
                "Do not send",
                metadata={"team_id": TEAM},
            )
        )
        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "ignored_channel")

    def test_local_hermes_attachments_use_the_shared_upload_guard(self):
        approved = self.home / "approved"
        staging = self.home / ".hermes" / "upload-staging"
        approved.mkdir(mode=0o700)
        safe = approved / "report.txt"
        safe.write_text("safe report", encoding="utf-8")
        secret = approved / "secret.txt"
        secret.write_text(
            "api_key=sk-" + "X" * 32,
            encoding="utf-8",
        )
        outside = self.home / "outside.txt"
        outside.write_text("outside", encoding="utf-8")
        self.runtime.UPLOAD_APPROVED_ROOTS = (str(approved),)
        self.runtime.UPLOAD_STAGING_DIRECTORY = str(staging)
        self.runtime.UPLOAD_MAX_BYTES = "4096"
        captured = []

        class SlackAdapter:
            _tether_prefilter = False

            async def connect(self):
                return True

            async def send(self, chat_id, content, reply_to=None, metadata=None):
                return {"ok": True}

            async def _handle_slack_message(self, event, payload=None):
                return event

            async def _upload_file(
                self,
                chat_id,
                file_path,
                caption=None,
                reply_to=None,
                metadata=None,
            ):
                path = pathlib.Path(file_path)
                captured.append(
                    ("upload", path, path.read_text(encoding="utf-8"), caption)
                )
                return {"ok": True}

            async def send_video(
                self,
                chat_id,
                video_path,
                caption=None,
                reply_to=None,
                metadata=None,
            ):
                path = pathlib.Path(video_path)
                captured.append(("video", path, path.exists(), caption))
                return {"ok": True}

            async def send_document(
                self,
                chat_id,
                file_path,
                caption=None,
                file_name=None,
                reply_to=None,
                metadata=None,
            ):
                path = pathlib.Path(file_path)
                captured.append(
                    ("document", path, path.exists(), caption, file_name)
                )
                return {"ok": True}

            async def send_multiple_images(
                self,
                chat_id,
                images,
                metadata=None,
                human_delay=0.0,
            ):
                for image_url, alt_text in images:
                    path = pathlib.Path(image_url.removeprefix("file://"))
                    captured.append(
                        ("batch", path, path.exists(), alt_text)
                    )

        live_module = sys.modules[
            "hermes_plugins.slack_platform.adapter"
        ]
        with mock.patch.object(
            live_module,
            "SlackAdapter",
            SlackAdapter,
        ), mock.patch.object(
            self.plugin,
            "_ensure_reply_poller",
        ):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            safe_result = asyncio.run(
                adapter._upload_file(
                    CHANNEL,
                    str(safe),
                    caption="safe caption",
                )
            )
            document_result = asyncio.run(
                adapter.send_document(
                    CHANNEL,
                    str(safe),
                )
            )
            provider_key = "sk-" + "I" * 32
            asyncio.run(
                adapter.send_multiple_images(
                    CHANNEL,
                    [(safe.as_uri(), f"api_key={provider_key}")],
                )
            )
            outside_result = asyncio.run(
                adapter.send_video(
                    CHANNEL,
                    str(outside),
                )
            )
            secret_result = asyncio.run(
                adapter.send_document(
                    CHANNEL,
                    str(secret),
                )
            )
            asyncio.run(
                adapter.send_multiple_images(
                    CHANNEL,
                    [(secret.as_uri(), "do not upload")],
                )
            )

        self.assertTrue(safe_result["ok"])
        self.assertTrue(document_result["ok"])
        self.assertFalse(outside_result["success"])
        self.assertFalse(secret_result["success"])
        self.assertEqual(
            [item[0] for item in captured],
            ["upload", "document", "batch"],
        )
        self.assertTrue(
            all(item[1].parent == staging for item in captured)
        )
        self.assertTrue(all(not item[1].exists() for item in captured))
        self.assertEqual(captured[1][4], "report.txt")
        self.assertNotIn(provider_key, captured[2][3])

    def test_hermes_reply_completes_only_through_linked_durable_outbox(self):
        raw = self.raw_event(
            ts="1785000002.000010",
            text=f"<@{LOCAL}> answer",
            thread_ts="",
        )
        raw.update(
            {
                "_tether_polled": True,
                "_tether_test_reply": "Verified reply",
            }
        )
        broker = self.runtime.Broker(
            "test-token",
            self.plugin.store,
            verified_workspace_team_id=TEAM,
        )
        self.plugin.state.broker = types.SimpleNamespace(broker=broker)
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000002.000011",
        ):
            self.assertIs(self.ingress(raw), raw)

        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            outbox = database.execute(
                """
                SELECT state,message_ts,ingress_event_id,egress_group_id
                FROM slack_messages WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchall()
        self.assertEqual(
            tuple(ingress),
            ("completed", 1, None),
        )
        self.assertEqual(len(outbox), 1)
        self.assertEqual(
            (outbox[0]["state"], outbox[0]["message_ts"]),
            ("sent", "1785000002.000011"),
        )
        self.assertEqual(outbox[0]["ingress_event_id"], event_id)
        self.assertRegex(outbox[0]["egress_group_id"], r"^hsg_[0-9a-f]{32}$")

    def test_hermes_reply_without_durable_broker_stays_uncertain(self):
        raw = self.raw_event(
            ts="1785000002.000020",
            text=f"<@{LOCAL}> answer",
            thread_ts="",
        )
        raw.update(
            {
                "_tether_polled": True,
                "_tether_test_reply": "Do not lose this",
            }
        )

        self.assertIs(self.ingress(raw), raw)

        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            outbox_count = database.execute(
                """
                SELECT count(*) FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(ingress["state"], "uncertain")
        self.assertEqual(ingress["egress_sealed"], 1)
        self.assertEqual(ingress["error_code"], "hermes_egress_pending")
        self.assertEqual(outbox_count, 1)

    def test_hermes_no_reply_completes_without_visible_egress(self):
        raw = self.raw_event(
            ts="1785000002.000030",
            text=f"<@{LOCAL}> check silently",
            thread_ts="",
        )
        raw.update(
            {
                "_tether_polled": True,
                "_tether_test_reply": "NO_REPLY",
            }
        )

        self.assertIs(self.ingress(raw), raw)

        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            outbox_count = database.execute(
                """
                SELECT count(*) FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(tuple(ingress), ("completed", 1, None))
        self.assertEqual(outbox_count, 0)
        self.assertEqual(self.adapter.sent, [])

    def test_hermes_background_dispatch_keeps_ingress_claim_until_task_finishes(self):
        base_adapter = type(self.adapter)
        gateway_allowed = asyncio.Event()
        gateway_started = asyncio.Event()
        dispatch_allowed = asyncio.Event()

        class BackgroundSlackAdapter(base_adapter):
            _tether_prefilter = False

            async def _handle_slack_message(self, event, payload=None):
                async def dispatch():
                    await gateway_allowed.wait()
                    gateway_event, gateway = self_test.gateway_event(event)
                    self_test.plugin._pre_gateway_dispatch(
                        event=gateway_event,
                        gateway=gateway,
                    )
                    gateway_started.set()
                    await dispatch_allowed.wait()

                self.dispatch_task = asyncio.create_task(dispatch())
                return event

        self_test = self
        live_module = sys.modules[
            "hermes_plugins.slack_platform.adapter"
        ]
        with mock.patch.object(
            live_module,
            "SlackAdapter",
            BackgroundSlackAdapter,
        ), mock.patch.object(
            self.plugin,
            "_ensure_reply_poller",
        ):
            self.plugin._install_slack_bridge_prefilter()
            adapter = BackgroundSlackAdapter()
            raw = self.raw_event(
                ts="1785000002.000032",
                text=f"<@{LOCAL}> investigate in the background",
                thread_ts="",
            )
            event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"

            async def exercise():
                self.assertIs(
                    await adapter._handle_slack_message(raw),
                    raw,
                )
                with self.plugin.store.connect() as database:
                    before_gateway = database.execute(
                        """
                        SELECT state,egress_sealed,error_code
                        FROM thread_ingress WHERE event_id=?
                        """,
                        (event_id,),
                    ).fetchone()
                self.assertEqual(
                    tuple(before_gateway),
                    ("processing", 0, None),
                )

                gateway_allowed.set()
                await gateway_started.wait()
                dispatch_allowed.set()
                await adapter.dispatch_task
                finalizers = tuple(
                    self.plugin.state.hermes_ingress_finalizers
                )
                if finalizers:
                    await asyncio.gather(*finalizers)

            asyncio.run(exercise())

        with self.plugin.store.connect() as database:
            completed = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
        self.assertEqual(tuple(completed), ("completed", 1, None))
        self.assertTrue(raw["_tether_ingress_dispatched"])

    def test_hermes_trailing_no_reply_suppresses_routing_explanation(self):
        raw = self.raw_event(
            ts="1785000002.000031",
            text=f"<@{LOCAL}> check silently",
            thread_ts="",
        )
        raw.update(
            {
                "_tether_polled": True,
                "_tether_test_reply": (
                    "This belongs to another participant.\n\nNO_REPLY"
                ),
            }
        )

        self.assertIs(self.ingress(raw), raw)

        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            outbox_count = database.execute(
                """
                SELECT count(*) FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(tuple(ingress), ("completed", 1, None))
        self.assertEqual(outbox_count, 0)
        self.assertEqual(self.adapter.sent, [])

    def ingress(self, event, payload=None):
        return asyncio.run(self.adapter._handle_slack_message(event, payload))

    def gateway_event(self, raw):
        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform,
            thread_id=raw.get("thread_ts"),
            scope_id=raw.get("team"),
            guild_id=raw.get("team"),
            chat_id=raw.get("channel"),
            user_id=raw.get("user"),
            message_id=raw.get("ts"),
            is_bot=bool(raw.get("bot_id")),
        )
        event = types.SimpleNamespace(
            source=source,
            message_id=raw.get("ts"),
            text=raw.get("text", ""),
            raw_message=raw,
        )
        gateway = types.SimpleNamespace(adapters={platform: self.adapter})
        decision = raw.get(self.plugin.ROUTING_DECISION_KEY)
        if (
            isinstance(decision, self.plugin.routing.RoutingDecision)
            and decision.action is self.plugin.routing.RouteAction.HERMES
        ):
            event_id = self.plugin._composite_event_id(decision)
            current_claim = raw.get("_tether_ingress_claim")
            with self.plugin.store.connect() as database:
                ingress = database.execute(
                    """
                    SELECT state,lease_id,fence_epoch
                    FROM thread_ingress WHERE event_id=?
                    """,
                    (event_id,),
                ).fetchone()
            claim_is_live = (
                isinstance(current_claim, tuple)
                and len(current_claim) == 3
                and ingress is not None
                and str(ingress["state"]) == "processing"
                and str(ingress["lease_id"] or "") == current_claim[1]
                and int(ingress["fence_epoch"]) == current_claim[2]
            )
            if not claim_is_live:
                claim = self.plugin.store.claim_thread_ingress(
                    event_id,
                    decision.message_identity.team_id,
                    decision.message_identity.channel_id,
                    str(
                        raw.get("thread_ts")
                        or decision.message_identity.message_ts
                    ),
                    route_action="hermes",
                    writer_id=str(decision.writer_id or ""),
                    bridge_id=str(decision.bridge_id or ""),
                    binding_generation=decision.binding_generation,
                    payload=self.plugin._hermes_ingress_payload(raw),
                )
                self.assertEqual(claim["status"], "claimed")
                raw["_tether_ingress_claim"] = (
                    event_id,
                    str(claim["lease_id"]),
                    int(claim["fence_epoch"]),
                )
        return event, gateway

    def test_other_bot_mention_silences_this_bot_even_in_bound_thread(self):
        self.make_bridge()
        raw = self.raw_event(text=f"<@{OTHER_BOT}> please investigate")

        result = self.ingress(raw)

        self.assertIsNone(result)
        self.assertEqual(self.adapter.handled, [])
        decision = raw[self.plugin.ROUTING_DECISION_KEY]
        self.assertEqual(decision.action, self.plugin.routing.RouteAction.SILENT)
        self.assertEqual(
            decision.reason,
            "another_participant_explicitly_targeted",
        )

    def test_human_mention_silences_owned_bound_thread(self):
        self.make_owned_root_bridge("claude_session")
        self.adapter._tether_user_kinds = {
            (TEAM, HUMAN): "human",
            (TEAM, OTHER_HUMAN): "human",
        }
        raw = self.raw_event(
            text=f"<@{OTHER_HUMAN}> please investigate",
        )

        result = self.ingress(raw)

        self.assertIsNone(result)
        self.assertEqual(self.adapter.handled, [])
        decision = raw[self.plugin.ROUTING_DECISION_KEY]
        self.assertEqual(
            decision.action,
            self.plugin.routing.RouteAction.SILENT,
        )
        self.assertEqual(
            decision.reason,
            "another_participant_explicitly_targeted",
        )

    def test_owned_root_routes_ambient_reply_without_history_read(self):
        bridge = self.make_owned_root_bridge("claude_session")
        self.assertIsNotNone(bridge)
        self.adapter._tether_user_kinds = {(TEAM, HUMAN): "human"}
        self.adapter._get_client = mock.Mock(
            side_effect=AssertionError("owned root must not read Slack history")
        )
        raw = self.raw_event(text="continue in the bound session")

        decision = asyncio.run(
            self.plugin._route_slack_event(self.adapter, raw)
        )

        self.assertEqual(decision.action, self.plugin.routing.RouteAction.NATIVE)
        self.assertEqual(decision.reason, "active_native_binding")
        self.assertEqual(decision.bridge_id, bridge.bridge_id)

    def test_existing_thread_attachment_is_not_ambient_root_owner(self):
        bridge = self.make_bridge("claude_session")
        self.plugin.store.reserve_root(
            bridge.bridge_id,
            "attached notification",
            THREAD,
        )
        claimed = self.plugin.store.claim_root(bridge.bridge_id)
        self.assertEqual(claimed["status"], "claimed")
        self.assertTrue(
            self.plugin.store.record_root_post(
                bridge.bridge_id,
                claimed["lease_id"],
                THREAD,
            )
        )
        self.client.thread_messages[THREAD] = [{
            "ts": THREAD,
            "user": HUMAN,
            "text": "human-owned root",
        }]
        raw = self.raw_event(text="ambient follow-up")

        decision = asyncio.run(
            self.plugin._route_slack_event(self.adapter, raw)
        )

        self.assertEqual(decision.action, self.plugin.routing.RouteAction.SILENT)
        self.assertEqual(decision.reason, "active_binding_not_owned")

    def test_multiple_explicit_bot_mentions_route_each_named_bot(self):
        raw_a = self.raw_event(
            text=f"<@{LOCAL}> <@{PEER}> compare",
            thread_ts="",
        )
        raw_b = dict(raw_a)
        adapter_b = types.SimpleNamespace(
            _bot_user_id=PEER,
            _bot_id=PEER_APP,
            _team_bot_user_ids={TEAM: PEER},
            _team_bot_ids={TEAM: PEER_APP},
            _channel_team={CHANNEL: TEAM},
            _get_client=lambda _channel, team_id=None: self.client,
            config=types.SimpleNamespace(extra={}),
        )

        decision_a = asyncio.run(self.plugin._route_slack_event(self.adapter, raw_a))
        with mock.patch.dict(
            os.environ,
            {"TETHER_ALLOWED_BOT_USERS": f"{LOCAL},{PEER}"},
            clear=False,
        ):
            decision_b = asyncio.run(
                self.plugin._route_slack_event(adapter_b, raw_b)
            )

        self.assertEqual(decision_a.action, self.plugin.routing.RouteAction.HERMES)
        self.assertEqual(decision_b.action, self.plugin.routing.RouteAction.HERMES)
        self.assertEqual(
            decision_a.targeted_bot_user_ids,
            frozenset({LOCAL, PEER}),
        )

    def test_trusted_peer_requires_exact_self_mention(self):
        self.make_bridge()
        self.adapter.config.extra.update(
            {"allow_bots": "none", "strict_mention": True}
        )
        ambient = self.raw_event(
            ts="1785000001.000002",
            text="general bot chatter",
            user=PEER,
            bot_id=PEER_APP,
        )
        targeted = self.raw_event(
            ts="1785000001.000003",
            text=f"<@{LOCAL}> challenge this",
            user=PEER,
            bot_id=PEER_APP,
        )

        self.assertIsNone(self.ingress(ambient))
        self.assertEqual(
            ambient[self.plugin.ROUTING_DECISION_KEY].reason,
            "peer_bot_did_not_target_self",
        )
        self.assertIs(self.ingress(targeted), targeted)
        self.assertEqual(len(self.adapter.handled), 1)
        self.assertEqual(self.adapter.config.extra["allow_bots"], "mentions")
        self.assertFalse(self.adapter.config.extra["strict_mention"])

    def test_unique_fresh_participation_lease_allows_ambient_human_reply(self):
        self.plugin.store.mark_participation(TEAM, CHANNEL, THREAD)
        self.client.thread_messages[THREAD] = [
            {"ts": THREAD, "user": HUMAN, "text": "root"},
            {
                "ts": "1785000000.000002",
                "user": LOCAL,
                "bot_id": LOCAL_APP,
                "text": "agent reply",
            },
        ]
        raw = self.raw_event(text="what about the retry?")

        self.assertIs(self.ingress(raw), raw)
        decision = raw[self.plugin.ROUTING_DECISION_KEY]
        self.assertEqual(decision.action, self.plugin.routing.RouteAction.HERMES)
        self.assertEqual(decision.reason, "unique_participation_lease")

    def test_competing_bot_or_expired_participation_fails_closed(self):
        self.plugin.store.mark_participation(TEAM, CHANNEL, THREAD)
        self.client.thread_messages[THREAD] = [
            {"ts": THREAD, "user": HUMAN, "text": "root"},
            {"ts": "2", "user": LOCAL, "bot_id": LOCAL_APP, "text": "ours"},
            {"ts": "3", "user": OTHER_BOT, "bot_id": "BOTHER001", "text": "theirs"},
        ]
        competing = self.raw_event(ts="1785000001.000004")

        self.assertIsNone(self.ingress(competing))
        self.assertEqual(
            competing[self.plugin.ROUTING_DECISION_KEY].reason,
            "not_confidently_addressed",
        )

        expired_thread = "1784000000.000001"
        expired_at = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=8)
        ).isoformat()
        self.plugin.store.mark_participation(
            TEAM,
            CHANNEL,
            expired_thread,
            observed_at=expired_at,
        )
        expired = self.raw_event(
            ts="1785000001.000005",
            thread_ts=expired_thread,
        )
        self.assertIsNone(self.ingress(expired))
        self.assertEqual(
            expired[self.plugin.ROUTING_DECISION_KEY].reason,
            "not_confidently_addressed",
        )

    def test_paginated_thread_history_cannot_hide_competing_bot(self):
        self.plugin.store.mark_participation(TEAM, CHANNEL, THREAD)

        class PaginatedClient(FakeClient):
            async def conversations_replies(self, *, cursor="", **_kwargs):
                if not cursor:
                    return {
                        "messages": [
                            {
                                "ts": "2",
                                "user": LOCAL,
                                "bot_id": LOCAL_APP,
                                "text": "ours",
                            },
                        ],
                        "response_metadata": {"next_cursor": "page-2"},
                    }
                return {
                    "messages": [
                        {
                            "ts": "3",
                            "user": OTHER_BOT,
                            "bot_id": "BOTHER001",
                            "text": "theirs",
                        },
                    ],
                    "response_metadata": {"next_cursor": ""},
                }

        self.adapter._get_client = (
            lambda _channel, team_id=None: PaginatedClient()
        )
        raw = self.raw_event(ts="1785000001.000007")

        self.assertIsNone(self.ingress(raw))
        self.assertEqual(
            raw[self.plugin.ROUTING_DECISION_KEY].reason,
            "not_confidently_addressed",
        )

    def test_dm_is_admitted_but_unowned_mpim_is_silent(self):
        dm = self.raw_event(
            channel="D12345678",
            team=TEAM,
            thread_ts="1785000000.100001",
            channel_type="im",
        )
        mpim = self.raw_event(
            ts="1785000001.100002",
            channel="G12345678",
            team=TEAM,
            thread_ts="1785000000.100002",
            channel_type="mpim",
        )

        self.assertIs(self.ingress(dm), dm)
        self.assertIsNone(self.ingress(mpim))
        self.assertEqual(
            mpim[self.plugin.ROUTING_DECISION_KEY].reason,
            "not_confidently_addressed",
        )

    def test_unresolved_workspace_or_mention_identity_fails_closed(self):
        no_team = self.raw_event(
            channel="C99999999",
            team="",
            thread_ts="",
        )
        unresolved_mention = self.raw_event(
            ts="1785000001.000006",
            text="<@UUNKNOWN1> investigate",
            thread_ts="",
        )

        self.assertIsNone(self.ingress(no_team))
        self.assertEqual(
            no_team[self.plugin.ROUTING_ERROR_KEY],
            "slack_identity_unresolved",
        )
        self.assertIsNone(self.ingress(unresolved_mention))
        self.assertEqual(
            unresolved_mention[self.plugin.ROUTING_DECISION_KEY].reason,
            "mention_resolution_incomplete",
        )

    def test_first_contact_lookup_failure_survives_store_restart(self):
        raw = self.raw_event(
            ts="1785000001.000016",
            text=f"<@{LOCAL}> inspect this",
            thread_ts="",
        )
        self.client.user_types.pop(HUMAN)

        self.assertIsNone(self.ingress(raw))
        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            row = database.execute(
                """
                SELECT state,route_action,error_code,retry_count
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            database.execute(
                """
                UPDATE thread_ingress
                SET updated_at=datetime('now','-10 minutes')
                WHERE event_id=?
                """,
                (event_id,),
            )
        self.assertEqual(
            tuple(row),
            ("routing", "unresolved", "actor_identity_unresolved", 1),
        )

        self.client.user_types[HUMAN] = False
        restarted = self.runtime.Store(self.plugin.store.path)
        self.plugin.store = restarted
        self.plugin.state.store = restarted
        handled_before = len(self.adapter.handled)
        asyncio.run(
            self.plugin._recover_pending_slack_ingress(self.adapter)
        )
        self.assertEqual(len(self.adapter.handled), handled_before + 1)
        with restarted.connect() as database:
            state = database.execute(
                "SELECT state FROM thread_ingress WHERE event_id=?",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(state, "pending")

    def test_gateway_uses_attached_headless_decision_and_composite_dedupe(self):
        bridge = self.make_bridge()
        raw = self.raw_event(text=f"<@{LOCAL}> continue the run")
        self.assertIs(self.ingress(raw), raw)
        event, gateway = self.gateway_event(raw)

        result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        duplicate = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)

        self.assertEqual(result["action"], "rewrite")
        self.assertIn("Durable Hermes continuation", result["text"])
        self.assertEqual(duplicate["reason"], "tether-duplicate")
        composite = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        self.assertTrue(self.plugin.store.has_ingress(composite))
        self.assertEqual(
            raw[self.plugin.ROUTING_DECISION_KEY].bridge_id,
            bridge.bridge_id,
        )

    def test_gateway_preserves_hermes_session_continuation(self):
        self.make_bridge("hermes_session")
        raw = self.raw_event(text="continue the durable session")
        self.assertIs(self.ingress(raw), raw)
        event, gateway = self.gateway_event(raw)

        result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)

        self.assertEqual(result["action"], "rewrite")
        self.assertIn("Durable Hermes continuation", result["text"])
        self.assertIn("continue the durable session", result["text"])

    def test_gateway_admitted_hermes_turn_terminates_later_hooks(self):
        raw = self.raw_event(
            text=f"<@{LOCAL}> investigate",
            thread_ts="",
        )
        self.assertIs(self.ingress(raw), raw)
        event, gateway = self.gateway_event(raw)

        result = self.plugin._pre_gateway_dispatch(
            event=event,
            gateway=gateway,
        )

        self.assertEqual(result, {"action": "allow"})

    def test_not_ready_hook_does_not_intercept_other_platforms(self):
        event = types.SimpleNamespace(
            source=types.SimpleNamespace(
                platform=types.SimpleNamespace(value="discord"),
            ),
        )
        self.plugin.state.ready = False

        self.assertIsNone(
            self.plugin._pre_gateway_dispatch(
                event=event,
                gateway=types.SimpleNamespace(),
            )
        )

    def test_gateway_routes_native_binding_to_one_writer(self):
        bridge = self.make_bridge("claude_session")
        raw = self.raw_event(text="continue in the pane")
        self.assertIs(self.ingress(raw), raw)
        event, gateway = self.gateway_event(raw)

        async def exercise():
            with mock.patch.object(
                self.plugin,
                "_drain_bridge",
                new=mock.AsyncMock(),
            ):
                result = self.plugin._pre_gateway_dispatch(
                    event=event,
                    gateway=gateway,
                )
                await asyncio.sleep(0)
                return result

        result = asyncio.run(exercise())

        self.assertEqual(result, {"action": "skip", "reason": "tether-handled"})
        composite = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            row = database.execute(
                "SELECT bridge_id, payload_json FROM bridge_events WHERE event_id = ?",
                (composite,),
            ).fetchone()
            ingress_payload = database.execute(
                "SELECT payload_json FROM thread_ingress WHERE event_id = ?",
                (composite,),
            ).fetchone()[0]
        self.assertEqual(row[0], bridge.bridge_id)
        self.assertEqual(json.loads(row[1]), {"text": "continue in the pane"})
        self.assertEqual(json.loads(ingress_payload)["user"], HUMAN)

    def test_native_edit_revises_queued_turn_without_duplicate_delivery(self):
        bridge = self.make_bridge("claude_session")
        original = self.raw_event(text="old instruction")
        self.assertIs(self.ingress(original), original)
        original_event, gateway = self.gateway_event(original)

        async def dispatch(event, active_gateway):
            with mock.patch.object(
                self.plugin,
                "_drain_bridge",
                new=mock.AsyncMock(),
            ):
                result = self.plugin._pre_gateway_dispatch(
                    event=event,
                    gateway=active_gateway,
                )
                await asyncio.sleep(0)
                return result

        self.assertEqual(
            asyncio.run(dispatch(original_event, gateway))["reason"],
            "tether-handled",
        )
        edit = {
            "subtype": "message_changed",
            "event_ts": "1785000002.000001",
            "ts": "1785000002.000001",
            "channel": CHANNEL,
            "team": TEAM,
            "message": {
                "ts": original["ts"],
                "thread_ts": THREAD,
                "text": "new authoritative instruction",
                "user": HUMAN,
            },
            "previous_message": {
                "ts": original["ts"],
                "thread_ts": THREAD,
                "text": "old instruction",
                "user": HUMAN,
            },
        }
        self.assertIs(self.ingress(edit), edit)
        edit_event, edit_gateway = self.gateway_event(edit)

        result = asyncio.run(dispatch(edit_event, edit_gateway))

        self.assertEqual(result["reason"], "tether-mutation-revised")
        original_id = f"slack:{TEAM}:{CHANNEL}:{original['ts']}"
        mutation_id = f"slack:{TEAM}:{CHANNEL}:{edit['ts']}"
        with self.plugin.store.connect() as database:
            original_row = database.execute(
                "SELECT state,payload_json FROM bridge_events WHERE event_id=?",
                (original_id,),
            ).fetchone()
            mutation_row = database.execute(
                "SELECT state FROM thread_ingress WHERE event_id=?",
                (mutation_id,),
            ).fetchone()
            duplicate = database.execute(
                "SELECT 1 FROM bridge_events WHERE event_id=?",
                (mutation_id,),
            ).fetchone()
        self.assertEqual(
            tuple(original_row),
            ("queued", '{"text":"new authoritative instruction"}'),
        )
        self.assertEqual(mutation_row[0], "transferred")
        self.assertIsNone(duplicate)
        self.assertEqual(bridge.binding_generation, 1)

    def test_native_delete_cancels_queued_turn(self):
        self.make_bridge("claude_session")
        original = self.raw_event(text="delete me")
        self.assertIs(self.ingress(original), original)
        original_event, gateway = self.gateway_event(original)

        async def dispatch(event, active_gateway):
            with mock.patch.object(
                self.plugin,
                "_drain_bridge",
                new=mock.AsyncMock(),
            ):
                result = self.plugin._pre_gateway_dispatch(
                    event=event,
                    gateway=active_gateway,
                )
                await asyncio.sleep(0)
                return result

        asyncio.run(dispatch(original_event, gateway))
        deletion = {
            "subtype": "message_deleted",
            "event_ts": "1785000002.000002",
            "ts": "1785000002.000002",
            "deleted_ts": original["ts"],
            "channel": CHANNEL,
            "team": TEAM,
            "previous_message": {
                "ts": original["ts"],
                "thread_ts": THREAD,
                "text": "delete me",
                "user": HUMAN,
            },
        }
        self.assertIs(self.ingress(deletion), deletion)
        deletion_event, deletion_gateway = self.gateway_event(deletion)

        result = asyncio.run(dispatch(deletion_event, deletion_gateway))

        self.assertEqual(result["reason"], "tether-mutation-cancelled")
        original_id = f"slack:{TEAM}:{CHANNEL}:{original['ts']}"
        with self.plugin.store.connect() as database:
            row = database.execute(
                "SELECT state,error FROM bridge_events WHERE event_id=?",
                (original_id,),
            ).fetchone()
        self.assertEqual(tuple(row), ("failed", "slack_message_deleted"))

    def test_gateway_rejects_messages_without_ingress_decision(self):
        raw = self.raw_event()
        event, gateway = self.gateway_event(raw)

        result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)

        self.assertEqual(
            result,
            {"action": "skip", "reason": "tether-routing-decision-missing"},
        )

    def test_reply_poller_uses_the_same_exact_peer_target_decision(self):
        self.make_bridge()
        self.client.thread_messages[THREAD] = [
            {
                "ts": THREAD,
                "user": LOCAL,
                "bot_id": LOCAL_APP,
                "text": "root",
            },
            {
                "ts": "1785000001.000010",
                "thread_ts": THREAD,
                "user": PEER,
                "bot_id": PEER_APP,
                "subtype": "bot_message",
                "text": f"<@{LOCAL}> challenge this",
            },
            {
                "ts": "1785000001.000011",
                "thread_ts": THREAD,
                "user": PEER,
                "bot_id": PEER_APP,
                "subtype": "bot_message",
                "text": "general bot chatter",
            },
        ]

        recovered = asyncio.run(self.plugin._poll_recent_replies(self.adapter))

        self.assertEqual(recovered, 1)
        self.assertEqual(
            [event["ts"] for event, _payload in self.adapter.handled],
            ["1785000001.000010"],
        )

    def test_pending_hermes_ingress_replays_locally_once(self):
        raw = self.raw_event(
            text=f"<@{LOCAL}> inspect this",
            thread_ts="",
        )
        self.assertIs(self.ingress(raw), raw)
        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            before = database.execute(
                """
                SELECT state,payload_json FROM thread_ingress
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            database.execute(
                """
                UPDATE thread_ingress
                SET updated_at=datetime('now','-10 minutes')
                WHERE event_id=?
                """,
                (event_id,),
            )
        self.assertEqual(before["state"], "pending")
        persisted = self.plugin.json.loads(before["payload_json"])
        self.assertEqual(persisted["user"], HUMAN)
        self.assertEqual(persisted["message_ts"], raw["ts"])

        recovered = asyncio.run(
            self.plugin._recover_pending_slack_ingress(self.adapter)
        )
        self.assertEqual(recovered, 1)
        with self.plugin.store.connect() as database:
            state = database.execute(
                "SELECT state FROM thread_ingress WHERE event_id=?",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(state, "completed")
        self.assertEqual(
            asyncio.run(
                self.plugin._recover_pending_slack_ingress(self.adapter)
            ),
            0,
        )

    def test_post_dispatch_failure_becomes_uncertain_and_is_not_replayed(self):
        raw = self.raw_event(
            ts="1785000001.000099",
            text=f"<@{LOCAL}> inspect this",
            thread_ts="",
        )
        self.assertIs(self.ingress(raw), raw)
        event_id = f"slack:{TEAM}:{CHANNEL}:{raw['ts']}"
        with self.plugin.store.connect() as database:
            database.execute(
                """
                UPDATE thread_ingress
                SET updated_at=datetime('now','-10 minutes')
                WHERE event_id=?
                """,
                (event_id,),
            )
        original_dispatch = self.plugin._pre_gateway_dispatch

        def dispatch_then_fail(*args, **kwargs):
            original_dispatch(*args, **kwargs)
            raise RuntimeError("gateway failed after dispatch")

        with mock.patch.object(
            self.plugin,
            "_pre_gateway_dispatch",
            side_effect=dispatch_then_fail,
        ):
            self.assertEqual(
                asyncio.run(
                    self.plugin._recover_pending_slack_ingress(self.adapter)
                ),
                0,
            )
        with self.plugin.store.connect() as database:
            state = database.execute(
                "SELECT state FROM thread_ingress WHERE event_id=?",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(state, "uncertain")
        self.assertEqual(
            asyncio.run(
                self.plugin._recover_pending_slack_ingress(self.adapter)
            ),
            0,
        )


if __name__ == "__main__":
    unittest.main()
