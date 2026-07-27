import asyncio
import concurrent.futures
import datetime
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"
NOTIFIER_PATH = ROOT / "skills" / "tether" / "scripts" / "tether_notify.py"
INSTALL_PATH = ROOT / "install.sh"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"bridge_runtime_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")

    def tearDown(self):
        self.temp.cleanup()

    def request(self, key="run-1"):
        return {
            "source_kind": "headless_run",
            "source": {"run_id": "run-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": key,
        }

    def test_idempotency_and_exact_thread_lookup(self):
        first = self.store.create(self.request())
        second = self.store.create(self.request())
        self.assertEqual(first.bridge_id, second.bridge_id)
        active = self.store.bind(first.bridge_id, "123.456")
        self.assertEqual(self.store.find("T12345678", "C12345678", "123.456"), active)
        self.assertIsNone(self.store.find("T12345678", "C99999999", "123.456"))

    def test_events_are_deduplicated_and_serialized(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "first"))
        self.assertFalse(self.store.enqueue_event("111.1", bridge.bridge_id, "duplicate"))
        self.assertTrue(self.store.enqueue_event("111.2", bridge.bridge_id, "second"))
        first = self.store.claim_next_event(bridge.bridge_id)
        self.assertEqual(first["text"], "first")
        self.assertIsNone(self.store.claim_next_event(bridge.bridge_id), "a processing event blocks the next claim")
        self.store.finish_event(first["event_id"])
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "second")

    def test_queued_followups_are_claimed_as_one_batch(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "first follow-up"))
        self.assertTrue(self.store.enqueue_event("111.2", bridge.bridge_id, "second follow-up"))
        batch = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual(
            [(item["event_id"], item["text"]) for item in batch],
            [("111.1", "first follow-up"), ("111.2", "second follow-up")],
        )
        self.assertEqual(self.store.claim_event_batch(bridge.bridge_id), [])

    def test_processing_events_are_requeued_after_restart(self):
        bridge = self.store.create(self.request())
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "resume me"))
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "resume me")
        self.store.requeue_processing()
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "resume me")

    def test_ingress_is_persistent_and_recognizes_legacy_native_events(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.mark_ingress("111.1", bridge.bridge_id))
        self.assertTrue(self.store.has_ingress("111.1"))
        self.assertFalse(self.store.mark_ingress("111.1", bridge.bridge_id))
        self.assertTrue(self.store.enqueue_event("111.2", bridge.bridge_id, "legacy event"))
        self.assertTrue(self.store.has_ingress("111.2"))

    def test_recent_active_bridges_include_native_and_headless_sources(self):
        native_request = self.request("native")
        native_request["source_kind"] = "claude_session"
        native_request["source"] = {"session_id": "claude-1", "cwd": "/tmp/project"}
        native = self.store.bind(self.store.create(native_request).bridge_id, "123.456")
        headless_request = self.request("headless")
        headless_request["source_kind"] = "headless_run"
        headless = self.store.bind(self.store.create(headless_request).bridge_id, "456.789")
        self.assertEqual(
            {bridge.bridge_id for bridge in self.store.recent_active_bridges()},
            {native.bridge_id, headless.bridge_id},
        )

    def test_stored_errors_are_truncated(self):
        bridge = self.store.create(self.request())
        self.store.claim_event("111.1", bridge.bridge_id)
        self.store.finish_event("111.1", "sensitive" * 500)
        with self.store.connect() as db:
            error = db.execute("SELECT error FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(len(error), 1000)

    def test_invalid_ids_fail_closed(self):
        request = self.request()
        request["owner_user_id"] = "not-a-slack-id"
        with self.assertRaises(ValueError):
            self.store.create(request)

    def test_user_id_cannot_be_stored_as_a_channel(self):
        with self.assertRaisesRegex(ValueError, "invalid Slack channel"):
            self.store.create({
                "source_kind": "headless_run",
                "source": {"run_id": "run-1"},
                "owner_user_id": "U12345678",
                "channel_id": "U12345678",
                "idempotency_key": "bad-user-destination",
            })

    def test_source_metadata_rejects_unknown_or_oversized_values(self):
        request = self.request()
        request["source"]["prompt"] = "should never be persisted"
        with self.assertRaisesRegex(ValueError, "invalid bridge source"):
            self.store.create(request)

        request = self.request()
        request["source"]["cwd"] = "x" * (self.runtime.MAX_SOURCE_VALUE + 1)
        with self.assertRaisesRegex(ValueError, "source value is too large"):
            self.store.create(request)

    def test_broker_socket_is_private(self):
        socket_path = self.home / "hermes" / "bridge.sock"
        server = self.runtime.start_broker("test-token", socket_path)
        try:
            self.assertTrue(socket_path.is_socket())
            self.assertEqual(socket_path.stat().st_mode & 0o777, 0o600)
            with self.assertRaisesRegex(RuntimeError, "unsupported operation"):
                self.runtime.broker_call({"op": "unsupported"}, socket_path)
        finally:
            server.shutdown()
            server.server_close()
            socket_path.unlink(missing_ok=True)

    def test_store_connections_close_after_each_transaction(self):
        context = self.store.connect()
        with context as db:
            self.assertEqual(db.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")

    def test_broker_reuses_hermes_channel_and_allowlist_without_copying_ids(self):
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-2", "cwd": "/tmp/project"},
            "idempotency_key": "run-2",
        }
        with mock.patch.dict(os.environ, {
            "SLACK_HOME_CHANNEL": "C12345678",
            "SLACK_ALLOWED_USERS": "U12345678,U87654321",
        }, clear=False), mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(self.runtime, "slack_post", return_value="123.456"):
            result = broker.handle(request)
            status = broker.handle({"op": "status"})
        bridge = self.store.get(result["bridge_id"])
        self.assertEqual(bridge.channel_id, "C12345678")
        self.assertEqual(bridge.owner_user_id, "*", "Hermes's explicit allowlist is shared by default")
        self.assertEqual(status["allowed_user_count"], 2)
        self.assertEqual(status["implementation"], "tether")
        self.assertEqual(status["protocol_version"], 3)
        self.assertNotIn("allowed_users", status, "status reports readiness, never identities")

    def test_shared_channel_rejects_accidental_owner_restriction(self):
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-owner", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "channel_id": "C12345678",
            "idempotency_key": "run-owner",
        }
        with mock.patch.dict(
            os.environ, {"SLACK_ALLOWED_USERS": "U12345678,U87654321"}, clear=False
        ), self.assertRaisesRegex(ValueError, "owner-restricted shared-channel"):
            broker.handle(request)

    def test_bound_reply_is_brief_and_idempotent_per_agent_turn(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "reply", "bridge_id": bridge.bridge_id,
            "reply_key": "tether-123456789abc", "text": "Fixed and verified.",
        }
        with mock.patch.object(broker, "_ensure_channel_membership"), mock.patch.object(
            self.runtime, "slack_post", return_value="123.457",
        ) as post:
            first = broker.handle(request)
            second = broker.handle(request)
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(post.call_count, 1)

        too_long = " ".join(["detail"] * 51)
        with self.assertRaisesRegex(ValueError, "Slack reply is too long"):
            broker.handle({
                "op": "reply", "bridge_id": bridge.bridge_id,
                "reply_key": "tether-abcdef123456", "text": too_long,
            })

    def test_explicit_rebind_updates_only_the_matching_active_thread(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        broker = self.runtime.Broker("test-token", self.store)
        result = broker.handle({
            "op": "rebind",
            "channel_id": bridge.channel_id,
            "thread_ts": bridge.thread_ts,
            "source_kind": "zellij_pane",
            "source": {
                "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "claude", "pane_command_hash": "new-fingerprint",
            },
        })
        rebound = self.store.get(bridge.bridge_id)
        self.assertEqual(result["bridge_id"], bridge.bridge_id)
        self.assertEqual(rebound.source_kind, "zellij_pane")
        self.assertEqual(rebound.source["pane_command_hash"], "new-fingerprint")
        with self.assertRaisesRegex(ValueError, "active bridge not found"):
            broker.handle({
                "op": "rebind",
                "channel_id": "C99999999",
                "thread_ts": bridge.thread_ts,
                "source_kind": "zellij_pane",
                "source": rebound.source,
            })

    def test_no_reply_control_token_is_suppressed(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(self.runtime, "slack_post") as post:
            result = broker.handle({
                "op": "reply", "bridge_id": bridge.bridge_id,
                "reply_key": "tether-abcdef123456", "text": "NO_REPLY",
            })
        self.assertTrue(result["suppressed"])
        post.assert_not_called()

    def test_thread_history_stays_behind_broker_and_returns_sanitized_messages(self):
        broker = self.runtime.Broker("test-token", self.store)
        response = {
            "messages": [{
                "ts": "123.456", "thread_ts": "100.000", "text": "reply",
                "user": "U12345678", "blocks": [{"private": "detail"}],
            }]
        }
        with mock.patch.object(self.runtime, "_slack_call", side_effect=[
            {"ok": True, "messages": []}, response,
        ]) as call:
            result = broker.handle({
                "op": "thread_history", "channel_id": "C12345678",
                "thread_ts": "100.000", "limit": 10,
            })
        self.assertEqual(result["messages"], [{
            "ts": "123.456", "thread_ts": "100.000", "text": "reply", "user": "U12345678",
        }])
        self.assertEqual(call.call_args_list, [
            mock.call(
                "test-token", "conversations.history",
                {"channel": "C12345678", "limit": 1},
            ),
            mock.call(
                "test-token", "conversations.replies",
                {"channel": "C12345678", "ts": "100.000", "limit": 10},
            ),
        ])

    def test_public_destination_is_joined_only_once_per_broker(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            side_effect=[
                RuntimeError("Slack API error: not_in_channel"),
                {"ok": True},
            ],
        ) as call:
            broker._ensure_channel_membership("C12345678")
            broker._ensure_channel_membership("C12345678")
            broker._ensure_channel_membership("D12345678")
        self.assertEqual(call.call_args_list, [
            mock.call(
                "test-token", "conversations.history",
                {"channel": "C12345678", "limit": 1},
            ),
            mock.call("test-token", "conversations.join", {"channel": "C12345678"}),
        ])

    def test_dm_with_c_prefixed_id_is_not_joined(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={"ok": True, "messages": []},
        ) as call:
            broker._ensure_channel_membership("C0BHSK52GP5")
        call.assert_called_once_with(
            "test-token",
            "conversations.history",
            {"channel": "C0BHSK52GP5", "limit": 1},
        )

    def test_identity_returns_only_nonsecret_bot_metadata(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(self.runtime, "_slack_call", return_value={
            "ok": True, "team_id": "T12345678", "user_id": "U12345678",
            "user": "agent", "url": "https://example.slack.com/",
        }):
            result = broker.handle({"op": "identity"})
        self.assertEqual(result, {
            "ok": True, "team_id": "T12345678", "user_id": "U12345678", "user": "agent",
        })

    def test_group_dm_opens_inside_broker_and_creates_resumable_bridge(self):
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "dm_notify",
            "text": "Welcome",
            "source_kind": "headless_run",
            "source": {"run_id": "onboarding-1", "cwd": "/tmp/project"},
            "user_ids": ["U12345678", "U87654321"],
            "idempotency_key": "onboarding-1",
        }
        with mock.patch.dict(
            os.environ,
            {"SLACK_ALLOWED_USERS": "U12345678,U87654321"},
            clear=False,
        ), mock.patch.object(self.runtime, "_slack_call", side_effect=[
            {"ok": True, "channel": {"id": "C12345678"}},
            {"ok": True, "team_id": "T12345678", "user_id": "U11111111", "user": "agent"},
        ]) as call, mock.patch.object(
            self.runtime, "slack_post", return_value="123.456",
        ) as post:
            result = broker.handle(request)
        self.assertEqual(call.call_args_list[0], mock.call(
            "test-token",
            "conversations.open",
            {"users": "U12345678,U87654321", "return_im": True},
        ))
        self.assertEqual(len(call.call_args_list), 2)
        post.assert_called_once()
        bridge = self.store.get(result["bridge_id"])
        self.assertEqual(bridge.channel_id, "C12345678")
        self.assertEqual(bridge.team_id, "T12345678")
        self.assertEqual(bridge.owner_user_id, "*")

    def test_group_dm_rejects_non_allowlisted_recipients_before_slack(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.dict(
            os.environ,
            {"SLACK_ALLOWED_USERS": "U12345678"},
            clear=False,
        ), mock.patch.object(self.runtime, "_slack_call") as call:
            with self.assertRaisesRegex(ValueError, "explicitly allowlisted"):
                broker.handle({
                    "op": "dm_notify",
                    "text": "Welcome",
                    "source_kind": "headless_run",
                    "source": {"run_id": "onboarding-2", "cwd": "/tmp/project"},
                    "user_ids": ["U12345678", "U87654321"],
                    "idempotency_key": "onboarding-2",
                })
        call.assert_not_called()

    def test_brokered_thread_post_does_not_create_a_second_bridge(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(broker, "_ensure_channel_membership"), mock.patch.object(
            self.runtime, "slack_post", return_value="123.457",
        ) as post:
            result = broker.handle({
                "op": "thread_reply", "channel_id": "C12345678",
                "thread_ts": "123.456", "text": "progress",
            })
        post.assert_called_once_with("test-token", "C12345678", "progress", "123.456")
        self.assertEqual(result["thread_ts"], "123.456")
        self.assertEqual(self.store.recent_active_bridges(), [])
        self.assertTrue(self.store.participates("", "C12345678", "123.456"))

    def test_attach_binds_existing_thread_without_posting(self):
        broker = self.runtime.Broker("test-token", self.store)
        with mock.patch.object(self.runtime, "slack_post") as post:
            result = broker.handle({
                "op": "attach",
                "source_kind": "claude_session",
                "source": {"session_id": "claude-1", "cwd": "/tmp/parcha"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "thread_ts": "123.456",
                "idempotency_key": "review-123.456",
            })
        post.assert_not_called()
        self.assertEqual(result["thread_ts"], "123.456")
        bridge = self.store.find("T12345678", "C12345678", "123.456")
        self.assertEqual(bridge.source_kind, "claude_session")
        self.assertEqual(bridge.source["session_id"], "claude-1")

    def test_attach_refuses_to_replace_active_binding(self):
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "attach", "source_kind": "claude_session",
            "source": {"session_id": "claude-1", "cwd": "/tmp/parcha"},
            "owner_user_id": "U12345678", "team_id": "T12345678",
            "channel_id": "C12345678", "thread_ts": "123.456",
            "idempotency_key": "review-one",
        }
        broker.handle(request)
        request["idempotency_key"] = "review-two"
        request["source"] = {"session_id": "claude-2", "cwd": "/tmp/parcha"}
        with self.assertRaisesRegex(ValueError, "already has an active"):
            broker.handle(request)

    def test_thread_participation_survives_store_reopen_and_team_lookup(self):
        self.store.mark_participation("T12345678", "C12345678", "123.456")
        reopened = self.runtime.Store(self.store.path)
        self.assertTrue(reopened.participates("T12345678", "C12345678", "123.456"))
        self.assertFalse(reopened.participates("T99999999", "C12345678", "123.456"))

    def test_participating_thread_ingress_is_deduplicated_and_kept_recent(self):
        self.store.mark_participation("T12345678", "C12345678", "123.456")
        self.assertTrue(self.store.mark_thread_ingress(
            "123.457", "T12345678", "C12345678", "123.456",
        ))
        self.assertFalse(self.store.mark_thread_ingress(
            "123.457", "T12345678", "C12345678", "123.456",
        ))
        self.assertTrue(self.store.has_ingress("123.457"))
        recent = self.store.recent_participating_threads()
        self.assertIn(
            ("T12345678", "C12345678", "123.456"),
            [item[:3] for item in recent],
        )
        self.assertIsInstance(recent[0][3], float)

    def test_concurrent_idempotent_notifications_post_one_root_message(self):
        broker = self.runtime.Broker("test-token", self.store)
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-concurrent", "cwd": "/tmp/project"},
            "idempotency_key": "run-concurrent",
        }
        barrier = threading.Barrier(2)

        def notify():
            barrier.wait()
            return broker.handle(dict(request))

        with mock.patch.dict(os.environ, {
            "SLACK_HOME_CHANNEL": "C12345678",
            "SLACK_ALLOWED_USERS": "U12345678",
        }, clear=False), mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(self.runtime, "slack_post", return_value="123.456") as post:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: notify(), range(2)))

        self.assertEqual(post.call_count, 1)
        self.assertEqual({result["thread_ts"] for result in results}, {"123.456"})
        self.assertEqual(sorted(result["deduplicated"] for result in results), [False, True])


class CredentialBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.bridge = self.runtime.Bridge(
            "brg_test", "codex_session", {"session_id": "session-1", "cwd": str(self.home)},
            "U12345678", "T12345678", "C12345678", "123.456", "key", "active",
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_gateway_secrets_are_absent_from_native_environment(self):
        config = self.runtime.Config()
        captured = {}

        class Process:
            returncode = 0
            pid = 12345

            def __init__(self, command, **kwargs):
                captured["command"] = command
                captured["env"] = kwargs["env"]

            def communicate(self, input=None, timeout=None):
                captured["input"] = input
                return "native answer", ""

        with mock.patch.dict(os.environ, {
            "SLACK_BOT_TOKEN": "not-forwarded",
            "OP_SERVICE_ACCOUNT_TOKEN": "not-forwarded",
            "HOME": str(self.home),
            "PATH": os.environ.get("PATH", ""),
        }, clear=False), mock.patch.object(self.runtime, "load_config", return_value=config), mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/codex"
        ), mock.patch.object(self.runtime.subprocess, "Popen", Process):
            output = self.runtime.continue_native(self.bridge, "private operator prompt")

        self.assertEqual(output, "native answer")
        self.assertNotIn("private operator prompt", captured["command"])
        self.assertEqual(captured["command"][-1], "-")
        self.assertEqual(captured["input"], "private operator prompt")
        self.assertNotIn("SLACK_BOT_TOKEN", captured["env"])
        self.assertNotIn("OP_SERVICE_ACCOUNT_TOKEN", captured["env"])

    def test_credential_helper_is_allowlisted_and_silent(self):
        config = self.runtime.Config(
            credential_command=("credential-helper",),
            credential_env_allowlist=("OPENAI_API_KEY", "OPENAI_BASE_URL"),
        )
        result = types.SimpleNamespace(returncode=0, stdout=json.dumps({"OPENAI_API_KEY": "short-lived"}))
        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/credential-helper"
        ), mock.patch.object(self.runtime.subprocess, "run", return_value=result) as run:
            values = self.runtime._credential_env(self.bridge, config)
        self.assertEqual(values, {"OPENAI_API_KEY": "short-lived"})
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertNotIn("SLACK_BOT_TOKEN", run.call_args.kwargs["env"])

    def test_credential_helper_rejects_unlisted_or_slack_keys(self):
        for values, allowlist in (
            ({"AWS_SECRET_ACCESS_KEY": "x"}, ("OPENAI_API_KEY",)),
            ({"SLACK_BOT_TOKEN": "x"}, ("SLACK_BOT_TOKEN",)),
        ):
            config = self.runtime.Config(
                credential_command=("credential-helper",), credential_env_allowlist=allowlist
            )
            result = types.SimpleNamespace(returncode=0, stdout=json.dumps(values))
            with mock.patch.object(
                self.runtime, "_resolve_executable", return_value="/usr/bin/credential-helper"
            ), mock.patch.object(self.runtime.subprocess, "run", return_value=result):
                with self.assertRaises(self.runtime.NativeContinuationError):
                    self.runtime._credential_env(self.bridge, config)

    def test_missing_configured_executable_fails_closed(self):
        with mock.patch.object(self.runtime.shutil, "which", return_value=None):
            with self.assertRaisesRegex(self.runtime.NativeContinuationError, "executable is unavailable"):
                self.runtime._resolve_executable("missing-agent-cli")

    def test_slack_egress_redacts_high_confidence_credentials(self):
        samples = (
            "xox" + "b-1234567890-abcdefghijklmnop",
            "xap" + "p-1-A1234567890-abcdefghijklmnop",
            "gh" + "p_abcdefghijklmnopqrstuvwxyz",
            "sk-abcdefghijklmnopqrstuvwxyz123456",
            "AKIAIOSFODNN7EXAMPLE",
            "-----BEGIN PRIVATE KEY-----\nsecret material\n-----END PRIVATE KEY-----",
        )
        redacted = self.runtime.redact_text("\n".join(samples))
        for sample in samples:
            self.assertNotIn(sample, redacted)
        self.assertGreaterEqual(redacted.count("[REDACTED_"), len(samples))

    def test_origin_is_sanitized_and_retained_at_maximum_message_size(self):
        bridge = self.runtime.Bridge(
            "brg_test", "zellij_pane",
            {"session_name": "work`\nspoof", "pane_id": "7", "cwd": "/tmp/project`\nspoof"},
            "U12345678", "T12345678", "C12345678", None, "key", "pending",
        )
        rendered = self.runtime.with_origin("x" * self.runtime.MAX_TEXT, bridge)
        self.assertLessEqual(len(rendered), self.runtime.MAX_TEXT)
        self.assertTrue(rendered.endswith("_"))
        self.assertIn("_Origin: Zellij `workspoof` / pane `7` · `projectspoof`_", rendered)

    def test_slack_transport_rejects_unapproved_api_methods(self):
        with self.assertRaisesRegex(ValueError, "unsupported Slack API method"):
            self.runtime._slack_call("hidden-token", "admin.users.list", {})

    def test_zellij_identity_requires_allowlisted_live_agent_and_returns_only_hash(self):
        panes = [{
            "id": 7,
            "is_plugin": False,
            "exited": False,
            "terminal_command": "node /opt/agents/codex --resume session-secret",
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ):
            identity = self.runtime.zellij_pane_identity("work", "7", "/tmp/project")
        self.assertEqual(identity["pane_agent"], "codex")
        self.assertEqual(len(identity["pane_command_hash"]), 64)
        self.assertNotIn("session-secret", json.dumps(identity))

        panes[0]["terminal_command"] = "bash"
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ):
            with self.assertRaisesRegex(self.runtime.NativeContinuationError, "not running an allowlisted agent"):
                self.runtime.zellij_pane_identity("work", "7")

    def test_zellij_identity_resolves_old_pane_with_null_command(self):
        panes = [{
            "id": 31,
            "is_plugin": False,
            "exited": False,
            "terminal_command": None,
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_zellij_agent_process",
            return_value=("claude", "proc:3381024:39575982:command-hash"),
        ) as process_identity:
            identity = self.runtime.zellij_pane_identity(
                "didactic-jellyfish", "31", "/tmp/project"
            )
        process_identity.assert_called_once_with(
            "didactic-jellyfish", "31", {"claude", "codex", "gemini", "hermes", "pi"}
        )
        self.assertEqual(identity["pane_agent"], "claude")
        self.assertEqual(len(identity["pane_command_hash"]), 64)

    def test_zellij_delivery_fails_closed_when_pane_process_changes(self):
        bridge = self.runtime.Bridge(
            "brg_test", "zellij_pane",
            {
                "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "codex", "pane_command_hash": "expected",
            },
            "U12345678", "T12345678", "C12345678", "123.456", "key", "active",
        )
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value={"pane_command_hash": "different"},
        ), mock.patch.object(self.runtime.subprocess, "run") as run:
            with self.assertRaisesRegex(self.runtime.NativeContinuationError, "different process"):
                self.runtime.deliver_zellij(bridge, "continue")
        run.assert_not_called()

    def test_native_zellij_delivery_verifies_visible_input_and_live_agent_after_enter(self):
        bridge = self.runtime.Bridge(
            "brg_test", "claude_session",
            {
                "session_id": "session-1", "zellij_session": "work",
                "zellij_pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "claude", "pane_command_hash": "expected",
            },
            "*", "T12345678", "C12345678", "123.456", "key", "active",
        )
        text = "review AJ's correction"
        marker = "tether-" + self.runtime.hashlib.sha256(
            f"{bridge.bridge_id}\0{text}".encode()
        ).hexdigest()[:12]

        def run(command, **_kwargs):
            if "dump-screen" in command:
                return types.SimpleNamespace(stdout=f"prompt contains {marker}", stderr="", returncode=0)
            return types.SimpleNamespace(stdout="", stderr="", returncode=0)

        with mock.patch.object(
            self.runtime, "zellij_pane_identity",
            return_value={"pane_command_hash": "expected"},
        ) as identity, mock.patch.object(self.runtime.subprocess, "run", side_effect=run) as invoked, mock.patch.object(
            self.runtime.time, "sleep"
        ), mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"):
            self.runtime.deliver_zellij(bridge, text)

        commands = [call.args[0] for call in invoked.call_args_list]
        self.assertTrue(any("write-chars" in command for command in commands))
        self.assertTrue(any("send-keys" in command and "Enter" in command for command in commands))
        self.assertEqual(sum("dump-screen" in command for command in commands), 2)
        self.assertGreaterEqual(identity.call_count, 2)
        written = next(
            command[-1] for command in commands
            if "write-chars" in command
        )
        self.assertIn("--reply-key " + marker, written)
        self.assertIn("at most one Slack message", written)
        self.assertIn("50 words", written)
        inbox = self.home / ".local" / "share" / "tether" / "inbox" / f"{marker}.txt"
        self.assertEqual(inbox.read_text().strip(), text)
        self.assertEqual(inbox.stat().st_mode & 0o777, 0o600)


class NotifierTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        data = self.home / "data" / "tether"
        data.mkdir(parents=True)
        shutil.copy2(RUNTIME_PATH, data / "bridge_runtime.py")
        env = {
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_DATA_HOME": str(self.home / "data"),
            "XDG_CONFIG_HOME": str(self.home / "config"),
        }
        self.env_patch = mock.patch.dict(os.environ, env, clear=False)
        self.env_patch.start()
        sys.modules.pop("bridge_runtime", None)
        spec = importlib.util.spec_from_file_location(f"notifier_test_{id(self)}", NOTIFIER_PATH)
        self.notifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.notifier)

    def tearDown(self):
        self.env_patch.stop()
        sys.modules.pop("bridge_runtime", None)
        self.temp.cleanup()

    def test_explicit_run_id_precedes_ambient_sessions(self):
        args = types.SimpleNamespace(run_id="cron-2026", hermes_session_id=None)
        with mock.patch.dict(os.environ, {
            "CLAUDE_CODE_SESSION_ID": "claude-session",
            "CODEX_THREAD_ID": "codex-session",
        }, clear=False):
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "headless_run")
        self.assertEqual(source["run_id"], "cron-2026")

    def test_native_session_keeps_zellij_metadata(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": str(pathlib.Path.cwd()),
            "pane_agent": "claude", "pane_command_hash": "abc123",
        }
        with mock.patch.dict(os.environ, {
            "CLAUDE_CODE_SESSION_ID": "claude-session",
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(self.notifier, "zellij_pane_identity", return_value=identity):
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "claude_session")
        self.assertEqual(source["zellij_session"], "work")
        self.assertEqual(source["zellij_pane_id"], "7")
        self.assertEqual(source["pane_command_hash"], "abc123")

    def test_zellij_only_source_captures_process_identity(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
            "pane_agent": "codex", "pane_command_hash": "abc123",
        }
        with mock.patch.dict(os.environ, {
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(self.notifier, "zellij_pane_identity", return_value=identity) as capture:
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "zellij_pane")
        self.assertEqual(source["pane_command_hash"], "abc123")
        capture.assert_called_once_with("work", "7", str(pathlib.Path.cwd()))

    def test_rebind_captures_the_current_exact_pane(self):
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
            "pane_agent": "claude", "pane_command_hash": "new-fingerprint",
        }
        with mock.patch.dict(os.environ, {
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(
            self.notifier, "zellij_pane_identity", return_value=identity,
        ), mock.patch.object(
            self.notifier, "broker_call",
            return_value={"ok": True, "thread_ts": "123.456"},
        ) as broker:
            result = self.notifier.main([
                "rebind", "--channel", "C12345678", "--thread-ts", "123.456",
            ])
        self.assertEqual(result, 0)
        request = broker.call_args.args[0]
        self.assertEqual(request["op"], "rebind")
        self.assertEqual(request["source"]["pane_command_hash"], "new-fingerprint")

    def test_attach_captures_an_explicit_existing_native_pane(self):
        identity = {
            "session_name": "didactic-jellyfish",
            "pane_id": "31",
            "cwd": "/tmp/project",
            "pane_agent": "claude",
            "pane_command_hash": "exact-fingerprint",
        }
        with mock.patch.object(
            self.notifier, "zellij_pane_identity", return_value=identity,
        ) as capture, mock.patch.object(
            self.notifier, "broker_call",
            return_value={
                "ok": True,
                "bridge_id": "brg_test",
                "thread_ts": "123.456",
            },
        ) as broker:
            result = self.notifier.main([
                "attach",
                "--channel", "C12345678",
                "--thread-ts", "123.456",
                "--claude-session-id", "claude-session",
                "--zellij-session", "didactic-jellyfish",
                "--zellij-pane-id", "31",
                "--cwd", "/tmp/project",
                "--idempotency-key", "attach-existing-123",
                "--json",
            ])
        self.assertEqual(result, 0)
        capture.assert_called_once_with(
            "didactic-jellyfish", "31", "/tmp/project"
        )
        request = broker.call_args.args[0]
        self.assertEqual(request["source_kind"], "claude_session")
        self.assertEqual(request["source"]["session_id"], "claude-session")
        self.assertEqual(
            request["source"]["pane_command_hash"], "exact-fingerprint"
        )

    def test_attach_requires_a_complete_explicit_pane(self):
        with self.assertRaisesRegex(
            SystemExit, "--zellij-session and --zellij-pane-id"
        ):
            self.notifier.main([
                "attach",
                "--channel", "C12345678",
                "--thread-ts", "123.456",
                "--claude-session-id", "claude-session",
                "--zellij-session", "work",
                "--idempotency-key", "attach-existing-123",
            ])

    def test_noninteractive_setup_delegates_manifest_to_hermes(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        completed = types.SimpleNamespace(returncode=0)
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", return_value=completed
        ) as run:
            self.assertEqual(self.notifier.run_setup(args), 0)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/bin/hermes", "plugins", "enable", "tether"],
                ["/usr/bin/hermes", "config", "set", "slack.allow_bots", "all"],
                ["/usr/bin/hermes", "config", "set", "display.busy_ack_enabled", "false"],
                ["/usr/bin/hermes", "slack", "manifest", "--write"],
            ],
        )

    def test_interactive_setup_runs_hermes_onboarding_restart_and_live_doctor(self):
        args = types.SimpleNamespace(non_interactive=False, no_restart=False)
        completed = types.SimpleNamespace(returncode=0)
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", return_value=completed
        ) as run, mock.patch.object(self.notifier, "doctor", return_value=(True, ["ok live broker"])):
            self.assertEqual(self.notifier.run_setup(args), 0)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/bin/hermes", "plugins", "enable", "tether"],
                ["/usr/bin/hermes", "config", "set", "slack.allow_bots", "all"],
                ["/usr/bin/hermes", "config", "set", "display.busy_ack_enabled", "false"],
                ["/usr/bin/hermes", "gateway", "setup"],
                ["/usr/bin/hermes", "gateway", "restart"],
            ],
        )
        self.assertEqual(
            [call.kwargs["timeout"] for call in run.call_args_list],
            [
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SETUP_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
            ],
        )

    def test_setup_disables_detected_legacy_bridge_before_restart(self):
        legacy = self.home / ".hermes" / "plugins" / "session-bridge"
        legacy.mkdir(parents=True)
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", return_value=types.SimpleNamespace(returncode=0)
        ) as run:
            result = self.notifier._enable_plugin("/usr/bin/hermes")
        self.assertEqual(result, 0)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/bin/hermes", "plugins", "enable", "tether"],
                ["/usr/bin/hermes", "plugins", "disable", "session-bridge"],
            ],
        )


class PluginRoutingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        sys.modules["bridge_runtime"] = self.runtime
        spec = importlib.util.spec_from_file_location(f"session_bridge_test_{id(self)}", PLUGIN_PATH)
        self.plugin = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.plugin
        spec.loader.exec_module(self.plugin)
        self.plugin_module_name = spec.name
        self.config = self.home / ".config" / "tether" / "config.toml"
        self.config.parent.mkdir(parents=True)
        self.config.write_text('allowed_users = ["U12345678"]\n')
        self.env_patch = mock.patch.dict(os.environ, {
            "SLACK_ALLOWED_USERS": "", "GATEWAY_ALLOWED_USERS": "",
        }, clear=False)
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()
        sys.modules.pop("bridge_runtime", None)
        sys.modules.pop(self.plugin_module_name, None)
        self.temp.cleanup()

    def make_bridge(self, owner="U12345678"):
        bridge = self.plugin.store.create({
            "source_kind": "headless_run",
            "source": {"run_id": "cron-1", "cwd": "/tmp/project"},
            "owner_user_id": owner,
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "cron-1",
        })
        return self.plugin.store.bind(bridge.bridge_id, "123.456")

    def test_imports_recent_native_slack_sessions_for_restart_recovery(self):
        sessions = self.runtime.HERMES_HOME / "sessions" / "sessions.json"
        sessions.parent.mkdir(parents=True)
        sessions.write_text(json.dumps({
            "agent:main:slack:dm:C0BHSK52GP5:1784319237.201969": {
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "origin": {
                    "platform": "slack",
                    "chat_id": "C0BHSK52GP5",
                    "thread_id": "1784319237.201969",
                },
            },
            "old": {
                "updated_at": "2020-01-01T00:00:00+00:00",
                "origin": {
                    "platform": "slack",
                    "chat_id": "COLD00000",
                    "thread_id": "100.000",
                },
            },
        }))
        adapter = types.SimpleNamespace(
            _channel_team={"C0BHSK52GP5": "T12345678"},
        )
        imported = self.plugin._import_native_slack_participation(adapter)
        self.assertEqual(imported, 1)
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C0BHSK52GP5", "1784319237.201969",
        ))
        participation = self.plugin.store.recent_participating_threads(
            hours=24 * 365, limit=10,
        )
        self.assertLess(participation[0][3], datetime.datetime.now().timestamp() + 1)

    def test_import_uses_newest_sessions_when_store_exceeds_limit(self):
        sessions = self.runtime.HERMES_HOME / "sessions" / "sessions.json"
        sessions.parent.mkdir(parents=True)
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            f"old-{index}": {
                "updated_at": (now - datetime.timedelta(days=30)).isoformat(),
                "origin": {
                    "platform": "slack",
                    "chat_id": "COLD00000",
                    "thread_id": f"{index}.000",
                },
            }
            for index in range(2000)
        }
        payload["recent"] = {
            "updated_at": now.isoformat(),
            "origin": {
                "platform": "slack",
                "chat_id": "C0BHSK52GP5",
                "thread_id": "1784319237.201969",
            },
        }
        sessions.write_text(json.dumps(payload))
        adapter = types.SimpleNamespace(
            _channel_team={"C0BHSK52GP5": "T12345678"},
        )

        imported = self.plugin._import_native_slack_participation(adapter)

        self.assertEqual(imported, 1)
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C0BHSK52GP5", "1784319237.201969",
        ))

    def test_authorization_fails_closed_and_honors_owner(self):
        bridge = self.make_bridge()
        self.assertTrue(self.plugin._authorized(bridge, "U12345678"))
        self.assertFalse(self.plugin._authorized(bridge, "U99999999"))
        self.config.write_text("allowed_users = []\n")
        self.assertFalse(self.plugin._authorized(bridge, "U12345678"))

    def test_authorization_reuses_hermes_allowlist(self):
        self.config.write_text("allowed_users = []\n")
        bridge = self.make_bridge(owner="*")
        with mock.patch.dict(os.environ, {"SLACK_ALLOWED_USERS": "U12345678"}, clear=False):
            self.assertTrue(self.plugin._authorized(bridge, "U12345678"))

    def test_unauthorized_bridge_reply_does_not_keep_success_reaction(self):
        self.make_bridge(owner="U12345678")

        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform, thread_id="123.456", guild_id="T12345678",
            chat_id="C12345678", user_id="U99999999", message_id="111.1",
            is_bot=False,
        )
        event = types.SimpleNamespace(source=source, message_id="111.1", text="continue")

        class Adapter:
            _reacting_message_ids = {"111.1"}

            def __init__(self):
                self.removed = []

            async def _remove_reaction(self, channel, event_id, reaction):
                self.removed.append((channel, event_id, reaction))

        adapter = Adapter()
        gateway = types.SimpleNamespace(adapters={platform: adapter})

        async def exercise():
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
            await asyncio.sleep(0)
            return result

        result = asyncio.run(exercise())
        self.assertEqual(result["reason"], "bridge-user-not-authorized")
        self.assertNotIn("111.1", adapter._reacting_message_ids)
        self.assertEqual(adapter.removed, [("C12345678", "111.1", "eyes")])

    def test_exact_thread_prefilter_marks_only_active_bridge(self):
        self.make_bridge()
        adapter = types.SimpleNamespace(_bot_message_ts=set(), _channel_team={"C12345678": "T12345678"})
        self.assertTrue(self.plugin._mark_bridge_thread_before_slack_gate(adapter, {
            "thread_ts": "123.456", "channel": "C12345678",
        }))
        self.assertIn("123.456", adapter._bot_message_ts)
        self.assertFalse(self.plugin._mark_bridge_thread_before_slack_gate(adapter, {
            "thread_ts": "999.999", "channel": "C12345678",
        }))

    def test_prefilter_marks_persisted_non_bridge_participation(self):
        self.plugin.store.mark_participation("T12345678", "C12345678", "123.456")
        adapter = types.SimpleNamespace(_bot_message_ts=set(), _channel_team={"C12345678": "T12345678"})
        self.assertTrue(self.plugin._mark_bridge_thread_before_slack_gate(adapter, {
            "thread_ts": "123.456", "channel": "C12345678",
        }))
        self.assertIn("123.456", adapter._bot_message_ts)

    def test_native_send_persists_thread_participation(self):
        class SlackAdapter:
            _tether_prefilter = False

            def __init__(self):
                self._bot_message_ts = set()
                self._channel_team = {"C12345678": "T12345678"}

            async def connect(self):
                return True

            async def send(self, channel, content, reply_to=None, metadata=None):
                return {"ok": True}

            async def _handle_slack_message(self, event):
                return event

        modules = {
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SlackAdapter
        with mock.patch.dict(sys.modules, modules), mock.patch.object(self.plugin, "_ensure_reply_poller"):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            asyncio.run(adapter.send("C12345678", "done", "123.456", None))
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C12345678", "123.456",
        ))

    def test_existing_bot_thread_is_discovered_once_and_persisted(self):
        class Client:
            calls = 0

            async def conversations_replies(self, **kwargs):
                self.calls += 1
                return {"messages": [
                    {"ts": "123.456", "user": "UHUMAN001"},
                    {"ts": "123.457", "user": "UBOT00001", "bot_id": "BBOT00001"},
                ]}

        client = Client()
        adapter = types.SimpleNamespace(
            _bot_message_ts=set(),
            _channel_team={"C12345678": "T12345678"},
            _team_bot_user_ids={"T12345678": "UBOT00001"},
            _get_client=lambda _channel: client,
        )
        event = {"thread_ts": "123.456", "channel": "C12345678"}
        self.assertTrue(asyncio.run(
            self.plugin._discover_existing_thread_participation(adapter, event)
        ))
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C12345678", "123.456",
        ))
        self.assertEqual(client.calls, 1)

    def test_prefilter_admits_unmentioned_reply_before_hermes_mention_gate(self):
        self.make_bridge()

        class SlackAdapter:
            _tether_prefilter = False

            def __init__(self):
                self._bot_message_ts = set()
                self._channel_team = {"C12345678": "T12345678"}
                self.sent = []

            async def connect(self):
                return True

            async def send(self, channel, content, metadata=None):
                self.sent.append((channel, content, metadata))
                return {"ok": True}

            async def _handle_slack_message(self, event, payload=None):
                return event.get("thread_ts") in self._bot_message_ts, payload

        modules = {
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SlackAdapter
        with mock.patch.dict(sys.modules, modules), mock.patch.object(self.plugin, "_ensure_reply_poller"):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            admitted, payload = asyncio.run(adapter._handle_slack_message({
                "ts": "111.1", "thread_ts": "123.456", "channel": "C12345678",
            }, {"team_id": "T12345678"}))
            ignored, _ = asyncio.run(adapter._handle_slack_message({
                "ts": "111.2", "thread_ts": "999.999", "channel": "C12345678",
            }))
            suppressed = asyncio.run(adapter.send("C12345678", "NO_REPLY"))
            delivered = asyncio.run(adapter.send("C12345678", "NO_REPLY is a control token"))
        self.assertTrue(admitted)
        self.assertEqual(payload, {"team_id": "T12345678"})
        self.assertFalse(ignored)
        self.assertTrue(suppressed["suppressed"])
        self.assertEqual(len(adapter.sent), 1)
        self.assertEqual(adapter.sent[0][1], "NO_REPLY is a control token")
        self.assertTrue(delivered["ok"])

    def test_live_hermes_adapter_alias_precedes_source_tree_fallback(self):
        class LiveSlackAdapter:
            async def _handle_slack_message(self, event):
                return event

        class SourceTreeSlackAdapter(LiveSlackAdapter):
            pass

        modules = {
            "hermes_plugins": types.ModuleType("hermes_plugins"),
            "hermes_plugins.slack_platform": types.ModuleType("hermes_plugins.slack_platform"),
            "hermes_plugins.slack_platform.adapter": types.ModuleType("hermes_plugins.slack_platform.adapter"),
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["hermes_plugins.slack_platform.adapter"].SlackAdapter = LiveSlackAdapter
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SourceTreeSlackAdapter
        with mock.patch.dict(sys.modules, modules):
            self.assertIs(self.plugin._resolve_slack_adapter(), LiveSlackAdapter)

    def test_reply_poller_recovers_only_authorized_unseen_human_reply(self):
        bridge = self.make_bridge()
        messages = [
            {"ts": bridge.thread_ts, "text": "root", "bot_id": "B12345678"},
            {"ts": "111.1", "thread_ts": bridge.thread_ts, "text": "continue", "user": "U12345678"},
            {"ts": "111.2", "thread_ts": bridge.thread_ts, "text": "no", "user": "U99999999"},
            {"ts": "111.3", "thread_ts": bridge.thread_ts, "text": "bot", "bot_id": "B12345678"},
        ]

        class Client:
            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible DM must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            def __init__(self):
                self.events = []

            def _get_client(self, _channel):
                return Client()

            async def _handle_slack_message(self, event):
                self.events.append(event)
                source = types.SimpleNamespace(
                    platform=platform,
                    thread_id=event["thread_ts"],
                    guild_id="T12345678",
                    chat_id=event["channel"],
                    user_id=event["user"],
                    message_id=event["ts"],
                    is_bot=False,
                )
                gateway_event = types.SimpleNamespace(
                    source=source,
                    message_id=event["ts"],
                    text=event["text"],
                )
                result = self_plugin._pre_gateway_dispatch(
                    event=gateway_event,
                    gateway=types.SimpleNamespace(adapters={platform: self}),
                )
                self.events[-1]["dispatch_result"] = result

        self_plugin = self.plugin

        class Platform:
            value = "slack"

        platform = Platform()
        adapter = Adapter()
        recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
        recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 1)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(adapter.events[0]["text"], "continue")
        self.assertTrue(adapter.events[0]["_tether_polled"])
        self.assertEqual(adapter.events[0]["dispatch_result"]["action"], "rewrite")

    def test_reply_poller_joins_only_after_not_in_channel(self):
        self.make_bridge()
        calls = []

        class Client:
            async def conversations_history(self, **_kwargs):
                calls.append("history")
                raise RuntimeError("Slack API error: not_in_channel")

            async def conversations_join(self, **_kwargs):
                calls.append("join")
                return {"ok": True}

            async def conversations_replies(self, **_kwargs):
                return {"messages": []}

        class Adapter:
            def _get_client(self, _channel):
                return Client()

            async def _handle_slack_message(self, _event):
                raise AssertionError("no messages should be dispatched")

        recovered = asyncio.run(self.plugin._poll_recent_replies(Adapter()))
        self.assertEqual(recovered, 0)
        self.assertEqual(calls, ["history", "join"])

    def test_reply_poller_recovers_unmentioned_participating_thread_reply(self):
        self.plugin.store.mark_participation(
            "T12345678", "C12345678", "123.456",
        )
        messages = [
            {"ts": "123.456", "text": "root", "user": "U12345678"},
            {
                "ts": "123.457", "thread_ts": "123.456",
                "text": "did you see this?", "user": "U12345678",
            },
        ]

        class Client:
            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible DM must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            def __init__(self):
                self.events = []

            def _get_client(self, _channel):
                return Client()

            async def _handle_slack_message(self, event):
                self.events.append(event)
        adapter = Adapter()
        recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
        recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 1)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(adapter.events[0]["text"], "did you see this?")
        self.assertTrue(adapter.events[0]["_tether_polled"])

    def test_reply_poller_recovers_peer_bot_thread_turns_when_enabled(self):
        bridge = self.make_bridge()
        messages = [
            {"ts": bridge.thread_ts, "text": "root", "bot_id": "BLOCAL", "user": "ULOCAL"},
            {
                "ts": "111.1", "thread_ts": bridge.thread_ts,
                "text": "<@ULOCAL> challenge this premise", "bot_id": "BPEER", "user": "UPEER",
                "subtype": "bot_message",
            },
            {
                "ts": "111.2", "thread_ts": bridge.thread_ts,
                "text": "general bot chatter", "bot_id": "BPEER", "user": "UPEER",
                "subtype": "bot_message",
            },
        ]

        class Client:
            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible thread must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            _bot_user_id = "ULOCAL"
            _team_bot_user_ids = {"T12345678": "ULOCAL"}
            config = types.SimpleNamespace(extra={"allow_bots": "all"})

            def __init__(self):
                self.events = []

            def _get_client(self, _channel):
                return Client()

            async def _handle_slack_message(self, event):
                self.events.append(event)
                source = types.SimpleNamespace(
                    platform=platform,
                    thread_id=event["thread_ts"],
                    guild_id="T12345678",
                    chat_id=event["channel"],
                    user_id=event["user"],
                    message_id=event["ts"],
                    is_bot=True,
                )
                gateway_event = types.SimpleNamespace(
                    source=source,
                    message_id=event["ts"],
                    text=event["text"],
                )
                result = self_plugin._pre_gateway_dispatch(
                    event=gateway_event,
                    gateway=types.SimpleNamespace(adapters={platform: self}),
                )
                self.events[-1]["dispatch_result"] = result

        self_plugin = self.plugin

        class Platform:
            value = "slack"

        platform = Platform()
        adapter = Adapter()
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
            recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 2)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(adapter.events[0]["ts"], "111.1")
        self.assertEqual(adapter.events[0]["dispatch_result"]["action"], "rewrite")

    def test_peer_bot_requires_explicit_tether_allowlist(self):
        adapter = types.SimpleNamespace(
            _bot_user_id="ULOCAL",
            _team_bot_user_ids={"T12345678": "ULOCAL"},
            config=types.SimpleNamespace(extra={"allow_bots": "all"}),
        )
        event = {
            "text": "general bot chatter",
            "bot_id": "BPEER",
            "user": "UPEER",
            "subtype": "bot_message",
        }
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(self.plugin._allows_bot_message(adapter, event, "T12345678"))
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UOTHER,UPEER"}, clear=True):
            self.assertTrue(self.plugin._allows_bot_message(adapter, event, "T12345678"))

    def test_peer_bot_can_be_allowlisted_by_bot_id_when_user_is_absent(self):
        adapter = types.SimpleNamespace(
            _bot_user_id="ULOCAL",
            _team_bot_user_ids={"T12345678": "ULOCAL"},
            config=types.SimpleNamespace(extra={"allow_bots": "all"}),
        )
        event = {
            "text": ":tada: New signup: person@example.com",
            "bot_id": "BPOSTHOG",
            "subtype": "bot_message",
        }
        with mock.patch.dict(
            os.environ, {"TETHER_ALLOWED_BOT_USERS": "BPOSTHOG"}, clear=True,
        ):
            self.assertTrue(self.plugin._allows_bot_message(adapter, event, "T12345678"))

    def test_trusted_peer_bot_in_bound_thread_routes_to_bound_session(self):
        self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform, thread_id="123.456", guild_id="T12345678",
            chat_id="C12345678", user_id="UPEER", message_id="111.1", is_bot=True,
        )
        event = types.SimpleNamespace(source=source, message_id="111.1", text="challenge this")
        gateway = types.SimpleNamespace(adapters={platform: types.SimpleNamespace()})
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["action"], "rewrite")
        self.assertIn("challenge this", result["text"])

    def test_untrusted_peer_bot_cannot_enter_bound_thread(self):
        self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform, thread_id="123.456", guild_id="T12345678",
            chat_id="C12345678", user_id="UUNTRUSTED", message_id="111.1", is_bot=True,
        )
        event = types.SimpleNamespace(source=source, message_id="111.1", text="run this")
        gateway = types.SimpleNamespace(adapters={platform: types.SimpleNamespace()})
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["reason"], "bridge-bot-not-authorized")

    def test_native_delta_drops_synthetic_thread_history(self):
        text = "old transcript\n[End of thread context]\nplease continue"
        self.assertEqual(self.plugin._reply_delta(text), "please continue")

    def test_authorized_headless_reply_rewrites_into_durable_hermes_context(self):
        self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform, thread_id="123.456", guild_id="T12345678",
            chat_id="C12345678", user_id="U12345678", message_id="111.1",
        )
        event = types.SimpleNamespace(source=source, message_id="111.1", text="continue the run")
        adapter = types.SimpleNamespace(_reacting_message_ids=set())
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["action"], "rewrite")
        self.assertIn("Durable Hermes continuation", result["text"])
        self.assertIn("continue the run", result["text"])

    def test_restart_recovery_delivers_queued_reply_and_marks_it_complete(self):
        bridge = self.make_bridge()
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "continue after restart")
        replies = []

        def broker_call(request):
            replies.append(request["text"])
            return {"ok": True}

        with mock.patch.object(self.plugin, "broker_call", side_effect=broker_call), mock.patch.object(
            self.plugin, "_run_recovered_event", return_value="finished after restart"
        ):
            self.plugin._recover_queued_events()

        with self.plugin.store.connect() as database:
            state = database.execute("SELECT state FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(state, "delivered")
        self.assertIn("finished after restart", replies)

    def test_busy_native_followups_share_one_agent_turn_and_one_slack_reply(self):
        bridge = self.plugin.store.create({
            "source_kind": "claude_session",
            "source": {"session_id": "claude-1", "cwd": "/tmp/project"},
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "claude-batch",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "456.789")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "first follow-up")
        self.plugin.store.enqueue_event("111.2", bridge.bridge_id, "latest follow-up")

        class Platform:
            value = "slack"

        platform = Platform()

        class Adapter:
            def __init__(self):
                self.sent = []

            async def send(self, channel, text, metadata):
                self.sent.append((channel, text, metadata))

        adapter = Adapter()
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        prompts = []

        def continue_native(_bridge, prompt, _cancellation):
            prompts.append(prompt)
            return "Fixed and verified."

        with mock.patch.object(self.plugin, "continue_native", side_effect=continue_native):
            asyncio.run(self.plugin._drain_bridge(bridge.bridge_id, gateway, platform))

        self.assertEqual(len(prompts), 1)
        self.assertIn("first follow-up", prompts[0])
        self.assertIn("latest follow-up", prompts[0])
        self.assertEqual(len(adapter.sent), 1)
        with self.plugin.store.connect() as database:
            states = [
                row[0] for row in database.execute(
                    "SELECT state FROM bridge_events ORDER BY event_id"
                ).fetchall()
            ]
        self.assertEqual(states, ["delivered", "delivered"])

    def test_cancel_reply_discards_queued_work(self):
        bridge = self.plugin.store.create({
            "source_kind": "codex_session",
            "source": {"session_id": "codex-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "codex-1",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "456.789")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "queued work")

        class Platform:
            value = "slack"

        platform = Platform()
        source = types.SimpleNamespace(
            platform=platform, thread_id="456.789", guild_id="T12345678",
            chat_id="C12345678", user_id="U12345678", message_id="111.2",
        )
        event = types.SimpleNamespace(source=source, message_id="111.2", text="stop")
        sent = []

        class Adapter:
            _reacting_message_ids = set()

            async def send(self, channel, text, metadata):
                sent.append((channel, text, metadata))

        gateway = types.SimpleNamespace(adapters={platform: Adapter()})

        async def exercise():
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
            await asyncio.sleep(0)
            return result

        result = asyncio.run(exercise())
        self.assertEqual(result["reason"], "tether-cancel")
        self.assertIn("Cancelled 1 queued replies", sent[0][1])
        with self.plugin.store.connect() as database:
            state = database.execute("SELECT state FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(state, "failed")


class InstallerAndPackageTest(unittest.TestCase):
    def run_installer(self, home, harness="both"):
        env = {
            **os.environ,
            "HOME": str(home),
            "CODEX_HOME": str(home / "codex"),
            "CLAUDE_HOME": str(home / "claude"),
            "HERMES_HOME": str(home / "hermes"),
            "XDG_DATA_HOME": str(home / "data"),
            "XDG_CONFIG_HOME": str(home / "config"),
        }
        return subprocess.run(
            [str(INSTALL_PATH), f"--harness={harness}"], env=env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def test_installer_supports_both_harnesses_and_preserves_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = pathlib.Path(tmp)
            for harness in ("codex", "claude"):
                legacy = home / harness / "skills" / "hermes-slack-bridge" / "scripts"
                legacy.mkdir(parents=True)
                (legacy / "hermes_notify.py").write_text(
                    'notify.add_argument("--owner", default="U12345678")\n'
                )
            first = self.run_installer(home)
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            for harness in ("codex", "claude"):
                self.assertTrue((home / harness / "skills" / "tether" / "SKILL.md").is_file())
                compatibility_client = (
                    home / harness / "skills" / "hermes-slack-bridge" / "scripts" / "hermes_notify.py"
                ).read_text()
                self.assertIn('notify.add_argument("--owner")', compatibility_client)
                self.assertNotIn('default="U12345678"', compatibility_client)
                compatibility_skill = (
                    home / harness / "skills" / "hermes-slack-bridge" / "SKILL.md"
                ).read_text()
                self.assertIn("For a shared Slack channel, omit `--owner`", compatibility_skill)
            self.assertTrue((home / "hermes" / "plugins" / "tether" / "__init__.py").is_file())
            manifest = home / "hermes" / "plugins" / "tether" / "plugin.yaml"
            self.assertTrue(manifest.is_file())
            self.assertIn("name: tether", manifest.read_text())
            config = home / "config" / "tether" / "config.toml"
            config.write_text('default_channel = "C12345678"\nallowed_users = ["U12345678"]\n')
            config.chmod(0o600)
            second = self.run_installer(home)
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertIn("C12345678", config.read_text(), "upgrades must preserve operator config")

    def test_one_command_setup_installs_then_uses_hermes_onboarding(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = pathlib.Path(tmp)
            fake_bin = home / "fake-bin"
            fake_bin.mkdir()
            hermes = fake_bin / "hermes"
            hermes.write_text("#!/bin/sh\nexit 0\n")
            hermes.chmod(0o700)
            env = {
                **os.environ,
                "HOME": str(home),
                "CODEX_HOME": str(home / "codex"),
                "CLAUDE_HOME": str(home / "claude"),
                "HERMES_HOME": str(home / "hermes"),
                "XDG_DATA_HOME": str(home / "data"),
                "XDG_CONFIG_HOME": str(home / "config"),
                "PATH": str(fake_bin) + os.pathsep + os.environ.get("PATH", ""),
            }
            result = subprocess.run(
                ["node", str(ROOT / "bin" / "tether.js"), "setup", "--harness=both", "--non-interactive"],
                env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((home / "codex" / "skills" / "tether" / "SKILL.md").is_file())
            self.assertTrue((home / "claude" / "skills" / "tether" / "SKILL.md").is_file())
            self.assertIn("Slack manifest generated", result.stdout)

    def test_public_tree_contains_no_known_private_identifiers_or_token_values(self):
        private_digests = {
            (12, "4e3c100b7e146ea64d5774c9fdddad6a9a3ec84bfd1c7b2ac8920f70f9f8ac64"),
            (12, "d1f84d64ec000ce0626824ba6112ac1443d20362bfe9f628e445e2c5f1577ce7"),
            (12, "a401343ad071c39758b906a27b1edbb3d4857b8ad4eaa6f1a37ae7ca3c7a8b83"),
            (26, "2d276b2dbe1b68717dcc126768295b9e85562c397f2165ee44c1c7861871d6e3"),
            (22, "3b627447db98828cf775837753f511df98a4c1717274f5d51815106189da5437"),
        }
        text = "\n".join(
            path.read_text(errors="ignore")
            for path in ROOT.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        )
        for length, digest in private_digests:
            windows = (text[index:index + length] for index in range(max(0, len(text) - length + 1)))
            self.assertFalse(any(hashlib.sha256(value.encode()).hexdigest() == digest for value in windows))
        self.assertNotIn("xox" + "b-", text)
        self.assertNotIn("xap" + "p-", text)

    def test_skill_references_and_manifests_are_complete(self):
        skill = ROOT / "skills" / "tether"
        text = (skill / "SKILL.md").read_text()
        self.assertIn("name: tether", text)
        self.assertTrue((skill / "references" / "setup.md").is_file())
        self.assertTrue((skill / "references" / "contract.md").is_file())
        package = json.loads((ROOT / "package.json").read_text())
        self.assertEqual(package["pi"]["skills"], ["./skills"])
        for manifest in (ROOT / ".claude-plugin" / "plugin.json", ROOT / ".codex-plugin" / "plugin.json"):
            payload = json.loads(manifest.read_text())
            self.assertEqual(payload["name"], "tether")
            self.assertEqual(payload["version"], package["version"])
        plugin_manifest = (ROOT / "runtime" / "plugin" / "plugin.yaml").read_text()
        self.assertIn(f"version: {package['version']}", plugin_manifest)


if __name__ == "__main__":
    unittest.main()
