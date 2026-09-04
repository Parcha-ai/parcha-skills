"""End to end: a Slack message on a bound thread becomes a schema-18 turn, runs
through the exact-turn driver against a fake harness, and the reply leaves via
the host's egress. Every step is checked against the schema's own invariants.
"""

from __future__ import annotations

import importlib
import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("domain_runtime", "domain_schema", "native_driver"):
            sys.modules.pop(name, None)
        sys.modules.pop("plugin_next", None)
        sys.modules.pop("plugin_next.active", None)
        domain_runtime = importlib.import_module("domain_runtime")
        domain_schema = importlib.import_module("domain_schema")
        native_driver = importlib.import_module("native_driver")
        plugin_next = importlib.import_module("plugin_next")
        active = importlib.import_module("plugin_next.active")
        return domain_runtime, domain_schema, native_driver, plugin_next, active
    finally:
        sys.path[:] = previous


class FakeSource:
    def __init__(self, thread="100.1", message_id="170.500", user_id="U12345678"):
        self.platform = types.SimpleNamespace(value="slack")
        self.chat_id = "C1"
        self.chat_type = "thread"
        self.thread_id = thread
        self.parent_chat_id = "C1"
        self.user_id = user_id
        self.is_bot = False
        self.scope_id = "T12345678"
        self.guild_id = None
        self.message_id = message_id


class FakeEvent:
    def __init__(self, text, **kwargs):
        self.source = FakeSource(**kwargs)
        self.message_id = self.source.message_id
        self.text = text


class ActiveSliceTest(unittest.TestCase):
    def setUp(self):
        (
            self.domain_runtime,
            self.schema,
            self.native_driver,
            self.plugin_next,
            self.active,
        ) = load()
        self.temp = tempfile.TemporaryDirectory(prefix="tether-active-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        self.db = base / "domain.db"
        connection = sqlite3.connect(self.db)
        try:
            self.schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={self.schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        self.runtime = self.domain_runtime.DomainRuntime(self.db)
        self.driver = self.native_driver.NativeDriver(self.runtime, work_root=base / "driver")
        self.descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        self.sent: list[tuple[str, str, str]] = []
        self.prompts: list[str] = []

    def tearDown(self):
        self.temp.cleanup()

    def make_slice(self, script: str):
        def factory(context, settings, prompt):
            self.prompts.append(prompt)
            return ["/bin/sh", "-c", script]

        settings = self.active.ActiveSettings(enabled=True, native_timeout_seconds=30)
        return self.active.ActiveSlice(
            runtime=self.runtime,
            driver=self.driver,
            settings=settings,
            egress=lambda channel, thread, text: self.sent.append((channel, thread, text)),
            descriptor=self.descriptor,
            command_factory=factory,
        )

    def violations(self):
        connection = sqlite3.connect(self.db)
        try:
            return self.schema.invariant_violations(connection)
        finally:
            connection.close()

    def fields(self, event):
        return self.plugin_next._event_fields(event)

    def test_bound_thread_message_is_answered_through_the_driver(self):
        slice_ = self.make_slice("printf 'claro <@U12345678>, ya quedo.'")
        binding = slice_.bind(
            source_kind="claude_session", session_id="sess-1", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="100.1",
            owner_user_id="U12345678",
        )
        self.assertEqual(binding["state"], "active")

        claimed = slice_.claim(self.fields(FakeEvent("can you ship it?")), "can you ship it?")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["binding_id"], binding["binding_id"])

        self.assertEqual(slice_.run_once(), 1)
        self.assertEqual(self.sent, [("C1", "100.1", "claro <@U12345678>, ya quedo.")])
        self.assertIn("<@U12345678>: can you ship it?", self.prompts[0])
        self.assertIn("NO_REPLY", self.prompts[0])
        self.assertIn("Tether continuation", self.prompts[0])
        self.assertIn("sess-1", self.prompts[0])
        self.assertIn("never infer a host or disk fault", self.prompts[0])
        self.assertEqual(self.violations(), [])
        # Nothing left to do, and nothing runs twice.
        self.assertEqual(slice_.run_once(), 0)
        self.assertEqual(len(self.sent), 1)

    def test_no_reply_is_silent_and_terminal(self):
        slice_ = self.make_slice("printf 'NO_REPLY\\n'")
        slice_.bind(
            source_kind="claude_session", session_id="sess-2", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="100.1",
            owner_user_id="U12345678",
        )
        slice_.claim(self.fields(FakeEvent("fyi")), "fyi")
        self.assertEqual(slice_.run_once(), 1)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.violations(), [])
        self.assertEqual(slice_.run_once(), 0)

    def test_crashed_harness_cancels_turns_and_posts_nothing(self):
        slice_ = self.make_slice("exit 7")
        slice_.bind(
            source_kind="claude_session", session_id="sess-3", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="100.1",
            owner_user_id="U12345678",
        )
        slice_.claim(self.fields(FakeEvent("go")), "go")
        self.assertEqual(slice_.run_once(), 1)
        self.assertEqual(self.sent, [])
        connection = sqlite3.connect(self.db)
        try:
            state = connection.execute("SELECT state FROM queued_turns").fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(state, "cancelled")
        self.assertEqual(self.violations(), [])

    def test_unbound_thread_is_not_claimed(self):
        slice_ = self.make_slice("printf x")
        self.assertIsNone(slice_.claim(self.fields(FakeEvent("hola", thread="999.9")), "hola"))
        self.assertEqual(slice_.run_once(), 0)

    def test_duplicate_delivery_admits_once(self):
        slice_ = self.make_slice("printf 'ok'")
        slice_.bind(
            source_kind="claude_session", session_id="sess-4", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="100.1",
            owner_user_id="U12345678",
        )
        event = FakeEvent("once")
        first = slice_.claim(self.fields(event), "once")
        second = slice_.claim(self.fields(event), "once")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(slice_.run_once(), 1)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.violations(), [])

    def test_trusted_peer_bot_is_admitted_and_stranger_bot_denied(self):
        admission = importlib.import_module("plugin_next.admission")
        settings = admission.AdmissionSettings(
            workspace_id="T12345678",
            allowed_users=frozenset({"U12345678"}),
            trusted_bot_users=frozenset({"U0PEER0001"}),
        )
        common = dict(
            platform="slack", workspace="T12345678", channel="C1", thread="100.1",
            message_id="170.500", settings=settings, bound_threads={("C1", "100.1")},
        )
        peer = admission.evaluate(actor="U0PEER0001", actor_is_bot=True, **common)
        stranger = admission.evaluate(actor="U0STRANGER", actor_is_bot=True, **common)
        self.assertEqual((peer["verdict"], peer["reason"]), ("admit", "trusted_peer_on_bound_thread"))
        self.assertEqual((stranger["verdict"], stranger["reason"]), ("deny", "untrusted_bot"))

    def test_harness_env_drops_proxy_and_secret_variables(self):
        env = self.active.child_env({
            "HOME": "/h", "PATH": "/bin", "ANTHROPIC_API_KEY": "x",
            "ANTHROPIC_BASE_URL": "http://proxy", "SLACK_BOT_TOKEN": "xoxb",
            "OP_SERVICE_ACCOUNT_TOKEN": "ops", "LANG": "C.UTF-8",
        })
        self.assertEqual(env, {"HOME": "/h", "PATH": "/bin", "LANG": "C.UTF-8"})

    def test_harness_env_passthrough_normalises_proxy_base_url(self):
        env = self.active.child_env(
            {"HOME": "/h", "ANTHROPIC_BASE_URL": "http://127.0.0.1:9413/v1",
             "ANTHROPIC_API_KEY": "k", "SLACK_BOT_TOKEN": "xoxb"},
            passthrough=("ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY"),
        )
        self.assertEqual(env, {"HOME": "/h", "ANTHROPIC_BASE_URL": "http://127.0.0.1:9413",
                               "ANTHROPIC_API_KEY": "k"})
        settings = self.active.load_active_settings(pathlib.Path("/nonexistent"))
        self.assertEqual(settings.harness_env, ())

    def test_self_messages_and_peer_status_notices_are_not_turns(self):
        admission = importlib.import_module("plugin_next.admission")
        settings = admission.AdmissionSettings(
            workspace_id="T12345678", allowed_users=frozenset({"U12345678"}),
            trusted_bot_users=frozenset({"U0PEER0001"}), self_user_id="UME",
        )
        common = dict(platform="slack", workspace="T12345678", channel="C1", thread="100.1",
                      message_id="170.500", settings=settings, bound_threads={("C1", "100.1")})
        mine = admission.evaluate(actor="UME", actor_is_bot=True, text="hola", **common)
        self.assertEqual((mine["verdict"], mine["reason"]), ("not_ours", "self_message"))
        for notice in (":hourglass_flowing_sand: Working — 3 min — waiting", ":zap: Interrupting current task.",
                       ":warning: Gateway shutting down — Your current task will be interrupted."):
            d = admission.evaluate(actor="U0PEER0001", actor_is_bot=True, text=notice, **common)
            self.assertEqual((d["verdict"], d["reason"]), ("not_ours", "peer_status_notice"), notice)
        real = admission.evaluate(actor="U0PEER0001", actor_is_bot=True, text="what was the bug?", **common)
        self.assertEqual(real["verdict"], "admit")

    def test_codex_and_claude_commands(self):
        settings = self.active.ActiveSettings(
            claude_binary="/bin/echo", codex_binary="/bin/echo",
            claude_resume_args=("--x",), codex_resume_args=("--y",),
        )
        claude = self.active.harness_command(
            {"source_kind": "claude_session", "source": {"session_id": "s"}}, settings, "p"
        )
        codex = self.active.harness_command(
            {"source_kind": "codex_session", "source": {"session_id": "s"}}, settings, "p"
        )
        self.assertEqual(claude, ["/bin/echo", "--print", "--resume", "s", "--output-format", "text", "--x", "p"])
        self.assertEqual(codex, ["/bin/echo", "exec", "resume", "--y", "s", "p"])

    def test_register_wires_active_mode_and_skips_claimed_events(self):
        home = pathlib.Path(self.temp.name) / "hermes"
        config_home = pathlib.Path(self.temp.name) / "config"
        (config_home / "tether").mkdir(parents=True)
        (config_home / "tether" / "config.toml").write_text(
            'active = true\nteam_id = "T12345678"\nallowed_users = ["U12345678"]\n'
            'claude_binary = "/bin/sh"\nclaude_resume_args = ["-c", "printf hola"]\n',
            encoding="utf-8",
        )
        os.environ["HERMES_HOME"] = str(home)
        os.environ["XDG_CONFIG_HOME"] = str(config_home)
        try:
            dispatched: list[tuple[str, dict]] = []

            class Ctx:
                def __init__(self):
                    self.hooks = {}
                    self.unload = []
                    self.cli = {}

                def register_hook(self, name, callback):
                    self.hooks[name] = callback

                def register_cli_command(self, name, help, setup_fn, handler_fn=None, description=""):
                    self.cli[name] = (setup_fn, handler_fn)

                def get_config(self, key, default=None):
                    return default

                def on_unload(self, callback):
                    self.unload.append(callback)

                def dispatch_tool(self, name, args):
                    dispatched.append((name, args))
                    return json.dumps({"success": True})

            ctx = Ctx()
            self.plugin_next.register(ctx)
            hook = ctx.hooks["pre_gateway_dispatch"]
            # Unbound thread: observed, not claimed.
            self.assertIsNone(hook(event=FakeEvent("hi")))
            # Bind through the CLI surface, then the same event is claimed.
            setup, handler = ctx.cli["tether"]
            args = types.SimpleNamespace(
                subcommand="bind", channel="C1", thread_ts="100.1", owner="U12345678",
                claude_session_id="sess-cli", codex_session_id=None, cwd=self.temp.name,
            )
            self.assertEqual(handler(args), 0)
            decision = hook(event=FakeEvent("now bound"))
            self.assertEqual(decision, {"action": "skip", "reason": "tether-claimed"})
            for callback in ctx.unload:
                callback()
        finally:
            os.environ.pop("HERMES_HOME", None)
            os.environ.pop("XDG_CONFIG_HOME", None)


if __name__ == "__main__":
    unittest.main()
