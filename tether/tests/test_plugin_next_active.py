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
from pathlib import Path
import types
import unittest
import unittest.mock

from tests import fakes

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("plugin_next", "plugin_next.active", "plugin_next.store", "plugin_next.session_driver"):
            sys.modules.pop(name, None)
        plugin_next = importlib.import_module("plugin_next")
        active = importlib.import_module("plugin_next.active")
        store = importlib.import_module("plugin_next.store")
        session_driver = importlib.import_module("plugin_next.session_driver")
        return store, session_driver, plugin_next, active
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
        self.store_module, self.session_driver_module, self.plugin_next, self.active = load()
        self.temp = tempfile.TemporaryDirectory(prefix="tether-active-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        self.db = base / "tether.db"
        self.runtime = self.store_module.Store(self.db)
        self.fake = fakes.write_fake_claude(base)
        self.prompts_file = base / "prompts.log"
        os.environ["FAKE_PROMPTS"] = str(self.prompts_file)
        self.descriptor = fakes.Descriptor()
        self.sent: list[tuple[str, str, str]] = []

    @property
    def prompts(self) -> list[str]:
        try:
            return [p for p in self.prompts_file.read_text().split("\n===\n") if p.strip()]
        except OSError:
            return []

    def tearDown(self):
        driver = getattr(self, "driver", None)
        if driver is not None:
            driver.shutdown()
        self.runtime.close()
        os.environ.pop("FAKE_PROMPTS", None)
        os.environ.pop("FAKE_REPLY", None)
        self.temp.cleanup()

    def make_slice(self, script: str):
        os.environ["FAKE_REPLY"] = script
        settings = self.active.ActiveSettings(
            enabled=True, native_timeout_seconds=30, launcher="direct", claude_binary=str(self.fake),
        )
        self.driver = self.session_driver_module.SessionDriver(
            self.runtime, pathlib.Path(self.temp.name) / "session", settings,
            launch_plan=fakes.direct_launch, child_env=fakes.child_env, idle_seconds=60,
        )
        return self.active.ActiveSlice(
            runtime=self.runtime,
            driver=self.driver,
            settings=settings,
            egress=lambda channel, thread, text: self.sent.append((channel, thread, text)),
            descriptor=self.descriptor,
        )

    def violations(self):
        return []

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
        # direct launcher = inside the gateway unit; the prompt must say so.
        self.assertIn("INSIDE the gateway's hardened systemd unit", self.prompts[0])
        self.assertIn("whatever you print is posted verbatim", self.prompts[0])
        self.assertEqual(self.violations(), [])
        # Nothing left to do, and nothing runs twice.
        self.assertEqual(slice_.run_once(), 0)
        self.assertEqual(len(self.sent), 1)

    def test_peer_chain_is_capped_after_two_peer_turns_without_a_human(self):
        slice_ = self.make_slice("printf 'ok'")
        slice_.bind(
            source_kind="claude_session", session_id="sess-peer", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="200.1",
            owner_user_id="U12345678",
        )
        peers = frozenset({"UPEER1", "UPEER2"})
        base = {"workspace": "T12345678", "channel": "C1", "thread": "200.1"}
        first = slice_.claim(dict(base, actor="UPEER1", message_id="200.2"), "ping", peer=True, peers=peers)
        second = slice_.claim(dict(base, actor="UPEER2", message_id="200.3"), "pong", peer=True, peers=peers)
        third = slice_.claim(dict(base, actor="UPEER1", message_id="200.4"), "ping again", peer=True, peers=peers)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertIsNone(third, "two peer turns with no human in between end the exchange")
        # A human speaking resets the chain; the next peer message is ours again.
        human = slice_.claim(dict(base, actor="U12345678", message_id="200.5"), "humans here", peer=False)
        self.assertIsNotNone(human)
        fourth = slice_.claim(dict(base, actor="UPEER2", message_id="200.6"), "reply to human", peer=True, peers=peers)
        self.assertIsNotNone(fourth)

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

    def test_crashed_harness_cancels_turns_and_says_why(self):
        slice_ = self.make_slice("printf 'You have hit your session limit - resets 6:40am (UTC)'; exit 7")
        slice_.bind(
            source_kind="claude_session", session_id="sess-3", cwd=self.temp.name,
            team_id="T12345678", channel_id="C1", thread_ts="100.1",
            owner_user_id="U12345678",
        )
        slice_.claim(self.fields(FakeEvent("go")), "go")
        self.assertEqual(slice_.run_once(), 1)
        # A failed turn is not silence: one line, the harness's own reason, no invention.
        self.assertEqual(len(self.sent), 1)
        channel, thread, text = self.sent[0]
        self.assertEqual((channel, thread), ("C1", "100.1"))
        self.assertIn("<@U12345678> I could not take this turn (harness_exited: You have hit your session limit", text)
        connection = sqlite3.connect(self.db)
        try:
            state = connection.execute("SELECT state FROM turns").fetchone()[0]
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

                def register_tool(self, name, toolset, schema, handler, **kwargs):
                    self.tools = getattr(self, "tools", {})
                    self.tools[name] = (toolset, schema, handler)

                def on_unload(self, callback):
                    self.unload.append(callback)

                def dispatch_tool(self, name, args):
                    dispatched.append((name, args))
                    return json.dumps({"success": True})

            # a Hermes state.db row maps the tool call's session to its Slack thread
            (home).mkdir(parents=True, exist_ok=True)
            state = sqlite3.connect(home / "state.db")
            state.executescript(
                "CREATE TABLE sessions(id TEXT, source TEXT, chat_id TEXT, thread_id TEXT, user_id TEXT);"
                "INSERT INTO sessions VALUES('hermes-s1','slack','C1','300.1','U12345678');"
            )
            state.commit()
            state.close()
            fake = pathlib.Path(self.temp.name) / "fake-claude-json"
            fake.write_text("#!/bin/sh\necho \"{\\\"type\\\":\\\"result\\\",\\\"session_id\\\":\\\"s-$$\\\"}\"\n", encoding="utf-8")
            fake.chmod(0o700)
            (config_home / "tether" / "config.toml").write_text(
                'active = true\nteam_id = "T12345678"\nallowed_users = ["U12345678"]\nlauncher = "direct"\n'
                f'claude_binary = "{fake}"\nclaude_resume_args = []\n',
                encoding="utf-8",
            )
            ctx = Ctx()
            self.plugin_next.register(ctx)
            hook = ctx.hooks["pre_gateway_dispatch"]
            # The model gets a first-class verb: tether_spawn binds a fresh session to the calling thread.
            toolset, schema, spawn = ctx.tools["tether_spawn"]
            self.assertEqual(toolset, "tether")
            self.assertEqual(schema["parameters"]["required"], ["task"])
            told = spawn({"task": "look into the flash_model migration", "cwd": self.temp.name}, session_id="hermes-s1")
            self.assertIn("Started a claude session s-", told)
            self.assertIn("thread 300.1 in C1", told)
            again = spawn({"task": "and again"}, session_id="hermes-s1")
            self.assertIn("already tethered", again)
            self.assertIn("Slack conversation only", spawn({"task": "x"}, session_id="unknown"))
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


class ReplyShapeTests(unittest.TestCase):
    def test_narration_before_the_addressed_reply_is_dropped(self):
        import importlib
        active = importlib.import_module("runtime.plugin_next.active")
        raw = ("Both suggesters changed identically.\n\nMiguel was right and my first answer was wrong. Reporting.\n\n"
               "<@U051FHN4SN8> You're right, that's ours: PR #8242 changed both suggesters.")
        self.assertEqual(active.reply_body(raw), "<@U051FHN4SN8> You're right, that's ours: PR #8242 changed both suggesters.")
        self.assertEqual(active.reply_body("<@U1> plain reply\nwith a second line"), "<@U1> plain reply\nwith a second line")
        self.assertEqual(active.reply_body("no mention at all, keep it"), "no mention at all, keep it")
        self.assertEqual(active.reply_body(""), "")


class LauncherTests(unittest.TestCase):
    def setUp(self):
        import importlib
        self.active = importlib.import_module("runtime.plugin_next.active")

    def test_direct_launcher_leaves_the_command_alone(self):
        settings = self.active.ActiveSettings(launcher="direct")
        argv, env, launcher = self.active.launch_plan(["claude", "-p", "x"], Path("/tmp"), {"HOME": "/h"}, settings)
        self.assertEqual((argv, env, launcher), (["claude", "-p", "x"], {"HOME": "/h"}, "direct"))

    def test_systemd_user_launcher_runs_in_the_operators_user_manager(self):
        with tempfile.TemporaryDirectory() as tmp:
            bus = Path(tmp) / "bus"
            bus.write_text("")
            settings = self.active.ActiveSettings(launcher="systemd-user", native_timeout_seconds=100)
            with unittest.mock.patch.object(self.active, "user_bus_path", return_value=bus), \
                 unittest.mock.patch.object(self.active.shutil, "which", return_value="/usr/bin/systemd-run"):
                argv, env, launcher = self.active.launch_plan(
                    ["claude", "--print", "hi"], Path("/work"), {"HOME": "/h", "PATH": "/bin"}, settings,
                )
        self.assertEqual(launcher, "systemd-user")
        self.assertEqual(argv[:6], ["/usr/bin/systemd-run", "--user", "--quiet", "--pipe", "--wait", "--collect"])
        self.assertIn("--property=WorkingDirectory=/work", argv)
        self.assertIn("--property=RuntimeMaxSec=130", argv)
        self.assertIn("--setenv=HOME=/h", argv)
        self.assertEqual(argv[-4:], ["--", "claude", "--print", "hi"])
        # The systemd-run client needs the bus; the harness gets only the allowlisted env.
        self.assertEqual(env["DBUS_SESSION_BUS_ADDRESS"], f"unix:path={bus}")
        self.assertEqual(env["XDG_RUNTIME_DIR"], str(bus.parent))
        self.assertNotIn("--setenv=XDG_RUNTIME_DIR", " ".join(argv))

    def test_systemd_user_falls_back_to_direct_and_the_prompt_tells_the_truth(self):
        settings = self.active.ActiveSettings(launcher="systemd-user")
        with unittest.mock.patch.object(self.active, "user_bus_path", return_value=Path("/nonexistent/bus")):
            self.assertEqual(self.active.resolve_launcher(settings), "direct")
        self.assertIn("operator's own systemd user session", self.active.runtime_truth("systemd-user"))
        self.assertIn("INSIDE the gateway's hardened systemd unit", self.active.runtime_truth("direct"))
        self.assertIn("not to the host", self.active.runtime_truth("direct"))


class JournalThreadingTests(unittest.TestCase):
    def test_record_and_summary_work_from_another_thread(self):
        import importlib
        import threading
        journal_mod = importlib.import_module("runtime.plugin_next.journal")
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            home.mkdir(mode=0o700)
            data = home / "tether"
            data.mkdir(mode=0o700)
            journal = journal_mod.DurableJournal(data)
            results: dict[str, object] = {}

            def worker():
                try:
                    results["record"] = journal.record("slack:T:C:1", {"verdict": "admit", "reason": "test"}, platform="slack")
                    results["summary"] = journal.summary()
                except Exception as exc:  # pragma: no cover - the assertion below reports it
                    results["error"] = repr(exc)

            t = threading.Thread(target=worker)
            t.start()
            t.join(10)
            self.assertNotIn("error", results, results.get("error"))
            self.assertTrue(results["record"])
            self.assertEqual(results["summary"]["events"], 1)
            journal.close()
