"""The broker is the one door for the CLI. Drive it over a real Unix socket
against a slice with a fake Slack and a fake harness."""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest

from tests import fakes

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class FakeSlack:
    configured = True

    def __init__(self):
        self.posts: list[tuple[str, str, str | None]] = []
        self.n = 0
        self.missing_threads: set[tuple[str, str]] = set()
        self.reactions: list[tuple[str, str, str, str]] = []

    def identity(self):
        return {"team_id": "T12345678", "user_id": "UBOT", "user": "bot"}

    def post(self, channel_id, text, *, thread_ts=None):
        self.n += 1
        self.posts.append((channel_id, text, thread_ts))
        return f"1700000000.{self.n:06d}"

    def thread_replies(self, channel_id, thread_ts, *, limit=50):
        if (channel_id, thread_ts) in self.missing_threads:
            return []
        return [{"ts": thread_ts, "text": "root"}]

    def history(self, channel_id, *, limit=20):
        return [{"ts": "1.0", "text": "hi", "user": "U1"}]

    def membership(self, channel_id):
        return "member"

    def react(self, channel_id, message_ts, emoji):
        self.reactions.append(("add", channel_id, message_ts, emoji))
        return True

    def unreact(self, channel_id, message_ts, emoji):
        self.reactions.append(("remove", channel_id, message_ts, emoji))
        return True


class BrokerTest(unittest.TestCase):
    def setUp(self):
        previous = list(sys.path)
        sys.path.insert(0, str(RUNTIME))
        try:
            for name in ("plugin_next", "plugin_next.active", "plugin_next.broker",
                         "plugin_next.store", "plugin_next.session_driver"):
                sys.modules.pop(name, None)
            from plugin_next import active, broker, session_driver, store
        finally:
            sys.path[:] = previous
        self.temp = tempfile.TemporaryDirectory(prefix="tether-broker-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        self.db = base / "tether.db"
        runtime = store.Store(self.db)
        self.runtime = runtime
        fake = fakes.write_fake_claude(base)
        os.environ["FAKE_REPLY"] = "printf 'listo'"
        settings = active.ActiveSettings(enabled=True, native_timeout_seconds=30, launcher="direct",
                                         claude_binary=str(fake), extra={"default_channel": "C1"})
        self.driver = session_driver.SessionDriver(
            runtime, base / "session", settings, launch_plan=fakes.direct_launch,
            child_env=fakes.child_env, idle_seconds=60,
        )
        descriptor = fakes.Descriptor()
        self.slack = FakeSlack()
        self.sent = []
        self.slice = active.ActiveSlice(
            runtime=runtime, driver=self.driver, settings=settings,
            egress=lambda c, t, x: self.sent.append((c, t, x)),
            descriptor=descriptor, slack=self.slack,
        )
        self.broker_module = broker
        self.server = broker.BrokerServer(base / "b.sock", self.slice.handle)
        self.server.start()
        self.socket = base / "b.sock"

    def tearDown(self):
        self.server.stop()
        self.driver.shutdown()
        self.runtime.close()
        os.environ.pop("FAKE_REPLY", None)
        self.temp.cleanup()

    def call(self, **request):
        return self.broker_module.call(self.socket, request)

    def source(self, sid="sess-1"):
        return {"source_kind": "claude_session", "source": {"session_id": sid, "cwd": self.temp.name}}

    def test_status_matches_the_doctor_contract(self):
        status = self.call(op="status")
        self.assertTrue(status["ok"])
        self.assertEqual(status["implementation"], "tether")
        self.assertEqual(status["protocol_version"], 6)
        self.assertTrue(status["peer_uid_enforced"] and status["root_refused"])
        self.assertTrue(status["owner_configured"])
        self.assertEqual(status["allowed_user_count"], 1)
        self.assertIs(status["slack_transport_connected"], True)
        self.assertEqual(status["default_channel_membership"], "member")

    def test_notify_posts_once_and_binds_the_thread(self):
        first = self.call(op="notify", text="hola equipo", idempotency_key="k1", **self.source())
        again = self.call(op="notify", text="hola equipo", idempotency_key="k1", **self.source())
        self.assertTrue(first["ok"])
        self.assertEqual(first["channel_id"], "C1")
        self.assertEqual(first["thread_ts"], "1700000000.000001")
        self.assertEqual(again["thread_ts"], first["thread_ts"])
        self.assertEqual(again["status"], "duplicate")
        self.assertEqual(len(self.slack.posts), 1)
        bound = self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1", thread_ts=first["thread_ts"])
        self.assertIsNotNone(bound)
        # A reply in that thread now drives the bound session and answers.
        fields = {"workspace": "T12345678", "channel": "C1", "thread": first["thread_ts"],
                  "actor": "U12345678", "message_id": "1700000000.000002"}
        self.assertIsNotNone(self.slice.claim(fields, "status?"))
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent, [("C1", first["thread_ts"], "listo")])

    def test_attach_rebind_close_and_thread_ops(self):
        attached = self.call(op="attach", channel_id="C1", thread_ts="100.1", idempotency_key="a1", **self.source())
        self.assertTrue(attached["ok"])
        rebound = self.call(op="rebind", channel_id="C1", thread_ts="100.1", **self.source("sess-2"))
        self.assertTrue(rebound["ok"])
        self.assertNotEqual(rebound["bridge_id"], attached["bridge_id"])
        posted = self.call(op="thread_reply", channel_id="C1", thread_ts="100.1", text="ya", idempotency_key="p1")
        self.assertEqual(posted["status"], "posted")
        self.assertEqual(self.slack.posts[-1], ("C1", "ya", "100.1"))
        quiet = self.call(op="thread_reply", channel_id="C1", thread_ts="100.1", text="NO_REPLY", idempotency_key="p2")
        self.assertEqual(quiet["status"], "no_reply")
        reply = self.call(op="reply", bridge_id=rebound["bridge_id"], reply_key="x", text="por bridge")
        self.assertEqual(reply["thread_ts"], "100.1")
        self.assertEqual(self.call(op="thread_history", channel_id="C1", thread_ts="100.1")["messages"][0]["text"], "root")
        self.assertEqual(self.call(op="history")["messages"][0]["user"], "U1")
        # The broker post above admitted a turn; a binding with ready work refuses
        # to close until it is drained, which is the schema protecting the queue.
        busy = self.call(op="close", channel_id="C1", thread_ts="100.1")
        self.assertEqual(busy["code"], "binding_has_ready_turns")
        self.assertEqual(self.slice.run_once(), 1)
        closed = self.call(op="close", channel_id="C1", thread_ts="100.1")
        self.assertEqual(closed["status"], "closed")
        self.assertIsNone(self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1", thread_ts="100.1"))

    def test_python_cli_client_speaks_the_same_protocol(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "tether_notify_under_test", ROOT / "skills" / "tether" / "scripts" / "tether_notify.py"
        )
        module = importlib.util.module_from_spec(spec)
        os.environ["TETHER_BROKER_SOCKET"] = str(self.socket)
        try:
            spec.loader.exec_module(module)
            status = module.broker_call({"op": "status"})
            self.assertEqual(status["implementation"], "tether")
            ok, checks = module.doctor()
            self.assertTrue(ok, checks)
            self.assertTrue(any(line.startswith("ok broker protocol=6") for line in checks))
            with self.assertRaises(module.BrokerError) as caught:
                module.broker_call({"op": "herdr_context"})
            self.assertEqual(caught.exception.code, "unsupported_op")
            identity = module.working_directory_identity(self.temp.name)
            self.assertEqual(identity["cwd_realpath"], os.path.realpath(self.temp.name))
        finally:
            os.environ.pop("TETHER_BROKER_SOCKET", None)

    def test_spawn_creates_a_session_binds_it_and_the_thread_drives_it(self):
        created = []

        def fake_create(source_kind, cwd, task):
            created.append((source_kind, str(cwd), task))
            return "sess-spawned"

        self.slice._create_session = fake_create
        spawned = self.call(op="spawn", harness="claude", task="fix the flaky test", cwd=self.temp.name)
        self.assertTrue(spawned["ok"], spawned)
        self.assertEqual(spawned["session_id"], "sess-spawned")
        self.assertEqual((created[0][0], created[0][1]), ("claude_session", self.temp.name))
        self.assertIn("Tether bootstrap", created[0][2], "the seed only creates the session")
        self.assertNotIn("Asked by", created[0][2], "no actor given: no addressee line")
        self.assertEqual(spawned["task_turn"], f"spawn:{spawned['bridge_id']}", "the task is the first turn")
        created.clear()
        again = self.slice.handle({"op": "spawn", "task": "again", "channel_id": "C1", "thread_ts": "100.77", "cwd": self.temp.name, "actor": "UPEER1"})
        self.assertIn("Asked by <@UPEER1> in Slack", created[0][2])
        self.assertEqual(again["task_turn"], f"spawn:{again['bridge_id']}")
        # the task turn runs like any bound turn: one drive, reply in the thread
        # the task turn runs like any bound turn; both spawns share one fake session, so one endpoint per pass
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual([m[2] for m in self.sent], ["listo", "listo"])
        self.sent.clear()
        # No thread given: a root was posted and the new thread is bound.
        self.assertEqual(self.slack.posts[-1][0], "C1")
        self.assertIn("On it: fix the flaky test", self.slack.posts[-1][1])
        self.assertEqual(spawned["thread_ts"], "1700000000.000001")
        bound = self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1", thread_ts=spawned["thread_ts"])
        self.assertIsNotNone(bound)
        # A follow-up in that thread reaches the spawned session with presence.
        self.slack.reactions.clear()
        fields = {"workspace": "T12345678", "channel": "C1", "thread": spawned["thread_ts"],
                  "actor": "U12345678", "message_id": "1700000000.000009"}
        self.assertIsNotNone(self.slice.claim(fields, "status?"))
        self.assertEqual(self.slack.reactions, [("add", "C1", "1700000000.000009", "eyes")])
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent[-1], ("C1", spawned["thread_ts"], "listo"))
        self.assertIn(("remove", "C1", "1700000000.000009", "eyes"), self.slack.reactions)
        self.assertIn(("add", "C1", "1700000000.000009", "white_check_mark"), self.slack.reactions)

    def test_spawn_into_an_existing_thread_and_refusals(self):
        self.slice._create_session = lambda k, c, t: "sess-2"
        spawned = self.call(op="spawn", harness="codex", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.7")
        self.assertEqual((spawned["ok"], spawned["thread_ts"], spawned["harness"]), (True, "100.7", "codex"))
        self.assertEqual(self.call(op="spawn", harness="vim", task="t", cwd=self.temp.name)["code"], "harness_unsupported")
        self.assertEqual(self.call(op="spawn", harness="claude", task="", cwd=self.temp.name)["code"], "task_required")
        self.assertEqual(self.call(op="spawn", harness="claude", task="t", cwd="/nonexistent-dir-x")["code"], "cwd_missing")
        # 2026-09-16: a thread id without its channel bound the session to the default channel and its
        # reports became stray roots in #agent-hub. Refuse it, and refuse a thread Slack does not have.
        self.assertEqual(self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, thread_ts="100.9")["code"],
                         "channel_required")
        self.slack.missing_threads.add(("C1", "100.8"))
        self.assertEqual(self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1",
                                   thread_ts="100.8")["code"], "thread_unknown")
        self.assertEqual(self.call(op="attach", channel_id="C1", thread_ts="100.8", idempotency_key="a-1",
                                   **self.source("sess-9"))["code"], "thread_unknown")
        self.assertEqual(len([p for p in self.slack.posts if p[2] == "100.8"]), 0, "nothing was posted to a thread that does not exist")

        def boom(k, c, t):
            raise RuntimeError("claude did not report a session id (exit 1)")

        self.slice._create_session = boom
        failed = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name)
        self.assertEqual(failed["code"], "spawn_failed")

    def herdr_client(self):
        from tests.fakes import write_fake_herdr
        fake = write_fake_herdr(pathlib.Path(self.temp.name))
        os.environ["FAKE_HERDR_STATE"] = str(pathlib.Path(self.temp.name) / "herdr-state.json")
        os.environ["FAKE_HERDR_LOG"] = str(pathlib.Path(self.temp.name) / "herdr-calls.log")
        for key in ("FAKE_HERDR_STATE", "FAKE_HERDR_LOG", "FAKE_HERDR_W1_CWD", "FAKE_HERDR_TRUST", "FAKE_HERDR_SCREEN"):
            self.addCleanup(os.environ.pop, key, None)
        herdr_module = sys.modules[type(self.slice).__module__.rsplit(".", 1)[0] + ".herdr"]
        return herdr_module.Herdr(binary=str(fake), session="pilot")

    def herdr_calls(self):
        return (pathlib.Path(self.temp.name) / "herdr-calls.log").read_text().splitlines()

    def test_spawn_places_the_session_in_a_herdr_tab(self):
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client

        def never(*a, **k):
            raise AssertionError("a Herdr spawn starts the harness in the pane, never headless")

        self.slice._create_session = never
        task = "<@U12345678> start a herdr tab called MCP in the grep.ai space debugging the expert creation MCP"
        spawned = self.call(op="spawn", harness="claude", task=task, cwd=self.temp.name, channel_id="C1",
                            thread_ts="100.7", herdr_workspace="grep.ai", tab="MCP", actor="U12345678")
        self.assertTrue(spawned["ok"], spawned)
        self.assertEqual(spawned["session_id"], "claude-sess-2")
        self.assertEqual(spawned["herdr"], {"session": "pilot", "workspace_id": "w1", "tab_id": "w1:t2",
                                            "pane_id": "w1:p2", "agent": "mcp", "kind": "claude"})
        calls = self.herdr_calls()
        self.assertIn(f"tab create --workspace w1 --cwd {self.temp.name} --label MCP --no-focus", calls)
        self.assertTrue(any(c.startswith("agent start mcp --kind claude --pane w1:p2") for c in calls))
        self.assertIn("pane report-metadata w1:p2 --source tether --token slack=C1/100.7", calls)
        binding = self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1", thread_ts="100.7")
        source = self.slice.runtime.endpoint(binding["endpoint_id"])["source"]
        self.assertEqual(source["herdr"]["pane_id"], "w1:p2")
        self.assertTrue(source["spawned"])
        # a second tab with the same label gets a distinct agent name
        again = self.call(op="spawn", harness="claude", task="more", cwd=self.temp.name, channel_id="C1",
                          thread_ts="100.8", herdr_workspace="grep.ai", tab="MCP")
        self.assertEqual(again["herdr"]["agent"], "mcp-2")

    def test_spawn_picks_the_repo_workspace_or_makes_one_and_derives_the_tab_label(self):
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        os.environ["FAKE_HERDR_W1_CWD"] = self.temp.name
        spawned = self.call(op="spawn", harness="claude", cwd=self.temp.name, channel_id="C1", thread_ts="100.7",
                            task="Fix the flaky test in <https://github.com/x/y/pull/1>: it times out on CI\nmore context")
        self.assertEqual(spawned["herdr"]["workspace_id"], "w1", "the workspace whose checkout holds cwd")
        self.assertIn("tab create --workspace w1 --cwd " + self.temp.name + " --label Fix the flaky test in: it times out on CI --no-focus",
                      self.herdr_calls())
        inside = pathlib.Path(self.temp.name) / "sub"
        inside.mkdir()
        spawned = self.call(op="spawn", harness="claude", cwd=str(inside), channel_id="C1", thread_ts="100.8", task="t")
        self.assertEqual(spawned["herdr"]["workspace_id"], "w1", "a subdirectory of the checkout is that workspace too")
        with tempfile.TemporaryDirectory() as elsewhere:
            os.environ["CODEX_HOME"] = str(pathlib.Path(elsewhere) / "no-codex")
            self.addCleanup(os.environ.pop, "CODEX_HOME", None)
            spawned = self.call(op="spawn", harness="codex", cwd=elsewhere, channel_id="C1", thread_ts="100.9", task="t")
            self.assertEqual(spawned["code"], "session_unknown", "Codex reports no id until the hook is approved and no rollout exists here")
            self.assertIn(f"workspace create --cwd {elsewhere} --label {pathlib.Path(elsewhere).name} --no-focus", self.herdr_calls())

    def test_spawn_answers_the_folder_trust_dialog_only_under_managed_roots(self):
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        os.environ["FAKE_HERDR_TRUST"] = "1"
        os.environ["FAKE_HERDR_SCREEN"] = "Quick safety check: Is this a project you created or one you trust?\n > No, exit\n   Yes, I trust this folder"
        refused = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.7", tab="x")
        self.assertEqual(refused["code"], "agent_blocked", "a temp dir is not a managed worktree: the dialog is not answered")
        self.assertNotIn("agent send-keys x enter", self.herdr_calls())
        managed = pathlib.Path(self.temp.name) / "worktrees" / "gre-1"
        managed.mkdir(parents=True)
        self.slice.settings = type(self.slice.settings)(**{**self.slice.settings.__dict__,
                                                          "extra": {**self.slice.settings.extra, "herdr_trusted_roots": [str(managed.parent)]}})
        spawned = self.call(op="spawn", harness="claude", task="t", cwd=str(managed), channel_id="C1", thread_ts="100.8", tab="y")
        self.assertTrue(spawned["ok"], spawned)
        self.assertIn("agent send-keys y down", self.herdr_calls())
        self.assertIn("agent send-keys y enter", self.herdr_calls())
        self.assertEqual(spawned["session_id"], "claude-sess-4")

    def test_spawn_accepts_the_bypass_warning_only_when_the_flag_is_configured(self):
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        os.environ["FAKE_HERDR_BYPASS"] = "1"
        self.addCleanup(os.environ.pop, "FAKE_HERDR_BYPASS", None)
        managed = pathlib.Path(self.temp.name) / "worktrees" / "gre-2"
        managed.mkdir(parents=True)
        base = self.slice.settings
        self.slice.settings = type(base)(**{**base.__dict__, "claude_resume_args": ("--model", "x"),
                                            "extra": {**base.extra, "herdr_trusted_roots": [str(managed.parent)]}})
        refused = self.call(op="spawn", harness="claude", task="t", cwd=str(managed), channel_id="C1", thread_ts="100.7", tab="a")
        self.assertEqual(refused["code"], "agent_blocked", "no bypass flag configured: the warning is not ours to accept")
        self.slice.settings = type(base)(**{**base.__dict__, "claude_resume_args": ("--dangerously-skip-permissions",),
                                            "extra": {**base.extra, "herdr_trusted_roots": [str(managed.parent)]}})
        os.environ["FAKE_HERDR_TRUST"] = "1"
        os.environ["FAKE_HERDR_SCREEN"] = "Is this a project you created or one you trust?\n > No, exit\n   Yes, I trust this folder"
        spawned = self.call(op="spawn", harness="claude", task="t", cwd=str(managed), channel_id="C1", thread_ts="100.8", tab="b")
        self.assertTrue(spawned["ok"], spawned)
        self.assertEqual(self.herdr_calls().count("agent send-keys b enter"), 2, "trust, then the bypass consent")
        self.assertEqual(spawned["session_id"], "claude-sess-4")

    def test_herdr_required_refuses_a_headless_fallback_and_defaults_the_space(self):
        base = self.slice.settings
        self.slice.settings = type(base)(**{**base.__dict__, "extra": {**base.extra, "herdr_required": True, "herdr_workspace": "claudio"}})
        self.slice.herdr_factory = lambda: None
        self.slice._create_session = lambda k, c, t: "sess-plain"
        refused = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.7")
        self.assertEqual(refused["code"], "herdr_unavailable")
        forced = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.7", herdr=False)
        self.assertEqual(forced["session_id"], "sess-plain", "--no-herdr is an explicit operator choice and still works")
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        spawned = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.8")
        self.assertTrue(spawned["ok"], spawned)
        self.assertIn(f"workspace create --cwd {self.temp.name} --label claudio --no-focus", self.herdr_calls(),
                      "the configured space is created when missing and used by default")
        self.assertEqual(spawned["herdr"]["workspace_id"], "w2")
        named = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.9", herdr_workspace="grep.ai")
        self.assertEqual(named["herdr"]["workspace_id"], "w1", "a space the person names still wins")

    def test_spawn_without_herdr_is_unchanged(self):
        self.slice.herdr_factory = lambda: None
        self.slice._create_session = lambda k, c, t: "sess-plain"
        spawned = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.7")
        self.assertEqual((spawned["session_id"], spawned["herdr"]), ("sess-plain", None))
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        spawned = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name, channel_id="C1", thread_ts="100.8", herdr=False)
        self.assertEqual(spawned["session_id"], "sess-plain", "--no-herdr keeps the session out of Herdr")

    def test_create_session_parses_both_harnesses(self):
        import subprocess as sp
        active = sys.modules["plugin_next.active"]
        settings = active.ActiveSettings(claude_binary="/bin/echo", codex_binary="/bin/echo", launcher="direct")

        def claude_runner(cmd, **kw):
            self.assertEqual(cmd[:4], ["/bin/echo", "-p", "--output-format", "json"])
            return sp.CompletedProcess(cmd, 0, stdout='{"type":"result","session_id":"c-123","result":"READY"}\n', stderr="")

        def codex_runner(cmd, **kw):
            self.assertEqual(cmd[:3], ["/bin/echo", "exec", "--json"])
            return sp.CompletedProcess(cmd, 0, stdout='{"type":"thread.started","thread_id":"x-9"}\n{"type":"turn.started"}\n', stderr="")

        self.assertEqual(active.create_session("claude_session", pathlib.Path(self.temp.name), "t", settings, runner=claude_runner), "c-123")
        self.assertEqual(active.create_session("codex_session", pathlib.Path(self.temp.name), "t", settings, runner=codex_runner), "x-9")
        with self.assertRaises(RuntimeError):
            active.create_session("claude_session", pathlib.Path(self.temp.name), "t", settings,
                                  runner=lambda cmd, **kw: sp.CompletedProcess(cmd, 1, stdout="", stderr="boom"))

    def test_claude_session_id_accepts_array_and_line_output(self):
        active = sys.modules["plugin_next.active"]
        array = '[{"type":"system","session_id":"s-array"},{"type":"result","session_id":"s-array","result":"READY"}]\n'
        lines = '{"type":"system","session_id":"s-line"}\n{"type":"result","session_id":"s-line"}\n'
        self.assertEqual(active.claude_session_id(array), "s-array")
        self.assertEqual(active.claude_session_id(lines), "s-line")
        self.assertEqual(active.claude_session_id("not json"), "")
        self.assertEqual(active.claude_session_id(""), "")

    def test_create_session_uses_the_same_launcher_as_bound_turns(self):
        import subprocess as sp
        from unittest import mock
        active = sys.modules["plugin_next.active"]
        settings = active.ActiveSettings(claude_binary="/bin/echo", launcher="systemd-user")
        seen = {}

        def runner(cmd, **kw):
            seen["cmd"], seen["env"] = cmd, kw["env"]
            return sp.CompletedProcess(cmd, 0, stdout='{"session_id":"c-7"}\n', stderr="")

        bus = pathlib.Path(self.temp.name) / "bus"
        bus.write_text("")
        with mock.patch.object(active, "user_bus_path", return_value=bus), \
             mock.patch.object(active.shutil, "which", side_effect=lambda name: {"systemd-run": "/usr/bin/systemd-run"}.get(name, name)):
            self.assertEqual(active.create_session("claude_session", pathlib.Path(self.temp.name), "t", settings, runner=runner), "c-7")
        self.assertEqual(seen["cmd"][:2], ["/usr/bin/systemd-run", "--user"])
        self.assertIn("/bin/echo", seen["cmd"])
        self.assertEqual(seen["env"]["DBUS_SESSION_BUS_ADDRESS"], f"unix:path={bus}")

    def test_failed_turn_marks_the_message_with_a_warning(self):
        os.environ["FAKE_REPLY"] = "exit 3"
        self.call(op="attach", channel_id="C1", thread_ts="100.3", idempotency_key="f1", **self.source("sess-f"))
        self.slack.reactions.clear()
        fields = {"workspace": "T12345678", "channel": "C1", "thread": "100.3", "actor": "U12345678", "message_id": "1700000000.000031"}
        self.slice.claim(fields, "do it")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn(("add", "C1", "1700000000.000031", "warning"), self.slack.reactions)
        # ...and says so once, instead of leaving the thread staring at an emoji.
        self.assertEqual(len(self.sent), 1)
        self.assertIn("I could not take this turn (harness_exited", self.sent[0][2])

    def test_post_and_reply_attach_files_natively(self):
        uploads = []
        self.slack.upload = lambda channel, path, thread_ts=None, initial_comment=None, title=None: (
            uploads.append((channel, path, thread_ts, initial_comment)) or {"file_id": "F1", "ts": "1700000000.000099"})
        clip = pathlib.Path(self.temp.name) / "demo.mp4"
        clip.write_bytes(b"\x00\x00\x00\x18ftypmp42")
        posted = self.call(op="thread_reply", channel_id="C1", thread_ts="100.7", text="the demo", file=str(clip))
        self.assertTrue(posted["ok"], posted)
        self.assertEqual(posted["message_ts"], "1700000000.000099")
        self.assertEqual(uploads[-1], ("C1", str(clip), "100.7", "the demo"))
        alone = self.call(op="thread_reply", channel_id="C1", thread_ts="100.7", file=str(clip))
        self.assertTrue(alone["ok"], "a file can stand alone")
        self.assertEqual(uploads[-1][3], None)
        missing = self.call(op="thread_reply", channel_id="C1", thread_ts="100.7", text="x", file=str(clip) + ".nope")
        self.assertEqual(missing["code"], "file_missing")
        relative = self.call(op="thread_reply", channel_id="C1", thread_ts="100.7", text="x", file="demo.mp4")
        self.assertEqual(relative["code"], "file_path_relative")
        # notify with a file: the share message becomes the bound root
        first = self.call(op="notify", text="watch this", file=str(clip), idempotency_key="file-1", **self.source("sess-file"))
        self.assertTrue(first["ok"], first)
        self.assertEqual(first["thread_ts"], "1700000000.000099")

    def test_post_into_a_bound_thread_wakes_the_session(self):
        self.call(op="attach", channel_id="C1", thread_ts="100.5", idempotency_key="w1", **self.source("sess-w"))
        posted = self.call(op="thread_reply", channel_id="C1", thread_ts="100.5", text="fix the env and tell Manuel", idempotency_key="w2")
        self.assertTrue(posted["turn_admitted"], posted)
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent[-1], ("C1", "100.5", "listo"))
        unbound = self.call(op="thread_reply", channel_id="C1", thread_ts="999.9", text="hello", idempotency_key="w3")
        self.assertFalse(unbound["turn_admitted"])

    def test_refusals_are_explicit(self):
        bad = self.call(op="herdr_context")
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["code"], "unsupported_op")
        missing = self.call(op="notify", text="x", idempotency_key="k")
        self.assertEqual(missing["code"], "source_unsupported")
        self.assertEqual(self.call(op="unresolved")["operations"], [])
        self.assertEqual(self.call(op="identity")["user_id"], "UBOT")


if __name__ == "__main__":
    unittest.main()


class CodexHandoffTest(BrokerTest):
    def bind_codex(self, thread, session_id, spawned=False):
        return self.slice.bind(source_kind="codex_session", session_id=session_id, cwd=self.temp.name,
                               team_id="T12345678", channel_id="C1", thread_ts=thread, owner_user_id="U12345678",
                               spawned=spawned)

    def fields(self, thread, ts):
        return {"workspace": "T12345678", "channel": "C1", "thread": thread, "actor": "U12345678", "message_id": ts}

    def test_codex_notify_binds_and_a_drivable_thread_is_claimed(self):
        from unittest import mock
        codex = {"source_kind": "codex_session", "source": {"session_id": "01a08eb2-thread", "cwd": self.temp.name}}
        first = self.call(op="notify", text="QA ready", idempotency_key="qa-1", **codex)
        self.assertTrue(first["ok"])
        self.assertEqual(first["status"], "posted")
        self.assertIsNotNone(self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1",
                                                                     thread_ts=first["thread_ts"]))
        for state in ("free", "daemon", "ours"):
            with mock.patch.object(sys.modules[type(self.slice).__module__], "codex_writer_holder", return_value=(state, 7)):
                claimed = self.slice.claim(self.fields(first["thread_ts"], f"1700000000.0000{len(state)}"), "status?")
            self.assertIsNotNone(claimed, f"a {state} writer is drivable")
        self.assertIsNone(self.slice.runtime.pending_origin("C1", first["thread_ts"]))

    def test_terminal_held_codex_thread_hands_off_at_claim(self):
        from unittest import mock
        held = self.bind_codex("500.1", "01a0-held")
        claude = self.slice.bind(source_kind="claude_session", session_id="c-1", cwd=self.temp.name,
                                 team_id="T12345678", channel_id="C1", thread_ts="500.3", owner_user_id="U12345678")
        with mock.patch.object(sys.modules[type(self.slice).__module__], "codex_writer_holder", return_value=("held", 4242)) as holder:
            self.assertIsNone(self.slice.claim(self.fields("500.1", "1700000000.000009"), "still there?"))
            self.assertIsNotNone(self.slice.claim(self.fields("500.3", "1700000000.000010"), "claude is fine"))
        holder.assert_called_once_with("01a0-held", set())
        self.assertEqual(self.slice.runtime.binding_thread(held["binding_id"])["state"], "closed")
        self.assertEqual(self.slice.runtime.binding_thread(claude["binding_id"])["state"], "active")
        origin = self.slice.runtime.pending_origin("C1", "500.1")
        self.assertEqual((origin["source_kind"], origin["session_id"], origin["cwd"]),
                         ("codex_session", "01a0-held", self.temp.name))
        # The note tells the gateway's agent where the work lives, once.
        from runtime.plugin_next.active import origin_note
        note = origin_note(origin)
        self.assertIn("01a0-held", note)
        self.assertIn(f"Work in {self.temp.name}", note)
        self.slice.runtime.mark_origin_delivered("C1", "500.1")
        self.assertIsNone(self.slice.runtime.pending_origin("C1", "500.1"))
        # Nothing was queued against the terminal-held thread; only the Claude turn runs.
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.slice.run_once(), 0)

    def test_codex_writer_holder_reads_the_flock(self):
        import subprocess
        import sys
        import time
        from runtime.plugin_next.active import codex_writer_holder
        home = pathlib.Path(self.temp.name) / "codex-home"
        locks = home / "thread-writer-locks"
        locks.mkdir(parents=True)
        os.environ["CODEX_HOME"] = str(home)
        try:
            self.assertEqual(codex_writer_holder("nolock"), ("free", None))
            (locks / "free.lock").write_text("")
            self.assertEqual(codex_writer_holder("free"), ("free", None))
            (locks / "held.lock").write_text("")
            holder = subprocess.Popen([sys.executable, "-c",
                                       "import fcntl,os,sys,time; fd=os.open(sys.argv[1], os.O_RDWR); "
                                       "fcntl.flock(fd, fcntl.LOCK_EX); print('locked', flush=True); time.sleep(30)",
                                       str(locks / "held.lock")], stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "locked")
                time.sleep(0.1)
                self.assertEqual(codex_writer_holder("held"), ("held", holder.pid))
                self.assertEqual(codex_writer_holder("held", {holder.pid}), ("ours", holder.pid))
            finally:
                holder.kill()
                holder.wait()
        finally:
            os.environ.pop("CODEX_HOME", None)

    def test_find_transcript_locates_a_codex_rollout(self):
        from runtime.plugin_next.active import find_transcript
        home = pathlib.Path(self.temp.name) / "codex-home"
        day = home / "sessions" / "2026" / "09" / "11"
        day.mkdir(parents=True)
        (day / "rollout-2026-09-11T04-21-05-01a08eb2-9c73.jsonl").write_text("{}\n")
        os.environ["CODEX_HOME"] = str(home)
        try:
            self.assertTrue(find_transcript("codex_session", "01a08eb2-9c73").endswith("01a08eb2-9c73.jsonl"))
            self.assertIsNone(find_transcript("codex_session", "nope"))
            self.assertIsNone(find_transcript("claude_session", "01a08eb2-9c73"))
        finally:
            os.environ.pop("CODEX_HOME", None)


class HerdrAttachTest(BrokerTest):
    def test_attach_by_herdr_agent_name(self):
        client = self.herdr_client()
        self.slice.herdr_factory = lambda: client
        tab = client.tab_create(workspace_id="w1", cwd=self.temp.name, label="hvrt")
        client.agent_start("hvrt", kind="claude", pane_id=tab["pane_id"])
        attached = self.call(op="attach", herdr_agent="hvrt", channel_id="C1", thread_ts="100.7", idempotency_key="a-1")
        self.assertTrue(attached["ok"], attached)
        self.assertEqual((attached["harness"], attached["session_id"], attached["herdr"]["pane_id"]),
                         ("claude", "claude-sess-2", "w1:p2"))
        binding = self.slice.runtime.find_active_binding(team_id="T12345678", channel_id="C1", thread_ts="100.7")
        source = self.slice.runtime.endpoint(binding["endpoint_id"])["source"]
        self.assertEqual((source["session_id"], source["cwd"], source["herdr"]["agent"]), ("claude-sess-2", self.temp.name, "hvrt"))
        self.assertIn("pane report-metadata w1:p2 --source tether --token slack=C1/100.7", self.herdr_calls())
        # unknown name, and an agent kind Tether cannot bind
        self.assertEqual(self.call(op="attach", herdr_agent="ghost", channel_id="C1", thread_ts="100.8",
                                   idempotency_key="a-2")["code"], "herdr_agent_unknown")
        client.agent_start("gem", kind="gemini", pane_id=tab["pane_id"])
        self.assertEqual(self.call(op="attach", herdr_agent="gem", channel_id="C1", thread_ts="100.8",
                                   idempotency_key="a-3")["code"], "harness_unsupported")
        # closing the thread leaves the pane but drops its Slack token
        closed = self.call(op="close", channel_id="C1", thread_ts="100.7")
        self.assertTrue(closed["ok"], closed)
        self.assertIn("pane report-metadata w1:p2 --source tether --clear-token slack", self.herdr_calls())

    def test_attach_without_herdr_running_is_refused(self):
        self.slice.herdr_factory = lambda: None
        self.assertEqual(self.call(op="attach", herdr_agent="hvrt", channel_id="C1", thread_ts="100.7",
                                   idempotency_key="a-1")["code"], "herdr_unavailable")
