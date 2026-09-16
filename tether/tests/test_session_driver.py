"""Session driver against a fake harness that speaks stream-json, and the slice on top of it."""

from __future__ import annotations

import json
import os
import struct
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.plugin_next import active  # noqa: E402
from runtime.plugin_next.session_driver import SessionDriver  # noqa: E402
from runtime.plugin_next.store import Store  # noqa: E402

from tests.fakes import child_env, direct_launch as _direct_launch, write_fake_claude, write_fake_codex  # noqa: E402


class SessionDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.fake = write_fake_claude(root)
        self.log = root / "turns.log"
        os.environ["FAKE_LOG"] = str(self.log)
        os.environ["CODEX_HOME"] = str(root / "no-codex")  # never the machine's real daemon
        self.store = Store(root / "tether.db")
        self.fake_codex = write_fake_codex(root)
        self.settings = active.ActiveSettings(
            enabled=True, driver="session", launcher="direct", claude_binary=str(self.fake),
            codex_binary=str(self.fake_codex), codex_resume_args=("--dangerously-bypass-approvals-and-sandbox",),
            native_timeout_seconds=20, harness_env=("FAKE_LOG",),
        )
        self.driver = SessionDriver(
            self.store, root / "session", self.settings, launch_plan=_direct_launch,
            child_env=child_env,
            idle_seconds=60,
        )
        self.sent: list[tuple[str, str, str]] = []
        self.slice = active.ActiveSlice(
            runtime=self.store, driver=self.driver, settings=self.settings,
            egress=lambda c, t, text: self.sent.append((c, t, text)),
            descriptor=type("D", (), {"authorized_owner_ids": ("U12345678",), "canonical_owner_ids": ("U12345678",), "workspace_id": "T1",
                                      "persona_id": "primary", "policy_generation": 1})(),
        )
        self.binding = self.slice.bind(
            source_kind="claude_session", session_id="sess-A", cwd=self.temp.name,
            team_id="T1", channel_id="C1", thread_ts="100.1", owner_user_id="U12345678",
        )

    def tearDown(self):
        self.driver.shutdown()
        self.store.close()
        os.environ.pop("FAKE_LOG", None)
        os.environ.pop("CODEX_HOME", None)
        self.temp.cleanup()

    def fields(self, ts: str, actor: str = "U12345678") -> dict:
        return {"workspace": "T1", "channel": "C1", "thread": "100.1", "actor": actor, "message_id": ts}

    def pids(self) -> list[str]:
        return [line.split()[0] for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_two_turns_share_one_process_and_the_reply_lands_in_thread(self):
        self.assertIsNotNone(self.slice.claim(self.fields("100.2"), "first question"))
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIsNotNone(self.slice.claim(self.fields("100.3"), "second question"))
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("turn 1 of pid", self.sent[0][2])
        self.assertIn("turn 2 of pid", self.sent[1][2], "same process took the second turn")
        self.assertEqual(len(set(self.pids())), 1)
        self.assertEqual(self.sent[0][:2], ("C1", "100.1"))
        # both turns reached the harness (it logs one line per user turn)
        self.assertEqual(len(self.pids()), 2)
        self.assertEqual(self.slice.run_once(), 0)

    def test_marker_only_reply_is_silence(self):
        self.slice.claim(self.fields("100.2"), "SILENT please")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.store.counts()["ready_turns"], 0)

    def test_marker_plus_delivery_posts_the_delivery(self):
        os.environ["FAKE_REPLY"] = "printf 'NO_REPLY\\n\\n<@U12345678> here is the fragment: {\"a\": 1}\\n'"
        try:
            self.slice.claim(self.fields("100.2"), "deliver")
            self.assertEqual(self.slice.run_once(), 1)
        finally:
            os.environ.pop("FAKE_REPLY", None)
        self.assertEqual(self.sent, [("C1", "100.1", '<@U12345678> here is the fragment: {"a": 1}')])

    def test_crash_posts_the_reason_and_the_next_turn_relaunches(self):
        self.slice.claim(self.fields("100.2"), "CRASH now")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("I could not take this turn (harness_exited: You've hit your session limit", self.sent[0][2])
        self.slice.claim(self.fields("100.3"), "are you back")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("turn 1 of pid", self.sent[1][2], "fresh process after the crash")
        self.assertEqual(len(set(self.pids())), 2)

    def test_harness_error_result_is_a_failed_turn(self):
        self.slice.claim(self.fields("100.2"), "ERRORFLAG")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("I could not take this turn (harness_error: API overloaded", self.sent[0][2])

    def test_timeout_fails_the_turn_and_drops_the_process(self):
        self.settings = active.ActiveSettings(**{**self.settings.__dict__, "native_timeout_seconds": 1})
        self.slice.settings = self.settings
        self.slice.claim(self.fields("100.2"), "SLEEP a while")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("I stopped this turn: my session produced nothing for 1 minutes", self.sent[0][2])
        self.assertEqual(self.driver.idle_sweep(), 0, "the timed-out process was already dropped")

    def test_a_working_turn_outlives_the_idle_timeout(self):
        # The clock is idle time: a session that keeps emitting events is working, not wedged.
        self.settings = active.ActiveSettings(**{**self.settings.__dict__, "native_timeout_seconds": 1})
        self.slice.settings = self.settings
        os.environ["FAKE_TICKS"] = "6"  # ~2.4 s of progress events before the result
        try:
            self.slice.claim(self.fields("100.2"), "long job")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertNotIn("stopped this turn", self.sent[0][2])
            self.assertIn("turn 1 of pid", self.sent[0][2])
        finally:
            os.environ.pop("FAKE_TICKS", None)

    def test_close_binding_terminates_the_process(self):
        self.slice.claim(self.fields("100.2"), "hello")
        self.slice.run_once()
        self.assertEqual(len(self.driver._sessions), 1)
        closed = self.slice.handle({"op": "close", "channel_id": "C1", "thread_ts": "100.1", "team_id": "T1"})
        self.assertEqual(closed["status"], "closed")
        self.assertEqual(len(self.driver._sessions), 0)

    def test_idle_sweep_drops_stale_processes(self):
        self.slice.claim(self.fields("100.2"), "hello")
        self.slice.run_once()
        self.driver.idle_seconds = 0
        self.assertEqual(self.driver.idle_sweep(), 1)
        self.slice.claim(self.fields("100.3"), "still there?")
        self.slice.run_once()
        self.assertIn("turn 1 of pid", self.sent[1][2], "relaunched after the sweep")

    def test_spawn_and_identity_ops_work_on_the_session_slice(self):
        # spawn seeds a session through create_session; stub it so no harness is needed
        self.slice._create_session = lambda kind, cwd, task: "sess-spawned"
        posted: list[tuple[str, str, str | None]] = []
        self.slice._post = lambda channel, text, thread: (posted.append((channel, text, thread)) or "300.1")
        spawned = self.slice.handle({"op": "spawn", "task": "look into it", "channel_id": "C1", "cwd": self.temp.name})
        self.assertEqual((spawned["status"], spawned["session_id"], spawned["thread_ts"], spawned["team_id"]),
                         ("spawned", "sess-spawned", "300.1", "T1"))
        self.assertEqual(posted[0][0], "C1")
        found = self.store.find_active_binding(team_id="T1", channel_id="C1", thread_ts="300.1")
        self.assertEqual(found["binding_id"], spawned["bridge_id"])

    def bind_codex(self, thread="500.1", session_id="thread-abc"):
        return self.slice.bind(
            source_kind="codex_session", session_id=session_id, cwd=self.temp.name,
            team_id="T1", channel_id="C1", thread_ts=thread, owner_user_id="U12345678",
        )

    def codex_fields(self, ts: str, thread="500.1") -> dict:
        return {"workspace": "T1", "channel": "C1", "thread": thread, "actor": "U12345678", "message_id": ts}

    def test_codex_turns_share_one_app_server_and_resume_the_thread(self):
        self.bind_codex()
        self.slice.claim(self.codex_fields("500.2"), "first")
        self.assertEqual(self.slice.run_once(), 1)
        self.slice.claim(self.codex_fields("500.3"), "second")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(len(self.sent), 2)
        self.assertIn("codex turn 1 of pid", self.sent[0][2])
        self.assertIn("codex turn 2 of pid", self.sent[1][2], "same app-server took the second turn")
        self.assertEqual(len(set(self.pids())), 1)
        # a second codex binding shares the server but resumes its own thread
        self.bind_codex(thread="600.1", session_id="thread-xyz")
        self.slice.claim(self.codex_fields("600.2", thread="600.1"), "other thread")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("codex turn 3 of pid", self.sent[2][2])
        self.assertEqual(len(set(self.pids())), 1)

    def test_codex_failed_turn_posts_the_reason(self):
        os.environ["FAKE_CODEX_FAIL"] = "1"
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "go")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertIn("I could not take this turn (codex_turn_failed: model refused", self.sent[0][2])
        finally:
            os.environ.pop("FAKE_CODEX_FAIL", None)

    def test_codex_turn_without_turn_completed_ends_after_quiet(self):
        from runtime.plugin_next.session_driver import CodexAppServer
        os.environ["FAKE_CODEX_NO_COMPLETE"] = "1"
        previous = CodexAppServer.QUIET_AFTER
        CodexAppServer.QUIET_AFTER = 0.5
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "go")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertIn("codex turn 1 of pid", self.sent[0][2])
        finally:
            CodexAppServer.QUIET_AFTER = previous
            os.environ.pop("FAKE_CODEX_NO_COMPLETE", None)

    def test_codex_preamble_and_a_long_tool_call_do_not_end_the_turn(self):
        # 2026-09-15 C09NKJDMV7C: Codex said "I'll read the thread" then ran exec for minutes;
        # the preamble was posted as the reply and the real answer never reached Slack.
        from runtime.plugin_next.session_driver import CodexAppServer
        os.environ["FAKE_CODEX_PREAMBLE"] = "1.5"
        previous = CodexAppServer.QUIET_AFTER
        CodexAppServer.QUIET_AFTER = 0.5
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "ptal")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertEqual(len(self.sent), 1)
            self.assertIn("codex turn 1 of pid", self.sent[0][2], "the final answer is the reply")
            self.assertNotIn("read the thread", self.sent[0][2])
        finally:
            CodexAppServer.QUIET_AFTER = previous
            os.environ.pop("FAKE_CODEX_PREAMBLE", None)

    def test_codex_turn_has_no_clock(self):
        # 2026-09-15 01:18 C09NKJDMV7C: the 30-minute cap posted "could not take this turn"
        # while Astra was still fixing the planner in the session. Codex ends the turn, not us.
        self.settings = active.ActiveSettings(**{**self.settings.__dict__, "native_timeout_seconds": 1})
        self.slice.settings = self.settings
        os.environ["FAKE_CODEX_PREAMBLE"] = "2.5"  # silent work well past the old clock
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "fix it yourself")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertIn("codex turn 1 of pid", self.sent[0][2])
        finally:
            os.environ.pop("FAKE_CODEX_PREAMBLE", None)

    def test_codex_no_reply_is_silence(self):
        os.environ["FAKE_CODEX_REPLY"] = "NO_REPLY"
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "fyi")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertEqual(self.sent, [])
        finally:
            os.environ.pop("FAKE_CODEX_REPLY", None)

    def test_status_reports_the_store_counts(self):
        status = self.slice.handle({"op": "status"})
        self.assertEqual(status["implementation"], "tether")
        self.assertEqual(status["queued_delivery_count"], 0)


if __name__ == "__main__":
    unittest.main()


class CodexDaemonTests(SessionDriverTests):
    def test_codex_turns_prefer_the_machine_daemon(self):
        from tests.fakes import FakeCodexDaemon
        home = Path(self.temp.name) / "codex-home"
        daemon = FakeCodexDaemon(home)
        os.environ["CODEX_HOME"] = str(home)
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "first")
            self.assertEqual(self.slice.run_once(), 1)
            self.slice.claim(self.codex_fields("500.3"), "second")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertEqual(len(daemon.turns), 2)
            self.assertIn("first", daemon.turns[0][1], "the Slack text is in the turn's input")
            self.assertIn("second", daemon.turns[1][1])
            self.assertEqual(daemon.turns[0][0], "thread-abc")
            self.assertEqual(self.sent[0][2], "daemon turn 1")
            self.assertEqual(self.sent[1][2], "daemon turn 2")
            self.assertEqual(daemon.clients, 1, "one connection per gateway, reused across turns")
            self.assertFalse(self.log.exists(), "no child app-server was started")
            self.assertEqual(self.driver.codex_pids(), set())
        finally:
            daemon.close()

    def test_dead_daemon_socket_falls_back_to_a_child(self):
        import socket
        home = Path(self.temp.name) / "codex-home"
        control = home / "app-server-control"
        control.mkdir(parents=True)
        dead = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        dead.bind(str(control / "app-server-control.sock"))
        dead.close()  # a socket file nobody listens on: the daemon died
        os.environ["CODEX_HOME"] = str(home)
        try:
            self.bind_codex()
            self.slice.claim(self.codex_fields("500.2"), "go")
            self.assertEqual(self.slice.run_once(), 1)
            self.assertIn("codex turn 1 of pid", self.sent[0][2])
            self.assertTrue(self.driver.codex_pids())
        finally:
            pass


class WebSocketFramingTests(unittest.TestCase):
    def test_fragmented_text_message_is_reassembled(self):
        import socket
        from runtime.plugin_next.session_driver import WsReader
        a, b = socket.socketpair()
        reader = WsReader(b)
        try:
            body = b'{"jsonrpc":"2.0","method":"turn/completed","params":{"x":"' + b"y" * 70000 + b'"}}'
            first, rest = body[:1000], body[1000:]
            # FIN=0 text frame, then FIN=0 continuation, then FIN=1 continuation, with a ping in between.
            a.sendall(bytes([0x01, 126]) + struct.pack(">H", len(first)) + first)
            a.sendall(bytes([0x89, 0]))  # ping
            a.sendall(bytes([0x00, 126]) + struct.pack(">H", 100) + rest[:100])
            a.sendall(bytes([0x80, 127]) + struct.pack(">Q", len(rest) - 100) + rest[100:])
            self.assertEqual(reader.read(), (0x9, b""))
            got = reader.read()
            self.assertEqual(got[0], 0x1)
            self.assertTrue(got[1] == body, f"reassembled {len(got[1])} bytes, expected {len(body)}")
            a.close()
            self.assertIsNone(reader.read())
        finally:
            b.close()


class HerdrPaneTests(SessionDriverTests):
    def herdr_client(self):
        from tests.fakes import write_fake_herdr
        from runtime.plugin_next.herdr import Herdr
        root = Path(self.temp.name)
        fake = write_fake_herdr(root)
        os.environ["FAKE_HERDR_STATE"] = str(root / "herdr-state.json")
        os.environ["FAKE_HERDR_LOG"] = str(root / "herdr-calls.log")
        os.environ["CLAUDE_CONFIG_DIR"] = str(root / "claude")
        for key in ("FAKE_HERDR_STATE", "FAKE_HERDR_LOG", "FAKE_HERDR_TRANSCRIPT", "FAKE_HERDR_REPLY",
                    "FAKE_HERDR_AFTER", "FAKE_HERDR_SCREEN", "CLAUDE_CONFIG_DIR"):
            self.addCleanup(os.environ.pop, key, None)
        return Herdr(binary=str(fake), session="pilot")

    def place_claude_in_pane(self, client, name="mcp"):
        tab = client.tab_create(workspace_id="w1", cwd=self.temp.name, label=name)
        client.agent_start(name, kind="claude", pane_id=tab["pane_id"])
        sid = client.session_id(name)
        project = Path(self.temp.name) / "claude" / "projects" / "-tmp-x"
        project.mkdir(parents=True)
        transcript = project / f"{sid}.jsonl"
        transcript.write_text(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "older answer"}]}}) + "\n")
        os.environ["FAKE_HERDR_TRANSCRIPT"] = str(transcript)
        placement = {"session": "pilot", "workspace_id": "w1", "tab_id": tab["tab_id"], "pane_id": tab["pane_id"],
                     "agent": name, "kind": "claude"}
        self.slice.bind(source_kind="claude_session", session_id=sid, cwd=self.temp.name, team_id="T1",
                        channel_id="C1", thread_ts="700.1", owner_user_id="U12345678", spawned=True, herdr=placement)
        return sid, transcript

    def pane_fields(self, ts):
        return {"workspace": "T1", "channel": "C1", "thread": "700.1", "actor": "U12345678", "message_id": ts}

    def test_claude_in_a_pane_is_prompted_there_and_replies_from_the_transcript(self):
        client = self.herdr_client()
        self.driver.herdr_factory = lambda session: client
        sid, transcript = self.place_claude_in_pane(client)
        os.environ["FAKE_HERDR_REPLY"] = "Fixed in PR 8623: the form was never persisted."
        self.slice.claim(self.pane_fields("700.2"), "why are MCP experts missing forms?")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent, [("C1", "700.1", "Fixed in PR 8623: the form was never persisted.")],
                         "the final answer only: not the older transcript text, not the narration")
        log = (Path(self.temp.name) / "herdr-calls.log").read_text()
        prompt_call = log[log.index("agent prompt mcp "):]
        self.assertIn("why are MCP experts missing forms?", prompt_call)
        self.assertIn("--wait", prompt_call, "a Herdr turn waits with no clock")
        self.assertFalse(self.log.exists(), "no headless claude process was started for a pane session")
        # a second turn appends to the same transcript and only its own reply is posted
        os.environ["FAKE_HERDR_REPLY"] = "379 experts affected; migration drafted."
        self.slice.claim(self.pane_fields("700.3"), "how many in prod?")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent[-1][2], "379 experts affected; migration drafted.")

    def test_a_blocked_dialog_is_the_reply(self):
        client = self.herdr_client()
        self.driver.herdr_factory = lambda session: client
        self.place_claude_in_pane(client)
        os.environ["FAKE_HERDR_AFTER"] = "blocked"
        os.environ["FAKE_HERDR_SCREEN"] = "Bash command\n  rm -rf build/\nDo you want to proceed?\n > 1. Yes\n   2. No"
        self.slice.claim(self.pane_fields("700.2"), "clean the build dir")
        self.assertEqual(self.slice.run_once(), 1)
        text = self.sent[0][2]
        self.assertIn("waiting on a dialog", text)
        self.assertIn("Do you want to proceed?", text)

    def test_a_pane_that_vanished_falls_back_to_headless_resume(self):
        client = self.herdr_client()
        self.driver.herdr_factory = lambda session: client
        placement = {"session": "pilot", "workspace_id": "w1", "tab_id": "w1:t9", "pane_id": "w1:p9", "agent": "ghost", "kind": "claude"}
        self.slice.bind(source_kind="claude_session", session_id="sess-gone", cwd=self.temp.name, team_id="T1",
                        channel_id="C1", thread_ts="700.1", owner_user_id="U12345678", spawned=True, herdr=placement)
        self.slice.claim(self.pane_fields("700.2"), "still there?")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("turn 1 of pid", self.sent[0][2], "the headless path took the turn")


class HerdrDialogTests(HerdrPaneTests):
    def test_the_next_thread_message_answers_the_dialog(self):
        client = self.herdr_client()
        self.driver.herdr_factory = lambda session: client
        self.place_claude_in_pane(client)
        os.environ["FAKE_HERDR_AFTER"] = "blocked"
        os.environ["FAKE_HERDR_SCREEN"] = "Bash command\n  rm -rf build/\nDo you want to proceed?\n > 1. Yes\n   2. No"
        self.slice.claim(self.pane_fields("700.2"), "clean the build dir")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn("option number", self.sent[0][2])
        marks = [r for r in self.slice.slack.reactions if r[0] == "add"] if hasattr(self.slice, "slack") and self.slice.slack else []
        # the thread answers with the option number: keys, not a prompt
        os.environ.pop("FAKE_HERDR_AFTER", None)
        os.environ["FAKE_HERDR_REPLY"] = "Build directory removed; 12 files."
        self.slice.claim(self.pane_fields("700.3"), "1")
        self.assertEqual(self.slice.run_once(), 1)
        log = (Path(self.temp.name) / "herdr-calls.log").read_text()
        self.assertIn("agent send-keys mcp 1\n", log)
        self.assertIn("agent send-keys mcp enter\n", log)
        self.assertNotIn("agent prompt mcp 1", log, "an answer is never submitted as a new prompt")
        self.assertEqual(self.sent[-1][2], "Build directory removed; 12 files.", "the resumed turn's answer lands")
        del marks

    def test_free_text_answers_a_question_dialog(self):
        client = self.herdr_client()
        self.driver.herdr_factory = lambda session: client
        self.place_claude_in_pane(client)
        os.environ["FAKE_HERDR_AFTER"] = "blocked"
        os.environ["FAKE_HERDR_SCREEN"] = "Which environment should this target?\n > staging\n   prod\n   other"
        self.slice.claim(self.pane_fields("700.2"), "deploy it")
        self.assertEqual(self.slice.run_once(), 1)
        os.environ.pop("FAKE_HERDR_AFTER", None)
        os.environ["FAKE_HERDR_REPLY"] = "Deployed to staging."
        self.slice.claim(self.pane_fields("700.3"), "staging please")
        self.assertEqual(self.slice.run_once(), 1)
        log = (Path(self.temp.name) / "herdr-calls.log").read_text()
        self.assertIn("pane send-text w1:p2 staging please\n", log)
        self.assertIn("agent send-keys mcp enter\n", log)
        self.assertEqual(self.sent[-1][2], "Deployed to staging.")

    def test_esc_cancels(self):
        from runtime.plugin_next.herdr import dialog_answer
        self.assertEqual(dialog_answer("2"), ("keys", ["2", "enter"]))
        self.assertEqual(dialog_answer(" esc "), ("keys", ["esc"]))
        self.assertEqual(dialog_answer("Escape"), ("keys", ["esc"]))
        self.assertEqual(dialog_answer("y"), ("keys", ["y"]))
        self.assertEqual(dialog_answer("use the blue one"), ("text", ["use the blue one"]))
