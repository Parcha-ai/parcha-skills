"""Session driver against a fake harness that speaks stream-json, and the slice on top of it."""

from __future__ import annotations

import os
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

    def test_trailing_no_reply_is_silence(self):
        self.slice.claim(self.fields("100.2"), "SILENT please")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.store.counts()["ready_turns"], 0)

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
        self.assertIn("I could not take this turn (timeout", self.sent[0][2])
        self.assertEqual(self.driver.idle_sweep(), 0, "the timed-out process was already dropped")

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

    def test_codex_no_reply_is_silence(self):
        os.environ["FAKE_CODEX_REPLY"] = "done\nNO_REPLY"
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
