"""The broker is the one door for the CLI. Drive it over a real Unix socket
against a slice with a fake Slack and a fake harness."""

from __future__ import annotations

import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class FakeSlack:
    configured = True

    def __init__(self):
        self.posts: list[tuple[str, str, str | None]] = []
        self.n = 0

    def identity(self):
        return {"team_id": "T12345678", "user_id": "UBOT", "user": "bot"}

    def post(self, channel_id, text, *, thread_ts=None):
        self.n += 1
        self.posts.append((channel_id, text, thread_ts))
        return f"1700000000.{self.n:06d}"

    def thread_replies(self, channel_id, thread_ts, *, limit=50):
        return [{"ts": thread_ts, "text": "root"}]

    def history(self, channel_id, *, limit=20):
        return [{"ts": "1.0", "text": "hi", "user": "U1"}]

    def membership(self, channel_id):
        return "member"

    reactions: list[tuple[str, str, str, str]] = []

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
            for name in ("domain_runtime", "domain_schema", "native_driver", "plugin_next",
                         "plugin_next.active", "plugin_next.broker"):
                sys.modules.pop(name, None)
            import domain_runtime, domain_schema, native_driver  # noqa: E401
            from plugin_next import active, broker
        finally:
            sys.path[:] = previous
        self.temp = tempfile.TemporaryDirectory(prefix="tether-broker-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        db = base / "domain.db"
        connection = sqlite3.connect(db)
        domain_schema.install_schema(connection)
        connection.execute(f"PRAGMA user_version={domain_schema.SCHEMA_VERSION}")
        connection.commit()
        connection.close()
        self.schema = domain_schema
        self.db = db
        runtime = domain_runtime.DomainRuntime(db)
        driver = native_driver.NativeDriver(runtime, work_root=base / "driver")
        descriptor = domain_schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(), workspace_id="T12345678", persona_id="primary",
            authorized_owner_ids=("U12345678",), policy_generation=1,
        )
        self.slack = FakeSlack()
        self.sent = []
        self.slice = active.ActiveSlice(
            runtime=runtime, driver=driver,
            settings=active.ActiveSettings(enabled=True, native_timeout_seconds=30,
                                           extra={"default_channel": "C1"}),
            egress=lambda c, t, x: self.sent.append((c, t, x)),
            descriptor=descriptor, slack=self.slack,
            command_factory=lambda ctx, st, prompt: ["/bin/sh", "-c", "printf 'listo'"],
        )
        self.broker_module = broker
        self.server = broker.BrokerServer(base / "b.sock", self.slice.handle)
        self.server.start()
        self.socket = base / "b.sock"

    def tearDown(self):
        self.server.stop()
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
        connection = sqlite3.connect(self.db)
        self.assertEqual(self.schema.invariant_violations(connection), [])
        connection.close()

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

    def test_same_session_in_another_domain_is_refused_with_a_code(self):
        other = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(), workspace_id="T12345678", persona_id="other",
            authorized_owner_ids=("U99999999",), policy_generation=1,
        )
        self.slice.runtime.register_endpoint(
            endpoint_key="detached_native:claude_session:sess-x", endpoint_kind="detached_native",
            source_kind="claude_session", source_json='{"session_id":"sess-x"}', ref_version=1,
            descriptor=other,
        )
        refused = self.call(op="attach", channel_id="C1", thread_ts="100.9", idempotency_key="ax", **self.source("sess-x"))
        self.assertFalse(refused["ok"])
        self.assertEqual(refused["code"], "endpoint_key_conflict")

    def test_spawn_creates_a_session_binds_it_and_the_thread_drives_it(self):
        created = []

        def fake_create(source_kind, cwd, task):
            created.append((source_kind, str(cwd), task))
            return "sess-spawned"

        self.slice._create_session = fake_create
        spawned = self.call(op="spawn", harness="claude", task="fix the flaky test", cwd=self.temp.name)
        self.assertTrue(spawned["ok"], spawned)
        self.assertEqual(spawned["session_id"], "sess-spawned")
        self.assertEqual(created, [("claude_session", self.temp.name, "fix the flaky test")])
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

        def boom(k, c, t):
            raise RuntimeError("claude did not report a session id (exit 1)")

        self.slice._create_session = boom
        failed = self.call(op="spawn", harness="claude", task="t", cwd=self.temp.name)
        self.assertEqual(failed["code"], "spawn_failed")

    def test_create_session_parses_both_harnesses(self):
        import subprocess as sp
        active = sys.modules["plugin_next.active"]
        settings = active.ActiveSettings(claude_binary="/bin/echo", codex_binary="/bin/echo")

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

    def test_failed_turn_marks_the_message_with_a_warning(self):
        self.slice.command_factory = lambda ctx, st, prompt: ["/bin/sh", "-c", "exit 3"]
        self.call(op="attach", channel_id="C1", thread_ts="100.3", idempotency_key="f1", **self.source("sess-f"))
        self.slack.reactions.clear()
        fields = {"workspace": "T12345678", "channel": "C1", "thread": "100.3", "actor": "U12345678", "message_id": "1700000000.000031"}
        self.slice.claim(fields, "do it")
        self.assertEqual(self.slice.run_once(), 1)
        self.assertIn(("add", "C1", "1700000000.000031", "warning"), self.slack.reactions)
        # ...and says so once, instead of leaving the thread staring at an emoji.
        self.assertEqual(len(self.sent), 1)
        self.assertIn("I could not take this turn (exit_3)", self.sent[0][2])

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
