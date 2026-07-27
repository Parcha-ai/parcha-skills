import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

from test_bridge import PLUGIN_PATH, load_runtime, process_identity


BOOT_ID = "00000000-0000-4000-8000-000000000001"


class SyntheticNativeIdentitySafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.proc_root = self.home / "proc"
        boot_dir = self.proc_root / "sys" / "kernel" / "random"
        boot_dir.mkdir(parents=True)
        (boot_dir / "boot_id").write_text(BOOT_ID, encoding="utf-8")
        self.codex = self.home / "bin" / "codex"
        self.shell = self.home / "bin" / "bash"

    def tearDown(self):
        self.temp.cleanup()

    def _write_process(
        self,
        *,
        pid: int,
        executable: pathlib.Path,
        parent_pid: int,
        tty_number: int,
        start_time: int,
        process_group: int | None = None,
        foreground_group: int | None = None,
    ) -> pathlib.Path:
        executable.parent.mkdir(parents=True, exist_ok=True)
        if not executable.exists():
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
        process = self.proc_root / str(pid)
        process.mkdir()
        (process / "environ").write_bytes(
            b"ZELLIJ_SESSION_NAME=work\0ZELLIJ_PANE_ID=51\0"
        )
        (process / "cmdline").write_bytes(
            (str(executable) + "\0").encode()
        )
        process_group = process_group if process_group is not None else pid
        foreground_group = (
            foreground_group
            if foreground_group is not None
            else process_group
        )
        fields = [
            "S",
            str(parent_pid),
            str(process_group),
            str(process_group),
            str(tty_number),
            str(foreground_group),
            *(["0"] * 13),
            str(start_time),
        ]
        (process / "stat").write_text(
            f"{pid} ({executable.name}) " + " ".join(fields),
            encoding="utf-8",
        )
        (process / "exe").symlink_to(executable)
        return process

    def _resolve_agent(self):
        return self.runtime._zellij_agent_process(
            "work",
            "51",
            {"codex"},
            self.proc_root,
            metadata_agent="codex",
            trusted_paths={"codex": {str(self.codex.resolve())}},
        )

    def test_foreground_agent_with_inherited_pane_env_on_foreign_tty_is_rejected(
        self,
    ):
        self._write_process(
            pid=100,
            executable=self.shell,
            parent_pid=1,
            tty_number=34_851,
            start_time=10_000,
        )
        self._write_process(
            pid=200,
            executable=self.codex,
            parent_pid=100,
            tty_number=34_999,
            start_time=20_000,
        )

        with self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self._resolve_agent()

        self.assertEqual(raised.exception.code, "process_identity_ambiguous")
        self.assertFalse(raised.exception.retryable)

    def test_pane_state_is_required_but_command_metadata_is_optional(self):
        self._write_process(
            pid=100,
            executable=self.shell,
            parent_pid=1,
            tty_number=34_851,
            start_time=10_000,
        )
        self._write_process(
            pid=200,
            executable=self.codex,
            parent_pid=100,
            tty_number=34_851,
            start_time=20_000,
        )
        original_resolver = self.runtime._zellij_agent_process

        def resolve(
            session,
            pane,
            allowed,
            metadata_agent="",
            trusted_paths=None,
        ):
            return original_resolver(
                session,
                pane,
                allowed,
                self.proc_root,
                metadata_agent=metadata_agent,
                trusted_paths=trusted_paths,
            )

        rejected = {
            "absent": [],
            "missing_state": [{
                "id": 51,
                "is_plugin": False,
            }],
        }
        for name, panes in rejected.items():
            with self.subTest(metadata=name), mock.patch.object(
                self.runtime,
                "_resolve_executable",
                return_value="/usr/bin/zellij",
            ), mock.patch.object(
                self.runtime.subprocess,
                "run",
                return_value=types.SimpleNamespace(stdout=json.dumps(panes)),
            ), mock.patch.object(
                self.runtime,
                "_trusted_agent_paths",
                return_value={"codex": {str(self.codex.resolve())}},
            ), mock.patch.object(
                self.runtime,
                "_zellij_agent_process",
                side_effect=resolve,
            ):
                with self.assertRaises(
                    self.runtime.NativeContinuationError
                ):
                    self.runtime.zellij_pane_identity("work", "51")

        accepted = (
            {
                "id": 51,
                "is_plugin": False,
                "exited": False,
            },
            {
                "id": 51,
                "is_plugin": False,
                "exited": False,
                "terminal_command": None,
            },
        )
        for record in accepted:
            with self.subTest(metadata=record), mock.patch.object(
                self.runtime,
                "_resolve_executable",
                return_value="/usr/bin/zellij",
            ), mock.patch.object(
                self.runtime.subprocess,
                "run",
                return_value=types.SimpleNamespace(
                    stdout=json.dumps([record])
                ),
            ), mock.patch.object(
                self.runtime,
                "_trusted_agent_paths",
                return_value={"codex": {str(self.codex.resolve())}},
            ), mock.patch.object(
                self.runtime,
                "_zellij_agent_process",
                side_effect=resolve,
            ):
                identity = self.runtime.zellij_pane_identity("work", "51")
            self.assertEqual(identity["pane_agent"], "codex")

    def test_reused_tty_does_not_reanchor_a_stale_agent_process(self):
        pane_shell = self._write_process(
            pid=100,
            executable=self.shell,
            parent_pid=1,
            tty_number=34_851,
            start_time=10_000,
        )
        agent = self._write_process(
            pid=200,
            executable=self.codex,
            parent_pid=100,
            tty_number=34_851,
            start_time=20_000,
        )
        _, original_identity = self._resolve_agent()

        pane_shell.rename(self.proc_root / "retired-pane-root")
        fields = (agent / "stat").read_text(encoding="utf-8").split()
        fields[3] = "1"
        (agent / "stat").write_text(" ".join(fields), encoding="utf-8")
        self._write_process(
            pid=300,
            executable=self.shell,
            parent_pid=1,
            tty_number=34_851,
            start_time=30_000,
        )

        with self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self._resolve_agent()

        self.assertEqual(raised.exception.code, "process_identity_missing")
        self.assertTrue(original_identity)


class NativeSubmissionAndDurabilitySafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.previous_bridge_runtime = sys.modules.get("bridge_runtime")
        sys.modules["bridge_runtime"] = self.runtime
        spec = importlib.util.spec_from_file_location(
            f"native_delivery_safety_plugin_{id(self)}",
            PLUGIN_PATH,
        )
        self.plugin = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.plugin
        spec.loader.exec_module(self.plugin)
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        self.plugin.state.ready = True
        self.plugin_module_name = spec.name
        self.thread_counter = 0

    def tearDown(self):
        sys.modules.pop(self.plugin_module_name, None)
        if self.previous_bridge_runtime is None:
            sys.modules.pop("bridge_runtime", None)
        else:
            sys.modules["bridge_runtime"] = self.previous_bridge_runtime
        self.temp.cleanup()

    def _bound_bridge(
        self,
        key: str,
        *,
        source_kind: str,
        source: dict[str, str],
    ):
        bridge = self.plugin.store.create(
            {
                "source_kind": source_kind,
                "source": source,
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        return self.plugin.store.bind(
            bridge.bridge_id,
            f"1790000000.{self.thread_counter:06d}",
        )

    def _claim(self, bridge, event_id: str):
        self.assertTrue(
            self.plugin.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "continue",
            )
        )
        items = self.plugin.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in items], [event_id])
        return items

    def test_dump_failure_after_accepted_write_is_uncertain_and_not_requeued(
        self,
    ):
        identity = process_identity(
            agent="codex",
            session="work",
            pane="51",
            tty="34851",
        )
        bridge = self._bound_bridge(
            "write-accepted-dump-failed",
            source_kind="codex_session",
            source={
                "session_id": "codex-session",
                "cwd": "/tmp/project",
                "zellij_session": "work",
                "zellij_pane_id": "51",
                "pane_agent": "codex",
                "pane_command_hash": "expected",
                "process_identity": identity,
            },
        )
        event_id = "1790000001.000001"
        items = self._claim(bridge, event_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        commands = []

        def run(command, **_kwargs):
            commands.append(command)
            if "write-chars" in command:
                return subprocess.CompletedProcess(command, 0, "", "")
            if "dump-screen" in command:
                raise subprocess.TimeoutExpired(command, 10)
            raise AssertionError(f"unexpected Zellij command: {command}")

        error_code = ""
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value={"process_identity": identity},
        ), mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/zellij",
        ), mock.patch.object(
            self.runtime.subprocess,
            "run",
            side_effect=run,
        ), mock.patch.object(
            self.runtime.time,
            "sleep",
        ):
            try:
                self.plugin._submit_zellij_attempt(
                    bridge,
                    items,
                    "continue",
                )
            except self.runtime.NativeContinuationError as exc:
                error_code = exc.code
            else:
                self.fail("dump-screen failure unexpectedly completed")

        with self.plugin.store.connect() as database:
            event = database.execute(
                """
                SELECT state,error,attempt_id
                FROM bridge_events WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            attempt = database.execute(
                """
                SELECT state,error_code
                FROM bridge_attempts WHERE attempt_id=?
                """,
                (attempt_id,),
            ).fetchone()
        observed = {
            "error_code": error_code,
            "attempt_state": str(attempt["state"]),
            "attempt_error": str(attempt["error_code"] or ""),
            "event_state": str(event["state"]),
            "event_error": str(event["error"] or ""),
            "event_attempt_id": str(event["attempt_id"] or ""),
            "enter_sent": any("send-keys" in command for command in commands),
        }
        self.assertEqual(
            observed,
            {
                "error_code": "terminal_submit_uncertain",
                "attempt_state": "uncertain",
                "attempt_error": "terminal_submit_uncertain",
                "event_state": "uncertain",
                "event_error": "terminal_submit_uncertain",
                "event_attempt_id": attempt_id,
                "enter_sent": False,
            },
        )

    def test_completed_detached_response_survives_return_boundary_crash_and_restart(
        self,
    ):
        bridge = self._bound_bridge(
            "detached-pre-stage-crash",
            source_kind="codex_session",
            source={
                "session_id": "codex-session",
                "cwd": "/tmp/project",
            },
        )
        event_id = "1790000002.000001"
        items = self._claim(bridge, event_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        response = "Exact detached response recovered after restart."
        agent_runs = 0

        class SimulatedProcessCrash(BaseException):
            pass

        crash_armed = False

        def continue_native(
            _bridge,
            _prompt,
            _cancellation,
            persist_response,
        ):
            nonlocal agent_runs, crash_armed
            agent_runs += 1
            persist_response(response)
            crash_armed = True
            return response

        submit_code = self.plugin._submit_detached_attempt.__code__

        def crash_after_continue_returns(frame, event, _arg):
            if (
                crash_armed
                and frame.f_code is submit_code
                and event == "line"
            ):
                raise SimulatedProcessCrash
            return crash_after_continue_returns

        previous_trace = sys.gettrace()
        try:
            sys.settrace(crash_after_continue_returns)
            with mock.patch.object(
                self.plugin,
                "continue_native",
                side_effect=continue_native,
            ):
                with self.assertRaises(SimulatedProcessCrash):
                    self.plugin._submit_detached_attempt(
                        bridge,
                        items,
                        "continue",
                        None,
                    )
        finally:
            sys.settrace(previous_trace)

        restarted_runtime = load_runtime(self.home)
        restarted_store = restarted_runtime.Store(self.plugin.store.path)
        restarted_broker = restarted_runtime.Broker(
            "test-token",
            restarted_store,
            verified_workspace_team_id="T12345678",
        )
        posted = []

        def slack_post(_token, _channel, text, _thread_ts, **_kwargs):
            posted.append(text)
            return "1790000002.000002"

        with mock.patch.object(
            restarted_broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            restarted_runtime,
            "slack_post",
            side_effect=slack_post,
        ):
            recovered = restarted_broker.recover_replies()

        with restarted_store.connect() as database:
            event_state = str(
                database.execute(
                    "SELECT state FROM bridge_events WHERE event_id=?",
                    (event_id,),
                ).fetchone()["state"]
            )
        observed = {
            "agent_runs": agent_runs,
            "recovered_replies": recovered,
            "posted": posted,
            "attempt_state": restarted_store.attempt_state(
                attempt_id,
                bridge.bridge_id,
            ),
            "event_state": event_state,
        }
        self.assertEqual(
            observed,
            {
                "agent_runs": 1,
                "recovered_replies": 1,
                "posted": [response],
                "attempt_state": "acknowledged",
                "event_state": "delivered",
            },
        )


if __name__ == "__main__":
    unittest.main()
