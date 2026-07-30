import concurrent.futures
import importlib.util
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

from test_bridge import PLUGIN_PATH, load_runtime, process_identity


BOOT_ID = "00000000-0000-4000-8000-000000000001"


class L1RuntimeRedTeamTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.thread_counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def _bound_bridge(
        self,
        key: str,
        *,
        source_kind: str = "headless_run",
        source: dict[str, str] | None = None,
    ):
        bridge = self.store.create(
            {
                "source_kind": source_kind,
                "source": source or {"run_id": key, "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        return self.store.bind(
            bridge.bridge_id,
            f"1786000000.{self.thread_counter:06d}",
        )

    def _prepared_attempt(self, bridge, event_id: str, text: str = "continue"):
        self.assertTrue(self.store.enqueue_event(event_id, bridge.bridge_id, text))
        items = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in items], [event_id])
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
            )
        )
        return attempt_id

    def test_binding_rejects_allowlisted_process_without_pane_foreground_tty(self):
        proc_root = self.home / "proc"
        boot_path = proc_root / "sys" / "kernel" / "random"
        boot_path.mkdir(parents=True)
        (boot_path / "boot_id").write_text(BOOT_ID, encoding="utf-8")

        executable = self.home / "bin" / "codex"
        executable.parent.mkdir(parents=True)
        executable.write_text("#!/bin/sh\n", encoding="utf-8")
        executable.chmod(0o700)

        process = proc_root / "200"
        process.mkdir()
        (process / "environ").write_bytes(
            b"ZELLIJ_SESSION_NAME=work\0ZELLIJ_PANE_ID=51\0"
        )
        (process / "cmdline").write_bytes((str(executable) + "\0").encode())
        # tpgid=-1 means this process has no foreground terminal relationship.
        fields = ["S", "1", "200", "200", "0", "-1", *(["0"] * 13), "20000"]
        (process / "stat").write_text(
            "200 (codex) " + " ".join(fields),
            encoding="utf-8",
        )
        (process / "exe").symlink_to(executable)

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.runtime._zellij_agent_process(
                "work",
                "51",
                {"codex"},
                proc_root,
                trusted_paths={"codex": {str(executable.resolve())}},
            )
        self.assertEqual(raised.exception.code, "process_identity_missing")

    def test_enter_accepted_then_timeout_is_submission_uncertain(self):
        identity = process_identity(agent="codex", session="work", pane="51")
        bridge = self.runtime.Bridge(
            "brg_" + "a" * 32,
            "codex_session",
            {
                "session_id": "codex-session",
                "cwd": str(self.home),
                "zellij_session": "work",
                "zellij_pane_id": "51",
                "pane_agent": "codex",
                "process_identity": identity,
            },
            "U12345678",
            "T12345678",
            "C12345678",
            "1786000001.000001",
            "enter-timeout",
            "active",
            1,
            2,
            "verified",
            "",
        )
        marker = "att_" + "b" * 24
        calls = 0
        enter_accepted = False

        def fake_run(command, **_kwargs):
            nonlocal calls, enter_accepted
            calls += 1
            if calls == 1:
                return subprocess.CompletedProcess(command, 0, "", "")
            if calls == 2:
                return subprocess.CompletedProcess(command, 0, marker, "")
            if calls == 3:
                enter_accepted = True
                raise subprocess.TimeoutExpired(command, 10)
            raise AssertionError(f"unexpected subprocess call {calls}")

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
            side_effect=fake_run,
        ), mock.patch.object(
            self.runtime.time,
            "sleep",
        ), self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self.runtime.deliver_zellij(bridge, "continue", marker)

        self.assertTrue(enter_accepted)
        self.assertEqual(raised.exception.code, "terminal_submit_uncertain")

    def test_fast_reply_transition_is_monotonic_through_replying(self):
        bridge = self._bound_bridge("fast-reply")
        attempt_id = self._prepared_attempt(
            bridge,
            "1786000002.000001",
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.runtime.stage_reply_payload(
            self.store,
            bridge.bridge_id,
            attempt_id,
            "done",
        )

        armed = self.store.mark_attempt_awaiting_ack(
            attempt_id,
            bridge.bridge_id,
            bridge.binding_generation,
        )
        if not armed:
            self.store.fail_attempt(
                attempt_id,
                bridge.bridge_id,
                "late acknowledgment arm failed",
            )
        claimed = self.store.claim_reply(
            attempt_id,
            bridge.bridge_id,
        )
        acknowledged = self.store.complete_reply(
            attempt_id,
            bridge.bridge_id,
            claimed["lease_id"],
            "1786000002.000002",
        )

        self.assertTrue(armed, "replying is already beyond awaiting_ack")
        self.assertEqual(acknowledged, 1)
        self.assertEqual(
            self.store.attempt_state(attempt_id, bridge.bridge_id),
            "acknowledged",
        )

    def test_concurrent_reply_retries_hold_one_outbound_lease(self):
        bridge = self._bound_bridge("concurrent-reply")
        attempt_id = self._prepared_attempt(
            bridge,
            "1786000003.000001",
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        brokers = (
            self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678"),
            self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678"),
        )
        first_entered = threading.Event()
        release_first = threading.Event()
        second_posted = threading.Event()
        calls: list[str | None] = []
        calls_lock = threading.Lock()

        def slack_post(*_args, **kwargs):
            with calls_lock:
                calls.append(kwargs.get("client_msg_id"))
                call_number = len(calls)
            if call_number == 1:
                first_entered.set()
                if not release_first.wait(timeout=5):
                    raise TimeoutError("test did not release first Slack post")
                return "1786000003.000002"
            second_posted.set()
            return "1786000003.000003"

        request = {
            "op": "reply",
            "bridge_id": bridge.bridge_id,
            "reply_key": attempt_id,
            "text": "done",
        }
        with mock.patch.object(
            brokers[0],
            "_ensure_channel_membership",
        ), mock.patch.object(
            brokers[1],
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=slack_post,
        ), concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(brokers[0].handle, request)
            self.assertTrue(first_entered.wait(timeout=5))
            second = executor.submit(brokers[1].handle, request)
            second_posted.wait(timeout=0.5)
            release_first.set()
            for future in (first, second):
                try:
                    future.result(timeout=5)
                except Exception:
                    # A contending caller may receive a typed retry/in-progress result.
                    pass

        self.assertEqual(
            len(calls),
            1,
            "only the holder of the durable reply lease may call Slack",
        )

    def test_ambiguous_attempts_do_not_expire_by_age(self):
        expected: dict[str, tuple[str, str]] = {}
        for index, state in enumerate(
            ("uncertain", "awaiting_ack", "replying"),
            start=1,
        ):
            bridge = self._bound_bridge(f"old-{state}")
            event_id = f"1786000010.{index:06d}"
            attempt_id = self._prepared_attempt(bridge, event_id)
            if state == "uncertain":
                self.assertTrue(
                    self.store.mark_attempt_submitting(
                        attempt_id,
                        bridge.bridge_id,
                        bridge.binding_generation,
                    )
                )
                self.assertTrue(
                    self.store.mark_attempt_uncertain(
                        attempt_id,
                        bridge.bridge_id,
                    )
                )
                event_state = "uncertain"
            else:
                self.assertTrue(
                    self.store.mark_attempt_awaiting_ack(
                        attempt_id,
                        bridge.bridge_id,
                        bridge.binding_generation,
                    )
                )
                event_state = "awaiting_ack"
                if state == "replying":
                    self.runtime.stage_reply_payload(
                        self.store,
                        bridge.bridge_id,
                        attempt_id,
                        "pending reply",
                    )
                    event_state = "replying"
            expected[attempt_id] = (state, event_state)

        with self.store.connect() as database:
            database.execute(
                """
                UPDATE bridge_attempts
                SET updated_at=datetime('now','-2 days')
                WHERE state IN ('uncertain','awaiting_ack','replying')
                """
            )
            database.execute(
                """
                UPDATE bridge_events
                SET updated_at=datetime('now','-2 days')
                WHERE state IN ('uncertain','awaiting_ack','replying')
                """
            )

        reopened = self.runtime.Store(self.store.path)
        actual: dict[str, tuple[str, str]] = {}
        with reopened.connect() as database:
            for attempt_id in expected:
                attempt = database.execute(
                    "SELECT state FROM bridge_attempts WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()
                event = database.execute(
                    "SELECT state FROM bridge_events WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()
                actual[attempt_id] = (attempt["state"], event["state"])

        self.assertEqual(actual, expected)

    def test_rebind_is_blocked_during_processing_to_attempt_gap(self):
        bridge = self._bound_bridge("rebind-gap")
        self.assertTrue(
            self.store.enqueue_event(
                "1786000020.000001",
                bridge.bridge_id,
                "continue",
            )
        )
        items = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual(len(items), 1)

        with self.assertRaises(ValueError):
            self.store.rebind(
                bridge.bridge_id,
                "headless_run",
                {"run_id": "replacement", "cwd": "/tmp/project"},
                expected_generation=bridge.binding_generation,
            )


class L1PluginRedTeamTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.previous_bridge_runtime = sys.modules.get("bridge_runtime")
        sys.modules["bridge_runtime"] = self.runtime
        spec = importlib.util.spec_from_file_location(
            f"l1_redteam_plugin_{id(self)}",
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
        source_kind: str = "headless_run",
        source: dict[str, str] | None = None,
    ):
        bridge = self.plugin.store.create(
            {
                "source_kind": source_kind,
                "source": source or {"run_id": key, "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        return self.plugin.store.bind(
            bridge.bridge_id,
            f"1787000000.{self.thread_counter:06d}",
        )

    def _awaiting_attempt(self, bridge, event_id: str):
        self.assertTrue(
            self.plugin.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "first request",
            )
        )
        items = self.plugin.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.plugin.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.plugin.store.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        return attempt_id, items

    def _event_state(self, event_id: str) -> str:
        with self.plugin.store.connect() as database:
            return str(
                database.execute(
                    "SELECT state FROM bridge_events WHERE event_id=?",
                    (event_id,),
                ).fetchone()["state"]
            )

    def test_detached_response_is_durable_before_broker_delivery(self):
        bridge = self._bound_bridge(
            "detached-response",
            source_kind="codex_session",
            source={"session_id": "codex-session", "cwd": "/tmp/project"},
        )
        event_id = "1787000001.000001"
        self.assertTrue(
            self.plugin.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "do work",
            )
        )
        items = self.plugin.store.claim_event_batch(bridge.bridge_id)
        response = "unique verified detached result"
        def continue_native(_bridge, _prompt, _cancellation, persist):
            persist(response)
            return response

        with mock.patch.object(
            self.plugin,
            "continue_native",
            side_effect=continue_native,
        ):
            attempt_id, returned = self.plugin._submit_detached_attempt(
                bridge,
                items,
                "do work",
                None,
            )
        self.assertEqual(returned, response)

        reopened = self.runtime.Store(self.plugin.store.path)
        durable_values: list[str] = []
        with reopened.connect() as database:
            tables = [
                str(row[0])
                for row in database.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """
                )
            ]
            for table in tables:
                columns = [
                    str(row[1])
                    for row in database.execute(f'PRAGMA table_info("{table}")')
                ]
                for row in database.execute(f'SELECT * FROM "{table}"'):
                    values = [str(row[column] or "") for column in columns]
                    if attempt_id in values:
                        durable_values.extend(values)

        observed = {
            "attempt_state": reopened.attempt_state(
                attempt_id,
                bridge.bridge_id,
            ),
            "response_persisted": any(
                response in value for value in durable_values
            ),
        }
        self.assertIn(
            observed["attempt_state"],
            {"replying", "acknowledged"},
            observed,
        )
        self.assertTrue(observed["response_persisted"], observed)

    def test_startup_recovery_wake_cannot_leave_worker_flag_stuck(self):
        bridge = self._bound_bridge("startup-wake")
        attempt_id, _ = self._awaiting_attempt(
            bridge,
            "1787000002.000001",
        )
        followup_id = "1787000002.000002"
        self.assertTrue(
            self.plugin.store.enqueue_event(
                followup_id,
                bridge.bridge_id,
                "follow-up",
            )
        )

        class ImmediateThread:
            def __init__(self, target, **_kwargs):
                self.target = target

            def start(self):
                self.target()

        manager = types.SimpleNamespace(_hooks={})
        context = types.SimpleNamespace(_manager=manager)

        def register_hook(name, callback):
            manager._hooks.setdefault(name, []).append(callback)

        context.register_hook = register_hook

        def complete_recovered(_bridge, items):
            self.plugin._finish_batch(items)

        with mock.patch.dict(
            os.environ,
            {"SLACK_BOT_TOKEN": "test-token"},
            clear=False,
        ), mock.patch.object(
            self.plugin,
            "start_broker",
            return_value=object(),
        ), mock.patch.object(
            self.plugin,
            "_install_slack_bridge_prefilter",
        ), mock.patch.object(
            self.plugin,
            "_validate_hermes_compatibility",
            return_value="0.19.0",
        ), mock.patch.object(
            self.plugin,
            "_run_recovered_event",
            side_effect=complete_recovered,
        ), mock.patch.object(
            self.plugin.threading,
            "Thread",
            ImmediateThread,
        ):
            self.plugin.register(context)
            self.plugin.store.acknowledge_attempt(
                attempt_id,
                bridge.bridge_id,
                ack_kind="no_reply",
            )
            self.plugin._schedule_bridge_drain(bridge.bridge_id)

        observed = {
            "worker_started": self.plugin.state.recovery_worker_started,
            "followup_state": self._event_state(followup_id),
        }
        self.assertEqual(
            observed,
            {"worker_started": False, "followup_state": "delivered"},
        )

    def test_active_recovery_worker_rescans_after_ack_wakeup(self):
        target = self._bound_bridge("rescan-target")
        attempt_id, _ = self._awaiting_attempt(
            target,
            "1787000003.000001",
        )
        target_followup = "1787000003.000002"
        self.assertTrue(
            self.plugin.store.enqueue_event(
                target_followup,
                target.bridge_id,
                "target follow-up",
            )
        )
        blocker = self._bound_bridge("rescan-blocker")
        blocker_event = "1787000003.000003"
        self.assertTrue(
            self.plugin.store.enqueue_event(
                blocker_event,
                blocker.bridge_id,
                "block recovery briefly",
            )
        )

        original_queued_bridge_ids = self.plugin.store.queued_bridge_ids
        first_scan = True
        blocker_entered = threading.Event()
        release_blocker = threading.Event()

        def queued_bridge_ids():
            nonlocal first_scan
            if first_scan:
                first_scan = False
                return [target.bridge_id, blocker.bridge_id]
            return original_queued_bridge_ids()

        def recover(bridge, items):
            if bridge.bridge_id == blocker.bridge_id:
                blocker_entered.set()
                if not release_blocker.wait(timeout=5):
                    raise TimeoutError("test did not release recovery worker")
            self.plugin._finish_batch(items)

        with self.plugin.state.recovery_lock:
            self.plugin.state.recovery_worker_started = True
        with mock.patch.object(
            self.plugin.store,
            "queued_bridge_ids",
            side_effect=queued_bridge_ids,
        ), mock.patch.object(
            self.plugin,
            "_run_recovered_event",
            side_effect=recover,
        ):
            worker = threading.Thread(
                target=self.plugin._recover_queued_events,
                daemon=True,
            )
            worker.start()
            self.assertTrue(blocker_entered.wait(timeout=5))
            self.plugin.store.acknowledge_attempt(
                attempt_id,
                target.bridge_id,
                ack_kind="no_reply",
            )
            self.plugin._schedule_bridge_drain(target.bridge_id)
            release_blocker.set()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())

            deadline = time.monotonic() + 1
            while (
                self._event_state(target_followup) == "queued"
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)

        self.assertEqual(
            self._event_state(target_followup),
            "delivered",
            "a wake received during recovery must force another ready-bridge scan",
        )


if __name__ == "__main__":
    unittest.main()
