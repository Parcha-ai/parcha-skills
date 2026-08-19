from __future__ import annotations

import hashlib
import importlib
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class SimulatedKill(BaseException):
    pass


def load_modules():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("native_driver", "domain_runtime", "domain_schema", "security"):
            sys.modules.pop(name, None)
        driver_module = importlib.import_module("native_driver")
        runtime_module = importlib.import_module("domain_runtime")
        return driver_module, runtime_module
    finally:
        sys.path[:] = previous


class NativeDriverTest(unittest.TestCase):
    def setUp(self):
        self.driver_module, self.runtime_module = load_modules()
        self.schema = self.runtime_module.domain_schema
        self.temp = tempfile.TemporaryDirectory(prefix="tether-native-driver-")
        base = pathlib.Path(self.temp.name)
        os.chmod(base, 0o700)
        self.db_path = base / "domain.db"
        connection = sqlite3.connect(self.db_path)
        try:
            self.schema.install_schema(connection)
            connection.execute(f"PRAGMA user_version={self.schema.SCHEMA_VERSION}")
            connection.commit()
        finally:
            connection.close()
        self.runtime = self.runtime_module.DomainRuntime(self.db_path)
        self.driver = self.driver_module.NativeDriver(
            self.runtime,
            work_root=base / "driver",
        )
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        endpoint = self.runtime.register_endpoint(
            endpoint_key="driver-session",
            endpoint_kind="detached_native",
            source_kind="claude_session",
            source_json='{"session_id":"work"}',
            ref_version=1,
            descriptor=descriptor,
        )
        self.endpoint_id = endpoint["endpoint_id"]
        binding = self.runtime.bind_thread(
            endpoint_id=self.endpoint_id,
            team_id="T12345678",
            channel_id="C1",
            thread_ts="100.1",
            owner_user_id="U12345678",
            idempotency_key="driver-binding",
        )
        self.binding_id = binding["binding_id"]

    def tearDown(self):
        self.temp.cleanup()

    def schedule(self, event_key="evt-1"):
        self.runtime.admit_turn(
            binding_id=self.binding_id,
            event_key=event_key,
            ordered_at="100.1",
            payload_inline="hola",
        )
        return self.runtime.schedule_next(self.endpoint_id)

    def shell(self, script: str) -> dict[str, str | list[str]]:
        return {
            "command": ["/bin/sh", "-c", script],
            "cwd": pathlib.Path(self.temp.name),
            "env": {"PATH": "/usr/bin:/bin"},
        }

    def assert_invariants_hold(self):
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def test_response_output_becomes_durable_completed_receipt(self):
        attempt = self.schedule()
        launched = self.driver.launch(attempt, **self.shell("printf 'resultado final'"))
        result = self.driver.reap(attempt, launched, timeout_seconds=30)
        self.assertEqual(result["state"], "completed_with_response")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertFalse(status["lease_open"])
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT response_ref,response_sha256,response_bytes "
                "FROM native_attempts WHERE attempt_id=?",
                (attempt["attempt_id"],),
            ).fetchone()
        finally:
            connection.close()
        digest = hashlib.sha256(b"resultado final").hexdigest()
        self.assertEqual(row["response_ref"], f"blob:sha256:{digest}")
        self.assertEqual(row["response_sha256"], digest)
        blob = self.driver.blob_root / digest
        self.assertEqual(blob.read_bytes(), b"resultado final")
        self.assertEqual(oct(blob.stat().st_mode & 0o777), "0o600")
        self.assert_invariants_hold()

    def test_exact_no_reply_token_closes_as_silence(self):
        attempt = self.schedule()
        launched = self.driver.launch(attempt, **self.shell("printf ' NO_REPLY\\n'"))
        result = self.driver.reap(attempt, launched, timeout_seconds=30)
        self.assertEqual(result["state"], "no_reply")

    def test_empty_output_is_failure_not_silence(self):
        attempt = self.schedule()
        launched = self.driver.launch(attempt, **self.shell("true"))
        result = self.driver.reap(attempt, launched, timeout_seconds=30)
        self.assertEqual(result["state"], "failed")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["error_code"], "empty_response")

    def test_nonzero_exit_is_failure_even_with_output(self):
        attempt = self.schedule()
        launched = self.driver.launch(
            attempt,
            **self.shell("printf 'partial'; exit 3"),
        )
        result = self.driver.reap(attempt, launched, timeout_seconds=30)
        self.assertEqual(result["state"], "failed")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["error_code"], "exit_3")

    def test_kill_before_accepted_receipt_recovers_uncertain_never_respawns(self):
        for mark in ("after_intent", "after_spawn"):
            with self.subTest(mark=mark):
                descriptor = self.schema.SecurityDomainDescriptor(
                    instance_uid=os.geteuid(),
                    workspace_id="T12345678",
                    persona_id="primary",
                    authorized_owner_ids=("U12345678",),
                    policy_generation=1,
                )
                endpoint = self.runtime.register_endpoint(
                    endpoint_key=f"driver-{mark}",
                    endpoint_kind="detached_native",
                    source_kind="claude_session",
                    source_json='{"session_id":"work"}',
                    ref_version=1,
                    descriptor=descriptor,
                )
                binding = self.runtime.bind_thread(
                    endpoint_id=endpoint["endpoint_id"],
                    team_id="T12345678",
                    channel_id=f"C-{mark}",
                    thread_ts="100.1",
                    owner_user_id="U12345678",
                    idempotency_key=f"bind-{mark}",
                )
                self.runtime.admit_turn(
                    binding_id=binding["binding_id"],
                    event_key=f"evt-{mark}",
                    ordered_at="100.1",
                    payload_inline="hola",
                )
                attempt = self.runtime.schedule_next(endpoint["endpoint_id"])

                def kill(seen: str, *, target=mark) -> None:
                    if seen == target:
                        raise SimulatedKill(target)

                with self.assertRaises(SimulatedKill):
                    self.driver.launch(
                        attempt,
                        fault_inject=kill,
                        **self.shell("printf 'nunca visto'"),
                    )
                recovery = self.driver.recover(attempt)
                self.assertEqual(recovery["classification"], "uncertain")
                status = self.runtime.attempt_status(attempt["attempt_id"])
                self.assertEqual(status["state"], "uncertain")
                self.assertTrue(status["lease_open"])
                with self.assertRaises(self.driver_module.NativeDriverError) as caught:
                    self.driver.launch(attempt, **self.shell("printf 'replay'"))
                self.assertEqual(caught.exception.code, "attempt_already_launched")
                self.assert_invariants_hold()

    def test_recovery_without_intent_proves_never_spawned_and_requeues(self):
        attempt = self.schedule()
        recovery = self.driver.recover(attempt)
        self.assertEqual(recovery["classification"], "never_spawned")
        status = self.runtime.attempt_status(attempt["attempt_id"])
        self.assertEqual(status["state"], "failed_before_start")
        self.assertFalse(status["lease_open"])
        rescheduled = self.runtime.schedule_next(self.endpoint_id)
        self.assertEqual(rescheduled["event_keys"], ["evt-1"])
        self.assert_invariants_hold()

    def test_recovery_after_accepted_reconciles_from_durable_outcome(self):
        attempt = self.schedule()
        launched = self.driver.launch(attempt, **self.shell("printf 'sobrevivio'"))
        launched["process"].wait(timeout=30)
        # Simulate a driver crash after acceptance: a fresh recover() must
        # reconcile purely from the journal and the response file.
        recovery = self.driver.recover(attempt)
        self.assertEqual(recovery["classification"], "exited_reconciled")
        self.assertEqual(recovery["state"], "completed_with_response")
        self.assert_invariants_hold()

    def test_recovery_while_process_alive_keeps_watching(self):
        attempt = self.schedule()
        launched = self.driver.launch(
            attempt,
            **self.shell("sleep 30; printf 'tarde'"),
        )
        try:
            recovery = self.driver.recover(attempt)
            self.assertEqual(recovery["classification"], "still_running")
            status = self.runtime.attempt_status(attempt["attempt_id"])
            self.assertEqual(status["state"], "accepted")
        finally:
            self.driver.cancel(
                attempt,
                launched,
                cancel_request_id="cancel-cleanup",
            )

    def test_cancellation_kills_the_process_group_and_terminalizes(self):
        attempt = self.schedule()
        launched = self.driver.launch(attempt, **self.shell("sleep 300"))
        result = self.driver.cancel(
            attempt,
            launched,
            cancel_request_id="cancel-1",
        )
        self.assertEqual(result["state"], "cancelled")
        connection = sqlite3.connect(self.db_path)
        try:
            turn_state = connection.execute(
                "SELECT state FROM queued_turns WHERE event_key='evt-1'"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(turn_state, "cancelled")
        self.assert_invariants_hold()


if __name__ == "__main__":
    unittest.main()
