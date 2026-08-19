"""L1 exit evidence: live-agent traces and the production gateway controller.

Covers the L1.1 requirement for a live integration trace — one endpoint,
two Slack thread bindings, real detached subprocesses — and the default
GatewayController's fail-closed attestation behavior.
"""

from __future__ import annotations

import importlib
import os
import pathlib
import sqlite3
import stat
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load_modules():
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in (
            "schema_rehearsal",
            "native_driver",
            "domain_runtime",
            "schema_orchestrator",
            "schema_receipt",
            "domain_control",
            "domain_schema",
            "bridge_runtime",
            "security",
            "routing",
            "slack_protocol",
        ):
            sys.modules.pop(name, None)
        return (
            importlib.import_module("domain_runtime"),
            importlib.import_module("native_driver"),
            importlib.import_module("schema_rehearsal"),
        )
    finally:
        sys.path[:] = previous


class TwoThreadLiveTraceTest(unittest.TestCase):
    """One endpoint serving two Slack threads with real subprocesses."""

    def setUp(self):
        self.runtime_module, self.driver_module, self.rehearsal = load_modules()
        self.schema = self.runtime_module.domain_schema
        self.temp = tempfile.TemporaryDirectory(prefix="tether-l1-trace-")
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

    def tearDown(self):
        self.temp.cleanup()

    def test_replies_route_to_their_originating_binding_generation(self):
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        endpoint = self.runtime.register_endpoint(
            endpoint_key="trace-session",
            endpoint_kind="detached_native",
            source_kind="claude_session",
            source_json='{"session_id":"trace"}',
            ref_version=1,
            descriptor=descriptor,
        )
        threads = {}
        for name, channel, thread_ts, ordered_at in (
            ("alpha", "C-alpha", "100.1", "100.0"),
            ("beta", "C-beta", "200.1", "150.0"),
        ):
            binding = self.runtime.bind_thread(
                endpoint_id=endpoint["endpoint_id"],
                team_id="T12345678",
                channel_id=channel,
                thread_ts=thread_ts,
                owner_user_id="U12345678",
                idempotency_key=f"trace-{name}",
            )
            self.runtime.admit_turn(
                binding_id=binding["binding_id"],
                event_key=f"evt-{name}",
                ordered_at=ordered_at,
                payload_inline=f"pregunta {name}",
            )
            threads[name] = binding

        trace = []
        for expected_name, expected_fence, reply in (
            ("alpha", 1, "respuesta alpha"),
            ("beta", 2, "respuesta beta"),
        ):
            attempt = self.runtime.schedule_next(endpoint["endpoint_id"])
            self.assertEqual(attempt["binding_id"], threads[expected_name]["binding_id"])
            self.assertEqual(attempt["lease_fence"], expected_fence)
            launched = self.driver.launch(
                attempt,
                command=["/bin/sh", "-c", f"printf %s '{reply}'"],
                cwd=pathlib.Path(self.temp.name),
                env={"PATH": "/usr/bin:/bin"},
            )
            result = self.driver.reap(attempt, launched, timeout_seconds=60)
            self.assertEqual(result["state"], "completed_with_response")
            status = self.runtime.attempt_status(attempt["attempt_id"])
            trace.append({
                "thread": expected_name,
                "binding_id": status["binding_id"],
                "binding_generation": status["binding_generation"],
                "lease_fence": status["lease_fence"],
                "state": status["state"],
            })

        # Each reply is bound to the thread that asked, by stored identity.
        self.assertEqual(trace[0]["binding_id"], threads["alpha"]["binding_id"])
        self.assertEqual(trace[1]["binding_id"], threads["beta"]["binding_id"])
        self.assertEqual(
            [entry["binding_generation"] for entry in trace],
            [threads["alpha"]["generation"], threads["beta"]["generation"]],
        )
        self.assertEqual([entry["lease_fence"] for entry in trace], [1, 2])

        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            self.assertEqual(self.schema.invariant_violations(connection), [])
            responses = {
                row["binding_id"]: row["response_sha256"]
                for row in connection.execute(
                    "SELECT binding_id,response_sha256 FROM native_attempts"
                )
            }
        finally:
            connection.close()
        self.assertEqual(len(set(responses.values())), 2)


class DefaultGatewayControllerTest(unittest.TestCase):
    def setUp(self):
        _runtime, _driver, self.rehearsal = load_modules()
        self.temp = tempfile.TemporaryDirectory(prefix="tether-controller-")
        self.base = pathlib.Path(self.temp.name)
        os.chmod(self.base, 0o700)
        self.calls = self.base / "calls.log"
        self.state = self.base / "unit-state"
        self.state.write_text("active\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def script(self, name: str, body: str) -> str:
        path = self.base / name
        path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return str(path)

    def controller(self, *, hermes_body: str, unit_active_file: pathlib.Path):
        hermes = self.script("hermes", hermes_body)
        systemctl = self.script(
            "systemctl",
            f'grep -qx active "{unit_active_file}" && exit 0 || exit 3\n',
        )
        return self.rehearsal.default_gateway_controller(
            hermes_bin=hermes,
            system_systemctl=systemctl,
            user_systemctl=(systemctl, "--user"),
        )

    def test_honest_stop_and_start_round_trip(self):
        hermes_body = (
            f'echo "$@" >>"{self.calls}"\n'
            f'case "$2" in\n'
            f'  stop) echo inactive >"{self.state}";;\n'
            f'  start) echo active >"{self.state}";;\n'
            f'esac\n'
        )
        controller = self.controller(
            hermes_body=hermes_body,
            unit_active_file=self.state,
        )
        self.assertTrue(controller.is_active())
        controller.stop()
        self.assertFalse(controller.is_active())
        controller.start()
        self.assertTrue(controller.is_active())
        recorded = self.calls.read_text(encoding="utf-8")
        self.assertIn("gateway stop", recorded)
        self.assertIn("gateway start", recorded)

    def test_lying_stop_is_exposed_by_the_supervisor_probe(self):
        controller = self.controller(
            hermes_body="exit 0\n",  # stop "succeeds" but changes nothing
            unit_active_file=self.state,
        )
        controller.stop()
        self.assertTrue(controller.is_active())

    def test_start_failure_raises_typed_error(self):
        controller = self.controller(
            hermes_body='[ "$2" = start ] && exit 7\nexit 0\n',
            unit_active_file=self.state,
        )
        with self.assertRaises(self.rehearsal.RehearsalError) as caught:
            controller.start()
        self.assertEqual(caught.exception.code, "gateway_start_failed")

    def test_no_probe_available_fails_closed_as_active(self):
        hermes = self.script("hermes", "exit 0\n")
        controller = self.rehearsal.default_gateway_controller(
            hermes_bin=hermes,
            system_systemctl=str(self.base / "missing-systemctl"),
            user_systemctl=(str(self.base / "missing-systemctl"), "--user"),
        )
        self.assertTrue(controller.is_active())

    def test_missing_hermes_binary_is_refused(self):
        with self.assertRaises(self.rehearsal.RehearsalError) as caught:
            self.rehearsal.default_gateway_controller(
                hermes_bin=str(self.base / "missing-hermes"),
            )
        self.assertEqual(caught.exception.code, "hermes_unavailable")


if __name__ == "__main__":
    unittest.main()
