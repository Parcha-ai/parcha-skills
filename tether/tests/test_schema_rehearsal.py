from __future__ import annotations

import hashlib
import importlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class SimulatedKill(BaseException):
    """Uncatchable-by-run() stand-in for SIGKILL at a durable boundary."""


def load_modules(home: pathlib.Path):
    environment = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_STATE_HOME": str(home / ".local" / "state"),
    }
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in (
            "schema_rehearsal",
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
        with mock.patch.dict(os.environ, environment, clear=False):
            return importlib.import_module("schema_rehearsal")
    finally:
        sys.path[:] = previous


class FakeController:
    def __init__(self, *, stop_lies: bool = False, start_fails: bool = False):
        self.active = True
        self.stop_lies = stop_lies
        self.start_fails = start_fails
        self.stop_calls = 0
        self.start_calls = 0

    def stop(self):
        self.stop_calls += 1
        if not self.stop_lies:
            self.active = False

    def start(self):
        self.start_calls += 1
        if not self.start_fails:
            self.active = True

    def is_active(self):
        return self.active

    def as_controller(self, module):
        return module.GatewayController(
            stop=self.stop,
            start=self.start,
            is_active=self.is_active,
        )


class SchemaRehearsalTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-rehearsal-")
        self.home = pathlib.Path(self.temp.name)
        self.module = load_modules(self.home)
        self.runtime = self.module.bridge_runtime
        self.schema = self.module.domain_schema
        self.orchestrator = self.module.schema_orchestrator
        self.receipts = self.module.schema_receipt
        self.env = mock.patch.dict(os.environ, {
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
            "XDG_DATA_HOME": str(self.home / ".local" / "share"),
            "XDG_STATE_HOME": str(self.home / ".local" / "state"),
        }, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def build_environment(self, name: str = "site") -> dict[str, pathlib.Path]:
        site = self.home / name
        hermes = site / ".hermes"
        state = site / "state" / "tether-installer"
        for directory in (hermes, state, state / "backups"):
            directory.mkdir(parents=True, mode=0o700)
        database = hermes / "bridges.db"
        self.runtime.Store(database)

        artifacts = site / "artifacts"
        predecessor = artifacts / "predecessor"
        target = artifacts / "target"
        for root in (predecessor, target):
            root.mkdir(parents=True, mode=0o700)
            for source in sorted(RUNTIME.glob("*.py")):
                destination = root / source.name
                shutil.copyfile(source, destination)
                destination.chmod(0o600)
        pin = predecessor / "schema_rehearsal.py"
        pin.write_text(
            pin.read_text(encoding="utf-8") + "\n# predecessor artifact pin\n",
            encoding="utf-8",
        )

        self.write_install_manifest(state, site)
        return {
            "database": database,
            "socket": hermes / "bridge.sock",
            "state": state,
            "predecessor": predecessor,
            "target": target,
        }

    def write_install_manifest(self, state: pathlib.Path, site: pathlib.Path) -> None:
        runtime_home = site / "installed" / "tether"
        metadata = {
            "harness": "codex",
            "runtime_home": str(runtime_home),
            "plugin_home": str(site / ".hermes" / "plugins" / "tether"),
            "local_bin": str(site / "bin"),
            "codex_root": str(site / ".codex"),
            "claude_root": str(site / ".claude"),
            "legacy": "none",
        }
        records = []
        target_modes = self.orchestrator._expected_manifest_target_modes(metadata)
        for target in sorted(target_modes):
            mode = target_modes[target]
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            target.write_text(f"# installed {target.name}\n", encoding="utf-8")
            target.chmod(mode)
            records.append(
                f"{target}\t{mode:o}\t{hashlib.sha256(target.read_bytes()).hexdigest()}"
            )
        manifest = state / "current.tsv"
        manifest.write_text(
            "# tether-manifest-v2\n"
            + "".join(f"@{key}\t{metadata[key]}\n" for key in (
                "harness",
                "runtime_home",
                "plugin_home",
                "local_bin",
                "codex_root",
                "claude_root",
                "legacy",
            ))
            + "\n".join(records)
            + "\n",
            encoding="utf-8",
        )
        manifest.chmod(0o600)

    def descriptor(self):
        return self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )

    def coordinator(self, paths, controller, fault_inject=None):
        return self.module.RehearsalCoordinator(
            database=paths["database"],
            socket_path=paths["socket"],
            state_root=paths["state"],
            target_root=paths["target"],
            predecessor_root=paths["predecessor"],
            descriptor=self.descriptor(),
            controller=controller.as_controller(self.module),
            fault_inject=fault_inject,
        )

    @staticmethod
    def sha256(path: pathlib.Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_full_rehearsal_completes_and_never_touches_the_live_database(self):
        paths = self.build_environment()
        live_before = self.sha256(paths["database"])
        controller = FakeController()
        result = self.coordinator(paths, controller).run()
        self.assertTrue(result["ok"])
        self.assertEqual(self.sha256(paths["database"]), live_before)
        receipt, error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertIsNone(error)
        self.assertEqual(receipt["phase"], "complete")
        self.assertEqual(
            receipt["phases"]["runtime_verified"]["synthetic_cycle"],
            "ok",
        )
        self.assertEqual(receipt["operation"], "rehearse")
        self.assertFalse(
            self.receipts.maintenance_armed(paths["state"] / "schema" / "maintenance")
        )
        backup = paths["state"] / "backups" / "schema" / f"{receipt['receipt_id']}.db"
        self.assertTrue(backup.exists())
        self.assertEqual(self.sha256(backup), result["backup_sha256"])
        self.assertEqual(controller.stop_calls, 1)
        self.assertEqual(controller.start_calls, 1)
        recovery = self.module.classify_recovery(paths["state"])
        self.assertEqual(recovery["state"], "complete")
        self.assertTrue(recovery["admission_allowed"])
        leftovers = [
            entry
            for entry in (paths["state"] / "backups" / "schema").iterdir()
            if entry.name.startswith("rehearsal-")
        ]
        self.assertEqual(leftovers, [])

    def test_lying_stop_fails_closed_before_the_singleton(self):
        paths = self.build_environment()
        controller = FakeController(stop_lies=True)
        with self.assertRaises(self.module.RehearsalError) as caught:
            self.coordinator(paths, controller).run()
        self.assertEqual(caught.exception.code, "gateway_still_active")
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")
        self.assertEqual(receipt["error_code"], "gateway_still_active")
        self.assertTrue(
            self.receipts.maintenance_armed(paths["state"] / "schema" / "maintenance")
        )
        self.assertFalse(
            (paths["state"] / "backups" / "schema").exists()
        )

    def test_held_database_singleton_fails_before_backup(self):
        paths = self.build_environment()
        holder_fd = self.runtime.acquire_database_singleton(paths["database"])
        try:
            controller = FakeController()
            with self.assertRaises(self.module.RehearsalError) as caught:
                self.coordinator(paths, controller).run()
        finally:
            os.close(holder_fd)
        self.assertEqual(caught.exception.code, "database_singleton_unavailable")
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")
        self.assertFalse((paths["state"] / "backups" / "schema").exists())

    def test_lifecycle_lock_contention_fails_before_any_receipt(self):
        paths = self.build_environment()
        lock_file = paths["state"] / "install.lock"
        holder = self.module._acquire_lifecycle_lock(lock_file)
        try:
            with self.assertRaises(self.module.RehearsalError) as caught:
                self.coordinator(paths, FakeController()).run()
        finally:
            os.close(holder)
        self.assertEqual(caught.exception.code, "lifecycle_lock_busy")
        receipt, error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertIsNone(receipt)
        self.assertIsNone(error)

    def test_artifact_drift_after_preflight_is_refused(self):
        paths = self.build_environment()

        def drift(mark: str) -> None:
            if mark == "backup_verified":
                pinned = paths["target"] / "schema_rehearsal.py"
                pinned.write_text(
                    pinned.read_text(encoding="utf-8") + "\n# drifted\n",
                    encoding="utf-8",
                )

        with self.assertRaises(self.module.RehearsalError) as caught:
            self.coordinator(paths, FakeController(), fault_inject=drift).run()
        self.assertEqual(caught.exception.code, "artifact_drift")
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")

    def test_tampered_backup_is_refused(self):
        paths = self.build_environment()

        def truncate(mark: str) -> None:
            if mark == "backup_written":
                receipt, _error = self.receipts.load(
                    paths["state"] / "schema" / "active.json"
                )
                backup = (
                    paths["state"] / "backups" / "schema" / f"{receipt['receipt_id']}.db"
                )
                with open(backup, "r+b") as handle:
                    handle.truncate(16)

        with self.assertRaises(self.module.RehearsalError):
            self.coordinator(paths, FakeController(), fault_inject=truncate).run()
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")

    def test_hardlinked_backup_is_refused(self):
        paths = self.build_environment()

        def hardlink(mark: str) -> None:
            if mark == "backup_written":
                receipt, _error = self.receipts.load(
                    paths["state"] / "schema" / "active.json"
                )
                backup = (
                    paths["state"] / "backups" / "schema" / f"{receipt['receipt_id']}.db"
                )
                os.link(backup, backup.with_suffix(".copy"))

        with self.assertRaises(self.module.RehearsalError):
            self.coordinator(paths, FakeController(), fault_inject=hardlink).run()
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")

    def test_gateway_restart_failure_holds_safe_after_resumed(self):
        paths = self.build_environment()
        controller = FakeController(start_fails=True)
        with self.assertRaises(self.module.RehearsalError) as caught:
            self.coordinator(paths, controller).run()
        self.assertEqual(caught.exception.code, "gateway_restart_failed")
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertEqual(receipt["phase"], "failed_safe")
        self.assertTrue(
            self.receipts.maintenance_armed(paths["state"] / "schema" / "maintenance")
        )

    def test_kill_after_every_durable_phase_recovers_to_one_state(self):
        expectations = {
            "planned": ("incomplete_planned", False),
            "maintenance_armed": ("incomplete_planned", False),
            "quiesced": ("incomplete_quiesced", False),
            "singleton_acquired": ("incomplete_singleton_acquired", False),
            "backup_written": ("incomplete_singleton_acquired", False),
            "backup_verified": ("incomplete_backup_verified", False),
            "runtime_verified": ("incomplete_runtime_verified", False),
            "resumed": ("incomplete_resumed", True),
        }
        for index, (mark, (expected_state, admission)) in enumerate(
            expectations.items()
        ):
            with self.subTest(mark=mark):
                paths = self.build_environment(name=f"kill-{index}")
                live_before = self.sha256(paths["database"])

                def kill(seen: str, *, target_mark: str = mark) -> None:
                    if seen == target_mark:
                        raise SimulatedKill(target_mark)

                with self.assertRaises(SimulatedKill):
                    self.coordinator(paths, FakeController(), fault_inject=kill).run()

                recovery = self.module.classify_recovery(paths["state"])
                self.assertEqual(recovery["state"], expected_state)
                self.assertEqual(recovery["admission_allowed"], admission)
                self.assertEqual(self.sha256(paths["database"]), live_before)

                with self.assertRaises(self.module.RehearsalError) as caught:
                    self.coordinator(paths, FakeController()).run()
                self.assertEqual(caught.exception.code, "operation_already_exists")

    def test_startup_gate_blocks_incomplete_and_allows_resumed(self):
        paths = self.build_environment()

        def kill(seen: str) -> None:
            if seen == "backup_verified":
                raise SimulatedKill(seen)

        with self.assertRaises(SimulatedKill):
            self.coordinator(paths, FakeController(), fault_inject=kill).run()

        with mock.patch.dict(os.environ, {
            "HOME": str(paths["state"].parents[1]),
            "XDG_STATE_HOME": str(paths["state"].parent),
        }, clear=False):
            with self.assertRaises(RuntimeError) as caught:
                self.runtime.open_locked_store(paths["database"])
            self.assertIn("schema_operation_incomplete", str(caught.exception))

    def test_sigkill_subprocess_between_backup_and_receipt_fsync(self):
        paths = self.build_environment(name="sigkill")
        script = f"""
import os
import sys
sys.path.insert(0, {str(RUNTIME)!r})
import schema_rehearsal
import domain_schema


def kill(mark):
    if mark == "backup_written":
        os.kill(os.getpid(), 9)


descriptor = domain_schema.SecurityDomainDescriptor(
    instance_uid=os.geteuid(),
    workspace_id="T12345678",
    persona_id="primary",
    authorized_owner_ids=("U12345678",),
    policy_generation=1,
)
state = {{"active": True}}
controller = schema_rehearsal.GatewayController(
    stop=lambda: state.update(active=False),
    start=lambda: state.update(active=True),
    is_active=lambda: state["active"],
)
schema_rehearsal.RehearsalCoordinator(
    database={str(paths["database"])!r},
    socket_path={str(paths["socket"])!r},
    state_root={str(paths["state"])!r},
    target_root={str(paths["target"])!r},
    predecessor_root={str(paths["predecessor"])!r},
    descriptor=descriptor,
    controller=controller,
    fault_inject=kill,
).run()
"""
        completed = subprocess.run(
            [sys.executable, "-c", script],
            env={**os.environ},
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(completed.returncode, -9, completed.stderr)
        recovery = self.module.classify_recovery(paths["state"])
        self.assertEqual(recovery["state"], "incomplete_singleton_acquired")
        self.assertFalse(recovery["admission_allowed"])
        self.assertTrue(recovery["maintenance_armed"])
        with self.assertRaises(self.module.RehearsalError) as caught:
            self.coordinator(paths, FakeController()).run()
        self.assertEqual(caught.exception.code, "operation_already_exists")

    def test_status_stays_redacted_during_a_rehearsal(self):
        paths = self.build_environment()

        def kill(seen: str) -> None:
            if seen == "backup_verified":
                raise SimulatedKill(seen)

        with self.assertRaises(SimulatedKill):
            self.coordinator(paths, FakeController(), fault_inject=kill).run()
        receipt, _error = self.receipts.load(paths["state"] / "schema" / "active.json")
        public = self.receipts.public_view(receipt)
        rendered = json.dumps(public)
        self.assertNotIn(str(self.home), rendered)
        self.assertNotIn("phases", public)
        self.assertNotIn("backup_sha256", rendered)

    def test_rehearsal_over_populated_store_preserves_live_content(self):
        paths = self.build_environment(name="populated")
        store = self.runtime.Store(paths["database"])
        for index in range(2):
            bridge = store.create({
                "source_kind": "headless_run",
                "source": {"run_id": f"run-{index}", "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": f"populated-{index}",
            })
            store.bind(bridge.bridge_id, f"1786000000.00000{index}")
        live_before = self.sha256(paths["database"])
        result = self.coordinator(paths, FakeController()).run()
        self.assertTrue(result["ok"])
        self.assertEqual(self.sha256(paths["database"]), live_before)
        receipt, error = self.receipts.load(paths["state"] / "schema" / "active.json")
        self.assertIsNone(error)
        self.assertEqual(receipt["phase"], "complete")
        self.assertEqual(
            receipt["phases"]["runtime_verified"]["synthetic_cycle"],
            "ok",
        )
        # Preservation is judged over the migration contract's key subset:
        # rollback retains the archived endpoint inventory by design, so the
        # whole-manifest digests differ while every preserved record must
        # round-trip exactly.
        self.assertEqual(
            receipt["phases"]["runtime_verified"]["post_rollback_preserved_sha256"],
            receipt["phases"]["backup_verified"]["preserved_manifest_sha256"],
        )
        self.assertNotEqual(
            receipt["phases"]["runtime_verified"]["post_rollback_manifest_sha256"],
            receipt["phases"]["backup_verified"]["logical_manifest_sha256"],
        )

    def test_installer_refuses_actions_while_maintenance_is_armed(self):
        paths = self.build_environment()
        self.receipts.arm_maintenance(paths["state"] / "schema" / "maintenance")
        environment = {
            **os.environ,
            "HOME": str(paths["state"].parents[1]),
            "XDG_STATE_HOME": str(paths["state"].parent),
        }
        completed = subprocess.run(
            ["bash", str(ROOT / "install.sh"), "uninstall"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(completed.returncode, 75, completed.stderr)
        self.assertIn("maintenance is armed", completed.stderr)


if __name__ == "__main__":
    unittest.main()
