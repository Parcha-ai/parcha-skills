from __future__ import annotations

import hashlib
import importlib
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load_orchestrator(home: pathlib.Path):
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
            "schema_orchestrator",
            "domain_control",
            "domain_schema",
            "bridge_runtime",
            "security",
            "routing",
            "slack_protocol",
        ):
            sys.modules.pop(name, None)
        with mock.patch.dict(os.environ, environment, clear=False):
            return importlib.import_module("schema_orchestrator")
    finally:
        sys.path[:] = previous


class SchemaOrchestratorStatusTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-schema-status-")
        self.home = pathlib.Path(self.temp.name)
        self.module = load_orchestrator(self.home)
        self.paths = self.module.SchemaPaths(
            database=self.home / ".hermes" / "bridges.db",
            receipt=self.home / ".local" / "state" / "tether-installer" / "schema" / "active.json",
            install_manifest=self.home / ".local" / "state" / "tether-installer" / "current.tsv",
        )
        self.paths.database.parent.mkdir(parents=True, mode=0o700)
        self.paths.install_manifest.parent.mkdir(parents=True, mode=0o700)
        runtime_home = self.home / ".local" / "share" / "tether"
        metadata = {
            "harness": "codex",
            "runtime_home": str(runtime_home),
            "plugin_home": str(self.home / ".hermes" / "plugins" / "tether"),
            "local_bin": str(self.home / ".local" / "bin"),
            "codex_root": str(self.home / ".codex"),
            "claude_root": str(self.home / ".claude"),
            "legacy": "none",
        }
        records = []
        target_modes = self.module._expected_manifest_target_modes(metadata)
        for target in sorted(target_modes):
            mode = target_modes[target]
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            target.write_text(f"# installed {target.name}\n", encoding="utf-8")
            target.chmod(mode)
            records.append(
                f"{target}\t{mode:o}\t{hashlib.sha256(target.read_bytes()).hexdigest()}"
            )
        self.paths.install_manifest.write_text(
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
        self.paths.install_manifest.chmod(0o600)
        self.config = self.module.bridge_runtime.Config(
            team_id="T12345678",
            allowed_users=("U12345678",),
            persona_id="primary",
            policy_generation=1,
        )

    def tearDown(self):
        self.temp.cleanup()

    def schema17_database(self):
        store = self.module.bridge_runtime.Store(self.paths.database)
        self.assertEqual(store.path, self.paths.database)

    def schema18_database(self):
        connection = sqlite3.connect(self.paths.database)
        try:
            self.module.domain_schema.install_schema(connection)
            connection.execute("PRAGMA user_version=18")
            connection.commit()
        finally:
            connection.close()
        self.paths.database.chmod(0o600)

    def schema18_uncertain_attempt(self):
        self.schema18_database()
        connection = sqlite3.connect(self.paths.database)
        connection.execute("PRAGMA foreign_keys=ON")
        descriptor = self.module.domain_schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        endpoint_id = "end_status_uncertain_0001"
        binding_id = "brg_status_uncertain_0001"
        attempt_id = "att_status_uncertain_0001"
        event_key = "event-status-uncertain"
        payload = "continue exact turn"
        connection.execute(
            """
            INSERT INTO endpoints(
              endpoint_id,endpoint_key,endpoint_kind,source_kind,source_json,
              ref_version,incarnation,security_domain_id,instance_uid,
              workspace_id,persona_id,authorized_owners_json,
              authorized_owners_hash,policy_generation,state,next_lease_fence
            ) VALUES(?,?,'detached_native','headless_run','{}',3,1,?,?,
                     'T12345678','primary','["U12345678"]',?,1,'ready',1)
            """,
            (
                endpoint_id,
                "status-uncertain-endpoint",
                descriptor.security_domain_id,
                os.geteuid(),
                descriptor.authorized_owners_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO endpoint_authorized_owners(
              endpoint_id,security_domain_id,owner_user_id
            ) VALUES(?,?,'U12345678')
            """,
            (endpoint_id, descriptor.security_domain_id),
        )
        connection.execute(
            """
            INSERT INTO thread_bindings(
              binding_id,endpoint_id,security_domain_id,team_id,channel_id,
              thread_ts,owner_user_id,idempotency_key,request_hash,generation,state
            ) VALUES(?, ?,?,'T12345678','C12345678','1786690136.400269',
                     'U12345678','status-root',?,1,'active')
            """,
            (
                binding_id,
                endpoint_id,
                descriptor.security_domain_id,
                hashlib.sha256(b"status-root").hexdigest(),
            ),
        )
        connection.execute(
            """
            INSERT INTO queued_turns(
              event_key,binding_id,binding_generation,ordered_at,mutation_kind,
              payload_inline,payload_sha256,payload_bytes,state
            ) VALUES(?,?,1,CURRENT_TIMESTAMP,'create',?,?,?,'ready')
            """,
            (
                event_key,
                binding_id,
                payload,
                hashlib.sha256(payload.encode()).hexdigest(),
                len(payload.encode()),
            ),
        )
        connection.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,1,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint_id),
        )
        connection.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,1,'detached_native',?,?,?,'prepared')
            """,
            (
                attempt_id,
                endpoint_id,
                binding_id,
                "submit-status",
                hashlib.sha256(b"submit-status").hexdigest(),
                hashlib.sha256(b"reply-status").hexdigest(),
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,1)
            """,
            (attempt_id, event_key, binding_id),
        )
        connection.execute(
            """
            UPDATE native_attempts
            SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            "UPDATE native_attempts SET state='uncertain' WHERE attempt_id=?",
            (attempt_id,),
        )
        connection.commit()
        connection.close()

    def reasons(self, status):
        return {condition["reason_code"] for condition in status["conditions"]}

    def test_schema17_runtime_is_healthy_but_migration_fails_closed(self):
        self.schema17_database()
        status = self.module.schema_status(self.paths, config=self.config)

        self.assertTrue(status["ok"])
        self.assertTrue(status["runtime_ready"])
        self.assertFalse(status["migration_ready"])
        self.assertEqual(status["database_schema_version"], 17)
        self.assertEqual(status["runtime_schema_version"], 17)
        self.assertIn("target_runtime_capability_missing", self.reasons(status))
        self.assertRegex(status["logical_manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(status["installed_manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertIn("schema_mutation_unavailable", self.reasons(status))

    def test_schema18_refuses_schema17_runtime_and_exposes_domain_snapshot(self):
        self.schema18_database()
        incompatible = self.module.schema_status(self.paths, config=self.config)
        self.assertFalse(incompatible["ok"])
        self.assertIn("runtime_schema_incompatible", self.reasons(incompatible))

        compatible = self.module.schema_status(
            self.paths,
            config=self.config,
            runtime_schema_version=18,
        )
        self.assertTrue(compatible["ok"])
        self.assertTrue(compatible["runtime_ready"])
        self.assertFalse(compatible["migration_ready"])
        self.assertEqual(compatible["domain"]["summary"]["condition_count"], 0)

    def test_domain_blocker_is_part_of_top_level_readiness(self):
        self.schema18_uncertain_attempt()
        status = self.module.schema_status(
            self.paths,
            config=self.config,
            runtime_schema_version=18,
        )
        self.assertFalse(status["runtime_ready"])
        self.assertFalse(status["ok"])
        self.assertFalse(status["domain"]["summary"]["ready"])
        native = [
            condition
            for condition in status["conditions"]
            if condition["category"] == "native_execution"
        ]
        self.assertEqual(len(native), 1)
        self.assertFalse(native[0]["operator_resolvable"])
        self.assertEqual(native[0]["allowed_actions"], [])

    def test_incomplete_receipt_and_descriptor_are_typed_blockers(self):
        self.schema17_database()
        self.paths.receipt.parent.mkdir(parents=True, mode=0o700)
        self.paths.receipt.write_text(
            "{\"version\":1,\"receipt_id\":\"schema-op-1\","
            "\"operation\":\"migrate\",\"phase\":\"db_committed\","
            "\"from_schema\":17,\"to_schema\":18}\n",
            encoding="utf-8",
        )
        self.paths.receipt.chmod(0o600)
        incomplete_config = self.module.bridge_runtime.Config()

        status = self.module.schema_status(self.paths, config=incomplete_config)
        reasons = self.reasons(status)
        self.assertFalse(status["ok"])
        self.assertFalse(status["runtime_ready"])
        self.assertFalse(status["migration_ready"])
        self.assertIn("schema_operation_incomplete", reasons)
        self.assertIn("security_domain_descriptor_incomplete", reasons)
        self.assertEqual(status["active_receipt"]["phase"], "db_committed")
        self.assertNotIn(str(self.paths.database), str(status))

    def test_group_readable_database_is_rejected_before_sqlite_open(self):
        self.schema17_database()
        self.paths.database.chmod(0o640)
        status = self.module.schema_status(self.paths, config=self.config)
        self.assertFalse(status["runtime_ready"])
        self.assertIn("database_file_unsafe", self.reasons(status))

    def test_trivial_manifest_and_symlinked_database_parent_fail_closed(self):
        self.schema17_database()
        self.paths.install_manifest.write_text(
            "# tether-manifest-v2\n",
            encoding="utf-8",
        )
        status = self.module.schema_status(
            self.paths,
            config=self.config,
            runtime_schema_version=18,
        )
        self.assertFalse(status["migration_ready"])
        self.assertFalse(status["migration_capabilities"]["managed_install_verified"])
        self.assertIn("installed_manifest_unavailable", self.reasons(status))

        real_parent = self.home / "real-hermes"
        real_parent.mkdir(mode=0o700)
        symlink_parent = self.home / "linked-hermes"
        symlink_parent.symlink_to(real_parent, target_is_directory=True)
        linked_database = symlink_parent / "bridges.db"
        connection = sqlite3.connect(real_parent / "bridges.db")
        connection.execute("PRAGMA user_version=17")
        connection.close()
        (real_parent / "bridges.db").chmod(0o600)
        linked_paths = self.module.SchemaPaths(
            database=linked_database,
            receipt=self.paths.receipt,
            install_manifest=self.paths.install_manifest,
        )
        linked = self.module.schema_status(linked_paths, config=self.config)
        self.assertFalse(linked["runtime_ready"])
        self.assertIn("database_file_unsafe", self.reasons(linked))

    def test_manifest_cannot_self_declare_unsafe_or_nonexecuting_modes(self):
        self.schema17_database()
        runtime_home = self.home / ".local" / "share" / "tether"
        orchestrator = runtime_home / "schema_orchestrator.py"
        launcher = self.home / ".local" / "bin" / "tether"

        def rewrite_mode(target, old_mode, new_mode):
            self.paths.install_manifest.write_text(
                self.paths.install_manifest.read_text(encoding="utf-8").replace(
                    f"{target}\t{old_mode:o}\t",
                    f"{target}\t{new_mode:o}\t",
                ),
                encoding="utf-8",
            )
            target.chmod(new_mode)

        rewrite_mode(orchestrator, 0o600, 0o666)
        unsafe = self.module.schema_status(self.paths, config=self.config)
        self.assertFalse(unsafe["migration_capabilities"]["managed_install_verified"])
        self.assertIn("installed_manifest_unavailable", self.reasons(unsafe))

        rewrite_mode(orchestrator, 0o666, 0o600)
        rewrite_mode(launcher, 0o700, 0o600)
        nonexecuting = self.module.schema_status(self.paths, config=self.config)
        self.assertFalse(
            nonexecuting["migration_capabilities"]["managed_install_verified"]
        )
        self.assertIn("installed_manifest_unavailable", self.reasons(nonexecuting))

    def test_json_status_redacts_unsafe_config_path_without_traceback(self):
        real_config_home = self.home / "real-config"
        config_directory = real_config_home / "tether"
        config_directory.mkdir(parents=True, mode=0o700)
        config_file = config_directory / "config.toml"
        config_file.write_text("config_version = 1\n", encoding="utf-8")
        config_file.chmod(0o600)
        linked_config_home = self.home / "linked-config"
        linked_config_home.symlink_to(real_config_home, target_is_directory=True)
        environment = {
            **os.environ,
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_CONFIG_HOME": str(linked_config_home),
            "XDG_DATA_HOME": str(self.home / ".local" / "share"),
            "XDG_STATE_HOME": str(self.home / ".local" / "state"),
        }
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "runtime" / "schema_orchestrator.py"),
                "status",
                "--json",
            ],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["code"], "config_file_unsafe")
        self.assertNotIn("Traceback", result.stderr)
        self.assertNotIn(str(self.home), result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
