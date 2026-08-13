from __future__ import annotations

import json
import plistlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from client.cli import parser
from client.macos_utility import (
    SOURCE_SPECS,
    MacUtilityError,
    apply_route,
    disable_source,
    mac_status,
    pause_source,
    reset_local_source,
    resume_source,
    revoke_source,
    route_info,
    support_report,
)
from collector.collector import Collector


class MacUtilityLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name) / "adversarial-private-home-CANARY"
        self.prefix = self.home / "private-install-CREDENTIAL-CANARY"
        self.agents = self.home / "private-agents-PATH-CANARY"
        (self.prefix / "state").mkdir(parents=True)
        self.agents.mkdir(parents=True)

    def _state(self, name: str, metadata: dict[str, str]) -> None:
        path = self.prefix / "state" / SOURCE_SPECS[name].spool_name
        connection = sqlite3.connect(path)
        connection.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        connection.executemany("INSERT INTO meta(key,value) VALUES (?,?)", metadata.items())
        connection.commit()
        connection.close()

    def _enable(self, name: str) -> None:
        spec = SOURCE_SPECS[name]
        with (self.agents / f"{spec.label}.plist").open("wb") as output:
            plistlib.dump({"ProgramArguments": [
                "synthetic-runtime", "-m", "client.cli", "synthetic-command",
                "--privacy-mode", "scrub",
            ]}, output)

    def test_status_is_closed_content_free_and_reports_health_lag_checkpoint(self) -> None:
        self.assertEqual(
            list(SOURCE_SPECS),
            [
                "claude-code", "codex", "cowork", "chatgpt-export",
                "imessage", "whatsapp", "selected-text", "safari", "chrome",
                "apple-notes", "hermes",
            ],
        )
        self._enable("claude-code")
        self._enable("cowork")
        self._state("claude-code", {"last_scan_at": "190"})
        self._state("cowork", {
            "committed_cursor": json.dumps("opaque"),
            "last_success_epoch": "195",
            "last_error_code": "brain_unavailable",
        })

        result = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)

        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["mode"], "mac-status")
        self.assertEqual(result["source_classes"], list(SOURCE_SPECS))
        self.assertEqual(result["sources"]["claude-code"], {
            "enabled": True, "health": "ready", "lag_seconds": 10,
            "running_age_seconds": None, "last_ack_age_seconds": None,
            "pending_count": 0, "dead_letter_count": 0,
            "remediation": "none",
            "checkpointed": True, "state_present": True,
            "privacy_mode": "scrub", "surface": "claude-code-project-jsonl",
            "connector_id": "local.claude-code",
        })
        self.assertEqual(result["sources"]["cowork"], {
            "enabled": True, "health": "degraded", "lag_seconds": 5,
            "running_age_seconds": None, "last_ack_age_seconds": 5,
            "pending_count": 0, "dead_letter_count": 0,
            "remediation": "retry_brain",
            "checkpointed": True, "state_present": True,
            "privacy_mode": "scrub", "surface": "claude-cowork-project-jsonl",
            "connector_id": "local.cowork",
        })
        self.assertEqual(result["sources"]["codex"]["health"], "disabled")
        self.assertEqual(
            result["sources"]["codex"]["surface"],
            "chatgpt-codex-desktop-rollouts",
        )
        self.assertEqual(
            result["sources"]["imessage"]["surface"],
            "apple-imessage-read-only-snapshot",
        )
        self.assertEqual(
            result["sources"]["whatsapp"]["surface"],
            "whatsapp-selected-text-export",
        )
        self.assertEqual(
            result["sources"]["selected-text"]["surface"],
            "selected-markdown-obsidian-root",
        )
        self.assertEqual(
            result["sources"]["hermes"]["surface"],
            "hermes-session-schema-v22",
        )
        rendered = json.dumps(result, sort_keys=True)
        for forbidden in (str(self.home), "CANARY", "CREDENTIAL", "PATH"):
            self.assertNotIn(forbidden, rendered)

    def test_status_maps_symlink_corruption_and_private_errors_to_closed_codes(self) -> None:
        self._enable("claude-code")
        self._state("claude-code", {"last_scan_at": "190"})
        target = self.home / "outside-private-target"
        target.write_text("synthetic")
        spec = SOURCE_SPECS["codex"]
        (self.agents / f"{spec.label}.plist").symlink_to(target)
        (self.prefix / "state" / spec.spool_name).write_bytes(b"private malformed bytes CANARY")

        result = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)

        self.assertEqual(result["sources"]["codex"]["health"], "invalid_local_state")
        self.assertEqual(result["sources"]["claude-code"]["health"], "ready")
        self.assertNotIn("CANARY", json.dumps(result))
        self.assertNotIn(str(target), json.dumps(result))

        self._enable("cowork")
        (self.prefix / "state" / "cowork.paused").symlink_to(target)
        result = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)
        self.assertEqual(
            result["sources"]["cowork"]["health"], "invalid_local_state"
        )

    def test_codex_without_archive_root_is_degraded_and_actionable(self) -> None:
        self._enable("codex")
        self._state("codex", {
            "last_scan_at": "195",
            "last_success_epoch": "100",
            "last_scan_complete": "1",
        })

        result = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)

        value = result["sources"]["codex"]
        self.assertEqual(value["health"], "degraded")
        self.assertEqual(value["remediation"], "upgrade_archive_coverage")
        self.assertFalse(value["archive_root_configured"])
        self.assertEqual(value["lag_seconds"], 5)
        self.assertEqual(value["last_ack_age_seconds"], 100)

    def test_status_distinguishes_running_stalled_backfill_and_dead_letters(self) -> None:
        self._enable("claude-code")
        state = self.prefix / "state" / SOURCE_SPECS["claude-code"].spool_name
        self._state("claude-code", {
            "last_scan_at": "190",
            "last_scan_complete": "0",
            "running_started_epoch": "100",
        })

        running = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)
        self.assertEqual(running["sources"]["claude-code"]["health"], "running")
        self.assertEqual(
            running["sources"]["claude-code"]["running_age_seconds"],
            100,
        )

        stalled = mac_status(prefix=self.prefix, launch_agents=self.agents, now=500)
        self.assertEqual(stalled["sources"]["claude-code"]["health"], "stalled")
        self.assertEqual(
            stalled["sources"]["claude-code"]["remediation"],
            "restart_stalled_source",
        )

        connection = sqlite3.connect(state)
        connection.execute(
            "DELETE FROM meta WHERE key='running_started_epoch'"
        )
        connection.commit()
        connection.close()
        backfill = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)
        self.assertEqual(
            backfill["sources"]["claude-code"]["health"],
            "backfilling",
        )

        connection = sqlite3.connect(state)
        connection.execute("CREATE TABLE outbox(state TEXT NOT NULL)")
        connection.execute(
            "CREATE TABLE dead_letters(error_code TEXT NOT NULL)"
        )
        connection.executemany(
            "INSERT INTO outbox(state) VALUES (?)",
            [("pending",), ("pending",)],
        )
        connection.execute(
            "INSERT INTO dead_letters(error_code) VALUES ('synthetic_parse')"
        )
        connection.commit()
        connection.close()
        degraded = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)
        value = degraded["sources"]["claude-code"]
        self.assertEqual(value["health"], "degraded")
        self.assertEqual(value["pending_count"], 2)
        self.assertEqual(value["dead_letter_count"], 1)
        self.assertEqual(value["remediation"], "inspect_dead_letters")

    def test_status_reads_a_live_wal_without_mutating_the_collector(self) -> None:
        self._enable("claude-code")
        root = self.home / "claude-root"
        root.mkdir()
        collector = Collector(
            root=root,
            harness="claude",
            source_id="claude:mac:synthetic",
            spool_path=(
                self.prefix / "state" / SOURCE_SPECS["claude-code"].spool_name
            ),
            endpoint="http://127.0.0.1:1",
            token="synthetic",
        )
        collector._set_meta("running_started_epoch", "100")
        collector.db.commit()

        result = mac_status(prefix=self.prefix, launch_agents=self.agents, now=200)

        value = result["sources"]["claude-code"]
        self.assertEqual(value["health"], "running")
        self.assertEqual(value["running_age_seconds"], 100)
        self.assertEqual(collector.db.execute("PRAGMA query_only").fetchone()[0], 0)
        collector.close()

    def test_per_source_disable_removes_only_agent_and_preserves_all_state(self) -> None:
        for name in SOURCE_SPECS:
            self._enable(name)
            self._state(name, {"last_scan_at": "190"})
        before = {path.name: path.read_bytes() for path in (self.prefix / "state").iterdir()}

        result = disable_source("cowork", launch_agents=self.agents, no_load=True)

        self.assertEqual(result, {
            "schema_version": 1, "mode": "mac-disable", "source": "cowork",
            "enabled": False, "state_retained": True,
        })
        self.assertFalse((self.agents / f"{SOURCE_SPECS['cowork'].label}.plist").exists())
        self.assertTrue((self.agents / f"{SOURCE_SPECS['codex'].label}.plist").exists())
        after = {path.name: path.read_bytes() for path in (self.prefix / "state").iterdir()}
        self.assertEqual(after, before)

    def test_disable_uses_the_fixed_system_launchctl_binary(self) -> None:
        self._enable("cowork")
        with mock.patch("client.macos_utility.subprocess.run", return_value=mock.Mock(returncode=1)) as run:
            disable_source("cowork", launch_agents=self.agents)
        self.assertEqual(run.call_args_list[0].args[0][0], "/bin/launchctl")
        self.assertEqual(run.call_args_list[1].args[0][0], "/bin/launchctl")

    def test_pause_resume_and_route_rotation_retain_exact_configuration(self) -> None:
        spec = SOURCE_SPECS["codex"]
        path = self.agents / f"{spec.label}.plist"
        with path.open("wb") as output:
            plistlib.dump({
                "ProgramArguments": [
                    "runtime", "-m", "client.cli", "collect",
                    "--source-id", "codex:mac:synthetic",
                    "--principal-id", "owner",
                    "--keychain-service", "ai.parcha.recall",
                    "--keychain-account", "codex:mac:synthetic",
                    "--privacy-mode", "scrub",
                ],
                "EnvironmentVariables": {},
            }, output)
        original = path.read_bytes()

        paused = pause_source(
            "codex", prefix=self.prefix, launch_agents=self.agents, no_load=True,
        )
        self.assertFalse(paused["enabled"])
        self.assertEqual(path.read_bytes(), original)
        marker = self.prefix / "state" / "codex.paused"
        self.assertEqual(marker.stat().st_mode & 0o777, 0o600)
        status = mac_status(
            prefix=self.prefix, launch_agents=self.agents, now=200,
        )
        self.assertEqual(status["sources"]["codex"]["health"], "paused")
        self.assertFalse(status["sources"]["codex"]["enabled"])

        route = route_info("codex", launch_agents=self.agents)
        self.assertEqual(route, {
            "schema_version": 1,
            "mode": "mac-route-info",
            "source": "codex",
            "connector_id": "local.codex",
            "source_id": "codex:mac:synthetic",
            "keychain_service": "ai.parcha.recall",
            "keychain_account": "codex:mac:synthetic",
            "privacy_mode": "scrub",
        })
        applied = apply_route(
            "codex",
            launch_agents=self.agents,
            tenant_id="tenant:personal",
            principal_id="principal:owner",
        )
        self.assertTrue(applied["canonical_v2"])
        configured = plistlib.loads(path.read_bytes())
        expected_arguments = plistlib.loads(original)["ProgramArguments"]
        expected_arguments[
            expected_arguments.index("--principal-id") + 1
        ] = "principal:owner"
        self.assertEqual(configured["ProgramArguments"], expected_arguments)
        self.assertEqual(
            configured["EnvironmentVariables"],
            {
                "RECALL_CANONICAL_V2_ENABLED": "1",
                "RECALL_TENANT_ID": "tenant:personal",
                "RECALL_PRINCIPAL_ID": "principal:owner",
            },
        )
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        routed = path.read_bytes()
        resumed = resume_source(
            "codex", prefix=self.prefix, launch_agents=self.agents, no_load=True,
        )
        self.assertTrue(resumed["enabled"])
        self.assertFalse(marker.exists())
        self.assertEqual(path.read_bytes(), routed)

    def test_route_apply_rejects_invalid_authority_and_symlink(self) -> None:
        self._enable("codex")
        with self.assertRaisesRegex(MacUtilityError, "canonical_route_invalid"):
            apply_route(
                "codex",
                launch_agents=self.agents,
                tenant_id="../personal",
                principal_id="principal:owner",
            )
        path = self.agents / f"{SOURCE_SPECS['codex'].label}.plist"
        path.unlink()
        path.symlink_to(self.home / "outside.plist")
        with self.assertRaisesRegex(MacUtilityError, "invalid_launch_agent"):
            apply_route(
                "codex",
                launch_agents=self.agents,
                tenant_id="tenant:personal",
                principal_id="principal:owner",
            )

    def test_revoke_disables_source_deletes_only_its_keychain_reference(self) -> None:
        self._enable("cowork")
        path = self.agents / f"{SOURCE_SPECS['cowork'].label}.plist"
        with path.open("wb") as output:
            plistlib.dump({"ProgramArguments": [
                "runtime", "-m", "client.cli", "cowork-local-sync",
                "--keychain-service", "synthetic.service",
                "--keychain-account", "cowork:mac:synthetic",
                "--privacy-mode", "scrub",
            ]}, output)

        with mock.patch("client.mac.delete_keychain_token", return_value=True) as delete:
            result = revoke_source(
                "cowork", launch_agents=self.agents, no_load=True,
            )

        delete.assert_called_once_with("synthetic.service", "cowork:mac:synthetic")
        self.assertEqual(result, {
            "schema_version": 1, "mode": "mac-revoke", "source": "cowork",
            "enabled": False, "credential_revoked": True, "state_retained": True,
        })
        self.assertFalse(path.exists())

    def test_local_reset_requires_exact_confirmation_and_never_claims_central_delete(self) -> None:
        self._enable("cowork")
        self._state("cowork", {"last_scan_at": "190"})
        spool = self.prefix / "state" / SOURCE_SPECS["cowork"].spool_name
        for suffix in ("-wal", "-shm", ".stdout.log", ".stderr.log"):
            Path(str(spool) + suffix).write_text("PRIVATE-CANARY")
        untouched = self.prefix / "state" / SOURCE_SPECS["codex"].spool_name
        untouched.write_text("UNTOUCHED")

        with self.assertRaisesRegex(ValueError, "confirmation_mismatch"):
            reset_local_source(
                "cowork", prefix=self.prefix, launch_agents=self.agents,
                confirmation="codex", no_load=True,
            )
        result = reset_local_source(
            "cowork", prefix=self.prefix, launch_agents=self.agents,
            confirmation="cowork", no_load=True,
        )

        self.assertEqual(result, {
            "schema_version": 1, "mode": "mac-reset-local", "source": "cowork",
            "enabled": False, "local_state_retained": False,
            "central_evidence_retained": True,
        })
        self.assertFalse(spool.exists())
        self.assertFalse(any(Path(str(spool) + suffix).exists() for suffix in (
            "-wal", "-shm", ".stdout.log", ".stderr.log",
        )))
        self.assertEqual(untouched.read_text(), "UNTOUCHED")
        self.assertNotIn("CANARY", json.dumps(result))

    def test_support_report_is_content_free_and_reports_integrity_aggregates(self) -> None:
        self._enable("cowork")
        self._state("cowork", {"last_scan_at": "190"})
        manifest = {
            "format": "recall-macos-v2",
            "files": [{
                "path": "lib/client/synthetic.py", "type": "file",
                "bytes": 9,
                "sha256": __import__("hashlib").sha256(b"synthetic").hexdigest(),
            }],
        }
        (self.prefix / "lib" / "client").mkdir(parents=True)
        (self.prefix / "lib" / "client" / "synthetic.py").write_bytes(b"synthetic")
        (self.prefix / "MANIFEST.json").write_text(json.dumps(manifest))

        result = support_report(
            prefix=self.prefix, launch_agents=self.agents, now=200,
        )

        self.assertEqual(result["mode"], "mac-support")
        self.assertEqual(result["package_integrity"], {
            "status": "verified", "checked_files": 1, "mismatches": 0,
        })
        self.assertEqual(result["enabled"], 1)
        rendered = json.dumps(result, sort_keys=True)
        for forbidden in (str(self.home), "CANARY", "synthetic.py"):
            self.assertNotIn(forbidden, rendered)

    def test_cli_has_safe_defaults_and_closed_source_choices(self) -> None:
        cowork = parser().parse_args([
            "cowork-local-sync", "--endpoint", "https://brain.example.invalid:9443",
            "--source-id", "cowork:mac:synthetic", "--keychain-service", "synthetic",
            "--keychain-account", "cowork:mac:synthetic", "--root", "/synthetic/root",
            "--spool", "/synthetic/state.db",
        ])
        self.assertEqual(cowork.privacy_mode, "scrub")
        self.assertEqual(cowork.visibility, "private")
        status = parser().parse_args(["mac-status"])
        self.assertTrue(status.prefix.endswith("RecallBrain"))
        self.assertTrue(status.launch_agents.endswith("LaunchAgents"))
        disabled = parser().parse_args(["mac-disable", "--source", "cowork", "--no-load"])
        self.assertEqual(disabled.source, "cowork")
        revoked = parser().parse_args(["mac-revoke", "--source", "cowork", "--no-load"])
        self.assertEqual(revoked.source, "cowork")
        reset = parser().parse_args([
            "mac-reset-local", "--source", "cowork",
            "--confirm-source", "cowork", "--no-load",
        ])
        self.assertEqual(reset.confirm_source, "cowork")
        support = parser().parse_args(["mac-support"])
        self.assertTrue(support.prefix.endswith("RecallBrain"))
        with self.assertRaises(SystemExit):
            parser().parse_args([
                "cowork-local-sync", "--endpoint", "https://brain.example.invalid:9443",
                "--source-id", "cowork:mac:synthetic", "--keychain-service", "synthetic",
                "--keychain-account", "cowork:mac:synthetic", "--root", "/synthetic/root",
                "--spool", "/synthetic/state.db", "--privacy-mode", "off",
            ])


if __name__ == "__main__":
    unittest.main()
