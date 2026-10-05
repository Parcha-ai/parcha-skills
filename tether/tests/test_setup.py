"""Configuration completion is independent from live readiness when requested."""
from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
import os
from pathlib import Path
import stat
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1] / "skills/tether/scripts/tether_notify.py"
SPEC = importlib.util.spec_from_file_location("offline_setup_tests", PATH)
notifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(notifier)


class SetupTest(unittest.TestCase):
    def run_configured_setup(self, *, setup_exit=0, peer_exit=0):
        events = []
        output, errors = io.StringIO(), io.StringIO()
        def hermes(argv, *_args, **_kwargs):
            self.assertEqual(argv, ["FIXTURE_HERMES", "gateway", "setup"])
            events.append("gateway_setup")
            return SimpleNamespace(returncode=setup_exit)
        with patch.object(notifier, "_find_hermes", return_value="FIXTURE_HERMES"), \
             patch.object(notifier, "_snapshot_setup", return_value={"fixture": True}), \
             patch.object(notifier, "_plan_tether_setup", return_value=None), \
             patch.object(notifier, "_apply_plugin_transition", side_effect=lambda *_a: events.append("plugin_config") or 0), \
             patch.object(notifier, "_configure_peer_agents", side_effect=lambda *_a: events.append("peer_config") or peer_exit), \
             patch.object(notifier, "_run_hermes", side_effect=hermes), \
             patch.object(notifier, "doctor", side_effect=AssertionError("No live readiness probe is permitted")), \
             patch.object(notifier, "_restore_setup", side_effect=lambda *_a: events.append("restore") or True), \
             redirect_stdout(output), redirect_stderr(errors):
            code = notifier.run_setup(SimpleNamespace(non_interactive=False, no_restart=True))
        return code, events, output.getvalue()

    def test_no_restart_preserves_configuration_without_live_gateway(self):
        code, events, text = self.run_configured_setup()
        self.assertEqual(code, 0)
        self.assertEqual(events, ["plugin_config", "peer_config", "gateway_setup"])
        self.assertIn("configuration is saved", text)
        self.assertIn("Start or restart", text)
        self.assertIn("tether doctor", text)
        self.assertNotIn("Tether is ready", text)

    def test_gateway_configuration_failure_still_rolls_back_without_restart(self):
        code, events, _text = self.run_configured_setup(setup_exit=7)
        self.assertEqual(code, 7)
        self.assertEqual(events, ["plugin_config", "peer_config", "gateway_setup", "restore"])

    def test_peer_configuration_failure_still_rolls_back_without_restart(self):
        code, events, _text = self.run_configured_setup(peer_exit=5)
        self.assertEqual(code, 5)
        self.assertEqual(events, ["plugin_config", "peer_config", "restore"])


class WorkspaceSetupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-setup-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.path = self.base / "config/tether/config.toml"
        environment = patch.dict(os.environ, {"HOME": str(self.base), "XDG_CONFIG_HOME": str(self.base / "config"),
                                             "TETHER_HERDR": "off", "TETHER_TEAM_CONFIG": ""})
        environment.start()
        self.addCleanup(environment.stop)
        self.args = SimpleNamespace(non_interactive=False, no_restart=True, team_id="T012ABCDEF")

    def write(self, data, mode=0o640):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(data)
        self.path.chmod(mode)

    def run_setup(self, command=None):
        events = []
        def hermes(argv, *_a, **_kw):
            events.append(argv[1:])
            return SimpleNamespace(returncode=command(argv) if command else 0)
        with patch.object(notifier, "_find_hermes", return_value="FIXTURE_HERMES"), \
             patch.object(notifier, "_snapshot_setup", return_value={"config": {}, "plugins": {},
                 "config_mutations": [], "plugin_mutations": []}), \
             patch.object(notifier, "_apply_plugin_transition", return_value=0), \
             patch.object(notifier, "_configure_peer_agents", return_value=0), \
             patch.object(notifier, "_run_hermes", side_effect=hermes), \
             patch.object(notifier, "doctor", side_effect=AssertionError("No live readiness is allowed")), \
             redirect_stdout(io.StringIO()) as output, redirect_stderr(io.StringIO()):
            code = notifier.run_setup(self.args)
        self.output = output.getvalue()
        return code, events

    def test_fresh_workspace_is_enabled_and_readable_by_actual_runtime(self):
        code, events = self.run_setup()
        self.assertEqual((code, events), (0, [["gateway", "setup"]]))
        self.assertEqual(tomllib.loads(self.path.read_text()), {"active": True, "team_id": "T012ABCDEF"})
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        # Load actual runtime settings only; do not register/start the plugin.
        root = PATH.parents[3] / "runtime/plugin_next"
        name = "offline_setup_runtime"
        spec = importlib.util.spec_from_file_location(name, root / "__init__.py", submodule_search_locations=[str(root)])
        plugin = importlib.util.module_from_spec(spec)
        sys.modules[name] = plugin
        try:
            spec.loader.exec_module(plugin)
            # Hermes's Slack setup owns the operator allowlist. Preserve that
            # separate prerequisite instead of inventing a default operator.
            with patch.dict(os.environ, {"SLACK_ALLOWED_USERS": "UFIXTURE"}, clear=True):
                self.assertTrue(plugin.active_module.load_active_settings(self.path).enabled)
                self.assertTrue(plugin.load_settings(self.path).configured)
                self.assertEqual(plugin.load_settings(self.path).workspace_id, "T012ABCDEF")
            with patch.dict(os.environ, {}, clear=True):
                self.assertFalse(plugin.load_settings(self.path).configured)
        finally:
            for key in list(sys.modules):
                if key == name or key.startswith(name + "."):
                    sys.modules.pop(key)

    def test_existing_workspace_comments_and_unrelated_settings_are_preserved(self):
        original = (b'# user configuration\r\nactive = false# keep this\r\nteam_id = "TEXISTING" # workspace\r\n'
                    b'team_config = ""\r\nnotes = """\r\nactive = false\r\n"""\r\n'
                    b'[custom]\r\nactive = false\r\nteam_id = "private-value"\r\n')
        self.write(original)
        self.args.team_id = None
        code, _ = self.run_setup()
        self.assertEqual(code, 0)
        self.assertEqual(self.path.read_bytes(), original.replace(b'active = false# keep this', b'active = true# keep this'))

    def test_quoted_workspace_replacement_preserves_only_real_comment(self):
        self.write(b"'active' = false # keep\n\"team_id\" = 'invalid#value' # workspace\n")
        self.assertEqual(self.run_setup()[0], 0)
        self.assertEqual(self.path.read_bytes(), b"'active' = true # keep\n\"team_id\" = \"T012ABCDEF\" # workspace\n")

    def test_missing_or_invalid_workspace_fails_before_hermes_discovery(self):
        for value in (None, "", "lowercase", "T1", "T" + "A" * 32):
            with self.subTest(value=value), patch.object(notifier, "_find_hermes", side_effect=AssertionError("Hermes must not be touched")), \
                 redirect_stderr(io.StringIO()) as error:
                self.args.team_id = value
                self.assertEqual(notifier.run_setup(self.args), 2)
                self.assertIn("--team-id", error.getvalue())
                self.assertFalse(self.path.exists())

    def test_invalid_toml_oversized_and_symlink_config_are_not_modified(self):
        for original in (b'team_id = [\n', b'#' + b'x' * 65536):
            with self.subTest(original_length=len(original)):
                self.write(original)
                self.assertEqual(self.run_setup()[0], 2)
                self.assertEqual(self.path.read_bytes(), original)
        self.path.unlink()
        target = self.base / "target.toml"
        target.write_bytes(b'team_id = "TEXISTING"\n')
        self.path.symlink_to(target)
        self.assertEqual(self.run_setup()[0], 2)
        self.assertTrue(self.path.is_symlink())
        self.assertEqual(target.read_bytes(), b'team_id = "TEXISTING"\n')

    def test_workspace_is_saved_before_restart_and_failure_restores_exact_bytes_mode(self):
        original = b'# preserve me\nteam_id = "TEXISTING"\nactive = false\n'
        self.write(original, 0o640)
        self.args.no_restart = False
        def command(argv):
            if argv[1:] == ["gateway", "restart"]:
                config = tomllib.loads(self.path.read_text())
                self.assertEqual(config, {"team_id": "T012ABCDEF", "active": True})
                return 9
            if argv[1:] == ["gateway", "install"]:
                return 8
            return 0
        self.assertEqual(self.run_setup(command)[0], 8)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o640)

    def test_failed_fresh_gateway_start_removes_owned_new_file(self):
        self.args.no_restart = False
        def command(argv):
            return 0 if argv[1:] == ["gateway", "setup"] else 7
        self.assertEqual(self.run_setup(command)[0], 7)
        self.assertFalse(self.path.exists())

    def test_failure_before_config_apply_preserves_original(self):
        original = b'active = false\nteam_id = "TEXISTING"\n'
        self.write(original)
        self.assertEqual(self.run_setup(lambda _argv: 5)[0], 5)
        self.assertEqual(self.path.read_bytes(), original)

    def test_concurrent_edit_before_apply_is_preserved(self):
        plan = notifier._plan_tether_setup(self.args)
        newer = b'active = false\nteam_id = "TNEWER"\n'
        self.write(newer)
        with self.assertRaisesRegex(RuntimeError, "changed during"):
            notifier._apply_tether_setup(plan)
        self.assertEqual(self.path.read_bytes(), newer)

    def test_concurrent_edit_after_apply_is_preserved_on_rollback(self):
        plan = notifier._plan_tether_setup(self.args)
        notifier._apply_tether_setup(plan)
        newer = self.path.read_bytes() + b'# edited by operator\n'
        self.write(newer)
        with self.assertRaisesRegex(RuntimeError, "changed after"):
            notifier._restore_tether_setup(plan)
        self.assertEqual(self.path.read_bytes(), newer)

    def test_interrupt_immediately_after_atomic_replace_rolls_back(self):
        for original in (None, b'team_id = "TEXISTING"\n'):
            with self.subTest(original=original):
                if self.path.exists():
                    self.path.unlink()
                if original is not None:
                    self.write(original, 0o640)
                plan = notifier._plan_tether_setup(self.args)
                atomic = notifier._atomic_config
                def interrupted(*args):
                    atomic(*args)
                    raise KeyboardInterrupt
                with patch.object(notifier, "_atomic_config", side_effect=interrupted), self.assertRaises(KeyboardInterrupt):
                    notifier._apply_tether_setup(plan)
                notifier._restore_tether_setup(plan)
                self.assertEqual(self.path.read_bytes() if self.path.exists() else None, original)

    def test_write_failure_before_replace_needs_no_restoration(self):
        plan = notifier._plan_tether_setup(self.args)
        with patch.object(notifier, "_atomic_config", side_effect=OSError("fixture write failure")), self.assertRaises(OSError):
            notifier._apply_tether_setup(plan)
        notifier._restore_tether_setup(plan)
        self.assertFalse(self.path.exists())

    def test_manifest_only_without_workspace_does_not_claim_configured_runtime(self):
        self.args.non_interactive = True
        self.args.team_id = None
        self.assertEqual(self.run_setup(), (0, [["slack", "manifest", "--write"]]))
        self.assertFalse(self.path.exists())
        self.assertIn("tether setup --team-id <workspace-id>", self.output)
        self.assertNotIn("tether doctor", self.output)

    def test_manifest_with_workspace_applies_only_after_manifest_success(self):
        self.args.non_interactive = True
        self.assertEqual(self.run_setup(lambda _argv: 6)[0], 6)
        self.assertFalse(self.path.exists())
        self.assertEqual(self.run_setup()[0], 0)
        self.assertTrue(tomllib.loads(self.path.read_text())["active"])
        self.assertIn("start or restart", self.output)
        self.assertIn("tether doctor", self.output)

    def test_tilde_config_home_matches_runtime_config_resolution(self):
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": "~/user-config"}):
            self.assertEqual(notifier._tether_config_path(), self.base / "user-config/tether/config.toml")


if __name__ == "__main__":
    unittest.main()
