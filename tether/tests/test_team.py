"""Portable team configuration and actual Hermes/native prompt integration."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "runtime" / "plugin_next"
NAME = "portable_team_test_plugin"
spec = importlib.util.spec_from_file_location(NAME, PLUGIN / "__init__.py", submodule_search_locations=[str(PLUGIN)])
plugin = importlib.util.module_from_spec(spec)
sys.modules[NAME] = plugin
spec.loader.exec_module(plugin)
team = plugin.team_module
active = plugin.active_module

MANIFEST = '''version = 1
self = "builder"
[[colleagues]]
id = "builder"
name = "Builder"
role = "Implement and verify changes"
slack_id = "U012ABCDEF"
computer = "claude-code"
projects = ["sample"]
[[colleagues]]
id = "reviewer"
name = "Reviewer"
role = "Review artifacts and contribute findings"
computer = "codex"
projects = ["sample"]
[[projects]]
id = "sample"
ref = "./sample-repository"
'''


class PromptHost:
    def __init__(self):
        self.sections = []

    def register_system_prompt_section(self, identifier, text, *, position):
        self.sections.append((identifier, text, position))


class TeamTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-team-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {"HOME": str(self.root), "XDG_CONFIG_HOME": str(self.root / "config"), "TETHER_HERDR": "off"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def write_manifest(self, name="team.toml", body=MANIFEST):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        return path

    def raw(self):
        import tomllib
        return tomllib.loads(MANIFEST)

    def test_unconfigured_default_has_no_private_roster(self):
        manifest = team.load_team(self.root / "missing-config.toml")
        self.assertEqual(manifest, team.TeamManifest())
        text = team.render_team(manifest)
        self.assertIn("human request or direct question", text)
        self.assertIn("evidence outside their usual roles", text)
        self.assertLessEqual(len(text), team.MAX_PROMPT_CHARS)
        for private in ("Parcha", "Claudio", "Miguel", "greppy", "<@U0B"):
            self.assertNotIn(private, text)

    def test_unrelated_tether_settings_do_not_select_a_team(self):
        config = self.root / "config.toml"
        config.write_text('active = false\nteam_id = "TEXAMPLE"\n', encoding="utf-8")
        self.assertEqual(team.load_team(config), team.TeamManifest())

    def test_selected_identity_roles_mentions_and_projects_are_rendered(self):
        manifest = team.load_manifest(self.write_manifest())
        self.assertEqual(manifest.selected.id, "builder")
        self.assertEqual(manifest.selected.projects, ("sample",))
        self.assertEqual(manifest.projects[0].ref, "./sample-repository")
        text = team.render_team(manifest)
        self.assertIn("You are Builder (builder)", text)
        self.assertIn("<@U012ABCDEF>", text)
        self.assertIn("Reviewer (reviewer)", text)
        self.assertIn("computer preference: claude-code", text)
        self.assertIn("not runtime selection, access grants", text)
        self.assertIn("sample: ./sample-repository", text)

    def test_relative_team_config_resolves_from_config_parent(self):
        manifest_path = self.write_manifest("settings/teams/team.toml")
        config = self.root / "settings" / "config.toml"
        config.write_text('team_config = "teams/team.toml"\n', encoding="utf-8")
        loaded = team.load_team(config)
        self.assertEqual(loaded.source, manifest_path)
        self.assertEqual(loaded.selected.name, "Builder")

    def test_environment_override_wins_even_over_invalid_main_config(self):
        target = self.write_manifest()
        config = self.root / "config.toml"
        config.write_text("not valid TOML!", encoding="utf-8")
        loaded = team.load_team(config, env={"TETHER_TEAM_CONFIG": str(target)})
        self.assertEqual(loaded.source, target)

    def test_environment_relative_path_uses_cwd(self):
        self.write_manifest()
        previous = Path.cwd()
        try:
            os.chdir(self.root)
            loaded = team.load_team(env={"TETHER_TEAM_CONFIG": "team.toml"})
            self.assertEqual(loaded.selected.name, "Builder")
        finally:
            os.chdir(previous)

    def test_tilde_path_is_supported(self):
        target = self.write_manifest()
        loaded = team.load_team(env={"TETHER_TEAM_CONFIG": "~/team.toml"})
        self.assertEqual(loaded.source, target)

    def test_injected_environment_home_controls_tilde_expansion(self):
        target = self.write_manifest("other-home/team.toml")
        loaded = team.load_team(env={"HOME": str(target.parent), "TETHER_TEAM_CONFIG": "~/team.toml"})
        self.assertEqual(loaded.source, target)

    def test_default_config_location_honors_environment(self):
        self.write_manifest("settings/team.toml")
        config = self.root / "settings" / "tether" / "config.toml"
        config.parent.mkdir()
        config.write_text('team_config = "../team.toml"\n', encoding="utf-8")
        loaded = team.load_team(env={"XDG_CONFIG_HOME": str(self.root / "settings")})
        self.assertEqual(loaded.selected.name, "Builder")

    def test_explicit_missing_file_errors_instead_of_changing_identity(self):
        with self.assertRaisesRegex(team.TeamConfigError, "cannot read file.*path and permissions"):
            team.load_team(env={"TETHER_TEAM_CONFIG": str(self.root / "missing.toml")})
        config = self.root / "config.toml"
        config.write_text('team_config = "missing.toml"\n', encoding="utf-8")
        with self.assertRaises(team.TeamConfigError):
            active.load_active_settings(config)

    def test_invalid_syntax_encoding_and_empty_override_are_actionable(self):
        for content in (b"version = [", b"\xff"):
            path = self.root / "broken.toml"
            path.write_bytes(content)
            with self.assertRaisesRegex(team.TeamConfigError, "invalid UTF-8 TOML.*syntax"):
                team.load_manifest(path)
        with self.assertRaisesRegex(team.TeamConfigError, "TETHER_TEAM_CONFIG.*non-empty"):
            team.load_team(env={"TETHER_TEAM_CONFIG": ""})

    def test_unknown_keys_invalid_types_ids_and_references_are_rejected(self):
        cases = [
            (lambda raw: raw.update(api_key="not-a-credential"), "unsupported field"),
            (lambda raw: raw.update(version=True), "version: expected 1"),
            (lambda raw: raw.update(version=2), "version: expected 1"),
            (lambda raw: raw.update(self="missing"), "self: must match"),
            (lambda raw: raw.update(colleagues={}), "use \\[\\[colleagues\\]\\]"),
            (lambda raw: raw["colleagues"][0].update(name=7), "name.*non-empty string"),
            (lambda raw: raw["colleagues"][0].update(id="bad id"), "lowercase ID"),
            (lambda raw: raw["colleagues"][0].update(role="line\n"), "surrounding whitespace"),
            (lambda raw: raw["colleagues"][0].update(slack_id="<@U012ABCDEF>"), "Slack user ID"),
            (lambda raw: raw["colleagues"][0].update(projects="sample"), "array of project IDs"),
            (lambda raw: raw["colleagues"][0].update(projects=["unknown"]), "unknown project ID"),
            (lambda raw: raw["colleagues"][0].update(projects=["sample", "sample"]), "duplicate project IDs"),
            (lambda raw: raw["colleagues"].append(raw["colleagues"][0].copy()), "duplicate colleague ID"),
            (lambda raw: raw["colleagues"][1].update(slack_id="U012ABCDEF"), "already assigned"),
            (lambda raw: raw["projects"].append(raw["projects"][0].copy()), "duplicate project ID"),
            (lambda raw: raw["projects"][0].update(secret="no"), "unsupported field"),
        ]
        for change, message in cases:
            with self.subTest(message=message):
                raw = self.raw()
                change(raw)
                with self.assertRaisesRegex(team.TeamConfigError, message):
                    team.parse_team(raw)

    def test_roster_does_not_guess_self_or_select_runtime(self):
        raw = self.raw()
        del raw["self"]
        raw["colleagues"][0]["computer"] = "my-custom-computer"
        loaded = team.parse_team(raw)
        self.assertIsNone(loaded.selected)
        self.assertNotIn("Your configured identity", team.render_team(loaded))
        self.assertIn("my-custom-computer", team.render_team(loaded))

    def test_oversized_prompt_is_rejected_without_silent_truncation(self):
        raw = {"colleagues": [{"id": f"colleague{i}", "name": "Example", "role": "x" * 200} for i in range(15)]}
        with self.assertRaisesRegex(team.TeamConfigError, "4000.*Shorten"):
            team.parse_team(raw)

    def test_manifest_file_read_is_bounded(self):
        target = self.root / "large.toml"
        target.write_bytes(b" " * (team.MAX_MANIFEST_BYTES + 1))
        with self.assertRaisesRegex(team.TeamConfigError, "exceeds.*bytes"):
            team.load_manifest(target)

    def test_empty_team_config_and_packaged_config_are_neutral(self):
        config = self.root / "config.toml"
        config.write_text('team_config = ""\n', encoding="utf-8")
        self.assertEqual(team.load_team(config), team.TeamManifest())
        self.assertEqual(team.load_team(ROOT / "runtime" / "config.example.toml"), team.TeamManifest())

    def test_optional_version_defaults_to_one(self):
        raw = self.raw()
        del raw["version"]
        self.assertEqual(team.parse_team(raw).version, 1)

    def test_full_contract_is_identical_in_hermes_and_native_prompts(self):
        target = self.write_manifest()
        config = self.root / "config.toml"
        config.write_text(f'team_config = "{target}"\nlauncher = "direct"\n', encoding="utf-8")
        settings = active.load_active_settings(config)
        host = PromptHost()
        plugin._register_team_prompt_section(host, settings.team)
        section = host.sections[0][1]
        context = {"source": {"session_id": "native-example", "cwd": str(self.root)}, "turns": [{"payload_inline": json.dumps({"user": "U012ABCDEF", "text": "Explain the change in detail"})}]}
        with mock.patch.object(active, "runtime_truth", return_value="Example runtime"):
            native = active.compose_prompt(context, settings)
        self.assertIn(section, native)
        self.assertEqual(native.count(section), 1)
        self.assertIn("give more detail when the human requests it", native)
        self.assertLessEqual(len(section), team.MAX_PROMPT_CHARS)
        self.assertIn("You are Builder (builder)", section)
        self.assertEqual(settings.claude_binary, "claude")
        self.assertEqual(settings.codex_binary, "codex")

    def test_registration_refuses_invalid_manifest_before_host_hooks(self):
        missing = self.root / "missing.toml"
        with mock.patch.dict(os.environ, {"TETHER_TEAM_CONFIG": str(missing)}):
            with self.assertRaisesRegex(team.TeamConfigError, "cannot read file"):
                plugin.register(PromptHost())


if __name__ == "__main__":
    unittest.main()
