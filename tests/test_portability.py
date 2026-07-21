import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILLS = ("hands-free", "parable", "cascade", "recall", "recap", "tether", "desloppify")


def load_json(path: Path):
    return json.loads(path.read_text())


class PortablePackagingTest(unittest.TestCase):
    def test_canonical_skill_inventory_is_complete(self):
        found = {
            path.parent.name
            for path in ROOT.glob("*/skills/*/SKILL.md")
            if ".pytest_cache" not in path.parts
        }
        self.assertEqual(found, set(SKILLS))

    def test_claude_and_codex_marketplaces_list_same_plugins(self):
        claude = load_json(ROOT / ".claude-plugin" / "marketplace.json")
        codex = load_json(ROOT / ".agents" / "plugins" / "marketplace.json")
        claude_names = [entry["name"] for entry in claude["plugins"]]
        codex_names = [entry["name"] for entry in codex["plugins"]]
        self.assertEqual(claude_names, list(SKILLS))
        self.assertEqual(codex_names, list(SKILLS))

        for entry in codex["plugins"]:
            self.assertEqual(entry["source"]["source"], "local")
            self.assertEqual(entry["source"]["path"], f"./{entry['name']}")

    def test_every_package_has_native_claude_and_codex_manifests(self):
        claude_market = {
            entry["name"]: entry for entry in load_json(ROOT / ".claude-plugin" / "marketplace.json")["plugins"]
        }
        for name in SKILLS:
            claude = load_json(ROOT / name / ".claude-plugin" / "plugin.json")
            codex = load_json(ROOT / name / ".codex-plugin" / "plugin.json")
            self.assertEqual(claude["name"], name)
            self.assertEqual(codex["name"], name)
            self.assertEqual(claude["version"], claude_market[name]["version"])
            self.assertEqual(codex["version"], claude_market[name]["version"])
            self.assertEqual(codex["skills"], "./skills/")
            package_path = ROOT / name / "package.json"
            self.assertTrue(package_path.exists(), f"{name} is missing package.json")
            package = load_json(package_path)
            self.assertEqual(package["version"], claude_market[name]["version"])
            self.assertEqual(package["pi"]["skills"], ["./skills"])
            self.assertIn("pi-package", package["keywords"])

    def test_root_pi_manifest_exposes_every_canonical_skill(self):
        package = load_json(ROOT / "package.json")
        self.assertIn("pi-package", package["keywords"])
        self.assertEqual(package["bin"]["tether"], "./tether/bin/tether.js")
        paths = package["pi"]["skills"]
        self.assertEqual(paths, [f"./{name}/skills" for name in SKILLS])
        for relative in paths:
            skill_root = ROOT / relative
            self.assertTrue(skill_root.is_dir())
            self.assertTrue(any(skill_root.glob("*/SKILL.md")))

    def test_skill_local_references_exist(self):
        reference_pattern = re.compile(r"(?<!https:)(?<!http:)(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+")
        for skill_file in ROOT.glob("*/skills/*/SKILL.md"):
            for relative in set(reference_pattern.findall(skill_file.read_text())):
                self.assertTrue((skill_file.parent / relative).exists(), f"{skill_file}: missing {relative}")

    def test_codex_component_paths_are_relative_and_present(self):
        for name in SKILLS:
            manifest = load_json(ROOT / name / ".codex-plugin" / "plugin.json")
            for field in ("skills", "hooks", "mcpServers", "apps"):
                value = manifest.get(field)
                if not isinstance(value, str):
                    continue
                self.assertTrue(value.startswith("./"), f"{name}.{field} must start with ./")
                self.assertTrue((ROOT / name / value).exists(), f"{name}.{field} points to missing {value}")

    def test_playbooks_name_harness_differences_honestly(self):
        hands_free = (ROOT / "hands-free/skills/hands-free/SKILL.md").read_text()
        parable = (ROOT / "parable/skills/parable/SKILL.md").read_text()
        cascade = (ROOT / "cascade/skills/cascade/SKILL.md").read_text()
        recall = (ROOT / "recall/skills/recall/SKILL.md").read_text()
        recap = (ROOT / "recap/skills/recap/SKILL.md").read_text()
        tether = (ROOT / "tether/skills/tether/SKILL.md").read_text()
        desloppify = (ROOT / "desloppify/skills/desloppify/SKILL.md").read_text()

        self.assertIn("Claude Code, Codex, and pi", hands_free)
        self.assertIn("If it does not (notably stock pi)", parable)
        self.assertIn("stock pi has no background bash", cascade)
        self.assertIn("pi's own session format is not yet", recall)
        self.assertIn("Claude Code or Codex", recap)
        self.assertIn("Codex or Claude Code", tether)
        self.assertIn("active harness's isolated", desloppify)
        self.assertIn("pi", (ROOT / "desloppify/skills/desloppify/references/review-routing.md").read_text())
        self.assertIn("stock pi publishes as a headless run", (ROOT / "README.md").read_text())

    def test_skills_sh_install_docs_cover_every_skill(self):
        root_readme = (ROOT / "README.md").read_text()
        self.assertIn("https://skills.sh/miguelrios/unc-skills", root_readme)
        self.assertIn("npx skills add miguelrios/unc-skills", root_readme)
        self.assertIn("github:miguelrios/unc-skills#main tether setup", root_readme)

        for name in SKILLS:
            package_readme = (ROOT / name / "README.md").read_text()
            self.assertIn(f"https://skills.sh/miguelrios/unc-skills/{name}", package_readme)
            self.assertIn(f"npx skills add miguelrios/unc-skills --skill {name}", package_readme)

    def test_effective_comms_is_a_complete_managed_snippet(self):
        snippet = (ROOT / "snippets/effective-comms/AGENTS.md").read_text()
        readme = (ROOT / "snippets/effective-comms/README.md").read_text()

        self.assertTrue(snippet.startswith("<!-- effective-comms:start -->"))
        self.assertTrue(snippet.rstrip().endswith("<!-- effective-comms:end -->"))
        self.assertIn("ISO 24495-1:2023's relevant/findable/understandable/usable test", snippet)
        self.assertIn("W3C COGA clear-content, focus-recovery, and no-memory patterns", snippet)
        self.assertIn("JAN-style written instructions, checklists, and task separation", snippet)
        self.assertIn("Lead with the answer, result, or next action", snippet)
        self.assertIn("Default to brief, not incomplete", snippet)
        self.assertIn("Accuracy, safety, security, privacy", snippet)
        self.assertIn("After three failed iterations", snippet)
        self.assertIn("Before sending, check:", snippet)
        self.assertIn("without reconstructing prior turns", snippet)
        self.assertIn("does not claim certification or", readme)
        self.assertIn("[`effective-comms`](snippets/effective-comms/)", (ROOT / "README.md").read_text())


if __name__ == "__main__":
    unittest.main()
