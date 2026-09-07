import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "precap" / "SKILL.md"


class PrecapPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: precap$")
        self.assertRegex(text, r"(?m)^description: .+")

    def test_referenced_files_ship(self):
        text = SKILL.read_text()
        refs = set(re.findall(r"(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+", text))
        self.assertTrue(refs)
        for relative in refs:
            self.assertTrue((SKILL.parent / relative).exists(), relative)

    def test_no_duplicate_skill_payload(self):
        self.assertEqual(list(ROOT.glob("**/SKILL.md")), [SKILL])

    def test_modes_and_phases_are_present(self):
        text = SKILL.read_text()
        for mode in ("**write**", "**check**", "**revise**"):
            self.assertIn(mode, text)
        phases = re.findall(r"(?m)^## Phase (\d)", text)
        self.assertEqual(phases, ["0", "1", "2", "3"])
        self.assertIn("## Check mode", text)
        self.assertIn("## Revise mode", text)

    def test_grounding_and_checkpoint_are_mandatory(self):
        text = SKILL.read_text()
        self.assertIn("No `Grounded in:`, no step.", text)
        self.assertIn("`Checkpoint:`", text)
        self.assertIn("[unverified]", text)

    def test_user_owned_forks_are_asked_not_decided(self):
        text = SKILL.read_text()
        self.assertIn("`AskUserQuestion`", text)
        self.assertIn("A fork that depends on the user is asked, not decided.", text)

    def test_template_sections_match_validator(self):
        import importlib.util

        import sys

        spec = importlib.util.spec_from_file_location("precap_script", SKILL.parent / "scripts" / "precap.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules["precap_script"] = module
        spec.loader.exec_module(module)
        text = SKILL.read_text()
        for name in module.SECTIONS:
            self.assertIn(f"**{name}**", text, name)


if __name__ == "__main__":
    unittest.main()
