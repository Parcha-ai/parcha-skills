import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "blast-radius" / "SKILL.md"


class BlastRadiusPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: blast-radius$")
        self.assertRegex(text, r"(?m)^description: .+")

    def test_referenced_files_ship(self):
        text = SKILL.read_text()
        refs = set(re.findall(r"(?<!https:)(?<!http:)(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+", text))
        for relative in refs:
            self.assertTrue((SKILL.parent / relative).exists(), relative)

    def test_no_duplicate_skill_payload(self):
        self.assertEqual(list(ROOT.glob("**/SKILL.md")), [SKILL])

    def test_readme_has_install_lines(self):
        readme = (ROOT / "README.md").read_text()
        self.assertIn("https://skills.sh/miguelrios/unc-skills/blast-radius", readme)
        self.assertIn("npx skills add miguelrios/unc-skills --skill blast-radius", readme)

    def test_is_invocation_only_and_proves_by_running_code(self):
        text = SKILL.read_text()
        self.assertRegex(text, r"(?m)^disable-model-invocation: true$")
        self.assertIn("## How sure are you", text)
        self.assertIn("4. You ran it.", text)
        self.assertIn("The one fact it's safe because of.", text)
        self.assertIn("Write it through `unslop`", text)


if __name__ == "__main__":
    unittest.main()
