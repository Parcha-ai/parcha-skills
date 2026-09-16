import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "unslop" / "SKILL.md"


class UnslopPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: unslop$")
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
        self.assertIn("https://skills.sh/Parcha-ai/parcha-skills/unslop", readme)
        self.assertIn("npx skills add Parcha-ai/parcha-skills --skill unslop", readme)

    def test_matches_upstream_body_and_carries_license(self):
        text = SKILL.read_text()
        self.assertRegex(text, r"(?m)^license: MIT$")
        self.assertNotIn("disable-model-invocation", text)
        numbered = re.findall(r"(?m)^(\d+)\. \*\*", text)
        self.assertEqual(numbered, [str(n) for n in range(1, 32)])
        self.assertIn("## Adding soul", text)
        self.assertTrue((ROOT / "LICENSE").read_text().startswith("MIT License"))
        self.assertIn("Lauren Tan", (ROOT / "LICENSE").read_text())
        self.assertIn("## Provenance", (ROOT / "README.md").read_text())


if __name__ == "__main__":
    unittest.main()
