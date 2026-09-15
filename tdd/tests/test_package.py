import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "tdd" / "SKILL.md"


class TddPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: tdd$")
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
        self.assertIn("https://skills.sh/miguelrios/unc-skills/tdd", readme)
        self.assertIn("npx skills add miguelrios/unc-skills --skill tdd", readme)

    def test_references_moved_under_references_dir(self):
        text = SKILL.read_text()
        self.assertIn("[references/tests.md](references/tests.md)", text)
        self.assertIn("[references/mocking.md](references/mocking.md)", text)
        self.assertNotIn("](tests.md)", text)
        self.assertNotIn("](mocking.md)", text)
        self.assertTrue((SKILL.parent / "agents" / "openai.yaml").exists())

    def test_seams_and_loop_rules(self):
        text = SKILL.read_text()
        self.assertIn("**Test only at pre-agreed seams.**", text)
        self.assertIn("**Red before green.**", text)
        self.assertIn("## Bug fixes: regression-first", text)


if __name__ == "__main__":
    unittest.main()
