import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "verify-release" / "SKILL.md"


class VerifyReleasePackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: verify-release$")
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
        self.assertIn("https://skills.sh/Parcha-ai/parcha-skills/verify-release", readme)
        self.assertIn("npx skills add Parcha-ai/parcha-skills --skill verify-release", readme)

    def test_verdict_vocabulary_and_agents_manifest(self):
        text = SKILL.read_text()
        for verdict in ("`VERIFIED`", "`FAILED`", "`INCOMPLETE`"):
            self.assertIn(verdict, text)
        for state in ("`PASS`", "`FAIL`", "`BLOCKED`", "`NOT RUN`"):
            self.assertIn(state, text)
        self.assertIn("Verification alone does not authorize deployment", text)
        self.assertIn("Use AutoQA when available", text)
        self.assertTrue((SKILL.parent / "agents" / "openai.yaml").exists())


if __name__ == "__main__":
    unittest.main()
