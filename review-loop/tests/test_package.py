import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "review-loop" / "SKILL.md"


class ReviewLoopPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: review-loop$")
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
        self.assertIn("https://skills.sh/miguelrios/unc-skills/review-loop", readme)
        self.assertIn("npx skills add miguelrios/unc-skills --skill review-loop", readme)

    def test_github_only_and_every_reviewer(self):
        text = SKILL.read_text()
        self.assertRegex(text, r"(?m)^license: MIT$")
        for banned in ("glab", "GitLab", "Perforce", "p4 "):
            self.assertNotIn(banned, text)
        self.assertIn("Do not filter to one reviewer", text)
        self.assertIn("## Greptile specifics", text)
        self.assertIn("## Devin specifics", text)
        self.assertIn("## Human threads", text)
        self.assertIn("resolveReviewThread", text)
        self.assertIn("`isResolved: true`", text)
        self.assertIn("`@greptile-apps review`", text)
        self.assertIn("Prompt to fix all with AI", text)
        self.assertIn("A draft PR never gets a Devin review", text)
        self.assertIn("Never dismiss a human review", text)
        self.assertIn("never mints", text)

    def test_bounded_iterations_timeout_and_report(self):
        text = SKILL.read_text()
        self.assertRegex(text, r"\| `--max-iterations N` \| no \| 3 \|")
        self.assertIn("## Stop on timeout", text)
        self.assertIn("Never continue\nwith stale or missing review results", text)
        for field in ("| Iterations |", "| Threads found (per reviewer) |", "| Resolved this run |", "| Remaining |", "| Final Greptile score |"):
            self.assertIn(field, text)
        self.assertIn("that is `autoqa`'s job", text)
        self.assertIn("that is `blast-radius`'s job", text)

    def test_reference_is_github_only(self):
        reference = (SKILL.parent / "references" / "graphql-queries.md").read_text()
        self.assertIn("resolveReviewThread", reference)
        self.assertNotIn("glab", reference)
        self.assertFalse((SKILL.parent / "references" / "gitlab-api.md").exists())
        self.assertIn("## Provenance", (ROOT / "README.md").read_text())


if __name__ == "__main__":
    unittest.main()
