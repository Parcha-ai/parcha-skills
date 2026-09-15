import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "before-after" / "SKILL.md"


class BeforeAfterPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: before-after$")
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
        self.assertIn("https://skills.sh/miguelrios/unc-skills/before-after", readme)
        self.assertIn("npx skills add miguelrios/unc-skills --skill before-after", readme)

    def test_scripts_are_shell_only_and_public_host_is_gone(self):
        scripts = SKILL.parent / "scripts"
        files = sorted(p.relative_to(scripts).as_posix() for p in scripts.rglob("*") if p.is_file())
        self.assertEqual(
            files,
            ["adapters/blob.sh", "adapters/gist.sh", "adapters/github-branch.sh", "capture.sh", "upload-and-copy.sh"],
        )
        for path in scripts.rglob("*.sh"):
            self.assertTrue(path.read_text().startswith("#!/bin/bash"), path)
            self.assertNotIn("0x0.st", path.read_text(), path)
        self.assertIn('IMAGE_ADAPTER="${IMAGE_ADAPTER:-github-branch}"', (scripts / "upload-and-copy.sh").read_text())
        self.assertNotIn("--public", (scripts / "adapters" / "gist.sh").read_text())

    def test_github_branch_adapter_uses_plumbing_and_raw_urls(self):
        text = (SKILL.parent / "scripts" / "adapters" / "github-branch.sh").read_text()
        for needle in ("git commit-tree", "GIT_INDEX_FILE", "git write-tree", "git hash-object -w", "?raw=true", "pr-assets/$PR_NUMBER"):
            self.assertIn(needle, text)
        self.assertNotIn("git worktree", text)
        self.assertNotIn("git checkout", text)

    def test_skill_states_single_job_and_no_judgement(self):
        text = SKILL.read_text()
        self.assertRegex(text, r"(?m)^license: PolyForm-Shield-1.0.0$")
        self.assertIn("that is `autoqa`'s job", text)
        self.assertIn("that is `blast-radius`'s job", text)
        self.assertIn("Never switch git branches", text)
        self.assertIn("ask for the other side", text)
        self.assertIn("## Before / After", text)
        self.assertIn("main preview, staging, or", text)
        self.assertIn("PolyForm Shield", (ROOT / "LICENSE").read_text())
        self.assertIn("## Provenance", (ROOT / "README.md").read_text())

    def test_emitter_outputs_section_table_sources_and_sha(self):
        text = (SKILL.parent / "scripts" / "upload-and-copy.sh").read_text()
        self.assertIn('MARKDOWN="## Before / After', text)
        self.assertIn("| | Before | After |", text)
        self.assertIn("--before-url", text)
        self.assertIn("--after-url", text)
        self.assertIn("--after-sha", text)
        self.assertIn("git rev-parse HEAD", text)


if __name__ == "__main__":
    unittest.main()
