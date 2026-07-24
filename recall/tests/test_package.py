import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "recall" / "SKILL.md"
BRAIN = SKILL.parent / "references" / "central-brain.md"
ENGINE = SKILL.parent / "scripts" / "recall.py"

# Brain-only surface: each token must be documented in central-brain.md and
# must NOT appear in SKILL.md, so central concepts never creep back into the
# default local experience. RECALL_URL and client.json are deliberately
# allowed in SKILL.md — the intro names them as the trigger for reading the
# central-brain reference.
BRAIN_ONLY_TOKENS = (
    "connector-supervisor",
    "connector-registry",
    "export-inbox",
    "recall-brain ",
    "recall_capture",
    "recall_forget",
    "RECALL_WRITE_SOURCE_ID",
    "RECALL_EXPORT_SOURCE_ID",
    "RECALL_TOKEN_FILE",
    "RECALL_SHADOW_LOG",
    "--source-alias",
    "--allow-local-index",
    "grep-ai-sync",
    "privacy-preview",
    "Hosted brain",
)


class RecallPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: recall$")
        self.assertRegex(text, r"(?m)^description: .+")

    def test_referenced_files_ship(self):
        for doc in (SKILL, BRAIN):
            text = doc.read_text()
            refs = set(re.findall(r"(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+", text))
            for relative in refs:
                self.assertTrue((SKILL.parent / relative).exists(), f"{doc.name}: {relative}")
        skill_refs = SKILL.read_text()
        self.assertIn("references/central-brain.md", skill_refs)
        self.assertIn("references/query-cookbook.md", skill_refs)

    def test_no_duplicate_skill_payload(self):
        self.assertEqual(list(ROOT.glob("**/SKILL.md")), [SKILL])

    def test_skill_is_local_first(self):
        skill = SKILL.read_text()
        brain = BRAIN.read_text()
        for token in BRAIN_ONLY_TOKENS:
            self.assertNotIn(token, skill, f"brain-only token leaked into SKILL.md: {token}")
            self.assertIn(token, brain, f"brain token missing from central-brain.md: {token}")

    def test_skill_pins_load_bearing_lines(self):
        skill = SKILL.read_text()
        self.assertIn("pi's own session format is not yet", skill)
        self.assertIn(
            "read [references/central-brain.md](references/central-brain.md)\nbefore running commands",
            skill,
        )
        self.assertIn("## Upgrade: central Recall Brain (optional)", skill)
        self.assertIn("## No index yet? Search anyway", skill)
        self.assertIn('rg -l -i "<terms>" ~/.claude/projects ~/.codex/sessions', skill)

    def test_doctor_advertises_upgrade(self):
        self.assertIn(
            "central Recall Brain available as an upgrade; see references/central-brain.md",
            ENGINE.read_text(),
        )

    def test_brain_reference_stands_alone(self):
        brain = BRAIN.read_text()
        self.assertIn("RECALL_MODE=local", brain)
        self.assertIn("client.json", brain)
        self.assertIn("Standing rule", brain)


if __name__ == "__main__":
    unittest.main()
