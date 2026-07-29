"""Static tests for Recall's agent-facing interaction contract."""
from pathlib import Path
import unittest


SKILL = Path(__file__).resolve().parents[1] / "skills/recall/SKILL.md"
BRAIN = Path(__file__).resolve().parents[1] / "skills/recall/references/central-brain.md"


class RecallSkillContractTest(unittest.TestCase):
    def test_skill_defaults_local_and_routes_brain_setup_to_reference(self):
        """Plain installs are local with zero prompts; the Brain setup
        question lives in the central-brain reference and fires only when the
        user deliberately configures a Brain connection."""
        instructions = SKILL.read_text(encoding="utf-8")

        for required in (
            "read [references/central-brain.md](references/central-brain.md)",
            "Otherwise everything below is fully local and nothing\ntouches a network.",
            "## No index yet? Search anyway",
        ):
            with self.subTest(required=required):
                self.assertIn(required, instructions)

        for banned in ("Hosted brain", "Where should Recall search?"):
            with self.subTest(banned=banned):
                self.assertNotIn(banned, instructions)

    def test_brain_setup_requires_native_mode_question_and_verified_exit(self):
        contract = BRAIN.read_text(encoding="utf-8")

        for required in (
            "`AskUserQuestion`",
            "`request_user_input`",
            "Hosted brain (Recommended)",
            "Local-only",
            "Where should Recall search?",
            "never on plain skill install",
            "python3 scripts/recall.py doctor",
            "OK remote",
            "never falls back to SQLite",
        ):
            with self.subTest(required=required):
                self.assertIn(required, contract)


if __name__ == "__main__":
    unittest.main()
