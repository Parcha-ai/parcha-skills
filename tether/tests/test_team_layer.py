"""The team layer is one file, applied idempotently, persona preserved."""

from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load():
    spec = importlib.util.spec_from_file_location("tether_team", ROOT / "skills" / "tether" / "scripts" / "tether_team.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TeamLayerTest(unittest.TestCase):
    def test_apply_is_idempotent_and_keeps_the_persona(self):
        team = load()
        with tempfile.TemporaryDirectory() as temp:
            soul = pathlib.Path(temp) / "SOUL.md"
            soul.write_text(
                "# Someone\n\nI am Someone.\n\n- Slack mentions: address people and agents as <@USERID> old rule\n",
                encoding="utf-8",
            )
            self.assertEqual(team.apply(ROOT / "team" / "TEAM.md", soul), "installed")
            first = soul.read_text(encoding="utf-8")
            self.assertEqual(team.apply(ROOT / "team" / "TEAM.md", soul), "updated")
            self.assertEqual(soul.read_text(encoding="utf-8"), first)
            self.assertEqual(first.count(team.BEGIN), 1)
            self.assertIn("# Someone\n\nI am Someone.", first)
            self.assertNotIn("old rule", first)
            self.assertLess(first.index(team.END), first.index("# Someone"))

    def test_team_md_names_every_agent_with_a_real_mention(self):
        text = (ROOT / "team" / "TEAM.md").read_text(encoding="utf-8")
        for uid in ("U09450ZLS81", "U095AHX1QQL", "U0BJATRKZ6V", "U0BHY13623U", "U0BFC6ZRRQX", "U0BJN78RJD8", "U0A9TAX8MSA"):
            self.assertIn(f"<@{uid}>", text)
        self.assertIn("tether spawn", text)


if __name__ == "__main__":
    unittest.main()
