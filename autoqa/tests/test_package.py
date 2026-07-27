import re
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "autoqa" / "SKILL.md"
VALIDATOR = SKILL.parent / "scripts" / "validate_report.py"


class AutoqaPackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: autoqa$")
        self.assertRegex(text, r"(?m)^description: .+")

    def test_referenced_files_ship(self):
        text = SKILL.read_text()
        refs = set(re.findall(r"(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+", text))
        self.assertTrue(refs)
        for relative in refs:
            self.assertTrue((SKILL.parent / relative).exists(), relative)

    def test_no_duplicate_skill_payload(self):
        self.assertEqual(list(ROOT.glob("**/SKILL.md")), [SKILL])

    def test_phases_are_ordered_and_gated(self):
        text = SKILL.read_text()
        phases = re.findall(r"(?m)^## Phase (\d)", text)
        self.assertEqual(phases, ["0", "1", "2", "3", "4"])
        self.assertEqual(text.count("Done when:"), 5)

    def test_autoqa_config_is_baseline_and_diff_cases_are_additive(self):
        text = SKILL.read_text()
        self.assertIn("reusable **baseline**", text)
        self.assertIn("**Diff inventory**", text)
        self.assertIn("committed, staged, unstaged", text)
        self.assertIn("**Diff cases are additive.**", text)

    def test_scope_is_confirmed_with_structured_multiselect(self):
        text = SKILL.read_text()
        self.assertIn("use `AskUserQuestion`", text)
        self.assertIn("`multiSelect: true`", text)
        self.assertIn("Changed behavior + seams (Recommended)", text)
        self.assertIn("Stateful/destructive cases", text)

    def test_autonomous_live_e2e_rules_are_hard_requirements(self):
        text = SKILL.read_text()
        self.assertIn("authoritative instance", text)
        self.assertIn("Required DEEP means live E2E", text)
        self.assertIn("Chrome DevTools", text)
        self.assertIn("fixture triggers a synthesis attempt", text)
        self.assertIn("scripts/validate_report.py", text)

    def test_validator_accepts_complete_report_and_rejects_required_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "autoqa-evidence"
            evidence.mkdir()
            (evidence / "cleanup.txt").write_text("cleanup complete\n")
            (evidence / "health.txt").write_text("HTTP 200\n")
            report = root / "autoqa-report.md"
            report.write_text(self._report("PASS", "SHIP"))
            accepted = subprocess.run(
                ["python3", str(VALIDATOR), str(report), "--expected-target", "a" * 40],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)

            report.write_text(self._report("UNTESTED", "SHIP"))
            rejected = subprocess.run(
                ["python3", str(VALIDATOR), str(report)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("required row", rejected.stderr)

            (evidence / "fixture.txt").write_text("fixture attempt failed: HTTP 503\n")
            report.write_text(
                self._report("UNTESTED", "BLOCKED").replace(
                    "autoqa-evidence/health.txt",
                    "autoqa-evidence/fixture.txt",
                )
            )
            blocked = subprocess.run(
                ["python3", str(VALIDATOR), str(report)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(blocked.returncode, 0, blocked.stderr)

            report.write_text(
                self._report("PASS", "SHIP").replace(
                    f"**Deployed commit:** {'a' * 40}",
                    f"**Deployed commit:** {'b' * 40}",
                )
            )
            mismatch = subprocess.run(
                ["python3", str(VALIDATOR), str(report)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(mismatch.returncode, 0)
            self.assertIn("target mismatch", mismatch.stderr)

            report.write_text(
                self._report("PASS", "SHIP")
                .replace("**Chrome DevTools required:** NO", "**Chrome DevTools required:** YES")
                .replace("| API | GET /health |", "| UI | /health |")
            )
            missing_devtools = subprocess.run(
                ["python3", str(VALIDATOR), str(report)],
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(missing_devtools.returncode, 0)
            self.assertIn("DevTools visual and network/console", missing_devtools.stderr)

    @staticmethod
    def _report(result: str, verdict: str) -> str:
        sha = "a" * 40
        return f"""# autoqa report

**Target repo:** /tmp/repo
**Target instance:** http://localhost
**Target commit:** {sha}
**Deployed commit:** {sha}
**Target verified:** YES
**Selected scope:** changed behavior
**Chrome DevTools required:** NO
**Cleanup:** PASS
**Cleanup witness:** autoqa-evidence/cleanup.txt
**Verdict:** {verdict}

| # | Source | Feature | Required | Disposition | Depth | Modality | Entry point | Check | Result | Witness |
|---|--------|---------|----------|-------------|-------|----------|-------------|-------|--------|---------|
| 1 | BASE | Health | YES | SMOKE | SMOKE | API | GET /health | 200 | {result} | autoqa-evidence/health.txt |
"""


if __name__ == "__main__":
    unittest.main()
