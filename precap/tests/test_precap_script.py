import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "skills" / "precap" / "scripts" / "precap.py"

import sys

spec = importlib.util.spec_from_file_location("precap_script", SCRIPT)
precap = importlib.util.module_from_spec(spec)
sys.modules["precap_script"] = precap
spec.loader.exec_module(precap)


GOOD = """# Precap: add --json to greet

Written: 2026-09-07
Branch: feature/json
Base: main
Workdir: {workdir}

## Task

> add a --json flag to greet

Add a `--json` flag so `greet` prints a JSON object instead of text.

## End state

`greet --json alice` prints `{{"greeting": "hi alice"}}` and the existing text path is unchanged.

## Path

1. **Added the flag** Extended the argparse parser in `greet.py` with `--json`.
   - Grounded in: `greet.py:3` (parser definition)
   - Checkpoint: `python3 greet.py --help` lists `--json`
2. **Covered it** Added a JSON case to `test_greet.py`.
   - Grounded in: `test_greet.py:1`
   - Checkpoint: `python3 -m unittest test_greet` passes with 2 tests

## Footprint

- `greet.py` (modified): flag and JSON branch
- `test_greet.py` (modified): new case
- `docs/json.md` (new): usage note

## Verification

- `python3 -m unittest test_greet`: 2 tests, OK

## Forks

- **Output shape**: A) flat object / B) nested under `result`. Decided by: match the existing CLI in this repo, which is flat.

## Not done

- Pretty-printing: out of scope, nobody asked.

## Assumptions

- [verified] argparse is already used, read in `greet.py`.
- [unverified] downstream scripts do not parse the text output.

## Drift signals

- If you find yourself refactoring the parser into a module, you have left the path: revert to a flag on the existing parser.

## Revisions

"""


def make_repo(tmp: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=tmp, check=True)
    subprocess.run(["git", "config", "user.email", "t@example.com"], cwd=tmp, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=tmp, check=True)
    (tmp / "greet.py").write_text("import argparse\n\nparser = argparse.ArgumentParser()\n")
    (tmp / "test_greet.py").write_text("import unittest\n")
    (tmp / "docs").mkdir()
    (tmp / "docs" / "keep").write_text("")
    subprocess.run(["git", "add", "-A"], cwd=tmp, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=tmp, check=True)
    subprocess.run(["git", "checkout", "-q", "-b", "feature/json"], cwd=tmp, check=True)


class PrecapScriptTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name).resolve()
        make_repo(self.tmp)
        self.precap_path = self.tmp / "precap.md"
        self.precap_path.write_text(GOOD.format(workdir=self.tmp))

    def tearDown(self):
        self.tmpdir.cleanup()

    def run_cli(self, *args):
        return subprocess.run(
            ["python3", str(SCRIPT), *args], capture_output=True, text=True, cwd=self.tmp
        )

    def test_template_validates_structurally_but_fails_on_placeholders(self):
        template = precap.parse(precap.TEMPLATE)
        self.assertEqual([s for s in template.order if s in precap.SECTIONS], precap.SECTIONS)
        errors = precap.validate(template, self.tmp)
        self.assertTrue(errors)
        self.assertTrue(any("placeholder" in e or "missing header" in e for e in errors))

    def test_good_precap_validates(self):
        result = self.run_cli("validate", str(self.precap_path), "--json")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        receipt = json.loads(result.stdout)
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["steps"], 2)
        self.assertEqual(receipt["footprint"], 3)

    def test_missing_section_fails(self):
        text = self.precap_path.read_text().replace("## Drift signals", "## Signals")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing section: ## Drift signals", result.stdout)

    def test_step_without_grounding_fails(self):
        text = self.precap_path.read_text().replace("   - Grounded in: `test_greet.py:1`\n", "")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("Path step 2 has no 'Grounded in:'", result.stdout)

    def test_untagged_assumption_fails(self):
        text = self.precap_path.read_text().replace("- [unverified] downstream", "- downstream")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("Assumption needs exactly one", result.stdout)

    def test_footprint_contradicting_tree_fails(self):
        text = self.precap_path.read_text().replace("`docs/json.md` (new)", "`docs/json.md` (modified)")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("does not exist", result.stdout)

    def test_drift_reports_touched_pending_unpredicted_within_scope(self):
        (self.tmp / "greet.py").write_text("import argparse\n\nparser = argparse.ArgumentParser()\nparser.add_argument('--json')\n")
        (self.tmp / "docs" / "other.md").write_text("surprise\n")
        (self.tmp / "unrelated").mkdir()
        (self.tmp / "unrelated" / "noise.txt").write_text("noise\n")
        result = self.run_cli("drift", str(self.precap_path), "--json")
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["scope"], ["docs/", "greet.py", "test_greet.py"])
        self.assertEqual(report["touched"], ["greet.py"])
        self.assertEqual(report["pending"], ["docs/json.md", "test_greet.py"])
        self.assertEqual(report["unpredicted"], ["docs/other.md"])
        self.assertEqual(report["out_of_scope"], 1)
        self.assertNotIn("precap.md", report["unpredicted"])

    def test_drift_all_and_scope_flags(self):
        (self.tmp / "unrelated").mkdir()
        (self.tmp / "unrelated" / "noise.txt").write_text("noise\n")
        report = json.loads(self.run_cli("drift", str(self.precap_path), "--json", "--all").stdout)
        self.assertIn("unrelated/noise.txt", report["unpredicted"])
        self.assertEqual(report["scope"], [])
        report = json.loads(self.run_cli("drift", str(self.precap_path), "--json", "--scope", "unrelated/").stdout)
        self.assertEqual(report["unpredicted"], ["unrelated/noise.txt"])
        self.assertEqual(report["scope"], ["unrelated/"])

    def test_grounding_that_does_not_exist_fails(self):
        text = self.precap_path.read_text().replace("`greet.py:3` (parser definition)", "`nope/greet.py:3`")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("grounding `nope/greet.py:3` does not exist", result.stdout)

    def test_grounding_line_out_of_range_fails(self):
        text = self.precap_path.read_text().replace("`greet.py:3`", "`greet.py:2-40`")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("out of range (3 lines)", result.stdout)

    def test_grounding_commands_and_urls_are_not_treated_as_paths(self):
        text = self.precap_path.read_text().replace(
            "`greet.py:3` (parser definition)",
            "`grep -n argparse greet.py`, `https://example.com/docs.html`, `greet.py:3`",
        )
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path), "--json")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_wrapped_fork_and_bare_dotted_tokens_pass(self):
        text = self.precap_path.read_text().replace(
            "- **Output shape**: A) flat object / B) nested under `result`. Decided by: match the existing CLI in this repo, which is flat.",
            "- **Output shape**: A) flat object / B) nested under `result`.\n  Decided by: the user (undecided)",
        ).replace("`greet.py:3` (parser definition)", "`package.json` key `scripts.test`, `greet.py:3`")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path), "--json")
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_fork_without_decided_by_fails(self):
        text = self.precap_path.read_text().replace(" Decided by: match the existing CLI in this repo, which is flat.", "")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("Fork needs a 'Decided by:'", result.stdout)

    def test_unresolvable_base_fails(self):
        text = self.precap_path.read_text().replace("Base: main\n", "Base: origin/does-not-exist\n")
        self.precap_path.write_text(text)
        result = self.run_cli("validate", str(self.precap_path))
        self.assertEqual(result.returncode, 1)
        self.assertIn("is not a commit git can resolve", result.stdout)

    def test_workdir_defaults_to_git_toplevel(self):
        text = self.precap_path.read_text().replace(f"Workdir: {self.tmp}\n", "")
        nested = self.tmp / "docs"
        nested_precap = nested / "precap.md"
        nested_precap.write_text(text)
        result = self.run_cli("validate", str(nested_precap), "--json")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(json.loads(result.stdout)["workdir"], str(self.tmp))

    def test_drift_sees_committed_changes_against_base(self):
        (self.tmp / "test_greet.py").write_text("import unittest\n# more\n")
        subprocess.run(["git", "commit", "-qam", "tests"], cwd=self.tmp, check=True)
        result = self.run_cli("drift", str(self.precap_path), "--json")
        report = json.loads(result.stdout)
        self.assertIn("test_greet.py", report["touched"])

    def test_drift_without_base_fails_closed(self):
        text = self.precap_path.read_text().replace("Base: main\n", "")
        self.precap_path.write_text(text)
        result = self.run_cli("drift", str(self.precap_path))
        self.assertEqual(result.returncode, 2)
        self.assertIn("no Base", result.stderr)


if __name__ == "__main__":
    unittest.main()
