import importlib.util
import re
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "cascade" / "SKILL.md"
TEMPLATES = SKILL.parent / "references" / "templates.md"
VALIDATOR = SKILL.parent / "scripts" / "validate_cascade.py"

SPEC = importlib.util.spec_from_file_location("validate_cascade", VALIDATOR)
validate_cascade = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(validate_cascade)


def fixture_chain() -> str:
    return textwrap.dedent(
        """\
        ---
        cascade_version: 2
        episode_id: example
        pacing: autonomous
        status: ACTIVE
        current_loop: L0
        authority: local edits only
        budget: two valid attempts
        human_gates: none
        target: repo; branch; abc123; Python 3.11
        ---

        # Example

        ## Current

        L0 is ready.

        ## Authority and budgets

        No external writes.

        ## Chain

        ### L0 — CHANGE

        - **goal:** Make one change.
        - **prompt:** Read the repository and implement the change.
        - **accept:** L0.1 passes with a falsifier.
        - **bound:** 2 valid attempts.
        - **at_bound ->** STOP.
        - **exit ->** COMPLETE.

        ## Invariants

        No advance without a receipt.
        """
    )


def fixture_exit(status: str = "COMPLETE", verdict: str = "PASS") -> str:
    return textwrap.dedent(
        f"""\
        ---
        cascade_version: 2
        episode_id: example
        loop: L0
        status: {status}
        target_head: abc123
        next: COMPLETE
        ---

        # L0 boundary receipt

        ## Bound accounting

        One valid attempt.

        ## Accept criteria → evidence

        | ID | Verdict | Evidence |
        |---|---|---|
        | L0.1 | {verdict} | `test` passed at abc123. |

        ## Evidence manifest

        | Criterion | Command/action | Runtime/environment | Target | Timestamp | Artifact/digest | Falsifier / negative | Cleanup / rollback |
        |---|---|---|---|---|---|---|---|
        | L0.1 | `test` | Python 3.11 | abc123 | 2026-08-13T00:00:00Z | trace.txt / digest | negative passed | revert commit |

        ## POST-ZEN

        N/A — no architecture change.

        ## Transition

        Follow the declared transition.
        """
    )


class CascadePackageTest(unittest.TestCase):
    def test_skill_has_required_frontmatter(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\n"))
        self.assertRegex(text, r"(?m)^name: cascade$")
        self.assertRegex(text, r"(?m)^description: .+")

    def test_referenced_files_ship(self):
        text = SKILL.read_text()
        refs = set(re.findall(r"(?:scripts|references|assets|agents)/[A-Za-z0-9_./-]+", text))
        self.assertTrue(refs)
        for relative in refs:
            self.assertTrue((SKILL.parent / relative).exists(), relative)

    def test_no_duplicate_skill_payload(self):
        self.assertEqual(list(ROOT.glob("**/SKILL.md")), [SKILL])

    def test_package_excludes_python_bytecode(self):
        result = subprocess.run(
            ["npm", "pack", "--dry-run", "--json"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertNotRegex(result.stdout, r"(?:__pycache__|\.py[co]\b)")

    def test_core_contract_is_present_and_concise(self):
        text = SKILL.read_text()
        for term in (
            "mutation authority",
            "at_bound ->",
            "COMPLETE",
            "AT_BOUND",
            "WAITING_HUMAN",
            "BLOCKED_EXTERNAL",
            "SUPERSEDED",
            "TAKEOVER",
            "native task",
            "POST-ZEN",
        ):
            self.assertIn(term, text)
        self.assertLessEqual(len(text.splitlines()), 180)

    def test_templates_do_not_teach_deferred_completion(self):
        text = TEMPLATES.read_text().lower()
        self.assertNotIn("complete (", text)
        self.assertNotIn("status line carries nuance", text)
        self.assertIn("never write “complete except”", text)

    def test_validator_accepts_v2_chain_and_complete_exit(self):
        self.assertEqual(validate_cascade.validate_chain(fixture_chain()), [])
        self.assertEqual(validate_cascade.validate_exit(fixture_exit()), [])

    def test_validator_rejects_fake_completion(self):
        errors = validate_cascade.validate_exit(fixture_exit(verdict="FAIL"))
        self.assertIn("COMPLETE requires every acceptance verdict to be PASS", errors)

        deferred = fixture_exit().replace(
            "| L0.1 | PASS | `test` passed at abc123. |",
            "| L0.1 | PASS | `test` passed at abc123. |\n"
            "| L0.2 | DEFERRED | moved to the next loop |",
        )
        errors = validate_cascade.validate_exit(deferred)
        self.assertIn("invalid acceptance verdict: DEFERRED", errors)
        self.assertIn("COMPLETE requires every acceptance verdict to be PASS", errors)

    def test_validator_checks_repair_loop_anatomy(self):
        repair = textwrap.dedent(
            """\
            ### R0 — LOCALIZED REPAIR

            - **prompt:** Repair only the diagnosed mechanism.
            - **accept:** R0.1 passes.
            - **bound:** 1 valid attempt.
            - **at_bound ->** STOP.
            - **exit ->** L0.

            """
        )
        chain = fixture_chain().replace("## Invariants", repair + "## Invariants")
        self.assertIn("R0 missing loop field: goal", validate_cascade.validate_chain(chain))

    def test_validator_enforces_bound_and_human_states(self):
        self.assertEqual(validate_cascade.validate_exit(fixture_exit("AT_BOUND", "FAIL")), [])
        self.assertEqual(validate_cascade.validate_exit(fixture_exit("WAITING_HUMAN", "WAIT")), [])
        self.assertTrue(validate_cascade.validate_exit(fixture_exit("AT_BOUND", "PASS")))
        self.assertTrue(validate_cascade.validate_exit(fixture_exit("WAITING_HUMAN", "FAIL")))


if __name__ == "__main__":
    unittest.main()
