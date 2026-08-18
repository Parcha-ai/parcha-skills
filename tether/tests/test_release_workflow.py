import json
import pathlib
import re
import unittest


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPOSITORY_ROOT / "tether"


class ReleaseWorkflowTest(unittest.TestCase):
    def test_marketplace_ref_is_an_immutable_commit(self) -> None:
        marketplace = json.loads(
            (PACKAGE_ROOT / ".agents/plugins/marketplace.json").read_text()
        )
        plugin = next(row for row in marketplace["plugins"] if row["name"] == "tether")
        self.assertRegex(plugin["source"]["ref"], r"^[0-9a-f]{40}$")

    def test_publish_and_github_write_have_separate_credentials(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github/workflows/tether-release.yml"
        ).read_text()
        npm_section = workflow.split("\n  npm-publish:", 1)[1].split(
            "\n  github-release:", 1
        )[0]
        github_section = workflow.split("\n  github-release:", 1)[1]

        self.assertIn("id-token: write", npm_section)
        self.assertNotIn("contents: write", npm_section)
        self.assertNotIn("GH_TOKEN:", npm_section)
        self.assertIn("contents: write", github_section)
        self.assertIn("GH_TOKEN:", github_section)

    def test_prerelease_classification_and_dist_tag_are_verified(self) -> None:
        workflow = (
            REPOSITORY_ROOT / ".github/workflows/tether-release.yml"
        ).read_text()
        self.assertIn('npm_dist_tag=${prerelease ? "next" : "latest"}', workflow)
        self.assertIn('--tag "$NPM_DIST_TAG"', workflow)
        self.assertIn('"dist-tags.$NPM_DIST_TAG"', workflow)
        self.assertIn("flags+=(--prerelease)", workflow)
        self.assertIn("--json isDraft,isPrerelease,tagName", workflow)

    def test_runtime_contract_matches_ci(self) -> None:
        package = json.loads((PACKAGE_ROOT / "package.json").read_text())
        self.assertEqual(
            package["engines"]["node"],
            "^22.0.0 || ^24.0.0",
        )
        workflow = (REPOSITORY_ROOT / ".github/workflows/tether-ci.yml").read_text()
        for version in ("22", "24"):
            self.assertRegex(workflow, rf'node:\s*"{version}"')
        for version in ("18", "20"):
            self.assertNotRegex(workflow, rf'node:\s*"{version}"')
        for version in ("3.11", "3.12", "3.13", "3.14"):
            self.assertRegex(workflow, rf'python:\s*"{re.escape(version)}"')
        self.assertIn('"bandit==1.8.6"', workflow)
        self.assertNotRegex(workflow, r"pip install(?:[^\n]*\s)bandit(?:\s|$)")
        self.assertGreaterEqual(
            workflow.count("npm ci --ignore-scripts --no-audit --no-fund"),
            2,
        )
        release_workflow = (
            REPOSITORY_ROOT / ".github/workflows/tether-release.yml"
        ).read_text()
        self.assertIn("Refuse stale release toolchain pins", release_workflow)
        self.assertIn(
            "https://nodejs.org/dist/index.json",
            release_workflow,
        )
        self.assertIn("npm view npm version --json", release_workflow)

    def test_repository_portability_job_runs_the_intended_suites(self) -> None:
        workflow = (REPOSITORY_ROOT / ".github/workflows/tether-ci.yml").read_text()
        portability = workflow.split("\n  repository-portability:", 1)[1].split(
            "\n  arm64:", 1
        )[0]

        self.assertIn(
            "python -m unittest discover -s tests -p 'test_portability.py' -v",
            portability,
        )
        self.assertRegex(
            portability,
            r"- run: npm test\s+working-directory: tether",
        )
        self.assertIn("timeout-minutes: 30", portability)

    def test_installer_rejects_root_before_environment_paths(self) -> None:
        installer = (PACKAGE_ROOT / "install.sh").read_text()
        guard = installer.index("if (( EUID == 0 ))")
        self.assertLess(guard, installer.index('DATA_HOME="${XDG_DATA_HOME'))
        self.assertLess(guard, installer.index('HERMES_HOME="${HERMES_HOME'))
        self.assertLess(guard, installer.index("find_hermes()"))
        self.assertNotIn("require('$ROOT_DIR", installer)

    def test_tarball_includes_installer(self) -> None:
        package = json.loads((PACKAGE_ROOT / "package.json").read_text())
        self.assertIn("install.sh", package["files"])

    def test_slack_protocol_is_packaged_installed_and_compiled(self) -> None:
        package = json.loads((PACKAGE_ROOT / "package.json").read_text())
        installer = (PACKAGE_ROOT / "install.sh").read_text()
        ci = (REPOSITORY_ROOT / ".github/workflows/tether-ci.yml").read_text()
        release = (
            REPOSITORY_ROOT / ".github/workflows/tether-release.yml"
        ).read_text()

        self.assertIn("runtime/slack_protocol.py", package["files"])
        self.assertIn(
            '"$ROOT_DIR/runtime/slack_protocol.py" "$RUNTIME_HOME/slack_protocol.py"',
            installer,
        )
        self.assertIn("runtime/slack_protocol.py", package["scripts"]["test"])
        self.assertIn(
            "python -m py_compile tether/runtime/slack_protocol.py",
            ci,
        )
        self.assertIn(
            'python -m py_compile "$installed/runtime/slack_protocol.py"',
            release,
        )
        self.assertIn("runtime/domain_schema.py", package["files"])
        self.assertIn(
            '"$ROOT_DIR/runtime/domain_schema.py" "$RUNTIME_HOME/domain_schema.py"',
            installer,
        )
        self.assertIn("runtime/domain_schema.py", package["scripts"]["test"])
        self.assertIn("python -m py_compile tether/runtime/domain_schema.py", ci)
        self.assertIn(
            'python -m py_compile "$installed/runtime/domain_schema.py"',
            release,
        )


if __name__ == "__main__":
    unittest.main()
