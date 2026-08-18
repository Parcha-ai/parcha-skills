#!/usr/bin/env python3
"""Capture immutable source-to-installed provenance for the L0 baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess  # nosec B404 - fixed local Git invocation only
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parent
BEHAVIOR_SOURCE_COMMIT = "b1e8b36d8ba418142a3b30b376dd062df1df0d74"
DEPLOYED_MATCH_COMMIT = "0fa83fb3766ca95d3efec0185bb138f07f4fd4c7"
UPSTREAM_MAIN_AT_START = "547d8f5db48f99fbfe61ca6f918ee385fcdee30e"

ARTIFACTS = {
    "runtime/bridge_runtime.py": pathlib.Path.home() / ".local/share/tether/bridge_runtime.py",
    "runtime/hermes_compat.py": pathlib.Path.home() / ".local/share/tether/hermes_compat.py",
    "runtime/routing.py": pathlib.Path.home() / ".local/share/tether/routing.py",
    "runtime/security.py": pathlib.Path.home() / ".local/share/tether/security.py",
    "runtime/slack_protocol.py": pathlib.Path.home() / ".local/share/tether/slack_protocol.py",
    "runtime/plugin/__init__.py": pathlib.Path.home() / ".hermes/plugins/tether/__init__.py",
    "runtime/plugin/plugin.yaml": pathlib.Path.home() / ".hermes/plugins/tether/plugin.yaml",
    "bin/tether.js": pathlib.Path.home() / ".local/bin/tether",
    "skills/tether/scripts/tether_notify.py": pathlib.Path.home() / ".local/share/tether/tether_notify.py",
    "herdr-plugin/tether_plugin.py": pathlib.Path.home() / ".local/share/tether/herdr-plugin/tether_plugin.py",
}

EVIDENCE_SOURCES = [
    "docs/ADR-001-TETHER-REPLACEMENT.md",
    "evals/incident-corpus.json",
    "evals/capture_baseline_metrics.py",
    "evals/capture_provenance.py",
    "evals/run_cross_repo_contracts.py",
    "evals/run_incident_corpus.py",
    "evals/validate_blueprint.py",
    "package.json",
    "package-lock.json",
]


def git(*args: str) -> str:
    return subprocess.run(  # nosec B603 - arguments are fixed by this module
        ["/usr/bin/git", *args], cwd=REPOSITORY, text=True, capture_output=True, check=True
    ).stdout.strip()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_at(commit: str, relative: str) -> bytes:
    return subprocess.run(  # nosec B603 - commits and paths are module constants
        ["/usr/bin/git", "show", f"{commit}:tether/{relative}"],
        cwd=REPOSITORY,
        capture_output=True,
        check=True,
    ).stdout


def parse_int(pattern: str, source: str) -> int:
    match = re.search(pattern, source)
    if not match:
        raise RuntimeError(f"missing observed value for pattern {pattern}")
    return int(match.group(1))


def artifact_report(relative: str, installed: pathlib.Path) -> dict[str, Any]:
    current = ROOT / relative
    current_hash = sha256_bytes(current.read_bytes())
    installed_hash = sha256_bytes(installed.read_bytes()) if installed.is_file() else None
    behavior_hash = sha256_bytes(source_at(BEHAVIOR_SOURCE_COMMIT, relative))
    deployed_match_hash = sha256_bytes(source_at(DEPLOYED_MATCH_COMMIT, relative))
    return {
        "source": relative,
        "installed_location_class": (
            "hermes_plugin" if "/.hermes/plugins/" in str(installed) else "user_managed_runtime"
        ),
        "current_sha256": current_hash,
        "behavior_source_sha256": behavior_hash,
        "deployed_match_sha256": deployed_match_hash,
        "installed_sha256": installed_hash,
        "matches_current": installed_hash == current_hash,
        "matches_behavior_source": installed_hash == behavior_hash,
        "matches_deployed_match_commit": installed_hash == deployed_match_hash,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    artifacts = [artifact_report(relative, installed) for relative, installed in ARTIFACTS.items()]
    head = git("rev-parse", "HEAD")
    status = git("status", "--porcelain=v1", "--untracked-files=all")
    evidence_hashes = {
        path: sha256_bytes((ROOT / path).read_bytes()) for path in EVIDENCE_SOURCES
    }
    evidence_digest = sha256_bytes(
        "".join(f"{path}\0{value}\n" for path, value in sorted(evidence_hashes.items())).encode()
    )
    installed_runtime = ARTIFACTS["runtime/bridge_runtime.py"].read_text(encoding="utf-8")
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    hermes_repo = pathlib.Path.home() / ".hermes/hermes-agent"
    hermes_pyproject = (hermes_repo / "pyproject.toml").read_text(encoding="utf-8")
    hermes_version_match = re.search(r'^version\s*=\s*"([^"]+)"', hermes_pyproject, re.MULTILINE)
    hermes_commit = subprocess.run(  # nosec B603 - fixed local Git invocation
        ["/usr/bin/git", "rev-parse", "HEAD"], cwd=hermes_repo, text=True,
        capture_output=True, check=True
    ).stdout.strip()
    report = {
        "schema_version": 2,
        "repository": "Parcha-ai/parcha-skills",
        "candidate_commit": head,
        "candidate_tree": git("rev-parse", "HEAD^{tree}"),
        "candidate_evidence_content_sha256": evidence_digest,
        "evidence_source_sha256": evidence_hashes,
        "candidate_parent_main": UPSTREAM_MAIN_AT_START,
        "behavior_source_commit": BEHAVIOR_SOURCE_COMMIT,
        "second_matching_source_commit": DEPLOYED_MATCH_COMMIT,
        "merge_base_with_baseline_main": git("merge-base", head, UPSTREAM_MAIN_AT_START),
        "behavior_commit_is_ancestor": subprocess.run(  # nosec B603
            ["/usr/bin/git", "merge-base", "--is-ancestor", BEHAVIOR_SOURCE_COMMIT, head],
            cwd=REPOSITORY,
            check=False,
        ).returncode == 0,
        "candidate_clean": status == "",
        "candidate_status": status.splitlines(),
        "artifacts": artifacts,
        "all_installed_artifacts_traced": all(
            artifact["matches_current"]
            and artifact["matches_behavior_source"]
            and artifact["matches_deployed_match_commit"]
            for artifact in artifacts
        ),
        "observed": {
            "candidate_package_version": package["version"],
            "installed_schema": parse_int(r"SCHEMA_VERSION\s*=\s*(\d+)", installed_runtime),
            "installed_broker_protocol": parse_int(r'"protocol_version":\s*(\d+)', installed_runtime),
            "deployed_hermes_version": hermes_version_match.group(1) if hermes_version_match else None,
            "deployed_hermes_commit": hermes_commit,
        },
        "expected": {
            "tether_version": "0.3.0-beta.1",
            "schema": 17,
            "broker_protocol": 6,
            "hermes_version": "0.19.0",
            "hermes_commit": "b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5",
        },
    }
    report["observed_matches_expected"] = report["observed"] == {
        "candidate_package_version": report["expected"]["tether_version"],
        "installed_schema": report["expected"]["schema"],
        "installed_broker_protocol": report["expected"]["broker_protocol"],
        "deployed_hermes_version": report["expected"]["hermes_version"],
        "deployed_hermes_commit": report["expected"]["hermes_commit"],
    }
    report["valid"] = (
        report["all_installed_artifacts_traced"]
        and report["candidate_clean"]
        and report["observed_matches_expected"]
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
