#!/usr/bin/env python3
"""Run the sanitized L0 corpus against one clean, content-addressed candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import platform
import shutil
import subprocess  # nosec B404 - fixed local test and Git invocations only
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parent
MANIFEST = pathlib.Path(__file__).with_name("incident-corpus.json")


def git(*args: str) -> str:
    return subprocess.run(  # nosec B603 - fixed binary; reviewed arguments
        ["/usr/bin/git", *args],
        cwd=REPOSITORY,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout.strip()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def selector_path(selector: str) -> pathlib.Path:
    return ROOT / "tests" / f"{selector.split('.', 1)[0]}.py"


def evidence_sources(manifest: dict[str, Any]) -> list[pathlib.Path]:
    paths = {
        MANIFEST,
        pathlib.Path(__file__),
        ROOT / "package.json",
        ROOT / "package-lock.json",
        *ROOT.glob("runtime/*.py"),
        *ROOT.glob("runtime/plugin/*.py"),
    }
    for incident in manifest["incidents"]:
        for key in ("probe", "control"):
            if selector := incident.get(key):
                paths.add(selector_path(selector))
    return sorted(path.resolve() for path in paths if path.is_file())


def source_inventory(paths: list[pathlib.Path]) -> tuple[dict[str, str], str]:
    inventory = {
        str(path.relative_to(REPOSITORY)): sha256(path.read_bytes()) for path in paths
    }
    joined = "".join(f"{name}\0{value}\n" for name, value in sorted(inventory.items()))
    return inventory, sha256(joined.encode())


def run_test(selector: str) -> dict[str, Any]:
    command = [sys.executable, "-m", "unittest", "-v", selector]
    completed = subprocess.run(  # nosec B603 - no shell; reviewed manifest selector
        command,
        cwd=ROOT / "tests",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=120,
        check=False,
    )
    return {
        "selector": selector,
        "command": ["python", "-m", "unittest", "-v", selector],
        "exit_code": completed.returncode,
        "passed": completed.returncode == 0,
        "output_tail": completed.stdout.strip().splitlines()[-8:],
    }


def external_results(path: pathlib.Path | None) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if path is None:
        return {}, None
    report = json.loads(path.read_text(encoding="utf-8"))
    return {item["id"]: item for item in report.get("results", [])}, report


def expected_verdict(classification: str) -> str:
    return {
        "baseline_defect": "defect_observed",
        "cross_repo_contract_defect": "cross_repo_defect_observed",
        "legacy_behavior_control": "legacy_control_preserved",
    }[classification]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cross-repo-report", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    external, cross_report = external_results(args.cross_repo_report)
    results = []
    for incident in manifest["incidents"]:
        if selector := incident.get("probe"):
            probe = run_test(selector)
        else:
            external_id = incident["external_probe"]
            external_result = external.get(external_id)
            probe = {
                "external_result_id": external_id,
                "passed": bool(external_result and external_result.get("passed")),
                "result": external_result,
            }
        control = run_test(incident["control"])
        passed = probe["passed"] and control["passed"]
        results.append(
            {
                "id": incident["id"],
                "classification": incident["classification"],
                "probe": probe,
                "control": control,
                "verdict": expected_verdict(incident["classification"]) if passed else "invalid",
                "falsifier": "The probe/control fails, an external receipt is absent, or candidate content differs from the recorded digest.",
            }
        )

    status = git("status", "--porcelain=v1", "--untracked-files=all")
    sources, target_digest = source_inventory(evidence_sources(manifest))
    target_commit = git("rev-parse", "HEAD")
    target_tree = git("rev-parse", "HEAD^{tree}")
    node_binary = shutil.which("node")
    if not node_binary:
        raise RuntimeError("node is unavailable")
    cross_required = any("external_probe" in item for item in manifest["incidents"])
    valid = (
        status == ""
        and all(item["verdict"] != "invalid" for item in results)
        and (not cross_required or bool(cross_report and cross_report.get("valid")))
    )
    report = {
        "schema_version": 2,
        "target_commit": target_commit,
        "target_tree": target_tree,
        "target_content_sha256": target_digest,
        "candidate_clean": status == "",
        "candidate_status": status.splitlines(),
        "source_sha256": sources,
        "manifest_sha256": sha256(MANIFEST.read_bytes()),
        "cross_repo_report_sha256": (
            sha256(args.cross_repo_report.read_bytes()) if args.cross_repo_report else None
        ),
        "environment": {
            "python": sys.version.split()[0],
            "node": subprocess.run(  # nosec B603 - resolved local executable
                [node_binary, "--version"], text=True, capture_output=True, check=True
            ).stdout.strip(),
            "os": platform.platform(),
            "architecture": platform.machine(),
        },
        "command": [
            "run_incident_corpus.py", "--cross-repo-report", "<report>", "--output", "<report>"
        ],
        "results": results,
        "valid": valid,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
