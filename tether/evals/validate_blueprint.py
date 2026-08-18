#!/usr/bin/env python3
"""Validate the L0 ADR, schema references, and published HTML blueprint."""

from __future__ import annotations

import argparse
from html.parser import HTMLParser
import json
import hashlib
import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parents[1]


class AssetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.external_assets: list[tuple[str, str]] = []
        self.script_count = 0
        self.svg_count = 0
        self.title_count = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "script":
            self.script_count += 1
        elif tag == "svg":
            self.svg_count += 1
        elif tag == "title":
            self.title_count += 1
        if tag in {"img", "script", "link", "source", "video", "audio", "iframe"}:
            for key in ("src", "href"):
                value = values.get(key) or ""
                if value.startswith(("http://", "https://", "//")):
                    self.external_assets.append((tag, value))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", required=True, type=pathlib.Path)
    parser.add_argument("--provenance", type=pathlib.Path)
    parser.add_argument("--corpus", type=pathlib.Path)
    parser.add_argument("--metrics", type=pathlib.Path)
    parser.add_argument("--cross-repo", type=pathlib.Path)
    parser.add_argument("--test-log", type=pathlib.Path)
    args = parser.parse_args()

    runtime = (ROOT / "runtime/bridge_runtime.py").read_text(encoding="utf-8")
    schema = int(re.search(r"SCHEMA_VERSION\s*=\s*(\d+)", runtime).group(1))
    docs = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
    stale = []
    for path in docs:
        text = path.read_text(encoding="utf-8")
        if re.search(r"(?:schema|schema version|user_version)\s+16\b|newer than 16\b", text, re.IGNORECASE):
            stale.append(str(path.relative_to(ROOT)))

    html = args.html.read_text(encoding="utf-8")
    parsed = AssetParser()
    parsed.feed(html)
    required_phrases = [
        "one endpoint can bind many Slack threads",
        "Ingress middleware v1",
        "Delivery ledger v2",
        "greppy-authorityd",
        "event/thread scoped",
        "Mechanical deletion gates",
        "persist before transport ACK",
        "get/watch/reconcile",
        "same-UID canary",
        "owner lease",
    ]
    missing_phrases = [phrase for phrase in required_phrases if phrase not in html]
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    required_package_files = {
        "docs/ADR-001-TETHER-REPLACEMENT.md",
        "evals/incident-corpus.json",
        "evals/capture_baseline_metrics.py",
        "evals/capture_provenance.py",
        "evals/run_cross_repo_contracts.py",
        "evals/run_incident_corpus.py",
        "evals/validate_blueprint.py",
    }
    missing_package_files = sorted(required_package_files - set(package["files"]))

    evidence = {}
    for name, path in {
        "provenance": args.provenance,
        "corpus": args.corpus,
        "metrics": args.metrics,
        "cross_repo": args.cross_repo,
    }.items():
        if path:
            evidence[name] = json.loads(path.read_text(encoding="utf-8"))

    evidence_errors = []
    commits = {
        value.get("candidate_commit") or value.get("target_commit")
        for name, value in evidence.items() if name != "cross_repo"
    }
    commits.discard(None)
    if len(commits) > 1:
        evidence_errors.append(f"candidate commits disagree: {sorted(commits)}")
    for name, value in evidence.items():
        if not value.get("valid", value.get("candidate_clean", False)):
            evidence_errors.append(f"{name} report is not valid")
    provenance = evidence.get("provenance")
    if provenance:
        adr_key = "docs/ADR-001-TETHER-REPLACEMENT.md"
        expected_adr = provenance.get("evidence_source_sha256", {}).get(adr_key)
        actual_adr = hashlib.sha256(
            (ROOT / "docs/ADR-001-TETHER-REPLACEMENT.md").read_bytes()
        ).hexdigest()
        if expected_adr != actual_adr:
            evidence_errors.append("ADR hash does not match provenance")
        commit = provenance["candidate_commit"]
        if commit[:12] not in html:
            evidence_errors.append("HTML does not name the proven candidate commit")

    test_count = None
    if args.test_log:
        test_text = args.test_log.read_text(encoding="utf-8", errors="replace")
        matches = re.findall(r"Ran (\d+) tests?", test_text)
        if not matches:
            evidence_errors.append("test log has no unittest count")
        else:
            test_count = int(matches[-1])
            if f"{test_count} tests" not in html:
                evidence_errors.append("HTML test count does not match test log")

    result = {
        "schema_version": schema,
        "stale_schema_documents": stale,
        "html_bytes": args.html.stat().st_size,
        "html_svg_count": parsed.svg_count,
        "html_script_count": parsed.script_count,
        "html_external_assets": parsed.external_assets,
        "missing_required_phrases": missing_phrases,
        "missing_package_files": missing_package_files,
        "evidence_reports": sorted(evidence),
        "evidence_errors": evidence_errors,
        "full_test_count": test_count,
    }
    valid = (
        schema == 17
        and not stale
        and parsed.svg_count >= 8
        and parsed.script_count == 0
        and not parsed.external_assets
        and not missing_phrases
        and not missing_package_files
        and not evidence_errors
    )
    result["valid"] = valid
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
