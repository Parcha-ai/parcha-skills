#!/usr/bin/env python3
"""Validate AutoQA report invariants without trusting the report verdict."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


FIELD_PATTERN = re.compile(r"^\*\*(?P<name>[^*]+):\*\*\s*(?P<value>.+?)\s*$", re.MULTILINE)
REQUIRED_COLUMNS = [
    "#",
    "Source",
    "Feature",
    "Required",
    "Disposition",
    "Depth",
    "Modality",
    "Entry point",
    "Check",
    "Result",
    "Witness",
]
SHIP_VERDICTS = {"SHIP", "SHIP WITH CAVEATS"}
RESULTS = {"PASS", "FAIL", "UNTESTED", "SKIPPED"}


def parse_fields(text: str) -> dict[str, str]:
    return {match.group("name").strip(): match.group("value").strip() for match in FIELD_PATTERN.finditer(text)}


def parse_table(text: str) -> list[dict[str, str]]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        cells = split_row(line)
        if cells == REQUIRED_COLUMNS:
            rows: list[dict[str, str]] = []
            for candidate in lines[index + 2 :]:
                values = split_row(candidate)
                if not values:
                    break
                if len(values) != len(REQUIRED_COLUMNS):
                    raise ValueError(f"verdict row has {len(values)} columns; expected {len(REQUIRED_COLUMNS)}")
                rows.append(dict(zip(REQUIRED_COLUMNS, values, strict=True)))
            return rows
    raise ValueError("verdict table with required columns was not found")


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return []
    return [cell.strip() for cell in stripped[1:-1].split("|")]


def witness_paths(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def text_witness_contains(path: Path, phrase: str) -> bool:
    try:
        return phrase.lower() in path.read_text(errors="ignore").lower()
    except OSError:
        return False


def validate(report: Path, expected_target: str | None = None) -> list[str]:
    errors: list[str] = []
    text = report.read_text()
    fields = parse_fields(text)
    required_fields = [
        "Target repo",
        "Target instance",
        "Target commit",
        "Deployed commit",
        "Target verified",
        "Selected scope",
        "Chrome DevTools required",
        "Cleanup",
        "Cleanup witness",
        "Verdict",
    ]
    for name in required_fields:
        if name not in fields:
            errors.append(f"missing metadata field: {name}")

    target = fields.get("Target commit", "")
    deployed = fields.get("Deployed commit", "")
    verdict = fields.get("Verdict", "")
    if target and deployed and target != deployed and verdict != "BLOCKED":
        errors.append(f"target mismatch: requested {target}, deployed {deployed}")
    if expected_target and target != expected_target:
        errors.append(f"report target {target or '<missing>'} does not match expected {expected_target}")
    if fields.get("Target verified") != "YES" and verdict != "BLOCKED":
        errors.append("Target verified must be YES")

    cleanup_witness = report.parent / fields.get("Cleanup witness", "")
    if fields.get("Cleanup") not in {"PASS", "NOT REQUIRED"}:
        errors.append("Cleanup must be PASS or NOT REQUIRED")
    if not fields.get("Cleanup witness") or not cleanup_witness.is_file():
        errors.append("cleanup witness does not resolve")

    chrome_required = fields.get("Chrome DevTools required") == "YES"
    if verdict not in SHIP_VERDICTS | {"DO NOT SHIP", "BLOCKED"}:
        errors.append(f"invalid verdict: {verdict or '<missing>'}")

    try:
        rows = parse_table(text)
    except ValueError as exc:
        errors.append(str(exc))
        return errors
    if not rows:
        errors.append("verdict table has no rows")

    for row in rows:
        row_id = row["#"] or "?"
        required = row["Required"] == "YES"
        result = row["Result"]
        if row["Required"] not in {"YES", "NO"}:
            errors.append(f"row {row_id}: Required must be YES or NO")
        if result not in RESULTS:
            errors.append(f"row {row_id}: invalid result {result}")
        if row["Disposition"] == "DEEP" and row["Depth"] != "LIVE-E2E":
            errors.append(f"row {row_id}: DEEP coverage must be LIVE-E2E")
        if required and row["Disposition"] in {"UNTESTED", "SKIPPED"}:
            errors.append(f"row {row_id}: required row cannot be planned {row['Disposition']}")
        if required and verdict in SHIP_VERDICTS and result != "PASS":
            errors.append(f"row {row_id}: ship verdict requires every required row to PASS")
        if required and result in {"UNTESTED", "SKIPPED"} and verdict != "BLOCKED":
            errors.append(f"row {row_id}: required coverage gap requires a BLOCKED verdict")

        paths = witness_paths(row["Witness"])
        resolved = [report.parent / path for path in paths]
        if not paths or any(not path.is_file() for path in resolved):
            errors.append(f"row {row_id}: witness path does not resolve")
            continue
        if result == "UNTESTED" and not any(text_witness_contains(path, "fixture attempt") for path in resolved):
            errors.append(f"row {row_id}: UNTESTED requires a witnessed fixture attempt")

        if chrome_required and required and row["Modality"] == "UI" and result in {"PASS", "FAIL"}:
            visual = any(path.suffix.lower() in {".png", ".html", ".mhtml"} for path in resolved)
            runtime = any(path.suffix.lower() in {".har", ".json", ".txt", ".log"} for path in resolved)
            if not (visual and runtime):
                errors.append(f"row {row_id}: required UI needs DevTools visual and network/console witnesses")
        elif chrome_required and required and row["Modality"] == "UI" and verdict != "BLOCKED":
            errors.append(f"row {row_id}: unavailable required DevTools coverage requires BLOCKED")

    if verdict in SHIP_VERDICTS and any(row["Required"] == "YES" and row["Result"] != "PASS" for row in rows):
        errors.append("ship verdict conflicts with required coverage gaps")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--expected-target")
    args = parser.parse_args()
    errors = validate(args.report.resolve(), args.expected_target)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"PASS: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
