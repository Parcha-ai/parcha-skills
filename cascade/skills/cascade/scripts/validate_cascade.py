#!/usr/bin/env python3
"""Structural validation for Cascade v2 chain documents and boundary receipts."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


BOUNDARY_STATES = {
    "COMPLETE",
    "AT_BOUND",
    "WAITING_HUMAN",
    "BLOCKED_EXTERNAL",
    "SUPERSEDED",
}
CHAIN_STATES = BOUNDARY_STATES | {"ACTIVE"}
CHAIN_FIELDS = {
    "cascade_version",
    "episode_id",
    "pacing",
    "status",
    "current_loop",
    "authority",
    "budget",
    "human_gates",
    "target",
}
EXIT_FIELDS = {
    "cascade_version",
    "episode_id",
    "loop",
    "status",
    "target_head",
    "next",
}
LOOP_FIELDS = ("goal", "prompt", "accept", "bound", "at_bound ->", "exit ->")
EXIT_SECTIONS = (
    "Bound accounting",
    "Accept criteria → evidence",
    "Evidence manifest",
    "POST-ZEN",
    "Transition",
)


def _frontmatter(text: str) -> tuple[dict[str, str], str, list[str]]:
    errors: list[str] = []
    if not text.startswith("---\n"):
        return {}, text, ["document must start with YAML frontmatter"]
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}, text, ["frontmatter is not closed with ---"]

    fields: dict[str, str] = {}
    for number, line in enumerate(text[4:end].splitlines(), start=2):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_-]*):\s*(.*)", line)
        if not match:
            errors.append(f"frontmatter line {number} must be a flat key: value pair")
            continue
        key, value = match.groups()
        if key in fields:
            errors.append(f"duplicate frontmatter field: {key}")
        fields[key] = value.strip()
    return fields, text[end + 5 :], errors


def _require_fields(fields: dict[str, str], required: set[str]) -> list[str]:
    errors = [f"missing frontmatter field: {key}" for key in sorted(required - fields.keys())]
    errors.extend(f"frontmatter field is empty: {key}" for key in sorted(required) if not fields.get(key))
    return errors


def _has_heading(body: str, heading: str) -> bool:
    return bool(re.search(rf"(?mi)^##\s+{re.escape(heading)}\s*$", body))


def _section(body: str, heading: str) -> str:
    match = re.search(
        rf"(?ms)^##\s+{re.escape(heading)}\s*$\n(.*?)(?=^##\s+|\Z)", body
    )
    return match.group(1) if match else ""


def validate_chain(text: str) -> list[str]:
    fields, body, errors = _frontmatter(text)
    errors.extend(_require_fields(fields, CHAIN_FIELDS))

    if fields.get("cascade_version") != "2":
        errors.append("cascade_version must be 2")
    if fields.get("pacing") not in {"autonomous", "checkpointed"}:
        errors.append("pacing must be autonomous or checkpointed")
    if fields.get("status") and fields["status"] not in CHAIN_STATES:
        errors.append(f"invalid chain status: {fields['status']}")

    for heading in ("Current", "Authority and budgets", "Chain", "Invariants"):
        if not _has_heading(body, heading):
            errors.append(f"missing section: ## {heading}")

    chain_body = _section(body, "Chain")
    loops = list(re.finditer(r"(?m)^###\s+([LR][A-Za-z0-9._-]+)\b[^\n]*\n", chain_body))
    if not loops:
        errors.append("Chain section must contain at least one ### L... or ### R... loop")
    for index, match in enumerate(loops):
        end = loops[index + 1].start() if index + 1 < len(loops) else len(chain_body)
        loop_id = match.group(1)
        loop_body = chain_body[match.end() : end]
        for field in LOOP_FIELDS:
            suffix = r"\*\*" if field.endswith("->") else r":\*\*"
            pattern = rf"(?mi)^\s*-\s*\*\*{re.escape(field)}{suffix}"
            if not re.search(pattern, loop_body):
                errors.append(f"{loop_id} missing loop field: {field}")
    return errors


def _criterion_verdicts(section: str) -> list[str]:
    verdicts: list[str] = []
    for line in section.splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        criterion, verdict = cells[0], cells[1].upper()
        if criterion.lower() in {"id", "criterion"} or re.fullmatch(r":?-+:?", criterion):
            continue
        if criterion:
            verdicts.append(verdict)
    return verdicts


def validate_exit(text: str) -> list[str]:
    fields, body, errors = _frontmatter(text)
    errors.extend(_require_fields(fields, EXIT_FIELDS))

    if fields.get("cascade_version") != "2":
        errors.append("cascade_version must be 2")
    status = fields.get("status")
    if status and status not in BOUNDARY_STATES:
        errors.append(f"invalid boundary status: {status}")

    for heading in EXIT_SECTIONS:
        if not _has_heading(body, heading):
            errors.append(f"missing section: ## {heading}")

    verdicts = _criterion_verdicts(_section(body, "Accept criteria → evidence"))
    if not verdicts:
        errors.append("acceptance section must contain at least one PASS/FAIL/WAIT/UNMET table row")
    invalid_verdicts = sorted(set(verdicts) - {"PASS", "FAIL", "WAIT", "UNMET"})
    for verdict in invalid_verdicts:
        errors.append(f"invalid acceptance verdict: {verdict}")
    if status == "COMPLETE" and any(verdict != "PASS" for verdict in verdicts):
        errors.append("COMPLETE requires every acceptance verdict to be PASS")
    if status == "AT_BOUND" and verdicts and all(verdict == "PASS" for verdict in verdicts):
        errors.append("AT_BOUND must identify at least one unmet acceptance criterion")
    if status == "WAITING_HUMAN" and verdicts and "WAIT" not in verdicts:
        errors.append("WAITING_HUMAN must identify at least one WAIT criterion")
    return errors


def validate(kind: str, text: str) -> list[str]:
    return validate_chain(text) if kind == "chain" else validate_exit(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("chain", "exit"))
    parser.add_argument("path", type=Path)
    args = parser.parse_args(argv)

    try:
        text = args.path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    errors = validate(args.kind, text)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"OK: {args.kind} structure is valid: {args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
