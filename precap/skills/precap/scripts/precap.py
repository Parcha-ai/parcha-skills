#!/usr/bin/env python3
"""Structural checks for precap.md files.

Commands:
  template            print a blank precap
  validate <file>     fail closed on structural defects
  drift <file>        compare the Footprint section with git state, scoped to the
                      Footprint's top-level directories unless --scope/--all say otherwise

The script never judges whether a precap is a good prediction. That is the agent's job,
with the rubric in references/eval-contract.md. This only checks shape and the parts that
git can answer deterministically.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

SECTIONS = [
    "Task",
    "End state",
    "Path",
    "Footprint",
    "Verification",
    "Forks",
    "Not done",
    "Assumptions",
    "Drift signals",
    "Revisions",
]

FOOTPRINT_RE = re.compile(r"^\s*[-*]\s+`(?P<path>[^`]+)`\s+\((?P<kind>new|modified|deleted)\)")
STEP_RE = re.compile(r"^(\d+)\.\s")
ASSUMPTION_TAG_RE = re.compile(r"\[(verified|unverified)\]")
GROUNDING_REF_RE = re.compile(r"`(?P<ref>[^`\s]+)`")
LINE_REF_RE = re.compile(r"^(?P<path>.+?):(?P<start>\d+)(?:-(?P<end>\d+))?$")
META_RE = re.compile(r"^(?P<key>Written|Branch|Base|Workdir):\s*(?P<value>.+?)\s*$")

TEMPLATE = """# Precap: <short title>

Written: <YYYY-MM-DD>
Branch: <branch>
Base: <base ref, e.g. origin/main>
Workdir: <path>

## Task

> <verbatim ask, quoted>

<one-paragraph interpretation>

## End state

<what exists when done and how a reader confirms it>

## Path

1. **<step title>** <past-tense account of the step>
   - Grounded in: `<file:line>` or `<command>` or `<commit>`
   - Checkpoint: <observable fact that proves the step happened>

## Footprint

- `<path>` (modified): <why>
- `<path>` (new): <why>

## Verification

- `<command>`: <expected result>

## Forks

- **<fork name>**: A) <branch> / B) <branch>. Decided by: <rule, or "the user (undecided)">

## Not done

- <out-of-scope item>: <why it stayed out>

## Assumptions

- [verified] <belief and how it was checked>
- [unverified] <belief and what would settle it>

## Drift signals

- If you find yourself <doing X>, you have left the path: <what to do>

## Revisions

<empty until reality diverges>
"""


@dataclass
class Precap:
    meta: dict[str, str] = field(default_factory=dict)
    sections: dict[str, str] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)


def parse(text: str) -> Precap:
    precap = Precap()
    current: str | None = None
    buffer: list[str] = []
    for line in text.splitlines():
        heading = re.match(r"^##\s+(.+?)\s*$", line)
        if heading:
            if current is not None:
                precap.sections[current] = "\n".join(buffer)
            current = heading.group(1)
            precap.order.append(current)
            buffer = []
            continue
        if current is None:
            meta = META_RE.match(line)
            if meta:
                precap.meta[meta.group("key")] = meta.group("value")
        else:
            buffer.append(line)
    if current is not None:
        precap.sections[current] = "\n".join(buffer)
    return precap


def split_steps(path_section: str) -> list[str]:
    steps: list[str] = []
    for line in path_section.splitlines():
        if STEP_RE.match(line):
            steps.append(line)
        elif steps and line.strip():
            steps[-1] += "\n" + line
    return steps


def footprint(precap: Precap) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    for line in precap.sections.get("Footprint", "").splitlines():
        match = FOOTPRINT_RE.match(line)
        if match:
            entries.append((match.group("path"), match.group("kind")))
    return entries


def is_placeholder(value: str) -> bool:
    return value.strip().startswith("<") and value.strip().endswith(">")


def bullets(section: str) -> list[str]:
    """Bullet items with their wrapped continuation lines joined."""
    items: list[str] = []
    for line in section.splitlines():
        stripped = line.strip()
        if stripped.startswith(("- ", "* ")):
            items.append(stripped)
        elif items and stripped:
            items[-1] += " " + stripped
    return items


def looks_like_path(ref: str, workdir: Path) -> bool:
    """A slash makes it a path claim; a bare dotted token only counts when it exists."""
    head = ref.split(":", 1)[0]
    if "/" in head:
        return True
    return re.search(r"\.[A-Za-z0-9]{1,8}$", head) is not None and (workdir / head).exists()


def check_grounding(step: str, workdir: Path) -> list[str]:
    """Verify backticked path refs in Grounded in: lines exist and their line ranges fit."""
    errors: list[str] = []
    for line in step.splitlines():
        if "Grounded in:" not in line:
            continue
        for match in GROUNDING_REF_RE.finditer(line.split("Grounded in:", 1)[1]):
            ref = match.group("ref").rstrip(".,;)")
            if ref.startswith(("http://", "https://")) or not looks_like_path(ref, workdir):
                continue
            line_match = LINE_REF_RE.match(ref)
            rel = line_match.group("path") if line_match else ref
            target = workdir / rel
            if not target.exists():
                errors.append(f"grounding `{ref}` does not exist under {workdir}")
                continue
            if line_match and target.is_file():
                total = sum(1 for _ in target.open("rb"))
                end = int(line_match.group("end") or line_match.group("start"))
                if int(line_match.group("start")) < 1 or end > total:
                    errors.append(f"grounding `{ref}` is out of range ({total} lines)")
    return errors


def git_ref_exists(workdir: Path, ref: str) -> bool | None:
    """True/False when workdir is a git repo, None when git cannot answer."""
    try:
        inside = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"], cwd=workdir,
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    if inside.returncode != 0:
        return None
    verify = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=workdir,
        capture_output=True, text=True, check=False,
    )
    return verify.returncode == 0


def validate(precap: Precap, workdir: Path) -> list[str]:
    errors: list[str] = []

    for key in ("Written", "Branch", "Base"):
        value = precap.meta.get(key)
        if not value or is_placeholder(value):
            errors.append(f"missing header line: {key}:")
    base = precap.meta.get("Base")
    if base and not is_placeholder(base) and git_ref_exists(workdir, base) is False:
        errors.append(f"Base: {base} is not a commit git can resolve under {workdir}")

    missing = [name for name in SECTIONS if name not in precap.sections]
    for name in missing:
        errors.append(f"missing section: ## {name}")
    present_in_order = [name for name in precap.order if name in SECTIONS]
    expected = [name for name in SECTIONS if name in precap.sections]
    if present_in_order != expected:
        errors.append("sections out of order; expected " + ", ".join(SECTIONS))

    for name in SECTIONS:
        body = precap.sections.get(name, "")
        if name == "Revisions":
            continue
        if name in precap.sections and not body.strip():
            errors.append(f"empty section: ## {name}")

    steps = split_steps(precap.sections.get("Path", ""))
    if "Path" in precap.sections and not steps:
        errors.append("Path has no numbered steps")
    for step in steps:
        number = STEP_RE.match(step).group(1)
        if "Grounded in:" not in step:
            errors.append(f"Path step {number} has no 'Grounded in:' line")
        elif re.search(r"Grounded in:\s*(<[^>]*>)?\s*$", step, re.MULTILINE):
            errors.append(f"Path step {number} has an empty 'Grounded in:'")
        if "Checkpoint:" not in step:
            errors.append(f"Path step {number} has no 'Checkpoint:' line")
        elif re.search(r"Checkpoint:\s*(<[^>]*>)?\s*$", step, re.MULTILINE):
            errors.append(f"Path step {number} has an empty 'Checkpoint:'")
        errors.extend(f"Path step {number}: {e}" for e in check_grounding(step, workdir))

    for item in bullets(precap.sections.get("Forks", "")):
        if "Decided by:" not in item:
            errors.append(f"Fork needs a 'Decided by:' rule (use 'the user (undecided)' when it is theirs): {item[:60]}")

    entries = footprint(precap)
    if "Footprint" in precap.sections and not entries:
        errors.append("Footprint has no entries shaped like - `path` (new|modified|deleted): why")
    for rel, kind in entries:
        if is_placeholder(rel):
            errors.append(f"Footprint placeholder left in place: {rel}")
            continue
        target = workdir / rel
        if kind in ("modified", "deleted") and not target.exists():
            errors.append(f"Footprint marks `{rel}` as {kind} but it does not exist under {workdir}")
        if kind == "new" and target.exists():
            errors.append(f"Footprint marks `{rel}` as new but it already exists under {workdir}")

    for stripped in bullets(precap.sections.get("Assumptions", "")):
        tags = ASSUMPTION_TAG_RE.findall(stripped)
        if len(tags) != 1:
            errors.append(f"Assumption needs exactly one [verified]/[unverified] tag: {stripped[:60]}")
        elif "<" in stripped and ">" in stripped and is_placeholder(stripped.split("]", 1)[1]):
            errors.append(f"Assumption placeholder left in place: {stripped[:60]}")

    verification = precap.sections.get("Verification", "")
    if "Verification" in precap.sections and "`" not in verification:
        errors.append("Verification names no command in backticks")

    for name in ("Task", "End state", "Forks", "Not done", "Drift signals", "Verification"):
        body = precap.sections.get(name, "")
        if re.search(r"<[a-z][^>\n]{2,}>", body):
            errors.append(f"template placeholder left in ## {name}")

    return errors


def git(args: list[str], cwd: Path) -> str:
    result = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout


def actual_changes(workdir: Path, base: str, precap_path: Path) -> set[str]:
    changed: set[str] = set()
    committed = git(["diff", "--name-only", f"{base}...HEAD"], workdir)
    changed.update(line.strip() for line in committed.splitlines() if line.strip())
    status = git(["status", "--porcelain", "--untracked-files=all"], workdir)
    for line in status.splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        changed.add(path.strip())
    try:
        rel = os.path.relpath(precap_path.resolve(), workdir.resolve())
        changed.discard(rel)
    except ValueError:
        pass
    return changed


def scope_prefixes(predicted: set[str]) -> list[str]:
    """Top-level directories of the predicted footprint; a bare top-level file scopes to itself."""
    prefixes: set[str] = set()
    for rel in predicted:
        head, _, _ = rel.partition("/")
        prefixes.add(head + "/" if "/" in rel else head)
    return sorted(prefixes)


def in_scope(path: str, prefixes: list[str]) -> bool:
    return any(path == p or path.startswith(p) for p in prefixes)


def drift(
    precap: Precap,
    workdir: Path,
    precap_path: Path,
    base_override: str | None,
    scope: list[str] | None = None,
    whole_tree: bool = False,
) -> dict:
    base = base_override or precap.meta.get("Base")
    if not base or is_placeholder(base):
        raise RuntimeError("no Base: header in precap and no --base given")
    predicted = {rel for rel, _ in footprint(precap)}
    actual_all = actual_changes(workdir, base, precap_path)
    prefixes = [] if whole_tree else (scope or scope_prefixes(predicted))
    actual = actual_all if whole_tree else {p for p in actual_all if in_scope(p, prefixes)}
    touched = sorted(predicted & actual)
    pending = sorted(predicted - actual)
    unpredicted = sorted(actual - predicted)
    return {
        "base": base,
        "scope": prefixes,
        "predicted": len(predicted),
        "actual": len(actual),
        "out_of_scope": len(actual_all - actual),
        "touched": touched,
        "pending": pending,
        "unpredicted": unpredicted,
    }


def cmd_template(_: argparse.Namespace) -> int:
    sys.stdout.write(TEMPLATE)
    return 0


def resolve_workdir(args: argparse.Namespace, precap: Precap, precap_path: Path) -> Path:
    if args.workdir:
        return Path(args.workdir).resolve()
    meta = precap.meta.get("Workdir")
    if meta and not is_placeholder(meta):
        candidate = Path(os.path.expanduser(meta))
        if candidate.is_dir():
            return candidate.resolve()
    here = precap_path.resolve().parent
    top = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], cwd=here, capture_output=True, text=True, check=False
    )
    if top.returncode == 0 and top.stdout.strip():
        return Path(top.stdout.strip()).resolve()
    return here


def cmd_validate(args: argparse.Namespace) -> int:
    path = Path(args.file)
    precap = parse(path.read_text(encoding="utf-8"))
    workdir = resolve_workdir(args, precap, path)
    errors = validate(precap, workdir)
    steps = len(split_steps(precap.sections.get("Path", "")))
    receipt = {
        "file": str(path),
        "workdir": str(workdir),
        "ok": not errors,
        "steps": steps,
        "footprint": len(footprint(precap)),
        "errors": errors,
    }
    if args.json:
        print(json.dumps(receipt, indent=2))
    else:
        if errors:
            for error in errors:
                print(f"error: {error}")
        print(f"{'ok' if not errors else 'invalid'}: {path} ({steps} steps, {receipt['footprint']} footprint entries)")
    return 0 if not errors else 1


def cmd_drift(args: argparse.Namespace) -> int:
    path = Path(args.file)
    precap = parse(path.read_text(encoding="utf-8"))
    workdir = resolve_workdir(args, precap, path)
    try:
        report = drift(precap, workdir, path, args.base, scope=args.scope or None, whole_tree=args.all)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, indent=2))
        return 0
    scope = ", ".join(report["scope"]) if report["scope"] else "whole tree"
    print(
        f"base {report['base']} scope [{scope}]: {report['predicted']} predicted, "
        f"{report['actual']} changed in scope, {report['out_of_scope']} changed outside it"
    )
    for label in ("touched", "pending", "unpredicted"):
        items = report[label]
        print(f"{label} ({len(items)}):")
        for item in items:
            print(f"  {item}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("template", help="print a blank precap").set_defaults(func=cmd_template)

    for name, func in (("validate", cmd_validate), ("drift", cmd_drift)):
        p = sub.add_parser(name)
        p.add_argument("file")
        p.add_argument("--workdir", help="repository root; defaults to the Workdir: header or the precap's directory")
        p.add_argument("--json", action="store_true")
        if name == "drift":
            p.add_argument("--base", help="override the Base: header")
            p.add_argument("--scope", action="append", help="path prefix to compare within (repeatable); default derives from the Footprint")
            p.add_argument("--all", action="store_true", help="compare against the whole tree, ignoring scope")
        p.set_defaults(func=func)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
