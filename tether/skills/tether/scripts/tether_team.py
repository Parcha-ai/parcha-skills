#!/usr/bin/env python3
"""Apply the shared team layer to this agent's SOUL.md.

The team layer (TEAM.md) is the one place that says who the colleagues are and
how a colleague behaves. Each agent's SOUL.md keeps its own persona below a
marker; this script replaces only the managed block, so persona edits survive.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

BEGIN = "<!-- tether-team-layer:begin -->"
END = "<!-- tether-team-layer:end -->"


def hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()


def apply(team_md: Path, soul: Path) -> str:
    block = f"{BEGIN}\n{team_md.read_text(encoding='utf-8').strip()}\n{END}\n"
    current = soul.read_text(encoding="utf-8") if soul.exists() else ""
    if BEGIN in current and END in current:
        updated = re.sub(re.escape(BEGIN) + r".*?" + re.escape(END) + r"\n?", block, current, flags=re.S)
        action = "updated"
    else:
        # Drop the older ad-hoc rules this layer supersedes, then prepend.
        lines = [
            line for line in current.splitlines()
            if not line.startswith("- Slack mentions: address people and agents as <@USERID>")
            and not line.startswith("- Peer agents are colleagues, not noise.")
        ]
        updated = block + "\n" + "\n".join(lines).lstrip("\n")
        action = "installed"
    if updated != current:
        tmp = soul.with_suffix(".md.tmp")
        tmp.write_text(updated.rstrip("\n") + "\n", encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, soul)
    return action


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tether team")
    parser.add_argument("command", choices=["apply", "status"])
    parser.add_argument("--team-md", default=str(Path(__file__).resolve().parents[3] / "team" / "TEAM.md"))
    parser.add_argument("--soul", default=str(hermes_home() / "SOUL.md"))
    args = parser.parse_args(argv)
    team_md, soul = Path(args.team_md), Path(args.soul)
    if not team_md.is_file():
        print(f"team layer missing: {team_md}", file=sys.stderr)
        return 2
    if args.command == "status":
        text = soul.read_text(encoding="utf-8") if soul.exists() else ""
        print("applied" if BEGIN in text else "absent")
        return 0
    print(apply(team_md, soul))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
