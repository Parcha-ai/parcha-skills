#!/usr/bin/env python3
"""Render board.json into the self-contained Agent Hub Board page.

Reads board/template.html, substitutes the generated fragments, and writes one
static HTML file with inline CSS, no JS, no external references -- the format
`~/docs` requires.

Usage:
    python3 board/render.py                                  # board.json -> board/agent-hub-board.html
    python3 board/render.py -i b.json -o ~/docs/2026-09-09-agent-hub-board.html
"""

from __future__ import annotations

import argparse
import html
import json
from datetime import datetime, timezone
from pathlib import Path

BOARD_DIR = Path(__file__).resolve().parent


def esc(value) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def rel_age(stamp: str | None, now: datetime) -> str:
    """'3h ago' / '2d ago'; em dash when there is nothing to report."""
    if not stamp:
        return "—"
    try:
        then = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return "—"
    seconds = (now - then).total_seconds()
    if seconds < 90:
        return "just now"
    if seconds < 3600:
        return "%dm ago" % (seconds // 60)
    if seconds < 86400:
        return "%dh ago" % (seconds // 3600)
    return "%dd ago" % (seconds // 86400)


def staleness(stamp: str | None, now: datetime) -> str:
    """Row tone: quiet for >24h silent, warn for >72h, ok otherwise."""
    if not stamp:
        return "quiet"
    try:
        then = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return "quiet"
    hours = (now - then).total_seconds() / 3600
    if hours > 72:
        return "warn"
    if hours > 24:
        return "quiet"
    return "ok"


def agent_rows(agents: list[dict], now: datetime) -> str:
    if not agents:
        return ('<tr><td colspan="7" class="empty">No agents reported. '
                'The collector reached no gateway.</td></tr>')
    rows = []
    for agent in agents:
        tone = staleness(agent["last_reply_at"], now)
        latency = ("—" if agent["latency_median_s"] is None
                   else "%.1fs" % agent["latency_median_s"])
        rows.append(
            '<tr class="{tone}">'
            '<td class="name">{name}<span class="sid">{sid}</span></td>'
            '<td class="lane">{lane}</td>'
            '<td class="mono">{host}</td>'
            '<td class="num">{live}</td>'
            '<td class="num">{replies}</td>'
            '<td class="num">{latency}</td>'
            '<td class="age" title="{title}">{age}</td>'
            "</tr>".format(
                tone=tone, name=esc(agent["name"]), sid=esc(agent["slack_id"]),
                lane=esc(agent["lane"]), host=esc(agent["host"]),
                live=agent["live_threads"], replies=agent["replies_24h"],
                latency=esc(latency), title=esc(agent["last_reply_at"] or "never"),
                age=esc(rel_age(agent["last_reply_at"], now))))
    return "\n".join(rows)


def machine_rows(machines: list[dict]) -> str:
    if not machines:
        return '<tr><td colspan="7" class="empty">No machines reported.</td></tr>'
    rows = []
    for machine in machines:
        if machine.get("error"):
            rows.append(
                '<tr class="warn"><td class="mono">{host}</td>'
                '<td colspan="6" class="err">unreachable — {err}</td></tr>'.format(
                    host=esc(machine["host"]), err=esc(machine["error"])))
            continue
        load = machine["load"] or []
        load_text = " ".join("%.2f" % x for x in load) if load else "—"
        tone = "warn" if machine["doctor_fail"] else "ok"
        doctor = ("%d ok" % machine["doctor_ok"] if not machine["doctor_fail"]
                  else "%d ok / %d fail" % (machine["doctor_ok"], machine["doctor_fail"]))
        rows.append(
            '<tr class="{tone}">'
            '<td class="mono">{host}</td>'
            '<td>{uptime}</td>'
            '<td class="mono">{load}</td>'
            '<td class="num">{disk}</td>'
            '<td class="mono">{hermes}</td>'
            '<td class="mono">{tether}</td>'
            '<td class="doctor">{doctor}</td>'
            "</tr>".format(
                tone=tone, host=esc(machine["host"]),
                uptime=esc(machine["uptime"] or "—"), load=esc(load_text),
                disk=esc(machine["disk_free"] or "—"),
                hermes=esc(machine["hermes_version"] or "—"),
                tether=esc(machine["tether_version"] or "—"), doctor=esc(doctor)))
    return "\n".join(rows)


def shipped_items(shipped: list[dict]) -> str:
    if not shipped:
        return '<li class="empty">Nothing landed on main in the last 7 days.</li>'
    items = []
    for entry in shipped:
        ref = ("#%d" % entry["pr"]) if entry.get("pr") else ""
        items.append(
            '<li><span class="pr">{ref}</span> <span class="t">{title}</span>'
            '<span class="meta">{author} · {when}</span></li>'.format(
                ref=esc(ref), title=esc(entry["title"]),
                author=esc(entry.get("author") or "—"),
                when=esc(entry["merged_at"][:10])))
    return "\n".join(items)


def render(board: dict) -> str:
    now = datetime.now(timezone.utc)
    template = (BOARD_DIR / "template.html").read_text()

    agents = board["agents"]
    machines = board["machines"]
    reachable = [m for m in machines if not m.get("error")]
    failing = sum(m["doctor_fail"] for m in reachable)

    replacements = {
        "{{GENERATED_AT}}": esc(board["generated_at"]),
        "{{GENERATED_AGE}}": esc(rel_age(board["generated_at"], now)),
        "{{AGENT_COUNT}}": str(len(agents)),
        "{{MACHINE_COUNT}}": "%d/%d" % (len(reachable), len(machines)),
        "{{LIVE_THREADS}}": str(sum(a["live_threads"] for a in agents)),
        "{{REPLIES_24H}}": str(sum(a["replies_24h"] for a in agents)),
        "{{DOCTOR_STATE}}": ("all clear" if not failing
                             else "%d failing check%s" % (failing, "" if failing == 1 else "s")),
        "{{DOCTOR_TONE}}": "ok" if not failing else "warn",
        "{{AGENT_ROWS}}": agent_rows(agents, now),
        "{{MACHINE_ROWS}}": machine_rows(machines),
        "{{SHIPPED_ITEMS}}": shipped_items(board["shipped_this_week"]),
        "{{SHIPPED_COUNT}}": str(len(board["shipped_this_week"])),
    }
    for token, value in replacements.items():
        template = template.replace(token, value)
    return template


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-i", "--input", default=str(BOARD_DIR / "board.json"))
    parser.add_argument("-o", "--output",
                        default=str(BOARD_DIR / "agent-hub-board.html"))
    args = parser.parse_args()

    board = json.loads(Path(args.input).read_text())
    page = render(board)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(output.suffix + ".tmp")
    tmp.write_text(page)
    tmp.replace(output)  # atomic: the docs server never serves a partial page
    print("wrote %s (%d bytes)" % (output, len(page)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
