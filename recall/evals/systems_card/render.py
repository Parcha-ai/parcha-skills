"""Render card.json into one self-contained HTML page."""
from __future__ import annotations

import html
import json
from typing import Any

STATUS_COLOR = {"ok": "#1f8f4e", "degraded": "#b7791f", "failed": "#c53030", "skipped": "#718096"}

CSS = """
:root{--bg:#0f1115;--fg:#e6e6e6;--muted:#9aa4b2;--card:#171a21;--line:#262b35}
@media (prefers-color-scheme: light){:root{--bg:#f7f7f5;--fg:#14171c;--muted:#5b6470;--card:#fff;--line:#e2e4e8}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 'JetBrains Mono',ui-monospace,SFMono-Regular,Menlo,monospace}
main{max-width:1100px;margin:0 auto;padding:28px 20px 60px}
h1{font-size:20px;margin:0 0 4px;letter-spacing:.02em}h2{font-size:15px;margin:26px 0 8px}
.sub{color:var(--muted);font-size:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px;margin-top:14px}
.dim{background:var(--card);border:1px solid var(--line);padding:12px}
.dim .name{text-transform:uppercase;font-size:11px;letter-spacing:.08em;color:var(--muted)}
.dim .status{font-size:18px;margin-top:2px}
.pill{display:inline-block;padding:1px 7px;font-size:11px;color:#fff}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:500}
td.num{text-align:right;font-variant-numeric:tabular-nums}
.probe{background:var(--card);border:1px solid var(--line);padding:10px 12px;margin:8px 0}
.probe h3{margin:0 0 6px;font-size:13px;display:flex;gap:10px;align-items:center}
.notes{color:var(--muted);font-size:12px;margin:6px 0 0}
details summary{cursor:pointer;color:var(--muted);font-size:12px}
svg.spark{width:120px;height:26px;vertical-align:middle}
.hist{display:flex;flex-wrap:wrap;gap:14px}
.hist div{font-size:11px;color:var(--muted)}
"""


def _fmt(value: Any) -> str:
    if value is None:
        return "–"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        if abs(value) >= 1000:
            return f"{value:,.0f}"
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if isinstance(value, (dict, list)):
        return html.escape(json.dumps(value, sort_keys=True))[:400]
    return html.escape(str(value))


def _pill(status: str) -> str:
    color = STATUS_COLOR.get(status, "#718096")
    return f'<span class="pill" style="background:{color}">{html.escape(status)}</span>'


def _spark(values: list[float | None]) -> str:
    points = [v for v in values if isinstance(v, (int, float))]
    if len(points) < 2:
        return ""
    lo, hi = min(points), max(points)
    span = (hi - lo) or 1.0
    coords = []
    n = len(values)
    for i, v in enumerate(values):
        if not isinstance(v, (int, float)):
            continue
        x = 2 + i * (116 / max(1, n - 1))
        y = 23 - (v - lo) / span * 20
        coords.append(f"{x:.1f},{y:.1f}")
    return f'<svg class="spark" viewBox="0 0 120 26"><polyline fill="none" stroke="#4c9be8" stroke-width="1.5" points="{" ".join(coords)}"/></svg>'


def render_html(card: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    history = history or []
    overall = card["overall"]
    parts = [
        "<title>Recall Systems Card</title>",
        f"<style>{CSS}</style>",
        "<main>",
        "<h1>Recall Systems Card</h1>",
        f'<div class="sub">{html.escape(card["target"]["mcp_url"])} · generated {html.escape(card["generated_at"])} · '
        f'{card["duration_seconds"]}s · git {html.escape(str(card["pins"].get("git_sha") or "")[:10])}'
        f'{" (dirty)" if card["pins"].get("git_dirty") else ""}</div>',
        f'<p>Overall {_pill(overall["status"])} · gates {overall["gates_passed"]}/{overall["gates_total"]} passed, '
        f'{overall["gates_failed"]} failed, {overall["gates_unknown"]} not measured</p>',
        '<p class="sub">What this is: one measured snapshot of the Recall Brain across availability, latency, accuracy, '
        'freshness, integrity, authorization, privacy, and cost, taken through the public MCP with an owner-scoped read '
        'token. For whoever operates or changes the brain. Every number is content-free: counts, rates, milliseconds, '
        'hashed source identities. Produced by <code>python -m evals.systems_card run</code> in parcha-skills/recall.</p>',
        '<div class="grid">',
    ]
    for name, dimension in card["dimensions"].items():
        probes = dimension["probes"]
        gates = [g for p in probes for g in p["gates"]]
        passed = sum(1 for g in gates if g["passed"] is True)
        parts.append(
            f'<div class="dim"><div class="name">{html.escape(name)}</div>'
            f'<div class="status">{_pill(dimension["status"])}</div>'
            f'<div class="sub">{len(probes)} probe(s) · {passed}/{len(gates)} gates</div></div>'
        )
    parts.append("</div>")

    if history:
        parts.append("<h2>History</h2><div class=\"hist\">")
        keys = sorted({k for row in history for k in row if k not in {"generated_at", "overall", "gates_failed", "git_sha"}})
        for key in keys:
            values = [row.get(key) for row in history]
            latest = values[-1] if values else None
            parts.append(f"<div>{html.escape(key)}<br>{_spark(values)} {_fmt(latest)}</div>")
        parts.append("</div>")

    for name, dimension in card["dimensions"].items():
        parts.append(f"<h2>{html.escape(name)} {_pill(dimension['status'])}</h2>")
        for probe in dimension["probes"]:
            parts.append(f'<div class="probe"><h3>{html.escape(probe["name"])} {_pill(probe["status"])}'
                         f'<span class="sub">{probe["samples"]} samples · {probe["duration_ms"]/1000:.1f}s</span></h3>')
            if probe["gates"]:
                parts.append("<table><tr><th>gate</th><th>observed</th><th>threshold</th><th>result</th></tr>")
                for gate in probe["gates"]:
                    verdict = "pass" if gate["passed"] else ("fail" if gate["passed"] is False else "n/a")
                    color = STATUS_COLOR["ok"] if verdict == "pass" else (STATUS_COLOR["failed"] if verdict == "fail" else STATUS_COLOR["skipped"])
                    parts.append(
                        f'<tr><td>{html.escape(gate["metric"])}</td><td class="num">{_fmt(gate["observed"])}</td>'
                        f'<td class="num">{html.escape(gate["op"])} {_fmt(gate["threshold"])}</td>'
                        f'<td style="color:{color}">{verdict}</td></tr>'
                    )
                parts.append("</table>")
            metrics = {k: v for k, v in probe["metrics"].items() if not isinstance(v, dict)}
            nested = {k: v for k, v in probe["metrics"].items() if isinstance(v, dict)}
            if metrics:
                parts.append("<details><summary>metrics</summary><table>")
                for key, value in sorted(metrics.items()):
                    parts.append(f"<tr><td>{html.escape(key)}</td><td class=\"num\">{_fmt(value)}</td></tr>")
                parts.append("</table></details>")
            for key, value in nested.items():
                parts.append(f"<details><summary>{html.escape(key)}</summary><table>")
                for sub_key, sub_value in sorted(value.items()):
                    parts.append(f"<tr><td>{html.escape(str(sub_key))}</td><td>{_fmt(sub_value)}</td></tr>")
                parts.append("</table></details>")
            if probe["notes"]:
                parts.append('<div class="notes">' + "<br>".join(html.escape(n) for n in probe["notes"]) + "</div>")
            parts.append("</div>")
    parts.append("</main>")
    return "\n".join(parts) + "\n"
