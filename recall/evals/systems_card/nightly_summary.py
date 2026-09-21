"""The nightly card summary line, built from card.json plus history.jsonl.

The nightly cron used to build this text inline, and for months it printed
`latency.tools.recall_search.p95_ms` under a "p50" label, so every summary and
everything quoting one understated the tail. Percentile labels here are derived
from the metric key instead of typed next to it, which makes that class of
mislabel unrepresentable rather than merely fixed once.

Content-free: only the aggregate numbers already in the card.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

DOCS_HOST = "https://docs.greppy3.parcha.dev"
MISSING = "–"

SEARCH_LATENCY_MS = "latency.tools.recall_search.p95_ms"
SERVER_LATENCY_MS = "latency.search_stages.server_p95_ms"
DEADLINE_RATE = "latency.search_stages.deadline_exceeded_rate"
RECALL_AT_20 = "accuracy.truth_boundary.boundary_recall@20"
BOUNDARY_MRR = "accuracy.truth_boundary.boundary_mrr"
NEWEST_AGE_HOURS = "freshness.source_age.newest_age_hours_min"
PROJECTION_PENDING = "freshness.source_age.projection_pending"
LEAKS = "authorization.negative_scope.leaks"
SECRET_HITS = "privacy.secret_scan.secret_hits_total"
INVOICE_MTD_USD = "cost.planetscale.invoice_mtd_usd"

_PERCENTILE = re.compile(r"(?:^|[._])(p\d{1,2})(?:_ms)?$")


def percentile_label(metric_key: str) -> str:
    """The percentile a latency metric actually reports, read off its own key."""
    match = _PERCENTILE.search(metric_key)
    if match is None:
        raise ValueError(f"{metric_key} carries no percentile to label")
    return match.group(1)


def _value(row: dict[str, Any], key: str, fmt: str = "{:.0f}") -> str:
    value = row.get(key)
    if value is None:
        return MISSING
    return fmt.format(value) if isinstance(value, (int, float)) else str(value)


def _delta(previous: dict[str, Any], now: dict[str, Any], key: str) -> str:
    before, after = previous.get(key), now.get(key)
    if not isinstance(before, (int, float)) or not isinstance(after, (int, float)):
        return ""
    if before == after:
        return ""
    return f" ({'+' if after > before else ''}{after - before:.0f})"


def failed_gates(card: dict[str, Any]) -> list[str]:
    return [
        f"{probe['name'].split('.')[-1]}.{gate['metric']}"
        for dimension in card["dimensions"].values()
        for probe in dimension["probes"]
        for gate in probe["gates"]
        if gate["passed"] is False
    ]


def summary_lines(
    card: dict[str, Any],
    history: list[dict[str, Any]],
    *,
    git_sha: str,
    date: str,
    reconcile_line: str = "",
) -> list[str]:
    now = history[-1] if history else {}
    previous = history[-2] if len(history) > 1 else now
    overall = card["overall"]
    search = percentile_label(SEARCH_LATENCY_MS)
    server = percentile_label(SERVER_LATENCY_MS)
    lines = [
        f"Recall systems card {date} · {overall['status'].upper()}"
        f" · gates {overall['gates_passed']}/{overall['gates_total']} · git {git_sha}",
        f"search {search} {_value(now, SEARCH_LATENCY_MS)} ms{_delta(previous, now, SEARCH_LATENCY_MS)}"
        f" · server {server} {_value(now, SERVER_LATENCY_MS)} ms"
        f" · deadline rate {_value(now, DEADLINE_RATE, '{:.2f}')}",
        f"recall@20 {_value(now, RECALL_AT_20, '{:.2f}')} · MRR {_value(now, BOUNDARY_MRR, '{:.2f}')}",
        f"freshness: newest {_value(now, NEWEST_AGE_HOURS, '{:.1f}')} h"
        f" · pending {_value(now, PROJECTION_PENDING)}",
        f"leaks {_value(now, LEAKS)} · secrets {_value(now, SECRET_HITS)}"
        f" · PlanetScale MTD ${_value(now, INVOICE_MTD_USD)}",
    ]
    if reconcile_line:
        lines.append(reconcile_line)
    failed = failed_gates(card)
    if failed:
        lines.append("red: " + ", ".join(failed[:8]))
    lines.append(f"{DOCS_HOST}/{date}-recall-systems-card.html")
    return lines


def summary_from_output_dir(
    output_dir: Path, *, git_sha: str, date: str, reconcile_line: str = ""
) -> str:
    card = json.loads((output_dir / "card.json").read_text())
    history_path = output_dir / "history.jsonl"
    history = [
        json.loads(line)
        for line in history_path.read_text().splitlines()
        if line.strip()
    ] if history_path.exists() else []
    return "\n".join(
        summary_lines(card, history, git_sha=git_sha, date=date, reconcile_line=reconcile_line)
    )
