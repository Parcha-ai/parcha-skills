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
import math
import re
from pathlib import Path
from typing import Any

DOCS_HOST = "https://docs.greppy3.parcha.dev"
MISSING = "–"
CARD_SCHEMA = "recall.systems-card.v1"
MAX_RECONCILE_CHARS = 300
_STATUSES = {"ok", "degraded", "failed"}

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


def _counted(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _metric(row: dict[str, Any], key: str) -> float | None:
    """A metric is absent or a finite number; anything else is a broken card."""
    value = row.get(key)
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"systems card metric {key} is invalid")
    return value


def validate_reconcile_line(reconcile_line: str) -> str:
    """Operator text lands verbatim in a summary, so keep it short and inert."""
    if len(reconcile_line) > MAX_RECONCILE_CHARS or any(
        ord(character) < 32 or ord(character) == 127 for character in reconcile_line
    ):
        raise ValueError("reconcile summary is invalid")
    return reconcile_line


def validate_card(card: Any) -> dict[str, Any]:
    """Refuse a card whose shape would let a summary print something false."""
    if not isinstance(card, dict):
        raise ValueError("systems card has an invalid shape")
    overall = card.get("overall")
    generated_at = card.get("generated_at")
    if (
        not isinstance(overall, dict)
        or not isinstance(card.get("dimensions"), dict)
        or card.get("schema_version") != CARD_SCHEMA
        or not isinstance(generated_at, str)
        or not generated_at
    ):
        raise ValueError("systems card has an invalid shape")
    passed, total = overall.get("gates_passed"), overall.get("gates_total")
    if (
        overall.get("status") not in _STATUSES
        or not _counted(passed)
        or not _counted(total)
        or not 0 <= passed <= total
    ):
        raise ValueError("systems card overall is invalid")
    return card


def _value(row: dict[str, Any], key: str, fmt: str = "{:.0f}") -> str:
    value = _metric(row, key)
    return MISSING if value is None else fmt.format(value)


def _delta(previous: dict[str, Any], now: dict[str, Any], key: str) -> str:
    before, after = _metric(previous, key), _metric(now, key)
    if before is None or after is None or before == after:
        return ""
    return f" ({'+' if after > before else ''}{after - before:.0f})"


def failed_gates(card: dict[str, Any]) -> list[str]:
    failed = []
    for dimension in card["dimensions"].values():
        if not isinstance(dimension, dict) or not isinstance(dimension.get("probes"), list):
            raise ValueError("systems card dimension is invalid")
        for probe in dimension["probes"]:
            if (
                not isinstance(probe, dict)
                or not isinstance(probe.get("name"), str)
                or not isinstance(probe.get("gates"), list)
            ):
                raise ValueError("systems card probe is invalid")
            for gate in probe["gates"]:
                if not isinstance(gate, dict) or not isinstance(gate.get("metric"), str):
                    raise ValueError("systems card gate is invalid")
                passed = gate.get("passed")
                if passed is not None and not isinstance(passed, bool):
                    raise ValueError("systems card gate is invalid")
                if passed is False:
                    failed.append(f"{probe['name'].split('.')[-1]}.{gate['metric']}")
    return failed


def summary_lines(
    card: dict[str, Any],
    history: list[dict[str, Any]],
    *,
    git_sha: str,
    date: str,
    reconcile_line: str = "",
) -> list[str]:
    validate_card(card)
    validate_reconcile_line(reconcile_line)
    if not history:
        raise ValueError("systems card history is empty")
    if not all(isinstance(row, dict) for row in history):
        raise ValueError("systems card history has an invalid shape")
    now = history[-1]
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
