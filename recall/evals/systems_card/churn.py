"""Freshness: how much of the corpus the projection worker rewrites per day.

Reads the brain's Prometheus ``/metrics`` endpoint with a metrics-scoped bearer
(``RECALL_METRICS_TOKEN_FILE``, mode-0600 JSON ``{"token": ...}``). Without a
token the probe is skipped: it never needs worker-host credentials and never
reads passage content.
"""
from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

from .mcp_client import McpClientError, _private_json
from .model import Gate, ProbeResult
from .probes import ProbeContext

MetricsGetter = Callable[[str, dict[str, str], float], tuple[int, bytes]]

GAUGES = {
    "passages_total": "recall_passages_total",
    "passages_unembedded": "recall_passages_unembedded",
    "passages_written_24h": "recall_passages_written_24h",
    "documents_projected_24h": "recall_passage_documents_projected_24h",
}
PROCESS_TOTALS = {
    "passages_written_total": "recall_projection_passages_written_total",
    "documents_projected_total": "recall_projection_documents_projected_total",
    "passages_embedded_total": "recall_projection_passages_embedded_total",
    "parquet_rows_written_total": "recall_projection_parquet_rows_written_total",
    "bodies_thinned_total": "recall_projection_bodies_thinned_total",
}
# Baseline 2026-09-10: ~150k passages rewritten/day for ~320 documents; the
# gate states the target, so it fails on that baseline by design.
MAX_PASSAGES_WRITTEN_24H = 20_000.0
MAX_EMBEDDING_LAG_RATIO = 0.02


def _metrics_get(url: str, headers: dict[str, str], timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - https only
            return response.status, response.read(1_000_000)
    except urllib.error.HTTPError as exc:
        return exc.code, b""


def parse_prometheus(text: str) -> dict[str, float]:
    """Sample name -> value for unlabeled samples; comments and junk are ignored."""
    samples: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or "{" in parts[0]:
            continue
        try:
            samples[parts[0]] = float(parts[1])
        except ValueError:
            continue
    return samples


def load_metrics_token(path: str | None) -> str | None:
    if not path:
        return None
    value = _private_json(Path(path).expanduser())
    token = value.get("token")
    if not isinstance(token, str) or not token:
        raise McpClientError("metrics token file has no token")
    return token


class ProjectionChurnProbe:
    name = "freshness.projection_churn"
    dimension = "freshness"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        token_path = context.options.get("metrics_token_file") or os.environ.get("RECALL_METRICS_TOKEN_FILE")
        token = load_metrics_token(token_path)
        if token is None:
            result.status = "skipped"
            result.notes.append("RECALL_METRICS_TOKEN_FILE not set; projection churn not measured")
            return result
        getter: MetricsGetter = context.options.get("_metrics_get") or _metrics_get
        origin = context.base_url.rsplit("/mcp", 1)[0]
        headers = {"Authorization": f"Bearer {token}", "Accept": "text/plain"}
        try:
            status, body = getter(f"{origin}/metrics", headers, 30.0)
        except Exception:  # transport failure: content-free
            status, body = 0, b""
        if status != 200:
            result.status = "failed"
            result.notes.append(f"/metrics returned status {status}")
            return result
        samples = parse_prometheus(body.decode("utf-8", "replace"))
        missing = [name for name in GAUGES.values() if name not in samples]
        if missing:
            result.status = "failed"
            result.notes.append(f"/metrics lacks {len(missing)} churn gauges; server predates projection churn counters")
            return result
        metrics: dict[str, float | int | str | None] = {key: int(samples[name]) for key, name in GAUGES.items()}
        for key, name in PROCESS_TOTALS.items():
            if name in samples:
                metrics[key] = int(samples[name])
        unembedded = metrics["passages_unembedded"]
        total = metrics["passages_total"]
        lag_ratio: float | None = None
        if isinstance(unembedded, int) and isinstance(total, int) and unembedded >= 0 and total > 0:
            lag_ratio = round(unembedded / total, 4)
        elif unembedded == -1:
            result.notes.append("semantic runtime not configured on the server; embedding lag unknown")
        metrics["embedding_lag_ratio"] = lag_ratio
        result.metrics = metrics
        result.samples = len(samples)
        result.gates = [
            Gate("passages_written_24h", "<=", MAX_PASSAGES_WRITTEN_24H, note="baseline ~150k/day; target after H0 rewrite").evaluate(float(metrics["passages_written_24h"])),
            Gate("embedding_lag_ratio", "<=", MAX_EMBEDDING_LAG_RATIO, note="unembedded / total passages").evaluate(lag_ratio),
        ]
        if any(g.passed is False for g in result.gates):
            result.status = "degraded"
        return result
