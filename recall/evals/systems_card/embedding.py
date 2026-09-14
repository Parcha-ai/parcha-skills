"""Freshness: embedding lag and the daily embedding budget (H5-3).

Reads the brain's Prometheus ``/metrics`` endpoint with the same
metrics-scoped bearer as the projection churn probe. Reports how many
passages still lack a vector, how many were embedded in the last day, and
how much of the daily cap is left, so an operator sees a stalled or capped
embedding worker on the card before search freshness degrades.
"""
from __future__ import annotations

import os

from .churn import MetricsGetter, _metrics_get, load_metrics_token, parse_prometheus
from .model import Gate, ProbeResult
from .probes import ProbeContext

GAUGES = {
    "passages_unembedded": "recall_passages_unembedded",
    "embedded_today": "recall_embedding_daily_total",
    "daily_cap": "recall_embedding_daily_cap",
}
# One worker cycle of 10 batches x 128 passages drains ~1.3k; 5k is under an
# hour of backlog at the production rate and well inside the 250k count cap.
MAX_PASSAGES_UNEMBEDDED = 5_000.0
MIN_CAP_REMAINING = 1.0


class EmbeddingLagProbe:
    name = "freshness.embedding_lag"
    dimension = "freshness"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        token_path = context.options.get("metrics_token_file") or os.environ.get("RECALL_METRICS_TOKEN_FILE")
        token = load_metrics_token(token_path)
        if token is None:
            result.status = "skipped"
            result.notes.append("RECALL_METRICS_TOKEN_FILE not set; embedding lag not measured")
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
            result.notes.append(f"/metrics lacks {len(missing)} embedding gauges; server predates the embedding ledger")
            return result
        unembedded = int(samples[GAUGES["passages_unembedded"]])
        embedded_today = int(samples[GAUGES["embedded_today"]])
        daily_cap = int(samples[GAUGES["daily_cap"]])
        cap_remaining = max(0, daily_cap - embedded_today)
        result.metrics = {
            "passages_unembedded": unembedded,
            "embedded_today": embedded_today,
            "daily_cap": daily_cap,
            "cap_remaining": cap_remaining,
        }
        result.samples = len(samples)
        unembedded_observed: float | None = float(unembedded)
        if unembedded < 0:
            unembedded_observed = None
            result.notes.append("semantic runtime not configured on the server; embedding lag unknown")
        result.gates = [
            Gate("passages_unembedded", "<=", MAX_PASSAGES_UNEMBEDDED, note="passages without a vector for the current runtime").evaluate(unembedded_observed),
            Gate("cap_remaining", ">=", MIN_CAP_REMAINING, note="daily cap minus passages embedded in the last 24h; 0 means the worker has stopped").evaluate(float(cap_remaining)),
        ]
        if cap_remaining == 0:
            result.notes.append("embedding daily cap reached; the embedding worker is idle until the window rolls")
        if any(g.passed is False for g in result.gates):
            result.status = "degraded"
        return result
