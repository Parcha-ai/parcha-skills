"""Availability: is the brain up, ready, and answering the MCP handshake."""
from __future__ import annotations

import time
import urllib.request

from .model import Gate, ProbeResult, summarize_latency
from .probes import ProbeContext


def _http_get(url: str, timeout: float) -> tuple[int, bytes]:
    request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - https only
            return response.status, response.read(65536)
    except urllib.error.HTTPError as exc:
        return exc.code, b""


class AvailabilityProbe:
    name = "availability.endpoints"
    dimension = "availability"

    def __init__(self, *, samples: int = 5, interval_seconds: float = 0.5) -> None:
        self.samples = samples
        self.interval_seconds = interval_seconds

    def run(self, context: ProbeContext) -> ProbeResult:
        getter = context.http_get or _http_get
        origin = context.base_url.rsplit("/mcp", 1)[0]
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        statuses: dict[str, list[int]] = {"healthz": [], "readyz": []}
        latencies: dict[str, list[float]] = {"healthz": [], "readyz": [], "ping": []}
        busy = 0
        for _ in range(self.samples):
            for path in ("healthz", "readyz"):
                started = time.monotonic()
                try:
                    status, body = getter(f"{origin}/{path}", 15.0)
                except Exception:  # transport failure counts as down
                    status, body = 0, b""
                latencies[path].append((time.monotonic() - started) * 1000.0)
                statuses[path].append(status)
                if path == "readyz" and b'"busy"' in body:
                    busy += 1
            ping = context.client.ping()
            latencies["ping"].append(ping.elapsed_ms)
            time.sleep(self.interval_seconds)
        ping_ok = sum(1 for c in context.client.calls[-self.samples:] if c.tool == "ping" and c.ok)
        n = self.samples
        result.samples = n
        result.metrics = {
            "healthz_success_rate": sum(1 for s in statuses["healthz"] if s == 200) / n,
            "readyz_success_rate": sum(1 for s in statuses["readyz"] if s == 200) / n,
            "readyz_busy_rate": busy / n,
            "mcp_ping_success_rate": ping_ok / n,
            **{f"healthz_{k}": v for k, v in summarize_latency(latencies["healthz"]).items() if k != "n"},
            **{f"readyz_{k}": v for k, v in summarize_latency(latencies["readyz"]).items() if k != "n"},
            **{f"mcp_ping_{k}": v for k, v in summarize_latency(latencies["ping"]).items() if k != "n"},
        }
        result.gates = [
            Gate("healthz_success_rate", ">=", 1.0).evaluate(result.metrics["healthz_success_rate"]),
            Gate("readyz_success_rate", ">=", 1.0).evaluate(result.metrics["readyz_success_rate"]),
            Gate("mcp_ping_success_rate", ">=", 1.0).evaluate(result.metrics["mcp_ping_success_rate"]),
            Gate("mcp_ping_p95_ms", "<=", 2000.0).evaluate(result.metrics["mcp_ping_p95_ms"]),
        ]
        if result.metrics["mcp_ping_success_rate"] < 1.0 or result.metrics["readyz_success_rate"] < 1.0:
            result.status = "failed" if result.metrics["mcp_ping_success_rate"] == 0 else "degraded"
        if busy:
            result.notes.append("readiness reported busy: database pool saturated during the window")
        return result
