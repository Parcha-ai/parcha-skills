"""Content-free data model for the systems card."""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import Any

SCHEMA_VERSION = "recall.systems-card.v1"

DIMENSIONS = (
    "availability",
    "latency",
    "accuracy",
    "freshness",
    "integrity",
    "authorization",
    "privacy",
    "cost",
)


@dataclass
class Gate:
    metric: str
    op: str  # one of <=, >=, ==
    threshold: float
    passed: bool | None = None
    observed: float | None = None
    note: str = ""

    def evaluate(self, observed: float | None) -> "Gate":
        self.observed = observed
        if observed is None or (isinstance(observed, float) and math.isnan(observed)):
            self.passed = None
            return self
        if self.op == "<=":
            self.passed = observed <= self.threshold
        elif self.op == ">=":
            self.passed = observed >= self.threshold
        elif self.op == "==":
            self.passed = observed == self.threshold
        else:
            raise ValueError("unsupported gate operator")
        return self


@dataclass
class ProbeResult:
    name: str
    dimension: str
    status: str  # ok | degraded | failed | skipped
    metrics: dict[str, float | int | str | None] = field(default_factory=dict)
    gates: list[Gate] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    samples: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "dimension": self.dimension,
            "status": self.status,
            "duration_ms": round(self.duration_ms, 3),
            "samples": self.samples,
            "metrics": self.metrics,
            "gates": [
                {
                    "metric": g.metric,
                    "op": g.op,
                    "threshold": g.threshold,
                    "observed": g.observed,
                    "passed": g.passed,
                    "note": g.note,
                }
                for g in self.gates
            ],
            "notes": self.notes,
        }


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def summarize_latency(values_ms: list[float]) -> dict[str, float | int | None]:
    if not values_ms:
        return {"n": 0, "p50_ms": None, "p95_ms": None, "max_ms": None, "mean_ms": None}
    return {
        "n": len(values_ms),
        "p50_ms": round(percentile(values_ms, 50) or 0.0, 1),
        "p95_ms": round(percentile(values_ms, 95) or 0.0, 1),
        "max_ms": round(max(values_ms), 1),
        "mean_ms": round(statistics.fmean(values_ms), 1),
    }


def dimension_status(results: list[ProbeResult]) -> str:
    statuses = {r.status for r in results}
    if not results or statuses == {"skipped"}:
        return "skipped"
    if "failed" in statuses:
        return "failed"
    if "degraded" in statuses:
        return "degraded"
    if any(g.passed is False for r in results for g in r.gates):
        return "degraded"
    return "ok"
