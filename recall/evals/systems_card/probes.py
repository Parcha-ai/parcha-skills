"""Probe protocol shared by every systems-card dimension."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from .mcp_client import McpClient
from .model import ProbeResult


@dataclass
class ProbeContext:
    client: McpClient
    base_url: str
    options: dict[str, Any] = field(default_factory=dict)
    since: str | None = None
    until: str | None = None
    http_get: Callable[[str, float], tuple[int, bytes]] | None = None
    private_dir: str | None = None


class Probe(Protocol):
    name: str
    dimension: str

    def run(self, context: ProbeContext) -> ProbeResult: ...


def timed(probe: Probe, context: ProbeContext) -> ProbeResult:
    started = time.monotonic()
    try:
        result = probe.run(context)
    except Exception as exc:  # a probe must never take the card down
        result = ProbeResult(
            name=probe.name,
            dimension=probe.dimension,
            status="failed",
            notes=[f"probe raised {type(exc).__name__}"],
        )
    result.duration_ms = (time.monotonic() - started) * 1000.0
    return result
