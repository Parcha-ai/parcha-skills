"""Integrity: how long a forgotten memory takes to disappear from search.

The probe writes one synthetic memory through ``recall_capture``, waits until
``recall_search`` can find it (ingest-to-searchable freshness), forgets it
through ``recall_forget``, then waits until search no longer returns it and
its receipt no longer resolves. It is off by default because it writes to the
brain; ``--forget-probe`` turns it on. The receipt and the memory body live in
local variables only and never reach metrics or notes.
"""
from __future__ import annotations

import secrets
import time
from typing import Any, Callable

from .model import Gate, ProbeResult
from .probes import ProbeContext

NONCE_PREFIX = "systemscard-forget-probe"
TITLE = "systems card forget probe"
PROVENANCE_URI = "manual://systems-card/forget-probe"
TAGS = ["systems-card"]


def result_receipts(search_result: dict[str, Any]) -> list[str]:
    """Every receipt string a search result points at, in rank order."""
    receipts: list[str] = []
    results = search_result.get("results")
    if not isinstance(results, list):
        return receipts
    for item in results:
        if not isinstance(item, dict):
            continue
        direct = item.get("receipt")
        if isinstance(direct, str) and direct:
            receipts.append(direct)
        ranges = item.get("matching_ranges")
        if not isinstance(ranges, list):
            continue
        for matching_range in ranges:
            if not isinstance(matching_range, dict):
                continue
            values = matching_range.get("receipts")
            if not isinstance(values, list):
                continue
            receipts.extend(value for value in values if isinstance(value, str) and value)
    return receipts


def receipt_matches(candidate: str, captured: str) -> bool:
    """Exact receipt match, or the same event (receipt minus the ``#item`` fragment)."""
    if candidate == captured:
        return True
    event = captured.split("#", 1)[0]
    return candidate == event or candidate.startswith(event + "#")


class ForgetLatencyProbe:
    name = "integrity.forget_latency"
    dimension = "integrity"

    def __init__(
        self,
        *,
        poll_interval_s: float = 15.0,
        visible_timeout_s: float = 900.0,
        absent_timeout_s: float = 900.0,
    ) -> None:
        self.poll_interval_s = poll_interval_s
        self.visible_timeout_s = visible_timeout_s
        self.absent_timeout_s = absent_timeout_s

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        if not context.options.get("forget_probe"):
            result.status = "skipped"
            result.notes.append("forget probe writes to the brain; enable with --forget-probe")
            return result

        options = context.options
        sleep: Callable[[float], None] = options.get("_sleep", time.sleep)
        clock: Callable[[], float] = options.get("_clock", time.monotonic)
        poll_interval = float(options.get("forget_poll_interval_s", self.poll_interval_s))
        visible_timeout = float(options.get("forget_visible_timeout_s", self.visible_timeout_s))
        absent_timeout = float(options.get("forget_absent_timeout_s", self.absent_timeout_s))
        client = context.client

        # Local only: the nonce, body, and receipt never leave this frame.
        nonce = f"{NONCE_PREFIX} {secrets.token_hex(8)}"
        body = (
            f"Synthetic memory written by the Recall systems card to measure forget latency. "
            f"Marker: {nonce}. It carries no user content and is forgotten by the same probe."
        )
        polls = 0

        def search_sees_receipt(receipt: str) -> bool | None:
            nonlocal polls
            polls += 1
            outcome = client.call_tool("recall_search", {"query": nonce, "limit": 5})
            if not outcome.ok or not outcome.result:
                return None
            return any(receipt_matches(value, receipt) for value in result_receipts(outcome.result))

        # Step 1: capture.
        capture = client.call_tool(
            "recall_capture",
            {
                "schema_version": 1,
                "title": TITLE,
                "body": body,
                "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "tags": list(TAGS),
                "provenance": {"uri": PROVENANCE_URI},
            },
        )
        receipt = capture.result.get("receipt") if capture.ok and capture.result else None
        capture_ok = isinstance(receipt, str) and bool(receipt)
        result.metrics = {
            "capture_ok": capture_ok,
            "capture_ms": round(capture.elapsed_ms, 1),
            "capture_visible_after_s": None,
            "forget_ok": False,
            "forget_ms": None,
            "forgotten_after_s": None,
            "receipt_unresolvable": None,
            "polls": 0,
            "poll_interval_s": poll_interval,
        }
        if not capture_ok:
            result.status = "failed"
            result.notes.append(f"capture failed ({capture.error or 'no receipt'}); nothing was written")
            result.gates = self._gates(result.metrics)
            return result
        assert isinstance(receipt, str)

        # Step 2: wait until search can see the capture.
        captured_at = clock()
        visible_after: float | None = None
        search_errors = 0
        while True:
            seen = search_sees_receipt(receipt)
            if seen is None:
                search_errors += 1
            elif seen:
                visible_after = clock() - captured_at
                break
            if clock() - captured_at >= visible_timeout:
                break
            sleep(poll_interval)
        result.metrics["capture_visible_after_s"] = None if visible_after is None else round(visible_after, 1)
        if visible_after is None:
            result.status = "degraded"
            result.notes.append(f"capture never became searchable within {visible_timeout:.0f}s; forgetting anyway")

        # Step 3: forget.
        forget = client.call_tool("recall_forget", {"receipt": receipt})
        forgotten_at = clock()
        result.metrics["forget_ok"] = bool(forget.ok)
        result.metrics["forget_ms"] = round(forget.elapsed_ms, 1)
        if not forget.ok:
            result.status = "failed"
            result.notes.append(f"forget failed ({forget.error or 'unknown_error'}); the synthetic memory may remain in the brain")
            result.metrics["polls"] = polls
            result.metrics["search_errors"] = search_errors
            result.gates = self._gates(result.metrics)
            return result

        # Step 4: wait until search no longer returns it.
        forgotten_after: float | None = None
        seen_after_forget = False
        while True:
            seen = search_sees_receipt(receipt)
            if seen is None:
                search_errors += 1
            elif seen:
                seen_after_forget = True
            elif visible_after is not None or seen_after_forget:
                forgotten_after = clock() - forgotten_at
                break
            if clock() - forgotten_at >= absent_timeout:
                break
            sleep(poll_interval)
        result.metrics["forgotten_after_s"] = None if forgotten_after is None else round(forgotten_after, 1)
        if forgotten_after is None:
            if seen_after_forget:
                result.status = "failed"
                result.notes.append(f"forgotten memory was still searchable after {absent_timeout:.0f}s")
            elif visible_after is None:
                result.notes.append("memory never appeared before or after forget; forget latency not measurable")
        elif visible_after is None:
            result.notes.append("memory first surfaced after forget, then disappeared")

        # Receipt must not resolve any more.
        show = client.call_tool("recall_show", {"target": receipt})
        session = client.call_tool("recall_session_context", {"target": receipt, "before": 0, "after": 0})
        unresolvable = not show.ok and not session.ok
        result.metrics["receipt_unresolvable"] = unresolvable
        if not unresolvable:
            result.status = "failed"
            result.notes.append("forgotten receipt still resolves through show or session_context")

        result.metrics["polls"] = polls
        result.metrics["search_errors"] = search_errors
        result.samples = polls
        result.gates = self._gates(result.metrics)
        return result

    @staticmethod
    def _gates(metrics: dict[str, Any]) -> list[Gate]:
        def as_number(value: Any) -> float | None:
            if value is None:
                return None
            return float(value)

        return [
            Gate("capture_ok", "==", 1.0).evaluate(as_number(metrics.get("capture_ok"))),
            Gate("capture_visible_after_s", "<=", 900.0).evaluate(as_number(metrics.get("capture_visible_after_s"))),
            Gate("forget_ok", "==", 1.0).evaluate(as_number(metrics.get("forget_ok"))),
            Gate("forgotten_after_s", "<=", 600.0).evaluate(as_number(metrics.get("forgotten_after_s"))),
            Gate("receipt_unresolvable", "==", 1.0).evaluate(as_number(metrics.get("receipt_unresolvable"))),
        ]
