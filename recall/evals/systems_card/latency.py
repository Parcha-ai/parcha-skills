"""Latency: per-tool wall clock, server-side search stages, and Archil phases."""
from __future__ import annotations

from typing import Any

from .model import Gate, ProbeResult, summarize_latency
from .probes import ProbeContext

# Generic engineering questions. They carry no private content and exist only
# to exercise the retrieval arms with realistic shapes (identifier, timeline,
# person, cross-source). Owners may override with a private query file.
DEFAULT_QUERIES = (
    "why did the deploy fail and how was it fixed",
    "database migration that changed the schema last month",
    "root cause of the timeout in the worker",
    "what did the team decide about the retry policy",
    "pull request that introduced the new endpoint",
    "cost of the managed database and what drove it",
    "how is authentication handled for the MCP server",
    "which tests were added for the collector",
)

SCAN_PROGRAM = (
    "duckdb -c \"SELECT count(*) AS passages, count(DISTINCT logical_document_id) AS docs "
    "FROM read_parquet('/datasets/*/*/passages-part-*.parquet', union_by_name=true)\""
)
EXEC_PROGRAM = "wc -l /docs/d1/part-*.jsonl | tail -1"


def _samples(context: ProbeContext, key: str, default: int) -> int:
    value = context.options.get(key, default)
    return max(1, int(value))


def _queries(context: ProbeContext) -> list[str]:
    queries = context.options.get("queries")
    if isinstance(queries, list) and queries:
        return [str(q) for q in queries]
    return list(DEFAULT_QUERIES)


def _filters(context: ProbeContext) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if context.since:
        filters["since"] = context.since
    if context.until:
        filters["until"] = context.until
    return filters


class ToolLatencyProbe:
    """Wall-clock latency per MCP tool, first call separated from warm calls."""

    name = "latency.tools"
    dimension = "latency"

    def run(self, context: ProbeContext) -> ProbeResult:
        client = context.client
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        reps = _samples(context, "latency_repetitions", 3)
        queries = _queries(context)
        per_tool: dict[str, list[float]] = {}
        first_call: dict[str, float] = {}
        errors: dict[str, int] = {}
        receipt: str | None = None
        ldoc: str | None = None

        def record(outcome: Any) -> None:
            # Every call is a sample; the first call per tool is also kept
            # separately so cold-path cost stays visible.
            per_tool.setdefault(outcome.tool, []).append(outcome.elapsed_ms)
            first_call.setdefault(outcome.tool, outcome.elapsed_ms)
            if not outcome.ok:
                errors[outcome.tool] = errors.get(outcome.tool, 0) + 1

        filters = _filters(context)
        for rep in range(reps):
            for query in queries:
                outcome = client.call_tool("recall_search", {"query": query, "filters": filters, "limit": 10})
                record(outcome)
                if outcome.ok and outcome.result:
                    for hit in outcome.result.get("results", []):
                        if ldoc is None and isinstance(hit.get("logical_document_id"), str):
                            ldoc = hit["logical_document_id"]
                        for rng in hit.get("matching_ranges", []):
                            for r in rng.get("receipts", []):
                                if receipt is None and isinstance(r, str):
                                    receipt = r
            record(client.call_tool("recall_scope", {"filters": filters, "limit": 40}))
            record(client.call_tool("recall_people", {}))
            if receipt:
                record(client.call_tool("recall_session_context", {"target": receipt, "before": 2, "after": 2}))
                record(client.call_tool("recall_show", {"target": receipt}))
            if ldoc:
                record(client.call_tool(
                    "recall_exec",
                    {"targets": [{"logical_document_id": ldoc, "alias": "d1"}], "program": EXEC_PROGRAM, "timeout_seconds": 20},
                    timeout_seconds=90,
                ))
            record(client.call_tool("recall_scan", {"filters": filters, "program": SCAN_PROGRAM, "timeout_seconds": 60}, timeout_seconds=150))

        metrics: dict[str, Any] = {}
        total_calls = 0
        total_errors = 0
        for tool, values in per_tool.items():
            summary = summarize_latency(values)
            for key, value in summary.items():
                metrics[f"{tool}.{key}"] = value
            metrics[f"{tool}.first_call_ms"] = round(first_call.get(tool, 0.0), 1)
            metrics[f"{tool}.errors"] = errors.get(tool, 0)
            total_calls += len(values)
            total_errors += errors.get(tool, 0)
        metrics["calls"] = total_calls
        metrics["error_rate"] = (total_errors / total_calls) if total_calls else None
        result.samples = total_calls
        result.metrics = metrics
        result.gates = [
            Gate("recall_search.p95_ms", "<=", 5000.0).evaluate(metrics.get("recall_search.p95_ms")),
            Gate("recall_scope.p95_ms", "<=", 3000.0).evaluate(metrics.get("recall_scope.p95_ms")),
            Gate("recall_show.p95_ms", "<=", 2000.0).evaluate(metrics.get("recall_show.p95_ms")),
            Gate("recall_exec.p95_ms", "<=", 30000.0).evaluate(metrics.get("recall_exec.p95_ms")),
            Gate("recall_scan.p95_ms", "<=", 60000.0).evaluate(metrics.get("recall_scan.p95_ms")),
            Gate("error_rate", "<=", 0.02).evaluate(metrics.get("error_rate")),
        ]
        if metrics.get("error_rate") and metrics["error_rate"] > 0.2:
            result.status = "failed"
        elif total_errors:
            result.status = "degraded"
        if receipt is None:
            result.notes.append("no receipt found in search results; show/session_context not measured")
        if ldoc is None:
            result.notes.append("no logical document found in search results; exec not measured")
        context.options["_shared_receipt"] = receipt
        context.options["_shared_ldoc"] = ldoc
        return result


class SearchStageProbe:
    """Server-side search diagnostics: arm health, deadlines, candidate counts."""

    name = "latency.search_stages"
    dimension = "latency"

    def run(self, context: ProbeContext) -> ProbeResult:
        client = context.client
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        queries = _queries(context)
        filters = _filters(context)
        server_ms: list[float] = []
        deadline_exceeded = 0
        dense_ok = 0
        arms = {"dense_candidates": [], "passage_lexical_candidates": [], "sparse_candidates": []}
        arm_ms: dict[str, list[float]] = {}
        clip_ms: list[float] = []
        empty = 0
        n = 0
        strategies: dict[str, int] = {}
        for query in queries:
            outcome = client.call_tool("recall_search", {"query": query, "filters": filters, "limit": 10})
            if not outcome.ok or not outcome.result:
                continue
            n += 1
            diag = outcome.result.get("diagnostics", {})
            if isinstance(diag.get("elapsed_ms"), (int, float)):
                server_ms.append(float(diag["elapsed_ms"]))
            deadline_exceeded += 1 if diag.get("deadline_exceeded") else 0
            dense_ok += 1 if diag.get("dense_status") == "ok" else 0
            strategy = str(diag.get("dense_strategy", "unknown"))
            strategies[strategy] = strategies.get(strategy, 0) + 1
            for key in arms:
                value = diag.get(key)
                if isinstance(value, (int, float)):
                    arms[key].append(float(value))
            for arm, value in (diag.get("arm_elapsed_ms") or {}).items():
                if isinstance(value, (int, float)):
                    arm_ms.setdefault(str(arm), []).append(float(value))
            if isinstance(diag.get("time_clip_elapsed_ms"), (int, float)):
                clip_ms.append(float(diag["time_clip_elapsed_ms"]))
            if not outcome.result.get("results"):
                empty += 1
        result.samples = n
        if n == 0:
            result.status = "failed"
            result.notes.append("no successful search call")
            return result
        metrics: dict[str, Any] = {
            **{f"server_{k}": v for k, v in summarize_latency(server_ms).items() if k != "n"},
            "deadline_exceeded_rate": deadline_exceeded / n,
            "dense_ok_rate": dense_ok / n,
            "empty_result_rate": empty / n,
        }
        for key, values in arms.items():
            metrics[f"{key}.mean"] = round(sum(values) / len(values), 1) if values else None
            metrics[f"{key}.zero_rate"] = (sum(1 for v in values if v == 0) / len(values)) if values else None
        for strategy, count in strategies.items():
            metrics[f"dense_strategy.{strategy}"] = count
        for arm, values in arm_ms.items():
            summary = summarize_latency(values)
            metrics[f"arm.{arm}.p50_ms"] = summary["p50_ms"]
            metrics[f"arm.{arm}.p95_ms"] = summary["p95_ms"]
        if clip_ms:
            summary = summarize_latency(clip_ms)
            metrics["time_clip.p50_ms"] = summary["p50_ms"]
            metrics["time_clip.p95_ms"] = summary["p95_ms"]
        result.metrics = metrics
        result.gates = [
            Gate("deadline_exceeded_rate", "<=", 0.05).evaluate(metrics["deadline_exceeded_rate"]),
            Gate("dense_ok_rate", ">=", 0.95).evaluate(metrics["dense_ok_rate"]),
            Gate("server_p95_ms", "<=", 5000.0).evaluate(metrics.get("server_p95_ms")),
        ]
        if metrics["dense_ok_rate"] < 0.5:
            result.status = "failed"
        elif any(g.passed is False for g in result.gates):
            result.status = "degraded"
        return result


class ArchilPhaseProbe:
    """Archil scan/exec phase timing: where the seconds go inside the sandbox."""

    name = "latency.archil_phases"
    dimension = "latency"

    def run(self, context: ProbeContext) -> ProbeResult:
        client = context.client
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        reps = _samples(context, "archil_repetitions", 3)
        filters = _filters(context)
        phases: dict[str, list[float]] = {}
        totals: dict[str, list[float]] = {"totalMs": [], "queueMs": [], "executeMs": []}
        ok = 0
        for _ in range(reps):
            outcome = client.call_tool("recall_scan", {"filters": filters, "program": SCAN_PROGRAM, "timeout_seconds": 60}, timeout_seconds=150)
            if not outcome.ok or not outcome.result:
                continue
            timing = outcome.result.get("timing", {})
            if outcome.result.get("exit_code") == 0:
                ok += 1
            for key in totals:
                if isinstance(timing.get(key), (int, float)):
                    totals[key].append(float(timing[key]))
            for key, value in (timing.get("phases") or {}).items():
                if isinstance(value, (int, float)):
                    phases.setdefault(key, []).append(float(value))
        result.samples = reps
        metrics: dict[str, Any] = {"scan_exit0_rate": ok / reps}
        for key, values in totals.items():
            summary = summarize_latency(values)
            metrics[f"{key}.p50"] = summary["p50_ms"]
            metrics[f"{key}.p95"] = summary["p95_ms"]
        for key, values in phases.items():
            metrics[f"phase.{key}.p50"] = summarize_latency(values)["p50_ms"]
        result.metrics = metrics
        result.gates = [
            Gate("scan_exit0_rate", ">=", 1.0).evaluate(metrics["scan_exit0_rate"]),
            Gate("queueMs.p95", "<=", 10000.0).evaluate(metrics.get("queueMs.p95")),
            Gate("totalMs.p95", "<=", 60000.0).evaluate(metrics.get("totalMs.p95")),
        ]
        if ok == 0:
            result.status = "failed"
        elif ok < reps:
            result.status = "degraded"
        return result
