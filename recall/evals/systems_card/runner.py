"""Assemble the systems card: run probes, evaluate gates, write JSON/HTML/history."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .accuracy import SyntheticSuiteProbe, TruthBoundaryProbe
from .availability import AvailabilityProbe
from .churn import ProjectionChurnProbe
from .corpus import AuthorizationProbe, FreshnessProbe, ScanConsistencyProbe, SecretScanProbe
from .cost import PlanetScaleCostProbe, StorageProbe
from .forget import ForgetLatencyProbe
from .latency import ArchilPhaseProbe, SearchStageProbe, ToolLatencyProbe
from .mcp_client import McpClient, load_profile
from .model import DIMENSIONS, SCHEMA_VERSION, ProbeResult, dimension_status
from .probes import ProbeContext, timed
from .render import render_html

PROBES: dict[str, Any] = {
    "availability": [AvailabilityProbe],
    "latency": [ToolLatencyProbe, SearchStageProbe, ArchilPhaseProbe],
    "accuracy": [TruthBoundaryProbe, SyntheticSuiteProbe],
    "freshness": [FreshnessProbe, ProjectionChurnProbe],
    "integrity": [ScanConsistencyProbe, ForgetLatencyProbe],
    "authorization": [AuthorizationProbe],
    "privacy": [SecretScanProbe],
    "cost": [PlanetScaleCostProbe, StorageProbe],
}


def git_pin(repo_root: Path) -> dict[str, Any]:
    def run(*args: str) -> str:
        try:
            return subprocess.run(["git", "-C", str(repo_root), *args], capture_output=True, text=True, check=False, timeout=10).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return ""
    return {"git_sha": run("rev-parse", "HEAD") or None, "git_dirty": bool(run("status", "--porcelain"))}


def build_card(
    results: list[ProbeResult],
    *,
    base_url: str,
    started_at: float,
    repo_root: Path,
    options: dict[str, Any],
) -> dict[str, Any]:
    by_dimension: dict[str, list[ProbeResult]] = {name: [] for name in DIMENSIONS}
    for result in results:
        by_dimension.setdefault(result.dimension, []).append(result)
    dimensions = {}
    for name in DIMENSIONS:
        items = by_dimension.get(name, [])
        dimensions[name] = {
            "status": dimension_status(items),
            "probes": [item.as_dict() for item in items],
        }
    gates_total = sum(len(r.gates) for r in results)
    gates_passed = sum(1 for r in results for g in r.gates if g.passed is True)
    gates_failed = sum(1 for r in results for g in r.gates if g.passed is False)
    gates_unknown = gates_total - gates_passed - gates_failed
    overall = "ok"
    statuses = {d["status"] for d in dimensions.values()}
    if "failed" in statuses:
        overall = "failed"
    elif "degraded" in statuses:
        overall = "degraded"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_seconds": round(time.time() - started_at, 1),
        "target": {"mcp_url": base_url, "window": {"since": options.get("since"), "until": options.get("until")}},
        "overall": {
            "status": overall,
            "gates_total": gates_total,
            "gates_passed": gates_passed,
            "gates_failed": gates_failed,
            "gates_unknown": gates_unknown,
        },
        "dimensions": dimensions,
        "pins": {
            **git_pin(repo_root),
            "python": platform.python_version(),
            "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "probe_set": sorted(r.name for r in results),
        },
    }


def history_row(card: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "generated_at": card["generated_at"],
        "overall": card["overall"]["status"],
        "gates_failed": card["overall"]["gates_failed"],
        "git_sha": card["pins"].get("git_sha"),
    }
    picks = {
        "availability.endpoints": ["mcp_ping_p95_ms", "readyz_success_rate"],
        "latency.tools": ["recall_search.p95_ms", "recall_scan.p95_ms", "recall_exec.p95_ms", "error_rate"],
        "latency.search_stages": ["server_p95_ms", "dense_ok_rate", "deadline_exceeded_rate", "arm.dense.p50_ms", "arm.passage_lexical.p50_ms", "arm.sparse_exact.p50_ms"],
        "accuracy.truth_boundary": ["boundary_recall@20", "boundary_mrr", "negative_false_hit_rate"],
        "freshness.source_age": ["newest_age_hours_min", "newest_age_hours_median", "projection_pending"],
        "freshness.projection_churn": ["passages_written_24h", "documents_projected_24h", "passages_unembedded", "embedding_lag_ratio"],
        "integrity.scan_consistency": ["scope_scan_agreement", "objects_unavailable"],
        "integrity.forget_latency": ["forgotten_after_s", "capture_visible_after_s"],
        "authorization.negative_scope": ["leaks"],
        "privacy.secret_scan": ["secret_hits_total"],
        "cost.planetscale": ["invoice_mtd_usd", "storage_iops"],
        "cost.storage": ["postgres_database_gib", "s3_evidence_gib"],
    }
    for dimension in card["dimensions"].values():
        for probe in dimension["probes"]:
            for key in picks.get(probe["name"], []):
                if key in probe["metrics"]:
                    row[f"{probe['name']}.{key}"] = probe["metrics"][key]
    return row


def load_history(path: Path, *, limit: int = 60) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows[-limit:]


def run_card(args: argparse.Namespace) -> dict[str, Any]:
    started = time.time()
    base, token = load_profile(url=args.url, token_file=args.token_file)
    client = McpClient(base, token, timeout_seconds=args.timeout)
    repo_root = Path(args.repo_root).resolve()
    options: dict[str, Any] = {
        "since": args.since,
        "until": args.until,
        "latency_repetitions": args.repetitions,
        "archil_repetitions": args.repetitions,
        "truth_path": args.truth,
        "truth_split": args.truth_split,
        "synthetic_report": args.synthetic_report,
        "repo_root": str(repo_root),
        "foreign_tenant_path": args.foreign_tenant_path,
        "planetscale_org": args.planetscale_org,
        "planetscale_database": args.planetscale_database,
        "active_window_hours": args.active_window_hours,
        "forget_probe": bool(args.forget_probe),
        "metrics_token_file": args.metrics_token_file,
    }
    if args.queries:
        queries_path = Path(args.queries).expanduser()
        options["queries"] = [line.strip() for line in queries_path.read_text().splitlines() if line.strip()]
    context = ProbeContext(client=client, base_url=base, options=options, since=args.since, until=args.until, private_dir=args.private_dir)
    selected = set(args.dimensions.split(",")) if args.dimensions else set(DIMENSIONS)
    results: list[ProbeResult] = []
    for dimension in DIMENSIONS:
        if dimension not in selected:
            continue
        for probe_type in PROBES[dimension]:
            probe = probe_type()
            print(f"[systems-card] {probe.name} ...", file=sys.stderr, flush=True)
            result = timed(probe, context)
            print(f"[systems-card] {probe.name} -> {result.status} ({result.duration_ms/1000:.1f}s)", file=sys.stderr, flush=True)
            results.append(result)
    card = build_card(results, base_url=base, started_at=started, repo_root=repo_root, options=options)
    out_dir = Path(args.output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "card.json").write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    history_path = out_dir / "history.jsonl"
    with history_path.open("a") as handle:
        handle.write(json.dumps(history_row(card), sort_keys=True) + "\n")
    (out_dir / "card.html").write_text(render_html(card, load_history(history_path)))
    return card


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-systems-card")
    commands = value.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run every probe against a live brain and write the card")
    run.add_argument("--url", help="MCP URL (default: RECALL_URL or ~/.config/recall-brain/client.json)")
    run.add_argument("--token-file", help="mode-0600 JSON {\"token\": ...} (default: RECALL_TOKEN_FILE or client profile)")
    run.add_argument("--metrics-token-file", help="mode-0600 JSON {\"token\": ...} with the metrics scope, for probes that read /metrics (default: RECALL_METRICS_TOKEN_FILE)")
    run.add_argument("--output-dir", required=True)
    run.add_argument("--private-dir", help="owner-only directory for per-case rankings (outside git)")
    run.add_argument("--truth", help="owner-private agentic truth JSONL (60 approved cases)")
    run.add_argument("--truth-split", default="validation", choices=("optimize", "validation", "test", "all"))
    run.add_argument("--synthetic-report", help="path to a recall.retrieval-eval.v1 report to surface")
    run.add_argument("--queries", help="text file, one latency query per line (default: built-in generic set)")
    run.add_argument("--since")
    run.add_argument("--until")
    run.add_argument("--repetitions", type=int, default=3)
    run.add_argument("--timeout", type=float, default=90.0)
    run.add_argument("--dimensions", help="comma list to restrict, e.g. availability,latency")
    run.add_argument("--foreign-tenant-path", help="e.g. /mcp/brains/tenant:company:other; expected to reject the token")
    run.add_argument("--planetscale-org")
    run.add_argument("--planetscale-database")
    run.add_argument("--active-window-hours", type=float, default=72.0)
    run.add_argument("--forget-probe", action="store_true", help="capture, then forget, one synthetic memory and time its disappearance from search (writes to the brain)")
    run.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    render = commands.add_parser("render", help="re-render card.html from an existing card.json")
    render.add_argument("--output-dir", required=True)
    return value


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "run":
        card = run_card(args)
        overall = card["overall"]
        print(json.dumps({"status": overall["status"], "gates_passed": overall["gates_passed"], "gates_failed": overall["gates_failed"], "output_dir": args.output_dir}, sort_keys=True))
        return 0 if overall["status"] != "failed" else 1
    out_dir = Path(args.output_dir).expanduser()
    card = json.loads((out_dir / "card.json").read_text())
    (out_dir / "card.html").write_text(render_html(card, load_history(out_dir / "history.jsonl")))
    print(json.dumps({"rendered": str(out_dir / "card.html")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
