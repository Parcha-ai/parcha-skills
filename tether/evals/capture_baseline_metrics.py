#!/usr/bin/env python3
"""Capture reproducible Tether L0 architecture and operability metrics."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import pathlib
import platform
import re
import shutil
import sqlite3
import statistics
import subprocess  # nosec B404 - fixed local Git invocation only
import sys
import tempfile
import time
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
REPOSITORY = ROOT.parent
TESTS = ROOT / "tests"
sys.path.insert(0, str(TESTS))
from test_bridge import load_runtime  # noqa: E402


COMPONENTS = {
    "runtime_core": ("runtime/*.py",),
    "hermes_plugin": ("runtime/plugin/*.py",),
    "node_cli": ("bin/*.js",),
    "installer": ("install.sh",),
    "herdr_plugin": ("herdr-plugin/*.py",),
    "skills": ("skills/**/*.py",),
    "tests": ("tests/*.py", "tests/*.sh"),
    "docs": ("docs/*.md", "README.md"),
}


def files_for(patterns: tuple[str, ...]) -> list[pathlib.Path]:
    return sorted({path for pattern in patterns for path in ROOT.glob(pattern) if path.is_file()})


def nonblank_noncomment_physical_lines(path: pathlib.Path) -> int:
    count = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith(("#", "//")):
            count += 1
    return count


def git(*args: str) -> str:
    return subprocess.run(  # nosec B603 - fixed binary; reviewed arguments
        ["/usr/bin/git", *args], cwd=REPOSITORY, text=True,
        capture_output=True, check=True
    ).stdout.strip()


def source_inventory(paths: list[pathlib.Path]) -> tuple[dict[str, str], str]:
    inventory = {
        str(path.relative_to(REPOSITORY)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(set(paths))
    }
    joined = "".join(f"{name}\0{value}\n" for name, value in sorted(inventory.items()))
    return inventory, hashlib.sha256(joined.encode()).hexdigest()


def private_hermes_surface() -> dict[str, Any]:
    source = (ROOT / "runtime/hermes_compat.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    required: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "required" and isinstance(node.value, ast.Dict):
                    required = [
                        key.value
                        for key in node.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    ]
    return {
        "exact_hermes_version": "0.19.0",
        "exact_hermes_commit": "b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5",
        "audited_source_paths": len(re.findall(r'^\s+"[^\"]+\.py",?$', source, re.MULTILINE)),
        "validated_adapter_methods": len(required),
        "private_or_replaced_methods": sorted(name for name in required if name.startswith("_") or name in {"connect", "send", "edit_message"}),
        "private_hook_registry_mutations": source.count("_manager") + source.count("_hooks"),
    }


def schema_metrics() -> dict[str, Any]:
    source = (ROOT / "runtime/bridge_runtime.py").read_text(encoding="utf-8")
    tables = sorted(set(re.findall(r"CREATE TABLE IF NOT EXISTS\s+([a-z_]+)", source)))
    state_literals = sorted(set(re.findall(r"(?:state\s*=\s*|to_state=)[\"']([a-z_]+)[\"']", source)))
    outbox_like = [
        name
        for name in tables
        if name in {"bridge_replies", "bridge_roots", "slack_messages", "slack_reconciliations"}
    ]
    return {
        "schema_version": int(re.search(r"SCHEMA_VERSION\s*=\s*(\d+)", source).group(1)),
        "table_count": len(tables),
        "tables": tables,
        "platform_outbox_or_reconciliation_tables": outbox_like,
        "distinct_explicit_state_literals": state_literals,
        "duplicate_endpoint_snapshot_field": "bridges.source_json",
        "endpoint_identity_form": "derived bridges.endpoint_key",
    }


def delivery_health_query_latency() -> dict[str, Any]:
    samples = []
    with tempfile.TemporaryDirectory() as directory:
        home = pathlib.Path(directory)
        runtime = load_runtime(home)
        store = runtime.Store(home / "bridges.db")
        bridge = store.bind(
            store.create(
                {
                    "source_kind": "headless_run",
                    "source": {"run_id": "metrics", "cwd": str(home)},
                    "owner_user_id": "U12345678",
                    "team_id": "T12345678",
                    "channel_id": "C12345678",
                    "idempotency_key": "metrics",
                }
            ).bridge_id,
            "1785000000.000001",
        )
        store.enqueue_event("1785000001.000001", bridge.bridge_id, "first")
        batch = store.claim_event_batch(bridge.bridge_id)
        attempt_id = runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in batch],
            bridge.binding_generation,
        )
        store.prepare_delivery_attempt(
            [item["event_id"] for item in batch],
            bridge.bridge_id,
            bridge.binding_generation,
            attempt_id,
        )
        store.mark_attempt_awaiting_ack(
            attempt_id, bridge.bridge_id, bridge.binding_generation
        )
        store.enqueue_event("1785000002.000001", bridge.bridge_id, "later")
        for _ in range(25):
            started = time.perf_counter_ns()
            health = store.delivery_health()
            samples.append((time.perf_counter_ns() - started) / 1_000_000)
            if health["blocked_bridge_count"] != 1:
                raise RuntimeError("synthetic blocker was not detected")
        with store.connect() as db:
            oldest_created = db.execute(
                "SELECT MIN(created_at) FROM bridge_attempts WHERE state='awaiting_ack'"
            ).fetchone()[0]
    ordered = sorted(samples)
    return {
        "sample_count": len(samples),
        "median_ms": round(statistics.median(samples), 3),
        "p95_ms": round(ordered[int(len(ordered) * 0.95) - 1], 3),
        "max_ms": round(max(samples), 3),
        "oldest_blocked_created_at": oldest_created,
        "operator_visibility": {
            "automatic_notice_emitted": False,
            "on_demand_health_detects": True,
            "unresolved_lists_attempt": False,
        },
        "semantic_limit": "This measures SQLite query latency, not alert time. Awaiting_ack has no automatic age transition or notice.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()

    component_metrics = {}
    measured_paths: list[pathlib.Path] = []
    for component, patterns in COMPONENTS.items():
        paths = files_for(patterns)
        measured_paths.extend(paths)
        component_metrics[component] = {
            "file_count": len(paths),
            "nonblank_noncomment_physical_lines": sum(
                nonblank_noncomment_physical_lines(path) for path in paths
            ),
        }

    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    measured_paths.extend([pathlib.Path(__file__), ROOT / "package.json", ROOT / "package-lock.json"])
    sources, content_digest = source_inventory([path for path in measured_paths if path.is_file()])
    commit = git("rev-parse", "HEAD")
    status = git("status", "--porcelain=v1", "--untracked-files=all")
    runtime_source = (ROOT / "runtime/bridge_runtime.py").read_text(encoding="utf-8")
    node_binary = shutil.which("node")
    if not node_binary:
        raise RuntimeError("node is unavailable")
    report = {
        "schema_version": 2,
        "target_commit": commit,
        "target_tree": git("rev-parse", "HEAD^{tree}"),
        "target_content_sha256": content_digest,
        "candidate_clean": status == "",
        "candidate_status": status.splitlines(),
        "source_sha256": sources,
        "environment": {
            "python": sys.version.split()[0],
            "node": subprocess.run(  # nosec B603 - resolved local executable
                [node_binary, "--version"], text=True, capture_output=True, check=True
            ).stdout.strip(),
            "sqlite": sqlite3.sqlite_version,
            "os": platform.platform(),
            "architecture": platform.machine(),
        },
        "measurement_definition": "Physical nonblank, non-comment lines; static AST/SQL inventory; 25 local SQLite delivery-health queries.",
        "components": component_metrics,
        "hermes_private_coupling": private_hermes_surface(),
        "state_model": schema_metrics(),
        "install_surfaces": {
            "npm_manifest_entries": len(package["files"]),
            "runtime_distribution_roots": [
                "XDG_DATA_HOME/tether",
                "HERMES_HOME/plugins/tether",
                "HOME/.local/bin",
                "Codex skills",
                "Claude Code skills",
                "Herdr plugin",
            ],
            "command_surfaces": ["Node tether CLI", "Python notifier", "local broker socket", "Hermes plugin", "Herdr plugin"],
        },
        "attempt_terminal_coverage": {
            "open_states": ["prepared", "submitting", "uncertain", "awaiting_ack", "replying"],
            "terminal_states": ["delivered", "acknowledged", "cancelled", "failed"],
            "unbounded_state": "awaiting_ack",
            "operator_listed_open_states": ["uncertain"],
        },
        "delivery_health_query_latency": delivery_health_query_latency(),
        "production_incident_observation": {
            "lower_bound_hours": 72,
            "exact_duration_available": False,
            "basis": "Sanitized production timeline: the missed-callback attempt remained blocked from August 14 through August 17 before explicit reconciliation.",
            "alert_time_measured": False,
            "target_slo": "Expose and alert within 60 seconds; resolution stays evidence-based and manual when execution is ambiguous.",
        },
        "observed_versions": {
            "package": package["version"],
            "schema": int(re.search(r"SCHEMA_VERSION\s*=\s*(\d+)", runtime_source).group(1)),
            "broker_protocol": int(re.search(r'"protocol_version":\s*(\d+)', runtime_source).group(1)),
        },
    }
    report["valid"] = report["candidate_clean"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
