from __future__ import annotations

from typing import Any, Mapping


def build_health_report(
    harness: str,
    doctor: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the content-free health contract shared by every collector CLI."""

    status = (
        "degraded"
        if doctor.get("last_error_code") or doctor.get("dead", 0)
        else "running"
        if doctor.get("running", False)
        else "backfilling"
        if not doctor.get("scan_complete", False)
        else "ready"
    )
    return {
        "schema_version": 1,
        "collector_kind": harness,
        "collector_version": doctor.get("collector_version", 1),
        "status": status,
        "scan_complete": doctor.get("scan_complete", False),
        "pending_records": doctor.get("pending", 0),
        "dead_records": doctor.get("dead", 0),
        "coverage_percent": doctor.get("coverage_percent", 0.0),
        "archive_coverage_percent": (
            doctor.get("archive_coverage_percent") if harness == "codex" else None
        ),
        "archive_backlog": (
            doctor.get("archive_backlog") if harness == "codex" else None
        ),
        "last_success_epoch": doctor.get("last_success_epoch") or None,
        "last_error_code": doctor.get("last_error_code"),
    }
