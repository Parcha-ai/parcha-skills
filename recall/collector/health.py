from __future__ import annotations

from typing import Any, Mapping


def build_health_report(
    harness: str,
    doctor: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the content-free health contract shared by every collector CLI."""
    error = doctor.get("last_error_code")
    if not error:
        for field, code in (
            ("identity_conflicts", "collector_identity_conflict"),
            ("quarantined_files", "collector_quarantined_files"),
            ("dead_letter_count", "collector_parse_errors"),
        ):
            if doctor.get(field, 0):
                error = code
                break
    if not error and harness == "codex" and doctor.get("archive_root_available") is False:
        error = "collector_archive_unavailable"
    backlog = bool(doctor.get("pending", 0) or doctor.get("archive_backlog", 0))
    coverage = doctor.get("coverage_percent", 0.0)
    archive_coverage = doctor.get("archive_coverage_percent") if harness == "codex" else None
    missing_files = coverage < 100 or (archive_coverage is not None and archive_coverage < 100)
    if not error and doctor.get("scan_complete", False) and missing_files and not backlog:
        error = "collector_coverage_incomplete"
    status = (
        "degraded"
        if error or doctor.get("dead", 0)
        else "running"
        if doctor.get("running", False)
        else "backfilling"
        if backlog or not doctor.get("scan_complete", False)
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
        "last_error_code": error,
    }
