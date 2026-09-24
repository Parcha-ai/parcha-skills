from collector.health import build_health_report
from collector.cli import _run_once


def test_backfilling_codex_health_preserves_archive_coverage() -> None:
    report = build_health_report(
        "codex",
        {
            "collector_version": 4,
            "scan_complete": True,
            "pending": 0,
            "dead": 0,
            "coverage_percent": 100.0,
            "archive_coverage_percent": 98.5,
            "archive_backlog": 3,
            "last_success_epoch": 1_800_000_000,
            "last_error_code": None,
            "running": False,
        },
    )

    assert report == {
        "schema_version": 1,
        "collector_kind": "codex",
        "collector_version": 4,
        "status": "backfilling",
        "scan_complete": True,
        "pending_records": 0,
        "dead_records": 0,
        "coverage_percent": 100.0,
        "archive_coverage_percent": 98.5,
        "archive_backlog": 3,
        "last_success_epoch": 1_800_000_000,
        "last_error_code": None,
    }


def test_non_codex_health_is_content_free_and_degraded_on_error() -> None:
    report = build_health_report(
        "claude",
        {
            "scan_complete": False,
            "pending": 2,
            "dead": 1,
            "coverage_percent": 75.0,
            "last_error_code": "scan_failed",
        },
    )

    assert report["status"] == "degraded"
    assert report["archive_coverage_percent"] is None
    assert report["archive_backlog"] is None
    assert set(report) == {
        "schema_version",
        "collector_kind",
        "collector_version",
        "status",
        "scan_complete",
        "pending_records",
        "dead_records",
        "coverage_percent",
        "archive_coverage_percent",
        "archive_backlog",
        "last_success_epoch",
        "last_error_code",
    }


def test_long_running_collector_iteration_publishes_health() -> None:
    class Collector:
        def scan(self):
            return {"files_seen": 1}

        def flush(self):
            return {"acked": 2}

        def doctor(self, *, include_dead_letters):
            assert include_dead_letters is False
            return {
                "collector_version": 1,
                "scan_complete": True,
                "pending": 0,
                "dead": 0,
                "coverage_percent": 100.0,
            }

    class Writer:
        report = None

        def report_health(self, report):
            self.report = report
            return {"schema_version": 1, "status": "accepted"}

    writer = Writer()
    result = _run_once(Collector(), writer, "codex")

    assert writer.report["collector_kind"] == "codex"
    assert writer.report["status"] == "ready"
    assert result["health_report"]["status"] == "accepted"


def test_completed_scan_with_missing_files_is_not_ready() -> None:
    report = build_health_report("codex", {
        "scan_complete": True, "coverage_percent": 99.7,
    })
    assert report["status"] == "degraded"
    assert report["last_error_code"] == "collector_coverage_incomplete"


def test_unfinished_scan_with_missing_files_is_backfilling() -> None:
    report = build_health_report("codex", {
        "scan_complete": False, "coverage_percent": 90,
    })
    assert report["status"] == "backfilling"
    assert report["last_error_code"] is None


def test_known_uncollected_records_cannot_be_hidden_by_empty_outbox() -> None:
    for field, code in (
        ("identity_conflicts", "collector_identity_conflict"),
        ("quarantined_files", "collector_quarantined_files"),
        ("dead_letter_count", "collector_parse_errors"),
    ):
        report = build_health_report("codex", {
            "scan_complete": True, "coverage_percent": 100,
            "pending": 0, "dead": 0, field: 1,
        })
        assert report["status"] == "degraded", field
        assert report["last_error_code"] == code, field


def test_upload_backlog_is_not_ready_after_scan_finishes() -> None:
    report = build_health_report("claude", {
        "scan_complete": True, "coverage_percent": 100, "pending": 8,
    })
    assert report["status"] == "backfilling"


def test_disconnected_archive_cannot_report_ready() -> None:
    report = build_health_report("codex", {
        "scan_complete": True, "coverage_percent": 100,
        "archive_root_available": False,
    })
    assert report["status"] == "degraded"
    assert report["last_error_code"] == "collector_archive_unavailable"


def test_existing_error_remains_primary_and_recovery_clears_health_error() -> None:
    doctor = {"scan_complete": True, "coverage_percent": 100,
              "identity_conflicts": 1, "last_error_code": "brain_unauthorized"}
    assert build_health_report("codex", doctor)["last_error_code"] == "brain_unauthorized"
    doctor.update(identity_conflicts=0, last_error_code=None)
    recovered = build_health_report("codex", doctor)
    assert recovered["status"] == "ready"
    assert recovered["last_error_code"] is None
