from collector.health import build_health_report
from collector.cli import _run_once


def test_ready_codex_health_preserves_archive_coverage() -> None:
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
        "status": "ready",
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
