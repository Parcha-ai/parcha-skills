import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

from client import cli
from collector.collector import CollectorRuntimeError


class MacCollectorHealthTest(unittest.TestCase):
    def setUp(self):
        self.collector = mock.Mock()
        self.collector.scan.return_value = {"scan_complete": True}
        self.collector.flush.return_value = {"acked": 1}
        self.collector.doctor.return_value = {
            "scan_complete": True, "pending": 0, "coverage_percent": 100,
        }
        self.writer = mock.Mock()
        self.writer.report_health.return_value = {"status": "accepted"}

    def run_cycle(self, harness, *, canonical=True):
        arguments = [
            "recall-brain", "collect", "--endpoint", "https://example.invalid",
            "--source-id", f"{harness}:mac:test", "--principal-id", "principal:test",
            "--visibility", "private", "--harness", harness, "--root", "/test/root",
            "--spool", "/test/state.db", "--token-file", "/test/token",
            "--privacy-mode", "scrub",
        ]
        route = (self.writer, mock.Mock(), "tenant:test") if canonical else None
        output = io.StringIO()
        with mock.patch("sys.argv", arguments), \
                mock.patch.object(cli, "Collector", return_value=self.collector), \
                mock.patch.object(cli, "load_file_token", return_value="test-token"), \
                mock.patch.object(cli, "_canonical_clients", return_value=route), \
                contextlib.redirect_stdout(output):
            cli.main()
        return json.loads(output.getvalue())

    def test_known_scan_failures_publish_degraded_health_without_flush(self):
        for harness in ("claude", "codex"):
            for code in ("archive_unavailable", "source_snapshot_deadline", "source_snapshot_changed"):
                with self.subTest(harness=harness, code=code):
                    self.setUp()
                    self.collector.scan.side_effect = CollectorRuntimeError(code)
                    self.collector.doctor.return_value.update(
                        scan_complete=False, last_error_code=code, pending=3,
                    )
                    with self.assertRaises(SystemExit) as caught:
                        self.run_cycle(harness)
                    self.assertEqual(caught.exception.code, 1)
                    report = self.writer.report_health.call_args.args[0]
                    self.assertEqual(report["collector_kind"], harness)
                    self.assertEqual(report["status"], "degraded")
                    self.assertEqual(report["last_error_code"], code)
                    self.assertEqual(report["pending_records"], 3)
                    self.collector.flush.assert_not_called()
                    self.collector.close.assert_called_once_with()

    def test_unknown_failure_still_raises_and_closes(self):
        error = CollectorRuntimeError("unknown_failure")
        self.collector.scan.side_effect = error
        with self.assertRaises(CollectorRuntimeError) as caught:
            self.run_cycle("claude")
        self.assertIs(caught.exception, error)
        self.writer.report_health.assert_not_called()
        self.collector.flush.assert_not_called()
        self.collector.close.assert_called_once_with()

    def test_success_keeps_ack_and_light_health(self):
        result = self.run_cycle("codex")
        self.assertEqual(result["flush"], {"acked": 1})
        self.assertEqual(self.writer.report_health.call_args.args[0]["status"], "ready")
        self.collector.doctor.assert_called_once_with(
            include_dead_letters=False, include_ack_latency=False,
        )
        self.collector.flush.assert_called_once_with()

    def test_legacy_route_does_not_invent_health_authority(self):
        result = self.run_cycle("claude", canonical=False)
        self.assertIsNone(result["health_report"])
        self.writer.report_health.assert_not_called()
        self.collector.flush.assert_called_once_with()

    def test_actual_cli_exit_status_and_json_after_scan_failure(self):
        program = '''
import os, runpy, sys
from unittest import mock
from collector.collector import CollectorRuntimeError
code = sys.argv[1]
collector = mock.Mock()
collector.scan.return_value = {"scan_complete": True}
collector.flush.return_value = {"acked": 1}
collector.doctor.return_value = {"scan_complete": not bool(code), "pending": 0,
                               "coverage_percent": 100, "last_error_code": code or None}
if code:
    collector.scan.side_effect = CollectorRuntimeError(code)
sys.argv = ["recall-brain", "collect", "--endpoint", "https://example.invalid",
    "--source-id", "codex:mac:test", "--principal-id", "principal:test",
    "--visibility", "private", "--harness", "codex", "--root", "/test/root",
    "--spool", "/test/state.db", "--token-file", "/test/token", "--privacy-mode", "scrub"]
os.environ.update(RECALL_CANONICAL_V2_ENABLED="1", RECALL_TENANT_ID="tenant:test",
                  RECALL_PRINCIPAL_ID="principal:test")
with mock.patch("collector.collector.Collector", return_value=collector), \\
     mock.patch("client.mac.load_file_token", return_value="test-token"), \\
     mock.patch("client.mac.CanonicalBrainWriter.report_health", return_value={"status":"accepted"}):
    runpy.run_module("client.cli", run_name="__main__")
'''
        for code in ("", "archive_unavailable", "source_snapshot_changed", "source_snapshot_deadline"):
            with self.subTest(code=code):
                result = subprocess.run(
                    [sys.executable, "-c", program, code], capture_output=True, text=True,
                    env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
                    timeout=10,
                )
                self.assertEqual(result.returncode, 1 if code else 0, result.stderr)
                value = json.loads(result.stdout)
                self.assertEqual(value["health_report"], {"status": "accepted"})
                if code:
                    self.assertEqual(value["scan"], {"scan_complete": False, "error_code": code})
                    self.assertEqual(value["flush"], {"status": "skipped", "reason": "scan_failed"})
                else:
                    self.assertEqual(value["flush"], {"acked": 1})


if __name__ == "__main__":
    unittest.main()
