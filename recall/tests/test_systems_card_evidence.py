"""The card must not turn unavailable or incomplete evidence into a pass."""

import contextlib
import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from evals.systems_card import corpus, runner
from evals.systems_card.mcp_client import CallOutcome
from evals.systems_card.model import Gate, ProbeResult, dimension_status
from tests.test_systems_card import FakeBrain, default_tools, make_context, scan_handler


class EvidenceTest(unittest.TestCase):
    def probe_scan(self, probe, **changes):
        tools = default_tools()
        tools["recall_scan"] = lambda args: {**scan_handler(args), **changes}
        return probe.run(make_context(FakeBrain(tools)))

    def assert_not_green(self, result):
        self.assertNotEqual(result.status, "ok")
        self.assertTrue(any(g.passed is not True for g in result.gates))

    def test_incomplete_freshness_is_not_green(self):
        self.assert_not_green(self.probe_scan(corpus.FreshnessProbe(), complete=False))

    def test_empty_freshness_is_not_green(self):
        self.assert_not_green(self.probe_scan(corpus.FreshnessProbe(), stdout="[]"))

    def test_incomplete_privacy_is_not_green(self):
        self.assert_not_green(self.probe_scan(corpus.SecretScanProbe(), complete=False))

    def test_empty_or_missing_privacy_counts_are_not_clean(self):
        for stdout in ("[]", "[{}]", '[{"passages": 10}]', '[{"passages": 0}]'):
            with self.subTest(stdout=stdout):
                self.assert_not_green(
                    self.probe_scan(corpus.SecretScanProbe(), stdout=stdout)
                )

    def test_missing_scan_completeness_is_unknown_not_success(self):
        for probe in (corpus.FreshnessProbe(), corpus.SecretScanProbe()):
            self.assert_not_green(self.probe_scan(probe, complete=None))

    def test_scan_transport_and_program_failure_have_failed_gates(self):
        for probe in (corpus.FreshnessProbe(), corpus.SecretScanProbe()):
            self.assert_not_green(
                probe.run(make_context(FakeBrain({}, http_error=503)))
            )
            self.assert_not_green(self.probe_scan(probe, exit_code=1))

    def test_complete_valid_scans_still_pass(self):
        for probe in (corpus.FreshnessProbe(), corpus.SecretScanProbe()):
            self.assertEqual(self.probe_scan(probe).status, "ok")

    def test_failed_negative_checks_are_not_no_leaks(self):
        result = corpus.AuthorizationProbe().run(
            make_context(FakeBrain({}, http_error=503))
        )
        self.assert_not_green(result)
        self.assertEqual(result.metrics["checks_unverified"], 4)

    def test_malformed_success_is_not_a_verified_denial(self):
        tools = {name: {} for name in default_tools()}
        self.assert_not_green(
            corpus.AuthorizationProbe().run(make_context(FakeBrain(tools)))
        )

    def test_explicit_foreign_denial_passes_but_provider_failure_does_not(self):
        for status, valid in ((401, True), (403, True), (503, False)):
            context = make_context(
                FakeBrain(default_tools()),
                foreign_tenant_path="/mcp/brains/synthetic-other",
            )
            outcome = CallOutcome(
                "recall_people", False, 1, http_status=status, error=f"http:{status}"
            )
            with patch("evals.systems_card.mcp_client.McpClient") as client:
                client.return_value.call_tool.return_value = outcome
                result = corpus.AuthorizationProbe().run(context)
            self.assertEqual(result.status == "ok", valid)

    def test_real_leak_remains_a_failure(self):
        tools = default_tools()
        tools["recall_search"] = {"results": [{"logical_document_id": "synthetic"}]}
        self.assert_not_green(
            corpus.AuthorizationProbe().run(make_context(FakeBrain(tools)))
        )


class CardOutcomeTest(unittest.TestCase):
    def test_unknown_gate_is_not_healthy(self):
        result = ProbeResult(
            "synthetic",
            "privacy",
            "ok",
            gates=[Gate("evidence", "==", 1).evaluate(None)],
        )
        self.assertEqual(dimension_status([result]), "degraded")

    def test_restricted_and_skipped_card_is_explicitly_not_full_readiness(self):
        results = [
            ProbeResult("availability.endpoints", "availability", "ok"),
            ProbeResult("accuracy.truth_boundary", "accuracy", "skipped"),
        ]
        with patch.object(runner, "git_pin", return_value={}):
            card = runner.build_card(
                results,
                base_url="https://invalid.example/mcp",
                started_at=0,
                repo_root=Path("."),
                options={},
            )
        self.assertFalse(card["coverage"]["all_probes_verified"])
        self.assertIn("freshness", card["coverage"]["omitted_dimensions"])
        self.assertIn("accuracy.truth_boundary", card["coverage"]["skipped_probes"])

    def test_cli_does_not_exit_success_for_degraded_or_unknown_gates(self):
        for status, unknown, expected in (
            ("ok", 0, 0),
            ("degraded", 0, 1),
            ("failed", 0, 1),
            ("ok", 1, 1),
        ):
            card = {
                "overall": {
                    "status": status,
                    "gates_passed": 1,
                    "gates_failed": 0,
                    "gates_unknown": unknown,
                }
            }
            with (
                patch.object(runner, "run_card", return_value=card),
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                code = runner.main(["run", "--output-dir", "unused"])
            self.assertEqual(code, expected)
            self.assertEqual(json.loads(stdout.getvalue())["status"], status)
