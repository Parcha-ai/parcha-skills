"""The nightly summary reports the percentile it actually read."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from evals.systems_card import nightly_summary
from evals.systems_card.nightly_summary import (
    SEARCH_LATENCY_MS,
    SERVER_LATENCY_MS,
    percentile_label,
    summary_from_output_dir,
    summary_lines,
)
from evals.systems_card.runner import main


def _card(status: str = "degraded", failed: list[tuple[str, str]] | None = None) -> dict:
    gates = [{"metric": metric, "passed": False} for _, metric in (failed or [])]
    probes = [
        {"name": probe, "metrics": {}, "gates": [g]}
        for (probe, _), g in zip(failed or [], gates)
    ]
    return {
        "schema_version": nightly_summary.CARD_SCHEMA,
        "generated_at": "2026-09-21T06:04:49Z",
        "overall": {"status": status, "gates_passed": 27, "gates_total": 31, "gates_failed": len(gates)},
        "dimensions": {"integrity": {"probes": probes}},
    }


def _rows() -> list[dict]:
    return [
        {SEARCH_LATENCY_MS: 1462.0, SERVER_LATENCY_MS: 900.0},
        {
            SEARCH_LATENCY_MS: 1605.0,
            SERVER_LATENCY_MS: 1059.0,
            "latency.search_stages.deadline_exceeded_rate": 0.0,
            "accuracy.truth_boundary.boundary_recall@20": 0.88,
            "accuracy.truth_boundary.boundary_mrr": 0.54,
            "freshness.source_age.newest_age_hours_min": 0.6,
            "freshness.source_age.projection_pending": 0,
            "authorization.negative_scope.leaks": 0,
            "privacy.secret_scan.secret_hits_total": 0,
            "cost.planetscale.invoice_mtd_usd": 319.0,
        },
    ]


class PercentileLabelTests(unittest.TestCase):
    def test_label_comes_from_the_metric_key(self) -> None:
        self.assertEqual(percentile_label(SEARCH_LATENCY_MS), "p95")
        self.assertEqual(percentile_label(SERVER_LATENCY_MS), "p95")
        self.assertEqual(percentile_label("latency.search_stages.arm.dense.p50_ms"), "p50")

    def test_a_key_without_a_percentile_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            percentile_label("freshness.source_age.projection_pending")


class SummaryLineTests(unittest.TestCase):
    def test_search_latency_is_labelled_p95_not_p50(self) -> None:
        line = summary_lines(_card(), _rows(), git_sha="6a823fe", date="2026-09-21")[1]
        self.assertIn("search p95 1605 ms (+143)", line)
        self.assertIn("server p95 1059 ms", line)
        self.assertNotIn("p50", line)

    def test_every_latency_label_matches_its_source_metric(self) -> None:
        line = summary_lines(_card(), _rows(), git_sha="6a823fe", date="2026-09-21")[1]
        for key in (SEARCH_LATENCY_MS, SERVER_LATENCY_MS):
            # re-derive the percentile independently of nightly_summary's regex
            tokens = [t for t in key.replace(".", "_").split("_") if t.startswith("p") and t[1:].isdigit()]
            self.assertEqual(tokens[-1], percentile_label(key))
            self.assertIn(f"{percentile_label(key)} ", line)

    def test_header_gates_and_reconcile_and_red_lines(self) -> None:
        card = _card(failed=[("integrity.scan_consistency", "scope_scan_agreement")])
        lines = summary_lines(
            card, _rows(), git_sha="6a823fe", date="2026-09-21", reconcile_line="plane rows 10 · stale 0 deleted 0 · missing 0 written 0"
        )
        self.assertTrue(lines[0].startswith("Recall systems card 2026-09-21 · DEGRADED · gates 27/31 · git 6a823fe"))
        self.assertEqual(lines[2], "recall@20 0.88 · MRR 0.54")
        self.assertEqual(lines[3], "freshness: newest 0.6 h · pending 0")
        self.assertIn("plane rows 10", lines[5])
        self.assertEqual(lines[6], "red: scan_consistency.scope_scan_agreement")
        self.assertEqual(lines[-1], f"{nightly_summary.DOCS_HOST}/2026-09-21-recall-systems-card.html")

    def test_missing_metrics_render_as_a_dash_without_a_delta(self) -> None:
        lines = summary_lines(_card(), [{}], git_sha="abc1234", date="2026-09-21")
        self.assertEqual(
            lines[1],
            f"search p95 {nightly_summary.MISSING} ms · server p95 {nightly_summary.MISSING} ms"
            f" · deadline rate {nightly_summary.MISSING}",
        )


class SummaryCommandTests(unittest.TestCase):
    def test_summary_subcommand_reads_an_existing_output_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "card.json").write_text(json.dumps(_card()))
            (out / "history.jsonl").write_text("\n".join(json.dumps(r) for r in _rows()) + "\n")
            expected = summary_from_output_dir(out, git_sha="6a823fe", date="2026-09-21")
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = main(["summary", "--output-dir", str(out), "--git-sha", "6a823fe", "--date", "2026-09-21"])
            self.assertEqual(code, 0)
            self.assertEqual(buffer.getvalue().strip(), expected)
            self.assertIn("search p95 1605 ms", expected)


class RefusalTests(unittest.TestCase):
    """The summary refuses a card it cannot describe truthfully.

    These defenses lived in the cron's inline heredoc before the summary moved
    into the repository; losing them would have traded one silent mislabel for
    a whole family of them.
    """

    def _summary(self, card: dict, rows: list[dict] | None = None, **kwargs) -> str:
        return "\n".join(
            summary_lines(
                card,
                _rows() if rows is None else rows,
                git_sha="6a823fe",
                date="2026-09-21",
                **kwargs,
            )
        )

    def test_a_reconcile_line_with_control_characters_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self._summary(_card(), reconcile_line="plane rows 1\nhttps://evil.example")

    def test_an_overlong_reconcile_line_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self._summary(_card(), reconcile_line="x" * (nightly_summary.MAX_RECONCILE_CHARS + 1))

    def test_a_foreign_schema_version_is_refused(self) -> None:
        card = _card() | {"schema_version": "recall.systems-card.v2"}
        with self.assertRaises(ValueError):
            self._summary(card)

    def test_gates_passed_above_total_is_refused(self) -> None:
        card = _card()
        card["overall"]["gates_passed"] = 32
        with self.assertRaises(ValueError):
            self._summary(card)

    def test_a_boolean_gate_count_is_refused(self) -> None:
        card = _card()
        card["overall"]["gates_passed"] = True
        with self.assertRaises(ValueError):
            self._summary(card)

    def test_an_unknown_overall_status_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self._summary(_card(status="fine"))

    def test_an_empty_history_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            self._summary(_card(), rows=[])

    def test_a_non_finite_metric_is_refused(self) -> None:
        rows = _rows()
        rows[-1][SEARCH_LATENCY_MS] = float("nan")
        with self.assertRaises(ValueError):
            self._summary(_card(), rows=rows)

    def test_a_non_numeric_metric_is_refused(self) -> None:
        rows = _rows()
        rows[-1][SEARCH_LATENCY_MS] = "1605"
        with self.assertRaises(ValueError):
            self._summary(_card(), rows=rows)

    def test_a_non_boolean_gate_verdict_is_refused(self) -> None:
        card = _card(failed=[("integrity.scan_consistency", "scope_scan_agreement")])
        card["dimensions"]["integrity"]["probes"][0]["gates"][0]["passed"] = "no"
        with self.assertRaises(ValueError):
            self._summary(card)

    def test_a_clean_reconcile_line_is_kept_verbatim(self) -> None:
        line = "plane rows 421189 · stale 3 deleted 3 · missing 0 written 0"
        self.assertIn(line, self._summary(_card(), reconcile_line=line))


if __name__ == "__main__":
    unittest.main()
