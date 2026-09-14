"""H5-3: the ``freshness.embedding_lag`` systems-card probe."""
from __future__ import annotations

import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from evals.systems_card import embedding
from evals.systems_card.model import ProbeResult
from evals.systems_card.runner import PROBES, build_card, history_row
from tests.test_systems_card import FakeBrain, default_tools, make_context


def _token_file(directory: str) -> str:
    path = Path(directory) / "metrics.json"
    path.write_text(json.dumps({"token": "synthetic-metrics"}))
    path.chmod(0o600)
    return str(path)


def _metrics_text(*, unembedded: int = 1234, total: int = 15000, cap: int = 200000) -> bytes:
    return (
        "# HELP recall_passages_unembedded ...\n"
        f"recall_passages_unembedded {unembedded}\n"
        f"recall_embedding_daily_total {total}\n"
        f"recall_embedding_daily_cap {cap}\n"
        'recall_table_bytes{table="x"} 1\n'
    ).encode()


class EmbeddingLagProbeTests(unittest.TestCase):
    def _run(self, text: bytes | None, status: int = 200):
        seen: list[tuple[str, dict]] = []

        def getter(url, headers, timeout):
            seen.append((url, headers))
            return status, text or b""

        with tempfile.TemporaryDirectory() as directory:
            context = make_context(FakeBrain(default_tools()), metrics_token_file=_token_file(directory), _metrics_get=getter)
            return embedding.EmbeddingLagProbe().run(context), seen

    def test_reports_lag_and_budget_and_passes_healthy_gates(self):
        result, seen = self._run(_metrics_text())
        self.assertEqual(seen[0][0], "https://brain.invalid/metrics")
        self.assertEqual(seen[0][1]["Authorization"], "Bearer synthetic-metrics")
        self.assertEqual(result.name, "freshness.embedding_lag")
        self.assertEqual(result.status, "ok")
        self.assertEqual(result.metrics["passages_unembedded"], 1234)
        self.assertEqual(result.metrics["embedded_today"], 15000)
        self.assertEqual(result.metrics["daily_cap"], 200000)
        self.assertEqual(result.metrics["cap_remaining"], 185000)
        gates = {g.metric: g for g in result.gates}
        self.assertTrue(gates["passages_unembedded"].passed)
        self.assertTrue(gates["cap_remaining"].passed)

    def test_degrades_on_lag_above_the_gate(self):
        result, _ = self._run(_metrics_text(unembedded=5001))
        self.assertEqual(result.status, "degraded")
        gates = {g.metric: g for g in result.gates}
        self.assertFalse(gates["passages_unembedded"].passed)
        self.assertTrue(gates["cap_remaining"].passed)

    def test_degrades_when_the_cap_is_exhausted(self):
        result, _ = self._run(_metrics_text(total=200000))
        self.assertEqual(result.status, "degraded")
        self.assertEqual(result.metrics["cap_remaining"], 0)
        gates = {g.metric: g for g in result.gates}
        self.assertFalse(gates["cap_remaining"].passed)
        self.assertTrue(any("cap reached" in note for note in result.notes))
        over, _ = self._run(_metrics_text(total=250000))
        self.assertEqual(over.metrics["cap_remaining"], 0)

    def test_unknown_lag_without_runtime_is_a_null_gate_not_a_failure(self):
        result, _ = self._run(_metrics_text(unembedded=-1))
        self.assertEqual(result.status, "ok")
        gates = {g.metric: g for g in result.gates}
        self.assertIsNone(gates["passages_unembedded"].passed)
        self.assertTrue(any("runtime not configured" in note for note in result.notes))

    def test_skips_without_metrics_token(self):
        with unittest.mock.patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("RECALL_METRICS_TOKEN_FILE", None)
            result = embedding.EmbeddingLagProbe().run(make_context(FakeBrain(default_tools())))
        self.assertEqual(result.status, "skipped")

    def test_fails_closed_on_denied_or_old_server(self):
        denied, _ = self._run(b"", status=403)
        self.assertEqual(denied.status, "failed")
        old, _ = self._run(b"recall_passages_unembedded 5\n")
        self.assertEqual(old.status, "failed")
        self.assertIn("lacks 2 embedding gauges", old.notes[0])

    def test_registered_and_picked_into_history(self):
        self.assertIn(embedding.EmbeddingLagProbe, PROBES["freshness"])
        result = ProbeResult("freshness.embedding_lag", "freshness", "ok", metrics={"passages_unembedded": 12, "embedded_today": 3, "cap_remaining": 9})
        with tempfile.TemporaryDirectory() as directory:
            card = build_card([result], base_url="https://brain.invalid/mcp", started_at=0.0, repo_root=Path(directory), options={})
        row = history_row(card)
        self.assertEqual(row["freshness.embedding_lag.passages_unembedded"], 12)
        self.assertEqual(row["freshness.embedding_lag.cap_remaining"], 9)


if __name__ == "__main__":
    unittest.main()
