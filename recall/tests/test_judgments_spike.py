"""The W0 experiment must bound spend and preserve failed/unknown observations."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))

from evals import judgments_spike as spike
from evals.retrieval import EvaluationInputError
from recall_server.judgments import ChoiceAnswer, JudgmentUnavailable
from tests.test_fusion_tuning import private_directory, private_write


class Client:
    def __init__(self, *, fail=False, unknown=False, input_tokens=42):
        self.calls = []
        self.fail = fail
        self.unknown = unknown
        self.input_tokens = input_tokens

    def judge(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise JudgmentUnavailable("judgment_http_error", status_code=404)
        return SimpleNamespace(
            answers={
                "mode": ChoiceAnswer(
                    choice="none", probabilities={"none": 1.0}, confidence=1.0
                )
            },
            metadata={
                "model": "jev-1.13.0",
                "contract_hash": "a" * 64,
                "elapsed_ms": 12.0,
                "question_count": 1,
                "input_tokens": None if self.unknown else self.input_tokens,
                "output_tokens": None if self.unknown else 6,
            },
        )


def row(phase="query"):
    return {
        "id": "opaque-case",
        "phase": phase,
        "state": {"query": "private company question"},
        "questions": {
            "mode": {
                "type": "choice",
                "instructions": "Read the date mode",
                "criteria": {"none": "No date"},
            }
        },
        "checks": {"regex": {"mode": "none"}},
    }


class SpikeTests(unittest.TestCase):
    def run_spike(self, client, rows=None, **options):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        private = private_directory(root)
        repo = root / "repo"
        repo.mkdir()
        fixture = private / "fixture.jsonl"
        private_write(fixture, rows or [row()])
        report = private / "report.json"
        result = spike.run(fixture, report, repo_root=repo, client=client, **options)
        return result, report, fixture, repo

    def test_repeats_are_measured_and_reports_do_not_contain_state_or_answers(self):
        client = Client()
        result, report, _fixture, _repo = self.run_spike(client, repeats=2)
        self.assertEqual(len(client.calls), 2)
        self.assertTrue(all("deadline" in call for call in client.calls))
        self.assertEqual(result["groups"]["query"]["successes"], 2)
        self.assertEqual(
            result["groups"]["query"]["checks"]["regex"], {"compared": 2, "matched": 2}
        )
        self.assertEqual(
            result["groups"]["query"]["checks"]["gold"], {"compared": 0, "matched": 0}
        )
        self.assertEqual(result["usage"]["observed_input_tokens"], 84)
        self.assertEqual(result["usage"]["unknown_requests"], 0)
        self.assertFalse(result["production_ready"])
        self.assertEqual(result["measurement_scope"], "individual_request")
        self.assertFalse(result["stage_budget_evaluated"])
        self.assertNotIn("private company", report.read_text())
        self.assertNotIn("Read the date mode", report.read_text())
        self.assertEqual(report.stat().st_mode & 0o777, 0o600)
        self.assertEqual(json.loads(report.read_text()), result)

    def test_unknown_usage_is_not_zero_cost(self):
        result, *_ = self.run_spike(Client(unknown=True))
        self.assertIsNone(result["usage"]["input_cost_usd"])
        self.assertEqual(result["usage"]["unknown_requests"], 1)
        self.assertGreater(result["usage"]["reserved_input_tokens"], 0)

    def test_failure_is_counted_once_without_retry_or_success_percentile(self):
        client = Client(fail=True)
        result, *_ = self.run_spike(client)
        self.assertEqual(len(client.calls), 1)
        group = result["groups"]["query"]
        self.assertEqual(group["errors"], {"judgment_http_error": 1})
        self.assertEqual(group["http_errors"], {"404": 1})
        self.assertIsNone(group["request_success_p95_ms"])
        self.assertEqual(result["usage"]["unknown_requests"], 1)
        self.assertEqual(result["transport_status"], "failed")

    def test_observed_usage_can_stop_remaining_dispatches(self):
        client = Client(input_tokens=100_000)
        result, *_ = self.run_spike(
            client, rows=[row(), row()], max_input_tokens=10_000
        )
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(result["groups"]["query"]["skipped_for_spend_cap"], 1)
        self.assertEqual(result["usage"]["unknown_requests"], 0)
        self.assertEqual(result["usage"]["accounted_input_tokens"], 100_000)
        self.assertEqual(result["transport_status"], "failed")

    def test_entire_run_is_preflighted_before_spend(self):
        client = Client()
        with self.assertRaises(EvaluationInputError):
            self.run_spike(client, max_input_tokens=1)
        self.assertEqual(client.calls, [])
        with self.assertRaises(EvaluationInputError):
            self.run_spike(
                client, rows=[row(), {**row(), "phase": "private-phase-text"}]
            )
        self.assertEqual(client.calls, [])
        for malformed in (
            {"type": "noul", "instructions": ""},
            {"type": "choice", "instructions": "Read", "criteria": {"none": 7}},
            {"type": "noul", "instructions": "Read", "extra": "unsupported"},
        ):
            invalid = row()
            invalid["questions"] = {"mode": malformed}
            with self.assertRaises(EvaluationInputError):
                self.run_spike(client, rows=[row(), invalid])
            self.assertEqual(client.calls, [])

    def test_inputs_and_output_must_be_private_and_output_must_be_new(self):
        client = Client()
        _, report, fixture, repo = self.run_spike(client)
        with self.assertRaises(EvaluationInputError):
            spike.run(fixture, report, repo_root=repo, client=client)
        repo.chmod(0o700)
        with self.assertRaises(EvaluationInputError):
            spike.run(fixture, repo / "report.json", repo_root=repo, client=client)
        self.assertEqual(len(client.calls), 1)

    def test_captured_rows_need_named_questions_and_safe_context_bounds(self):
        client = Client()
        invalid = row()
        invalid["questions"] = {}
        with self.assertRaises(EvaluationInputError):
            self.run_spike(client, [invalid])
        oversized = row()
        oversized["state"] = "x" * 33_000
        with self.assertRaises(EvaluationInputError):
            self.run_spike(client, [oversized])
        self.assertEqual(client.calls, [])

    def test_unsupported_expected_fields_are_not_silent_matches(self):
        client = Client()
        invalid = row()
        invalid["checks"] = {"gold": {"unknown_question": "none"}}
        with self.assertRaises(EvaluationInputError):
            self.run_spike(client, [invalid])
        self.assertEqual(client.calls, [])
        for expectation in ("not_an_option", 1, True):
            invalid["checks"] = {"gold": {"mode": expectation}}
            with self.assertRaises(EvaluationInputError):
                self.run_spike(client, [invalid])
        invalid["questions"] = {
            "mode": {"type": "noul", "instructions": "Does a date exist?"}
        }
        invalid["checks"] = {"gold": {"mode": 1}}
        with self.assertRaises(EvaluationInputError):
            self.run_spike(client, [invalid])
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
