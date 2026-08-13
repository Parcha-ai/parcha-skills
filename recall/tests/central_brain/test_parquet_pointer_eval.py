from __future__ import annotations

import unittest

from evals.parquet_pointer import evaluate, evaluate_plan_open, fixture, run


class ParquetPointerEvalTest(unittest.TestCase):
    def test_full_fixture_covers_every_stratum_with_compact_receipt_pointers(self):
        result = run()
        self.assertTrue(result["passed"])
        self.assertEqual(result["candidate_recall"], 1.0)
        self.assertEqual(result["exact_identifier_recall"], 1.0)
        self.assertEqual(result["positive_receipt_support"], 1.0)
        self.assertEqual(result["opened_receipt_support"], 1.0)
        self.assertTrue(result["plan_open_passed"])
        self.assertLessEqual(result["max_candidates_per_open"], 20)
        self.assertGreaterEqual(result["physical_reduction"], 10.0)
        self.assertEqual(
            set(result["by_stratum"]),
            {
                "fleet_inventory",
                "person_time",
                "team_time",
                "project_topic",
                "exact_identifier",
                "cold_negative",
            },
        )

    def test_fake_fast_empty_plane_fails_recall(self):
        _raw, _passages, cases = fixture()
        result = evaluate([], cases)
        self.assertFalse(result["passed"])
        self.assertLess(result["candidate_recall"], 0.1)

    def test_missing_actor_or_receipt_cannot_fake_quality(self):
        _raw, passages, cases = fixture()
        without_actors = [{**row, "actor_names": []} for row in passages]
        self.assertFalse(evaluate(without_actors, cases)["passed"])
        without_receipts = [{**row, "receipts": []} for row in passages]
        self.assertFalse(evaluate(without_receipts, cases)["passed"])

    def test_incomplete_snapshot_cannot_pass_with_perfect_candidates(self):
        _raw, passages, cases = fixture()
        result = evaluate(passages, cases, complete=False)
        self.assertEqual(result["candidate_recall"], 1.0)
        self.assertFalse(result["passed"])

    def test_missing_authoritative_raw_receipts_fails_plan_open(self):
        raw, passages, cases = fixture()
        without_receipts = [{**row, "receipts": []} for row in raw]
        result = evaluate_plan_open(without_receipts, passages, cases)
        self.assertEqual(result["candidate_recall"], 1.0)
        self.assertEqual(result["opened_receipt_support"], 0.0)
        self.assertFalse(result["plan_open_passed"])


if __name__ == "__main__":
    unittest.main()
