"""Exercise passage capacity through the real provider runtime, without network IO."""
from __future__ import annotations

import copy
import os
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from recall_server.passage_retrieval import PassageHintRetrieval
from recall_server.rerank import RerankRuntime


class RecordingProvider:
    def __init__(self, *, error=None, invalid=False):
        self.calls = []
        self.error = error
        self.invalid = invalid
        self.observer = lambda: None

    def post(self, *, url, headers, body, timeout):
        self.observer()
        self.calls.append((copy.deepcopy(body), timeout))
        if self.error:
            raise self.error
        if self.invalid:
            return {"data": [{"index": len(body["documents"]), "relevance_score": 1}]}
        return {"data": [
            {"index": index, "relevance_score": 1.0 if "DIRECT ANSWER" in text else 0.1}
            for index, text in enumerate(body["documents"])
        ]}


class MatchedRangeRerankTests(unittest.TestCase):
    def setUp(self):
        patch = mock.patch.dict(os.environ, {"RERANK_TEST_KEY": "synthetic-only"})
        patch.start()
        self.addCleanup(patch.stop)
        self.retrieval = object.__new__(PassageHintRetrieval)
        self.retrieval.store = SimpleNamespace(rerank_blend=0.6, rerank_min_budget_seconds=0.05)

    def fixture(self, *, alias=False):
        pool, rows = [], []
        # The authorized document pool has already been chosen. The last
        # ordinary document's second range answers the question best.
        for index in range(50):
            key = f"passage-{index}"
            pool.append({"source_id": "source-a", "logical_document_id": f"doc-{index}",
                         "rank": 1.0, "matching_ranges": [{"passage_id": key}]})
            rows.append({"passage_id": key, "text_redacted": f"incidental mention {index}"})
        pool[-1]["matching_ranges"].append({"passage_id": "answer"})
        answer = "DIRECT ANSWER " + "evidence " * 400
        rows.append({"passage_id": "answer", "text_redacted": answer})
        pool.append({"source_id": "source-a", "logical_document_id": "nominee",
                     "rank": 1.0, "nominated": True,
                     "matching_ranges": [{"passage_id": "nominee-range"}]})
        rows.append({"passage_id": "nominee-range", "text_redacted": "nominated evidence"})
        if alias:
            pool[-2]["matching_ranges"].append({"receipts": ["answer-receipt"]})
            rows.append({"receipt": "answer-receipt", "text_redacted": answer})
        # Merely present in the arms, outside the authorized selected pool.
        rows.append({"passage_id": "unused-tail", "text_redacted": "NEVER SEND THIS TAIL"})
        return pool, (("dense", 1.0, rows),)

    def run_rerank(self, provider, pool, legs, *, remaining=5):
        runtime = RerankRuntime(protocol="voyage", key_env="RERANK_TEST_KEY",
                                max_candidates=50, max_doc_chars=2000,
                                timeout_seconds=2.5, transport=provider)
        # A process-shared runtime must remain unchanged even DURING the call.
        provider.observer = lambda: self.assertEqual(runtime.max_candidates, 50)
        before = copy.deepcopy((pool, legs))
        elapsed = {}
        output, diagnostics = self.retrieval._rerank_fused(
            "question", pool, legs, runtime=runtime,
            deadline_at=time.monotonic() + remaining, arm_elapsed_ms=elapsed,
        )
        self.assertEqual((pool, legs), before)
        self.assertEqual(runtime.max_candidates, 50)
        self.assertEqual(runtime.max_doc_chars, 2000)
        self.assertEqual(runtime.timeout_seconds, 2.5)
        return output, diagnostics, elapsed, runtime

    def test_second_range_scores_and_leads_without_expanding_document_pool(self):
        pool, legs = self.fixture()
        provider = RecordingProvider()
        output, diagnostics, elapsed, _ = self.run_rerank(provider, pool, legs)
        self.assertEqual(len(provider.calls), 1)
        body, timeout = provider.calls[0]
        self.assertEqual(len(body["documents"]), 52)
        self.assertEqual(body["top_k"], 52)
        self.assertIn("nominated evidence", body["documents"])
        self.assertTrue(any(text.startswith("DIRECT ANSWER") for text in body["documents"]))
        self.assertFalse(any("NEVER SEND" in text for text in body["documents"]))
        self.assertEqual(max(map(len, body["documents"])), 2000)
        self.assertLessEqual(timeout, 2.5)
        self.assertEqual(diagnostics["rerank_candidates"], 52)
        self.assertEqual(diagnostics["rerank_status"], "ok")
        self.assertIn("rerank", elapsed)
        self.assertEqual(output[0]["logical_document_id"], "doc-49")
        self.assertEqual(output[0]["matching_ranges"][0]["passage_id"], "answer")
        self.assertEqual({r["logical_document_id"] for r in output},
                         {r["logical_document_id"] for r in pool})

    def test_passage_and_receipt_alias_share_one_provider_score(self):
        pool, legs = self.fixture(alias=True)
        provider = RecordingProvider()
        output, _, _, _ = self.run_rerank(provider, pool, legs)
        self.assertEqual(len(provider.calls), 1)
        texts = provider.calls[0][0]["documents"]
        self.assertEqual(sum(text.startswith("DIRECT ANSWER") for text in texts), 1)
        document = next(row for row in output if row["logical_document_id"] == "doc-49")
        scores = {span.get("passage_id") or span.get("receipts", [None])[0]: span.get("rerank_score")
                  for span in document["matching_ranges"]}
        self.assertEqual(scores["answer"], 1.0)
        self.assertEqual(scores["answer-receipt"], 1.0)

    def test_provider_error_and_malformed_result_preserve_fused_order_once(self):
        for provider in (RecordingProvider(error=TimeoutError()), RecordingProvider(invalid=True)):
            with self.subTest(invalid=provider.invalid):
                pool, legs = self.fixture()
                output, diagnostics, elapsed, _ = self.run_rerank(provider, pool, legs)
                self.assertIs(output, pool)
                self.assertEqual(len(provider.calls), 1)
                self.assertEqual(diagnostics["rerank_status"], "unavailable")
                self.assertIn("rerank", elapsed)

    def test_insufficient_deadline_keeps_pool_without_provider_call(self):
        pool, legs = self.fixture()
        provider = RecordingProvider()
        output, diagnostics, elapsed, _ = self.run_rerank(provider, pool, legs, remaining=0)
        self.assertIs(output, pool)
        self.assertEqual(provider.calls, [])
        self.assertEqual(diagnostics["rerank_status"], "skipped-budget")
        self.assertNotIn("rerank", elapsed)

    def test_remaining_deadline_still_bounds_the_single_request(self):
        pool, legs = self.fixture()
        provider = RecordingProvider()
        _, diagnostics, _, _ = self.run_rerank(provider, pool, legs, remaining=0.25)
        self.assertEqual(len(provider.calls), 1)
        self.assertGreater(provider.calls[0][1], 0)
        self.assertLessEqual(provider.calls[0][1], 0.25)
        self.assertEqual(diagnostics["rerank_status"], "ok")

    def test_standalone_runtime_retains_its_configured_passage_capacity(self):
        pool, legs = self.fixture()
        provider = RecordingProvider()
        _, _, _, runtime = self.run_rerank(provider, pool, legs)
        provider.calls.clear()
        runtime.rerank("question", [f"text {i}" for i in range(70)])
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(len(provider.calls[0][0]["documents"]), 50)


if __name__ == "__main__":
    unittest.main()
