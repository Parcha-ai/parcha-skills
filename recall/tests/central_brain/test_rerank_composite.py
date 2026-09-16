"""The reranker reads a document through the heads of its strongest passages."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from recall_server import passage_retrieval  # noqa: E402
from recall_server.passage_retrieval import (  # noqa: E402
    PassageHintRetrieval,
    rerank_composite,
)
from tests.central_brain.test_canonical_retrieval import (  # noqa: E402
    ActorRecordingStore,
    RerankWiringTests,
)
from tests.central_brain.test_passage_fusion import candidate  # noqa: E402


def _row(document: str, kind: str, score: float, text: str, passage: str) -> dict:
    row = candidate(document, kind, score)
    row["text_redacted"] = text
    if kind == "dense":
        row["passage_id"] = f"psg_{passage:0<32}"
        row["receipts"] = [f"recall://source:test/{passage}?rev=1#item=0"]
    else:
        row["passage_id"] = f"psg_{passage:0<32}"
        row["receipts"] = [f"recall://source:test/{passage}?rev=1#item=0"]
        row["passage_ordinal"] = 0
        row["spans"] = [{"record_ordinal": 0}]
        row.pop("receipt", None)
    return row


class CompositeDocumentTests(unittest.TestCase):
    def test_one_passage_keeps_the_whole_width(self) -> None:
        row = {"text_redacted": "x" * 500, "source_id": "codex:linux:test", "first_occurred_at": "2026-05-03 10:00:00+00:00"}
        self.assertEqual(rerank_composite([row], [], 300), passage_retrieval.rerank_document(row, [], 300))

    def test_two_passages_split_the_width_after_the_context_line(self) -> None:
        first = {"text_redacted": "a" * 500, "source_id": "codex:linux:test", "first_occurred_at": "2026-05-03 10:00:00+00:00"}
        second = {"text_redacted": "b" * 500}
        text = rerank_composite([first, second], [], 400)
        context, body = text.split("\n\n", 1)
        self.assertEqual(context, passage_retrieval.rerank_context(first))
        heads = body.split("\n\n")
        self.assertEqual(len(heads), 2)
        self.assertTrue(heads[0].startswith("a") and heads[1].startswith("b"))
        self.assertEqual(len(heads[0]), len(heads[1]))
        self.assertLessEqual(len(text), 400)

    def test_a_narrow_width_falls_back_to_the_first_passage(self) -> None:
        first = {"text_redacted": "a" * 500, "source_id": "codex:linux:test", "first_occurred_at": "2026-05-03 10:00:00+00:00"}
        second = {"text_redacted": "b" * 500}
        self.assertEqual(rerank_composite([first, second], [], 120), passage_retrieval.rerank_document(first, [], 120))

    def test_env_clamps(self) -> None:
        for raw, expected in (("", 2), ("1", 1), ("3", 3), ("9", 2), ("x", 2)):
            with mock.patch.dict(os.environ, {"RECALL_RERANK_COMPOSITE_RANGES": raw}):
                self.assertEqual(passage_retrieval._composite_ranges_from_env(), expected, raw)


class RerankReadsTwoPassagesTests(unittest.TestCase):
    """Live: a document carried by the exact arm sent its tool-output passage
    and scored 0.24; its lexical passage (the answer) scored 0.44 the run
    before, when the arm order happened to lead with it. Both heads go now."""

    class _Store(ActorRecordingStore):
        search_deadline_ms = 20000
        temporal_hints = None
        query_clauses = False

    def _retrieval(self, store):
        retrieval = PassageHintRetrieval(
            store, tenant_id="tenant:test", sources=["codex:linux:test"], policy_fingerprint="fp-policy",
        )
        # Document "g": dense saw a weak passage, lexical the one with the
        # answer. Document "h": one strong dense passage.
        dense = [_row("h", "dense", 0.95, "strong neighbour", "h1"), _row("g", "dense", 0.40, "weak dense passage", "g1")]
        lexical = [_row("g", "passage-lexical", 9.0, "greptile flagged the P2 review", "g2")]
        retrieval._dense_candidates = lambda query, **kwargs: (dense, "ok", "ann", None)
        retrieval._lexical_candidates = lambda query, **kwargs: (lexical, "ok")
        retrieval._sparse_candidates = lambda query, original_query=None, **kwargs: ([], "skipped-prose-query")
        return retrieval

    def _search(self, store):
        return self._retrieval(store).search(
            "what did greptile flag", lexical_query="greptile flag", since=None, until=None, limit=10,
        )

    def test_the_document_is_sent_once_with_both_heads_and_both_ranges_share_the_score(self) -> None:
        store = self._Store()
        store.rerank_runtime = RerankWiringTests._FakeRerank({"greptile flagged the P2 review": 0.9, "strong neighbour": 0.5})
        with mock.patch.object(passage_retrieval, "RERANK_COMPOSITE_RANGES", 2):
            response = self._search(store)
        documents = store.rerank_runtime.calls[-1]["documents"]
        self.assertEqual(len(documents), 2)
        g_document = next(doc for doc in documents if "P2 review" in doc)
        self.assertIn("weak dense passage", g_document)
        self.assertEqual(response["diagnostics"]["rerank_composite_ranges"], 2)
        by_document = {row["logical_document_id"][5]: row for row in response["results"]}
        self.assertEqual(by_document["g"]["rerank_score"], 0.9)
        self.assertEqual([item.get("rerank_score") for item in by_document["g"]["matching_ranges"]], [0.9, 0.9])
        self.assertEqual([row["logical_document_id"][5] for row in response["results"]], ["g", "h"])

    def test_one_range_per_document_is_the_old_contract(self) -> None:
        store = self._Store()
        store.rerank_runtime = RerankWiringTests._FakeRerank({"greptile flagged the P2 review": 0.9, "strong neighbour": 0.5})
        with mock.patch.object(passage_retrieval, "RERANK_COMPOSITE_RANGES", 1):
            response = self._search(store)
        documents = store.rerank_runtime.calls[-1]["documents"]
        # Two documents, one passage each in the first pass; the second pass
        # sends g's other passage on its own.
        self.assertEqual(len(documents), 3)
        self.assertFalse(any("weak dense passage" in doc and "P2 review" in doc for doc in documents))
        self.assertEqual(response["diagnostics"]["rerank_composite_ranges"], 1)


if __name__ == "__main__":
    unittest.main()
