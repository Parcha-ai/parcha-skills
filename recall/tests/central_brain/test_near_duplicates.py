"""Near-duplicate documents fold into the best-ranked copy after the reranker."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from recall_server.passage_retrieval import (  # noqa: E402
    PassageHintRetrieval,
    group_near_duplicates,
    near_duplicate_threshold,
    near_duplicates_enabled,
    shingle_similarity,
    text_shingles,
)
from tests.central_brain.test_canonical_retrieval import ActorRecordingStore  # noqa: E402
from tests.central_brain.test_passage_fusion import candidate  # noqa: E402

COPIED = (
    "I'll make this in grep8 and compare parcha_custom_tools rendering with the ati tools: "
    "the inline status messages need the right icons and a collapsed row per call."
)


def _result(document: str, text: str, *, rank: float, first: str = "2026-05-01T21:45:00Z") -> dict:
    return {
        "logical_document_id": "ldoc_" + document * 32,
        "source_id": "codex:linux:m",
        "native_parent_id": f"codex-session-{document}",
        "revision": 2,
        "first_occurred_at": first,
        "last_occurred_at": first,
        "manifest_object_key": "objects/01/" + "a" * 64,
        "manifest_content_sha256": "b" * 64,
        "rank": rank,
        "rerank_score": rank,
        "matching_ranges": [{"kind": "dense", "text": text, "receipts": [f"recall://codex:linux:m/{document}?rev=2#item=0"] * 6}],
    }


class ShingleTests(unittest.TestCase):
    def test_shingles_and_similarity(self) -> None:
        self.assertEqual(text_shingles("one two three four"), frozenset())
        self.assertEqual(text_shingles("one two three four five"), frozenset({"one two three four five"}))
        a = text_shingles(COPIED)
        b = text_shingles("Sure. " + COPIED)
        self.assertGreater(shingle_similarity(a, b), 0.9)
        self.assertEqual(shingle_similarity(a, frozenset()), 0.0)
        self.assertLess(shingle_similarity(a, text_shingles("the deploy failed because the migration lock timed out on the worker")), 0.1)

    def test_env_switch_and_threshold(self) -> None:
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_NEAR_DUPLICATES": "off"}):
            self.assertFalse(near_duplicates_enabled())
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(near_duplicates_enabled())
            self.assertEqual(near_duplicate_threshold(), 0.6)
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_NEAR_DUPLICATE_THRESHOLD": "0.8"}):
            self.assertEqual(near_duplicate_threshold(), 0.8)
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_NEAR_DUPLICATE_THRESHOLD": "0.1"}):
            self.assertEqual(near_duplicate_threshold(), 0.6)


class GroupTests(unittest.TestCase):
    def test_copies_fold_into_one_group_at_the_best_rank_led_by_the_latest_continuation(self) -> None:
        results = [
            _result("a", COPIED, rank=0.9),
            _result("b", "Sure. " + COPIED, rank=0.8),
            _result("c", "the deploy failed because the migration lock timed out on the worker tonight", rank=0.7),
            _result("d", COPIED + " Then I opened the transcript.", rank=0.6, first="2026-05-01T22:15:00Z"),
            _result("e", "", rank=0.5),
        ]
        kept, diagnostics = group_near_duplicates(results)
        # The group sits at rank 1; "d" continued latest, so it leads it.
        self.assertEqual([row["logical_document_id"][5] for row in kept], ["d", "c", "e"])
        self.assertEqual(kept[0]["rank"], 0.6)
        similar = kept[0]["similar_documents"]
        self.assertEqual([item["logical_document_id"][5] for item in similar], ["a", "b"])
        self.assertEqual(similar[0]["source_id"], "codex:linux:m")
        self.assertEqual(len(similar[0]["receipts"]), 4)
        self.assertEqual(similar[0]["similarity"], 1.0)
        self.assertGreaterEqual(similar[1]["similarity"], 0.9)
        self.assertNotIn("text", similar[0])
        self.assertNotIn("_similarity", kept[0])
        self.assertNotIn("similar_documents", kept[1])
        self.assertEqual(diagnostics["near_duplicates_folded"], 2)
        self.assertEqual(diagnostics["near_duplicate_groups"], 1)
        # Untouched originals: grouping copies nothing into the folded rows.
        self.assertNotIn("similar_documents", results[1])
        self.assertNotIn("_similarity", results[1])

    def test_ties_on_the_end_time_keep_the_best_ranked_member(self) -> None:
        results = [_result("a", COPIED, rank=0.9), _result("b", "Sure. " + COPIED, rank=0.8)]
        kept, _ = group_near_duplicates(results)
        self.assertEqual([row["logical_document_id"][5] for row in kept], ["a"])
        self.assertEqual([item["logical_document_id"][5] for item in kept[0]["similar_documents"]], ["b"])

    def test_short_or_empty_leading_text_never_groups(self) -> None:
        results = [_result("a", "short text here", rank=0.9), _result("b", "short text here", rank=0.8), _result("c", "", rank=0.7), _result("d", "", rank=0.6)]
        kept, diagnostics = group_near_duplicates(results)
        self.assertEqual(len(kept), 4)
        self.assertEqual(diagnostics["near_duplicates_folded"], 0)


class SearchGroupsTests(unittest.TestCase):
    class _Store(ActorRecordingStore):
        search_deadline_ms = 20000
        rerank_runtime = None
        temporal_hints = None
        query_clauses = False

    def _retrieval(self):
        retrieval = PassageHintRetrieval(
            self._Store(), tenant_id="tenant:test", sources=["codex:linux:test"], policy_fingerprint="fp-policy",
        )
        rows = []
        for document, score, text in (("a", 0.95, COPIED), ("b", 0.90, "Sure. " + COPIED), ("c", 0.85, "the deploy failed because the migration lock timed out on the worker tonight")):
            row = candidate(document, "dense", score)
            row["text_redacted"] = text
            rows.append(row)
        retrieval._dense_candidates = lambda query, **kwargs: (rows, "ok", "ann", None)
        retrieval._lexical_candidates = lambda query, **kwargs: ([], "ok")
        retrieval._sparse_candidates = lambda query, original_query=None, **kwargs: ([], "skipped-prose-query")
        return retrieval

    def test_search_returns_primaries_with_their_copies_attached(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            response = self._retrieval().search("inline status messages for custom tools", lexical_query="inline status", since=None, until=None, limit=10)
        ids = [row["logical_document_id"][5] for row in response["results"]]
        self.assertEqual(ids, ["a", "c"])
        self.assertEqual([item["logical_document_id"][5] for item in response["results"][0]["similar_documents"]], ["b"])
        self.assertEqual(response["diagnostics"]["near_duplicates_folded"], 1)
        self.assertEqual(response["diagnostics"]["near_duplicate_groups"], 1)

    def test_switch_off_restores_every_document(self) -> None:
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_NEAR_DUPLICATES": "off"}):
            response = self._retrieval().search("inline status messages for custom tools", lexical_query="inline status", since=None, until=None, limit=10)
        self.assertEqual([row["logical_document_id"][5] for row in response["results"]], ["a", "b", "c"])
        self.assertNotIn("near_duplicates_folded", response["diagnostics"])
        self.assertFalse(any("similar_documents" in row for row in response["results"]))


if __name__ == "__main__":
    unittest.main()
