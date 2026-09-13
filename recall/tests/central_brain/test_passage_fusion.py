"""Convex min-max fusion (H2-b): normalisation, fallbacks, alpha parsing."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server import fusion  # noqa: E402
from recall_server.passage_retrieval import collapse_document_candidates  # noqa: E402


def candidate(document: str, kind: str, score: float) -> dict:
    base = {
        "source_id": "source:test",
        "logical_document_id": f"ldoc_{document:0<32}",
        "revision": 1,
        "native_parent_id": f"session:{document}",
        "first_occurred_at": "2026-08-01T00:00:00Z",
        "last_occurred_at": "2026-08-01T00:10:00Z",
        "manifest_object_key": "objects/01/" + "a" * 64,
        "manifest_content_sha256": "b" * 64,
        "text_redacted": document,
        "score": score,
    }
    if kind == "dense":
        return {
            **base,
            "passage_id": f"psg_{document:0<32}",
            "passage_ordinal": 0,
            "spans": [{"record_ordinal": 0}],
            "receipts": [f"recall://source:test/{document}?rev=1#item=0"],
        }
    return {**base, "receipt": f"recall://source:test/{document}?rev=1#item=0"}


def leg(kind: str, pairs: list[tuple[str, float]]) -> list[dict]:
    return [candidate(document, kind, score) for document, score in pairs]


def order(results: list[dict]) -> list[str]:
    return [row["logical_document_id"][5:].rstrip("0") for row in results]


def fixture_legs() -> tuple:
    dense = leg("dense", [("a", 0.91), ("b", 0.88), ("c", 0.87), ("d", 0.80), ("e", 0.79), ("f", 0.60)])
    lexical = leg("passage-lexical", [("c", 0.42), ("g", 0.40), ("a", 0.31), ("h", 0.20), ("b", 0.12)])
    sparse = leg("sparse-exact", [("g", 0.90), ("c", 0.85), ("i", 0.30)])
    return (
        ("dense", fusion.RRF_LEG_WEIGHTS["dense"], dense),
        ("passage-lexical", fusion.RRF_LEG_WEIGHTS["passage-lexical"], lexical),
        ("sparse-exact", fusion.RRF_LEG_WEIGHTS["sparse-exact"], sparse),
    )


class ConvexFusionTests(unittest.TestCase):
    def test_convex_fusion_normalises_each_arm_before_weighting(self) -> None:
        # Raw scales differ per arm (dense ~0.6-0.9, lexical ~0.1-0.4); min-max
        # puts every arm on [0, 1] so alpha alone decides the arm's influence.
        report: dict = {}
        results = collapse_document_candidates(
            fixture_legs(),
            limit=20,
            fusion="convex",
            alphas={"dense": 0.2, "passage-lexical": 0.3, "sparse-exact": 0.5},
            fusion_report=report,
        )
        by_doc = {order([row])[0]: row for row in results}
        self.assertEqual(by_doc["a"]["arm_scores"]["dense"]["normalized"], 1.0)
        self.assertEqual(by_doc["f"]["arm_scores"]["dense"]["normalized"], 0.0)
        self.assertEqual(by_doc["c"]["arm_scores"]["passage-lexical"]["normalized"], 1.0)
        self.assertEqual(by_doc["b"]["arm_scores"]["passage-lexical"]["normalized"], 0.0)
        self.assertEqual(by_doc["c"]["arm_scores"]["dense"]["score"], 0.87)
        self.assertEqual(by_doc["c"]["arm_scores"]["dense"]["rank"], 3)
        # c: dense (0.87-0.60)/(0.91-0.60) * 0.2 + lexical 1.0 * 0.3 + sparse (0.85-0.30)/(0.90-0.30) * 0.5
        expected_c = 0.2 * (0.27 / 0.31) + 0.3 + 0.5 * (0.55 / 0.60)
        self.assertAlmostEqual(by_doc["c"]["rank"], expected_c, places=6)
        # Documents absent from an arm contribute nothing for that arm.
        self.assertEqual(set(by_doc["g"]["arm_scores"]), {"passage-lexical", "sparse-exact"})
        self.assertEqual(order(results)[:2], ["c", "g"])
        self.assertEqual(
            report,
            {
                "dense": {"candidates": 6, "documents": 6, "normalized": True},
                "passage-lexical": {"candidates": 5, "documents": 5, "normalized": True},
                "sparse-exact": {"candidates": 3, "documents": 3, "normalized": True},
            },
        )
        self.assertNotIn("_arm_scores", results[0])

    def test_convex_defaults_keep_rrf_top_ranks_and_exact_precedence(self) -> None:
        rrf = order(collapse_document_candidates(fixture_legs(), limit=20, fusion="rrf"))
        convex = order(collapse_document_candidates(
            fixture_legs(), limit=20, fusion="convex", alphas=fusion.DEFAULT_FUSION_ALPHAS,
        ))
        self.assertEqual(rrf[:2], ["c", "g"])
        self.assertEqual(convex[:2], rrf[:2])
        self.assertEqual(set(convex), set(rrf))
        # Dense-only documents keep their dense order under both modes.
        dense_only = [doc for doc in rrf if doc in {"d", "e", "f"}]
        self.assertEqual([doc for doc in convex if doc in {"d", "e", "f"}], dense_only)
        # A single exact identifier hit still precedes every dense-only document.
        exact = leg("sparse-exact", [("exact", 0.01)])
        dense = leg("dense", [(f"dense{i}", 0.99 - i / 100) for i in range(5)])
        ranked = order(collapse_document_candidates(
            (("dense", 0.15, dense), ("sparse-exact", 0.55, exact)),
            limit=6, fusion="convex", alphas=fusion.DEFAULT_FUSION_ALPHAS,
        ))
        self.assertEqual(ranked[0], "exact")

    def test_small_legs_fall_back_to_rank_scores(self) -> None:
        # Two candidates: min-max would spread them to 1.0 and 0.0 no matter
        # how close their scores are; ranks keep them near each other.
        report: dict = {}
        results = collapse_document_candidates(
            (("sparse-exact", 0.55, leg("sparse-exact", [("x", 0.90), ("y", 0.89)])),),
            limit=5, fusion="convex", alphas=fusion.DEFAULT_FUSION_ALPHAS, fusion_report=report,
        )
        self.assertEqual(report["sparse-exact"], {"candidates": 2, "documents": 2, "normalized": False})
        scores = [row["arm_scores"]["sparse-exact"]["normalized"] for row in results]
        self.assertEqual(scores[0], 1.0)
        self.assertAlmostEqual(scores[1], 61 / 62, places=6)
        self.assertAlmostEqual(results[0]["rank"], 0.55, places=6)
        # Three distinct documents is the threshold; duplicates of one
        # document do not count as separate candidates.
        report.clear()
        collapse_document_candidates(
            (("sparse-exact", 0.55, leg("sparse-exact", [("x", 0.9), ("x", 0.8), ("x", 0.7), ("y", 0.5)])),),
            limit=5, fusion="convex", alphas=fusion.DEFAULT_FUSION_ALPHAS, fusion_report=report,
        )
        self.assertEqual(report["sparse-exact"], {"candidates": 4, "documents": 2, "normalized": False})

    def test_recent_first_legs_fall_back_to_rank_scores(self) -> None:
        # A text arm that timed out on ranking returns its most recent matches
        # with score 0.0; the recency order is the only signal left.
        recent = leg("passage-lexical", [("r1", 0.0), ("r2", 0.0), ("r3", 0.0), ("r4", 0.0)])
        report: dict = {}
        results = collapse_document_candidates(
            (("dense", 0.15, leg("dense", [("a", 0.9), ("b", 0.8), ("r3", 0.7)])),
             ("passage-lexical", 0.30, recent)),
            limit=10, fusion="convex", alphas=fusion.DEFAULT_FUSION_ALPHAS, fusion_report=report,
        )
        self.assertEqual(report["passage-lexical"], {"candidates": 4, "documents": 4, "normalized": False})
        self.assertEqual(report["dense"]["normalized"], True)
        by_doc = {order([row])[0]: row for row in results}
        self.assertEqual(by_doc["r1"]["arm_scores"]["passage-lexical"]["normalized"], 1.0)
        self.assertAlmostEqual(by_doc["r4"]["arm_scores"]["passage-lexical"]["normalized"], 61 / 64, places=6)
        self.assertAlmostEqual(by_doc["r1"]["rank"], 0.30, places=6)
        # r3 is in both arms: dense minimum (0.0) plus its recency rank share.
        self.assertAlmostEqual(by_doc["r3"]["rank"], 0.30 * 61 / 63, places=6)
        self.assertEqual(order(results)[0], "r1")

    def test_rrf_flag_restores_prior_ordering(self) -> None:
        legs = fixture_legs()
        expected = sorted(
            {
                doc: sum(
                    weight / (60 + rank)
                    for _name, weight, rows in legs
                    for rank, row in enumerate(rows, start=1)
                    if order([row])[0] == doc
                )
                for doc in "abcdefghi"
            }.items(),
            key=lambda item: -item[1],
        )
        report: dict = {}
        results = collapse_document_candidates(legs, limit=20, fusion="rrf", fusion_report=report)
        self.assertEqual(order(results), [doc for doc, _ in expected])
        for row, (_doc, score) in zip(results, expected, strict=True):
            self.assertAlmostEqual(row["rank"], score, places=8)
        self.assertTrue(all(value["normalized"] is False for value in report.values()))
        self.assertAlmostEqual(results[0]["arm_scores"]["dense"]["normalized"], 1 / 63, places=8)


class FusionAlphaParsingTests(unittest.TestCase):
    def test_alpha_parsing_accepts_aliases_and_defaults(self) -> None:
        self.assertEqual(fusion.parse_fusion_alphas(None), fusion.DEFAULT_FUSION_ALPHAS)
        self.assertEqual(fusion.parse_fusion_alphas("  "), fusion.DEFAULT_FUSION_ALPHAS)
        self.assertEqual(
            fusion.parse_fusion_alphas("dense:0.5, lexical:0.3 ,sparse:0.2"),
            {"dense": 0.5, "passage-lexical": 0.3, "sparse-exact": 0.2},
        )
        self.assertEqual(
            fusion.parse_fusion_alphas("sparse-exact:1,passage-lexical:0,dense:0"),
            {"dense": 0.0, "passage-lexical": 0.0, "sparse-exact": 1.0},
        )
        self.assertAlmostEqual(sum(fusion.DEFAULT_FUSION_ALPHAS.values()), 1.0)
        self.assertEqual(fusion.parse_fusion_mode(None), "convex")
        self.assertEqual(fusion.parse_fusion_mode(" RRF "), "rrf")

    def test_alpha_parsing_rejects_non_simplex_and_bad_shapes(self) -> None:
        for bad in (
            "dense:0.5,lexical:0.3,sparse:0.3",     # sums to 1.1
            "dense:0.5,lexical:0.5",                # sparse missing
            "dense:-0.2,lexical:0.7,sparse:0.5",    # negative
            "dense:nan,lexical:0.5,sparse:0.5",     # non-finite
            "dense:0.5,dense:0.5,sparse:0",         # duplicate arm
            "dense:0.5,lexical:0.5,sparse:0,extra:0",
            "dense=0.5,lexical=0.3,sparse=0.2",
            "0.5,0.3,0.2",
        ):
            with self.assertRaises(ValueError, msg=bad):
                fusion.parse_fusion_alphas(bad)
        with self.assertRaises(ValueError):
            fusion.parse_fusion_mode("linear")
        # A zero weight is a valid simplex vertex.
        self.assertEqual(
            fusion.parse_fusion_alphas("dense:0.5,lexical:0.5,sparse:0")["sparse-exact"], 0.0,
        )

    def test_bad_env_fails_brainstore_startup(self) -> None:
        from recall_server.db import BrainStore

        with mock.patch.dict(os.environ, {"RECALL_SEARCH_FUSION_ALPHAS": "dense:0.9,lexical:0.9,sparse:0.9"}):
            with self.assertRaisesRegex(ValueError, "RECALL_SEARCH_FUSION_ALPHAS"):
                BrainStore("postgresql://synthetic.invalid/recall")
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_FUSION": "linear"}):
            with self.assertRaisesRegex(ValueError, "RECALL_SEARCH_FUSION"):
                BrainStore("postgresql://synthetic.invalid/recall")
        with mock.patch.dict(
            os.environ,
            {"RECALL_SEARCH_FUSION": "rrf", "RECALL_SEARCH_FUSION_ALPHAS": "dense:0.2,lexical:0.3,sparse:0.5"},
        ):
            store = BrainStore("postgresql://synthetic.invalid/recall")
            self.assertEqual(store.fusion_mode, "rrf")
            self.assertEqual(store.fusion_alphas, {"dense": 0.2, "passage-lexical": 0.3, "sparse-exact": 0.5})
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RECALL_SEARCH_FUSION", None)
            os.environ.pop("RECALL_SEARCH_FUSION_ALPHAS", None)
            store = BrainStore("postgresql://synthetic.invalid/recall")
            self.assertEqual(store.fusion_mode, "convex")
            self.assertEqual(store.fusion_alphas, fusion.DEFAULT_FUSION_ALPHAS)


if __name__ == "__main__":
    unittest.main()
