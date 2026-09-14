"""H2-h: temporal hints parsed from the question boost the matching window."""

from __future__ import annotations

import os
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server import temporal_hints  # noqa: E402
from recall_server.fusion import RRF_LEG_WEIGHTS  # noqa: E402
from recall_server.passage_retrieval import (  # noqa: E402
    PassageHintRetrieval,
    collapse_document_candidates,
    merge_dense_pools,
)
from recall_server.temporal_hints import (  # noqa: E402
    TemporalHint,
    TemporalHintSettings,
    parse_temporal_hint,
    temporal_settings_from_env,
    window_intersects,
)
from tests.central_brain.test_canonical_retrieval import ActorRecordingStore  # noqa: E402
from tests.central_brain.test_passage_fusion import candidate  # noqa: E402

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


def _hint(query: str) -> tuple[str, str, str] | None:
    hint = parse_temporal_hint(query, now=NOW)
    if hint is None:
        return None
    return hint.since[:10], hint.until[:10], hint.confidence


class ParserTableTests(unittest.TestCase):
    POSITIVE = [
        # explicit dates
        ("the deploy on 2026-05-03 failed", ("2026-05-03", "2026-05-03", "exact")),
        ("between 2026-05-02 and 2026-05-04", ("2026-05-02", "2026-05-04", "exact")),
        ("2026-05-02..2026-05-04", ("2026-05-02", "2026-05-04", "exact")),
        ("May 3", ("2026-05-03", "2026-05-03", "exact")),
        ("May 3rd, 2025", ("2025-05-03", "2025-05-03", "exact")),
        ("on the 3rd of May", ("2026-05-03", "2026-05-03", "exact")),
        ("3 May", ("2026-05-03", "2026-05-03", "exact")),
        ("May 2-4", ("2026-05-02", "2026-05-04", "exact")),
        ("May 2 to 4", ("2026-05-02", "2026-05-04", "exact")),
        ("between May 2 and May 4", ("2026-05-02", "2026-05-04", "exact")),
        ("May 30 - June 2", ("2026-05-30", "2026-06-02", "exact")),
        ("Sep 14 2026", ("2026-09-14", "2026-09-14", "exact")),
        ("Aug 14", ("2026-08-14", "2026-08-14", "exact")),
        ("Oct 3", ("2025-10-03", "2025-10-03", "exact")),  # not after now: last year
        ("on 5/3 we shipped", ("2026-05-03", "2026-05-03", "exact")),
        ("5/3/2025", ("2025-05-03", "2025-05-03", "exact")),
        ("5/2-5/4", ("2026-05-02", "2026-05-04", "exact")),
        # hedged day-level hints widen and become loose
        (
            "What was the P2 issue Greptile flagged on PR #6076 around May 2-4, and how did we fix it?",
            ("2026-04-29", "2026-05-07", "loose"),
        ),
        ("roughly 2026-05-03", ("2026-04-30", "2026-05-06", "loose")),
        # months, optional year, parts
        ("in May", ("2026-05-01", "2026-05-31", "loose")),
        ("May 2026", ("2026-05-01", "2026-05-31", "loose")),
        ("early May", ("2026-05-01", "2026-05-10", "loose")),
        ("mid-August", ("2026-08-11", "2026-08-20", "loose")),
        ("late September 2025", ("2025-09-21", "2025-09-30", "loose")),
        ("in august", ("2026-08-01", "2026-08-31", "loose")),
        ("what happened in October", ("2025-10-01", "2025-10-31", "loose")),
        # relative
        ("today", ("2026-09-14", "2026-09-14", "exact")),
        ("yesterday", ("2026-09-13", "2026-09-13", "exact")),
        ("last tuesday", ("2026-09-08", "2026-09-08", "exact")),
        ("last week", ("2026-09-07", "2026-09-14", "loose")),
        ("last month", ("2026-08-01", "2026-08-31", "loose")),
        ("two weeks ago", ("2026-08-28", "2026-09-03", "loose")),
        ("3 days ago", ("2026-09-10", "2026-09-12", "loose")),
        ("past 10 days", ("2026-09-04", "2026-09-14", "loose")),
        ("this month", ("2026-09-01", "2026-09-14", "loose")),
        # quarters and years
        ("Q2", ("2026-04-01", "2026-06-30", "loose")),
        ("Q2 2026", ("2026-04-01", "2026-06-30", "loose")),
        ("2026Q2", ("2026-04-01", "2026-06-30", "loose")),
        ("second quarter of 2025", ("2025-04-01", "2025-06-30", "loose")),
        ("Q4", ("2025-10-01", "2025-12-31", "loose")),  # Q4 2026 has not started
        ("in 2025", ("2025-01-01", "2025-12-31", "loose")),
    ]
    NEGATIVE = [
        "P2 issue",
        "v2 of the api",
        "PR #6076",
        "a 503 from the proxy",
        "items 2-4 of the list",
        "release 1.2/3",
        "score 5/3/x",
        "how may I help",
        "it may be broken",
        "mar the surface",
        "dec the counter",
        "sep",
        "2026-13-40",
        "Q5",
        "January 2027 plan",  # entirely in the future
        "why did the deploy fail",
        "",
        "   ",
    ]

    def test_positive_table(self) -> None:
        for query, expected in self.POSITIVE:
            with self.subTest(query=query):
                self.assertEqual(_hint(query), expected)

    def test_negative_table(self) -> None:
        for query in self.NEGATIVE:
            with self.subTest(query=query):
                self.assertIsNone(_hint(query))

    def test_table_is_large_enough(self) -> None:
        self.assertGreaterEqual(len(self.POSITIVE) + len(self.NEGATIVE), 20)

    def test_span_is_only_the_date_phrase(self) -> None:
        hint = parse_temporal_hint(
            "secret customer name around May 2-4 and more secrets", now=NOW
        )
        assert hint is not None
        self.assertEqual(hint.span, "around May 2-4")
        self.assertEqual(hint.since, "2026-04-29T00:00:00+00:00")
        self.assertEqual(hint.until, "2026-05-07T23:59:59+00:00")
        self.assertEqual(
            hint.as_diagnostics(1.25),
            {
                "since": "2026-04-29T00:00:00+00:00",
                "until": "2026-05-07T23:59:59+00:00",
                "confidence": "loose",
                "boost": 1.25,
            },
        )

    def test_day_level_flag_survives_a_hedge_but_not_wide_forms(self) -> None:
        for query, expected in (
            ("around May 2-4", True),
            ("roughly 2026-05-03", True),
            ("on 5/3", True),
            ("yesterday", True),
            ("last tuesday", True),
            ("in May", False),
            ("early May", False),
            ("last week", False),
            ("two weeks ago", False),
            ("Q2", False),
            ("in 2025", False),
        ):
            with self.subTest(query=query):
                hint = parse_temporal_hint(query, now=NOW)
                assert hint is not None
                self.assertEqual(hint.day_level, expected)

    def test_day_level_hint_wins_over_wider_phrases(self) -> None:
        self.assertEqual(_hint("May 2-4 last year"), ("2026-05-02", "2026-05-04", "exact"))
        self.assertEqual(_hint("in Q2, on May 3"), ("2026-05-03", "2026-05-03", "exact"))

    def test_naive_now_is_utc(self) -> None:
        hint = parse_temporal_hint("yesterday", now=datetime(2026, 9, 14, 1, 0))
        assert hint is not None
        self.assertEqual(hint.since[:10], "2026-09-13")

    def test_window_intersects(self) -> None:
        since, until = "2026-05-02T00:00:00+00:00", "2026-05-04T23:59:59+00:00"
        self.assertTrue(window_intersects("2026-05-03 10:00:00+00:00", "2026-05-03 11:00:00+00:00", since, until))
        self.assertTrue(window_intersects("2026-04-01 00:00:00+00:00", "2026-05-02 00:00:00+00:00", since, until))
        self.assertTrue(window_intersects("2026-05-04 23:00:00+00:00", "2026-06-01 00:00:00+00:00", since, until))
        self.assertFalse(window_intersects("2026-05-05 00:00:00+00:00", "2026-05-06 00:00:00+00:00", since, until))
        self.assertFalse(window_intersects("2026-04-01 00:00:00+00:00", "2026-05-01 23:59:59+00:00", since, until))
        self.assertFalse(window_intersects(None, "2026-05-03 00:00:00+00:00", since, until))
        self.assertFalse(window_intersects("garbage", "2026-05-03 00:00:00+00:00", since, until))


class SettingsTests(unittest.TestCase):
    def test_defaults(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            settings = temporal_settings_from_env()
        self.assertEqual(
            settings,
            TemporalHintSettings(enabled=True, boost_exact=0.5, boost_loose=0.25, window_budget_ms=150),
        )
        self.assertEqual(settings.boost_for("exact"), 0.5)
        self.assertEqual(settings.boost_for("loose"), 0.25)

    def test_env_overrides_and_validation(self) -> None:
        env = {
            "RECALL_TEMPORAL_HINTS": "off",
            "RECALL_TEMPORAL_BOOST": "1.0",
            "RECALL_TEMPORAL_BOOST_LOOSE": "0.1",
            "RECALL_TEMPORAL_WINDOW_BUDGET_MS": "300",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            settings = temporal_settings_from_env()
        self.assertEqual(
            settings,
            TemporalHintSettings(enabled=False, boost_exact=1.0, boost_loose=0.1, window_budget_ms=300),
        )
        for name, value in (
            ("RECALL_TEMPORAL_HINTS", "maybe"),
            ("RECALL_TEMPORAL_BOOST", "-1"),
            ("RECALL_TEMPORAL_BOOST", "nan"),
            ("RECALL_TEMPORAL_BOOST_LOOSE", "7"),
            ("RECALL_TEMPORAL_WINDOW_BUDGET_MS", "1"),
        ):
            with self.subTest(name=name, value=value), mock.patch.dict(
                os.environ, {name: value}, clear=True
            ), self.assertRaises(ValueError):
                temporal_settings_from_env()

    def test_bad_env_fails_brainstore_startup(self) -> None:
        from recall_server.db import BrainStore

        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_BOOST": "banana"}):
            with self.assertRaises(ValueError):
                BrainStore("postgresql://synthetic-user:synthetic-pass@127.0.0.1:1/synthetic")
        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_HINTS": "off"}):
            store = BrainStore("postgresql://synthetic-user:synthetic-pass@127.0.0.1:1/synthetic")
        self.assertFalse(store.temporal_hints.enabled)


def _rows(kind: str, pairs: list[tuple[str, float, str]]) -> list[dict]:
    rows = []
    for document, score, day in pairs:
        row = candidate(document, kind, score)
        row["first_occurred_at"] = f"{day} 09:00:00+00:00"
        row["last_occurred_at"] = f"{day} 10:00:00+00:00"
        rows.append(row)
    return rows


def _order(results: list[dict]) -> list[str]:
    return [row["logical_document_id"][5:].rstrip("0") for row in results]


class CollapseBoostTests(unittest.TestCase):
    """The boost multiplies the fused score before ranking and truncation."""

    WINDOW = ("2026-05-02T00:00:00+00:00", "2026-05-04T23:59:59+00:00")

    def _legs(self):
        # Six dense-only documents from other months outrank the one May doc.
        dense = _rows("dense", [
            ("a", 0.95, "2026-03-01"), ("b", 0.94, "2026-04-01"), ("c", 0.93, "2026-06-01"),
            ("d", 0.92, "2026-07-01"), ("e", 0.91, "2026-08-01"), ("f", 0.90, "2026-05-03"),
        ])
        return (("dense", RRF_LEG_WEIGHTS["dense"], dense),)

    def test_boost_reorders_and_marks_intersecting_documents(self) -> None:
        plain = collapse_document_candidates(self._legs(), limit=10)
        self.assertEqual(_order(plain), ["a", "b", "c", "d", "e", "f"])
        self.assertTrue(all("temporal_boost" not in row for row in plain))
        boosted = collapse_document_candidates(
            self._legs(), limit=10, window_boost=(*self.WINDOW, 1.5)
        )
        self.assertEqual(_order(boosted)[0], "f")
        self.assertEqual(_order(boosted)[1:], ["a", "b", "c", "d", "e"])
        f_plain = next(row for row in plain if _order([row]) == ["f"])
        f_boosted = boosted[0]
        self.assertEqual(f_boosted["temporal_boost"], 1.5)
        self.assertAlmostEqual(f_boosted["rank"], round(f_plain["rank"] * 1.5, 8))
        self.assertTrue(all("temporal_boost" not in row for row in boosted[1:]))
        # arm evidence is untouched by the boost
        self.assertEqual(f_boosted["arm_scores"], f_plain["arm_scores"])

    def test_boost_applies_before_truncation(self) -> None:
        # With limit=3 the May document would have been cut before any
        # post-collapse boost could lift it.
        boosted = collapse_document_candidates(
            self._legs(), limit=3, window_boost=(*self.WINDOW, 1.25)
        )
        self.assertIn("f", _order(boosted))
        self.assertEqual(len(boosted), 3)

    def test_documents_straddling_the_window_are_boosted(self) -> None:
        rows = _rows("dense", [("x", 0.9, "2026-05-01")])
        rows[0]["last_occurred_at"] = "2026-05-02 00:00:01+00:00"
        legs = (("dense", RRF_LEG_WEIGHTS["dense"], rows),)
        boosted = collapse_document_candidates(legs, limit=5, window_boost=(*self.WINDOW, 1.5))
        self.assertEqual(boosted[0]["temporal_boost"], 1.5)

    def test_boost_applies_before_the_fused_cut_and_nominations_follow(self) -> None:
        # H2-i nominations: with limit=3 the plain head is a,b,c and the
        # dense top-10 rest (d,e,f) trail as nominated. With the boost f
        # moves into the head; the nominations still follow the head.
        plain = collapse_document_candidates(self._legs(), limit=3, nominate_per_arm=10)
        self.assertEqual(_order(plain), ["a", "b", "c", "d", "e", "f"])
        self.assertEqual([row.get("nominated", False) for row in plain], [False] * 3 + [True] * 3)
        boosted = collapse_document_candidates(
            self._legs(), limit=3, nominate_per_arm=10, window_boost=(*self.WINDOW, 1.5)
        )
        self.assertEqual(_order(boosted), ["f", "a", "b", "c", "d", "e"])
        self.assertEqual([row.get("nominated", False) for row in boosted], [False] * 3 + [True] * 3)
        self.assertEqual(boosted[0]["temporal_boost"], 1.5)
        self.assertNotIn("nominated", boosted[0])

    def test_factor_one_is_a_no_op_for_scores_but_still_marks(self) -> None:
        boosted = collapse_document_candidates(self._legs(), limit=10, window_boost=(*self.WINDOW, 1.0))
        self.assertEqual(_order(boosted), ["a", "b", "c", "d", "e", "f"])


class MergeDensePoolsTests(unittest.TestCase):
    def test_union_adds_new_documents_and_reorders_by_score(self) -> None:
        primary = _rows("dense", [("a", 0.9, "2026-03-01"), ("b", 0.8, "2026-04-01")])
        windowed = _rows("dense", [("a", 0.9, "2026-03-01"), ("w", 0.85, "2026-05-03"), ("z", 0.1, "2026-05-03")])
        merged, added = merge_dense_pools(primary, windowed)
        self.assertEqual(added, 2)
        self.assertEqual(_order(merged), ["a", "w", "b", "z"])
        self.assertEqual(merge_dense_pools(primary, []), (primary, 0))
        self.assertEqual(merge_dense_pools(primary, primary[:1]), (primary, 0))


class SearchWiringTests(unittest.TestCase):
    """search() derives the hint, boosts the collapse, and runs the window pass."""

    class _Store(ActorRecordingStore):
        search_deadline_ms = 20000
        rerank_runtime = None
        temporal_hints = None  # fall through to the environment

    def _retrieval(self, store, *, dense_calls: list[dict] | None = None):
        retrieval = PassageHintRetrieval(
            store,
            tenant_id="tenant:test",
            sources=["codex:linux:test"],
            policy_fingerprint="fp-policy",
        )
        dense = _rows("dense", [
            ("a", 0.95, "2026-03-01"), ("b", 0.94, "2026-04-01"), ("c", 0.93, "2026-06-01"),
            ("f", 0.90, "2026-05-03"),
        ])
        windowed = _rows("dense", [("f", 0.90, "2026-05-03"), ("w", 0.80, "2026-05-02")])
        lexical = _rows("passage-lexical", [("c", 0.42, "2026-06-01"), ("a", 0.31, "2026-03-01")])

        def fake_dense(query, **kwargs):
            if dense_calls is not None:
                dense_calls.append(dict(kwargs))
            if kwargs.get("since") is not None:
                return windowed, "ok", "exact-scoped", 2
            return dense, "ok", "ann-oversampled", None

        retrieval._dense_candidates = fake_dense
        retrieval._lexical_candidates = lambda query, **kwargs: (lexical, "ok")
        retrieval._sparse_candidates = lambda query, original_query=None, **kwargs: ([], "skipped-prose-query")
        return retrieval

    def _search(self, retrieval, query, **kwargs):
        return retrieval.search(
            query, lexical_query="deploy fail", since=None, until=None, limit=10, now=NOW, **kwargs
        )

    @staticmethod
    def _normalise(response: dict) -> dict:
        response["diagnostics"].pop("elapsed_ms")
        response["diagnostics"]["arm_elapsed_ms"] = {
            key: 0.0 for key in response["diagnostics"]["arm_elapsed_ms"]
        }
        return response

    def test_query_without_a_hint_is_byte_identical(self) -> None:
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {}, clear=True):
            on = self._normalise(self._search(self._retrieval(self._Store(), dense_calls=calls), "why did the deploy fail"))
        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_HINTS": "off"}, clear=True):
            off = self._normalise(self._search(self._retrieval(self._Store()), "why did the deploy fail"))
        self.assertEqual(on, off)
        self.assertNotIn("temporal_hint", on["diagnostics"])
        self.assertNotIn("dense_window_status", on["diagnostics"])
        self.assertEqual(set(on["diagnostics"]["arm_elapsed_ms"]), {"dense", "passage_lexical", "sparse_exact"})
        self.assertEqual(len(calls), 1)
        self.assertIsNone(calls[0]["since"])
        self.assertTrue(all("temporal_boost" not in row for row in on["results"]))

    def test_wide_loose_hint_boosts_without_a_window_pass(self) -> None:
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {}, clear=True):
            response = self._search(
                self._retrieval(self._Store(), dense_calls=calls),
                "the P2 Greptile flagged on PR #6076 in early May",
            )
        diagnostics = response["diagnostics"]
        self.assertEqual(
            diagnostics["temporal_hint"],
            {
                "since": "2026-05-01T00:00:00+00:00",
                "until": "2026-05-10T23:59:59+00:00",
                "confidence": "loose",
                "boost": 1.25,
            },
        )
        self.assertEqual(diagnostics["temporal_boosted"], 1)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("dense_window", diagnostics["arm_elapsed_ms"])
        self.assertNotIn("dense_window_status", diagnostics)
        f = next(row for row in response["results"] if _order([row]) == ["f"])
        self.assertEqual(f["temporal_boost"], 1.25)
        # convex: f is the dense minimum (normalised 0.0); floored at b's
        # fused score 0.12 and x1.25 it lands above b, below the lexical hits.
        self.assertEqual(_order(response["results"]), ["a", "c", "f", "b"])
        self.assertAlmostEqual(f["rank"], 0.15, places=6)

    def test_hedged_day_range_keeps_the_window_pass_with_the_loose_boost(self) -> None:
        # The validation case: "around May 2-4". Loose boost over the padded
        # window, and the windowed dense pass still runs so the gold document
        # at the bottom of the global pool is guaranteed into the collapse.
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {}, clear=True):
            response = self._search(
                self._retrieval(self._Store(), dense_calls=calls),
                "the P2 Greptile flagged on PR #6076 around May 2-4",
            )
        diagnostics = response["diagnostics"]
        self.assertEqual(
            diagnostics["temporal_hint"],
            {
                "since": "2026-04-29T00:00:00+00:00",
                "until": "2026-05-07T23:59:59+00:00",
                "confidence": "loose",
                "boost": 1.25,
            },
        )
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["since"], "2026-04-29T00:00:00+00:00")
        self.assertEqual(calls[1]["until"], "2026-05-07T23:59:59+00:00")
        self.assertEqual(diagnostics["dense_window_status"], "ok")
        self.assertEqual(diagnostics["dense_window_added"], 1)
        self.assertIn("dense_window", diagnostics["arm_elapsed_ms"])
        # both f (05-03) and the window-added w (05-02) sit in the padded window
        self.assertEqual(diagnostics["temporal_boosted"], 2)
        order = _order(response["results"])
        self.assertIn("w", order)
        w = next(row for row in response["results"] if _order([row]) == ["w"])
        self.assertEqual(w["temporal_boost"], 1.25)

    def test_exact_hint_runs_the_windowed_dense_pass_and_unions_pools(self) -> None:
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {}, clear=True):
            response = self._search(
                self._retrieval(self._Store(), dense_calls=calls),
                "what did Greptile flag on 2026-05-03",
            )
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["temporal_hint"]["confidence"], "exact")
        self.assertEqual(diagnostics["temporal_hint"]["boost"], 1.5)
        self.assertEqual(len(calls), 2)
        self.assertIsNone(calls[0]["since"])
        self.assertEqual(calls[1]["since"], "2026-05-03T00:00:00+00:00")
        self.assertEqual(calls[1]["until"], "2026-05-03T23:59:59+00:00")
        self.assertLessEqual(calls[1]["deadline_at"], calls[0]["deadline_at"])
        self.assertLessEqual(calls[1]["deadline_at"] - time.monotonic(), 0.151)
        self.assertEqual(diagnostics["dense_window_status"], "ok")
        self.assertEqual(diagnostics["dense_window_strategy"], "exact-scoped")
        self.assertEqual(diagnostics["dense_window_candidates"], 2)
        self.assertEqual(diagnostics["dense_window_added"], 1)
        self.assertEqual(diagnostics["dense_candidates"], 5)
        self.assertIn("dense_window", diagnostics["arm_elapsed_ms"])
        self.assertEqual(diagnostics["temporal_boosted"], 1)
        order = _order(response["results"])
        # w entered the pool from the windowed pass; f (floored, x1.5) rises above b.
        self.assertEqual(order, ["a", "c", "f", "b", "w"])
        w = next(row for row in response["results"] if _order([row]) == ["w"])
        self.assertNotIn("temporal_boost", w)  # 05-02 is outside the one-day window

    def test_env_off_disables_boost_and_window_pass(self) -> None:
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_HINTS": "off"}, clear=True):
            response = self._search(
                self._retrieval(self._Store(), dense_calls=calls),
                "what did Greptile flag on 2026-05-03",
            )
        self.assertNotIn("temporal_hint", response["diagnostics"])
        self.assertEqual(len(calls), 1)
        self.assertTrue(all("temporal_boost" not in row for row in response["results"]))

    def test_store_settings_win_over_the_environment(self) -> None:
        store = self._Store()
        store.temporal_hints = TemporalHintSettings(boost_exact=2.0, window_budget_ms=40)
        calls: list[dict] = []
        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_HINTS": "off"}, clear=True):
            response = self._search(
                self._retrieval(store, dense_calls=calls), "what did Greptile flag on 2026-05-03"
            )
        self.assertEqual(response["diagnostics"]["temporal_hint"]["boost"], 3.0)
        self.assertLessEqual(calls[1]["deadline_at"] - time.monotonic(), 0.041)

    def test_explicit_filters_skip_the_hint(self) -> None:
        calls: list[dict] = []
        retrieval = self._retrieval(self._Store(), dense_calls=calls)
        with mock.patch.dict(os.environ, {}, clear=True):
            response = retrieval.search(
                "what did Greptile flag on 2026-05-03", lexical_query="greptile",
                since="2026-01-01T00:00:00+00:00", until=None, limit=10, now=NOW,
            )
        self.assertNotIn("temporal_hint", response["diagnostics"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["since"], "2026-01-01T00:00:00+00:00")

    def test_window_pass_is_skipped_when_the_global_pass_failed(self) -> None:
        calls: list[dict] = []
        retrieval = self._retrieval(self._Store(), dense_calls=calls)
        retrieval._dense_candidates = lambda query, **kwargs: (calls.append(kwargs), [], "deadline-exceeded", "unavailable", None)[1:]
        with mock.patch.dict(os.environ, {}, clear=True):
            response = self._search(retrieval, "what did Greptile flag on 2026-05-03")
        self.assertEqual(len(calls), 1)
        self.assertNotIn("dense_window_status", response["diagnostics"])
        self.assertIn("temporal_hint", response["diagnostics"])

    def test_boost_feeds_the_rerank_blend(self) -> None:
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests

        store = self._Store()
        store.rerank_runtime = RerankWiringTests._FakeRerank({"a": 0.9, "f": 0.9, "b": 0.5})
        store.rerank_min_budget_seconds = 0.0
        store.rerank_blend = 0.5
        query = "what did Greptile flag on 2026-05-03"
        with mock.patch.dict(os.environ, {}, clear=True):
            boosted = self._search(self._retrieval(store), query)
        with mock.patch.dict(os.environ, {"RECALL_TEMPORAL_HINTS": "off"}, clear=True):
            plain = self._search(self._retrieval(store), query)
        self.assertEqual(boosted["diagnostics"]["rerank_status"], "ok")
        self.assertEqual(plain["diagnostics"]["rerank_status"], "ok")

        def blended(response, name):
            return next(row for row in response["results"] if _order([row]) == [name])["blended_score"]

        # the boosted fused score is what the blend mixes with the reranker
        self.assertGreater(blended(boosted, "f"), blended(plain, "f"))
        self.assertEqual(blended(boosted, "a"), blended(plain, "a"))


class HintTypeTests(unittest.TestCase):
    def test_hint_is_frozen_and_content_free(self) -> None:
        hint = TemporalHint(since="a", until="b", confidence="exact", span="May 3")
        with self.assertRaises(Exception):
            hint.since = "c"  # type: ignore[misc]
        self.assertEqual(set(hint.as_diagnostics(1.5)), {"since", "until", "confidence", "boost"})

    def test_module_constants(self) -> None:
        self.assertEqual(temporal_hints.HEDGE_PAD_DAYS, 3)
        self.assertEqual(temporal_hints.DEFAULT_TEMPORAL_WINDOW_BUDGET_MS, 150)


if __name__ == "__main__":
    unittest.main()
