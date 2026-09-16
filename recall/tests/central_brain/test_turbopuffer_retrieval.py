"""turbopuffer search plane: arms, filters, row mapping, wiring, plane switch."""

from __future__ import annotations

import hashlib
import os
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server import turbopuffer_plane as plane  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings, namespace_schema, passage_row  # noqa: E402
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval, scope_filters  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402
from tests.central_brain.test_canonical_retrieval import RerankWiringTests  # noqa: E402

SETTINGS = TurbopufferSettings(api_key="synthetic-key")
TENANT = "tenant:test"
LDOC = "ldoc_" + "a" * 32


def catalog_passage(index: int, text: str, *, source="codex:linux:test", ldoc=LDOC, first="2026-05-03 10:00:00+00", actors=()):
    return {
        "passage_id": f"psg_{index:032x}", "source_id": source, "logical_document_id": ldoc,
        "policy_fingerprint": "fp-policy", "first_occurred_at": first, "last_occurred_at": first,
        "actors": list(actors), "native_parent_id": "parent", "revision": 2, "ordinal": index,
        "doc_first_occurred_at": "2026-05-03 09:00:00+00", "doc_last_occurred_at": "2026-05-03 12:00:00+00",
        "manifest_object_key": "objects/aa/bb", "manifest_content_sha256": "c" * 64, "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "roles": ["assistant"], "receipts": [f"recall://{source}/x-{index}?rev=1#item=0"],
        "spans": [{"message_index": index}], "header_redacted": "source family: codex", "text_redacted": text,
    }


class SettingsTests(unittest.TestCase):
    def test_env_key_file_and_plane(self) -> None:
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_PLANE": "turbopuffer"}, clear=False):
            self.assertEqual(plane.search_plane_from_env(), "turbopuffer")
        with mock.patch.dict(os.environ, {"RECALL_SEARCH_PLANE": "lance"}), self.assertRaises(plane.TurbopufferConfigError):
            plane.search_plane_from_env()
        with mock.patch.dict(os.environ, {"RECALL_TPUF_API_KEY": "", "RECALL_TPUF_KEY_FILE": ""}):
            self.assertIsNone(plane.turbopuffer_settings_from_env())
            with self.assertRaises(plane.TurbopufferConfigError):
                plane.turbopuffer_settings_from_env(required=True)
        with tempfile.TemporaryDirectory() as tmp:
            key = Path(tmp) / "key"
            key.write_text("secret-key\n")
            key.chmod(0o644)
            with mock.patch.dict(os.environ, {"RECALL_TPUF_API_KEY": "", "RECALL_TPUF_KEY_FILE": str(key)}), self.assertRaises(plane.TurbopufferConfigError):
                plane.turbopuffer_settings_from_env()
            key.chmod(stat.S_IRUSR | stat.S_IWUSR)
            with mock.patch.dict(os.environ, {"RECALL_TPUF_API_KEY": "", "RECALL_TPUF_KEY_FILE": str(key), "RECALL_TPUF_EMBED_DIMS": "1024"}):
                settings = plane.turbopuffer_settings_from_env()
            self.assertEqual((settings.api_key, settings.embed_dims, settings.region), ("secret-key", 1024, "aws-us-west-2"))
        self.assertNotIn("tenant", SETTINGS.namespace("tenant:company:parcha"))
        self.assertTrue(SETTINGS.namespace("tenant:company:parcha").startswith("recall-"))

    def test_schema_and_rows(self) -> None:
        schema = namespace_schema(SETTINGS)
        self.assertEqual(schema["text"]["full_text_search"]["stemming"], False)
        self.assertEqual(schema["embed_text"]["embed"], {"model": "voyage/voyage-4", "attribute": "vector", "dims": 512})
        self.assertEqual(schema["vector"], {"type": "[512]f16", "ann": True})
        self.assertFalse(schema["receipts"]["filterable"])
        row = passage_row(catalog_passage(1, "hello world", actors=[("author", "actor_" + "1" * 32)]))
        self.assertEqual(row["id"], "psg_" + "0" * 31 + "1")
        self.assertEqual(row["month"], "2026-05")
        self.assertEqual(row["actor_keys"], ["author:actor_" + "1" * 32])
        self.assertEqual(row["embed_text"], "source family: codex\n\nhello world")
        self.assertEqual(row["first_occurred_at"], "2026-05-03T10:00:00+00:00")


class ArmTests(unittest.TestCase):
    def _retrieval(self, client, **kwargs):
        store = RerankWiringTests._Store()
        return TurbopufferHintRetrieval(
            store, settings=SETTINGS, client=client, tenant_id=TENANT,
            sources=["codex:linux:test"], policy_fingerprint="fp-policy", **kwargs,
        ), store

    def _seed(self, client):
        ns = client.namespace(SETTINGS.namespace(TENANT))
        ns.write(upsert_rows=[
            passage_row(catalog_passage(1, "the deploy failed because the migration lock timed out", actors=[("author", "actor_" + "1" * 32)])),
            passage_row(catalog_passage(2, "greptile flagged PR #6076 as P2 on May 3", first="2026-05-03 11:00:00+00")),
            passage_row(catalog_passage(3, "unrelated chatter about lunch", source="claude:linux:other", ldoc="ldoc_" + "b" * 32)),
            passage_row(catalog_passage(4, "the deploy failed because the migration lock timed out", ldoc="ldoc_" + "c" * 32)),
        ])
        return ns

    def test_scope_filters(self) -> None:
        clauses = scope_filters(sources=["s1"], policy_fingerprint="fp", since="2026-05-01", until="2026-05-04 00:00:00+00", actor_ids=["a1"], actor_relations=["author"])
        self.assertEqual(clauses[0], ("source_id", "In", ["s1"]))
        self.assertEqual(clauses[2], ("last_occurred_at", "Gte", "2026-05-01T00:00:00+00:00"))
        self.assertEqual(clauses[3], ("first_occurred_at", "Lte", "2026-05-04T00:00:00+00:00"))
        self.assertEqual(clauses[4], ("actor_keys", "ContainsAny", ["author:a1"]))
        self.assertEqual(scope_filters(sources=["s1"], policy_fingerprint="fp", since=None, until=None, actor_ids=["a1"], actor_relations=None)[-1], ("actor_ids", "ContainsAny", ["a1"]))

    def test_dense_arm_embeds_nothing_locally_and_dedupes_texts(self) -> None:
        client = FakeTurbopuffer()
        ns = self._seed(client)
        retrieval, _ = self._retrieval(client)
        self.assertEqual(retrieval._embed_query("why did the deploy fail"), "why did the deploy fail")
        rows, status, strategy, scope = retrieval._dense_candidates(
            "why did the deploy fail", since=None, until=None, candidate_limit=40, actor_ids=None, actor_relations=None,
            deadline_at=time.monotonic() + 5, vector="why did the deploy fail",
        )
        self.assertEqual((status, strategy, scope), ("ok", "turbopuffer-ann", None))
        self.assertEqual(ns.queries[-1]["rank_by"], ("embed_text", "ANN", ["Embed", "why did the deploy fail"]))
        self.assertEqual(ns.queries[-1]["filters"], ("And", [("source_id", "In", ["codex:linux:test"]), ("policy_fingerprint", "Eq", "fp-policy")]))
        # Only the authorized source; the two identical texts collapse to one row.
        self.assertTrue(all(row["source_id"] == "codex:linux:test" for row in rows))
        self.assertEqual(rows[0]["logical_document_id"], LDOC)
        self.assertEqual(len([r for r in rows if "deploy failed" in r["text_redacted"]]), 1)
        self.assertEqual(rows[0]["header_redacted"], "source family: codex")
        self.assertEqual(rows[0]["spans"], [{"message_index": 1}])
        self.assertGreater(rows[0]["score"], rows[-1]["score"])

    def test_lexical_and_sparse_arms(self) -> None:
        client = FakeTurbopuffer()
        ns = self._seed(client)
        retrieval, _ = self._retrieval(client)
        common = dict(since=None, until=None, candidate_limit=40, actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5)
        rows, status = retrieval._lexical_candidates("deploy migration lock", **common)
        self.assertEqual(status, "ok")
        # BM25 scores arrive as $dist (as the live service does); the leg is ranked by them.
        from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval as _T
        scored = _T._scored([{"id": "psg_" + "1" * 32, "text": "x", "$dist": 0.9}, {"id": "psg_" + "2" * 32, "text": "y", "$dist": 2.5}])
        self.assertEqual([r["score"] for r in scored], [2.5, 0.9])
        self.assertEqual(ns.queries[-1]["rank_by"], ("text", "BM25", "deploy migration lock"))
        self.assertEqual(retrieval.lexical_plan, {"terms": 3, "plane": "turbopuffer"})
        self.assertTrue(rows and rows[0]["score"] >= rows[-1]["score"])
        rows, status = retrieval._sparse_candidates("greptile flagged 6076", original_query="what did Greptile flag on PR #6076?", **common)
        self.assertEqual(status, "ok")
        self.assertEqual([r["passage_id"] for r in rows], ["psg_" + "0" * 31 + "2"])
        self.assertIn("('text', 'ContainsAllTokens', '6076')", str(ns.queries[-1]["filters"]))
        self.assertEqual(retrieval._sparse_candidates("just prose words", **common), ([], "skipped-prose-query"))
        # Actor scope: the filter reaches turbopuffer; the sparse arm skips as on Postgres.
        scoped, _ = self._retrieval(client, actor_ids=("actor_" + "1" * 32,), actor_relations=("author",))
        rows, status = scoped._lexical_candidates("deploy", **{**common, "actor_ids": ["actor_" + "1" * 32], "actor_relations": ["author"]})
        self.assertEqual([r["passage_id"] for r in rows], ["psg_" + "0" * 31 + "1"])
        self.assertEqual(scoped._sparse_candidates("6076", **common)[1], "skipped-actor-scope")

    def test_statuses_on_failure_and_deadline(self) -> None:
        client = FakeTurbopuffer()
        ns = self._seed(client)
        retrieval, _ = self._retrieval(client)
        common = dict(since=None, until=None, candidate_limit=40, actor_ids=None, actor_relations=None)

        class FakeTimeout(Exception):
            pass

        ns.fail_queries = FakeTimeout("slow")
        self.assertEqual(retrieval._dense_candidates("q", deadline_at=time.monotonic() + 5, **common)[1], "deadline-exceeded")
        ns.fail_queries = RuntimeError("boom")
        self.assertEqual(retrieval._lexical_candidates("q", deadline_at=time.monotonic() + 5, **common)[1], "unavailable")
        ns.fail_queries = None
        self.assertEqual(retrieval._lexical_candidates("q", deadline_at=time.monotonic() - 1, **common)[1], "deadline-exceeded")

    def test_search_runs_fusion_rerank_and_hints_on_the_plane(self) -> None:
        client = FakeTurbopuffer()
        self._seed(client)
        retrieval, store = self._retrieval(client)
        store.rerank_runtime = RerankWiringTests._FakeRerank({"the deploy failed because the migration lock timed out": 0.9})
        store.query_clauses = False
        response = retrieval.search("In the Codex work, why did the deploy fail?", lexical_query="deploy fail", since=None, until=None, limit=10)
        diagnostics = response["diagnostics"]
        self.assertEqual(diagnostics["search_plane"], "turbopuffer")
        self.assertEqual(diagnostics["dense_status"], "ok")
        self.assertEqual(diagnostics["rerank_status"], "ok")
        self.assertEqual(diagnostics["source_hint"]["families"], ["codex"])
        self.assertEqual(response["results"][0]["logical_document_id"], LDOC)
        self.assertIn("arm_scores", response["results"][0])
        # Temporal hint: a dated question runs the window pass on the plane too.
        response = retrieval.search("what did greptile flag around May 2-4?", lexical_query="greptile flag", since=None, until=None, limit=10)
        self.assertIn("temporal_hint", response["diagnostics"])
        self.assertEqual(response["diagnostics"]["dense_window_status"], "ok")
        self.assertGreaterEqual(response["diagnostics"]["dense_window_candidates"], 1)

    def test_window_pass_owns_its_budget_and_runs_beside_the_arms(self) -> None:
        # Live: the 150 ms Postgres window budget timed out the filtered ANN
        # pass, the SDK retried four times, and every dated question paid
        # ~4 s for zero rows. The plane sizes the pass itself and the SDK
        # retries are off for the arms.
        client = FakeTurbopuffer()
        ns = self._seed(client)
        retrieval, store = self._retrieval(client)
        self.assertEqual(client.options, {"max_retries": 0})
        self.assertTrue(retrieval.window_pass_concurrent)
        self.assertEqual(retrieval.temporal_window_budget_ms, 1500)
        with mock.patch.dict(os.environ, {"RECALL_TPUF_WINDOW_BUDGET_MS": "800"}):
            self.assertEqual(self._retrieval(client)[0].temporal_window_budget_ms, 800)
        with mock.patch.dict(os.environ, {"RECALL_TPUF_WINDOW_BUDGET_MS": "5"}):
            self.assertEqual(self._retrieval(client)[0].temporal_window_budget_ms, 1500)
        store.query_clauses = False
        store.rerank_runtime = None
        # The window pass is submitted with the arms: its query reaches the
        # plane before the global dense pass returns, i.e. the plane sees the
        # windowed filters while the dense query is still outstanding.
        order: list[str] = []
        original_query = ns.query

        def recording_query(**kwargs):
            filters = repr(kwargs.get("filters"))
            order.append("window" if "last_occurred_at" in filters else "other")
            if order[-1] == "other" and kwargs.get("rank_by", ("",))[0] == plane.EMBED_TEXT_ATTRIBUTE:
                time.sleep(0.05)
            return original_query(**kwargs)

        ns.query = recording_query
        started = time.monotonic()
        response = retrieval.search("what did greptile flag on May 3?", lexical_query="greptile flag", since=None, until=None, limit=10)
        elapsed_ms = response["diagnostics"]["arm_elapsed_ms"]
        self.assertEqual(response["diagnostics"]["dense_window_status"], "ok")
        self.assertIn("window", order[:2])
        self.assertLess(elapsed_ms["dense_window"], 1000)
        self.assertLess((time.monotonic() - started) * 1000, 5000)


class RowValueTests(unittest.TestCase):
    def test_missing_keys_on_sdk_like_rows_return_the_default(self) -> None:
        from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval, _value
        from tests.central_brain.fake_turbopuffer import FakeRow

        row = FakeRow({"id": "psg_" + "1" * 32, "text": "x", "$dist": 1.5})
        self.assertEqual(_value(row, "$dist"), 1.5)
        self.assertIsNone(_value(row, "$score"))
        self.assertEqual(_value(row, "missing", 7), 7)
        # BM25 rows (no $score) rank by $dist without raising.
        self.assertEqual([r["score"] for r in TurbopufferHintRetrieval._scored([row])], [1.5])


class PlaneSwitchTests(unittest.TestCase):
    def test_canonical_retrieval_picks_the_plane_class(self) -> None:
        import inspect

        from recall_server import canonical_retrieval

        source = inspect.getsource(canonical_retrieval)
        self.assertIn('search_plane", "postgres") == "turbopuffer"', source)
        self.assertIn("TurbopufferHintRetrieval", source)

    def test_store_requires_settings_when_the_plane_is_turbopuffer(self) -> None:
        with self.assertRaises(ValueError):
            TurbopufferHintRetrieval(RerankWiringTests._Store(), tenant_id=TENANT, sources=["s"], policy_fingerprint="fp")


if __name__ == "__main__":
    unittest.main()
