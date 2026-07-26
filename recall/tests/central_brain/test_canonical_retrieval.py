from __future__ import annotations

import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import (  # noqa: E402
    BoundCanonicalRetrieval,
    _informative_query_terms,
)
from recall_server.db import SearchDeadlineExceeded  # noqa: E402


class DeadlineStore:
    search_deadline_ms = 25
    semantic_runtime = None

    def __init__(self) -> None:
        self.deadline_at: float | None = None
        self.deadlines: list[float] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, _sql, _values, deadline_at):
        self.deadline_at = deadline_at
        self.deadlines.append(deadline_at)
        raise SearchDeadlineExceeded("synthetic canonical deadline")


class SemanticRuntime:
    fingerprint = "synthetic-runtime"

    @staticmethod
    def embed_query_bounded(_query):
        return [0.0, 1.0]


class RecordingSemanticRuntime:
    fingerprint = "synthetic-runtime"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def embed_query_bounded(self, query):
        self.calls.append(query)
        return [0.0, 1.0]


class EmptyRows:
    @staticmethod
    def fetchall():
        return []


class RecordingStore:
    search_deadline_ms = 25
    semantic_runtime = None

    def __init__(self) -> None:
        self.sql: list[str] = []
        self.values: list[tuple] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, sql, _values, _deadline_at):
        self.sql.append(" ".join(sql.split()))
        self.values.append(tuple(_values))
        return EmptyRows()


class CanonicalRetrievalDeadlineTest(unittest.TestCase):
    def test_uuid_routes_exactly_even_when_the_question_has_other_terms(self) -> None:
        session_id = "8668a658-a6cf-4358-9d7e-c29e5782c1dd"
        self.assertEqual(
            _informative_query_terms(
                f"In session {session_id}, what was verified about ATI?"
            ),
            [session_id],
        )

    def test_query_scaffolding_does_not_dilute_the_domain_concept(self) -> None:
        self.assertEqual(
            _informative_query_terms(
                "Across Codex and Claude coding sessions from July 22 "
                "through July 24, 2026, synthesize ATI harness decisions, "
                "implementation steps, verification evidence, and "
                "unresolved blockers."
            ),
            ["ati", "harness"],
        )

    def test_date_only_filters_are_normalized_to_utc_boundaries(self) -> None:
        self.assertEqual(
            BoundCanonicalRetrieval._filters(
                {"since": "2026-07-23", "until": "2026-07-25"}
            )[3:],
            ("2026-07-23T00:00:00Z", "2026-07-25T00:00:00Z"),
        )

    def test_lexical_deadline_degrades_to_optional_semantic_path(self) -> None:
        store = DeadlineStore()
        started = time.monotonic()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.search("synthetic canonical deadline query")

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(result["diagnostics"]["semantic_status"], "disabled")
        self.assertIsNotNone(store.deadline_at)
        assert store.deadline_at is not None
        self.assertGreaterEqual(store.deadline_at, started)
        self.assertLessEqual(store.deadline_at, started + 0.1)

    def test_semantic_database_query_gets_an_independent_hard_deadline(self) -> None:
        store = DeadlineStore()
        store.semantic_runtime = SemanticRuntime()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.search("synthetic canonical deadline query")

        self.assertEqual(result["results"], [])
        self.assertEqual(result["diagnostics"]["lexical_mode"], "deadline-exceeded")
        self.assertEqual(
            result["diagnostics"]["semantic_status"],
            "deadline-exceeded",
        )
        self.assertEqual(len(store.deadlines), 2)
        self.assertGreaterEqual(store.deadlines[1], store.deadlines[0])

    def test_semantic_search_adds_one_domain_noun_probe(self) -> None:
        store = RecordingStore()
        runtime = RecordingSemanticRuntime()
        store.semantic_runtime = runtime
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        query = (
            "ATI harness decisions implementation verification evidence"
        )

        result = retrieval.search(query)

        self.assertEqual(runtime.calls, [query, "harness"])
        self.assertEqual(result["diagnostics"]["semantic_probes"], 2)

    def test_session_expansion_uses_the_caller_deadline(self) -> None:
        store = DeadlineStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        with self.assertRaises(SearchDeadlineExceeded):
            retrieval.session_context(
                "recall://canonical/test",
                _deadline_at=time.monotonic() + 0.025,
            )
        self.assertIsNotNone(store.deadline_at)

    def test_lexical_search_ranks_bounded_chunks_before_metadata_joins(self) -> None:
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        retrieval.search("ATI harness default runtime")

        self.assertEqual(len(store.sql), 2)
        for sql in store.sql:
            self.assertIn("WITH candidates AS MATERIALIZED", sql)
            self.assertIn("FROM candidates candidate", sql)
            self.assertLess(
                sql.index("LIMIT %s ) SELECT"),
                sql.index("JOIN canonical_documents"),
            )

    def test_strict_lexical_query_treats_uuid_hyphens_as_text(self) -> None:
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        retrieval.search("8668a658-a6cf-4358-9d7e-c29e5782c1dd")

        self.assertIn(
            "ts_rank_cd( chunk.search_vector, "
            "plainto_tsquery('simple',%s)",
            store.sql[0],
        )
        self.assertIn(
            "chunk.search_vector @@ plainto_tsquery('simple',%s)",
            store.sql[0],
        )

    def test_exact_session_deep_route_is_parent_scoped_and_term_ranked(self):
        store = RecordingStore()
        retrieval = BoundCanonicalRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("claude:test",),
        )
        session_id = "8668a658-a6cf-4358-9d7e-c29e5782c1dd"

        receipts = retrieval._exact_session_receipts(
            f"In session {session_id}, verify ATI harness default runtime",
            {
                "investigations": [{
                    "match": {
                        "source_id": "claude:test",
                        "native_parent_id": "claude-session-hash",
                    },
                }],
            },
            {
                "since": "2026-07-23T00:00:00Z",
                "until": "2026-07-25T00:00:00Z",
            },
            limit=60,
        )

        self.assertEqual(receipts, ())
        self.assertIn(
            "COALESCE( event.native_parent_id,event.native_id )=%s",
            store.sql[0],
        )
        self.assertIn("websearch_to_tsquery('simple',%s)", store.sql[0])
        self.assertIn("matched_term_count DESC", store.sql[0])
        self.assertIn("claude-session-hash", store.values[0])


if __name__ == "__main__":
    unittest.main()
