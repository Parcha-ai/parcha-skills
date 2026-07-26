from __future__ import annotations

import sys
import time
import unittest
from contextlib import contextmanager
from pathlib import Path


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
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


class EmptyRows:
    @staticmethod
    def fetchall():
        return []


class RecordingStore:
    search_deadline_ms = 25
    semantic_runtime = None

    def __init__(self) -> None:
        self.sql: list[str] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, sql, _values, _deadline_at):
        self.sql.append(" ".join(sql.split()))
        return EmptyRows()


class CanonicalRetrievalDeadlineTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
