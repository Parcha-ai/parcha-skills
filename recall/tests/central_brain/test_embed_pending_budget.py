"""H5-3: ``CanonicalPassageProjector.embed_pending`` never exceeds ``max_passages``."""

from __future__ import annotations

import unittest

from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy


class _Runtime:
    dimensions = 512
    model = "synthetic-model"
    passage_fingerprint = "synthetic-runtime"
    # These tests exercise the budget plumbing only; pin the v1 contract so
    # the H2-a header backfill (which needs catalog rows) stays out of the way.
    passage_write_contract = "v1"
    passage_write_fingerprint = "synthetic-runtime"

    def __init__(self) -> None:
        self.calls: list[int] = []

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(len(texts))
        return [[0.0] * 512 for _ in texts]


class _Cursor:
    def __init__(self, sink: list) -> None:
        self.sink = sink

    def executemany(self, sql, rows) -> None:
        self.sink.append(list(rows))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Result:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value

    def fetchall(self):
        return self.value


class _Connection:
    """Serves the unembedded queue from a list, honouring the LIMIT parameter."""

    def __init__(self, queue: list[dict]) -> None:
        self.queue = queue
        self.inserted: list[list] = []
        self.limits: list[int] = []

    def execute(self, sql, params=None):
        if "pg_try_advisory_lock" in sql:
            return _Result({"value": True})
        if "pg_advisory_unlock" in sql:
            return _Result(None)
        if "SELECT EXISTS" in sql:
            return _Result({"value": bool(self.queue)})
        if "LIMIT %s" in sql:
            limit = params[-1]
            self.limits.append(limit)
            return _Result(self.queue[:limit])
        raise AssertionError(f"unexpected sql: {sql[:60]}")

    def commit(self) -> None:
        pass

    def transaction(self):
        return self

    def cursor(self):
        return _Cursor(self._sink)

    @property
    def _sink(self):
        class Sink(list):
            connection = self

            def append(inner, rows):
                super().append(rows)
                del self.queue[: len(rows)]
                self.inserted.append(rows)

        return Sink()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Store:
    def __init__(self, queue: list[dict]) -> None:
        self.semantic_runtime = _Runtime()
        self.connection = _Connection(queue)

    def connect(self):
        return self.connection


def _queue(n: int) -> list[dict]:
    return [
        {
            "tenant_id": "tenant:company:test",
            "source_id": "src",
            "passage_id": f"p{i}",
            "text_redacted": f"text {i}",
            "text_sha256": "0" * 64,
        }
        for i in range(n)
    ]


class EmbedPendingBudgetTests(unittest.TestCase):
    def _projector(self, store: _Store) -> CanonicalPassageProjector:
        return CanonicalPassageProjector(
            store,
            None,  # type: ignore[arg-type]
            policy=PassagePolicy(target_tokens=4, overlap_tokens=1),
            bound_tenant_id="tenant:company:test",
        )

    def test_without_a_budget_behaviour_is_unchanged(self):
        store = _Store(_queue(25))
        result = self._projector(store).embed_pending(batch_size=10, max_batches=10)
        self.assertEqual(result, {"status": "complete", "processed": 25, "batches": 3, "contract": "v1", "headers_backfilled": 0})
        self.assertEqual(store.semantic_runtime.calls, [10, 10, 5])
        self.assertEqual(store.connection.limits, [10, 10, 10, 10])  # last fetch is empty

    def test_budget_shrinks_the_last_batch_and_stops(self):
        store = _Store(_queue(25))
        result = self._projector(store).embed_pending(
            batch_size=10, max_batches=10, max_passages=13
        )
        self.assertEqual(result["processed"], 13)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(store.semantic_runtime.calls, [10, 3])
        self.assertEqual(store.connection.limits, [10, 3])
        self.assertEqual(sum(len(rows) for rows in store.connection.inserted), 13)

    def test_budget_below_batch_size_limits_the_first_fetch(self):
        store = _Store(_queue(25))
        result = self._projector(store).embed_pending(
            batch_size=10, max_batches=10, max_passages=4
        )
        self.assertEqual(result["processed"], 4)
        self.assertEqual(store.connection.limits, [4])

    def test_budget_larger_than_the_queue_drains_it(self):
        store = _Store(_queue(7))
        result = self._projector(store).embed_pending(
            batch_size=10, max_batches=10, max_passages=1_000
        )
        self.assertEqual(result, {"status": "complete", "processed": 7, "batches": 1, "contract": "v1", "headers_backfilled": 0})

    def test_invalid_budget_is_rejected_before_any_lock(self):
        store = _Store(_queue(1))
        for value in (0, -1, True, "5"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self._projector(store).embed_pending(max_passages=value)  # type: ignore[arg-type]
        self.assertEqual(store.semantic_runtime.calls, [])


if __name__ == "__main__":
    unittest.main()
