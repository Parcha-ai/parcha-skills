from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class Store:
    pool_max_size = 4

    def __init__(self, clock):
        self.clock = clock
        self.error = None
        self.rolled_back = False
        self.closed = False

    def prepare_pool(self, minimum):
        self.clock.advance(1)

    @contextmanager
    def connect(self):
        try:
            yield self
        finally:
            self.closed = True

    @contextmanager
    def transaction(self):
        try:
            yield self
        except BaseException:
            self.rolled_back = True
            raise

    def execute(self, sql, values):
        if self.error is not None:
            raise self.error
        self.clock.advance(5)
        return SimpleNamespace(fetchone=lambda: {"count": 0})


class Projector(CanonicalPassageProjector):
    def __init__(self, store, *, empty=False):
        super().__init__(store, None, policy=PassagePolicy(target_tokens=256, overlap_tokens=32))
        self.empty = empty
        self.calls = []

    def _pending(self, **kwargs):
        self.calls.append("pending")
        self.store.clock.advance(2)
        return () if self.empty else (SimpleNamespace(logical_document_id="PRIVATE"),)

    def _prepare(self, candidate):
        self.calls.append("prepare")
        self.store.clock.advance(3)
        return SimpleNamespace(candidate=candidate)

    def _commit(self, prepared):
        self.calls.append("commit")
        self.store.clock.advance(4)
        return {"status": "complete", "inserted": 0, "deleted": 0, "retained": 0}


class PassagePhaseTimingTest(unittest.TestCase):
    def run_projector(self, projector, **kwargs):
        # Replace only this module's clock reference; real executor clocks stay real.
        with patch("recall_server.passage_index.time", projector.store.clock):
            return projector.project_pending(concurrency=1, **kwargs)

    def test_distinct_phase_delays_accumulate_across_batches(self):
        projector = Projector(Store(Clock()))
        result = self.run_projector(projector, batch_size=1, max_batches=2)
        self.assertEqual({key: result[key] for key in (
            "warmup_ms", "pending_ms", "prepare_ms", "commit_ms", "count_ms"
        )}, {"warmup_ms": 1000, "pending_ms": 4000, "prepare_ms": 6000,
            "commit_ms": 8000, "count_ms": 5000})
        self.assertEqual(result["documents"], 2)
        self.assertEqual(result["elapsed_seconds"], 23.0)  # Existing timer excludes warmup.
        self.assertEqual(projector.calls, ["pending", "prepare", "commit"] * 2)
        self.assertNotIn("PRIVATE", str(result))

    def test_empty_batch_reports_no_prepare_or_commit(self):
        projector = Projector(Store(Clock()), empty=True)
        result = self.run_projector(projector, batch_size=1, max_batches=2)
        self.assertEqual(result["pending_ms"], 2000)
        self.assertEqual(result["prepare_ms"], 0)
        self.assertEqual(result["commit_ms"], 0)
        self.assertEqual(result["count_ms"], 5000)
        self.assertEqual(result["documents"], 0)
        self.assertEqual(projector.calls, ["pending"])

    def test_prepare_exception_identity_survives_without_new_log(self):
        projector = Projector(Store(Clock()))
        error = ValueError("PRIVATE message and payload")
        with patch.object(projector, "_prepare", side_effect=error), self.assertNoLogs():
            with self.assertRaises(ValueError) as raised:
                self.run_projector(projector, batch_size=1, max_batches=1)
        self.assertIs(raised.exception, error)
        self.assertNotIn("commit", projector.calls)

    def test_real_commit_boundary_rolls_back_and_preserves_exception(self):
        store = Store(Clock())
        projector = Projector(store)
        error = RuntimeError("PRIVATE SQL details")
        store.error = error
        # Exercise the real owning transaction boundary, not a mocked rollback.
        with patch.object(projector, "_commit", CanonicalPassageProjector._commit.__get__(projector)):
            with self.assertNoLogs(), self.assertRaises(RuntimeError) as raised:
                self.run_projector(projector, batch_size=1, max_batches=1)
        self.assertIs(raised.exception, error)
        self.assertTrue(store.rolled_back)
        self.assertTrue(store.closed)


if __name__ == "__main__":
    unittest.main()
