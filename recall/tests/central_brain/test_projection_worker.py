from __future__ import annotations

import unittest

from recall_server.projection_worker import run_projection_worker


class _Logical:
    def __init__(
        self,
        calls: list[str],
        *,
        work: int = 2,
        pruned: int = 0,
        cleanup_failures: int = 0,
    ):
        self.calls = calls
        self.work = work
        self.pruned = pruned
        self.cleanup_failures = cleanup_failures

    def project_pending(self, **_kwargs):
        self.calls.append("logical")
        return {
            "status": "complete",
            "documents": self.work,
            "records": self.work * 3,
            "batches": 1,
            "cleanup_failures": self.cleanup_failures,
            "pruned": self.pruned,
        }


class _Passages:
    def __init__(self, calls: list[str], *, work: int = 2):
        self.calls = calls
        self.work = work

    def project_pending(self, **_kwargs):
        self.calls.append("passages")
        return {
            "status": "complete",
            "documents": self.work,
            "passages": self.work * 4,
            "stale": 0,
        }

    def embed_pending(self, **_kwargs):
        self.calls.append("embeddings")
        return {"status": "complete", "processed": self.work * 4}


class ProjectionWorkerTest(unittest.TestCase):
    def test_runs_the_dependency_chain_in_one_cycle(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls),  # type: ignore[arg-type]
            _Passages(calls),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=25,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=5,
            once=True,
        )
        self.assertEqual(calls, ["logical", "passages", "embeddings"])
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["documents"], 2)
        self.assertEqual(result["passages"], 8)
        self.assertEqual(result["embedded"], 8)

    def test_cleanup_failure_does_not_create_a_hot_loop(self):
        calls: list[str] = []

        class Stopped(RuntimeError):
            pass

        def stop(_seconds: float) -> None:
            raise Stopped

        with self.assertRaises(Stopped):
            run_projection_worker(
                _Logical(calls, work=0, cleanup_failures=1),  # type: ignore[arg-type]
                _Passages(calls, work=0),  # type: ignore[arg-type]
                tenant_id="tenant:company:test",
                logical_batch_size=25,
                passage_batch_size=100,
                embedding_batch_size=128,
                max_batches_per_cycle=10,
                upload_concurrency=2,
                passage_concurrency=4,
                interval_seconds=3,
                sleep=stop,
            )
        self.assertEqual(calls, ["logical", "passages", "embeddings"])

    def test_idle_cycle_sleeps_before_the_next_cycle(self):
        calls: list[str] = []

        class Stopped(RuntimeError):
            pass

        def stop(seconds: float) -> None:
            self.assertEqual(seconds, 3)
            raise Stopped

        with self.assertRaises(Stopped):
            run_projection_worker(
                _Logical(calls, work=0),  # type: ignore[arg-type]
                _Passages(calls, work=0),  # type: ignore[arg-type]
                tenant_id="tenant:company:test",
                logical_batch_size=25,
                passage_batch_size=100,
                embedding_batch_size=128,
                max_batches_per_cycle=10,
                upload_concurrency=2,
                passage_concurrency=4,
                interval_seconds=3,
                sleep=stop,
            )
        self.assertEqual(calls, ["logical", "passages", "embeddings"])


if __name__ == "__main__":
    unittest.main()
