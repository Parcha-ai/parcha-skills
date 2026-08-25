from __future__ import annotations

import unittest
from http.client import RemoteDisconnected

from recall_server.projection_worker import run_projection_worker


class _Logical:
    def __init__(
        self,
        calls: list[str],
        *,
        work: int = 2,
        pending: int = 0,
        pruned: int = 0,
        cleanup_failures: int = 0,
    ):
        self.calls = calls
        self.work = work
        self.pending = pending
        self.pruned = pruned
        self.cleanup_failures = cleanup_failures

    def project_pending(self, **_kwargs):
        self.calls.append("logical")
        return {
            "status": "complete" if self.pending == 0 else "pending",
            "documents": self.work,
            "records": self.work * 3,
            "batches": 1,
            "cleanup_failures": self.cleanup_failures,
            "pruned": self.pruned,
            "pending": self.pending,
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


class _Scan:
    def __init__(
        self,
        calls: list[str],
        *,
        work: int = 1,
        stale: int = 0,
        contended: int = 0,
    ):
        self.calls = calls
        self.work = work
        self.stale = stale
        self.contended = contended

    def project_pending(self, **_kwargs):
        self.calls.append("scan")
        return {
            "status": "complete",
            "shards": self.work,
            "rows": self.work * 10,
            "stale": self.stale,
            "contended": self.contended,
        }


class ProjectionWorkerTest(unittest.TestCase):
    def test_embedding_disconnect_does_not_terminate_the_projection_worker(self):
        calls: list[str] = []

        class DisconnectedPassages(_Passages):
            def embed_pending(self, **_kwargs):
                self.calls.append("embeddings")
                raise RemoteDisconnected("synthetic")

        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            DisconnectedPassages(calls, work=0),  # type: ignore[arg-type]
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

        self.assertEqual(calls, ["embeddings", "passages", "logical"])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["embedding_error"], 1)

    def test_services_downstream_then_coalesces_scan_after_upstream_work(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls),  # type: ignore[arg-type]
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
        self.assertEqual(calls, ["embeddings", "passages", "logical", "scan"])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["documents"], 2)
        self.assertEqual(result["passages"], 0)
        self.assertEqual(result["embedded"], 0)
        self.assertEqual(result["parquet_shards"], 1)

    def test_defers_scan_until_passage_pointer_plane_is_drained(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=2),  # type: ignore[arg-type]
            _Scan(calls, work=4),  # type: ignore[arg-type]
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
        self.assertEqual(calls, ["embeddings", "passages", "logical"])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["passage_documents"], 2)
        self.assertEqual(result["parquet_shards"], 0)

    def test_defers_scan_until_logical_backfill_is_drained(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=100, pending=8_547),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=4),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=100,
            passage_batch_size=100,
            embedding_batch_size=500,
            max_batches_per_cycle=1,
            upload_concurrency=2,
            passage_concurrency=2,
            interval_seconds=5,
            once=True,
        )
        self.assertEqual(calls, ["embeddings", "passages", "logical"])
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["logical_pending"], 8_547)
        self.assertEqual(result["parquet_shards"], 0)

    def test_stale_scan_keeps_the_worker_pending_for_retry(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=0, stale=1),  # type: ignore[arg-type]
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
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["parquet_stale"], 1)

    def test_contended_scan_is_visible_and_not_truthfully_complete(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=0, contended=1),  # type: ignore[arg-type]
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
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["parquet_contended"], 1)

    def test_empty_cycle_is_truthfully_complete(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
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
        self.assertEqual(result["status"], "complete")

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
        self.assertEqual(calls, ["embeddings", "passages", "logical"])

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
        self.assertEqual(calls, ["embeddings", "passages", "logical"])


if __name__ == "__main__":
    unittest.main()
