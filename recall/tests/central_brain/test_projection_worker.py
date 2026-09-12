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

    def project_pending(self, **kwargs):
        self.calls.append("logical")
        self.kwargs = kwargs
        return {
            "status": "complete" if self.pending == 0 else "pending",
            "waiting": kwargs.get("quiet_seconds", 0) and 7 or 0,
            "documents": self.work,
            "repaired": 0,
            "records": self.work * 3,
            "batches": 1,
            "cleanup_failures": self.cleanup_failures,
            "pruned": self.pruned,
            "pending": self.pending,
        }


class _Passages:
    def __init__(
        self,
        calls: list[str],
        *,
        work: int = 2,
        pending: int = 0,
    ):
        self.calls = calls
        self.work = work
        self.pending = pending

    def project_pending(self, **_kwargs):
        self.calls.append("passages")
        return {
            "status": "complete",
            "documents": self.work,
            "passages": self.work * 4,
            "stale": 0,
            "pending": self.pending,
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

    def test_reports_passage_backlog_for_operational_exit_gates(self):
        calls: list[str] = []
        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=0, pending=321),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=1,
            passage_batch_size=2,
            embedding_batch_size=2,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=30,
            once=True,
        )
        self.assertEqual(result["passage_pending"], 321)

    def test_reports_object_only_logical_repairs(self):
        calls: list[str] = []
        logical = _Logical(calls, work=0)

        def repaired(**_kwargs):
            logical.calls.append("logical")
            return {
                "status": "complete",
                "documents": 0,
                "repaired": 17,
                "records": 0,
                "batches": 1,
                "cleanup_failures": 0,
                "pruned": 0,
                "pending": 0,
            }

        logical.project_pending = repaired  # type: ignore[method-assign]
        result = run_projection_worker(
            logical,  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=20,
            passage_batch_size=20,
            embedding_batch_size=20,
            max_batches_per_cycle=1,
            upload_concurrency=2,
            passage_concurrency=2,
            interval_seconds=30,
            once=True,
        )
        self.assertEqual(result["logical_repaired"], 17)

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

    def test_thins_authoritative_rows_after_each_projection_cycle(self):
        calls: list[str] = []

        def thin(busy=False):
            calls.append("thin")
            return {
                "status": "complete",
                "documents": 3,
                "refused": 0,
                "document_bytes_removed": 120,
                "event_bytes_replaced": 240,
            }

        result = run_projection_worker(
            _Logical(calls, work=0),  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=0),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=25,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=5,
            once=True,
            body_thinner=thin,
        )
        self.assertEqual(
            calls,
            ["embeddings", "passages", "logical", "scan", "thin"],
        )
        self.assertEqual(result["canonical_bodies_thinned"], 3)
        self.assertEqual(result["canonical_document_bytes_removed"], 120)
        self.assertEqual(result["canonical_event_bytes_replaced"], 240)
        self.assertEqual(result["status"], "complete")

    def test_unrelated_projection_backlog_does_not_starve_safe_thinning(self):
        calls: list[str] = []

        def thin(busy=False):
            calls.append("thin")
            return {
                "status": "pending",
                "documents": 3,
                "refused": 0,
                "document_bytes_removed": 120,
                "event_bytes_replaced": 240,
            }

        result = run_projection_worker(
            _Logical(calls, work=100, pending=8_547),  # type: ignore[arg-type]
            _Passages(calls, work=2),  # type: ignore[arg-type]
            _Scan(calls, work=4),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=100,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=5,
            once=True,
            body_thinner=thin,
        )

        self.assertEqual(calls, ["embeddings", "passages", "logical", "thin"])
        self.assertEqual(result["canonical_bodies_thinned"], 3)
        self.assertEqual(result["status"], "pending")

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


class DebounceTests(unittest.TestCase):
    def test_worker_passes_quiet_budget_and_reports_waiting_groups(self):
        calls: list[str] = []
        logical = _Logical(calls, work=0)
        result = run_projection_worker(
            logical,  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=1,
            passage_batch_size=2,
            embedding_batch_size=2,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=30,
            once=True,
            quiet_seconds=90,
            max_wait_seconds=600,
        )
        self.assertEqual(logical.kwargs["quiet_seconds"], 90)
        self.assertEqual(logical.kwargs["max_wait_seconds"], 600)
        self.assertEqual(result["logical_waiting"], 7)
        # waiting groups are not pending work: the cycle is still complete
        self.assertEqual(result["status"], "complete")



class _FakeClock:
    """Monotonic clock that only moves when a phase fake advances it."""

    def __init__(self):
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _TimedLogical(_Logical):
    clock: _FakeClock
    seconds = 1.5
    cleanup: dict[str, int] = {}

    def project_pending(self, **kwargs):
        self.clock.advance(self.seconds)
        result = super().project_pending(**kwargs)
        result.update(self.cleanup)
        return result


class _TimedPassages(_Passages):
    clock: _FakeClock
    embed_seconds = 0.2
    project_seconds = 0.3

    def embed_pending(self, **kwargs):
        self.clock.advance(self.embed_seconds)
        return super().embed_pending(**kwargs)

    def project_pending(self, **kwargs):
        self.clock.advance(self.project_seconds)
        return super().project_pending(**kwargs)


class _TimedScan(_Scan):
    clock: _FakeClock
    seconds = 0.05

    def project_pending(self, **kwargs):
        self.clock.advance(self.seconds)
        return super().project_pending(**kwargs)


ELAPSED_KEYS = (
    "cycle_elapsed_ms",
    "embed_elapsed_ms",
    "passage_elapsed_ms",
    "logical_elapsed_ms",
    "parquet_elapsed_ms",
    "thin_elapsed_ms",
)


class CycleTimingTests(unittest.TestCase):
    def _run(
        self,
        clock: _FakeClock,
        *,
        cleanup: dict[str, int] | None = None,
        scan: bool = True,
        thinner: bool = True,
        logical_seconds: float = 1.5,
    ):
        calls: list[str] = []
        logical = _TimedLogical(calls, work=0)
        logical.clock = clock
        logical.seconds = logical_seconds
        logical.cleanup = cleanup or {}
        passages = _TimedPassages(calls, work=0)
        passages.clock = clock
        scanner = _TimedScan(calls, work=0)
        scanner.clock = clock

        def thin(busy=False):
            clock.advance(0.01)
            return {
                "status": "complete",
                "documents": 3,
                "refused": 0,
                "document_bytes_removed": 30,
                "event_bytes_replaced": 3,
            }

        return run_projection_worker(
            logical,  # type: ignore[arg-type]
            passages,  # type: ignore[arg-type]
            scanner if scan else None,  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=25,
            passage_batch_size=100,
            embedding_batch_size=128,
            max_batches_per_cycle=10,
            upload_concurrency=2,
            passage_concurrency=4,
            interval_seconds=5,
            once=True,
            body_thinner=thin if thinner else None,
            clock=clock,
        )

    def test_per_phase_elapsed_uses_the_injected_clock(self):
        result = self._run(_FakeClock())

        self.assertEqual(result["embed_elapsed_ms"], 200)
        self.assertEqual(result["passage_elapsed_ms"], 300)
        self.assertEqual(result["logical_elapsed_ms"], 1500)
        self.assertEqual(result["parquet_elapsed_ms"], 50)
        self.assertEqual(result["thin_elapsed_ms"], 10)
        self.assertEqual(result["cycle_elapsed_ms"], 2060)
        for key in ELAPSED_KEYS:
            self.assertIsInstance(result[key], int)

    def test_deferred_phases_still_report_zero_elapsed(self):
        result = self._run(_FakeClock(), scan=False, thinner=False)

        self.assertEqual(result["parquet_elapsed_ms"], 0)
        self.assertEqual(result["thin_elapsed_ms"], 0)
        self.assertEqual(result["cycle_elapsed_ms"], 2000)

    def test_cycle_log_line_carries_timing_and_cleanup_fields(self):
        cleanup = {
            "old_objects_deleted": 4321,
            "cleanup_completed": 12,
            "cleanup_pending": 987,
        }

        with self.assertLogs("recall_server.projection_worker", level="INFO") as logs:
            result = self._run(_FakeClock(), cleanup=cleanup)

        line = next(m for m in logs.output if "projection cycle status=" in m)
        for fragment in (
            "cycle_elapsed_ms=2060",
            "embed_elapsed_ms=200",
            "passage_elapsed_ms=300",
            "logical_elapsed_ms=1500",
            "parquet_elapsed_ms=50",
            "thin_elapsed_ms=10",
            "old_objects_deleted=4321",
            "logical_cleanup_completed=12",
            "logical_cleanup_pending=987",
        ):
            self.assertIn(fragment, line)
        self.assertEqual(result["old_objects_deleted"], 4321)
        self.assertEqual(result["logical_cleanup_pending"], 987)
        self.assertEqual(result["logical_cleanup_completed"], 12)

    def test_record_cycle_accumulates_phase_totals(self):
        from unittest import mock

        from recall_server import projection_worker

        with mock.patch.dict(
            projection_worker.PROJECTION_TOTALS,
            {key: 0 for key in projection_worker.PROJECTION_TOTALS},
        ):
            first = self._run(_FakeClock())
            second = self._run(_FakeClock(), logical_seconds=0.5)
            totals = projection_worker.projection_totals()

        self.assertEqual(first["cycle_elapsed_ms"], 2060)
        self.assertEqual(second["cycle_elapsed_ms"], 1060)
        self.assertEqual(totals["cycle_elapsed_ms"], 3120)
        self.assertEqual(totals["embed_elapsed_ms"], 400)
        self.assertEqual(totals["passage_elapsed_ms"], 600)
        self.assertEqual(totals["logical_elapsed_ms"], 2000)
        self.assertEqual(totals["parquet_elapsed_ms"], 100)
        self.assertEqual(totals["thin_elapsed_ms"], 20)
        self.assertEqual(totals["bodies_thinned"], 6)


class ThinnerBusyModeTests(unittest.TestCase):
    """Thinning yields to queued freshness work: busy batches while a backlog exists."""

    def _run(self, *, logical_pending: int) -> tuple[list[bool], dict]:
        seen: list[bool] = []

        def thin(busy=False):
            seen.append(busy)
            return {
                "status": "complete",
                "documents": 1,
                "refused": 0,
                "document_bytes_removed": 1,
                "event_bytes_replaced": 1,
            }

        calls: list[str] = []
        logical = _Logical(calls, work=0)
        logical.pending = logical_pending  # type: ignore[attr-defined]
        result = run_projection_worker(
            logical,  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=0),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=5,
            passage_batch_size=5,
            embedding_batch_size=64,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=30,
            once=True,
            body_thinner=thin,
        )
        return seen, result

    def test_thinner_is_busy_while_logical_work_is_queued(self):
        seen, result = self._run(logical_pending=3)
        self.assertEqual(seen, [True])
        self.assertEqual(result["thin_mode"], "busy")

    def test_thinner_is_idle_when_nothing_is_queued(self):
        seen, result = self._run(logical_pending=0)
        self.assertEqual(seen, [False])
        self.assertEqual(result["thin_mode"], "idle")


class ParquetNeverStarvesTests(unittest.TestCase):
    """The scan plane rebuilds at least every N cycles even while logical work is queued."""

    def _run(self, *, logical_pending: int, every: int) -> tuple[list[str], dict, "_Logical"]:
        calls: list[str] = []
        logical = _Logical(calls, work=0, pending=logical_pending)
        result = run_projection_worker(
            logical,  # type: ignore[arg-type]
            _Passages(calls, work=0),  # type: ignore[arg-type]
            _Scan(calls, work=0),  # type: ignore[arg-type]
            tenant_id="tenant:company:test",
            logical_batch_size=5,
            passage_batch_size=5,
            embedding_batch_size=64,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=30,
            once=True,
            parquet_every_cycles=every,
            cleanup_concurrency=8,
        )
        return calls, result, logical

    def test_parquet_runs_when_the_cycle_budget_is_due_despite_backlog(self):
        calls, result, _ = self._run(logical_pending=12, every=1)
        self.assertIn("scan", calls)
        self.assertNotEqual(result["status"], "deferred")

    def test_parquet_is_deferred_before_the_budget_while_backlog_exists(self):
        calls, _, _ = self._run(logical_pending=12, every=2)
        self.assertNotIn("scan", calls)

    def test_parquet_runs_immediately_when_queues_are_drained(self):
        calls, _, _ = self._run(logical_pending=0, every=50)
        self.assertIn("scan", calls)

    def test_cleanup_concurrency_is_passed_to_the_logical_projector(self):
        _, _, logical = self._run(logical_pending=0, every=3)
        self.assertEqual(logical.kwargs["cleanup_concurrency"], 8)

    def test_invalid_budgets_are_rejected(self):
        calls: list[str] = []
        for bad in ({"parquet_every_cycles": 0}, {"cleanup_concurrency": 65}, {"parquet_every_cycles": True}):
            with self.assertRaises(ValueError):
                run_projection_worker(
                    _Logical(calls, work=0),  # type: ignore[arg-type]
                    _Passages(calls, work=0),  # type: ignore[arg-type]
                    _Scan(calls, work=0),  # type: ignore[arg-type]
                    tenant_id="tenant:company:test",
                    logical_batch_size=5,
                    passage_batch_size=5,
                    embedding_batch_size=64,
                    max_batches_per_cycle=1,
                    upload_concurrency=1,
                    passage_concurrency=1,
                    interval_seconds=30,
                    once=True,
                    **bad,
                )
