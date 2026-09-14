"""H5-2/H5-3: the dedicated embedding worker loop, its daily cap, and ledger."""

from __future__ import annotations

import unittest
from http.client import RemoteDisconnected
from unittest import mock

from recall_server import embedding_ledger, projection_worker
from recall_server.projection_worker import run_embedding_worker


class _Runtime:
    passage_fingerprint = "synthetic-runtime"


class _Connection:
    def __init__(self, store: "_Store") -> None:
        self.store = store
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Store:
    """Ledger, lag, and connection fakes; the SQL helpers are patched."""

    def __init__(self, *, ledger: int = 0, lag: int = 0, runtime=True) -> None:
        self.ledger = ledger
        self.lag = lag
        self.semantic_runtime = _Runtime() if runtime else None
        self.recorded: list[int] = []
        self.lag_calls = 0
        self.connections = 0

    def connect(self):
        self.connections += 1
        return _Connection(self)


class _Passages:
    def __init__(self, *, pending: int = 0, status: str | None = None, error=None) -> None:
        self.pending = pending  # passages the fake still has to embed
        self.calls: list[dict] = []
        self.status = status
        self.error = error

    def embed_pending(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        allowed = kwargs["batch_size"] * kwargs["max_batches"]
        if kwargs.get("max_passages") is not None:
            allowed = min(allowed, kwargs["max_passages"])
        processed = min(self.pending, allowed)
        self.pending -= processed
        status = self.status or ("complete" if self.pending == 0 else "pending")
        return {"status": status, "processed": processed, "batches": 1}


def _patched(store: _Store):
    def window_total(connection, *, tenant_id=None):
        return store.ledger

    def record_embedded(connection, *, tenant_id, embedded):
        store.recorded.append(embedded)
        store.ledger += embedded

    def count_unembedded(connection, *, passage_fingerprint, limit, tenant_id=None):
        store.lag_calls += 1
        assert passage_fingerprint == "synthetic-runtime"
        assert tenant_id == "tenant:company:test"
        return min(store.lag, limit)

    return mock.patch.multiple(
        projection_worker,
        window_total=window_total,
        record_embedded=record_embedded,
        count_unembedded_passages=count_unembedded,
    )


def _run(passages, store, **kwargs):
    options = dict(
        tenant_id="tenant:company:test",
        batch_size=100,
        max_batches_per_cycle=2,
        interval_seconds=5,
        daily_cap=1000,
        once=True,
    )
    options.update(kwargs)
    return run_embedding_worker(passages, store, **options)


class EmbeddingWorkerTests(unittest.TestCase):
    def test_cycle_embeds_under_the_cap_and_upserts_the_ledger(self):
        store = _Store(ledger=100)
        passages = _Passages(pending=150)
        with _patched(store), self.assertLogs("recall_server.projection_worker", level="INFO") as logs:
            result = _run(passages, store)

        self.assertEqual(passages.calls[0]["max_passages"], 900)
        self.assertEqual(passages.calls[0]["tenant_id"], "tenant:company:test")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["embedded"], 150)
        self.assertEqual(store.recorded, [150])
        self.assertEqual(result["embedded_24h"], 250)
        self.assertEqual(result["cap_remaining"], 750)
        self.assertEqual(result["lag"], 0)
        self.assertEqual(store.lag_calls, 0)  # a complete drain needs no count
        line = next(m for m in logs.output if "embedding cycle status=" in m)
        for fragment in (
            "status=complete", "embedded=150", "pending=0", "lag=0",
            "cap_remaining=750", "elapsed_ms=",
        ):
            self.assertIn(fragment, line)

    def test_remaining_budget_is_passed_and_the_cap_stops_the_worker(self):
        store = _Store(ledger=950)
        passages = _Passages(pending=500)
        with _patched(store), self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
            result = _run(passages, store)

        self.assertEqual(passages.calls[0]["max_passages"], 50)
        self.assertEqual(result["embedded"], 50)
        self.assertEqual(result["status"], "capped")
        self.assertEqual(result["cap_remaining"], 0)
        self.assertEqual(store.ledger, 1000)
        self.assertTrue(any("embedding cap reached" in m for m in logs.output))

    def test_cap_reached_skips_the_provider_entirely(self):
        store = _Store(ledger=1000, lag=7)
        passages = _Passages(pending=500)
        with _patched(store), self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
            result = _run(passages, store)

        self.assertEqual(passages.calls, [])
        self.assertEqual(result["status"], "capped")
        self.assertEqual(result["embedded"], 0)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["lag"], 7)
        self.assertEqual(store.recorded, [])
        self.assertIn("embedding cap reached", logs.output[0])
        self.assertIn("cap=1000", logs.output[0])

    def test_cap_survives_restarts_because_it_is_read_from_the_ledger(self):
        # A fresh process with an in-memory count of 0 still sees the ledger.
        store = _Store(ledger=1000)
        with _patched(store):
            first = _run(_Passages(pending=5), store)
            second = _run(_Passages(pending=5), store)
        self.assertEqual((first["status"], second["status"]), ("capped", "capped"))

    def test_lag_is_sampled_when_work_remains(self):
        store = _Store(lag=12_345)
        passages = _Passages(pending=250)  # 2 batches x 100 leaves 50
        with _patched(store):
            result = _run(passages, store, lag_sample_limit=10_000)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["embedded"], 200)
        self.assertEqual(result["lag"], 10_000)
        self.assertEqual(store.lag_calls, 1)

    def test_lag_is_minus_one_without_a_runtime(self):
        store = _Store(runtime=False)
        with _patched(store):
            result = _run(_Passages(pending=1, status="busy"), store)
        self.assertEqual(result["lag"], -1)

    def test_provider_disconnect_keeps_the_loop_alive(self):
        store = _Store(lag=3)
        passages = _Passages(pending=10, error=RemoteDisconnected())
        with _patched(store), self.assertLogs("recall_server.projection_worker", level="WARNING") as logs:
            result = _run(passages, store)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["embedding_error"], 1)
        self.assertEqual(result["embedded"], 0)
        self.assertEqual(store.recorded, [])
        self.assertTrue(any("embedding unavailable type=RemoteDisconnected" in m for m in logs.output))

    def test_busy_and_capped_cycles_sleep_and_a_working_cycle_does_not(self):
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)

        store = _Store()
        passages = _Passages(pending=450)  # 200, 200, 50 -> three working cycles
        with _patched(store):
            result = _run(passages, store, once=False, max_cycles=4, sleep=sleep)
        self.assertEqual(result["status"], "complete")
        # Cycles 1-2 left work behind and looped straight on; cycle 3 drained
        # the queue and slept; cycle 4 (the last) returned before sleeping.
        self.assertEqual(sleeps, [5])
        self.assertEqual(store.recorded, [200, 200, 50])

        store = _Store(ledger=1000)
        with _patched(store):
            _run(_Passages(pending=1), store, once=False, max_cycles=2, sleep=sleep)
        self.assertEqual(sleeps, [5, 5])

    def test_a_failed_cycle_does_not_stop_the_service(self):
        store = _Store()
        boom = _Passages(pending=10, error=RuntimeError("synthetic"))
        sleeps: list[float] = []
        with _patched(store), self.assertLogs("recall_server.projection_worker", level="ERROR"):
            with self.assertRaises(RuntimeError):
                _run(boom, store)  # --once surfaces the failure
            with self.assertRaises(RuntimeError):
                # Cycle 1 fails, sleeps, and the loop continues; only the
                # final cycle (max_cycles) re-raises.
                _run(boom, store, once=False, max_cycles=2, sleep=sleeps.append)
        self.assertEqual(sleeps, [5])

    def test_invalid_budgets_are_rejected(self):
        store = _Store()
        for kwargs in (
            {"daily_cap": 0},
            {"daily_cap": True},
            {"batch_size": 0},
            {"max_batches_per_cycle": 101},
            {"interval_seconds": 0},
            {"tenant_id": ""},
            {"lag_sample_limit": 0},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                _run(_Passages(), store, **kwargs)


class LedgerHelperTests(unittest.TestCase):
    def test_daily_cap_from_env(self):
        self.assertEqual(embedding_ledger.daily_cap_from_env({}), 200_000)
        self.assertEqual(embedding_ledger.daily_cap_from_env({"RECALL_EMBEDDING_DAILY_CAP": " 5000 "}), 5000)
        for raw in ("0", "-1", "abc", str(embedding_ledger.MAX_DAILY_CAP + 1)):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                embedding_ledger.daily_cap_from_env({"RECALL_EMBEDDING_DAILY_CAP": raw})

    def test_record_embedded_skips_zero_and_rejects_negative(self):
        class Conn:
            def __init__(self):
                self.calls = []

            def execute(self, sql, params=None):
                self.calls.append((sql, params))

        conn = Conn()
        embedding_ledger.record_embedded(conn, tenant_id="t", embedded=0)
        self.assertEqual(conn.calls, [])
        embedding_ledger.record_embedded(conn, tenant_id="t", embedded=3)
        self.assertEqual(len(conn.calls), 1)
        self.assertIn("ON CONFLICT (tenant_id,day) DO UPDATE", conn.calls[0][0])
        self.assertEqual(conn.calls[0][1], ("t", 3))
        with self.assertRaises(ValueError):
            embedding_ledger.record_embedded(conn, tenant_id="t", embedded=-1)


if __name__ == "__main__":
    unittest.main()
