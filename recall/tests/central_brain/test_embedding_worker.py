from __future__ import annotations

import inspect
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
for candidate in (str(ROOT), str(SERVER)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from recall_server.app import Handler  # noqa: E402
from recall_server.canonical_retrieval import CanonicalRetrieval  # noqa: E402
from recall_server.embedding_worker import run_canonical_embedding_worker  # noqa: E402


class FakeRetrieval:
    def __init__(self, results: list[dict[str, int | str]]):
        self.results = list(results)
        self.calls: list[dict[str, int | str | None]] = []

    def embed_pending(
        self,
        *,
        tenant_id: str | None,
        batch_size: int,
        max_batches: int,
    ) -> dict[str, int | str]:
        self.calls.append(
            {
                "tenant_id": tenant_id,
                "batch_size": batch_size,
                "max_batches": max_batches,
            }
        )
        return self.results.pop(0)


class ParallelRetrieval:
    def __init__(self) -> None:
        self.barrier = threading.Barrier(2)
        self.calls: list[str] = []
        self.store = self

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql):
        if "FROM canonical_sources" not in sql:
            raise AssertionError(sql)
        return SimpleNamespace(
            fetchall=lambda: [
                {"tenant_id": "tenant:company:example"},
                {"tenant_id": "tenant:personal:example"},
            ]
        )

    def embed_pending(
        self,
        *,
        tenant_id: str | None,
        batch_size: int,
        max_batches: int,
    ) -> dict[str, int | str]:
        assert tenant_id is not None
        self.calls.append(tenant_id)
        self.barrier.wait(timeout=1)
        return {"status": "complete", "processed": 5000, "batches": 1}


class EmbeddingWorkerTests(TestCase):
    def test_canonical_retrieval_uses_the_bounded_bulk_limit(self) -> None:
        class EmptyResult:
            @staticmethod
            def fetchall() -> list:
                return []

            @staticmethod
            def fetchone() -> dict:
                return {"value": True}

        class Connection:
            def __init__(self) -> None:
                self.batch_limit = None

            def execute(self, sql, values):
                if "FROM canonical_chunk_embeddings" in sql:
                    return SimpleNamespace(fetchone=lambda: None)
                if "FROM canonical_embedding_projection_watermarks" in sql:
                    return SimpleNamespace(
                        fetchone=lambda: {
                            "last_tenant_id": "",
                            "last_source_id": "",
                            "last_chunk_id": "",
                        }
                    )
                if "FROM canonical_chunks" in sql:
                    self.batch_limit = values[-2]
                return EmptyResult()

            @staticmethod
            def commit() -> None:
                pass

        class Store:
            semantic_runtime = SimpleNamespace(
                dimensions=512,
                fingerprint="synthetic-fingerprint",
            )

            def __init__(self) -> None:
                self.connection = Connection()

            @contextmanager
            def connect(self):
                yield self.connection

        store = Store()
        retrieval = CanonicalRetrieval(store)  # type: ignore[arg-type]

        result = retrieval.embed_pending(batch_size=5000, max_batches=1)

        self.assertEqual(result, {"status": "complete", "processed": 0, "batches": 0})
        self.assertEqual(store.connection.batch_limit, 5000)
        with self.assertRaisesRegex(ValueError, "invalid canonical embedding batch"):
            retrieval.embed_pending(batch_size=5001, max_batches=1)

    def test_canonical_retrieval_advances_over_an_ineligible_key_window(
        self,
    ) -> None:
        class EmptyResult:
            @staticmethod
            def fetchall() -> list:
                return []

            @staticmethod
            def fetchone() -> dict:
                return {"value": True}

        class Connection:
            def __init__(self) -> None:
                self.scan_sql = ""
                self.watermark_update = None

            def execute(self, sql, values):
                if "FROM canonical_chunk_embeddings" in sql:
                    return SimpleNamespace(fetchone=lambda: None)
                if "FROM canonical_embedding_projection_watermarks" in sql:
                    return SimpleNamespace(
                        fetchone=lambda: {
                            "last_tenant_id": "",
                            "last_source_id": "",
                            "last_chunk_id": "",
                        }
                    )
                if "WITH scan_window AS MATERIALIZED" in sql:
                    self.scan_sql = sql
                    return SimpleNamespace(
                        fetchall=lambda: [
                            {
                                "tenant_id": "tenant:company:example",
                                "source_id": "source:one",
                                "chunk_id": "chunk:one",
                                "text_redacted": "already embedded",
                                "text_sha256": "content-sha",
                                "eligible": False,
                            }
                        ]
                    )
                if (
                    "UPDATE canonical_embedding_projection_watermarks" in sql
                    and values[0] == "tenant:company:example"
                ):
                    self.watermark_update = values
                return EmptyResult()

            @staticmethod
            def commit() -> None:
                pass

            @staticmethod
            @contextmanager
            def transaction():
                yield

        class Store:
            semantic_runtime = SimpleNamespace(
                dimensions=512,
                fingerprint="synthetic-fingerprint",
            )

            def __init__(self) -> None:
                self.connection = Connection()

            @contextmanager
            def connect(self):
                yield self.connection

        store = Store()
        retrieval = CanonicalRetrieval(store)  # type: ignore[arg-type]

        result = retrieval.embed_pending(batch_size=2000, max_batches=1)

        self.assertEqual(result, {"status": "complete", "processed": 0, "batches": 1})
        self.assertIn("WITH scan_window AS MATERIALIZED", store.connection.scan_sql)
        self.assertEqual(
            store.connection.watermark_update[:3],
            ("tenant:company:example", "source:one", "chunk:one"),
        )

    def test_canonical_ingest_does_not_call_the_embedding_provider(self) -> None:
        source = inspect.getsource(Handler.do_POST)
        canonical_route = source[source.index('if path == "/v2/ingest/canonical":') :]
        canonical_route = canonical_route[
            : canonical_route.index("if path == WEBHOOK_PATH:")
        ]

        self.assertIn("self.canonical_plane.ingest_batch(", canonical_route)
        self.assertNotIn("embed_pending(", canonical_route)

    def test_once_runs_one_bounded_restart_safe_cycle(self) -> None:
        retrieval = FakeRetrieval(
            [{"status": "complete", "processed": 5000, "batches": 1}]
        )

        result = run_canonical_embedding_worker(
            retrieval,  # type: ignore[arg-type]
            tenant_id="tenant:company:example",
            batch_size=5000,
            max_batches_per_cycle=10,
            interval_seconds=5,
            once=True,
        )

        self.assertEqual(result["processed"], 5000)
        self.assertEqual(
            retrieval.calls,
            [
                {
                    "tenant_id": "tenant:company:example",
                    "batch_size": 5000,
                    "max_batches": 10,
                }
            ],
        )

    def test_worker_sleeps_only_when_the_queue_is_empty(self) -> None:
        retrieval = FakeRetrieval(
            [
                {"status": "complete", "processed": 2, "batches": 1},
                {"status": "complete", "processed": 0, "batches": 0},
            ]
        )

        class StopAfterIdle(Exception):
            pass

        sleeps: list[float] = []

        def stop(seconds: float) -> None:
            sleeps.append(seconds)
            raise StopAfterIdle

        with self.assertRaises(StopAfterIdle):
            run_canonical_embedding_worker(
                retrieval,  # type: ignore[arg-type]
                tenant_id=None,
                batch_size=64,
                max_batches_per_cycle=2,
                interval_seconds=3,
                sleep=stop,
            )

        self.assertEqual(len(retrieval.calls), 2)
        self.assertEqual(sleeps, [3])

    def test_worker_drains_tenants_in_parallel_and_aggregates_results(self) -> None:
        retrieval = ParallelRetrieval()

        result = run_canonical_embedding_worker(
            retrieval,  # type: ignore[arg-type]
            tenant_id=None,
            parallel_tenants=2,
            batch_size=5000,
            max_batches_per_cycle=10,
            interval_seconds=5,
            once=True,
        )

        self.assertEqual(result["processed"], 10_000)
        self.assertEqual(result["batches"], 2)
        self.assertEqual(
            sorted(retrieval.calls),
            ["tenant:company:example", "tenant:personal:example"],
        )

    def test_worker_rejects_unbounded_configuration(self) -> None:
        retrieval = FakeRetrieval([])
        with self.assertRaisesRegex(ValueError, "batch size"):
            run_canonical_embedding_worker(
                retrieval,  # type: ignore[arg-type]
                tenant_id=None,
                batch_size=5001,
                max_batches_per_cycle=1,
                interval_seconds=1,
                once=True,
            )
        with self.assertRaisesRegex(ValueError, "parallel tenants"):
            run_canonical_embedding_worker(
                retrieval,  # type: ignore[arg-type]
                tenant_id=None,
                parallel_tenants=9,
                batch_size=5000,
                max_batches_per_cycle=1,
                interval_seconds=1,
                once=True,
            )
        with self.assertRaisesRegex(ValueError, "combine one tenant"):
            run_canonical_embedding_worker(
                retrieval,  # type: ignore[arg-type]
                tenant_id="tenant:company:example",
                parallel_tenants=2,
                batch_size=5000,
                max_batches_per_cycle=1,
                interval_seconds=1,
                once=True,
            )
