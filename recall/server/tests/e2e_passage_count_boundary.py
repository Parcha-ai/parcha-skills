"""Exact queue depth is read at worker decisions, not every publication wave."""
import json
import time
import unittest
from unittest.mock import patch

import psycopg

from e2e_passage_parquet_cadence import OrdinaryPassageDuringParquet
from recall_server.projection_worker import run_projection_worker


class PassageCountBoundary(OrdinaryPassageDuringParquet):
    def worker(self, **kwargs):
        return run_projection_worker(
            self.projector, self.passages, tenant_id=self.tenant,
            logical_batch_size=1, passage_batch_size=2, embedding_batch_size=2,
            max_batches_per_cycle=5, upload_concurrency=1, passage_concurrency=2,
            interval_seconds=0.1, once=True, skip_embedding=True, **kwargs,
        )

    def test_multiwave_native_publication_counts_once_at_decision(self):
        for ordinal in range(5):
            self.ingest_parent(f"new-{ordinal}", f"new-{ordinal}/one", "Synthetic queued answer")
        native_execute = psycopg.Connection.execute
        counts = []

        def execute(connection, query, *args, **kwargs):
            is_count = isinstance(query, str) and "SELECT count(*) AS count FROM canonical_passage_projection_queue WHERE" in " ".join(query.split())
            started = time.monotonic()
            result = native_execute(connection, query, *args, **kwargs)
            if is_count:
                counts.append(time.monotonic() - started)
            return result

        thin_busy = []
        with patch.object(psycopg.Connection, "execute", execute):
            result = self.worker(body_thinner=lambda busy: thin_busy.append(busy) or dict(status="complete", documents=0, refused=0, document_bytes_removed=0, event_bytes_replaced=0))
        self.assertEqual(result["documents"], 5)
        self.assertEqual(result["passage_documents"], 5)
        self.assertEqual(result["passage_pending"], 0)
        self.assertEqual(thin_busy, [False])
        with self.store.connect() as connection:
            self.assertEqual(connection.execute(
                "SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s AND reason<>'certification'", (self.tenant,),
            ).fetchone()["n"], 0)
        print(json.dumps({"case": "five_native_waves", "count_queries": len(counts), "count_seconds": sum(counts), "documents": result["passage_documents"], "pending": result["passage_pending"]}), flush=True)
        self.assertEqual(len(counts), 1, "intermediate waves must not repeat the global queue count")

    def test_default_exact_and_deferred_unknown_on_empty_wave(self):
        exact = self.passages.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        self.assertEqual((exact["status"], exact["pending"]), ("complete", 0))
        deferred = self.passages.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, defer_count=True)
        self.assertEqual((deferred["status"], deferred["pending"], deferred["count_ms"]), ("unknown", None, 0))
        result = self.worker()
        self.assertEqual((result["status"], result["passage_pending"]), ("complete", 0))
        for ordinal in range(3):
            self.ingest_parent(f"default-{ordinal}", f"default-{ordinal}/one", "Default exact contract")
        self.projector.project_pending(tenant_id=self.tenant, batch_size=3, max_batches=1, upload_concurrency=1)
        exact = self.passages.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1)
        self.assertEqual((exact["status"], exact["pending"]), ("pending", 2))

    def test_logical_final_enqueue_is_counted_before_compaction_and_thinning(self):
        native_logical = self.projector.project_pending
        native_scan = self.scan.project_pending
        scans, thinning = [], []

        def logical(**kwargs):
            result = native_logical(**kwargs)
            self.ingest_parent("late", "late/one", "Late native logical commit")
            late = native_logical(tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1)
            self.assertEqual(late["documents"], 1)
            return result

        def scan(**kwargs):
            scans.append(kwargs)
            return native_scan(**kwargs)

        with patch.object(self.projector, "project_pending", logical), patch.object(self.scan, "project_pending", scan):
            result = run_projection_worker(
                self.projector, self.passages, self.scan, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=2, embedding_batch_size=2,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=2,
                interval_seconds=0.1, once=True, skip_embedding=True, parquet_every_cycles=1,
                body_thinner=lambda busy: thinning.append(busy) or dict(status="complete", documents=0, refused=0, document_bytes_removed=0, event_bytes_replaced=0),
            )
        self.assertEqual(result["passage_pending"], 1)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(thinning, [True])
        self.assertEqual([s["compaction_budget"] for s in scans], [0])
        self.assertEqual(self.passages.count_pending(tenant_id=self.tenant)["pending"], 1)

    def test_final_scan_callback_refreshes_exact_native_count(self):
        native_execute = psycopg.Connection.execute
        counts = []

        def execute(connection, query, *args, **kwargs):
            if isinstance(query, str) and "SELECT count(*) AS count FROM canonical_passage_projection_queue WHERE" in " ".join(query.split()):
                counts.append(1)
            return native_execute(connection, query, *args, **kwargs)

        with patch.object(psycopg.Connection, "execute", execute):
            result = self._exercise_generation(converge=False)
        self.assertEqual(result["passage_pending"], 0)
        self.assertEqual(len(counts), 2, "one before final scan, one after its passage callbacks")

    def test_cancelled_final_count_propagates_before_thinning_or_success(self):
        native_execute = psycopg.Connection.execute
        thinning = []

        def execute(connection, query, *args, **kwargs):
            if isinstance(query, str) and "SELECT count(*) AS count FROM canonical_passage_projection_queue WHERE" in " ".join(query.split()):
                native_execute(connection, "SET LOCAL statement_timeout = '10ms'")
                native_execute(connection, "SELECT pg_sleep(0.1)")
            return native_execute(connection, query, *args, **kwargs)

        with patch.object(psycopg.Connection, "execute", execute), self.assertLogs("recall_server.projection_worker", level="ERROR") as logs:
            with self.assertRaises(psycopg.errors.QueryCanceled):
                self.worker(body_thinner=lambda busy: thinning.append(busy))
        self.assertEqual(thinning, [])
        self.assertTrue(any("failed_phase=passage" in row for row in logs.output))
        self.assertFalse(any("projection cycle status=" in row for row in logs.output))
        self.assertEqual(self.passages.count_pending(tenant_id=self.tenant)["pending"], 0)


if __name__ == "__main__":
    suite = unittest.TestSuite(PassageCountBoundary(name) for name in PassageCountBoundary.__dict__ if name.startswith("test_"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
