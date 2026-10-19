"""Eligible ordinary passages must not wait behind an unrelated Parquet build."""

import threading
import queue
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from e2e_parquet_worker_fairness import ParquetFairness
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.parquet_scan import PARQUET_MEDIA_TYPE
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projection_worker import run_projection_worker, record_cycle
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class OrdinaryPassageDuringParquet(ParquetFairness):
    def _exercise_generation(self, label="first", *, converge=True):
        # Three eligible tiny updates exceed one actual two-owner passage wave.
        for parent in ("alpha", "beta", "gamma"):
            self.ingest_parent(
                parent, parent + "/one", "Synthetic updated answer " + parent + label
            )
        logical = self.projector.project_pending(
            tenant_id=self.tenant, batch_size=20, max_batches=1, upload_concurrency=1
        )
        self.assertEqual(logical["failed"], 0)
        self.passages = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        )
        blocked, release, published = (threading.Event() for _ in range(3))
        target = {}
        heartbeat_calls = []
        waves = []
        native_passages = self.passages.project_pending

        def passages(**kwargs):
            waves.append((threading.get_ident(), kwargs))
            if len(waves) > 1:
                self.assertTrue(
                    blocked.wait(5), "scan must reach the native archive barrier"
                )
            return native_passages(**kwargs)

        native_verify = self.archive.verify_raw_bounded
        client = FakeTurbopuffer()
        writer = TurbopufferProjector(
            self.store,
            TurbopufferSettings(api_key="synthetic-local-only"),
            client=client,
            batch_rows=20,
            write_concurrency=1,
        )

        def verify(reference, *, deadline_at):
            if reference["media_type"] == PARQUET_MEDIA_TYPE and not release.is_set():
                with self.store.connect() as c:
                    rows = c.execute(
                        """SELECT q.logical_document_id,q.revision,
                        e.native_parent_id,e.record_count,e.part_count,
                        p.revision AS projected_revision
                        FROM canonical_passage_projection_queue q
                        JOIN canonical_evidence_documents e USING(tenant_id,source_id,logical_document_id)
                        JOIN canonical_passage_documents p USING(tenant_id,source_id,logical_document_id)
                        WHERE q.tenant_id=%s AND q.reason<>'certification' """,
                        (self.tenant,),
                    ).fetchall()
                    self.assertEqual(
                        len(rows), 1, "fixture must leave one eligible ordinary update"
                    )
                    row = rows[0]
                    self.assertEqual(row["record_count"], 1)
                    self.assertEqual(row["part_count"], 1)
                    self.assertGreater(row["revision"], row["projected_revision"])
                    event = c.execute(
                        """SELECT e.native_id,e.revision FROM canonical_events e
                        JOIN canonical_documents d USING(tenant_id,source_id,event_id)
                        WHERE e.tenant_id=%s AND e.source_id=%s AND e.native_parent_id=%s
                        AND d.is_current AND d.deleted_at IS NULL""",
                        (self.tenant, self.source, row["native_parent_id"]),
                    ).fetchone()
                    target["receipt"] = (
                        f"recall://{self.source}/{event['native_id']}?rev={event['revision']}#item=0"
                    )
                blocked.set()
                if not release.wait(12):
                    raise AssertionError("fixture_archive_not_released")
            return native_verify(reference, deadline_at=time.monotonic() + 1)

        def publish():
            result = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            if blocked.is_set():
                heartbeat_calls.append(threading.get_ident())
                if target and any(
                    target["receipt"] in row.get("receipts", ())
                    for namespace in client.namespaces.values()
                    for row in namespace.rows.values()
                ):
                    published.set()
            return result

        def worker():
            return run_projection_worker(
                self.projector,
                self.passages,
                self.scan,
                tenant_id=self.tenant,
                logical_batch_size=4,
                passage_batch_size=100,
                embedding_batch_size=4,
                max_batches_per_cycle=1,
                upload_concurrency=1,
                passage_concurrency=2,
                interval_seconds=0.1,
                once=True,
                skip_embedding=True,
                parquet_every_cycles=1,
                search_plane=publish,
            )

        with (
            patch.object(self.passages, "project_pending", passages),
            patch.object(self.archive, "verify_raw_bounded", verify),
            patch(
                "recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS",
                0.02,
            ),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(worker)
            try:
                self.assertTrue(blocked.wait(5), "native Parquet archive did not pause")
                advanced = published.wait(3)
                self.assertGreaterEqual(
                    len(heartbeat_calls), 2, "native search heartbeat must keep running"
                )
                self.assertFalse(future.done())
                self.assertTrue(
                    advanced, "eligible passage publication waited behind Parquet"
                )
            finally:
                release.set()
            result = future.result(timeout=10)
        self.assertEqual(result["passage_documents"], 3)
        self.assertEqual(len({thread for thread, _ in waves}), 1)
        for _, bounds in waves:
            self.assertEqual(bounds["batch_size"], 2)
            self.assertEqual(bounds["max_batches"], 1)
            self.assertEqual(bounds["concurrency"], 2)
        self.assertEqual(result["parquet_stale"], 1)
        self.assertEqual(result["parquet_shards"], 0)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s AND reason<>'certification'",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )
            self.assertGreater(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s AND media_type=%s",
                    (self.tenant, PARQUET_MEDIA_TYPE),
                ).fetchone()["n"],
                0,
            )
        if not converge:
            return result
        converged = self.scan.project_pending(
            tenant_id=self.tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        self.assertEqual(converged["stale"], 0)
        self.assertEqual(converged["failed"], 0)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )
        self.assertGreater(converged["shards"], 0)

    def test_eligible_small_passage_publishes_before_held_parquet_finishes(self):
        self._exercise_generation()

    def test_continuing_arrivals_allow_quiet_scan_between_publication_waves(self):
        ready = queue.Queue()
        pause_lock = threading.Lock()
        claimed = set()
        released = [threading.Event() for _ in range(4)]
        paused = [threading.Event() for _ in range(4)]
        observed = [threading.Event() for _ in range(4)]
        targets, scans, wave_threads, committed = {}, [], [], []
        scan_number = [-1]
        native_scan = self.scan.project_pending
        native_verify = self.archive.verify_raw_bounded
        self.passages = CanonicalPassageProjector(
            self.store,
            LogicalEvidenceProjectionStore(self.archive),
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        )
        native_passages = self.passages.project_pending
        client = FakeTurbopuffer()
        writer = TurbopufferProjector(
            self.store,
            TurbopufferSettings(api_key="synthetic-local-only"),
            client=client,
            batch_rows=20,
            write_concurrency=1,
        )

        def arrivals(label):
            for parent in ("alpha", "beta", "gamma"):
                self.ingest_parent(
                    parent, parent + "/one", "Continuous answer " + label + parent
                )
            tick = self.projector.project_pending(
                tenant_id=self.tenant,
                batch_size=20,
                max_batches=1,
                upload_concurrency=1,
            )
            self.assertEqual(tick["failed"], 0)
            self.assertEqual(tick["documents"], 3, repr(tick))

        arrivals("initial")
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s AND reason<>'certification'",
                    (self.tenant,),
                ).fetchone()["n"],
                3,
            )

        def scan(**kwargs):
            scan_number[0] += 1
            result = native_scan(**kwargs)
            scans.append(result)
            return result

        def passages(**kwargs):
            wave_threads.append(threading.get_ident())
            n = scan_number[0]
            if n >= 0 and not released[n].is_set():
                self.assertTrue(paused[n].wait(5))
            result = native_passages(**kwargs)
            committed.append(result["documents"])
            return result

        def verify(reference, *, deadline_at):
            n = scan_number[0]
            with pause_lock:
                should_pause = (
                    reference["media_type"] == PARQUET_MEDIA_TYPE and n not in claimed
                )
                if should_pause:
                    claimed.add(n)
            if should_pause:
                with self.store.connect() as c:
                    row = c.execute(
                        """SELECT e.native_id,e.revision
                        FROM canonical_passage_projection_queue q
                        JOIN canonical_evidence_documents l USING(tenant_id,source_id,logical_document_id)
                        JOIN canonical_events e ON e.tenant_id=l.tenant_id AND e.source_id=l.source_id
                            AND e.native_parent_id=l.native_parent_id
                        JOIN canonical_documents d ON d.tenant_id=e.tenant_id AND d.source_id=e.source_id
                            AND d.event_id=e.event_id AND d.is_current AND d.deleted_at IS NULL
                        WHERE q.tenant_id=%s AND q.reason<>'certification'
                        ORDER BY e.native_id LIMIT 1""",
                        (self.tenant,),
                    ).fetchone()
                self.assertIsNotNone(
                    row, f"scan={n}; commits={committed}; scans={scans}"
                )
                targets[n] = (
                    f"recall://{self.source}/{row['native_id']}?rev={row['revision']}#item=0"
                )
                paused[n].set()
                ready.put(n)
                self.assertTrue(released[n].wait(12))
            return native_verify(reference, deadline_at=time.monotonic() + 1)

        def search():
            result = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            for n, receipt in tuple(targets.items()):
                if any(
                    receipt in row.get("receipts", ())
                    for namespace in client.namespaces.values()
                    for row in namespace.rows.values()
                ):
                    observed[n].set()
            return result

        def recorded(result):
            record_cycle(result)
            if len(scans) < 4:
                arrivals(str(len(scans)))

        def worker():
            return run_projection_worker(
                self.projector,
                self.passages,
                self.scan,
                tenant_id=self.tenant,
                logical_batch_size=4,
                passage_batch_size=100,
                embedding_batch_size=4,
                max_batches_per_cycle=1,
                upload_concurrency=1,
                passage_concurrency=2,
                interval_seconds=0.1,
                once=False,
                max_cycles=4,
                skip_embedding=True,
                parquet_every_cycles=1,
                search_plane=search,
                sleep=lambda _: None,
            )

        with (
            patch("recall_server.projection_worker.record_cycle", recorded),
            patch.object(self.scan, "project_pending", scan),
            patch.object(self.archive, "verify_raw_bounded", verify),
            patch.object(self.passages, "project_pending", passages),
            patch(
                "recall_server.projection_worker.PROJECTION_PROGRESS_INTERVAL_SECONDS",
                0.02,
            ),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(worker)
            try:
                for expected in range(4):
                    self.assertEqual(ready.get(timeout=7), expected)
                    if expected % 2 == 0:
                        self.assertTrue(observed[expected].wait(3))
                    else:
                        self.assertFalse(
                            observed[expected].wait(0.15),
                            "quiet scan admitted a passage wave",
                        )
                    self.assertFalse(future.done())
                    released[expected].set()
            finally:
                for event in released:
                    event.set()
            result = future.result(timeout=10)
        self.assertEqual([scan["stale"] for scan in scans], [1, 0, 1, 0])
        self.assertTrue(all(scans[n]["shards"] > 0 for n in (1, 3)))
        self.assertEqual(sum(scan["stale"] for scan in scans), 2)
        self.assertEqual(result["passage_documents"], 2)
        self.assertEqual(sum(committed), 10)  # 3, 2, 3, 2 current revisions; newer arrivals supersede queued revisions.
        self.assertEqual(len(set(wave_threads)), 1)


if __name__ == "__main__":
    suite = unittest.TestSuite(
        OrdinaryPassageDuringParquet(name)
        for name in OrdinaryPassageDuringParquet.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
