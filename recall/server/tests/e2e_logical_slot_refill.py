#!/usr/bin/env python3
"""Fresh arrivals enter active logical slots without reducing the finite work budget."""

# ruff: noqa: E402
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from e2e_logical_admission_sharing import AdmissionSharing
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projection_worker import run_projection_worker
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class Admission(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ["RECALL_DATABASE_URL"], pool_max_size=8)
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def exercise(self, batch):
        tenant = "tenant:admission:" + uuid.uuid4().hex
        old, fresh = "codex:backlog", "codex:fresh"
        initial_ready, arrival, later_blocked, release, published = (
            threading.Event() for _ in range(5)
        )
        lock = threading.Lock()
        started = 0
        pending = []
        callbacks = 0

        class Logical(CanonicalLogicalEvidenceProjector):
            def _pending(self, **kwargs):
                rows = super()._pending(**kwargs)
                pending.append(
                    dict(
                        limit=kwargs["limit"],
                        prefer_recent=kwargs["prefer_recent"],
                        admitted_sources=[c.source_id for c in rows],
                        at=time.monotonic(),
                    )
                )
                return rows

            def _prepare_batch_and_upload(self, candidates, **kwargs):
                nonlocal started
                uploads = super()._prepare_batch_and_upload(candidates, **kwargs)
                if candidates[0].source_id == old:
                    with lock:
                        started += 1
                        ordinal = started
                        if started == 2:
                            initial_ready.set()
                    gate = arrival if ordinal <= 2 else release
                    if ordinal > 2:
                        later_blocked.set()
                    if not gate.wait(15):
                        self._schedule_upload_cleanup(
                            [u for u in uploads if u is not None]
                        )
                        raise AssertionError("fixture_upload_not_released")
                return uploads

        with tempfile.TemporaryDirectory() as t:
            archive = FilesystemArchiveStore(Path(t), namespace_key=b"p" * 32)
            projection = LogicalEvidenceProjectionStore(archive)
            logical = Logical(
                self.store, projection, bound_tenant_id=tenant, raw_archive=archive
            )
            passages = CanonicalPassageProjector(
                self.store,
                projection,
                policy=DEFAULT_PASSAGE_POLICY,
                bound_tenant_id=tenant,
            )
            with self.store.connect() as c:
                for source in (old, fresh):
                    insert_source(c, tenant, "principal:test", source)
                for i in range(6):
                    name = f"old-{i}"
                    insert_record(
                        c,
                        tenant=tenant,
                        source=old,
                        parent=name,
                        native=name,
                        text=f"Synthetic old searchable evidence {i}.",
                        role="user",
                        byte_start=0,
                    )
                    mark_logical_evidence_dirty(
                        c,
                        tenant_id=tenant,
                        source_id=old,
                        native_ids=[name],
                        reason="ingest",
                    )
            client = FakeTurbopuffer()
            settings = TurbopufferSettings(api_key="synthetic-local-only")
            writer = TurbopufferProjector(self.store, settings, client=client)
            retrieval = TurbopufferHintRetrieval(
                self.store,
                tenant_id=tenant,
                sources=[old, fresh],
                policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
                settings=settings,
                client=client,
            )

            def search():
                return retrieval.search(
                    "searchable evidence",
                    lexical_query="searchable evidence",
                    since=None,
                    until=None,
                    limit=20,
                )["results"]

            def publish():
                nonlocal callbacks
                callbacks += 1
                tick = writer.drain(tenant_id=tenant, max_months=4)
                if any(row["source_id"] == fresh for row in search()):
                    published.set()
                return tick

            def worker(**kw):
                return run_projection_worker(
                    logical,
                    passages,
                    tenant_id=tenant,
                    logical_batch_size=batch,
                    passage_batch_size=100,
                    embedding_batch_size=64,
                    max_batches_per_cycle=2,
                    upload_concurrency=2,
                    passage_concurrency=2,
                    interval_seconds=1,
                    skip_embedding=True,
                    quiet_seconds=0,
                    max_wait_seconds=0,
                    search_plane=publish,
                    **kw,
                )

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(worker, once=True)
                try:
                    self.assertTrue(
                        initial_ready.wait(5), "initial actual uploads did not arrive"
                    )
                    with self.store.connect() as c:
                        insert_record(
                            c,
                            tenant=tenant,
                            source=fresh,
                            parent="fresh",
                            native="fresh",
                            text="Synthetic fresh searchable evidence.",
                            role="user",
                            byte_start=0,
                        )
                        mark_logical_evidence_dirty(
                            c,
                            tenant_id=tenant,
                            source_id=fresh,
                            native_ids=["fresh"],
                            reason="ingest",
                        )
                    ack_at = time.monotonic()
                    arrival.set()
                    self.assertTrue(
                        later_blocked.wait(5), "later old uploads did not block"
                    )
                    observed = published.wait(2)
                    self.assertTrue(
                        observed,
                        repr(
                            dict(
                                message="fresh arrival waited behind already queued old parents",
                                pending=pending,
                                callbacks=callbacks,
                                logical_module=sys.modules[
                                    CanonicalLogicalEvidenceProjector.__module__
                                ].__file__,
                            )
                        ),
                    )
                    self.assertTrue(
                        any(fresh in row["admitted_sources"] for row in pending[1:])
                    )
                    before_release = dict(
                        fresh_published=observed,
                        pending_queries=len(pending),
                        callbacks=callbacks,
                        age_seconds=time.monotonic() - ack_at,
                    )
                finally:
                    arrival.set()
                    release.set()
                first = future.result(timeout=10)
            sleeps = []
            worker(max_cycles=6, sleep=sleeps.append)
            found = search()
            self.assertEqual(
                len(found),
                7,
                repr(
                    dict(
                        first=first,
                        final_keys=[
                            (r.get("source_id"), r.get("logical_document_id"))
                            for r in found
                        ],
                        pending=pending,
                    )
                ),
            )
            self.assertEqual(sum(row["source_id"] == fresh for row in found), 1)
            with self.store.connect() as c:
                for table in (
                    "canonical_evidence_document_queue",
                    "canonical_passage_projection_queue",
                ):
                    self.assertEqual(
                        c.execute(
                            f"SELECT count(*)AS n FROM {table} WHERE tenant_id=%s",
                            (tenant,),
                        ).fetchone()["n"],
                        0,
                    )
            return dict(
                batch_size=batch,
                before_block_release=before_release,
                final_public_documents=7,
                pending_queries=len(pending),
                callbacks=callbacks,
                busy_and_idle_sleep_calls=len(sleeps),
                first_cycle=first,
            )

    def test_fresh_arrival_enters_before_later_old_uploads_finish(self):
        result = self.exercise(500)
        self.assertEqual(result["first_cycle"]["documents"], 7)


class SlotBounds(AdmissionSharing):
    def test_exact_total_budget_and_nonfull_batch_cleanup(self):
        for batch, maximum, expected in ((1, 1, 1), (2, 2, 4), (500, 2, 7)):
            with self.subTest(batch=batch):
                # Separate tenant for each finite campaign.
                self.tenant = "tenant:budget:" + uuid.uuid4().hex
                self.sources = set()
                self.logical = self.projector()
                for index in range(7):
                    self.add(f"old-{index}", age=100 + index)
                cleanups = []
                original = self.logical.drain_cleanup

                def cleanup(**kwargs):
                    cleanups.append(True)
                    return original(**kwargs)

                self.logical.drain_cleanup = cleanup
                result = self.logical.project_pending(
                    batch_size=batch,
                    max_batches=maximum,
                    upload_concurrency=min(2, batch),
                )
                self.assertEqual(result["documents"], expected)
                self.assertEqual(result["batches"], 1 if batch == 500 else maximum)
                self.assertEqual(len(cleanups), 1 + result["batches"])
                self.assertEqual(result["pending"], 7 - expected)

    def test_hot_source_does_not_take_third_source_slot(self):
        self.add("slow-a", age=90000, source="source:a")
        self.add("first-b", age=80000, source="source:b")
        initial = threading.Event()
        arrival = threading.Event()
        release = threading.Event()
        third = threading.Event()
        started = []
        lock = threading.Lock()
        original = self.logical._prepare_batch_and_upload

        def prepare(candidates, **kwargs):
            uploads = original(candidates, **kwargs)
            candidate = candidates[0]
            with lock:
                started.append(candidate.native_parent_id)
                if len(started) == 2:
                    initial.set()
            if candidate.native_parent_id == "third-c":
                third.set()
            gate = arrival if candidate.native_parent_id == "first-b" else release
            if candidate.native_parent_id != "third-c" and not gate.wait(10):
                raise AssertionError("fixture_gate_unreleased")
            return uploads

        self.logical._prepare_batch_and_upload = prepare
        hot_arrivals = []

        def add_hot_arrival():
            if len(hot_arrivals) < 3:
                name = f"repeated-hot-{len(hot_arrivals)}"
                hot_arrivals.append(name)
                self.add(name, age=60000, source="source:b")

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                self.logical.project_pending,
                batch_size=500,
                max_batches=2,
                upload_concurrency=2,
                on_progress=add_hot_arrival,
            )
            try:
                self.assertTrue(initial.wait(5))
                self.add("hot-b", age=70000, source="source:b")
                self.add("third-c", age=100, source="source:c")
                arrival.set()
                self.assertTrue(
                    third.wait(1),
                    "hot source took the free slot ahead of a third source",
                )
            finally:
                arrival.set()
                release.set()
            self.assertEqual(future.result(timeout=10)["documents"], 7)
            self.assertEqual(len(hot_arrivals), 3)

    def test_expiring_deadline_settles_active_owner_without_more_admission(self):
        for name in ("first", "second"):
            self.add(name, age=100)
        started = threading.Event()
        release = threading.Event()
        original = self.logical._prepare_batch_and_upload

        def prepare(candidates, **kwargs):
            uploads = original(candidates, **kwargs)
            started.set()
            if not release.wait(5):
                raise AssertionError("fixture_gate_unreleased")
            return uploads

        self.logical._prepare_batch_and_upload = prepare
        deadline = time.monotonic() + 0.2
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                self.logical.project_pending,
                batch_size=500,
                max_batches=2,
                upload_concurrency=1,
                admission_deadline_at=deadline,
            )
            try:
                self.assertTrue(started.wait(2))
                time.sleep(max(0, deadline - time.monotonic()) + 0.02)
                self.assertFalse(
                    future.done(), "active proof was cancelled on admission expiry"
                )
            finally:
                release.set()
            result = future.result(timeout=5)
        self.assertEqual(result["documents"], 1)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["batches"], 1)

    def test_due_admission_deadline_preserves_all_pending_work(self):
        self.add("still-pending", age=100)
        result = self.logical.project_pending(
            batch_size=500, max_batches=2, admission_deadline_at=time.monotonic() - 1
        )
        self.assertEqual(result["documents"], 0)
        self.assertEqual(result["batches"], 0)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(self.run_round()["documents"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
