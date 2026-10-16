"""Controlled native-owner checkout overlap, not attribution of a past incident."""

import contextlib
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
import e2e_logical_admission_sharing as fixtures
from recall_server.worker_connections import WorkerWriteStore
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.canonical_thinning import CanonicalBodyThinner

H = Path(__file__).parent
RESULTS = []


class WorkerConnectionAdmission(unittest.TestCase):
    setUpClass = classmethod(fixtures.AdmissionSharing.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.AdmissionSharing.tearDownClass.__func__)
    setUp = fixtures.AdmissionSharing.setUp
    projector = fixtures.AdmissionSharing.projector
    add = fixtures.AdmissionSharing.add

    def test_publication_progress_with_logical_and_background_overlap(self):
        with_thinner = True
        for name in ("passage-a", "passage-b"):
            self.add(name, age=100, source="source:passage")
        self.assertEqual(
            self.logical.project_pending(
                batch_size=2, max_batches=1, upload_concurrency=1
            )["documents"],
            2,
        )
        passages = CanonicalPassageProjector(
            self.store,
            self.projection,
            policy=DEFAULT_PASSAGE_POLICY,
            bound_tenant_id=self.tenant,
        )
        candidates = passages._pending(tenant_id=self.tenant, limit=2)
        self.assertEqual(len(candidates), 2)
        prepared = [passages._prepare(c) for c in candidates]
        for name in ("logical-a", "logical-b"):
            self.add(name, age=100, source="source:logical")
        logical_candidates = self.logical._pending(tenant_id=self.tenant, limit=2)
        self.assertEqual(len(logical_candidates), 2)
        self.store.prepare_pool(4)
        background_store = WorkerWriteStore(self.store)
        self.logical.store = background_store
        native_connect = self.store.connect
        local = threading.local()
        release = threading.Event()
        changed = threading.Condition()
        held = set()
        seen = set()
        hold_roles = {"logical-a", "logical-b", "passage-a"} | (
            {"thinner"} if with_thinner else set()
        )
        occupancy = []
        active = set()

        @contextlib.contextmanager
        def observed_connect(**kwargs):
            role = getattr(local, "role", "coordinator")
            with native_connect(**kwargs) as connection:
                with changed:
                    active.add((role, id(connection)))
                    occupancy.append(len(active))
                try:
                    if role in hold_roles and role not in seen:
                        # Delay a real successful statement inside the checkout
                        # requested by this unchanged native owner. No extra
                        # connection or altered pool timeout is introduced.
                        connection.execute("SELECT 1").fetchone()
                        with changed:
                            seen.add(role)
                            held.add(role)
                            changed.notify_all()
                        if not release.wait(15):
                            raise AssertionError("fixture_holder_not_released")
                    yield connection
                finally:
                    with changed:
                        active.remove((role, id(connection)))

        def invoke(role, call):
            local.role = role
            return call()

        def logical_owner(candidate):
            current = self.logical._candidate_at_dispatch(candidate)
            (upload,) = self.logical._prepare_batch_and_upload(
                (current,), require_current_queue=True
            )
            return self.logical._commit_upload(current, upload)

        futures = []
        victim_status = None
        with (
            patch.object(self.store, "connect", side_effect=observed_connect),
            ThreadPoolExecutor(max_workers=5) as executor,
        ):
            try:
                thinner = CanonicalBodyThinner(background_store, tenant_id=self.tenant)
                futures.append(
                    executor.submit(
                        invoke, "thinner", lambda: thinner.thin(batch_size=1)
                    )
                )
                with changed:
                    self.assertTrue(
                        changed.wait_for(lambda: "thinner" in held, timeout=5)
                    )
                for i, candidate in enumerate(logical_candidates):
                    futures.append(
                        executor.submit(
                            invoke,
                            ("logical-a", "logical-b")[i],
                            lambda c=candidate: logical_owner(c),
                        )
                    )
                futures.append(
                    executor.submit(
                        invoke, "passage-a", lambda: passages._commit(prepared[0])
                    )
                )
                with changed:
                    self.assertTrue(
                        changed.wait_for(
                            lambda: (
                                "passage-a" in held
                                and bool({"logical-a", "logical-b"} & held)
                            ),
                            timeout=5,
                        ),
                        held,
                    )
                    changed.wait_for(
                        lambda: {"logical-a", "logical-b"} <= held, timeout=0.2
                    )
                victim_status = invoke(
                    "passage-b", lambda: passages._commit(prepared[1])
                )
                self.assertEqual(victim_status["status"], "committed")
            finally:
                release.set()
            owner_results = [f.result(timeout=10) for f in futures]
        self.assertEqual(max(occupancy), 4)
        self.assertFalse(active)
        self.assertEqual(len(owner_results), 4)
        self.assertEqual(self.store.pool_max_size, 4)

    def test_warmup_with_active_thinner_and_guarded_sql_no_nested_checkout(self):
        view = WorkerWriteStore(self.store)
        held, release = threading.Event(), threading.Event()

        def thinner_checkout():
            with view.connect():
                held.set()
                self.assertTrue(release.wait(5))

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(thinner_checkout)
            try:
                self.assertTrue(held.wait(2))
                view.prepare_pool(2)
                with view.connect() as connection:
                    self.assertEqual(
                        view._execute_bounded(
                            connection, "SELECT 7 AS n", (), None
                        ).fetchone()["n"],
                        7,
                    )
                    view._set_statement_deadline(connection, None)
                    with self.assertRaisesRegex(RuntimeError, "nested worker"):
                        with view.connect():
                            self.fail("nested checkout was admitted")
                with self.assertRaisesRegex(RuntimeError, "admitted connection"):
                    view._execute_bounded(connection, "SELECT 1", (), None)
                with self.store.connect() as outside:
                    with self.assertRaisesRegex(RuntimeError, "admitted connection"):
                        view._set_statement_deadline(outside, None)
            finally:
                release.set()
            future.result(timeout=2)

        # Both slots return after an owner error, including BaseException.
        class Cancelled(BaseException):
            pass

        with self.assertRaises(Cancelled):
            with view.connect():
                raise Cancelled()
        view.prepare_pool(2)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute("SELECT 1 AS n").fetchone()["n"], 1)

    def test_continuous_worker_pool4_keeps_original_publication_and_drain_guarantees(
        self,
    ):
        import e2e_passage_callback_quantum as quantum
        from recall_server.projection_worker import BackgroundBodyThinner

        original_projector = quantum.CanonicalLogicalEvidenceProjector
        original_worker = quantum.run_projection_worker
        view = WorkerWriteStore(self.store)
        runs = []

        def projector(store, *args, **kwargs):
            self.assertIs(store, self.store)
            return original_projector(view, *args, **kwargs)

        def worker(logical, passages, **kwargs):
            self.assertIs(logical.store, view)
            self.assertIs(passages.store, self.store)
            self.assertEqual(kwargs["upload_concurrency"], 2)
            self.assertEqual(kwargs["passage_concurrency"], 2)
            thinner = CanonicalBodyThinner(view, tenant_id=kwargs["tenant_id"])
            held, release = threading.Event(), threading.Event()
            native_connect = self.store.connect
            observed = False
            original_publish = kwargs["search_plane"]

            @contextlib.contextmanager
            def connect(**options):
                nonlocal observed
                with native_connect(**options) as connection:
                    if (
                        threading.current_thread().name.startswith(
                            "recall-body-thinning"
                        )
                        and not observed
                    ):
                        observed = True
                        connection.execute("SELECT 1").fetchone()
                        held.set()
                        if not release.wait(10):
                            raise AssertionError(
                                "foreground publication did not progress"
                            )
                    yield connection

            def publish():
                result = original_publish()
                release.set()
                return result

            def prepare(busy):
                hints = logical._take_committed_body_keys()
                return lambda: thinner.thin(committed_keys=hints, batch_size=1)

            with (
                patch.object(self.store, "connect", side_effect=connect),
                BackgroundBodyThinner(prepare) as background,
            ):
                background.tick(True)
                self.assertTrue(held.wait(3))
                try:
                    result = original_worker(
                        logical,
                        passages,
                        **dict(
                            kwargs, search_plane=publish, background_thinner=background
                        ),
                    )
                finally:
                    release.set()
                background.close()
                self.assertEqual(background.failed_attempts, 0)
                self.assertGreaterEqual(background.completed_attempts, 1)
                runs.append(result)
                return result

        with (
            patch.object(
                quantum, "CanonicalLogicalEvidenceProjector", side_effect=projector
            ),
            patch.object(quantum, "run_projection_worker", side_effect=worker),
        ):
            # Run the existing native logical/passage/publisher/authorized-search
            # scenario unchanged, now pool4 plus real background thinning.
            quantum.CallbackQuantum.test_fresh_source_publishes_before_backlog_drains_then_all_sources_drain(
                self
            )
        self.assertEqual(len(runs), 2)
        self.assertEqual(self.store.pool_max_size, 4)

    def test_bounded_wait_releases_failed_admission_and_does_not_take_foreground_slots(
        self,
    ):
        import time
        from psycopg_pool import PoolTimeout

        view = WorkerWriteStore(self.store)
        ready = threading.Barrier(3)
        release = threading.Event()

        def hold():
            with view.connect():
                ready.wait(timeout=3)
                self.assertTrue(release.wait(8))

        with ThreadPoolExecutor(max_workers=2) as executor:
            holders = [executor.submit(hold) for _ in range(2)]
            ready.wait(timeout=3)
            try:
                started = time.monotonic()
                with self.assertRaises(PoolTimeout):
                    with view.connect():
                        self.fail("saturated background admission succeeded")
                self.assertLess(time.monotonic() - started, 6)
                with self.store.connect() as first, self.store.connect() as second:
                    self.assertEqual(first.execute("SELECT 1 AS n").fetchone()["n"], 1)
                    self.assertEqual(second.execute("SELECT 2 AS n").fetchone()["n"], 2)
            finally:
                release.set()
            for future in holders:
                future.result(timeout=2)
        # A failure in the real checkout owner cannot consume the admission slot.
        with patch.object(
            self.store, "connect", side_effect=PoolTimeout("synthetic checkout failure")
        ):
            with self.assertRaises(PoolTimeout):
                with view.connect():
                    self.fail("failed native checkout was yielded")
        with patch.object(
            self.store,
            "prepare_pool",
            side_effect=PoolTimeout("synthetic warmup failure"),
        ):
            with self.assertRaises(PoolTimeout):
                view.prepare_pool(2)
        view.prepare_pool(2)

    def test_two_archive_preparations_remain_parallel_without_holding_sql_slots(self):
        view = WorkerWriteStore(self.store)
        self.logical.store = view
        for index in range(2):
            self.add(f"parallel-{index}", age=100, source=f"source:parallel-{index}")
        barrier = threading.Barrier(3)
        release = threading.Event()
        native_put = self.projection.put_records

        def put(*args, **kwargs):
            barrier.wait(timeout=5)
            self.assertTrue(release.wait(5))
            return native_put(*args, **kwargs)

        with (
            patch.object(self.projection, "put_records", side_effect=put),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(
                self.logical.project_pending,
                batch_size=2,
                max_batches=1,
                upload_concurrency=2,
            )
            try:
                barrier.wait(timeout=5)
                # Both real logical owners are in archive preparation. The same
                # admission budget remains available for thinning SQL.
                thinner = CanonicalBodyThinner(view, tenant_id=self.tenant)
                thinner.thin(batch_size=1)
                view.prepare_pool(2)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)["documents"], 2)
        self.assertEqual(self.logical._pending(tenant_id=self.tenant, limit=2), [])


if __name__ == "__main__":
    unittest.main()
