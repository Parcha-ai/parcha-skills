"""Actual coordinator queries while an owner and downstream callback are held."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import time
import unittest

import e2e_logical_admission_sharing as fixture
import e2e_logical_slot_refill as retained
from recall_server.logical_evidence_projection import PROJECTION_PROGRESS_INTERVAL_SECONDS


class RefillPolling(unittest.TestCase):
    setUpClass = classmethod(fixture.AdmissionSharing.setUpClass.__func__)
    tearDownClass = classmethod(fixture.AdmissionSharing.tearDownClass.__func__)
    setUp = fixture.AdmissionSharing.setUp
    projector = fixture.AdmissionSharing.projector
    add = fixture.AdmissionSharing.add

    test_completed_owner_refills_immediately_during_callback = (
        retained.SlotBounds.test_fresh_third_source_enters_during_held_publication
    )

    def test_empty_slot_does_not_poll_sql_at_handoff_join_frequency(self):
        self.add('large', age=90000, source='source:large')
        self.add('small', age=100, source='source:small')
        release = threading.Event()
        large_started = threading.Event()
        callback_active = threading.Event()
        queries = []
        original_prepare = self.logical._prepare_batch_and_upload
        original_pending = self.logical._pending

        def prepare(candidates, **kwargs):
            uploads = original_prepare(candidates, **kwargs)
            if candidates[0].native_parent_id == 'large':
                large_started.set()
                if not release.wait(10):
                    raise AssertionError('fixture_owner_not_released')
            return uploads

        def pending(**kwargs):
            result = original_pending(**kwargs)
            if callback_active.is_set():
                queries.append(time.monotonic())
            return result

        callback_times = []

        def publish():
            if callback_times:
                return
            self.assertTrue(large_started.wait(3))
            callback_times.append(time.monotonic())
            callback_active.set()
            # Less than the existing five-second progress interval. An empty
            # spare slot must not create a query at every 50ms feeder join poll.
            release.wait(.8)
            callback_active.clear()
            release.set()

        self.logical._prepare_batch_and_upload = prepare
        self.logical._pending = pending
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.logical.project_pending, batch_size=500,
                max_batches=2, upload_concurrency=2, on_progress=publish)
            try:
                result = future.result(timeout=15)
            finally:
                release.set()
        self.assertEqual(result['documents'], 2)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(len(self.selected), len(set(self.selected)))
        print(json.dumps(dict(callback_seconds=.8,
            existing_progress_seconds=PROJECTION_PROGRESS_INTERVAL_SECONDS,
            actual_native_pending_queries=len(queries),
            query_gaps_ms=[round((b-a)*1000,1) for a,b in zip(queries,queries[1:])],
            documents=result['documents'])))
        self.assertLessEqual(len(queries), 1,
            'no-completion handoff repeatedly polled the native pending SQL')

    def exercise_callback(self, callback):
        self.add('large', age=90000, source='source:large')
        self.add('small', age=100, source='source:small')
        release, first_poll, fresh_started = (threading.Event() for _ in range(3))
        polls = []
        original_prepare = self.logical._prepare_batch_and_upload
        original_pending = self.logical._pending

        def prepare(candidates, **kwargs):
            uploads = original_prepare(candidates, **kwargs)
            name = candidates[0].native_parent_id
            if name == 'large' and not release.wait(15):
                raise AssertionError('fixture_owner_not_released')
            if name == 'fresh':
                fresh_started.set()
            return uploads

        def pending(**kwargs):
            rows = original_pending(**kwargs)
            if threading.current_thread().name == 'recall-logical-refill':
                polls.append(time.monotonic())
                first_poll.set()
            return rows

        calls = []

        def publish():
            calls.append(True)
            if len(calls) > 1:
                return
            try:
                callback(first_poll, fresh_started, polls)
            finally:
                release.set()

        self.logical._pending = pending
        self.logical._prepare_batch_and_upload = prepare
        try:
            return self.logical.project_pending(batch_size=500, max_batches=2,
                upload_concurrency=2, on_progress=publish)
        finally:
            release.set()

    def test_first_feeder_refresh_admits_arrival_without_waiting_five_seconds(self):
        def callback(first_poll, fresh_started, polls):
            self.add('fresh', age=100, source='source:fresh')
            self.assertTrue(fresh_started.wait(2), 'first idle slot was not refreshed')
        result = self.exercise_callback(callback)
        self.assertEqual(result['documents'], 3)

    def test_arrival_after_empty_refresh_uses_existing_progress_timer(self):
        observed = {}
        def callback(first_poll, fresh_started, polls):
            self.assertTrue(first_poll.wait(3))
            self.add('fresh', age=100, source='source:fresh')
            self.assertTrue(fresh_started.wait(PROJECTION_PROGRESS_INTERVAL_SECONDS + 2),
                            'new work was stranded behind the busy owner')
            observed['gap'] = polls[1] - polls[0]
            self.assertGreaterEqual(observed['gap'], PROJECTION_PROGRESS_INTERVAL_SECONDS)
        result = self.exercise_callback(callback)
        self.assertEqual(result['documents'], 3)
        print(json.dumps(dict(case='new_arrival_timer', refresh_gap_seconds=observed['gap'])))

    def test_callback_failure_joins_feeder_without_waiting_progress_interval(self):
        failed_at = []
        def callback(first_poll, fresh_started, polls):
            self.assertTrue(first_poll.wait(3))
            failed_at.append(time.monotonic())
            raise RuntimeError('synthetic_callback_failure')
        with self.assertRaisesRegex(RuntimeError, '^synthetic_callback_failure$'):
            self.exercise_callback(callback)
        self.assertLess(time.monotonic() - failed_at[0], 2)
        self.assertFalse(any(t.name == 'recall-logical-refill' for t in threading.enumerate()))


if __name__ == '__main__':
    unittest.main()
