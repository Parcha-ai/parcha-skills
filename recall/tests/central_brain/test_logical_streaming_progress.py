"""Bounded refill, isolation and cancellation of logical parent admission."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, LogicalGroupCandidate
from tests.central_brain.test_logical_parent_progress import Store


def candidate(name):
    return LogicalGroupCandidate('tenant:test', 'source:test', name,
        datetime.now(timezone.utc), 1, 1)


class QueueProjector(CanonicalLogicalEvidenceProjector):
    def _check_candidate_current(self, candidate):
        # Scheduler-only fake; real queue authority is covered by PostgreSQL tests.
        return None

    def __init__(self, names):
        super().__init__(Store(), None, bound_tenant_id='tenant:test')
        self.queue = [candidate(name) for name in names]
        self.lock = threading.Lock()
        self.started = Counter()
        self.running = set()
        self.peak = 0
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.small_done = threading.Event()
        self.cleaned = []
        self.statuses = {}
        self.admission_limits = []
        self.hold_replacement = False
        self.buffer_done = threading.Event()

    def _pending(self, **kwargs):
        with self.lock:
            self.admission_limits.append(kwargs['limit'])
            return list(self.queue[:kwargs['limit']])

    def drain_cleanup(self, **kwargs):
        return dict(deleted=0, failures=0, completed=0, pending=0)

    def _schedule_upload_cleanup(self, uploads):
        self.cleaned.extend(upload.name for upload in uploads)

    def _mark_failed(self, candidate, error):
        # Simulate a concurrent newer generation remaining eligible after failure.
        pass

    def _prepare_batch_and_upload(self, candidates, **kwargs):
        item, = candidates
        name = item.native_parent_id
        with self.lock:
            if name in self.running:
                raise AssertionError('duplicate parent in flight')
            self.running.add(name)
            self.started[name] += 1
            self.peak = max(self.peak, len(self.running))
        if name == 'large' or (self.hold_replacement and name.startswith('tiny')):
            if name == 'large':
                self.blocked.set()
            if not self.release.wait(10):
                raise AssertionError('test did not release giant')
        if self.statuses.get(name) == 'failed':
            with self.lock:
                self.running.remove(name)
            raise ValueError('synthetic failure with newer queued generation')
        return [SimpleNamespace(name=name, all_references=(),
            prepared=SimpleNamespace(record_count=1, receipt_count=1))]

    def _commit_upload(self, item, upload):
        with self.lock:
            self.running.remove(item.native_parent_id)
            status = self.statuses.get(item.native_parent_id, 'committed')
            if status == 'committed':
                self.queue.remove(item)
        if item.native_parent_id == 'small':
            self.small_done.set()
        if item.native_parent_id == 'tiny3':
            self.buffer_done.set()
        return status


class StreamingAdmissionTests(unittest.TestCase):
    def test_refill_is_bounded_and_never_duplicates_busy_parent(self):
        projector = QueueProjector(['large', 'small', *[f'tiny{i}' for i in range(8)]])
        fifth = threading.Event()
        def publish():
            if sum(projector.started.values()) >= 6:
                fifth.set()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=2, max_batches=3,
                upload_concurrency=2, on_progress=publish)
            try:
                self.assertTrue(projector.blocked.wait(2))
                self.assertTrue(fifth.wait(2), 'later admission rounds blocked behind giant')
                self.assertFalse(future.done())
                self.assertEqual(sum(projector.started.values()), 6)
                self.assertEqual(projector.started['large'], 1)
                self.assertLessEqual(projector.peak, 2)
            finally:
                projector.release.set()
            result = future.result(timeout=5)
        self.assertEqual(result['documents'], 6)
        self.assertEqual(result['batches'], 3)
        self.assertEqual(len(projector.queue), 4)

    def test_new_arrival_uses_idle_slot_while_only_giant_remains(self):
        projector = QueueProjector(['large', 'small'])
        arrived = threading.Event()
        def publish():
            if projector.started['later']:
                arrived.set()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=2, max_batches=2,
                upload_concurrency=2, on_progress=publish)
            try:
                self.assertTrue(projector.blocked.wait(2))
                self.assertTrue(projector.small_done.wait(2))
                with projector.lock:
                    projector.queue.append(candidate('later'))
                self.assertTrue(arrived.wait(7), 'idle slot ignored newly eligible source')
                self.assertFalse(future.done())
            finally:
                projector.release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 3)

    def test_stale_failed_and_adopted_parents_are_not_repeated_this_call(self):
        projector = QueueProjector(['stale', 'failed', 'adopted', 'small'])
        projector.statuses = dict(stale='stale', failed='failed', adopted='adopted')
        result = projector.project_pending(batch_size=2, max_batches=4, upload_concurrency=2)
        self.assertEqual(projector.started, Counter(stale=1, failed=1, adopted=1, small=1))
        self.assertEqual(result['source_races'], 1)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(result['documents'], 1)
        self.assertEqual(result['batches'], 2)

    def test_buffered_parents_commit_during_held_publication(self):
        projector = QueueProjector(['large', 'small', *[f'tiny{i}' for i in range(4)]])
        held = threading.Event()
        def publish():
            held.set()
            if not projector.buffer_done.wait(2):
                raise AssertionError('buffered preparation waited for publication callback')
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=6, max_batches=1,
                upload_concurrency=2, on_progress=publish)
            try:
                self.assertTrue(projector.blocked.wait(2))
                self.assertTrue(held.wait(2))
                self.assertTrue(projector.buffer_done.wait(3),
                    'only one replacement ran while coordinator published')
                self.assertLessEqual(projector.peak, 2)
                self.assertFalse(future.done())
            finally:
                projector.release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 6)

    def test_publication_interrupt_settles_uploads_without_admitting_buffer(self):
        for error in (RuntimeError('publication failed'), KeyboardInterrupt(), TimeoutError('budget expired')):
            with self.subTest(error=type(error).__name__):
                projector = QueueProjector(['large', 'small', *[f'tiny{i}' for i in range(10)]])
                projector.hold_replacement = True
                raised, settling = threading.Event(), threading.Event()
                class SettlingExecutor(ThreadPoolExecutor):
                    def shutdown(self, *args, **kwargs):
                        settling.set()
                        return super().shutdown(*args, **kwargs)
                def publish():
                    raised.set()
                    raise error
                with patch('recall_server.logical_evidence_projection.ThreadPoolExecutor', SettlingExecutor), \
                        ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(projector.project_pending, batch_size=10, max_batches=3,
                        upload_concurrency=2, on_progress=publish)
                    try:
                        self.assertTrue(projector.blocked.wait(2))
                        self.assertTrue(raised.wait(2))
                        self.assertTrue(settling.wait(2), 'cancellation did not settle active owners')
                    finally:
                        projector.release.set()
                    with self.assertRaises(type(error)):
                        future.result(timeout=5)
                # At most one replacement was admitted before the failed tick;
                # remaining buffered parents never upload. Giant is settled.
                self.assertLessEqual(sum(projector.started.values()), 3)
                self.assertEqual(projector.started['large'], 1)
                self.assertTrue('large' in projector.cleaned or not any(
                    item.native_parent_id == 'large' for item in projector.queue))


class PublicationHandoffTests(unittest.TestCase):
    def test_deadline_during_callback_settles_without_admitting_more(self):
        import time
        projector = QueueProjector(['large', 'small', 'tiny0', 'tiny1'])
        projector.hold_replacement = True
        now = [time.monotonic()]
        deadline = now[0] + 1000
        callbacks = []

        def publish():
            callbacks.append(True)
            now[0] = deadline + 1
            projector.release.set()

        with patch('recall_server.logical_evidence_projection.time',
                   SimpleNamespace(monotonic=lambda: now[0])):
            result = projector.project_pending(batch_size=4, max_batches=1,
                upload_concurrency=2, on_progress=publish,
                admission_deadline_at=deadline)
        self.assertEqual(result['documents'], 3)
        self.assertEqual([c.native_parent_id for c in projector.queue], ['tiny1'])
        self.assertEqual(set(projector.started), {'large', 'small', 'tiny0'})
        self.assertEqual(projector.peak, 2)
        self.assertTrue(callbacks)
        self.assertFalse(any(t.name == 'recall-logical-refill' for t in threading.enumerate()))

    def test_feeder_failure_joins_and_preserves_exception(self):
        projector = QueueProjector(['large', 'small', 'tiny0', 'tiny1'])
        failed = threading.Event()
        pending = projector._pending
        failure = RuntimeError('synthetic nomination failure')

        def nominate(**kwargs):
            if threading.current_thread().name == 'recall-logical-refill':
                failed.set()
                raise failure
            return pending(**kwargs)

        def publish():
            self.assertTrue(failed.wait(2))
            projector.release.set()

        with patch.object(projector, '_pending', side_effect=nominate):
            try:
                with self.assertRaises(RuntimeError) as caught:
                    projector.project_pending(batch_size=4, max_batches=1,
                        upload_concurrency=2, on_progress=publish)
                self.assertIs(caught.exception, failure)
            finally:
                projector.release.set()
        self.assertEqual(set(projector.started), {'large', 'small', 'tiny0'})
        self.assertTrue('large' in projector.cleaned or not any(
            c.native_parent_id == 'large' for c in projector.queue))
        self.assertFalse(any(t.name.startswith('recall-logical-evidence')
                             for t in threading.enumerate()))
        self.assertFalse(any(t.name == 'recall-logical-refill' for t in threading.enumerate()))

    def test_callback_failure_stops_feeder_before_handoff_returns(self):
        projector = QueueProjector(['large', 'small', 'tiny0', 'tiny1', 'tiny2'])
        nominee_started, release_nominee = threading.Event(), threading.Event()
        pending = projector._pending
        callback_failed = threading.Event()
        failure = RuntimeError('synthetic publication failure')
        handoff_stopped = threading.Event()
        native_thread = threading.Thread

        class HandoffThread(native_thread):
            def join(self, *args, **kwargs):
                if self.name == 'recall-logical-refill':
                    handoff_stopped.set()
                return super().join(*args, **kwargs)

        def nominate(**kwargs):
            if threading.current_thread().name == 'recall-logical-refill':
                nominee_started.set()
                if not release_nominee.wait(3):
                    raise AssertionError('nomination_not_released')
            return pending(**kwargs)

        def publish():
            self.assertTrue(nominee_started.wait(2))
            callback_failed.set()
            raise failure

        with patch.object(projector, '_pending', side_effect=nominate), \
                patch('recall_server.logical_evidence_projection.threading.Thread', HandoffThread), \
                ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=5,
                max_batches=1, upload_concurrency=2, on_progress=publish)
            try:
                self.assertTrue(callback_failed.wait(3))
                self.assertTrue(handoff_stopped.wait(2))
                self.assertFalse(future.done())
                admitted = dict(projector.started)
            finally:
                release_nominee.set()
                projector.release.set()
            with self.assertRaises(RuntimeError) as caught:
                future.result(timeout=5)
            self.assertIs(caught.exception, failure)
        self.assertEqual(dict(projector.started), admitted)
        self.assertTrue('large' in projector.cleaned or not any(
            c.native_parent_id == 'large' for c in projector.queue))
        self.assertFalse(any(t.name.startswith('recall-logical-evidence')
                             for t in threading.enumerate()))
        self.assertFalse(any(t.name == 'recall-logical-refill' for t in threading.enumerate()))


if __name__ == '__main__':
    unittest.main()
