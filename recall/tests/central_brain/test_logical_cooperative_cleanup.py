"""Cleanup waves must not monopolize the existing logical coordinator."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from tests.central_brain.test_logical_cleanup_concurrency import Store
from tests.central_brain.test_logical_streaming_progress import QueueProjector


class Archive:
    def __init__(self, store, *, held_wave=1, advance=lambda: None):
        self.store, self.held_wave, self.advance = store, held_wave, advance
        self.calls = []
        self.entered, self.release = threading.Event(), threading.Event()

    def delete_reference(self, reference):
        if not self.store.in_transaction:
            raise AssertionError('native deletion must retain its original transaction')
        self.calls.append(reference['artifact_id'])
        if len(self.calls) == 1:
            self.advance()
        if len(self.calls) == self.held_wave:
            self.entered.set()
            if not self.release.wait(5):
                raise AssertionError('test failed to release its cleanup wave')
        raise OSError('synthetic archive confirmation pending')


class TrackedStore(Store):
    def __init__(self):
        super().__init__()
        self.connections = 0
        self.fail_exit = None

    @contextmanager
    def connect(self):
        self.connections += 1
        try:
            yield self
            if self.fail_exit is not None:
                raise self.fail_exit
        finally:
            self.connections -= 1


class Projector(QueueProjector):
    drain_cleanup = CanonicalLogicalEvidenceProjector.drain_cleanup

    def __init__(self, names, *, held_wave=1, advance=lambda: None):
        super().__init__(names)
        self.store = TrackedStore()
        for number in range(9):
            key = f'artifact-{number}'
            self.store.rows[key] = dict(
                tenant_id='tenant:test', source_id='source:test', artifact_id=key,
                storage_backend='s3', object_key=key, content_sha256='a'*64,
                size_bytes=1, media_type='application/json', encryption='none',
                version_id='v1', created_at=datetime.now(timezone.utc),
                removable=True, attempts=1)
        self.projection = Archive(self.store, held_wave=held_wave, advance=advance)
        self.all_committed = threading.Event()

    def _commit_upload(self, item, upload):
        status = super()._commit_upload(item, upload)
        if not self.queue:
            self.all_committed.set()
        return status


class CooperativeCleanupTests(unittest.TestCase):
    def test_existing_feeder_admits_more_than_one_owner_wave_during_cleanup(self):
        projector = Projector(['small', 'tiny1', 'tiny2'])
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=3,
                max_batches=1, upload_concurrency=1, cleanup_concurrency=1,
                on_heartbeat=lambda: None)
            try:
                self.assertTrue(projector.projection.entered.wait(2))
                self.assertTrue(projector.all_committed.wait(1),
                    'initial cleanup blocked ordinary logical commits/refill')
                self.assertFalse(future.done())
            finally:
                projector.projection.release.set()
            result = future.result(timeout=5)
        self.assertEqual(result['documents'], 3)
        self.assertEqual(sum(projector.started.values()), 3)
        self.assertLessEqual(projector.peak, 1)
        self.assertEqual(len(projector.store.rows), 9)
        self.assertEqual(projector.store.acknowledged, [])

    def test_maintenance_callback_runs_between_waves_after_connection_release(self):
        clock = [0.0]
        def advance(): clock[0] += 6.0
        projector = Projector([], held_wave=2, advance=advance)
        between_waves = threading.Event()
        callback_threads = []
        def maintenance():
            callback_threads.append(threading.get_ident())
            self.assertFalse(projector.store.in_transaction,
                'maintenance callback inherited cleanup transaction/locks')
            self.assertEqual(projector.store.connections, 0)
            if projector.projection.calls:
                between_waves.set()
        with patch('recall_server.logical_evidence_projection.time',
                   SimpleNamespace(monotonic=lambda: clock[0])), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=1,
                max_batches=1, upload_concurrency=1, cleanup_concurrency=1,
                on_heartbeat=maintenance)
            try:
                self.assertTrue(projector.projection.entered.wait(2))
                self.assertTrue(between_waves.wait(.5),
                    'retirement/search maintenance waited behind the next cleanup wave')
                self.assertFalse(future.done())
            finally:
                projector.projection.release.set()
            result = future.result(timeout=5)
        self.assertEqual(result['documents'], 0)
        self.assertEqual(len(set(callback_threads)), 1)
        self.assertEqual(projector.store.acknowledged, [])
        self.assertEqual(len(projector.store.rows), 9)

    def test_one_drain_never_retries_failed_identity_across_yields(self):
        clock = [0.0]
        def advance(): clock[0] += 6.0
        projector = Projector([], advance=advance)
        projector.projection.release.set()
        callbacks = []
        def maintenance():
            self.assertEqual(projector.store.connections, 0)
            self.assertFalse(projector.store.in_transaction)
            callbacks.append(len(projector.projection.calls))
        with patch('recall_server.logical_evidence_projection.time',
                   SimpleNamespace(monotonic=lambda: clock[0])):
            result = projector.drain_cleanup(limit=9, concurrency=1, on_progress=maintenance)
        self.assertEqual(callbacks, [0, 1])
        self.assertEqual(len(projector.projection.calls), 9)
        self.assertEqual(len(set(projector.projection.calls)), 9)
        self.assertEqual((result['deleted'], result['failures'], result['pending']), (0, 9, 9))
        self.assertEqual(projector.store.acknowledged, [])
        self.assertTrue(all(row['attempts'] == 2 for row in projector.store.rows.values()))

    def test_unknown_initial_cleanup_exit_is_not_retried_or_acknowledged(self):
        projector = Projector(['small'])
        projector.projection.release.set()
        unknown = OSError('synthetic lost transaction acknowledgement')
        projector.store.fail_exit = unknown
        with self.assertRaises(OSError) as raised:
            projector.project_pending(batch_size=1, max_batches=1,
                upload_concurrency=1, cleanup_concurrency=1, on_heartbeat=lambda: None)
        self.assertIs(raised.exception, unknown)
        self.assertEqual(projector.projection.calls, ['artifact-0'])
        self.assertEqual(projector.store.acknowledged, [])
        self.assertFalse(projector.running)
        self.assertEqual(projector.store.connections, 0)

    def test_callback_baseexception_settles_feeder_and_active_owner_without_second_drain(self):
        clock = [0.0]
        def advance(): clock[0] += 6.0
        projector = Projector(['large'], advance=advance)
        projector.projection.release.set()
        callback_failed = threading.Event()
        interruption = KeyboardInterrupt('synthetic interruption')
        def maintenance():
            self.assertEqual(projector.store.connections, 0)
            if projector.projection.calls:
                callback_failed.set()
                raise interruption
        with patch('recall_server.logical_evidence_projection.time',
                   SimpleNamespace(monotonic=lambda: clock[0])), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=1,
                max_batches=1, upload_concurrency=1, cleanup_concurrency=1,
                on_heartbeat=maintenance)
            try:
                self.assertTrue(projector.blocked.wait(2))
                self.assertTrue(callback_failed.wait(2))
                self.assertFalse(future.done(), 'active logical owner was abandoned')
            finally:
                projector.release.set()
            with self.assertRaises(KeyboardInterrupt) as raised:
                future.result(timeout=5)
        self.assertIs(raised.exception, interruption)
        self.assertEqual(projector.projection.calls, ['artifact-0'])
        self.assertEqual(projector.store.acknowledged, [])
        self.assertEqual(sum(projector.started.values()), 1)
        # The inherited fake removes `running` only on commit. Cancellation must
        # instead retain the queue and schedule the settled upload for cleanup.
        self.assertEqual(projector.cleaned, ['large'])
        self.assertEqual([item.native_parent_id for item in projector.queue], ['large'])
        self.assertFalse(any(thread.name.startswith(('recall-logical-refill', 'recall-logical-evidence', 'recall-logical-cleanup'))
                             for thread in threading.enumerate()))

    def test_standalone_keeps_initial_cleanup_before_logical_admission(self):
        projector = Projector(['small'])
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=1,
                max_batches=1, upload_concurrency=1, cleanup_concurrency=1)
            try:
                self.assertTrue(projector.projection.entered.wait(2))
                self.assertEqual(sum(projector.started.values()), 0)
            finally:
                projector.projection.release.set()
            result = future.result(timeout=5)
        self.assertEqual(result['documents'], 1)
        self.assertEqual(projector.store.acknowledged, [])


if __name__ == '__main__':
    unittest.main()
