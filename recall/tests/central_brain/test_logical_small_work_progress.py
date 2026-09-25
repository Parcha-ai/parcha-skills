"""Witness the boundary between batch sorting and free-owner dispatch."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests.central_brain.test_logical_streaming_progress import QueueProjector, candidate

# Prior-revision estimates, in the measured oldest-admission order. These are
# scheduling hints only: no fixture allocates production-sized bodies.
SAMPLED = (
    ('slack_history', 1), ('unknown_a', 1), ('held_338mb', 338793399),
    ('short_a', 115926), ('unknown_b', 1), ('short_b', 4091006),
    ('held_140mb', 140411501), ('short_c', 7508312), ('short_d', 33286012),
    ('small', 1),
)


class SampledProjector(QueueProjector):
    def __init__(self, *, sort_control=False):
        super().__init__([name for name, _ in SAMPLED])
        self.queue = [replace(item, estimated_bytes=size)
                      for item, (_, size) in zip(self.queue, SAMPLED, strict=True)]
        self.sort_control = sort_control
        self.both_large = threading.Event()
        self.later_done = threading.Event()

    def _pending(self, **kwargs):
        admitted = super()._pending(**kwargs)
        # Diagnostic control only; it does not change the production owner.
        return sorted(admitted, key=lambda item: item.estimated_bytes) if self.sort_control else admitted

    def _prepare_batch_and_upload(self, candidates, **kwargs):
        item, = candidates
        name = item.native_parent_id
        with self.lock:
            if name in self.running:
                raise AssertionError('duplicate parent owner')
            self.running.add(name)
            self.started[name] += 1
            self.peak = max(self.peak, len(self.running))
            if sum(value.startswith('held_') for value in self.running) == 2:
                self.both_large.set()
        if name.startswith('held_') and not self.release.wait(10):
            raise AssertionError('fixture did not release large preparation')
        return [SimpleNamespace(name=name, all_references=(),
            prepared=SimpleNamespace(record_count=1, receipt_count=1))]

    def _commit_upload(self, item, upload):
        status = super()._commit_upload(item, upload)
        if item.native_parent_id == 'later':
            self.later_done.set()
        return status


class SmallWorkProgressTests(unittest.TestCase):
    def test_cost_order_does_not_change_excluded_overscan_selection(self):
        class ConcurrentlyRetired(QueueProjector):
            def _commit_upload(self, item, upload):
                status = super()._commit_upload(item, upload)
                if status == 'stale':
                    with self.lock:
                        self.queue.remove(item)
                return status
        projector = ConcurrentlyRetired(['stale', 'seed', 'heavy_next', 'medium_next', 'cheap_outside'])
        projector.statuses['stale'] = 'stale'
        projector.queue = [replace(item, estimated_bytes=size) for item, size in
                           zip(projector.queue, (1, 1, 100, 50, 1), strict=True)]
        result = projector.project_pending(batch_size=2, max_batches=2, upload_concurrency=1)
        self.assertEqual(set(projector.started), {'stale', 'seed', 'heavy_next', 'medium_next'})
        self.assertEqual(result['batches'], 2)
        self.assertEqual(result['source_races'], 1)
        self.assertEqual(list(projector.started)[-2:], ['medium_next', 'heavy_next'])
        self.assertEqual([c.native_parent_id for c in projector.queue], ['cheap_outside'])

    def test_owner_logs_hash_identity_and_keep_failures_content_free(self):
        for failing in (False, True):
            with self.subTest(failing=failing):
                projector = QueueProjector(['private-parent-identity'])
                failure = ValueError('private-source-body')
                with self.assertLogs('recall_server.logical_evidence_projection', level='INFO') as logs:
                    if failing:
                        with patch.object(projector, '_commit_upload', side_effect=failure):
                            with self.assertRaises(ValueError):
                                projector.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
                    else:
                        projector.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
                messages = [record.getMessage() for record in logs.records]
                self.assertEqual(len(messages), 2)
                self.assertTrue(messages[0].startswith('logical owner started '))
                self.assertTrue(messages[1].startswith('logical owner completed '))
                self.assertIn('status=error' if failing else 'status=committed', messages[1])
                self.assertRegex(messages[0], r'parent_sha256=[0-9a-f]{64}\b')
                self.assertRegex(messages[1], r'elapsed_ms=\d+\b')
                self.assertIn('estimated_bytes=1', messages[0])
                self.assertNotIn('private-parent-identity', '\n'.join(messages))
                self.assertNotIn('private-source-body', '\n'.join(messages))

    def test_owner_log_failure_does_not_change_commit(self):
        projector = QueueProjector(['small'])
        with patch('recall_server.logical_evidence_projection.LOG.info', side_effect=RuntimeError('logging failed')):
            self.assertEqual(projector.project_pending(batch_size=1, max_batches=1,
                upload_concurrency=1)['documents'], 1)

    def test_admitted_small_slack_finishes_before_large_preparations(self):
        projector = SampledProjector()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=10,
                max_batches=1, upload_concurrency=2)
            try:
                self.assertTrue(projector.both_large.wait(2))
                self.assertTrue(projector.small_done.wait(.5),
                    'FIFO submitted both large parents before admitted small Slack')
                self.assertLessEqual(projector.peak, 2)
            finally:
                projector.release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 10)

    def test_size_order_control_reaches_already_admitted_small_slack(self):
        projector = SampledProjector(sort_control=True)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=10,
                max_batches=1, upload_concurrency=2)
            try:
                self.assertTrue(projector.small_done.wait(2))
                self.assertTrue(projector.both_large.wait(2))
                self.assertFalse(future.done())
                self.assertLessEqual(projector.peak, 2)
            finally:
                projector.release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 10)

    def test_size_order_control_still_cannot_admit_after_both_owners_are_busy(self):
        projector = SampledProjector(sort_control=True)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=10,
                max_batches=2, upload_concurrency=2)
            try:
                self.assertTrue(projector.small_done.wait(2))
                self.assertTrue(projector.both_large.wait(2))
                with projector.lock:
                    projector.queue.append(candidate('later'))
                self.assertFalse(projector.later_done.wait(.5),
                    'non-preemptible owners unexpectedly yielded their capacity')
                self.assertEqual(projector.started['later'], 0)
                self.assertLessEqual(projector.peak, 2)
            finally:
                projector.release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 11)
            self.assertTrue(projector.later_done.is_set())


if __name__ == '__main__':
    unittest.main()
