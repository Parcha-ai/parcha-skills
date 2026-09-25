"""Real worker/scan loop returns to arrivals between busy source-month builds."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import date
import unittest

from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_parquet_scan import _WindowProbe, _candidate
from tests.central_brain.test_projection_worker import _Logical, _Passages


class ScanAdmissionBoundaryTests(unittest.TestCase):
    def run_worker(self, *, busy):
        events = []
        state = dict(arrived=False, logical=False, passage=False, lease=False)

        class Scan(_WindowProbe):
            def __init__(self):
                super().__init__()
                self.queue = [replace(_candidate(), bucket_start=date(2026, month, 1))
                              for month in range(1, 5)]
                self.original = list(self.queue)
                self.completed = []
                self.bounds = []

            def project_pending(self, **kwargs):
                self.bounds.append(kwargs)
                return super().project_pending(**kwargs)

            def _pending(self, *, tenant_id, limit):
                return self.queue[:limit]

            @contextmanager
            def _candidate_lease(self, candidate):
                state['lease'] = True
                try:
                    yield True
                finally:
                    state['lease'] = False

            def _build(self, candidate, **kwargs):
                # One complete build is still non-preemptible. The change must
                # not claim admission while that first build owns its lease.
                if not self.completed:
                    assert not state['logical']
                return super()._build(candidate, **kwargs)

            def _commit(self, candidate, upload):
                assert state['lease']
                self.completed.append(candidate)
                self.queue.remove(candidate)
                events.append('scan' + str(len(self.completed)))
                if len(self.completed) == 1:
                    state['arrived'] = True
                    events.append('arrival')
                return 'committed'

        class Logical(_Logical):
            def project_pending(self, **kwargs):
                assert not state['lease'], 'upstream work ran inside a scan lease'
                row = super().project_pending(**kwargs)
                if state['arrived'] and not state['logical']:
                    state['logical'] = True
                    events.append('fresh-logical')
                    kwargs['on_progress']()
                    row['documents'] = 1
                return row

        class Passages(_Passages):
            def project_pending(self, **kwargs):
                if state['logical'] and not state['passage']:
                    state['passage'] = True
                    events.append('fresh-passage')
                return super().project_pending(**kwargs)

        def search():
            if state['passage']:
                events.append('fresh-search')
            return dict(status='complete', months=0, rows=0, deleted=0, failed=0)

        scan = Scan()
        run_projection_worker(Logical([], work=0, pending=int(busy)), Passages([], work=0), scan,
            tenant_id='tenant:test', logical_batch_size=500, passage_batch_size=10,
            embedding_batch_size=1, max_batches_per_cycle=2, upload_concurrency=2,
            passage_concurrency=2, interval_seconds=1, max_cycles=2 if busy else 1,
            parquet_every_cycles=1, sleep=lambda _: None, skip_embedding=True,
            search_plane=search)
        return scan, events

    def test_arrival_after_first_busy_build_publishes_before_second_build(self):
        scan, events = self.run_worker(busy=True)
        self.assertLess(events.index('fresh-logical'), events.index('scan2'), events)
        self.assertLess(events.index('fresh-passage'), events.index('scan2'), events)
        self.assertLess(events.index('fresh-search'), events.index('scan2'), events)
        self.assertEqual(scan.completed, scan.original[:2])
        self.assertEqual(scan.queue, scan.original[2:])
        self.assertTrue(all(bound['batch_size'] == 1 and bound['max_batches'] == 1
                            and bound['compaction_budget'] == 0 for bound in scan.bounds))

    def test_idle_turn_keeps_four_candidate_catchup_budget(self):
        scan, events = self.run_worker(busy=False)
        self.assertEqual(scan.completed, scan.original)
        self.assertEqual(scan.queue, [])
        self.assertNotIn('fresh-logical', events)
        self.assertEqual(scan.bounds[0]['batch_size'], 4)
        self.assertEqual(scan.bounds[0]['max_batches'], 2)
        self.assertEqual(scan.bounds[0]['compaction_budget'], 1)
