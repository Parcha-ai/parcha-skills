"""Passage owners keep bodies bounded and preserve commit pool headroom."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import threading
from types import SimpleNamespace
import unittest
import weakref
from unittest.mock import patch

from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy


class Store:
    pool_max_size = 3

    def connect(self):
        return nullcontext(self)

    def execute(self, *_args):
        return SimpleNamespace(fetchone=lambda: {'count': 0})


class Prepared:
    def __init__(self, candidate):
        self.candidate = candidate


class PassageOwnerTests(unittest.TestCase):
    def test_prepared_bodies_are_bounded_and_commits_reserve_pool_headroom(self):
        release, capped = threading.Event(), threading.Event()
        lock = threading.Lock()
        counts = dict(live=0, peak_live=0, commits=0, peak_commits=0)
        def freed():
            with lock:
                counts['live'] -= 1
        class Projector(CanonicalPassageProjector):
            def _pending(self, **kwargs):
                return [SimpleNamespace(name=n) for n in range(64)]
            def _prepare(self, candidate):
                value = Prepared(candidate)
                weakref.finalize(value, freed)
                with lock:
                    counts['live'] += 1
                    counts['peak_live'] = max(counts['peak_live'], counts['live'])
                return value
            def _commit(self, prepared):
                with lock:
                    counts['commits'] += 1
                    counts['peak_commits'] = max(counts['peak_commits'], counts['commits'])
                    if counts['commits'] == 2:
                        capped.set()
                try:
                    if not release.wait(5):
                        raise AssertionError('test did not release commit owners')
                    return dict(status='complete', inserted=1, deleted=0, retained=0)
                finally:
                    with lock:
                        counts['commits'] -= 1
        projector = Projector(Store(), None, policy=PassagePolicy(target_tokens=256, overlap_tokens=32))
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=64, max_batches=1, concurrency=8)
            try:
                self.assertTrue(capped.wait(3))
                self.assertLessEqual(counts['peak_live'], 8,
                    'whole-batch preparation retained more bodies than active owners')
                self.assertEqual(counts['peak_commits'], 2)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 64)
        self.assertLessEqual(counts['peak_live'], 8)
        self.assertEqual(counts['live'], 0)

    def test_missing_and_unavailable_yield_after_batch_without_blocking_valid_sibling(self):
        calls, owners = [], set()
        class Projector(CanonicalPassageProjector):
            def _pending(self, **kwargs):
                calls.append('pending')
                return [SimpleNamespace(name=name) for name in ('missing', 'unavailable', 'ready')]
            def _prepare(self, candidate):
                if candidate.name != 'ready':
                    raise LogicalEvidenceError('logical_evidence_' +
                        ('not_found' if candidate.name == 'missing' else 'unavailable'))
                return Prepared(candidate)
            def _commit(self, prepared):
                calls.append('commit')
                return dict(status='complete', inserted=1, deleted=0, retained=0)
            def _requeue_missing(self, candidate):
                owners.add(threading.get_ident())
                return 1
        projector = Projector(Store(), None, policy=PassagePolicy(target_tokens=256, overlap_tokens=32))
        result = projector.project_pending(batch_size=3, max_batches=5, concurrency=2,
            on_progress=lambda: owners.add(threading.get_ident()))
        self.assertEqual(calls.count('pending'), 1)
        self.assertEqual(calls.count('commit'), 1)
        self.assertEqual((result['documents'], result['requeued'], result['unavailable']), (1, 1, 1))
        self.assertEqual(owners, {threading.get_ident()})

    def test_publication_failure_cancels_queued_work_before_releasing_owners(self):
        for error in (RuntimeError('publish failed'), KeyboardInterrupt(), TimeoutError('deadline')):
            with self.subTest(error=type(error).__name__):
                release, blocked, settling = threading.Event(), threading.Event(), threading.Event()
                started, committed = [], []
                class Projector(CanonicalPassageProjector):
                    def _pending(self, **kwargs):
                        return [SimpleNamespace(name=name) for name in
                            ('large', 'small', *[f'tiny{n}' for n in range(8)])]
                    def _prepare(self, candidate):
                        started.append(candidate.name)
                        if candidate.name in {'large', 'tiny0'}:
                            if candidate.name == 'large':
                                blocked.set()
                            if not release.wait(5):
                                raise AssertionError('test did not release prepared owner')
                        return Prepared(candidate)
                    def _commit(self, prepared):
                        committed.append(prepared.candidate.name)
                        return dict(status='complete', inserted=1, deleted=0, retained=0)
                class SettlingExecutor(ThreadPoolExecutor):
                    def shutdown(self, *args, **kwargs):
                        settling.set()
                        return super().shutdown(*args, **kwargs)
                def publish():
                    raise error
                projector = Projector(Store(), None, policy=PassagePolicy(target_tokens=256, overlap_tokens=32))
                with patch('recall_server.passage_index.ThreadPoolExecutor', SettlingExecutor), \
                        ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(projector.project_pending, batch_size=10, max_batches=3,
                        concurrency=2, on_progress=publish)
                    try:
                        self.assertTrue(blocked.wait(2))
                        self.assertTrue(settling.wait(2))
                    finally:
                        release.set()
                    with self.assertRaises(type(error)):
                        future.result(timeout=5)
                self.assertEqual(committed, ['small'])
                self.assertLessEqual(len(started), 3)


class WorkerPassageTimingTests(unittest.TestCase):
    def test_search_callbacks_do_not_recurse_or_count_as_passage_wall_time(self):
        from recall_server.projection_worker import run_projection_worker
        from tests.central_brain.test_projection_worker import _Logical
        for fail in (False, True):
            with self.subTest(failed_search=fail):
                now, calls = [0.0], []
                class Passages:
                    def project_pending(self, **kwargs):
                        calls.append('passages')
                        now[0] += 2
                        kwargs['on_progress']()
                        now[0] += 1
                        kwargs['on_progress']()
                        now[0] += 4
                        return dict(status='complete', documents=2, passages=2, stale=0)
                def search():
                    calls.append('search')
                    now[0] += 3
                    if fail and calls.count('search') == 2:
                        raise RuntimeError('synthetic publication timeout')
                    return dict(status='complete', months=1, rows=1, deleted=0, failed=0)
                def run():
                    return run_projection_worker(_Logical(calls, work=0), Passages(),
                        tenant_id='tenant:test', logical_batch_size=2, passage_batch_size=2,
                        embedding_batch_size=2, max_batches_per_cycle=1, upload_concurrency=2,
                        passage_concurrency=2, interval_seconds=1, once=True,
                        skip_embedding=True, search_plane=search, clock=lambda: now[0])
                if fail:
                    with self.assertLogs('recall_server.projection_worker', level='ERROR') as logged:
                        with self.assertRaises(RuntimeError):
                            run()
                    for expected in ('failed_phase=search_plane', 'passage_elapsed_ms=3000',
                                     'search_plane_elapsed_ms=6000', 'cycle_elapsed_ms=9000'):
                        self.assertIn(expected, logged.output[0])
                else:
                    result = run()
                    self.assertEqual(result['passage_elapsed_ms'], 7000)
                    self.assertEqual(result['search_plane_elapsed_ms'], 6000)
                    self.assertEqual(result['cycle_elapsed_ms'], 13000)
                    self.assertEqual(result['search_plane_rows'], 2)
                self.assertEqual(calls.count('passages'), 1)
                self.assertEqual(calls.count('search'), 2)


if __name__ == '__main__':
    unittest.main()
