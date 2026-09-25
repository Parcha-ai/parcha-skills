"""Independent parents must publish before unrelated preparation finishes."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
import threading
from types import SimpleNamespace
import unittest

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, LogicalGroupCandidate
from recall_server.projection_worker import run_projection_worker


class Store:
    pool_max_size = 2

    def connect(self):
        return nullcontext(self)

    def execute(self, *_args):
        return SimpleNamespace(fetchone=lambda: dict(queued=0, waiting=0, quarantined=0, backoff=0))


class ParentProgressTests(unittest.TestCase):
    def test_small_parent_commits_while_large_parent_is_blocked(self):
        blocked, release, committed = threading.Event(), threading.Event(), threading.Event()

        class Projector(CanonicalLogicalEvidenceProjector):
            def _check_candidate_current(self, candidate):
                # Scheduler-only fake; real queue authority is covered by PostgreSQL tests.
                return None

            def _pending(self, **kwargs):
                return [LogicalGroupCandidate('tenant:test', 'source:test', name,
                    datetime.now(timezone.utc), 1, 1, estimated_bytes=size)
                    for name, size in [('large', 100), ('small', 1)]]

            def drain_cleanup(self, **kwargs):
                return dict(deleted=0, failures=0, completed=0, pending=0)

            def _prepare_batch_and_upload(self, candidates, **kwargs):
                if candidates[0].native_parent_id == 'large':
                    blocked.set()
                    if not release.wait(5):
                        raise AssertionError('large parent was not released')
                return [SimpleNamespace(all_references=(), prepared=SimpleNamespace(record_count=1, receipt_count=1)) for _ in candidates]

            def _commit_upload(self, candidate, upload):
                if candidate.native_parent_id == 'small':
                    committed.set()
                return 'committed'

        projector = Projector(Store(), None, bound_tenant_id='tenant:test')
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(projector.project_pending, batch_size=2, max_batches=1, upload_concurrency=2)
            try:
                self.assertTrue(blocked.wait(2))
                self.assertTrue(committed.wait(1), 'small parent waited for unrelated large preparation')
                self.assertFalse(future.done())
            finally:
                release.set()
            self.assertEqual(future.result()['documents'], 2)

    def test_worker_publishes_progress_with_one_downstream_owner(self):
        calls, owners = [], set()
        logical_running = False
        class Logical:
            def project_pending(self, **kwargs):
                nonlocal logical_running
                logical_running = True
                kwargs['on_progress']()
                self.assert_published = 'search-ready' in calls
                logical_running = False
                return dict(status='complete', documents=1, records=1, pruned=0, cleanup_failures=0)
        class Passages:
            def project_pending(self, **kwargs):
                owners.add(threading.get_ident())
                self.budget = kwargs
                calls.append('passages-ready' if logical_running else 'passages-initial')
                return dict(status='complete', documents=int(logical_running), passages=int(logical_running), stale=0)
        def search():
            owners.add(threading.get_ident())
            calls.append('search-ready' if logical_running else 'search-initial')
            return dict(status='complete', months=int(logical_running), rows=int(logical_running), deleted=0, failed=0)
        logical, passages = Logical(), Passages()
        result = run_projection_worker(logical, passages, tenant_id='tenant:test',
            logical_batch_size=2, passage_batch_size=3, embedding_batch_size=4,
            max_batches_per_cycle=1, upload_concurrency=2, passage_concurrency=2,
            interval_seconds=1, once=True, skip_embedding=True, search_plane=search)
        self.assertTrue(logical.assert_published)
        self.assertEqual(owners, {threading.get_ident()})
        self.assertEqual(passages.budget['batch_size'], 3)
        self.assertEqual(passages.budget['max_batches'], 1)
        self.assertEqual(passages.budget['concurrency'], 2)
        self.assertEqual(result['passage_documents'], 1)
        self.assertEqual(result['search_plane_rows'], 1)

    def test_failed_publication_keeps_prior_logical_and_tick_timings(self):
        now = [0.0]
        class Logical:
            def project_pending(self, **kwargs):
                now[0] += 5
                kwargs['on_progress']()
        class Passages:
            def project_pending(self, **kwargs):
                now[0] += 2
                return dict(status='complete', documents=0, passages=0, stale=0)
        searches = 0
        def search():
            nonlocal searches
            searches += 1
            now[0] += 3
            if searches == 2:
                raise RuntimeError('synthetic publication failure')
            return dict(status='complete', months=0, rows=0, deleted=0, failed=0)
        with self.assertLogs('recall_server.projection_worker', level='ERROR') as logged:
            with self.assertRaises(RuntimeError):
                run_projection_worker(Logical(), Passages(), tenant_id='tenant:test',
                    logical_batch_size=2, passage_batch_size=3, embedding_batch_size=4,
                    max_batches_per_cycle=1, upload_concurrency=2, passage_concurrency=2,
                    interval_seconds=1, once=True, skip_embedding=True, search_plane=search,
                    clock=lambda: now[0])
        for expected in ('failed_phase=search_plane', 'logical_elapsed_ms=5000',
                         'passage_elapsed_ms=4000', 'search_plane_elapsed_ms=6000',
                         'cycle_elapsed_ms=15000'):
            self.assertIn(expected, logged.output[0])
