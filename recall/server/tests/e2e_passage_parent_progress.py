#!/usr/bin/env python3
"""Separate passage commit and worker publication barriers with real PostgreSQL."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
import unittest

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_parent_progress import ParentProgress  # noqa: E402


class PassageProgress(ParentProgress):
    def setUp(self):
        super().setUp()
        # Logical evidence is already authoritative. Only passage preparation
        # is stalled; this cannot be explained by a logical admission barrier.
        self.release.set()
        report = self.logical.project_pending(batch_size=2, max_batches=1, upload_concurrency=2)
        self.assertEqual(report['documents'], 2)
        self.release.clear()
        self.blocked.clear()
        original = self.passages._prepare
        def prepare(candidate, **kwargs):
            prepared = original(candidate, **kwargs)
            if candidate.source_id == self.sources['large']:
                self.blocked.set()
                if not self.release.wait(15):
                    raise AssertionError('test did not release large passage preparation')
            return prepared
        self.passages._prepare = prepare

    def test_small_passage_commits_before_unrelated_large_prepare_finishes(self):
        committed = threading.Event()
        original = self.passages._commit
        def commit(prepared):
            status = original(prepared)
            if prepared.candidate.source_id == self.sources['small'] and status['status'] != 'stale':
                committed.set()
            return status
        self.passages._commit = commit
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.passages.project_pending, batch_size=2, max_batches=1, concurrency=2)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(committed.wait(3),
                    'ready small passage waited for every document in the preparation batch')
                self.assertFalse(future.done())
            finally:
                self.release.set()
            self.assertEqual(future.result(timeout=10)['documents'], 2)

    def test_worker_searches_small_passage_while_large_prepare_is_blocked(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(self.published.wait(3),
                    'worker withheld search publication until the passage batch returned')
                self.assertFalse(future.done())
                hits = self.search()
                self.assertTrue(any(self.receipts['small'] in item.get('receipts', ())
                    for row in hits for item in row['matching_ranges']))
            finally:
                self.release.set()
            self.assertEqual(future.result(timeout=10)['passage_documents'], 2)


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageProgress(name) for name in PassageProgress.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
