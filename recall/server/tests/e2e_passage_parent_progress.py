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
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty  # noqa: E402
from recall_server.search_outbox import enqueue_search_outbox, outbox_months  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceError  # noqa: E402


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

    def exercise_external_outbox(self, stage):
        if stage == 'logical':
            self.release.set()
            self.passages.project_pending(batch_size=2, max_batches=1, concurrency=2)
            self.release.clear()
            self.blocked.clear()
            with self.store.connect() as c:
                mark_logical_evidence_dirty(c, tenant_id=self.tenant,
                    source_id=self.sources['large'], native_ids=['large'], reason='ingest')
        else:
            small = next(candidate for candidate in self.passages._pending(tenant_id=self.tenant, limit=2)
                if candidate.source_id == self.sources['small'])
            self.passages._commit(self.passages._prepare(small))
            with self.store.connect() as c:
                c.execute('DELETE FROM search_projection_outbox WHERE tenant_id=%s', (self.tenant,))
        owners = set()
        original_publish = self.publish
        def publish():
            owners.add(threading.get_ident())
            return original_publish()
        self.publish = publish
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                # Simulate an already committed repair whose remote hint is
                # absent. Only the ordinary worker's fake external writer runs.
                for namespace in self.client.namespaces.values():
                    namespace.delete_all()
                self.published.clear()
                self.assertFalse(self.search())
                with self.store.connect() as c:
                    spans = c.execute("""SELECT first_occurred_at,last_occurred_at FROM canonical_passages
                        WHERE tenant_id=%s AND source_id=%s""",
                        (self.tenant, self.sources['small'])).fetchall()
                    self.assertTrue(spans)
                    enqueue_search_outbox(c, tenant_id=self.tenant, source_id=self.sources['small'],
                        months=outbox_months((row['first_occurred_at'], row['last_occurred_at']) for row in spans),
                        reason='backfill')
                self.assertTrue(self.published.wait(7),
                    'new committed outbox work waited for an unrelated busy owner')
                self.assertFalse(future.done())
                self.assertTrue(any(self.receipts['small'] in item.get('receipts', ())
                    for row in self.search() for item in row['matching_ranges']))
                with self.store.connect() as c:
                    remaining = c.execute("""SELECT count(*) AS n FROM search_projection_outbox
                        WHERE tenant_id=%s AND source_id=%s""",
                        (self.tenant, self.sources['small'])).fetchone()['n']
                self.assertEqual(remaining, 0)
            finally:
                self.release.set()
            future.result(timeout=10)
        self.assertEqual(len(owners), 1)

    def test_external_outbox_publishes_while_only_passage_owner_is_busy(self):
        self.exercise_external_outbox('passage')

    def test_external_outbox_publishes_while_only_logical_owner_is_busy(self):
        self.exercise_external_outbox('logical')

    def test_logical_heartbeat_does_not_requeue_its_busy_archive_rebuild(self):
        heartbeat = threading.Event()
        read_part = self.projection.read_part
        def missing_large(reference, **kwargs):
            if kwargs.get('source_id') == self.sources['large'] and not self.release.is_set():
                raise LogicalEvidenceError('logical_evidence_not_found')
            return read_part(reference, **kwargs)
        self.projection.read_part = missing_large
        prepare = self.logical._prepare_batch_and_upload
        def hold_before_rebuild(candidates, **kwargs):
            if any(item.source_id == self.sources['large'] for item in candidates):
                self.blocked.set()
                if not self.release.wait(15):
                    raise AssertionError('test did not release archive rebuild')
            return prepare(candidates, **kwargs)
        self.logical._prepare_batch_and_upload = hold_before_rebuild
        original_publish = self.publish
        def publish():
            result = original_publish()
            if self.blocked.is_set():
                heartbeat.set()
            return result
        self.publish = publish
        def generation():
            with self.store.connect() as c:
                return c.execute("""SELECT generation FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                    (self.tenant, self.sources['large'], 'large')).fetchone()['generation']
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                pinned_generation = generation()
                self.assertTrue(heartbeat.wait(7))
                self.assertFalse(future.done())
                self.assertEqual(generation(), pinned_generation,
                    'heartbeat invalidated the logical rebuild by requeuing its missing archive')
            finally:
                self.release.set()
            future.result(timeout=10)


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageProgress(name) for name in PassageProgress.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
