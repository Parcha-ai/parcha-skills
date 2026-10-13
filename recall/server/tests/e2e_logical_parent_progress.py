#!/usr/bin/env python3
"""Real PostgreSQL publication and source races with one stalled parent.

Only the external search service is fake; queues, generations, currentness,
logical uploads, passage construction, search writes and authority SQL are real.
"""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import archive_object_count, insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projection_worker import run_projection_worker
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class ParentProgress(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=8)
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.tenant = 'tenant:progress:' + uuid.uuid4().hex
        self.sources = {name: 'codex:' + name for name in ('large', 'small')}
        self.receipts = {}
        with self.store.connect() as c:
            for name, source in self.sources.items():
                insert_source(c, self.tenant, 'principal:test', source)
                self.receipts[name] = insert_record(c, tenant=self.tenant, source=source,
                    parent=name, native=name, text='Synthetic ' + name + ' fresh publication evidence.',
                    role='user', byte_start=0)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=source,
                    native_ids=[name], reason='ingest')
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.archive_root = Path(tmp.name)/'archive'
        archive = FilesystemArchiveStore(self.archive_root, namespace_key=b'p'*32)
        self.projection = LogicalEvidenceProjectionStore(archive)
        self.blocked, self.release, self.published = threading.Event(), threading.Event(), threading.Event()
        outer = self
        class BlockingProjector(CanonicalLogicalEvidenceProjector):
            def _prepare_batch_and_upload(self, candidates, **kwargs):
                uploads = super()._prepare_batch_and_upload(candidates, **kwargs)
                if any(c.native_parent_id == 'large' for c in candidates):
                    outer.blocked.set()
                    if not outer.release.wait(15):
                        self._schedule_upload_cleanup([u for u in uploads if u is not None])
                        raise AssertionError('test did not release blocked parent')
                return uploads
        self.logical = BlockingProjector(self.store, self.projection,
            bound_tenant_id=self.tenant, raw_archive=archive)
        self.passages = CanonicalPassageProjector(self.store, self.projection,
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        self.client = FakeTurbopuffer()
        self.settings = TurbopufferSettings(api_key='synthetic-local-only')
        self.writer = TurbopufferProjector(self.store, self.settings, client=self.client)
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=list(self.sources.values()), policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=self.settings, client=self.client)

    def search(self):
        return self.retrieval.search('small fresh publication evidence', lexical_query='small fresh publication',
            since=None, until=None, limit=10)['results']

    def publish(self):
        result = self.writer.drain(tenant_id=self.tenant, max_months=4)
        if self.search():
            self.published.set()
        return result

    def run_worker(self):
        return run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
            logical_batch_size=2, passage_batch_size=2, embedding_batch_size=2,
            max_batches_per_cycle=1, upload_concurrency=2, passage_concurrency=2,
            interval_seconds=1, once=True, skip_embedding=True, search_plane=self.publish)

    def exercise(self, mutation=None):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(self.published.wait(5), 'small parent is not searchable while large parent is blocked')
                self.assertFalse(future.done())
                found = self.search()
                self.assertTrue(any(row['source_id'] == self.sources['small'] for row in found))
                if mutation:
                    with self.store.connect() as c:
                        if mutation == 'forget':
                            c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s',
                                (self.tenant, self.sources['large']))
                            c.execute('UPDATE canonical_documents SET is_current=false, deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s',
                                (self.tenant, self.sources['large']))
                        else:
                            insert_record(c, tenant=self.tenant, source=self.sources['large'],
                                parent='large', native='new-turn', text='Synthetic later generation.', role='user', byte_start=100)
                        mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources['large'],
                            native_ids=['large'], reason='forget' if mutation == 'forget' else 'ingest')
            finally:
                self.release.set()
            result = future.result(timeout=10)
        if mutation:
            with self.store.connect() as c:
                count = c.execute('SELECT count(*) AS n FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large'])).fetchone()['n']
                queued = c.execute('SELECT generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large'])).fetchone()
            self.assertEqual(count, 0, 'stale upload committed after source mutation')
            self.assertGreater(queued['generation'], 1)
            self.assertEqual(result['documents'], 1)
        else:
            self.assertEqual(result['documents'], 2)

    def test_interrupt_cleans_uncommitted_large_upload_but_keeps_published_small(self):
        original = self.logical._commit_upload
        def interrupt_large(candidate, upload):
            if candidate.native_parent_id == 'large':
                raise KeyboardInterrupt
            return original(candidate, upload)
        self.logical._commit_upload = interrupt_large
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(self.published.wait(5))
            finally:
                self.release.set()
            with self.assertRaises(KeyboardInterrupt):
                future.result(timeout=10)
        self.assertTrue(self.search())
        with self.store.connect() as c:
            parents = c.execute('SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            pending = c.execute('SELECT native_parent_id FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            cleanup = c.execute('SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n']
        self.assertEqual([r['native_parent_id'] for r in parents], ['small'])
        self.assertEqual([r['native_parent_id'] for r in pending], ['large'])
        self.assertEqual(cleanup, 0)
        self.assertEqual(archive_object_count(self.archive_root), 2)

    def test_publication_failure_leaves_every_upload_committed_or_cleaned(self):
        original = self.publish
        def fail_after_publish():
            result = original()
            if self.published.is_set():
                raise RuntimeError('synthetic publication failure')
            return result
        self.publish = fail_after_publish
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(self.published.wait(5))
            finally:
                self.release.set()
            with self.assertRaisesRegex(RuntimeError, 'synthetic publication failure'):
                future.result(timeout=10)
        self.assertTrue(self.search())
        with self.store.connect() as c:
            parents = c.execute('SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            pending = c.execute('SELECT native_parent_id FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            cleanup = c.execute('SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n']
        committed = {r['native_parent_id'] for r in parents}
        self.assertIn('small', committed)
        self.assertEqual(committed | {r['native_parent_id'] for r in pending}, {'small', 'large'})
        self.assertEqual(cleanup, 0)
        self.assertEqual(archive_object_count(self.archive_root), 2 * len(committed))

    def test_small_parent_searches_before_large_parent_finishes(self):
        self.exercise()

    def test_update_during_large_upload_retains_new_generation(self):
        self.exercise('update')

    def test_forget_during_large_upload_cannot_publish_stale_evidence(self):
        self.exercise('forget')


if __name__ == '__main__':
    unittest.main()
