#!/usr/bin/env python3
"""Actual projection owners must admit fresh logical work before draining passages."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore, PART_MEDIA_TYPE
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projection_worker import run_projection_worker
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class CallbackQuantum(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=8)
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_fresh_source_publishes_before_backlog_drains_then_all_sources_drain(self):
        tenant = 'tenant:quantum:' + uuid.uuid4().hex
        old_source, fresh_source = 'codex:backlog', 'codex:fresh'
        blocked, release, fresh_published = threading.Event(), threading.Event(), threading.Event()
        part_lock = threading.Lock()
        old_parts = set()
        armed = False

        class Archive(FilesystemArchiveStore):
            def read_raw(self, value):
                if armed and value['source_id'] == old_source and value['media_type'] == PART_MEDIA_TYPE:
                    with part_lock:
                        old_parts.add(value['object_key'])
                        should_block = len(old_parts) > 2
                    if should_block:
                        blocked.set()
                        if not release.wait(15):
                            raise AssertionError('test did not release archive read')
                return super().read_raw(value)

        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(Path(directory), namespace_key=b'p'*32)
            projection = LogicalEvidenceProjectionStore(archive)
            logical = CanonicalLogicalEvidenceProjector(self.store, projection,
                bound_tenant_id=tenant, raw_archive=archive)
            passages = CanonicalPassageProjector(self.store, projection,
                policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=tenant)
            with self.store.connect() as connection:
                for source in (old_source, fresh_source):
                    insert_source(connection, tenant, 'principal:test', source)
                for index in range(6):
                    name = f'backlog-{index}'
                    insert_record(connection, tenant=tenant, source=old_source,
                        parent=name, native=name, text=f'Synthetic backlog publication evidence {index}.',
                        role='user', byte_start=0)
                    mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=old_source,
                        native_ids=[name], reason='ingest')
            seeded = logical.project_pending(tenant_id=tenant, batch_size=100, max_batches=2,
                upload_concurrency=2, quiet_seconds=0, max_wait_seconds=0)
            self.assertEqual(seeded['documents'], 6)
            client = FakeTurbopuffer()
            settings = TurbopufferSettings(api_key='synthetic-local-only')
            writer = TurbopufferProjector(self.store, settings, client=client)
            retrieval = TurbopufferHintRetrieval(self.store, tenant_id=tenant,
                sources=[old_source, fresh_source], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
                settings=settings, client=client)
            inserted = False
            inserted_at = None
            published_at = None

            def search():
                return retrieval.search('fresh publication evidence', lexical_query='publication evidence',
                    since=None, until=None, limit=20)['results']

            def publish():
                nonlocal inserted, inserted_at, published_at
                tick = writer.drain(tenant_id=tenant, max_months=4)
                if not inserted and tick.get('rows', 0):
                    # New work arrives after the initial passage callback began.
                    with self.store.connect() as connection:
                        insert_record(connection, tenant=tenant, source=fresh_source,
                            parent='fresh', native='fresh', text='Synthetic fresh publication evidence.',
                            role='user', byte_start=0)
                        mark_logical_evidence_dirty(connection, tenant_id=tenant, source_id=fresh_source,
                            native_ids=['fresh'], reason='ingest')
                    inserted = True
                    inserted_at = time.monotonic()
                if any(row['source_id'] == fresh_source for row in search()):
                    published_at = published_at or time.monotonic()
                    fresh_published.set()
                return tick

            def worker(**options):
                return run_projection_worker(logical, passages, tenant_id=tenant,
                    logical_batch_size=500, passage_batch_size=100, embedding_batch_size=64,
                    max_batches_per_cycle=2, upload_concurrency=2, passage_concurrency=2,
                    interval_seconds=1, skip_embedding=True, search_plane=publish, **options)

            armed = True
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(worker, once=True)
                try:
                    self.assertTrue(blocked.wait(5), 'backlog did not reach the archive boundary')
                    self.assertTrue(fresh_published.wait(3),
                        'fresh source was not admitted while later backlog archive reads were blocked')
                    self.assertFalse(release.is_set())
                    self.assertIsNotNone(inserted_at)
                    self.assertLess(published_at - inserted_at, 5)
                finally:
                    release.set()
                future.result(timeout=10)
            sleeps = []
            worker(max_cycles=4, sleep=sleeps.append)
            found = search()
            self.assertEqual(len(found), 7)
            self.assertEqual(sum(row['source_id'] == old_source for row in found), 6)
            self.assertEqual(sum(row['source_id'] == fresh_source for row in found), 1)
            with self.store.connect() as connection:
                self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s',
                    (tenant,)).fetchone()['n'], 0)
            self.assertLess(len(sleeps), 3, 'busy passage drain slept between every cycle')


if __name__ == '__main__':
    unittest.main()
