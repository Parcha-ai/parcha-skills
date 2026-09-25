#!/usr/bin/env python3
"""Later bounded admission rounds must progress while a giant parent prepares."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
import unittest

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_parent_progress import ParentProgress  # noqa: E402
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty  # noqa: E402
from recall_server.projection_worker import run_projection_worker  # noqa: E402


class StreamingProgress(ParentProgress):
    def test_later_tiny_parents_search_before_large_parent_releases(self):
        complete = threading.Event()
        with self.store.connect() as c:
            for index in range(1,5):
                parent = f'small-{index}'
                receipt = insert_record(c, tenant=self.tenant, source=self.sources['small'],
                    parent=parent, native=parent,
                    text=f'Tiny independent parent {index} publication evidence '
                         + ('smalltinyfinalmarker' if index==4 else f'uniquemarker{index}'),
                    role='user', byte_start=index*100)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources['small'],
                    native_ids=[parent], reason='ingest')
                if index==4:
                    final_receipt = receipt
        publication_owners = set()
        def publish():
            publication_owners.add(threading.get_ident())
            result = self.writer.drain(tenant_id=self.tenant, max_months=4)
            hits = self.retrieval.search('smalltinyfinalmarker', lexical_query='smalltinyfinalmarker',
                since=None, until=None, limit=10)['results']
            # Recent-change admission may publish the last-created parent first.
            # Signal only once every tiny parent has committed, retaining the
            # proof that all later batches finish before the giant releases.
            with self.store.connect() as c:
                count = c.execute('''SELECT count(*) AS n FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s''',
                    (self.tenant,self.sources['small'])).fetchone()['n']
            if count == 5 and any(final_receipt in item.get('receipts', ()) and 'smalltinyfinalmarker' in item['text']
                   for row in hits for item in row['matching_ranges']):
                complete.set()
            return result
        def worker():
            return run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
                logical_batch_size=2, passage_batch_size=2, embedding_batch_size=2,
                max_batches_per_cycle=3, upload_concurrency=2, passage_concurrency=2,
                interval_seconds=1, once=True, skip_embedding=True, search_plane=publish)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(worker)
            try:
                self.assertTrue(self.blocked.wait(5), 'giant parent was not admitted')
                self.assertTrue(complete.wait(3),
                    'later tiny parents stayed behind the first admission batch barrier')
                self.assertFalse(future.done())
                with self.store.connect() as c:
                    count = c.execute('''SELECT count(*) AS n FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s''',
                        (self.tenant,self.sources['small'])).fetchone()['n']
                self.assertEqual(count, 5)
            finally:
                self.release.set()
            result = future.result(timeout=15)
        self.assertEqual(result['documents'], 6)
        self.assertEqual(len(publication_owners), 1)

    def test_new_source_arrives_during_giant_and_forget_keeps_priority(self):
        idle, ready = threading.Event(), threading.Event()
        started = []
        original_pending = self.logical._pending
        original_prepare = self.logical._prepare_batch_and_upload
        source = 'codex:newly-active'
        def pending(**kwargs):
            rows = original_pending(**kwargs)
            if len(rows) == 1 and rows[0].native_parent_id == 'large':
                idle.set()
            return rows
        def prepare(candidates, **kwargs):
            started.extend(item.native_parent_id for item in candidates)
            return original_prepare(candidates, **kwargs)
        def progress():
            with self.store.connect() as c:
                count = c.execute("""SELECT count(*) AS n FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s""", (self.tenant, source)).fetchone()['n']
            if count == 2:
                ready.set()
        self.logical._pending = pending
        self.logical._prepare_batch_and_upload = prepare
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.logical.project_pending, batch_size=2, max_batches=2,
                upload_concurrency=2, on_progress=progress)
            try:
                self.assertTrue(self.blocked.wait(5))
                self.assertTrue(idle.wait(3), 'coordinator did not check spare capacity')
                with self.store.connect() as c:
                    insert_source(c, self.tenant, 'principal:test', source)
                    for name, reason in [('ordinary', 'ingest'), ('forget-priority', 'forget')]:
                        insert_record(c, tenant=self.tenant, source=source, parent=name, native=name,
                            text='Synthetic newly eligible source evidence.', role='user', byte_start=0)
                        mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=source,
                            native_ids=[name], reason=reason)
                self.assertTrue(ready.wait(7), 'new source waited for unrelated giant')
                self.assertFalse(future.done())
                self.assertLess(started.index('forget-priority'), started.index('ordinary'))
                self.assertEqual(started.count('large'), 1)
            finally:
                self.release.set()
            result = future.result(timeout=10)
        self.assertEqual(result['documents'], 4)
        self.assertEqual(result['batches'], 2)


if __name__ == '__main__':
    suite = unittest.TestSuite(StreamingProgress(name) for name in StreamingProgress.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
