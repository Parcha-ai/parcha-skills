#!/usr/bin/env python3
"""Real PostgreSQL owner progress and authority during unfinished TP months."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import threading
import unittest
from e2e_logical_parent_progress import ParentProgress, insert_record
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.search_outbox import enqueue_search_outbox


class CooperativePublication(ParentProgress):
    def setUp(self):
        super().setUp()
        with self.store.connect() as c:
            for index in range(2):
                parent = f'large-extra-{index}'
                insert_record(c, tenant=self.tenant, source=self.sources['large'],
                    parent=parent, native=parent, text='Synthetic giant page.', role='user', byte_start=0)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources['large'],
                    native_ids=[parent], reason='ingest')
        self.release.set()
        self.logical.project_pending(batch_size=10, max_batches=1, upload_concurrency=2)
        self.passages.project_pending(batch_size=10, max_batches=1, concurrency=2)
        self.writer = TurbopufferProjector(self.store, self.settings, client=self.client,
            batch_rows=1, write_concurrency=1, tokens_per_minute=0)
        with self.store.connect() as c:
            c.execute("""UPDATE search_projection_outbox SET queued_at=clock_timestamp()-interval '1 hour'
                WHERE tenant_id=%s AND source_id=%s""", (self.tenant, self.sources['large']))

    def quantum(self):
        method = getattr(self.writer, 'drain_quantum', self.writer.drain)
        return method(tenant_id=self.tenant, max_months=2)

    def test_fresh_logical_commit_resumes_before_giant_month_second_page(self):
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=self.sources['small'], parent='fresh', native='fresh',
                text='Synthetic fresh logical work.', role='user', byte_start=100)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources['small'],
                native_ids=['fresh'], reason='ingest')
        held, release, committed = threading.Event(), threading.Event(), threading.Event()
        page = self.writer.passage_page
        def hold_second(connection, claim, **kwargs):
            if claim['source_id'] == self.sources['large'] and kwargs['after'] is not None:
                held.set()
                if not release.wait(12):
                    raise AssertionError('test did not release second giant page')
            return page(connection, claim, **kwargs)
        self.writer.passage_page = hold_second
        commit = self.logical._commit_upload
        def observed_commit(candidate, upload):
            status = commit(candidate, upload)
            if candidate.native_parent_id == 'fresh' and status == 'committed':
                committed.set()
            return status
        self.logical._commit_upload = observed_commit
        owners = set()
        def publish():
            owners.add(threading.get_ident())
            return self.quantum()
        self.publish = publish
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(committed.wait(3),
                    'fresh logical admission waited for the entire giant search month')
                with self.store.connect() as c:
                    self.assertEqual(c.execute('''SELECT count(*) AS n FROM search_projection_shards
                        WHERE tenant_id=%s AND source_id=%s''',
                        (self.tenant, self.sources['large'])).fetchone()['n'], 0)
            finally:
                release.set()
            future.result(timeout=15)
        self.assertEqual(len(owners), 1)

    def test_replayed_vendor_rows_do_not_bypass_forget_or_current_document(self):
        self.quantum()
        with self.store.connect() as c:
            self.assertEqual(c.execute('''SELECT count(*) AS n FROM search_projection_shards
                WHERE tenant_id=%s''', (self.tenant,)).fetchone()['n'], 0)
        # Restart in-memory progress; same vendor rows can be replayed. Current
        # source authority still rejects an invalidated canonical document.
        self.writer = TurbopufferProjector(self.store, self.settings, client=self.client,
            batch_rows=1, write_concurrency=1, tokens_per_minute=0)
        for _ in range(8):
            self.quantum()
        def small_receipt_visible():
            return any(self.receipts['small'] in item.get('receipts', ())
                for row in self.search() for item in row['matching_ranges'])
        self.assertTrue(small_receipt_visible())
        with self.store.connect() as c:
            c.execute('''UPDATE canonical_documents SET is_current=false
                WHERE tenant_id=%s AND source_id=%s''', (self.tenant, self.sources['small']))
            enqueue_search_outbox(c, tenant_id=self.tenant, source_id=self.sources['small'],
                months=[date(2026,7,1)], reason='header-change')
        self.quantum()
        self.assertFalse(small_receipt_visible())
        with self.store.connect() as c:
            c.execute('''UPDATE canonical_documents SET is_current=true
                WHERE tenant_id=%s AND source_id=%s''', (self.tenant, self.sources['small']))
            c.execute('''UPDATE canonical_chunks SET deleted_at=clock_timestamp()
                WHERE tenant_id=%s AND source_id=%s''', (self.tenant, self.sources['small']))
            enqueue_search_outbox(c, tenant_id=self.tenant, source_id=self.sources['small'],
                months=[date(2026,7,1)], reason='forget')
        self.quantum()
        self.assertFalse(small_receipt_visible())


if __name__ == '__main__':
    suite = unittest.TestSuite(CooperativePublication(name) for name in CooperativePublication.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
