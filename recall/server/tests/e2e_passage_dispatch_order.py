#!/usr/bin/env python3
"""Selected small passages must retain priority after joining archive parts."""
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch

from e2e_logical_parent_progress import ParentProgress, TurbopufferHintRetrieval, DEFAULT_PASSAGE_POLICY
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty


class PassageDispatch(ParentProgress):
    def setUp(self):
        super().setUp()
        self.release.set()
        with self.store.connect() as c:
            # Replace only this fixture's pending small parent with a Slack source.
            c.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                (self.tenant, self.sources['small']))
            self.sources['small'] = 'slack:dispatch'
            insert_source(c, self.tenant, 'principal:test', self.sources['small'])
            self.receipts['small'] = insert_record(c, tenant=self.tenant,
                source=self.sources['small'], parent='small', native='small',
                text='Synthetic small fresh publication evidence.', role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant,
                source_id=self.sources['small'], native_ids=['small'], reason='ingest')
            for parent, count in [('large', 400), ('large-two', 200)]:
                native = parent + '-body'
                insert_record(c, tenant=self.tenant, source=self.sources['large'],
                    parent=parent, native=native, text=('Synthetic large body evidence. ' * count),
                    role='user', byte_start=100)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant,
                    source_id=self.sources['large'], native_ids=[native], reason='ingest')
        put = self.projection.put_records
        with patch.object(self.projection, 'put_records',
                lambda **kwargs: put(**dict(kwargs, part_bytes=4096))):
            result = self.logical.project_pending(batch_size=3, max_batches=1, upload_concurrency=2)
        self.assertEqual(result['documents'], 3)
        self.release.clear()
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=list(self.sources.values()), policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=self.settings, client=self.client)
        with self.store.connect() as c:
            rows = c.execute('SELECT native_parent_id,logical_document_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            self.ids = {row['native_parent_id']: row['logical_document_id'] for row in rows}
            sizes = {row['logical_document_id']: row for row in c.execute(
                '''SELECT logical_document_id,sum(size_bytes) AS bytes,count(*) AS parts
                   FROM canonical_evidence_document_parts WHERE tenant_id=%s
                   GROUP BY logical_document_id''', (self.tenant,)).fetchall()}
            self.assertGreater(sizes[self.ids['large']]['bytes'], sizes[self.ids['large-two']]['bytes'])
            self.assertGreater(sizes[self.ids['large-two']]['bytes'], sizes[self.ids['small']]['bytes'])
            self.assertGreater(sizes[self.ids['large']]['parts'], 1)
            # All three are in the fresh (<five-minute) class. Giants are older.
            for parent, minutes in [('large', 3), ('large-two', 2), ('small', 1)]:
                c.execute("UPDATE canonical_passage_projection_queue SET changed_at=clock_timestamp()-%s*interval '1 minute' WHERE tenant_id=%s AND logical_document_id=%s",
                    (minutes, self.tenant, self.ids[parent]))

    def candidates(self, limit=3):
        self.passages._prefer_notification_admission = False
        return self.passages._pending(tenant_id=self.tenant, limit=limit)

    def test_same_age_class_small_slack_publishes_before_two_held_giants(self):
        both_held = threading.Event()
        lock = threading.Lock()
        held = set()
        prepare = self.passages._prepare
        def blocked(candidate):
            if candidate.source_id == self.sources['large']:
                with lock:
                    held.add(candidate.logical_document_id)
                    if len(held) == 2:
                        both_held.set()
                if not self.release.wait(15):
                    raise AssertionError('large passage preparation not released')
            return prepare(candidate)
        with patch.object(self.passages, '_prepare', blocked), ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.passages.project_pending, batch_size=3,
                max_batches=1, concurrency=2, on_progress=self.publish)
            try:
                self.assertTrue(both_held.wait(5), 'both large parents were not admitted')
                self.assertTrue(self.published.wait(3),
                    'selected Slack waited behind older large parents in the same age class')
                self.assertFalse(future.done())
                self.assertTrue(any(self.receipts['small'] in item.get('receipts', ())
                    for row in self.search() for item in row['matching_ranges']))
            finally:
                self.release.set()
            self.assertEqual(future.result(timeout=10)['documents'], 3)

    def test_exact_selected_set_size_order_and_multipart_order(self):
        candidates = self.candidates()
        self.assertEqual([c.logical_document_id for c in candidates],
            [self.ids['small'], self.ids['large-two'], self.ids['large']])
        self.assertEqual({c.logical_document_id for c in self.candidates(limit=2)},
            {self.ids['small'], self.ids['large']})  # Oldest head per source, then cost dispatch.
        for candidate in candidates:
            self.passages._prepare(candidate)  # Real decoder validates multipart order/hashes.

    def test_aged_large_parent_retains_priority_over_fresh_small(self):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_passage_projection_queue SET changed_at=clock_timestamp()-interval '10 minutes' WHERE tenant_id=%s AND logical_document_id=%s",
                (self.tenant, self.ids['large']))
        self.assertEqual(self.candidates()[0].logical_document_id, self.ids['large'])
        self.assertEqual(self.candidates(limit=1)[0].logical_document_id, self.ids['large'])


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageDispatch(name) for name in PassageDispatch.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
