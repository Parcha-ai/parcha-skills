#!/usr/bin/env python3
"""Real admission priority and finite-batch execution order with held large owners."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest

from e2e_logical_admission_sharing import AdmissionSharing, insert_record, mark_logical_evidence_dirty


class SmallWorkOrdering(AdmissionSharing):
    def seed(self, rows):
        for name, size, age, reason in rows:
            self.add(name, age=age)
            with self.store.connect() as c:
                insert_record(c, tenant=self.tenant, source=self.source, parent=name,
                    native=name+'-body', text='Synthetic '+('x'*size), role='assistant', byte_start=100)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                    native_ids=[name], reason='ingest')
        self.assertEqual(self.logical.project_pending(batch_size=len(rows), max_batches=1,
            upload_concurrency=2)['documents'], len(rows))
        for name, _size, age, reason in rows:
            with self.store.connect() as c:
                insert_record(c, tenant=self.tenant, source=self.source, parent=name,
                    native=name+'-append', text='Synthetic newly appended turn.', role='user', byte_start=200)
                mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                    native_ids=[name], reason=reason)
                c.execute('''UPDATE canonical_evidence_document_queue
                    SET first_queued_at=clock_timestamp()-%s*interval '1 second',
                        changed_at=clock_timestamp()-%s*interval '1 second'
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''',
                    (age, age, self.tenant, self.source, name))
        # Model the first oldest-first admission of a fresh worker; seeding
        # used another process and must not consume its recent/old turn.
        self.logical = self.projector()
        self.selected.clear()
        prepare = self.logical._prepare_batch_and_upload
        def record(candidates, **kwargs):
            self.selected.extend(item.native_parent_id for item in candidates)
            return prepare(candidates, **kwargs)
        self.logical._prepare_batch_and_upload = record

    def test_forget_first_stable_cost_ties_and_unchanged_selected_set(self):
        self.seed([('old-large', 20000, 1000, 'ingest'),
                   ('forget', 30000, 10, 'forget'),
                   ('tie-a', 40, 900, 'ingest'), ('tie-b', 40, 800, 'ingest'),
                   ('outside', 1, 700, 'ingest')])
        admitted = self.logical._pending(tenant_id=self.tenant, limit=4)
        self.assertEqual([c.native_parent_id for c in admitted], ['forget', 'old-large', 'tie-a', 'tie-b'])
        by_name = {c.native_parent_id: c for c in admitted}
        self.assertEqual(by_name['tie-a'].estimated_bytes, by_name['tie-b'].estimated_bytes)
        self.logical.project_pending(batch_size=4, max_batches=1, upload_concurrency=1)
        self.assertEqual(self.selected, ['forget', 'old-large', 'tie-a', 'tie-b'])
        self.assertEqual(set(self.selected), set(by_name))
        self.assertEqual([c.native_parent_id for c in self.logical._pending(tenant_id=self.tenant, limit=10)], ['outside'])

    def test_small_commits_while_large_preparations_are_held_then_giants_finish(self):
        self.seed([('large-a', 20000, 1000, 'ingest'),
                   ('large-b', 10000, 900, 'ingest'), ('small', 1, 800, 'ingest')])
        both_large, release, small_done = threading.Event(), threading.Event(), threading.Event()
        active, peak, lock = set(), [0], threading.Lock()
        prepare = self.logical._prepare_batch_and_upload
        commit = self.logical._commit_upload
        def held_prepare(candidates, **kwargs):
            item, = candidates
            with lock:
                active.add(item.native_parent_id)
                peak[0] = max(peak[0], len(active))
                if {'large-a', 'large-b'} <= active:
                    both_large.set()
            uploads = prepare(candidates, **kwargs)
            if item.native_parent_id.startswith('large-') and not release.wait(10):
                self.logical._schedule_upload_cleanup([u for u in uploads if u is not None])
                raise AssertionError('fixture did not release large owners')
            return uploads
        def observed_commit(item, upload):
            status = commit(item, upload)
            with lock:
                active.remove(item.native_parent_id)
            if item.native_parent_id == 'small' and status == 'committed':
                small_done.set()
            return status
        self.logical._prepare_batch_and_upload = held_prepare
        self.logical._commit_upload = observed_commit
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.logical.project_pending, batch_size=3,
                max_batches=1, upload_concurrency=2)
            try:
                self.assertTrue(both_large.wait(3))
                self.assertTrue(small_done.wait(.5), 'admitted small work waited for large owners')
                self.assertFalse(future.done())
                self.assertLessEqual(peak[0], 2)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)['documents'], 3)
        self.assertEqual(self.logical._pending(tenant_id=self.tenant, limit=10), [])

    def test_oldest_nominee_is_not_perpetually_cost_deferred(self):
        self.seed([('old-large', 20000, 1000, 'ingest'), ('small-0', 1, 800, 'ingest')])
        original_commit = self.logical._commit_upload
        deadline = [None]
        def finite_commit(candidate, upload):
            status = original_commit(candidate, upload)
            # Model one acknowledged owner using the remaining finite cycle.
            # No production query or admission policy is replaced.
            remaining = deadline[0] - time.monotonic()
            if remaining > 0: time.sleep(remaining + .02)
            return status
        self.logical._commit_upload = finite_commit
        for index in range(4):
            if index:
                self.add('small-' + str(index), age=100)
            nominations = self.logical._pending(tenant_id=self.tenant, limit=2,
                prefer_recent=self.logical._prefer_recent_admission)
            self.assertIn('old-large', [c.native_parent_id for c in nominations])
            deadline[0] = time.monotonic() + 1.0
            result = self.logical.project_pending(batch_size=2, max_batches=1,
                upload_concurrency=1, admission_deadline_at=deadline[0])
            self.assertEqual(result['failed'], 0)
            self.assertEqual(result['documents'], 1, 'fixture must admit exactly one real owner')
            if 'old-large' in self.selected: break
        self.assertIn('old-large', self.selected,
            'oldest eligible source nominee was repeatedly displaced by cheaper arrivals')


if __name__ == '__main__':
    suite = unittest.TestSuite(SmallWorkOrdering(name) for name in SmallWorkOrdering.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
