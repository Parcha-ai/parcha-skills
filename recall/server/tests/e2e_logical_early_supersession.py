#!/usr/bin/env python3
"""Obsolete admitted work must stop before archive work without becoming empty."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import threading
import unittest
from unittest.mock import patch

import psycopg

from e2e_logical_parent_progress import ParentProgress
from e2e_logical_evidence_projection import insert_record
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty,
)


class EarlySupersession(ParentProgress):
    def setUp(self):
        super().setUp()
        self.release.set()
        self.logical = CanonicalLogicalEvidenceProjector(self.store, self.projection,
            bound_tenant_id=self.tenant, raw_archive=self.projection.archive)
        self.owners = threading.local()
        self.prepared, self.encoded, self.uploaded = [], [], []
        prepare = self.logical._prepare_batch_and_upload
        records = self.logical._record_stream
        upload = self.projection.put_records
        def tracked_prepare(candidates, **kwargs):
            self.owners.parent = candidates[0].native_parent_id
            self.prepared.extend(c.native_parent_id for c in candidates)
            return prepare(candidates, **kwargs)
        def tracked_records(*args, **kwargs):
            self.encoded.append(getattr(self.owners, 'parent', 'direct'))
            return records(*args, **kwargs)
        def tracked_upload(**kwargs):
            self.uploaded.append(kwargs['native_parent_id'])
            return upload(**kwargs)
        for obj, name, replacement in (
            (self.logical, '_prepare_batch_and_upload', tracked_prepare),
            (self.logical, '_record_stream', tracked_records),
            (self.projection, 'put_records', tracked_upload),
        ):
            p = patch.object(obj, name, replacement); p.start(); self.addCleanup(p.stop)

    def mutate(self, kind):
        with self.store.connect() as c:
            if kind == 'forget':
                c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large']))
                c.execute('UPDATE canonical_documents SET is_current=false,deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large']))
            else:
                insert_record(c, tenant=self.tenant, source=self.sources['large'], parent='large',
                    native='later', text='Synthetic later turn.', role='user', byte_start=100)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources['large'],
                native_ids=['large'], reason='forget' if kind == 'forget' else 'ingest')

    def project(self):
        return self.logical.project_pending(batch_size=2, max_batches=1, upload_concurrency=2)

    def assert_superseded(self, result):
        self.assertEqual((result['documents'], result['source_races'], result['failed']), (1, 1, 0))
        self.assertNotIn('large', self.uploaded, 'obsolete parent reached archive upload')
        self.assertNotIn('large', self.encoded, 'obsolete parent reached record encoding')
        self.assertIn('small', self.uploaded)
        with self.store.connect() as c:
            parents = c.execute('SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
            queued = c.execute('SELECT native_parent_id,generation,attempts,next_attempt_at FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,)).fetchall()
        self.assertEqual([r['native_parent_id'] for r in parents], ['small'])
        self.assertEqual(len(queued), 1)
        self.assertEqual((queued[0]['native_parent_id'], queued[0]['generation'], queued[0]['attempts'], queued[0]['next_attempt_at']),
            ('large', 2, 0, None))

    def before_owner(self, kind):
        pending = self.logical._pending
        def supersede_admission(**kwargs):
            rows = pending(**kwargs)
            self.mutate(kind)
            return rows
        with patch.object(self.logical, '_pending', supersede_admission):
            result = self.project()
        self.assert_superseded(result)
        self.assertNotIn('large', self.prepared, 'obsolete admission read its body')

    def after_sql(self, kind):
        read_done, release, small_done = threading.Event(), threading.Event(), threading.Event()
        original_connect, commit = self.store.connect, self.logical._commit_upload
        def observed_commit(candidate, upload):
            outcome = commit(candidate, upload)
            if candidate.native_parent_id == 'small' and outcome == 'committed':
                small_done.set()
            return outcome
        class Cursor:
            def __init__(inner, actual, owner): inner.actual, inner.owner = actual, owner
            @property
            def itersize(inner): return inner.actual.itersize
            @itersize.setter
            def itersize(inner, value): inner.actual.itersize = value
            def execute(inner, sql, params):
                inner.owner.large = 'large' in params[3]
                return inner.actual.execute(sql, params)
            def __iter__(inner): return iter(inner.actual)
        class Connection:
            def __init__(inner, actual): inner.actual, inner.large = actual, False
            def __getattr__(inner, name): return getattr(inner.actual, name)
            def cursor(inner, *args, **kwargs):
                if kwargs.get('name') != 'logical_evidence_batch_stream':
                    return inner.actual.cursor(*args, **kwargs)
                @contextmanager
                def traced():
                    with inner.actual.cursor(*args, **kwargs) as cursor:
                        yield Cursor(cursor, inner)
                return traced()
        @contextmanager
        def paused_after_read():
            with original_connect() as actual:
                wrapped = Connection(actual)
                yield wrapped
            # Deliberately after cursor/connection release, before caller resumes.
            if wrapped.large:
                read_done.set()
                if not release.wait(10): raise AssertionError('SQL checkpoint not released')
        with patch.object(self.store, 'connect', paused_after_read), patch.object(self.logical, '_commit_upload', observed_commit):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.project)
                try:
                    self.assertTrue(read_done.wait(5))
                    self.assertTrue(small_done.wait(5), 'sibling stalled behind SQL checkpoint')
                    self.mutate(kind)
                finally:
                    release.set()
                result = future.result(timeout=10)
        self.assert_superseded(result)

    def test_append_before_owner_skips_body_and_upload(self): self.before_owner('append')
    def test_forget_before_owner_skips_body_and_upload(self): self.before_owner('forget')
    def test_append_after_sql_skips_encoding_and_upload(self): self.after_sql('append')
    def test_forget_after_sql_skips_encoding_and_upload(self): self.after_sql('forget')

    def test_changed_timestamp_without_generation_change_is_stale(self):
        pending = self.logical._pending
        def changed_stamp(**kwargs):
            candidates = pending(**kwargs)
            with self.store.connect() as c:
                c.execute("UPDATE canonical_evidence_document_queue SET changed_at=changed_at+interval '1 second' WHERE tenant_id=%s AND source_id=%s",
                    (self.tenant, self.sources['large']))
            return candidates
        with patch.object(self.logical, '_pending', changed_stamp): result = self.project()
        self.assertEqual((result['documents'], result['source_races'], result['failed']), (1, 1, 0))
        self.assertNotIn('large', self.prepared)
        self.assertNotIn('large', self.uploaded)
        with self.store.connect() as c:
            queued = c.execute('SELECT generation,attempts FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                (self.tenant, self.sources['large'])).fetchone()
        self.assertEqual((queued['generation'], queued['attempts']), (1, 0))

    def test_missing_queue_is_stale_not_empty_projection(self):
        pending = self.logical._pending
        def consumed_queue(**kwargs):
            candidates = pending(**kwargs)
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.sources['large']))
            return candidates
        with patch.object(self.logical, '_pending', consumed_queue): result = self.project()
        self.assertEqual((result['documents'], result['source_races'], result['pruned'], result['failed']), (1, 1, 0, 0))
        self.assertNotIn('large', self.prepared)
        self.assertNotIn('large', self.uploaded)

    def test_direct_multi_candidate_preparation_keeps_existing_semantics(self):
        candidates = tuple(self.logical._pending(tenant_id=self.tenant, limit=2))
        self.mutate('append')
        uploads = self.logical._prepare_batch_and_upload(candidates)
        self.assertEqual(len(uploads), 2)
        self.assertTrue(all(upload is not None for upload in uploads))
        self.assertCountEqual(self.uploaded, ['large', 'small'])
        self.logical._schedule_upload_cleanup(uploads)
        for upload in uploads: upload.body_locators.close()

    def test_queue_read_database_error_remains_failure_not_stale(self):
        original = self.store.connect
        class Connection:
            def __init__(inner, actual): inner.actual = actual
            def __getattr__(inner, name): return getattr(inner.actual, name)
            def execute(inner, sql, params=None, **kwargs):
                if ('SELECT generation,changed_at' in sql and 'canonical_evidence_document_queue' in sql
                        and 'FOR UPDATE' not in sql and self.sources['large'] in params):
                    raise psycopg.OperationalError('synthetic queue read failure')
                return inner.actual.execute(sql, params, **kwargs)
        @contextmanager
        def failing_queue_read():
            with original() as connection: yield Connection(connection)
        with patch.object(self.store, 'connect', failing_queue_read): result = self.project()
        self.assertEqual((result['documents'], result['failed'], result['source_races']), (1, 1, 0))
        with original() as c:
            queued = c.execute('SELECT attempts,next_attempt_at,last_error_code FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                (self.tenant, self.sources['large'])).fetchone()
        self.assertEqual(queued['attempts'], 1)
        self.assertIsNotNone(queued['next_attempt_at'])
        self.assertEqual(queued['last_error_code'], 'OperationalError')


if __name__ == '__main__':
    suite = unittest.TestSuite(EarlySupersession(name) for name in EarlySupersession.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
