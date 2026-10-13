#!/usr/bin/env python3
"""Real commit lock timeouts roll back one document without aborting siblings."""
from contextlib import contextmanager
from datetime import date
import unittest
from unittest.mock import patch

import psycopg

from e2e_logical_parent_progress import ParentProgress
from recall_server.search_outbox import enqueue_search_outbox


class PassageLockContention(ParentProgress):
    def setUp(self):
        super().setUp()
        self.release.set()
        self.assertEqual(self.logical.project_pending(
            batch_size=2, max_batches=1, upload_concurrency=2)['documents'], 2)
        self.original_connect = self.store.connect

        @contextmanager
        def short_lock_timeout():
            with self.original_connect() as connection:
                connection.execute("SET LOCAL lock_timeout='100ms'")
                yield connection

        self.patch_connect = patch.object(self.store, 'connect', short_lock_timeout)
        self.patch_connect.start()
        self.addCleanup(self.patch_connect.stop)

    def queue(self, connection, source):
        return connection.execute('''SELECT logical_document_id,revision,generation,changed_at
            FROM canonical_passage_projection_queue WHERE tenant_id=%s AND source_id=%s''',
            (self.tenant, source)).fetchone()

    def exercise_lock(self, late):
        source = self.sources['large']
        with self.store.connect() as connection:
            before = self.queue(connection, source)
            scan_before = connection.execute('''SELECT * FROM canonical_parquet_scan_queue
                WHERE tenant_id=%s AND source_id=%s ORDER BY bucket_start''',
                (self.tenant, source)).fetchall()
            if late:
                enqueue_search_outbox(connection, tenant_id=self.tenant, source_id=source,
                    months=[date(2026, 7, 1)], reason='backfill')
        with self.original_connect() as holder:
            if late:
                locked = holder.execute('''SELECT generation,reason FROM search_projection_outbox
                    WHERE tenant_id=%s AND source_id=%s FOR UPDATE''',
                    (self.tenant, source)).fetchone()
                self.assertIsNotNone(locked)
            else:
                holder.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                    ('lossless-passages\x1f' + before['logical_document_id'],))
            result = self.passages.project_pending(batch_size=2, max_batches=1, concurrency=2)
            self.assertEqual((result['documents'], result['stale'], result['pending']), (1, 1, 1))
            self.assertEqual(result['status'], 'pending')
            with self.store.connect() as connection:
                self.assertEqual(self.queue(connection, source), before)
                self.assertIsNone(self.queue(connection, self.sources['small']))
                for table in ('canonical_passages', 'canonical_passage_documents',
                              'search_projection_tombstones'):
                    self.assertEqual(connection.execute(
                        f'SELECT count(*) AS n FROM {table} WHERE tenant_id=%s AND source_id=%s',
                        (self.tenant, source)).fetchone()['n'], 0, table)
                self.assertEqual(connection.execute('''SELECT * FROM canonical_parquet_scan_queue
                    WHERE tenant_id=%s AND source_id=%s ORDER BY bucket_start''',
                    (self.tenant, source)).fetchall(), scan_before)
                if late:
                    self.assertEqual(connection.execute('''SELECT generation,reason
                        FROM search_projection_outbox WHERE tenant_id=%s AND source_id=%s''',
                        (self.tenant, source)).fetchone(), locked)
        retried = self.passages.project_pending(batch_size=2, max_batches=1, concurrency=2)
        self.assertEqual((retried['documents'], retried['stale'], retried['pending']), (1, 0, 0))
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('''SELECT count(*) AS n
                FROM canonical_passage_documents WHERE tenant_id=%s''',
                (self.tenant,)).fetchone()['n'], 2)

    def test_document_lock_timeout_keeps_sibling_and_retries_after_release(self):
        self.exercise_lock(late=False)

    def test_late_outbox_timeout_rolls_back_catalog_and_queue_before_retry(self):
        self.exercise_lock(late=True)

    def test_unexpected_commit_error_still_propagates(self):
        error = ValueError('synthetic commit failure')
        with patch.object(self.passages, '_commit', side_effect=error):
            with self.assertRaises(ValueError) as raised:
                self.passages.project_pending(batch_size=2, max_batches=1, concurrency=2)
        self.assertIs(raised.exception, error)

    def test_lock_error_outside_commit_still_propagates(self):
        with patch.object(self.passages, '_prepare', side_effect=psycopg.errors.LockNotAvailable):
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                self.passages.project_pending(batch_size=2, max_batches=1, concurrency=2)


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageLockContention(name) for name in PassageLockContention.__dict__
        if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
