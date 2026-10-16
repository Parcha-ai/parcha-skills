"""Retain queued scan work when a native membership statement times out."""
import time
import unittest
from unittest.mock import patch

from e2e_singleton_projection_membership import SingletonProjectionMembership
from recall_server.db import BrainStore, SearchDeadlineExceeded


class ParquetDeadlineIsolation(SingletonProjectionMembership):
    def test_deadline_keeps_native_queue_and_cleans_unpublished_build(self):
        self.scan_fixture()
        with self.store.connect() as connection:
            before = connection.execute('SELECT generation,changed_at FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
        execute = self.store._execute_bounded
        calls = []

        def blocked_membership(connection, statement, parameters, deadline):
            if statement.startswith('WITH requested AS MATERIALIZED'):
                calls.append('native-membership')
                # Real PostgreSQL timeout at the existing verifier execute boundary;
                # no production statement, timeout setting or verifier is edited.
                return BrainStore._execute_bounded(connection, 'SELECT pg_sleep(0.05)', (), time.monotonic()+0.005)
            return execute(connection, statement, parameters, deadline)

        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.store, '_execute_bounded', side_effect=blocked_membership):
                deferred = self.scan.project_pending(tenant_id=self.tenant, batch_size=1,
                                                    max_batches=1, compaction_budget=0)
        self.assertEqual(deferred['status'], 'pending')
        self.assertEqual(deferred['failed'], 1)
        self.assertEqual(deferred['shards'], 0)
        self.assertEqual(calls, ['native-membership'])
        with self.store.connect() as connection:
            after = connection.execute('SELECT generation,changed_at FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
            published = connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_shards WHERE tenant_id=%s', (self.tenant,)).fetchone()['n']
        self.assertEqual(after, before)
        self.assertEqual(published, 0)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            recovered = self.scan.project_pending(tenant_id=self.tenant, batch_size=1,
                                                 max_batches=1, compaction_budget=0)
        self.assertEqual(recovered['shards'], 1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)

    def test_lost_publication_ack_stays_unknown_after_actual_commit(self):
        self.scan_fixture()
        commit = self.scan._commit

        def lost_ack(*args, **kwargs):
            self.assertEqual(commit(*args, **kwargs), 'committed')
            raise SearchDeadlineExceeded('synthetic lost acknowledgement')

        with patch.object(self.scan, '_commit', side_effect=lost_ack):
            with self.assertRaises(SearchDeadlineExceeded):
                self.scan.project_pending(tenant_id=self.tenant, batch_size=1,
                                          max_batches=1, compaction_budget=0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)
            self.assertGreater(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_shards WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)

    def test_cleanup_failure_is_not_misclassified_as_safe_deferred_build(self):
        from recall_server.parquet_scan import ParquetScanError
        self.scan_fixture()
        create = self.scan._streaming_upload

        def failed_cleanup(*args, **kwargs):
            upload = create(*args, **kwargs)
            upload.abort = lambda: (_ for _ in ()).throw(RuntimeError('synthetic cleanup failure'))
            return upload

        with patch.object(self.scan, '_streaming_upload', side_effect=failed_cleanup):
            with patch.object(self.scan, '_passages', side_effect=SearchDeadlineExceeded()):
                with self.assertRaisesRegex(ParquetScanError, 'parquet_scan_cleanup_enqueue_failed'):
                    self.scan.project_pending(tenant_id=self.tenant, batch_size=1,
                                              max_batches=1, compaction_budget=0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_parquet_scan_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 1)


if __name__ == '__main__':
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(ParquetDeadlineIsolation(name) for name in ParquetDeadlineIsolation.__dict__ if name.startswith('test_')))
    raise SystemExit(not result.wasSuccessful())
