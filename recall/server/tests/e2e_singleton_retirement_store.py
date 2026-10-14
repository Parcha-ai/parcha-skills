#!/usr/bin/env python3
"""The one-connection worker shares the activated receipt-reader contract."""
import os
import unittest
from unittest.mock import patch

from e2e_singleton_profile_capability import SingletonProfileCapability
from recall_server.db import BrainStore
from recall_server.retirement_process import RetirementStore


class SingletonRetirementStore(SingletonProfileCapability):
    def active(self):
        return patch.dict(os.environ, {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '1',
            'RECALL_CHUNK_BODY_READS': 'archive', 'RECALL_SEARCH_PLANE': 'turbopuffer'})

    def test_worker_enabled_uses_shared_schema_owner_and_one_connection(self):
        with self.active(), patch.object(BrainStore, 'verify_search_plane', autospec=True,
                side_effect=BrainStore.verify_search_plane) as verify:
            worker = RetirementStore(self.store.dsn)
            self.addCleanup(worker.close)
            self.assertTrue(worker.verified_singleton_receipts)
            self.assertEqual(worker.search_plane, 'turbopuffer')
            self.assertEqual(worker.pool.max_size, 1)
            with worker.connect() as connection:
                self.assertEqual(connection.execute('SELECT 1 AS n').fetchone()['n'], 1)
            verify.assert_called_once()

    def test_worker_disabled_keeps_optional_schema_query_absent(self):
        with patch.dict(os.environ, {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '0'}), \
                patch.object(BrainStore, 'verify_search_plane', side_effect=AssertionError('unexpected validation')):
            worker = RetirementStore(self.store.dsn)
            self.addCleanup(worker.close)
            self.assertFalse(worker.verified_singleton_receipts)
            with worker.connect() as connection:
                self.assertEqual(connection.execute('SELECT 1 AS n').fetchone()['n'], 1)

    def test_worker_invalid_activation_refuses_before_pool(self):
        for key, value in (('RECALL_VERIFIED_SINGLETON_RECEIPTS', 'true'),
                ('RECALL_CHUNK_BODY_READS', 'postgres'), ('RECALL_SEARCH_PLANE', 'postgres')):
            with self.subTest(key=key), self.active(), patch.dict(os.environ, {key: value}), \
                    patch('recall_server.retirement_process.ConnectionPool') as pool:
                with self.assertRaises(ValueError):
                    RetirementStore(self.store.dsn)
                pool.assert_not_called()

    def test_worker_failed_validation_closes_pool_and_retry_still_refuses(self):
        from psycopg_pool import ConnectionPool
        with self.store.connect() as connection:
            connection.execute('DELETE FROM schema_migrations WHERE version=80')
        pools = []
        def capture(*args, **kwargs):
            pool = ConnectionPool(*args, **kwargs)
            pools.append(pool)
            return pool
        try:
            with self.active(), patch('recall_server.retirement_process.ConnectionPool', side_effect=capture):
                for _ in range(2):
                    with self.assertRaisesRegex(RuntimeError, 'singleton.*schema'):
                        RetirementStore(self.store.dsn)
            self.assertEqual(len(pools), 2)
            self.assertTrue(all(pool.closed for pool in pools))
        finally:
            for pool in pools:
                pool.close()
            with self.store.connect() as connection:
                connection.execute('INSERT INTO schema_migrations(version) VALUES(80) ON CONFLICT DO NOTHING')

    def test_worker_wrong_shape_refuses_before_use(self):
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_documents ALTER COLUMN chunk_receipt_profile SET DEFAULT 1')
        worker = None
        try:
            with self.active(), self.assertRaisesRegex(RuntimeError, 'singleton.*schema'):
                worker = RetirementStore(self.store.dsn)
        finally:
            if worker is not None:
                worker.close()
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_documents ALTER COLUMN chunk_receipt_profile DROP DEFAULT')


from e2e_singleton_metadata_admission import SingletonMetadataAdmission


class WorkerAdmittedProof(SingletonMetadataAdmission):
    def test_worker_proves_actual_admitted_parent_with_one_connection(self):
        from recall_server.chunk_retirement import retire_parent_chunks
        original = self.prepare()
        preview = self.admit(original)
        self.admit(original, apply=True, reviewed_plan=preview['plan'])
        with patch.dict(os.environ, {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '1',
                'RECALL_CHUNK_BODY_READS': 'archive', 'RECALL_SEARCH_PLANE': 'turbopuffer'}):
            worker = RetirementStore(self.store.dsn)
            self.addCleanup(worker.close)
            result = retire_parent_chunks(worker, self.archive, tenant_id=self.tenant,
                source_id=self.source, native_parent_id='session')
        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['eligible_documents'], 1)
        self.assertIsNone(self.baseline(original['receipt']))
        self.assertEqual(worker.pool.max_size, 1)


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        cls(name) for cls in (SingletonRetirementStore, WorkerAdmittedProof)
        for name in cls.__dict__ if name.startswith('test_worker_')))
    raise SystemExit(not result.wasSuccessful())
