#!/usr/bin/env python3
"""Optional profile schema and explicit runtime activation, without admission."""
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from e2e_receipt_membership import ReceiptMembershipTests
from recall_server.db import BrainStore


class SingletonProfileCapability(ReceiptMembershipTests):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        super().setUp()
        self.enterContext(patch("recall_server.db.turbopuffer_settings_from_env", return_value=None))

    def schema(self):
        return (Path(__file__).resolve().parents[1] / 'schema' / '080_singleton_receipt_profile.sql').read_text()

    def test_profile_env_is_explicit_default_off_and_requires_archive_tp(self):
        scope = {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '1', 'RECALL_CHUNK_BODY_READS': 'archive',
                 'RECALL_SEARCH_PLANE': 'turbopuffer'}
        with patch.dict(os.environ, scope):
            enabled = BrainStore(self.store.dsn)
            self.addCleanup(enabled.close)
            self.assertTrue(enabled.verified_singleton_receipts)
            explicit = BrainStore(self.store.dsn, verified_singleton_receipts=False)
            self.addCleanup(explicit.close)
            self.assertFalse(explicit.verified_singleton_receipts)
        for key, value in (('RECALL_VERIFIED_SINGLETON_RECEIPTS', 'true'),
                           ('RECALL_CHUNK_BODY_READS', 'postgres'), ('RECALL_SEARCH_PLANE', 'postgres')):
            with patch.dict(os.environ, dict(scope, **{key: value})):
                with self.assertRaises(ValueError):
                    BrainStore(self.store.dsn)
        with patch.dict(os.environ, dict(scope, RECALL_VERIFIED_SINGLETON_RECEIPTS='0')):
            disabled = BrainStore(self.store.dsn)
            self.addCleanup(disabled.close)
            self.assertFalse(disabled.verified_singleton_receipts)

    def test_profile_migration_is_nullable_no_default_and_no_heap_rewrite(self):
        self.ingest('turn/profile-shape', 'Existing document remains unadmitted')
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_documents DROP COLUMN IF EXISTS chunk_receipt_profile')
            connection.execute('DELETE FROM schema_migrations WHERE version=80')
            before = connection.execute("SELECT pg_relation_filenode('canonical_documents') AS node").fetchone()['node']
            connection.execute(self.schema())
            shape = connection.execute("""SELECT data_type,is_nullable,column_default
                FROM information_schema.columns WHERE table_schema='public'
                AND table_name='canonical_documents' AND column_name='chunk_receipt_profile'""").fetchone()
            after = connection.execute("SELECT pg_relation_filenode('canonical_documents') AS node").fetchone()['node']
            self.assertEqual(shape, dict(data_type='smallint', is_nullable='YES', column_default=None))
            self.assertEqual(after, before)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_documents WHERE chunk_receipt_profile IS NOT NULL').fetchone()['n'], 0)
            self.assertIsNotNone(connection.execute('SELECT version FROM schema_migrations WHERE version=80').fetchone())

    def test_profile_enabled_connection_rejects_missing_marker_and_wrong_column(self):
        with self.store.connect() as connection:
            connection.execute(self.schema())
            versions = connection.execute('SELECT version FROM schema_migrations WHERE version=80').fetchone()
            self.assertIsNotNone(versions)
        scope = {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '1', 'RECALL_CHUNK_BODY_READS': 'archive',
                 'RECALL_SEARCH_PLANE': 'turbopuffer'}
        with patch.dict(os.environ, scope):
            enabled = BrainStore(self.store.dsn)
            self.addCleanup(enabled.close)
            with self.store.connect() as connection, connection.transaction(force_rollback=True):
                connection.execute('DELETE FROM schema_migrations WHERE version=80')
                with self.assertRaisesRegex(RuntimeError, 'singleton.*schema'):
                    enabled.verify_search_plane(connection)
            with self.store.connect() as connection, connection.transaction(force_rollback=True):
                connection.execute('ALTER TABLE canonical_documents ALTER COLUMN chunk_receipt_profile TYPE integer')
                with self.assertRaisesRegex(RuntimeError, 'singleton.*schema'):
                    enabled.verify_search_plane(connection)
            with self.store.connect() as connection:
                enabled.verify_search_plane(connection)


    def test_profile_failed_pool_start_cannot_skip_schema_check_on_retry(self):
        scope = {'RECALL_VERIFIED_SINGLETON_RECEIPTS': '1', 'RECALL_CHUNK_BODY_READS': 'archive',
                 'RECALL_SEARCH_PLANE': 'turbopuffer'}
        with self.store.connect() as connection:
            connection.execute(self.schema())
            connection.execute('DELETE FROM schema_migrations WHERE version=80')
        try:
            with patch.dict(os.environ, scope):
                enabled = BrainStore(self.store.dsn)
                self.addCleanup(enabled.close)
                for attempt in range(2):
                    with self.subTest(attempt=attempt), self.assertRaisesRegex(RuntimeError, 'singleton.*schema'):
                        with enabled.connect():
                            self.fail('unverified pool was reused after failed startup')
        finally:
            with self.store.connect() as connection:
                connection.execute('INSERT INTO schema_migrations(version) VALUES(80) ON CONFLICT DO NOTHING')


    def test_profile_concurrent_connect_waits_for_pool_validation(self):
        from concurrent.futures import ThreadPoolExecutor, TimeoutError
        from threading import Event
        enabled = BrainStore(self.store.dsn, verified_singleton_receipts=True)
        self.addCleanup(enabled.close)
        entered, release, second_started = Event(), Event(), Event()
        verify = enabled.verify_search_plane
        def paused_verify(connection):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('synthetic validation synchronization timeout')
            return verify(connection)
        def read(second=False):
            if second:
                second_started.set()
            with enabled.connect() as connection:
                return connection.execute('SELECT 1 AS n').fetchone()['n']
        with patch.object(enabled, 'verify_search_plane', paused_verify), ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(read)
            try:
                self.assertTrue(entered.wait(5))
                second = workers.submit(read, True)
                self.assertTrue(second_started.wait(5))
                with self.assertRaises(TimeoutError):
                    second.result(timeout=0.1)
            finally:
                release.set()
            self.assertEqual(first.result(timeout=5), 1)
            self.assertEqual(second.result(timeout=5), 1)


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        SingletonProfileCapability(name) for name in SingletonProfileCapability.__dict__ if name.startswith('test_profile_')))
    raise SystemExit(not result.wasSuccessful())
