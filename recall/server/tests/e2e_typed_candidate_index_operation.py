#!/usr/bin/env python3
"""Optional candidate index: unchanged admission results and concurrent lifecycle."""
import json
import os
import uuid
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.conninfo import make_conninfo

from e2e_typed_upgrade_capacity_index import NATIVE, TypedUpgradeCapacityIndex

INDEX = 'canonical_passage_documents_typed_candidate_idx'

SERVER = Path(__file__).resolve().parents[1]
BUILD = (SERVER/'operations/typed_candidate_index_concurrent.sql').read_text()
READY = (SERVER/'operations/typed_candidate_index_readiness.sql').read_text()


class TypedCandidateIndexOperation(TypedUpgradeCapacityIndex):
    def setUp(self):
        # Keep bulk fixture rows and ANALYZE statistics out of the shared CI DB.
        admin_dsn = os.environ['RECALL_DATABASE_URL']
        database = 'typed_candidate_' + uuid.uuid4().hex
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        def drop_database():
            with psycopg.connect(admin_dsn, autocommit=True) as admin:
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))
        self.addCleanup(drop_database)
        with patch.dict(os.environ, {'RECALL_DATABASE_URL': make_conninfo(admin_dsn, dbname=database)}):
            super().setUp()
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT current_database() AS name').fetchone()['name'], database)

    def admin(self, timeout=60000):
        return psycopg.connect(self.store.dsn, autocommit=True, row_factory=dict_row,
            options=f'-c statement_timeout={timeout} -c lock_timeout=1000 -c maintenance_work_mem=64MB -c max_parallel_maintenance_workers=0')

    def state(self, c):
        return c.execute('''SELECT indexrelid,indisvalid,indisready,indislive,
            pg_get_indexdef(indexrelid) AS definition,pg_relation_size(indexrelid) AS bytes
            FROM pg_index WHERE indexrelid=to_regclass(%s)''', ('public.'+INDEX,)).fetchone()

    def build(self, timeout=60000):
        with self.admin(timeout) as c:
            self.store._migrate_concurrently(c, BUILD)
            c.autocommit=True
            c.execute(READY)
            return self.state(c)

    def cleanup_index(self):
        with self.admin() as c:
            if self.state(c) is not None:
                c.execute('DROP INDEX CONCURRENTLY public.'+INDEX)

    def test_original_full_query_matches_across_capacity_cursor_and_completion(self):
        self.seed(total=4500)
        cursors = (None, '', self.source, 'zzzz')
        def outputs():
            with self.store.connect() as c:
                return [self.store._execute_bounded(c, NATIVE,
                    (self.tenant, 100, self.passages.policy.fingerprint, 100,
                     self.tenant, cursor, cursor, 100), time.monotonic()+3).fetchall()
                    for cursor in cursors]
        before = {}
        for count in (0, 1, 49, 99, 100, 101):
            self.shape(count)
            before[count] = outputs()
            self.assertEqual(len(before[count][0]), max(0, 100-count))
        self.addCleanup(self.cleanup_index)
        self.build()
        for count in before:
            self.shape(count)
            self.assertEqual(outputs(), before[count])
        self.shape(0)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_passage_documents SET requested_extraction_version='codex_tool_groups_v1',"
                      "applied_extraction_version='codex_tool_groups_v1' WHERE tenant_id=%s", (self.tenant,))
        self.assertEqual(outputs(), [[], [], [], []])
        self.cleanup_index()
        self.assertEqual(outputs(), [[], [], [], []])

    def test_concurrent_build_exact_reads_and_explicit_rollback(self):
        self.seed(total=300)
        self.shape(49)
        with self.store.connect() as c:
            before = self.full(c)
            self.assertEqual(self.capacity(c), 49)
        with self.admin() as c:
            self.assertIsNone(self.state(c))
            versions = c.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()
            existing = c.execute('''SELECT indexrelid FROM pg_index
                WHERE indrelid='canonical_passage_documents'::regclass ORDER BY indexrelid''').fetchall()
            lsn = c.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
        self.addCleanup(self.cleanup_index)
        started = time.monotonic()
        built = self.build()
        build_ms = (time.monotonic()-started)*1000
        with self.store.connect() as c:
            self.assertEqual(self.full(c), before)
            self.assertEqual(self.capacity(c), 49)
        self.assertEqual(self.build()['indexrelid'], built['indexrelid'])
        with self.admin() as c:
            wal = int(c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s::pg_lsn) AS n', (lsn,)).fetchone()['n'])
            self.assertEqual(c.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall(), versions)
            self.assertEqual(c.execute('''SELECT indexrelid FROM pg_index
                WHERE indrelid='canonical_passage_documents'::regclass AND indexrelid<>%s
                ORDER BY indexrelid''', (built['indexrelid'],)).fetchall(), existing)
            self.assertFalse(c.execute('SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conindid=%s) AS owned',
                                      (built['indexrelid'],)).fetchone()['owned'])
        self.cleanup_index()
        with self.store.connect() as c:
            self.assertEqual(self.full(c), before)
            self.assertEqual(self.capacity(c), 49)
        print(json.dumps(dict(concurrent_build_ms=build_ms, index_bytes=built['bytes'],
            wal_bytes_during_build=wal, exact_before_after_rollback_rows=len(before),
            schema_and_existing_indexes_unchanged=True), sort_keys=True), flush=True)

    def test_wrong_definition_and_interrupted_build_are_not_ready(self):
        self.seed(total=20)
        self.shape(5)
        with self.admin() as c:
            self.assertIsNone(self.state(c))
        self.addCleanup(self.cleanup_index)
        with self.admin() as c:
            c.execute('CREATE INDEX '+INDEX+' ON canonical_passage_documents(tenant_id)')
            wrong = self.state(c)
            self.store._migrate_concurrently(c, BUILD)
            c.autocommit=True
            with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation, 'typed_candidate_index_unavailable'):
                c.execute(READY)
            self.assertEqual(self.state(c)['indexrelid'], wrong['indexrelid'])
        self.cleanup_index()
        with psycopg.connect(self.store.dsn) as writer, ThreadPoolExecutor(max_workers=1) as executor:
            writer.execute('''UPDATE canonical_passage_documents SET requested_extraction_version=requested_extraction_version
                WHERE tenant_id=%s''', (self.tenant,))
            future = executor.submit(self.build, 1000)
            with self.assertRaises((psycopg.errors.QueryCanceled, psycopg.errors.LockNotAvailable)):
                future.result(timeout=5)
            writer.rollback()
        with self.admin() as c:
            invalid = self.state(c)
            self.assertIsNotNone(invalid)
            self.assertFalse(invalid['indisvalid'])
            with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation, 'typed_candidate_index_unavailable'):
                c.execute(READY)
            self.assertEqual(self.state(c)['indexrelid'], invalid['indexrelid'])
        # Explicit fixture-only cleanup after observing the exact failed index.
        # The operation itself neither drops nor retries an invalid build.
        self.cleanup_index()
        rebuilt = self.build()
        self.assertTrue(rebuilt['indisvalid'] and rebuilt['indisready'] and rebuilt['indislive'])


if __name__ == '__main__':
    cases = [TypedCandidateIndexOperation(n) for n in TypedCandidateIndexOperation.__dict__ if n.startswith('test_')]
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(cases)).wasSuccessful())
