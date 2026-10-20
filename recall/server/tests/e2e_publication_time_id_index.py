#!/usr/bin/env python3
"""Optional page-order index: exact reads, concurrent build and explicit recovery."""
import json
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import psycopg
from psycopg.rows import dict_row

from e2e_search_publication_page_keys import PublicationPageKeys

SERVER = Path(__file__).resolve().parents[1]
NAME = 'canonical_passages_page_time_id_idx'
BUILD = (SERVER/'operations/publication_page_time_id_index_concurrent.sql').read_text()
READY = (SERVER/'operations/publication_page_time_id_index_readiness.sql').read_text()


class PublicationTimeIndex(PublicationPageKeys):
    def admin(self, timeout=60000):
        return psycopg.connect(self.store.dsn, autocommit=True, row_factory=dict_row,
            options=f'-c statement_timeout={timeout} -c lock_timeout=1000 -c maintenance_work_mem=64MB -c max_parallel_maintenance_workers=0')

    def state(self,c):
        return c.execute('''SELECT indexrelid,indisvalid,indisready,indislive,
            pg_get_indexdef(indexrelid) AS definition,pg_relation_size(indexrelid) AS bytes
            FROM pg_index WHERE indexrelid=to_regclass(%s)''',('public.'+NAME,)).fetchone()

    def build(self,timeout=60000):
        with self.admin(timeout) as c:
            self.store._migrate_concurrently(c,BUILD)
            c.autocommit=True
            c.execute(READY)
            return self.state(c)

    def test_concurrent_build_exact_reads_size_and_explicit_rollback(self):
        owner,claim=self.tied_fixture()
        def page():
            with patch.object(self.store,'verified_singleton_receipts',True),self.store.connect() as c,c.transaction():
                c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                return owner.passage_page(c,claim,since=None,after=None,deadline_at=time.monotonic()+3)
        before=page()
        with self.admin() as c:
            versions=c.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()
            self.assertIsNone(self.state(c))
            metrics=c.execute('''SELECT count(*) AS rows,
                avg(pg_column_size(tenant_id)+pg_column_size(source_id)+pg_column_size(first_occurred_at)+pg_column_size(passage_id)) AS mean_key_payload_bytes,
                pg_relation_size('canonical_passages') AS table_bytes,
                pg_relation_size('canonical_passages_time_idx') AS old_time_index_bytes
                FROM canonical_passages''').fetchone()
            old=c.execute("SELECT 'canonical_passages_time_idx'::regclass::oid AS oid").fetchone()['oid']
            lsn=c.execute('SELECT pg_current_wal_insert_lsn() AS lsn').fetchone()['lsn']
        started=time.monotonic();built=self.build();build_ms=(time.monotonic()-started)*1000
        self.assertEqual(page(),before)
        with self.admin() as c:
            wal=int(c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s::pg_lsn) AS bytes',(lsn,)).fetchone()['bytes'])
            self.assertEqual(self.build()['indexrelid'],built['indexrelid'])
            self.assertEqual(c.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall(),versions)
            self.assertEqual(c.execute("SELECT 'canonical_passages_time_idx'::regclass::oid AS oid").fetchone()['oid'],old)
            self.assertFalse(c.execute('SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conindid=%s) AS owned',(built['indexrelid'],)).fetchone()['owned'])
            c.execute('DROP INDEX CONCURRENTLY public.'+NAME)
            self.assertIsNone(self.state(c))
        self.assertEqual(page(),before)
        print(json.dumps(dict(status='pass',concurrent_build_ms=build_ms,index_bytes=built['bytes'],
            wal_bytes_during_build=wal,bytes_per_row=built['bytes']/metrics['rows'],
            footprint=metrics,exact_before_after_rollback_rows=len(before),schema_history_unchanged=True,
            old_index_oid_unchanged=True),default=str,sort_keys=True),flush=True)

    def test_readiness_refuses_wrong_definition_and_interrupted_build(self):
        self.tied_fixture()
        with self.admin() as c:
            c.execute('CREATE INDEX '+NAME+' ON canonical_passages(tenant_id,source_id,first_occurred_at,last_occurred_at)')
            wrong=self.state(c)
            self.store._migrate_concurrently(c,BUILD)
            c.autocommit=True
            with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation,'publication_page_time_id_index_unavailable'):
                c.execute(READY)
            self.assertEqual(self.state(c)['indexrelid'],wrong['indexrelid'])
            c.execute('DROP INDEX CONCURRENTLY public.'+NAME)
        with psycopg.connect(self.store.dsn) as writer,ThreadPoolExecutor(max_workers=1) as executor:
            writer.execute('UPDATE canonical_passages SET header_redacted=header_redacted WHERE tenant_id=%s AND passage_id=%s',(self.tenant,self.ids[0]))
            future=executor.submit(self.build,1000)
            with self.assertRaises((psycopg.errors.QueryCanceled,psycopg.errors.LockNotAvailable)):
                future.result(timeout=5)
            writer.rollback()
        with self.admin() as c:
            invalid=self.state(c)
            self.assertIsNotNone(invalid)
            self.assertFalse(invalid['indisvalid'])
            with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation,'publication_page_time_id_index_unavailable'):
                c.execute(READY)
            # Fixture-only inspected cleanup. No implicit cleanup/retry exists
            # in the operation; production requires the same identity review.
            self.assertEqual(self.state(c)['indexrelid'],invalid['indexrelid'])
            c.execute('DROP INDEX CONCURRENTLY public.'+NAME)
        rebuilt=self.build()
        self.assertTrue(rebuilt['indisvalid'] and rebuilt['indisready'] and rebuilt['indislive'])
        with self.admin() as c:c.execute('DROP INDEX CONCURRENTLY public.'+NAME)


if __name__=='__main__':
    cases=[PublicationTimeIndex(n) for n in PublicationTimeIndex.__dict__ if n.startswith('test_')]
    # Original methods and assertions unchanged. The optional operation is not
    # installed by migrations; its separate before/after proof is above.
    cases += [PublicationPageKeys(n) for n in PublicationPageKeys.__dict__ if n.startswith('test_')]
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(cases)).wasSuccessful())
