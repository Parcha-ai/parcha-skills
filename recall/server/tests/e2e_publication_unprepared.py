#!/usr/bin/env python3
"""Incremental publication keeps the current watermark selective after warmup."""
import json
import os
from pathlib import Path
import sys
import time
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_publication_correlated_eligibility import CorrelatedEligibility
from e2e_search_publication_page_keys import BASELINE_SQL
from recall_server import turbopuffer_projection as publication
from recall_server.db import BrainStore, SearchDeadlineExceeded


class PublicationUnprepared(CorrelatedEligibility):
    @classmethod
    def setUpClass(cls):
        admin_dsn = os.environ['RECALL_DATABASE_URL']
        database = 'publication_unprepared_' + uuid.uuid4().hex
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))

        def remove_database():
            with psycopg.connect(admin_dsn, autocommit=True) as admin:
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))

        cls.addClassCleanup(remove_database)
        cls.store = BrainStore(make_conninfo(admin_dsn, dbname=database))
        cls.addClassCleanup(cls.store.close)
        cls.store.migrate()

    def test_warm_publication_keeps_dense_sparse_rows_and_watermark_plan_local(self):
        owner, claim = self.tied_fixture()
        statement = publication._PASSAGE_PAGE_SQL
        self.install_time_index()
        cached_pages = []
        for case in ('dense', 'sparse'):
            if case == 'sparse':
                with self.store.connect() as connection:
                    connection.execute("UPDATE canonical_passages SET created_at=CASE WHEN ordinal>=19990 "
                                       "THEN '2026-10-19'::timestamptz ELSE '2020-01-01'::timestamptz END "
                                       'WHERE tenant_id=%s', (self.tenant,))
                    connection.execute('ANALYZE canonical_passages')
            since = datetime(2026, 10, 1, tzinfo=timezone.utc) if case == 'sparse' else None
            with patch.object(self.store, 'verified_singleton_receipts', True), \
                    psycopg.connect(self.store.dsn, row_factory=dict_row) as connection, connection.transaction():
                self.assertEqual(connection.prepare_threshold, 5)
                self.assertEqual(connection.execute('SHOW plan_cache_mode').fetchone()['plan_cache_mode'], 'auto')
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                expected, expected_pages, _ = self.drain(connection, owner, claim, BASELINE_SQL, since)
                for _ in range(12):
                    owner.passage_page(connection, claim, since=since, after=None,
                                       deadline_at=time.monotonic() + 5)
                cached = connection.execute(
                    "SELECT statement,generic_plans,custom_plans FROM pg_prepared_statements "
                    "WHERE statement LIKE 'WITH eligible_page AS MATERIALIZED (%%'", prepare=False,
                ).fetchall()
                actual, pages, elapsed = self.drain(connection, owner, claim, statement, since)
                self.assertEqual(actual, expected)
                self.assertEqual(pages, expected_pages)
                self.assertEqual(len({row['passage_id'] for row in actual}), len(actual))
                self.assertEqual(len(actual), 11 if case == 'sparse' else 10001)
                self.assertEqual(pages, 1 if case == 'sparse' else 79)
                cached_pages.extend(cached)
                # Other bounded queries retain the driver's ordinary preparation.
                for _ in range(12):
                    self.store._execute_bounded(connection,
                        'SELECT %s::integer AS publication_prepare_control', (42,),
                        time.monotonic() + 5).fetchone()
                control = connection.execute(
                    'SELECT generic_plans,custom_plans FROM pg_prepared_statements WHERE statement=%s',
                    ('SELECT $1::integer AS publication_prepare_control',), prepare=False,
                ).fetchall()
                self.assertEqual(len(control), 1)
                self.assertGreater(control[0]['generic_plans'] + control[0]['custom_plans'], 0)
                print(json.dumps(dict(case=case, rows=len(actual), pages=pages,
                                      elapsed_ms=elapsed, full_rows_and_order_equal=True,
                                      cached_publication_plans=[{key: row[key] for key in
                                          ('generic_plans', 'custom_plans')} for row in cached],
                                      ordinary_preparation_retained=True)), flush=True)
        self.assertEqual(cached_pages, [], 'publication reuses a generic watermark plan after driver warmup')

    def test_unprepared_query_preserves_native_errors_deadlines_and_rollback(self):
        with self.store.connect() as connection:
            connection.execute('CREATE TEMP TABLE publication_deadline_fixture(id integer)')
            connection.commit()
            with self.assertRaises(SearchDeadlineExceeded) as caught:
                with connection.transaction():
                    connection.execute('INSERT INTO publication_deadline_fixture VALUES (1)')
                    self.store._execute_bounded(connection, 'SELECT pg_sleep(%s)', (1,),
                                                time.monotonic() + .05, prepare=False)
            self.assertIsInstance(caught.exception.__cause__, psycopg.errors.QueryCanceled)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM publication_deadline_fixture').fetchone()['n'], 0)
            connection.commit()
            with self.assertRaises(SearchDeadlineExceeded):
                with connection.transaction():
                    self.store._execute_bounded(connection,
                        'INSERT INTO publication_deadline_fixture VALUES (2)', (),
                        time.monotonic() - 1, prepare=False)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM publication_deadline_fixture').fetchone()['n'], 0)
            connection.commit()
            with self.assertRaises(psycopg.errors.UndefinedTable):
                with connection.transaction():
                    self.store._execute_bounded(connection, 'SELECT * FROM missing_publication_fixture', (),
                                                time.monotonic() + 5, prepare=False)
            self.assertEqual(self.store._execute_bounded(connection, 'SELECT 42 AS value', (),
                                                        None, prepare=False).fetchone(), {'value': 42})


if __name__ == '__main__':
    suite = unittest.TestSuite(PublicationUnprepared(name) for name in PublicationUnprepared.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
