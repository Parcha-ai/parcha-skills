#!/usr/bin/env python3
"""Prove source status can use the native/revision UNIQUE index alone.

Requires an explicitly supplied disposable PostgreSQL database. Creates only
transaction-local temporary tables and never initializes the application store.
"""
import ast
import os
from pathlib import Path
import unittest

import psycopg
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]


def latest_query():
    tree = ast.parse((SERVER / 'recall_server/canonical.py').read_text())
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.FunctionDef) and node.name == 'source_status')
    prefix = next(node.value for node in ast.walk(method)
                  if isinstance(node, ast.Constant) and isinstance(node.value, str)
                  and node.value.lstrip().startswith('WITH latest AS ('))
    return prefix.split('), expected AS (', 1)[0] + ') SELECT * FROM latest'


def plan_nodes(node):
    yield node
    for child in node.get('Plans', ()):
        yield from plan_nodes(child)


class SourceStatusNativeOrderTest(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(os.environ['RECALL_DATABASE_URL'], row_factory=dict_row)
        self.addCleanup(self.connection.close)
        self.connection.execute('''CREATE TEMP TABLE canonical_events (
            tenant_id text NOT NULL, source_id text NOT NULL,
            native_id text NOT NULL, native_parent_id text,
            revision integer NOT NULL, is_tombstone boolean NOT NULL,
            CONSTRAINT native_revision_key UNIQUE(tenant_id,source_id,native_id,revision)
        ) ON COMMIT DROP''')

    def test_latest_revision_and_tombstones_are_independent_of_native_identity_order(self):
        rows = [
            ('t', 's', 'zeta', 'old-z', 1, False),
            ('t', 's', 'alpha', 'old-a', 1, False),
            ('t', 's', 'beta', 'old-b', 1, True),
            ('t', 's', 'zeta', 'new-z', 7, True),
            ('t', 's', 'beta', 'new-b', 4, False),
            ('t', 's', 'alpha', 'new-a', 3, False),
            ('t', 's', 'middle', None, 1, True),
            ('foreign', 's', 'alpha', 'foreign', 99, True),
            ('t', 'foreign', 'beta', 'foreign', 99, True),
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany('INSERT INTO canonical_events VALUES (%s,%s,%s,%s,%s,%s)', rows)
        actual = self.connection.execute(latest_query(), ('t', 's')).fetchall()
        selected = {row['native_id']: (row['native_parent_id'], row['revision'], row['is_tombstone'])
                    for row in actual}
        self.assertEqual(selected, {'alpha': ('new-a', 3, False), 'beta': ('new-b', 4, False),
                                    'middle': (None, 1, True), 'zeta': ('new-z', 7, True)})
        # This is source_status's expected/live partition: a newer live revision
        # restores a tombstoned identity, while a newer tombstone removes it.
        self.assertEqual({row['native_id'] for row in actual if not row['is_tombstone']},
                         {'alpha', 'beta'})
        self.assertEqual(sum(row['is_tombstone'] for row in actual), 2)

    def test_surviving_unique_index_supplies_latest_order_without_sort(self):
        self.connection.execute('''INSERT INTO canonical_events
            SELECT 't','s',lpad(n::text,6,'0'),'parent',revision,false
            FROM generate_series(1,10000) n CROSS JOIN generate_series(1,3) revision''')
        self.connection.execute('ANALYZE canonical_events')
        # The table has only the production-equivalent UNIQUE access path,
        # with normal planner settings and no descending native index.
        explained = self.connection.execute('EXPLAIN (FORMAT JSON) ' + latest_query(),
                                            ('t', 's')).fetchone()['QUERY PLAN'][0]['Plan']
        nodes = list(plan_nodes(explained))
        self.assertFalse(any('Sort' in node['Node Type'] for node in nodes), explained)
        self.assertTrue(any(node.get('Index Name') == 'native_revision_key'
                            and node.get('Scan Direction') == 'Backward' for node in nodes), explained)
        self.assertEqual(len(self.connection.execute(latest_query(), ('t', 's')).fetchall()), 10000)


if __name__ == '__main__':
    unittest.main()
