#!/usr/bin/env python3
"""A worker warmup must coexist with active work and survive pool contention."""
import os
from pathlib import Path
import sys
import unittest

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from psycopg.rows import dict_row  # noqa: E402
from psycopg_pool import ConnectionPool, PoolClosed, PoolTimeout  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402


class PoolWarmup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        try:
            store.migrate()
        finally:
            store.close()

    def store(self, size):
        store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=size)
        self.addCleanup(store.close)
        store._pool = ConnectionPool(os.environ['RECALL_DATABASE_URL'],
            kwargs={'row_factory':dict_row}, min_size=4, max_size=size, open=True)
        store._pool.wait(timeout=5)
        return store

    def test_warmup_grows_within_capacity_while_parent_holds_connection(self):
        store = self.store(8)
        store.prepare_pool(4, timeout=1)
        with store.connect() as active_parent:
            store.prepare_pool(4, timeout=1)
            self.assertEqual(active_parent.execute('SELECT 1 AS n').fetchone()['n'], 1)
        self.assertFalse(store._pool.closed)
        with store.connect() as connection:
            self.assertEqual(connection.execute('SELECT 2 AS n').fetchone()['n'], 2)

    def test_contention_timeout_does_not_destroy_pool_or_prevent_next_cycle(self):
        store = self.store(4)
        store.prepare_pool(4, timeout=1)
        with store.connect() as active_parent:
            pool = store._pool
            with self.assertRaises(PoolTimeout):
                store.prepare_pool(4, timeout=1)
            self.assertFalse(pool.closed, 'warmup permanently poisoned the worker pool')
            self.assertEqual(active_parent.execute('SELECT 1 AS n').fetchone()['n'], 1)
        store.prepare_pool(4, timeout=1)
        self.assertIs(store._pool, pool)
        with store.connect() as connection:
            self.assertEqual(connection.execute('SELECT 3 AS n').fetchone()['n'], 3)
        stats = pool.get_stats()
        self.assertEqual(stats['pool_available'], stats['pool_size'])

    def test_explicit_store_close_remains_terminal(self):
        store = self.store(4)
        store.prepare_pool(2, timeout=1)
        pool = store._pool
        store.close()
        with self.assertRaises(PoolClosed):
            with store.connect():
                self.fail('explicitly closed store reopened')
        self.assertIs(store._pool, pool)


if __name__ == '__main__':
    unittest.main(verbosity=2)
