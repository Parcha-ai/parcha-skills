"""Index retirement is explicit, bounded, and leaves receipt bodies intact."""
from contextlib import contextmanager
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.storage_retirement import retire_chunk_search_index


class Store:
    def __init__(self, plane='turbopuffer', present=True):
        self.search_plane = plane
        self.present = present
        self.calls = []
        self.autocommit = False

    @contextmanager
    def connect(self):
        yield self

    def execute(self, sql, params=None):
        self.calls.append(sql)
        if sql.startswith('DROP INDEX'):
            self.present = False
        return self

    def fetchone(self):
        return {'bytes': 12345 if self.present else 0, 'present': self.present}


class RetirementTests(unittest.TestCase):
    def test_preview_never_mutates(self):
        store = Store()
        self.assertEqual(retire_chunk_search_index(store)['bytes_before'], 12345)
        self.assertTrue(store.present)
        self.assertFalse(any('DROP' in s or 'SET ' in s for s in store.calls))

    def test_postgres_plane_refused_before_io(self):
        store = Store('postgres')
        with self.assertRaisesRegex(ValueError, 'turbopuffer'):
            retire_chunk_search_index(store, apply=True)
        self.assertEqual(store.calls, [])

    def test_explicit_apply_drops_only_index_with_bounded_waits(self):
        store = Store()
        result = retire_chunk_search_index(store, apply=True)
        self.assertEqual(result['bytes_reclaimed'], 12345)
        self.assertFalse(store.present)
        self.assertFalse(store.autocommit)
        self.assertIn("SET lock_timeout='2s'", store.calls)
        self.assertIn("SET statement_timeout='60s'", store.calls)
        self.assertEqual([s for s in store.calls if 'DROP' in s], [
            'DROP INDEX CONCURRENTLY IF EXISTS public.canonical_chunks_search_idx'])
        self.assertFalse(any('DELETE' in s or 'UPDATE' in s for s in store.calls))

    def test_absent_index_is_noop(self):
        store = Store(present=False)
        result = retire_chunk_search_index(store, apply=True)
        self.assertEqual(result['status'], 'already_absent')
        self.assertEqual(result['bytes_reclaimed'], 0)
        self.assertFalse(any('DROP' in s for s in store.calls))


if __name__ == '__main__':
    unittest.main()
