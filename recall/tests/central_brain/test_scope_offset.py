"""Scope pagination has page/work bounds, not a corpus-size ceiling."""
from contextlib import contextmanager
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import SearchDeadlineExceeded


class Store:
    search_deadline_ms = 100
    def __init__(self, timeout=False):
        self.calls = []
        self.timeout = timeout
    @contextmanager
    def connect(self):
        yield self
    def _execute_bounded(self, con, sql, params, deadline):
        self.calls.append((sql, params, deadline))
        if self.timeout:
            raise SearchDeadlineExceeded('synthetic deadline')
        return self
    def fetchall(self):
        return []


class ScopeOffsetTests(unittest.TestCase):
    def bound(self, store):
        return BoundCanonicalRetrieval(store, tenant_id='tenant:test',
            principal_id='principal:test', authorized_sources=('source:allowed',))

    def test_late_page_retains_scope_page_size_and_deadline(self):
        store = Store()
        started = time.monotonic()
        result = self.bound(store).scope_documents(
            filters={'source_id':'source:allowed'}, offset=10_080, limit=80)
        self.assertTrue(result['complete'])
        self.assertEqual(result['offset'], 10_080)
        sql, params, deadline = store.calls[0]
        self.assertIn('document.tenant_id=%s', sql)
        self.assertIn('document.source_id=ANY(%s)', sql)
        self.assertEqual(params[:2], ('tenant:test', ['source:allowed']))
        self.assertEqual(params[-2:], (10_080, 81))
        self.assertGreaterEqual(deadline, started)
        self.assertLessEqual(deadline, time.monotonic() + 0.1)

    def test_late_page_deadline_is_incomplete_not_empty_success(self):
        result = self.bound(Store(timeout=True)).scope_documents(offset=10_080)
        self.assertFalse(result['complete'])
        self.assertIsNone(result['total_documents'])
        self.assertEqual(result['diagnostics']['status'], 'deadline-exceeded')

    def test_invalid_offsets_and_page_sizes_still_refuse(self):
        for arguments in ({'offset':-1}, {'offset':True}, {'offset':1.5}, {'offset':'10080'},
                          {'limit':0}, {'limit':81}, {'limit':True}):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                self.bound(Store()).scope_documents(**arguments)

    def test_late_page_cannot_expand_source_authority(self):
        store = Store()
        result = self.bound(store).scope_documents(filters={'source_id':'source:foreign'}, offset=10_080)
        self.assertEqual(result['documents'], [])
        self.assertEqual(store.calls, [])


if __name__ == '__main__':
    unittest.main()
