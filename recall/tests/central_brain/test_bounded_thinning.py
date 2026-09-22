"""Cursor state is published only after an acknowledged thinning transaction."""
import unittest
from contextlib import contextmanager
from recall_server.canonical_thinning import CanonicalBodyThinner


class Store:
    def __init__(self, keys, results, *, fail_commit=False):
        self.keys, self.results = list(keys), list(results)
        self.fail_commit = fail_commit
        self.calls = []

    @contextmanager
    def connect(self):
        yield self
        if self.fail_commit:
            raise RuntimeError('unknown commit')

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if sql.lstrip().startswith('SELECT source_id,document_id') and 'ORDER BY source_id DESC' in sql:
            self.row = {'source_id': 's', 'document_id': 'z'}
        elif sql.lstrip().startswith('SELECT source_id,document_id'):
            self.rows = self.keys.pop(0)
        elif 'updated_documents AS' not in sql:
            result = self.results[0]
            self.rows = ([dict(source_id=result['last_source'],
                               document_id=result['last_document'])]
                         if result['candidates'] else [])
            if not self.rows:
                self.results.pop(0)
        else:
            self.row = self.results.pop(0)
        return self

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


def result(count=0, end=None):
    return dict(candidates=count, documents=count, events=count,
                document_bytes=count * 10, event_bytes=count * 20,
                last_source='s' if end else None, last_document=end)


def keys(*values):
    return [dict(source_id='s', document_id=value) for value in values]


class CursorTests(unittest.TestCase):
    def test_empty_eligibility_advances_and_wraps_without_claiming_completion(self):
        store = Store([keys('a', 'b'), keys('z')], [result(), result()])
        thinner = CanonicalBodyThinner(store, tenant_id='tenant')
        first = thinner.thin(batch_size=10)
        self.assertEqual(thinner._after, ('s', 'b'))
        self.assertEqual(first['status'], 'pending')
        second = thinner.thin(batch_size=10)
        self.assertIsNone(thinner._after)
        self.assertIsNone(thinner._through)
        self.assertTrue(second['pass_complete'])
        self.assertEqual(second['status'], 'pending')

    def test_full_batch_preserves_unprocessed_window_suffix(self):
        store = Store([keys('a', 'b', 'z')], [result(1, 'a')])
        thinner = CanonicalBodyThinner(store, tenant_id='tenant')
        thinner.thin(batch_size=1)
        self.assertEqual(thinner._after, ('s', 'a'))

    def test_unknown_commit_publishes_no_cursor(self):
        store = Store([keys('a')], [result()], fail_commit=True)
        thinner = CanonicalBodyThinner(store, tenant_id='tenant')
        with self.assertRaisesRegex(RuntimeError, 'unknown commit'):
            thinner.thin(batch_size=10)
        self.assertIsNone(thinner._after)
        self.assertIsNone(thinner._through)

    def test_statement_failure_preserves_prior_cursor(self):
        store = Store([keys('a')], [])
        thinner = CanonicalBodyThinner(store, tenant_id='tenant')
        thinner._after, thinner._through = ('s', '0'), ('s', 'z')
        with self.assertRaises(IndexError):
            thinner.thin(batch_size=10)
        self.assertEqual(thinner._after, ('s', '0'))
        self.assertEqual(thinner._through, ('s', 'z'))

    def test_budget_invalid_before_connection(self):
        store = Store([], [])
        thinner = CanonicalBodyThinner(store, tenant_id='tenant')
        for batch in (True, 0, 10001, '10'):
            with self.assertRaises(ValueError):
                thinner.thin(batch_size=batch)
        self.assertEqual(store.calls, [])
