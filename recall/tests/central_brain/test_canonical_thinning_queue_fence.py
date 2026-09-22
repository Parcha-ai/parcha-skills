"""The queue guard remains correlated without changing driver/settings policy."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.canonical_thinning import thin_canonical_bodies  # noqa: E402


class Capture:
    def __init__(self, error=None):
        self.calls = []
        self.error = error
        self.exits = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        self.exits.append(error)

    def execute(self, sql, parameters=None, **kwargs):
        self.calls.append((sql, parameters, kwargs))
        if self.error:
            raise self.error
        return self

    def fetchone(self):
        return dict(candidates=0, documents=0, events=0, document_bytes=0, event_bytes=0)


class ThinningQueueFenceTest(unittest.TestCase):
    def test_only_parent_queue_lookup_has_a_zero_offset_fence(self):
        store = Capture()
        result = thin_canonical_bodies(store, tenant_id='tenant:test', batch_size=10)
        self.assertEqual(result['documents'], 0)
        self.assertEqual(len(store.calls), 1)
        sql, parameters, kwargs = store.calls[0]
        self.assertEqual(parameters, ('tenant:test', 10))
        self.assertEqual(kwargs, {})  # no prepare/GUC/driver change
        self.assertEqual(sql.count('OFFSET 0'), 1)
        start = sql.index('AND NOT EXISTS (')
        end = sql.index('ORDER BY document.source_id', start)
        self.assertIn('OFFSET 0', sql[start:end])
        self.assertEqual(store.exits, [None])

    def test_original_failure_propagates_once(self):
        error = RuntimeError('synthetic failure')
        store = Capture(error)
        with self.assertRaises(RuntimeError) as raised:
            thin_canonical_bodies(store, tenant_id='tenant:test', max_batches=3)
        self.assertIs(raised.exception, error)
        self.assertEqual(store.exits, [error])
        self.assertEqual(len(store.calls), 1)

    def test_invalid_budget_performs_no_sql(self):
        store = Capture()
        with self.assertRaises(ValueError):
            thin_canonical_bodies(store, tenant_id='tenant:test', batch_size=True)
        self.assertEqual(store.calls, [])


if __name__ == '__main__':
    unittest.main()
