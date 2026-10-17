"""Parquet proof requests stay bounded without renewing the fetched-batch budget."""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server import parent_membership_read
from recall_server.parquet_scan import CanonicalParquetScanProjector, ScanCandidate
from recall_server.projectors import item_receipt


class Store:
    verified_singleton_receipts = True
    search_deadline_ms = 5000

    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.fetch_sizes = []
        self.restored_deadlines = []
        self.allowed = set(rows[-1]['receipts'])

    @contextmanager
    def connect(self):
        yield self

    def cursor(self, *, name):
        assert name == 'parquet_passage_stream'
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, *args):
        return self

    def fetchmany(self, size):
        self.fetch_sizes.append(size)
        rows, self.rows = self.rows, []
        return rows

    def _execute_bounded(self, connection, sql, parameters, deadline):
        assert connection is self
        self.queries.append((sql, parameters, deadline))
        self.result = [dict(source_id=source, receipt=receipt, allowed=True)
                       for receipt, source in zip(parameters[0], parameters[1])
                       if receipt in self.allowed]
        return self

    def fetchall(self):
        return self.result

    def _set_statement_deadline(self, connection, deadline):
        assert connection is self
        self.restored_deadlines.append(deadline)


class ParquetReceiptSlices(unittest.TestCase):
    def test_native_proof_batches_cover_every_receipt_without_deadline_renewal(self):
        receipts = [item_receipt('source:test', f'event-{i}', 1, 0) for i in range(1001)]
        rows = [dict(passage_id='unproved', receipts=receipts),
                dict(passage_id='proved', receipts=[receipts[-1]])]
        store = Store(rows)
        owner = CanonicalParquetScanProjector(store, SimpleNamespace(archive=None))
        candidate = ScanCandidate('tenant:test', 'source:test', date(2026, 1, 1), 1,
                                  datetime(2026, 1, 1, tzinfo=timezone.utc))
        with patch.object(parent_membership_read.ReadSettings, 'from_env',
                          return_value=parent_membership_read.ReadSettings(enabled=False)):
            actual = list(owner._passages(candidate, ['document']))
        self.assertEqual([row['passage_id'] for row in actual], ['proved'])
        self.assertEqual([len(parameters[0]) for _, parameters, _ in store.queries], [500, 500, 1])
        self.assertEqual([r for _, parameters, _ in store.queries for r in parameters[0]], receipts)
        self.assertEqual(len({deadline for _, _, deadline in store.queries}), 1)
        self.assertEqual(len({sql for sql, _, _ in store.queries}), 1)
        self.assertEqual(store.fetch_sizes, [2000, 2000])
        self.assertEqual(len(store.restored_deadlines), 1)


    def test_later_proof_refusal_yields_no_earlier_authorized_passage(self):
        from recall_server.db import SearchDeadlineExceeded
        receipts = [item_receipt('source:test', f'event-{i}', 1, 0) for i in range(501)]
        store = Store([dict(passage_id='proved', receipts=[receipts[0]]),
                       dict(passage_id='large', receipts=receipts)])
        store.allowed = {receipts[0]}
        owner = CanonicalParquetScanProjector(store, SimpleNamespace(archive=None))
        candidate = ScanCandidate('tenant:test', 'source:test', date(2026, 1, 1), 1,
                                  datetime(2026, 1, 1, tzinfo=timezone.utc))
        original = store._execute_bounded
        def refuse(connection, sql, parameters, deadline):
            if store.queries:
                raise SearchDeadlineExceeded('synthetic second-unit deadline')
            return original(connection, sql, parameters, deadline)
        with patch.object(parent_membership_read.ReadSettings, 'from_env',
                          return_value=parent_membership_read.ReadSettings(enabled=False)), \
             patch.object(store, '_execute_bounded', side_effect=refuse):
            iterator = owner._passages(candidate, ['document'])
            with self.assertRaises(SearchDeadlineExceeded):
                next(iterator)
        self.assertEqual(len(store.queries), 1)
        self.assertEqual(store.restored_deadlines, [])

    def test_all_certified_batch_makes_no_fallback_query(self):
        rows = [dict(passage_id='certified', receipts=[item_receipt('source:test', 'event', 1, 0)],
                     tenant_id='tenant:test', source_id='source:test', text_sha256='f'*64,
                     manifest_content_sha256='a'*64)]
        store = Store(rows)
        store._set_statement_deadline = lambda connection, deadline: None
        owner = CanonicalParquetScanProjector(store, SimpleNamespace(archive=None))
        candidate = ScanCandidate('tenant:test', 'source:test', date(2026, 1, 1), 1,
                                  datetime(2026, 1, 1, tzinfo=timezone.utc))
        with patch.object(parent_membership_read.ReadSettings, 'from_env',
                          return_value=parent_membership_read.ReadSettings(enabled=True, binding_epoch="fixture")), \
             patch.object(parent_membership_read, 'partition', return_value=({('source:test', 'certified')}, {})):
            actual = list(owner._passages(candidate, ['document']))
        self.assertEqual([row['passage_id'] for row in actual], ['certified'])
        self.assertEqual(store.queries, [])
        self.assertNotIn('manifest_content_sha256', actual[0])


if __name__ == '__main__':
    unittest.main()
