"""Metadata fetch keeps the existing proof bounds while avoiding a second request."""
from contextlib import nullcontext
from pathlib import Path
import sys
import time
import types
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / 'server')]
from recall_server import parent_chunk_proof as proof
from recall_server.chunk_retirement import ChunkRetirementError, ParentRetirementLimits
from recall_server.db import SearchDeadlineExceeded


def metadata(index):
    return dict(tenant_id='tenant', source_id='source', document_id=f'doc-{index}',
                native_id=f'native-{index}', revision=1, text_sha256='a' * 64,
                body_record_ordinal=None, body_record_count=None, kind='message',
                raw_media_type='application/json', structural_types=[],
                chunks=[dict(ordinal=0, receipt=f'receipt-{index}', text_sha256='b' * 64, pg_bytes=3)])


class Connection:
    def __init__(self, rows):
        self.rows = rows
        self.requests = []
        self.deadline_calls = 0
        self.result_mode = 'normal'
        self.failure = None

    def transaction(self):
        return nullcontext()

    def execute(self, query):
        assert query == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'

    def cursor(self, name=None):
        connection = self

        class Cursor:
            result = 0

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def execute(self, query, args=None, **kwargs):
                if name:
                    assert name == 'retirement_metadata'
                    return
                connection.requests.append((query.as_string(), kwargs))
                if connection.failure:
                    raise connection.failure

            def fetchmany(self, count):
                raise AssertionError('legacy separate FETCH request')

            def fetchone(self):
                return {'set_config': '1min'} if connection.result_mode != 'bad-setting' else {}

            def nextset(self):
                self.result += 1
                return (self.result == 1 and connection.result_mode != 'missing-fetch') or connection.result_mode == 'extra-result'

            def fetchall(self):
                batch, connection.rows = connection.rows[:32], connection.rows[32:]
                return batch if connection.result_mode != 'oversized-fetch' else [metadata(i) for i in range(33)]

        return Cursor()


class Store:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return nullcontext(self.connection)

    def _set_statement_deadline(self, connection, deadline):
        connection.deadline_calls += 1

    def _execute_bounded(self, *args):
        return type('Parts', (), {'fetchall': lambda self: []})()


class MetadataFetchTests(unittest.TestCase):
    def capture(self, connection, *, deadline=None, **limits):
        catalog = {'manifest': {'logical_document_id': 'logical', 'revision': 1}, 'queue': None}
        with proof.ParentMetadataSpool(max_bytes=1024**2) as spool:
            self.spool_path = Path(spool.directory.name)
            with patch.object(proof, 'read_parent_catalog', return_value=catalog):
                return proof._capture(Store(connection), spool, ('tenant', 'source', 'parent'),
                                      ParentRetirementLimits(**limits), deadline or time.monotonic() + 60)

    def test_fixed_32_rows_and_one_request_per_fetch(self):
        connection = Connection([metadata(i) for i in range(65)])
        self.assertEqual(self.capture(connection)[2:], (65, 65))
        self.assertEqual(len(connection.requests), 4)
        self.assertEqual(connection.deadline_calls, 1)  # Before DECLARE only.
        for query, options in connection.requests:
            self.assertIn('FETCH FORWARD 32 FROM "retirement_metadata"', query)
            self.assertIn("set_config('statement_timeout'", query)
            self.assertEqual(options, {'prepare': False})
        self.assertFalse(self.spool_path.exists())

    def test_expired_before_fetch_and_original_error_identity(self):
        connection = Connection([metadata(1)])
        with self.assertRaises(SearchDeadlineExceeded):
            self.capture(connection, deadline=time.monotonic() - 1)
        self.assertEqual(connection.requests, [])
        sentinel = RuntimeError('private SQL failure')
        connection.failure = sentinel
        with self.assertRaises(RuntimeError) as caught:
            self.capture(connection)
        self.assertIs(caught.exception, sentinel)
        self.assertFalse(self.spool_path.exists())

    def test_remaining_deadline_recomputed_without_extending_it(self):
        connection = Connection([metadata(i) for i in range(33)])
        start = time.monotonic()
        ticks = iter((start, start + .25, start + .49))
        with patch.object(proof, 'time', types.SimpleNamespace(monotonic=lambda: next(ticks))):
            self.capture(connection, deadline=start + .5)
        durations = [int(query.split("statement_timeout', '", 1)[1].split('ms', 1)[0])
                     for query, _ in connection.requests]
        self.assertTrue(0 < durations[2] <= 10 < durations[1] <= 250 < durations[0] <= 500)

    def test_row_chunk_and_serialized_byte_budgets_still_refuse(self):
        for limits in ({'max_documents': 1}, {'max_chunks': 1}):
            with self.assertRaises(ChunkRetirementError):
                self.capture(Connection([metadata(1), metadata(2)]), **limits)
            self.assertFalse(self.spool_path.exists())
        row = metadata(1)
        row['native_id'] = 'x' * (1024**2)
        with self.assertRaises(ChunkRetirementError):
            self.capture(Connection([row]))
        self.assertFalse(self.spool_path.exists())

    def test_exact_two_result_sets_and_batch_bound_required(self):
        for mode in ('bad-setting', 'missing-fetch', 'extra-result', 'oversized-fetch'):
            with self.subTest(mode=mode), self.assertRaises(ChunkRetirementError):
                connection = Connection([metadata(1)])
                connection.result_mode = mode
                self.capture(connection)
            self.assertFalse(self.spool_path.exists())


if __name__ == '__main__':
    unittest.main()
