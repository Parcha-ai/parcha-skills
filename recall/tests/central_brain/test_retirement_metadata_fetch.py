"""One bounded metadata stream retains the original proof and deadline limits."""
from contextlib import nullcontext
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / 'server')]
from recall_server import parent_chunk_proof as proof, receipt_membership
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
        self.deadlines = []
        self.failure = None
        self.closed = False

    def transaction(self):
        return nullcontext()

    def execute(self, query):
        assert query == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'

    def cursor(self):
        connection = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                connection.closed = True

            def stream(self, query, args, *, size):
                connection.requests.append((query, args, size))
                if connection.failure:
                    raise connection.failure
                yield from connection.rows

        return Cursor()


class Store:
    def __init__(self, connection, verified=False):
        self.connection = connection
        self.verified_singleton_receipts = verified

    def connect(self):
        return nullcontext(self.connection)

    def _set_statement_deadline(self, connection, deadline):
        connection.deadlines.append(deadline)
        if deadline is not None and deadline <= time.monotonic():
            raise SearchDeadlineExceeded('search deadline exceeded')

    def _execute_bounded(self, *args):
        return type('Parts', (), {'fetchall': lambda self: []})()


class MetadataFetchTests(unittest.TestCase):
    def capture(self, connection, *, deadline=None, verified=False, **limits):
        catalog = {'manifest': {'logical_document_id': 'logical', 'revision': 1}, 'queue': None}
        with proof.ParentMetadataSpool(max_bytes=1024**2) as spool:
            self.spool_path = Path(spool.directory.name)
            with patch.object(proof, 'read_parent_catalog', return_value=catalog):
                return proof._capture(Store(connection, verified), spool, ('tenant', 'source', 'parent'),
                                      ParentRetirementLimits(**limits), deadline or time.monotonic() + 60)

    def test_fixed_256_result_chunks_and_one_metadata_request(self):
        connection = Connection([metadata(i) for i in range(257)])
        self.assertEqual(self.capture(connection)[2:], (257, 257))
        self.assertEqual(len(connection.requests), 1)
        self.assertEqual(len(connection.deadlines), 1)
        query, args, size = connection.requests[0]
        self.assertEqual(size, 256)
        self.assertIn('SELECT selected.*,items.chunks FROM (', query)
        limits = ParentRetirementLimits()
        self.assertEqual(args, ('tenant', 'source', 'parent', limits.max_documents+1, limits.batch_chunks+1))
        self.assertTrue(connection.closed)
        self.assertFalse(self.spool_path.exists())

    def test_expired_before_stream_and_original_error_identity(self):
        connection = Connection([metadata(1)])
        with self.assertRaises(SearchDeadlineExceeded):
            self.capture(connection, deadline=time.monotonic() - 1)
        self.assertEqual(connection.requests, [])
        sentinel = RuntimeError('private SQL failure')
        connection.failure = sentinel
        with self.assertRaises(RuntimeError) as caught:
            self.capture(connection)
        self.assertIs(caught.exception, sentinel)
        self.assertTrue(connection.closed)
        self.assertFalse(self.spool_path.exists())

    def test_one_stream_reuses_absolute_deadline_and_bounded_native_rows(self):
        rows = [metadata(i) for i in range(257)]
        for i, row in enumerate(rows):
            row.update(membership_current=True, membership_deleted=None,
                       membership_profile=None, membership_physical=False)
            row['chunks'][0].update(chunk_id=f'chunk-{i}',
                                   receipt=f'recall://source/native-{i}?rev=1#item=0')
        connection = Connection(rows)
        deadline = time.monotonic() + 60
        sizes = []
        native = receipt_membership.receipt_metadata_from_rows

        def verified(records, **kwargs):
            sizes.append(len(records))
            return native(records, **kwargs)

        with patch.object(receipt_membership, 'receipt_metadata_from_rows', verified):
            self.assertEqual(self.capture(connection, deadline=deadline, verified=True)[2:], (257, 257))
        self.assertEqual(connection.deadlines, [deadline])
        self.assertEqual(sizes, [1] * 257)
        self.assertEqual(len(connection.requests), 1)

    def test_midstream_deadline_discards_spool(self):
        connection = Connection([metadata(i) for i in range(3)])
        with patch.object(proof, '_check_deadline', side_effect=[None, SearchDeadlineExceeded('search deadline exceeded')]):
            with self.assertRaises(SearchDeadlineExceeded):
                self.capture(connection)
        self.assertTrue(connection.closed)
        self.assertFalse(self.spool_path.exists())

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

    def test_unexpected_metadata_shape_still_refuses(self):
        for extra in ('row', 'chunk'):
            row = metadata(1)
            if extra == 'row':
                row['unexpected'] = True
            else:
                row['chunks'][0]['unexpected'] = True
            with self.subTest(extra=extra), self.assertRaises(ChunkRetirementError):
                self.capture(Connection([row]))
            self.assertFalse(self.spool_path.exists())


if __name__ == '__main__':
    unittest.main()
