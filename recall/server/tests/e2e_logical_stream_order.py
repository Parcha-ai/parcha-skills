#!/usr/bin/env python3
"""The worker streams one parent in source order without sorting wide bodies."""
from contextlib import contextmanager
from pathlib import Path
import os
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty,
)


class StreamOrder(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.tenant = 'tenant:stream-order:' + uuid.uuid4().hex
        self.source = 'codex:stream-order'
        with self.store.connect() as connection:
            insert_source(connection, self.tenant, 'principal:stream-order', self.source)
            for index, ordinal in enumerate([None, 4, 0, None, 4, -2]):
                native = f'event-{index}'
                insert_record(connection, tenant=self.tenant, source=self.source,
                    parent='parent', native=native, text=f'Synthetic ordered evidence {index}.',
                    role='user', byte_start=ordinal or 0)
                # Migration 057 made this an ingestion-owned scalar.
                connection.execute('UPDATE canonical_events SET source_ordinal=%s WHERE tenant_id=%s AND source_id=%s AND native_id=%s',
                    (ordinal, self.tenant, self.source, native))
            mark_logical_evidence_dirty(connection, tenant_id=self.tenant,
                source_id=self.source, native_ids=['event-0'], reason='ingest')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        archive = FilesystemArchiveStore(Path(temporary.name), namespace_key=b'o' * 32)
        self.projector = CanonicalLogicalEvidenceProjector(self.store,
            LogicalEvidenceProjectionStore(archive), bound_tenant_id=self.tenant,
            raw_archive=archive)

    def test_worker_keeps_source_order_without_global_body_sort(self):
        plans, observed = [], []
        connect = self.store.connect
        @contextmanager
        def instrumented_connect():
            with connect() as connection:
                class Connection:
                    def __getattr__(self, name):
                        return getattr(connection, name)
                    def cursor(self, *args, **kwargs):
                        cursor = connection.cursor(*args, **kwargs)
                        if kwargs.get('name') != 'logical_evidence_batch_stream':
                            return cursor
                        class Cursor:
                            def __enter__(self):
                                cursor.__enter__()
                                return self
                            def __exit__(self, *args):
                                return cursor.__exit__(*args)
                            def execute(self, query, params):
                                text = query if isinstance(query, str) else query.as_string(connection)
                                # Make the existing index available even on this tiny fixture.
                                # No serving planner settings are changed by the implementation.
                                connection.execute('SET LOCAL enable_seqscan=off')
                                connection.execute('SET LOCAL enable_bitmapscan=off')
                                plan = connection.execute('EXPLAIN (FORMAT JSON) DECLARE probe NO SCROLL CURSOR FOR ' + text, params).fetchone()['QUERY PLAN'][0]['Plan']
                                plans.append(plan)
                                cursor.execute(query, params)
                            def __iter__(self):
                                for row in cursor:
                                    observed.append((row['native_id'], row['byte_start']))
                                    yield row
                        return Cursor()
                yield Connection()
        with patch.object(self.store, 'connect', instrumented_connect):
            result = self.projector.project_pending(batch_size=1, max_batches=1,
                upload_concurrency=1)
        self.assertEqual(result['documents'], 1)
        self.assertEqual(observed, [('event-5', -2), ('event-2', 0),
            ('event-1', 4), ('event-4', 4), ('event-0', None), ('event-3', None)])
        self.assertEqual(len(plans), 1)
        self.assertNotEqual(plans[0]['Node Type'], 'Sort',
            'worker parent cursor sorts all hydrated bodies before emitting its first row')


if __name__ == '__main__':
    unittest.main()
