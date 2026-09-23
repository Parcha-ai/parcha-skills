#!/usr/bin/env python3
"""Real PostgreSQL: canonical commit stays pending until every scan input drains."""
from contextlib import contextmanager
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.parquet_scan import CanonicalParquetScanProjector


class ScanCompleteness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store._pool.close()

    def setUp(self):
        nonce = uuid.uuid4().hex
        self.tenant, self.principal = 'tenant:scan:' + nonce, 'principal:scan:' + nonce
        self.source = 'codex:scan:' + nonce
        self.insert(self.tenant, self.source)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        archive = FilesystemArchiveStore(Path(tmp.name) / 'archive', namespace_key=b's' * 32)
        projection = LogicalEvidenceProjectionStore(archive)
        self.logical = CanonicalLogicalEvidenceProjector(self.store, projection,
            bound_tenant_id=self.tenant, raw_archive=archive)
        self.passages = CanonicalPassageProjector(self.store, projection,
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        self.parquet = CanonicalParquetScanProjector(self.store, projection)
        self.inspector = mock.Mock()
        self.inspector.execute_scan.return_value = dict(stdout='[]', stderr='', exit_code=0,
            complete=True, stopped_reason='completed', output_truncated=False, timing={})

    def insert(self, tenant, source):
        with self.store.connect() as c:
            insert_source(c, tenant, self.principal, source)
            insert_record(c, tenant=tenant, source=source, parent='parent:synthetic', native='turn:synthetic',
                text='Synthetic scan readiness fixture.', role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=tenant, source_id=source,
                native_ids=['turn:synthetic'], reason='ingest')

    def retrieval(self, store=None, sources=None):
        return BoundCanonicalRetrieval(store or self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=tuple(sources or [self.source]),
            deep_inspector=self.inspector)

    def scan(self, *, store=None, filters=None, sources=None):
        return self.retrieval(store, sources).execute_parquet_scan('true', filters=filters or {}, timeout_seconds=10)

    def project_logical(self):
        self.assertEqual(self.logical.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)['documents'], 1)

    def project_passages(self):
        self.assertEqual(self.passages.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, concurrency=1)['documents'], 1)

    def project_parquet(self):
        self.assertEqual(self.parquet.project_pending(tenant_id=self.tenant,
            batch_size=4, max_batches=1, compaction_budget=0)['shards'], 1)

    def test_new_source_logical_then_passage_then_clean_and_scoped_dates(self):
        before = self.scan()
        self.assertFalse(before['complete'])
        self.assertEqual(before['projection_pending'], 1)
        self.assertEqual(before['datasets_available'], 0)
        # A parent's latest event-time range is unknown until projection: even
        # out-of-month reads must not certify a dirty logical parent as absent.
        self.assertFalse(self.scan(filters={'since':'2026-09-01'})['complete'])
        unresolved_person = self.scan(filters={'person':'No projected actor yet'})
        self.assertFalse(unresolved_person['complete'])
        self.assertEqual(unresolved_person['projection_pending'], 1)
        self.project_logical()
        self.assertEqual(self.scan()['projection_pending'], 2)  # passage + Parquet
        self.project_parquet()
        waiting = self.scan()
        self.assertFalse(waiting['complete'])
        self.assertEqual(waiting['projection_pending'], 1)
        self.assertEqual(waiting['stopped_reason'], 'projection_pending')
        self.assertGreater(waiting['datasets_available'], 0)
        # Passage bounds are known: July work does not dirty a September scan.
        september = self.scan(filters={'since':'2026-09-01'})
        self.assertTrue(september['complete'])
        self.assertEqual(september['projection_pending'], 0)
        self.assertTrue(self.scan(filters={'until':'2026-06-30'})['complete'])
        # Shards are admitted by month, not exact day; retain that behavior.
        self.assertFalse(self.scan(filters={'since':'2026-07-30'})['complete'])
        self.project_passages()
        self.assertEqual(self.scan()['projection_pending'], 1)  # replacement Parquet
        self.project_parquet()
        self.assertTrue(self.scan()['complete'])
        self.assertEqual(self.scan()['projection_pending'], 0)
        # Other-source and other-tenant work cannot dirty this clean source.
        self.insert(self.tenant, self.source + ':other')
        self.insert(self.tenant + ':other', self.source)
        self.assertTrue(self.scan()['complete'])
        narrowed = self.scan(filters={'source_id':self.source}, sources=[self.source, self.source + ':other'])
        self.assertTrue(narrowed['complete'])

    def test_publication_between_catalog_read_and_counter_keeps_snapshot_truthful(self):
        owner = self
        published = []
        class Cursor:
            def __init__(self, cursor): self.cursor = cursor
            def fetchall(self):
                rows = self.cursor.fetchall()
                if not published:
                    published.append(True)
                    owner.project_logical()
                    owner.project_passages()
                    owner.project_parquet()
                return rows
        class Connection:
            def __init__(self, connection): self.connection = connection
            def execute(self, statement, params=()):
                cursor = self.connection.execute(statement, params)
                return Cursor(cursor) if 'FROM canonical_parquet_scan_shards' in statement else cursor
        class RacingStore:
            search_deadline_ms = 5000
            @contextmanager
            def connect(self):
                with owner.store.connect() as connection:
                    yield Connection(connection)
        during = self.scan(store=RacingStore())
        self.assertEqual(len(published), 1)
        self.assertEqual(during['datasets_available'], 0)
        self.assertFalse(during['complete'])
        self.assertEqual(during['projection_pending'], 1)
        after = self.scan()
        self.assertTrue(after['complete'])
        self.assertGreater(after['datasets_available'], 0)


if __name__ == '__main__':
    unittest.main()
