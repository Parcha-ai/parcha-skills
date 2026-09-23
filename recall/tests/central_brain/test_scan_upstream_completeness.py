"""Scan completeness includes unmaterialized, source-scoped queue work."""
from contextlib import contextmanager
from datetime import date
import unittest
from unittest import mock

from recall_server.canonical_retrieval import BoundCanonicalRetrieval

class Store:
    search_deadline_ms = 5000
    def __init__(self, *, logical=0, passage=0, parquet=0, rows=()):
        self.counts = dict(canonical_evidence_document_queue=logical,
            canonical_passage_projection_queue=passage, canonical_parquet_scan_queue=parquet)
        self.rows = list(rows)
        self.calls = []
    @contextmanager
    def connect(self):
        yield self
    def execute(self, statement, params=()):
        self.calls.append((statement, params))
        return mock.Mock(fetchall=lambda: self.rows,
            fetchone=lambda: dict(count=sum(count for table,count in self.counts.items() if table in statement)))

class UpstreamCompletenessTests(unittest.TestCase):
    def scan(self, store, *, person=False):
        inspector = mock.Mock()
        inspector.execute_scan.return_value = dict(stdout='[]', stderr='', exit_code=0,
            complete=True, stopped_reason='completed', output_truncated=False, timing={})
        retrieval = BoundCanonicalRetrieval(store, tenant_id='tenant:test', principal_id='principal:test',
            authorized_sources=('source:test',), deep_inspector=inspector)
        with mock.patch.object(retrieval, '_parquet_person_sources', return_value=[]):
            result = retrieval.execute_parquet_scan('true', filters={'person':'Synthetic'} if person else {}, timeout_seconds=10)
        return result

    def test_new_source_is_pending_before_any_parquet_catalog_exists(self):
        store = Store(logical=1)
        result = self.scan(store)
        self.assertFalse(result['complete'])
        self.assertEqual(result['projection_pending'], 1)
        self.assertEqual(result['datasets_available'], 0)
        self.assertEqual(result['stopped_reason'], 'projection_pending')

    def test_passage_only_backlog_is_pending(self):
        result = self.scan(Store(passage=1))
        self.assertFalse(result['complete'])
        self.assertEqual(result['projection_pending'], 1)

    def test_person_not_yet_projected_does_not_hide_pending_source(self):
        result = self.scan(Store(logical=1), person=True)
        self.assertFalse(result['complete'])
        self.assertEqual(result['projection_pending'], 1)
        self.assertEqual(result['sources_available'], 0)

    def test_catalog_and_queue_counts_use_one_snapshot(self):
        store = Store()
        self.assertTrue(self.scan(store)['complete'])
        self.assertIn('REPEATABLE READ', store.calls[0][0])
        self.assertIn('READ ONLY', store.calls[0][0])

    def test_pending_units_are_queue_work_and_nonempty_reason_is_truthful(self):
        row = dict(tenant_id='tenant:test', artifact_id='art_'+'a'*32,
            storage_backend='s3', size_bytes=12, media_type='application/vnd.apache.parquet',
            encryption='sse-s3', version_id='r2-sha256-'+'b'*64, created_at='2026-09-23T00:00:00Z',
            source_id='source:test', bucket_start=date(2026,9,1), dataset='documents',
            shard_index=0, object_key='objects/aa/'+'a'*64, content_sha256='b'*64)
        result = self.scan(Store(logical=2, passage=3, parquet=1, rows=[row]))
        self.assertFalse(result['complete'])
        self.assertEqual(result['projection_pending'], 6)
        self.assertEqual(result['stopped_reason'], 'projection_pending')
