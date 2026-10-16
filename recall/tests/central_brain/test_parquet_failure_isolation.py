from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.db import SearchDeadlineExceeded
from recall_server.parquet_scan import CanonicalParquetScanProjector, ScanCandidate
from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages


class ParquetFailureIsolationTests(unittest.TestCase):
    def test_scan_build_deadline_does_not_skip_independent_maintenance_or_reporting(self):
        calls, reported = [], []
        candidate = ScanCandidate('tenant:test', 'source:test', date(2026, 10, 1), 1,
                                  datetime(2026, 10, 16, tzinfo=timezone.utc), 'logical-update')

        class Scan(CanonicalParquetScanProjector):
            def __init__(self):
                self.queued = True
                self.failed_once = False
                self.compaction_fragments = 16

            def _pending(self, **kwargs):
                return [candidate] if self.queued else []

            @contextmanager
            def _candidate_lease(self, value):
                self.asserted_candidate = value
                yield True

            def _build(self, value, **kwargs):
                calls.append('scan-build')
                if not self.failed_once:
                    self.failed_once = True
                    raise SearchDeadlineExceeded('synthetic native membership timeout')
                return SimpleNamespace(created=False, mode='reuse', row_counts={},
                                       references={}, documents_dirty=0)

            def _commit(self, value, upload):
                calls.append('scan-commit')
                self.queued = False
                return 'committed'

            def _over_fragmented(self, **kwargs):
                return []

            def _fragment_total(self, **kwargs):
                return 0

        scan = Scan()

        def search():
            calls.append('search')
            return dict(status='complete', months=1, rows=5, deleted=0, failed=0)

        def thin(busy):
            calls.append('thin')
            return dict(status='complete', documents=1, refused=0,
                        document_bytes_removed=7, event_bytes_replaced=2)

        def retirement(busy):
            calls.append('retirement')
            return dict(status='pending', cleared_documents=1, cleared_utf8_bytes=11)

        options = dict(tenant_id='tenant:test', logical_batch_size=1,
                       passage_batch_size=1, embedding_batch_size=1,
                       max_batches_per_cycle=1, upload_concurrency=1,
                       passage_concurrency=1, interval_seconds=1, once=True,
                       skip_embedding=True, parquet_every_cycles=1,
                       search_plane=search, body_thinner=thin,
                       retirement_maintainer=retirement)
        with patch('recall_server.projection_worker.record_cycle', side_effect=reported.append):
            first = run_projection_worker(_Logical(calls, work=0), _Passages(calls, work=0),
                                          scan, **options)
        self.assertTrue(scan.queued)
        self.assertNotIn('scan-commit', calls)
        self.assertEqual(calls.count('scan-build'), 1)
        self.assertIn('thin', calls)
        self.assertIn('retirement', calls)
        self.assertEqual(first['status'], 'pending')
        self.assertEqual(first['parquet_failed'], 1)
        self.assertEqual(first['parquet_shards'], 0)
        self.assertEqual(first['search_plane_rows'], 5)
        self.assertEqual(first['retirement_cleared_utf8_bytes'], 11)
        self.assertEqual(reported, [first])
        with patch('recall_server.projection_worker.record_cycle'):
            second = run_projection_worker(_Logical(calls, work=0), _Passages(calls, work=0),
                                           scan, **options)
        self.assertFalse(scan.queued)
        self.assertEqual(second['parquet_failed'], 0)
        self.assertEqual(second['parquet_shards'], 1)
        self.assertEqual(calls.count('scan-commit'), 1)

    def test_prior_acknowledged_counts_survive_and_later_admission_stops(self):
        from dataclasses import replace
        from tests.central_brain.test_parquet_scan import _WindowProbe, _candidate

        class Scan(_WindowProbe):
            def _pending(self, **kwargs):
                return [replace(_candidate(), source_id='source:'+name)
                        for name in ('first', 'timeout', 'later')]

            @contextmanager
            def _candidate_lease(self, candidate):
                yield True

            def _build(self, candidate, **kwargs):
                upload = super()._build(candidate, **kwargs)
                if candidate.source_id == 'source:timeout':
                    raise SearchDeadlineExceeded()
                return replace(upload, created=True, documents_dirty=2)

        scan = Scan()
        report = scan.project_pending(batch_size=3, max_batches=2, compaction_budget=1)
        self.assertEqual(report['status'], 'pending')
        self.assertEqual(report['failed'], 1)
        self.assertEqual(report['shards'], 1)
        self.assertEqual(report['rows'], 1)
        self.assertEqual(report['documents_dirty'], 2)
        self.assertEqual(scan.built, ['source:first', 'source:timeout'])
        self.assertFalse(getattr(scan, 'compaction_sweeps', []))

    def test_commit_deadline_is_not_misclassified_as_safe_deferred_build(self):
        from tests.central_brain.test_parquet_scan import _WindowProbe

        class Scan(_WindowProbe):
            def _commit(self, *_):
                raise SearchDeadlineExceeded('synthetic uncertain publication')

        with self.assertRaises(SearchDeadlineExceeded):
            Scan().project_pending(batch_size=1, max_batches=1)
