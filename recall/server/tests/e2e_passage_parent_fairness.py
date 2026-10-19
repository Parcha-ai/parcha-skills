#!/usr/bin/env python3
"""Real queue admission: unavailable siblings cannot pin middle history."""
from contextlib import contextmanager
from datetime import timedelta
import threading
import unittest
from unittest.mock import patch

from e2e_passage_set_admission import SetAdmission
from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.passage_index import PreparedPassageDocument


class PassageParentFairness(SetAdmission):
    def setUp(self):
        super().setUp()
        self.connection.execute('''CREATE TEMP TABLE canonical_passage_documents (
            tenant_id text, source_id text, logical_document_id text,
            requested_extraction_version text,
            PRIMARY KEY(tenant_id,source_id,logical_document_id))''')
        self.projector.store.pool_max_size = 4

    def test_known_unavailable_extremes_do_not_starve_healthy_middle(self):
        self.add('unavailable-oldest', [1], age=900)
        self.add('healthy-middle', [1], age=600)
        self.add('unavailable-newest', [1], age=10)
        before = self.connection.execute('''SELECT * FROM canonical_passage_projection_queue
            WHERE logical_document_id<>'healthy-middle' ORDER BY logical_document_id''').fetchall()
        attempts = []
        def prepare(candidate, *, incremental=False):
            self.assertTrue(incremental)
            attempts.append(candidate.logical_document_id)
            if candidate.logical_document_id.startswith('unavailable-'):
                raise LogicalEvidenceError('logical_evidence_unavailable')
            return PreparedPassageDocument(candidate, 0, 0, ())
        def commit(prepared):
            candidate = prepared.candidate
            self.connection.execute('''DELETE FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                  AND revision=%s AND generation=%s''',
                (candidate.tenant_id,candidate.source_id,candidate.logical_document_id,
                 candidate.revision,candidate.generation))
            return dict(status='complete',inserted=1,deleted=0,retained=0)
        reports = []
        with patch.object(self.projector, '_prepare', prepare), patch.object(self.projector, '_commit', commit):
            for _ in range(6):
                reports.append(self.projector.project_pending(
                    tenant_id='a',batch_size=1,max_batches=1,concurrency=1))
        self.assertIn('healthy-middle', attempts, attempts)
        self.assertEqual(len(attempts), 6, 'one original attempt per turn; no hidden refill')
        after = self.connection.execute('''SELECT * FROM canonical_passage_projection_queue
            ORDER BY logical_document_id''').fetchall()
        self.assertEqual(after, before, 'known refusals retain their exact queue generation and age')
        self.assertEqual(sum(report['documents'] for report in reports), 1)

    def test_production_two_slot_waves_progress_across_sources_with_failed_endpoints(self):
        # Saved production command: passage_batch_size=100, concurrency=2;
        # publish_ready uses min(100,2), max_batches=1.
        for source in ('a','b','c','d'):
            self.add('bad-old', [1], source=source, age=900)
            self.add('middle', [2], source=source, age=600)
            self.add('bad-new', [1], source=source, age=10)
        lock = threading.RLock()
        @contextmanager
        def connect():
            with lock:
                yield self.connection
        self.projector.store.connect = connect
        attempted, committed = [], []
        def prepare(candidate, *, incremental=False):
            with lock:
                attempted.append((candidate.source_id,candidate.logical_document_id))
            if candidate.logical_document_id.startswith('bad-'):
                raise LogicalEvidenceError('legacy_tool_migration_required')
            return PreparedPassageDocument(candidate,0,0,())
        def commit(prepared):
            candidate=prepared.candidate
            with lock:
                committed.append((candidate.source_id,candidate.logical_document_id))
                self.connection.execute('''DELETE FROM canonical_passage_projection_queue WHERE
                    tenant_id=%s AND source_id=%s AND logical_document_id=%s AND generation=%s''',
                    (candidate.tenant_id,candidate.source_id,candidate.logical_document_id,candidate.generation))
            return dict(status='complete',inserted=1,deleted=0,retained=0)
        with patch.object(self.projector,'_prepare',prepare),patch.object(self.projector,'_commit',commit):
            for _ in range(8):
                result=self.projector.project_pending(tenant_id='a',batch_size=2,max_batches=1,concurrency=2)
                self.assertEqual(result['batches'],1)
        self.assertEqual(set(committed),{(source,'middle') for source in ('a','b','c','d')})
        self.assertEqual(len(attempted),16)
        self.assertEqual(len(set(source for source,_ in attempted[:8])),4)
        self.assertEqual(self.connection.execute('SELECT count(*) AS n FROM canonical_passage_projection_queue').fetchone()['n'],8)

    def test_deleted_or_moved_failed_key_does_not_pin_sibling_admission(self):
        self.add('failed', [1], age=900)
        self.add('middle', [1], age=600)
        self.add('new', [1], age=10)
        with patch.object(self.projector,'_prepare',side_effect=LogicalEvidenceError('logical_evidence_unavailable')):
            self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)
        for moved in (True,False):
            with self.subTest(moved=moved):
                if moved:
                    self.connection.execute("UPDATE canonical_passage_projection_queue SET changed_at=%s,generation=generation+1 WHERE logical_document_id='failed'",(self.now+timedelta(seconds=1),))
                else:
                    self.connection.execute("DELETE FROM canonical_passage_projection_queue WHERE logical_document_id='failed'")
                self.projector._prefer_notification_admission=False
                rows=self.projector._pending(tenant_id='a',limit=1)
                self.assertEqual([row.logical_document_id for row in rows],['middle'])

    def test_trusted_notification_fifo_still_precedes_failed_source_cursor(self):
        self.add('old', [1], age=900)
        self.add('new', [1], age=10)
        self.projector._prefer_notification_admission=True
        with patch.object(self.projector,'_prepare',side_effect=LogicalEvidenceError('logical_evidence_unavailable')):
            self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)
        self.add('first', [900000], age=1, notified=300)
        self.add('second', [1], age=1, notified=100)
        self.projector._prefer_notification_admission=True
        rows=self.projector._pending(tenant_id='a',limit=2)
        self.assertEqual([row.logical_document_id for row in rows],['first','second'])

    def test_unknown_commit_stops_without_moving_failure_cursor_or_replaying(self):
        self.add('old', [1], age=900)
        self.add('middle', [1], age=600)
        original=self.connection.execute('SELECT * FROM canonical_passage_projection_queue ORDER BY logical_document_id').fetchall()
        def prepare(candidate, *, incremental=False):
            return PreparedPassageDocument(candidate,0,0,())
        with patch.object(self.projector,'_prepare',prepare),patch.object(self.projector,'_commit',side_effect=RuntimeError('commit outcome unknown')) as commit:
            with self.assertRaisesRegex(RuntimeError,'commit outcome unknown'):
                self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=2,concurrency=1)
        self.assertEqual(commit.call_count,1)
        self.assertEqual(self.projector._ordinary_parent_cursor,{False:{},True:{}})
        self.assertEqual(self.connection.execute('SELECT * FROM canonical_passage_projection_queue ORDER BY logical_document_id').fetchall(),original)

    def test_logical_pending_does_not_hide_complete_current_passage_candidate(self):
        self.add('current', [1,2], revision=14)
        self.connection.execute('ALTER TABLE canonical_evidence_documents ADD COLUMN native_parent_id text')
        self.connection.execute("UPDATE canonical_evidence_documents SET native_parent_id='native-current'")
        self.connection.execute('''CREATE TEMP TABLE canonical_evidence_document_queue (
            tenant_id text,source_id text,native_parent_id text,generation bigint,
            reason text,changed_at timestamptz,
            PRIMARY KEY(tenant_id,source_id,native_parent_id))''')
        self.connection.execute("INSERT INTO canonical_evidence_document_queue VALUES ('a','x','native-current',2,'append',now())")
        candidate,=self.projector._pending(tenant_id='a',limit=1)
        self.assertEqual((candidate.logical_document_id,candidate.revision),('current',14))
        self.assertEqual(len(candidate.part_references),2)

    def test_mixed_missing_and_failed_batch_keeps_original_admission_order(self):
        self.add('bad-prepare', [1], age=900)
        self.add('missing-parts', [], age=800)
        self.add('healthy-middle', [1], age=600)
        self.add('new', [1], age=10)
        with patch.object(self.projector,'_prepare',side_effect=LogicalEvidenceError('logical_evidence_unavailable')):
            self.projector.project_pending(tenant_id='a',batch_size=2,max_batches=1,concurrency=2)
        self.projector._prefer_notification_admission=False
        rows=self.projector._pending(tenant_id='a',limit=1)
        self.assertEqual([row.logical_document_id for row in rows],['healthy-middle'])

    def test_known_refusal_logs_only_fixed_status_and_identity(self):
        self.add('old', [1], age=900)
        statuses=('logical_evidence_unavailable','legacy_tool_migration_required','logical_evidence_not_found')
        for status in statuses:
            with self.subTest(status=status),self.assertLogs('recall_server.passage_index',level='WARNING') as logs:
                with patch.object(self.projector,'_prepare',side_effect=LogicalEvidenceError(status)),patch.object(self.projector,'_requeue_missing',return_value=0):
                    self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)
            self.assertEqual([row.getMessage() for row in logs.records],[
                f"passage preparation refused source_id='x' logical_document_id='old' status={status}"])
        with patch.object(self.projector,'_prepare',side_effect=LogicalEvidenceError('private unexpected details')):
            with self.assertNoLogs('recall_server.passage_index',level='WARNING'):
                with self.assertRaisesRegex(LogicalEvidenceError,'private unexpected details'):
                    self.projector.project_pending(tenant_id='a',batch_size=1,max_batches=1,concurrency=1)


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageParentFairness(name) for name in PassageParentFairness.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
