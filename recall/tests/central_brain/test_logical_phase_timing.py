"""Observed logical maintenance/query boundaries, including failed operations."""
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from tests.central_brain.test_logical_cleanup_concurrency import Store
from tests.central_brain.test_logical_streaming_progress import QueueProjector

MODULE = 'recall_server.logical_evidence_projection'


class Clock:
    now = 0.0

    def advance(self, seconds):
        self.now += seconds

    def __call__(self):
        return self.now


class LogicalPhaseTimingTests(unittest.TestCase):
    def test_cleanup_times_actual_delete_and_reports_acknowledged_counts(self):
        clock, store = Clock(), Store()
        store.rows['artifact'] = dict(tenant_id='tenant:synthetic', source_id='source:synthetic',
            artifact_id='artifact', storage_backend='s3', object_key='private-object',
            content_sha256='a'*64, size_bytes=1, media_type='application/json',
            encryption='none', version_id='v1', created_at=datetime.now(timezone.utc),
            removable=True, attempts=0)
        def delete(reference):
            self.assertTrue(store.in_transaction)
            clock.advance(.125)
            return True
        projector = CanonicalLogicalEvidenceProjector(store, SimpleNamespace(delete_reference=delete),
            bound_tenant_id='tenant:synthetic')
        with patch(MODULE+'.time.perf_counter', clock), self.assertLogs(MODULE, level='INFO') as log:
            result = projector.drain_cleanup()
        self.assertEqual(result['completed'], 1)
        self.assertEqual(store.acknowledged, ['artifact'])
        self.assertEqual(len(log.output), 1)
        self.assertIn('phase=cleanup elapsed_ms=125 succeeded=1 completed=1 deleted=1 failures=0 pending=0', log.output[0])
        self.assertNotIn('private-object', log.output[0])
        self.assertNotIn('tenant:synthetic', log.output[0])

    def test_cleanup_database_failure_is_timed_and_propagated_without_success_counts(self):
        clock = Clock()
        error = RuntimeError('private failure detail')
        class BrokenStore(Store):
            def execute(self, query, values=()):
                clock.advance(.25)
                raise error
        projector = CanonicalLogicalEvidenceProjector(BrokenStore(), None, bound_tenant_id='tenant:synthetic')
        with patch(MODULE+'.time.perf_counter', clock), self.assertLogs(MODULE, level='INFO') as log:
            with self.assertRaises(RuntimeError) as raised:
                projector.drain_cleanup()
        self.assertIs(raised.exception, error)
        self.assertEqual(len(log.output), 1)
        self.assertIn('phase=cleanup elapsed_ms=250 succeeded=0', log.output[0])
        self.assertNotIn('completed=', log.output[0])
        self.assertNotIn('private failure detail', log.output[0])

    def test_pending_and_count_measure_only_their_actual_boundaries(self):
        clock = Clock()
        class Probe(QueueProjector):
            def _pending(self, **kwargs):
                clock.advance(.017)
                return []
        projector = Probe([])
        original = projector.store.execute
        def execute(query, values=()):
            if 'SELECT count(*) AS queued' in query:
                clock.advance(.013)
            return original(query, values)
        projector.store.execute = execute
        with patch(MODULE+'.time.perf_counter', clock), self.assertLogs(MODULE, level='INFO') as log:
            result = projector.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(result['documents'], 0)
        self.assertEqual(len(log.output), 2)
        self.assertIn('phase=pending elapsed_ms=17 succeeded=1', log.output[0])
        self.assertIn('phase=count elapsed_ms=13 succeeded=1', log.output[1])

    def test_pending_failure_is_timed_and_propagates(self):
        clock = Clock()
        error = RuntimeError('private SQL failure')
        class Probe(QueueProjector):
            def _pending(self, **kwargs):
                clock.advance(.019)
                raise error
        projector = Probe([])
        with patch(MODULE+'.time.perf_counter', clock), self.assertLogs(MODULE, level='INFO') as log:
            with self.assertRaises(RuntimeError) as raised:
                projector.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertIs(raised.exception, error)
        self.assertEqual(len(log.output), 1)
        self.assertIn('phase=pending elapsed_ms=19 succeeded=0', log.output[0])
        self.assertNotIn('private SQL failure', log.output[0])

    def test_logging_failure_does_not_mask_cleanup_success_or_error(self):
        projector = CanonicalLogicalEvidenceProjector(Store(), None, bound_tenant_id='tenant:synthetic')
        error = RuntimeError('owning failure')
        with patch(MODULE+'.LOG.info', side_effect=RuntimeError('logger failed')):
            self.assertEqual(projector.drain_cleanup()['completed'], 0)
            with patch.object(projector.store, 'execute', side_effect=error):
                with self.assertRaises(RuntimeError) as raised:
                    projector.drain_cleanup()
            self.assertIs(raised.exception, error)

    def test_count_failure_is_timed_without_reclassifying_error(self):
        clock, error = Clock(), RuntimeError('count failed')
        projector = QueueProjector([])
        def execute(query, values=()):
            clock.advance(.031)
            raise error
        projector.store.execute = execute
        with patch(MODULE+'.time.perf_counter', clock), self.assertLogs(MODULE, level='INFO') as log:
            with self.assertRaises(RuntimeError) as raised:
                projector.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertIs(raised.exception, error)
        self.assertIn('phase=count elapsed_ms=31 succeeded=0', log.output[-1])
