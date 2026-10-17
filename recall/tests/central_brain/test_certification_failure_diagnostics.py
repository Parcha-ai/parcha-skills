import time
import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch

import psycopg

from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment
from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages


class CertificationFailureDiagnosticsTests(unittest.TestCase):
    def test_later_native_failure_keeps_prior_ack_and_exact_scope_without_error_text(self):
        settings = enrollment.CertificationSettings(enabled=True, binding_epoch='test')
        query = Mock()
        query.fetchone.return_value = dict(owner_principal_id='owner', native_parent_id='parent')
        error = psycopg.errors.QueryCanceled('private source text must not be retained')
        with patch.object(enrollment, '_connect', return_value=nullcontext(object())), \
             patch.object(enrollment, '_query', return_value=query), \
             patch.object(enrollment, 'enroll_parent_passages', side_effect=[
                 dict(status='enrolled', passages=128), error]):
            result = enrollment.certify_published_passages(object(), tenant_id='tenant',
                source_id='source', logical_document_id='logical', passage_ids=tuple(range(129)),
                settings=settings, deadline_at=time.monotonic()+10)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['certified_passages'], 128)
        self.assertEqual(result['failure'], dict(error_class='QueryCanceled', sqlstate='57014',
            source_id='source', logical_document_id='logical', native_parent_id='parent'))
        self.assertNotIn('private source', repr(result))

    def test_wrapped_deadline_preserves_native_sqlstate_without_exception_text(self):
        from recall_server.db import SearchDeadlineExceeded
        try:
            try:
                raise psycopg.errors.QueryCanceled('private query parameters')
            except psycopg.errors.QueryCanceled as cause:
                raise SearchDeadlineExceeded('private wrapper text') from cause
        except SearchDeadlineExceeded as error:
            detail = enrollment.certification_failure(error, source_id='source')
        self.assertEqual(detail['error_class'], 'SearchDeadlineExceeded')
        self.assertEqual(detail['sqlstate'], '57014')
        self.assertNotIn('private', repr(detail))

    def test_remainder_connection_failure_identifies_queue_row_without_guessing_parent(self):
        row = dict(tenant_id='tenant', source_id='source', logical_document_id='logical',
                   checkpoint={})
        with patch.object(enrollment, '_connect', side_effect=OSError('private error')):
            result = remainder.resume(object(), row,
                settings=SimpleNamespace(enabled=True), deadline_at=time.monotonic()+10)
        self.assertEqual(result['certified_passages'], 0)
        self.assertEqual(result['failure'], dict(error_class='OSError', sqlstate=None,
            source_id='source', logical_document_id='logical', native_parent_id=None))
        self.assertNotIn('private error', repr(result))

    def test_worker_logs_each_tick_failure_once_and_preserves_counts(self):
        calls = []
        failure = dict(source_id="source\nsecond line", logical_document_id='logical',
            native_parent_id=None, error_class='QueryCanceled', sqlstate='57014')
        class Passages(_Passages):
            def project_pending(self, **kwargs):
                return dict(super().project_pending(**kwargs), membership_failures=[failure],
                    membership_failed_documents=1, membership_certified_passages=128)
        with self.assertLogs('recall_server.projection_worker', level='WARNING') as logs:
            result = run_projection_worker(_Logical(calls, work=0), Passages(calls, work=0),
                once=True, skip_embedding=True, interval_seconds=1, tenant_id='tenant',
                logical_batch_size=1, passage_batch_size=1, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1)
        self.assertEqual(result['membership_failed_documents'], len(logs.output))
        self.assertEqual(result['membership_certified_passages'], 128*len(logs.output))
        self.assertEqual(len(logs.output), calls.count('passages'))
        self.assertIn("source_id='source\\nsecond line'", logs.output[0])
        self.assertIn("error_class='QueryCanceled' sqlstate='57014'", logs.output[0])
        self.assertNotIn('source\nsecond line', logs.output[0])
