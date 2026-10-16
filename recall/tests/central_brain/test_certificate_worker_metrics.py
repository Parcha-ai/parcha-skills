"""Certification work must be observable at the existing worker boundaries."""
import unittest

from recall_server.managed_worker import _run_projection_cycle
from recall_server.passage_worker import run_passage_worker
from recall_server.projection_worker import run_projection_worker
from tests.central_brain.test_projection_worker import _Logical, _Passages, _Scan

FIELDS = ('membership_certified_passages', 'membership_failed_documents',
          'membership_deferred_documents', 'membership_elapsed_ms')


class CertificateWorkerMetrics(unittest.TestCase):
    def test_projection_cycle_accumulates_all_publication_ticks_and_logs_them(self):
        calls = []
        class Logical(_Logical):
            def project_pending(self, **kwargs):
                kwargs['on_progress']()
                kwargs['on_progress']()
                return super().project_pending(**kwargs)
        class Passages(_Passages):
            ticks = iter(((2, 0, 0, 11), (1, 1, 0, 19), (0, 0, 1, 7)))
            def project_pending(self, **kwargs):
                return {**super().project_pending(**kwargs), **dict(zip(FIELDS, next(self.ticks)))}
        with self.assertLogs('recall_server.projection_worker', level='INFO') as logs:
            result = run_projection_worker(Logical(calls), Passages(calls),
                tenant_id='tenant:metrics', logical_batch_size=5,
                passage_batch_size=2, embedding_batch_size=2, max_batches_per_cycle=1,
                upload_concurrency=1, passage_concurrency=1, interval_seconds=1,
                once=True, skip_embedding=True)
        for key, expected in zip(FIELDS, (3, 1, 1, 37)):
            self.assertEqual(result[key], expected)
            self.assertIn(f'{key}={expected}', '\n'.join(logs.output))

    def test_standalone_worker_preserves_counts_and_logs_them(self):
        class Passages(_Passages):
            def project_pending(self, **kwargs):
                return {**super().project_pending(**kwargs), **dict(zip(FIELDS, (3, 1, 2, 41)))}
        with self.assertLogs('recall_server.passage_worker', level='INFO') as logs:
            result = run_passage_worker(Passages([]), tenant_id='tenant:metrics',
                projection_batch_size=2, embedding_batch_size=2, max_batches_per_cycle=1,
                concurrency=1, interval_seconds=1, once=True)
        for key, expected in zip(FIELDS, (3, 1, 2, 41)):
            self.assertEqual(result[key], expected)
            self.assertIn(f'{key}={expected}', '\n'.join(logs.output))

    def test_managed_projection_result_and_log_preserve_counts(self):
        class Passages(_Passages):
            def project_pending(self, **kwargs):
                return {**super().project_pending(**kwargs), **dict(zip(FIELDS, (3, 1, 2, 41)))}
        with self.assertLogs('recall_server.managed_worker', level='INFO') as logs:
            result = _run_projection_cycle(_Logical([]), Passages([]), _Scan([]))
        for key, expected in zip(FIELDS, (3, 1, 2, 41)):
            self.assertEqual(result[key], expected)
            self.assertIn(f'{key}={expected}', '\n'.join(logs.output))

    def test_older_projector_results_report_zero_optional_work(self):
        with self.assertLogs('recall_server.managed_worker', level='INFO'):
            result = _run_projection_cycle(_Logical([]), _Passages([]), _Scan([]))
        self.assertEqual({key: result[key] for key in FIELDS}, dict.fromkeys(FIELDS, 0))


if __name__ == '__main__':
    unittest.main()
