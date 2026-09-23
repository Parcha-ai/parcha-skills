"""Busy thinning yields cycles without changing its row-authority callback."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from recall_server import projection_worker as worker
from tests.central_brain.test_projection_worker import _Logical, _Passages, _Scan


class BusyThinningCadenceTest(unittest.TestCase):
    def exercise(self, times, *, busy=None, fail_thin=(), fail_logical=(), scan_delay=None,
                 enabled=True, once=False):
        calls, attempts, results, sleeps = [], [], [], []
        clock = [0.0]
        current = [0]
        busy = busy or [True] * len(times)
        errors = {cycle: RuntimeError('synthetic') for cycle in fail_thin}

        class Logical(_Logical):
            def project_pending(self, **kwargs):
                current[0] += 1
                cycle = current[0]
                clock[0] = times[cycle - 1]
                self.pending = int(busy[cycle - 1])
                value = super().project_pending(**kwargs)
                if cycle in fail_logical:
                    raise RuntimeError('synthetic earlier phase')
                return value

        class Scan(_Scan):
            def project_pending(self, **kwargs):
                value = super().project_pending(**kwargs)
                clock[0] += (scan_delay or {}).get(current[0], 0)
                return value

        def thin(is_busy):
            calls.append('thin')
            attempts.append((current[0], is_busy, clock[0]))
            if current[0] in errors:
                raise errors[current[0]]
            return dict(status='complete', documents=7, refused=2,
                        document_bytes_removed=70, event_bytes_replaced=140)

        def search():
            calls.append('search')
            return dict(months=0, rows=0, deleted=0, failed=0)

        original_record = worker.record_cycle

        def record(result):
            results.append(dict(result))
            original_record(result)

        before = worker.projection_totals()
        with patch.object(worker, 'record_cycle', side_effect=record), patch.object(worker.LOG, 'exception'):
            with self.assertLogs(worker.LOG, level='INFO') as logs:
                result = worker.run_projection_worker(
                    Logical(calls, work=0), _Passages(calls, work=0), Scan(calls, work=0),
                    tenant_id='tenant:test', logical_batch_size=1, passage_batch_size=1,
                    embedding_batch_size=1, max_batches_per_cycle=1, upload_concurrency=1,
                    passage_concurrency=1, interval_seconds=1, clock=lambda: clock[0],
                    sleep=sleeps.append, max_cycles=len(times), once=once,
                    parquet_every_cycles=1, search_plane=search,
                    body_thinner=thin if enabled else None,
                )
        after = worker.projection_totals()
        self.assertEqual(after['bodies_thinned'] - before['bodies_thinned'],
                         sum(row['canonical_bodies_thinned'] for row in results))
        return result, attempts, results, calls, sleeps, logs.output

    def test_first_busy_and_every_third_cycle_without_starvation(self):
        _, attempts, results, calls, _, logs = self.exercise([0] * 8)
        self.assertEqual([a[0] for a in attempts], [1, 4, 7])
        self.assertEqual([r['thin_deferred'] for r in results], [0, 1, 1, 0, 1, 1, 0, 1])
        expected = []
        for cycle in range(1, 9):
            expected.extend(['embeddings', 'passages', 'search', 'logical', 'scan'])
            if cycle in (1, 4, 7):
                expected.append('thin')
        self.assertEqual(calls, expected)
        self.assertTrue(any('canonical_bodies_thinned=0' in line and 'thin_mode=busy' in line for line in logs))
        self.assertTrue(all('thin_deferred=' not in line for line in logs))
        for result in results:
            self.assertEqual(result['thin_mode'], 'busy')
            if result['thin_deferred']:
                self.assertEqual(result['status'], 'pending')
                for key in ('canonical_bodies_thinned', 'canonical_bodies_refused',
                            'canonical_document_bytes_removed', 'canonical_event_bytes_replaced',
                            'thin_elapsed_ms'):
                    self.assertEqual(result[key], 0)

    def test_thirty_seconds_runs_before_third_cycle_boundary(self):
        _, attempts, results, *_ = self.exercise([0, 29.999, 30])
        self.assertEqual([a[0] for a in attempts], [1, 3])
        self.assertEqual([r['thin_deferred'] for r in results], [0, 1, 0])

    def test_time_is_checked_after_long_prior_phase_not_a_hard_deadline(self):
        _, attempts, results, *_ = self.exercise([0, 1], scan_delay={2: 90})
        self.assertEqual(attempts, [(1, True, 0), (2, True, 91)])
        self.assertEqual(results[1]['parquet_elapsed_ms'], 90000)
        self.assertEqual(results[1]['thin_deferred'], 0)

    def test_idle_immediate_and_new_busy_streak_runs_first_boundary(self):
        _, attempts, results, *_ = self.exercise([0] * 7, busy=[True, True, False, False, True, True, True])
        self.assertEqual([(a[0], a[1]) for a in attempts], [(1, True), (3, False), (4, False), (5, True)])
        self.assertEqual([r['thin_deferred'] for r in results], [0, 1, 0, 0, 0, 1, 1])

    def test_failed_thinner_keeps_existing_next_cycle_retry_no_fake_success(self):
        _, attempts, results, _, sleeps, _ = self.exercise([0] * 5, fail_thin=(1,))
        self.assertEqual([a[0] for a in attempts], [1, 2, 5])
        self.assertEqual(len(results), 4)
        self.assertEqual(sum(r['canonical_bodies_thinned'] for r in results), 14)
        self.assertEqual(sleeps[0], 1)

    def test_earlier_failed_cycle_does_not_consume_first_thin_opportunity(self):
        _, attempts, results, *_ = self.exercise([0] * 3, fail_logical=(1,))
        self.assertEqual([a[0] for a in attempts], [2])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[1]['thin_deferred'], 1)

    def test_once_failure_propagates_original_error(self):
        error = RuntimeError('synthetic original')
        with patch.object(worker.LOG, 'exception'), self.assertRaises(RuntimeError) as caught:
            worker.run_projection_worker(
                _Logical([], work=0, pending=1), _Passages([], work=0),
                tenant_id='tenant:test', logical_batch_size=1, passage_batch_size=1,
                embedding_batch_size=1, max_batches_per_cycle=1, upload_concurrency=1,
                passage_concurrency=1, interval_seconds=1, once=True,
                body_thinner=lambda busy: (_ for _ in ()).throw(error),
            )
        self.assertIs(caught.exception, error)

    def test_no_callback_is_not_reported_as_cadence_deferral(self):
        _, attempts, results, *_ = self.exercise([0, 0], enabled=False)
        self.assertEqual(attempts, [])
        self.assertTrue(all(r['thin_deferred'] == 0 for r in results))


if __name__ == '__main__':
    unittest.main()
