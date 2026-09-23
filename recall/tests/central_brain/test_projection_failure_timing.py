"""Failed cycles retain timing attribution without reporting a successful cycle."""

import unittest
from unittest.mock import patch
from recall_server import projection_worker as worker
from tests.central_brain.test_projection_worker import (
    _FakeClock,
    _TimedLogical,
    _TimedPassages,
    _TimedScan,
)


class FailureTimingTests(unittest.TestCase):
    def run_cycle(self, thin):
        clock = _FakeClock()
        logical, passages, scan = (
            _TimedLogical([], work=0),
            _TimedPassages([], work=0),
            _TimedScan([], work=0),
        )
        for owner in (logical, passages, scan):
            owner.clock = clock
        logical.seconds = 240
        return worker.run_projection_worker(
            logical,
            passages,
            scan,
            tenant_id="tenant",
            logical_batch_size=1,
            passage_batch_size=1,
            embedding_batch_size=1,
            max_batches_per_cycle=1,
            upload_concurrency=1,
            passage_concurrency=1,
            interval_seconds=1,
            once=True,
            clock=clock,
            body_thinner=lambda busy: thin(clock),
        )

    def test_failed_thinner_logs_prior_phases_and_only_its_actual_duration(self):
        def thin(clock):
            clock.advance(2)
            raise RuntimeError("synthetic unknown commit")

        with patch.object(worker, "record_cycle") as record:
            with self.assertLogs(worker.LOG, level="ERROR") as logged:
                with self.assertRaises(RuntimeError):
                    self.run_cycle(thin)
        record.assert_not_called()
        message = logged.output[0]
        for value in (
            "failed_phase=thin",
            "cycle_elapsed_ms=242550",
            "logical_elapsed_ms=240000",
            "thin_elapsed_ms=2000",
            "embed_elapsed_ms=200",
            "passage_elapsed_ms=300",
            "parquet_elapsed_ms=50",
        ):
            self.assertIn(value, message)

    def test_recovered_timeout_keeps_cycle_pending_and_numeric_counts(self):
        def thin(clock):
            clock.advance(2)
            return dict(
                status="pending",
                documents=1,
                refused=0,
                document_bytes_removed=10,
                event_bytes_replaced=20,
                historical_probe_timeouts=1,
                historical_window_size=512,
                committed_hints_pending=2,
            )

        with self.assertLogs(worker.LOG, level="INFO") as logged:
            result = self.run_cycle(thin)
        self.assertEqual(result["status"], "pending")
        for key, value in [
            ("thin_probe_timeouts", 1),
            ("thin_window_size", 512),
            ("thin_hints_pending", 2),
        ]:
            self.assertEqual(result[key], value)
            self.assertIn(f"{key}={value}", logged.output[0])
        self.assertEqual(result["logical_elapsed_ms"], 240000)
        self.assertEqual(result["thin_elapsed_ms"], 2000)

    def test_failed_cycle_timings_do_not_leak_into_the_next_cycle(self):
        clock = _FakeClock()

        class Logical(_TimedLogical):
            attempts = 0

            def project_pending(self, **kwargs):
                self.attempts += 1
                if self.attempts == 2:
                    clock.advance(0.07)
                    raise ValueError("second-cycle logical failure")
                return super().project_pending(**kwargs)

        logical, passages, scan = (
            Logical([], work=0),
            _TimedPassages([], work=0),
            _TimedScan([], work=0),
        )
        for owner in (logical, passages, scan):
            owner.clock = clock
        logical.seconds = 240

        def thin(_busy):
            clock.advance(2)
            raise RuntimeError("first-cycle thin failure")

        with self.assertLogs(worker.LOG, level="ERROR") as logged:
            with self.assertRaises(ValueError):
                worker.run_projection_worker(
                    logical,
                    passages,
                    scan,
                    tenant_id="tenant",
                    logical_batch_size=1,
                    passage_batch_size=1,
                    embedding_batch_size=1,
                    max_batches_per_cycle=1,
                    upload_concurrency=1,
                    passage_concurrency=1,
                    interval_seconds=1,
                    max_cycles=2,
                    clock=clock,
                    sleep=lambda _seconds: None,
                    body_thinner=thin,
                )
        self.assertEqual(len(logged.output), 2)
        for value in (
            "failed_phase=logical",
            "cycle_elapsed_ms=570",
            "logical_elapsed_ms=70",
            "thin_elapsed_ms=0",
            "parquet_elapsed_ms=0",
        ):
            self.assertIn(value, logged.output[1])
