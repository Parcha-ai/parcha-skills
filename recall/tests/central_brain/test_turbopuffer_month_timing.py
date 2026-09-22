"""Numeric month attribution uses actual catalog/write loops, with no network."""
from __future__ import annotations

import logging
import threading
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from .test_turbopuffer_projection import (
    JULY, SETTINGS, TENANT, RateLimitError, _Catalog, _passage, _projector,
)
from recall_server import turbopuffer_projection as owner


class Clock:
    def __init__(self):
        self.value = 0.0
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.value

    def advance(self, seconds):
        with self.lock:
            self.value += seconds


class MonthTimingTest(unittest.TestCase):
    def fixture(self, count=1, **kwargs):
        catalog = _Catalog()
        catalog.passages = [_passage(i, 7, i + 1) for i in range(count)]
        catalog.enqueue(JULY)
        projector, client = _projector(catalog, tokens_per_minute=0, **kwargs)
        return catalog, projector, client.namespace(SETTINGS.namespace(TENANT))

    @contextmanager
    def capture(self):
        with self.assertLogs(owner.LOG, level=logging.INFO) as captured:
            yield captured

    def fields(self, captured):
        lines = [r.getMessage() for r in captured.records
                 if r.getMessage().startswith('search plane month timing ')]
        self.assertEqual(len(lines), 1)
        fields = dict(item.split('=') for item in lines[0].split()[4:])
        self.assertEqual(set(fields), {
            'succeeded', 'month_ms', 'catalog_ms', 'catalog_calls', 'page_ms', 'page_calls',
            'commit_ms', 'commit_calls',
            'pacer_ms', 'pacer_calls', 'sdk_ms', 'sdk_calls', 'backoff_ms',
            'backoff_calls', 'concurrency',
        })
        self.assertIn(fields.pop('succeeded'), {'0', '1'})
        self.assertTrue(all(value.isdecimal() for value in fields.values()))
        return {key: int(value) for key, value in fields.items()}

    def test_serial_phase_costs_include_acquisition_commit_and_close(self):
        clock = Clock()
        catalog, projector, namespace = self.fixture()
        connect = projector.store.connect

        @contextmanager
        def delayed_connection():
            clock.advance(.125)  # pool/connection acquisition
            with connect() as connection:
                yield connection
            clock.advance(.125)  # connection return/close

        page = projector.passage_page
        def delayed_page(*args, **kwargs):
            clock.advance(.5)
            return page(*args, **kwargs)

        write = namespace.write
        def delayed_write(**kwargs):
            clock.advance(1)
            return write(**kwargs)

        with patch.object(owner.time, 'monotonic', clock), \
                patch.object(projector.store, 'connect', delayed_connection), \
                patch.object(projector, 'passage_page', delayed_page), \
                patch.object(projector.pacer, 'wait_for', lambda _tokens: clock.advance(.25)), \
                patch.object(namespace, 'write', delayed_write), self.capture() as captured:
            result = projector.drain(tenant_id=TENANT, max_months=1)
        metrics = self.fields(captured)
        self.assertEqual(result['rows'], 1)
        self.assertEqual(metrics, {
            'month_ms': 2500, 'catalog_ms': 250, 'catalog_calls': 1,
            'commit_ms': 250, 'commit_calls': 1,
            'page_ms': 750, 'page_calls': 1, 'pacer_ms': 250, 'pacer_calls': 1,
            'sdk_ms': 1000, 'sdk_calls': 1, 'backoff_ms': 0, 'backoff_calls': 0,
            'concurrency': 1,
        })
        self.assertFalse(catalog.outbox)

    def test_outer_retry_times_calls_and_backoff_without_changing_result(self):
        clock = Clock()
        catalog, projector, namespace = self.fixture(sleep=clock.advance)
        write = namespace.write
        calls = []
        def transient_then_success(**kwargs):
            calls.append(kwargs)
            clock.advance(.25)
            if len(calls) == 1:
                raise RateLimitError('private body must not escape')
            return write(**kwargs)
        with patch.object(owner.time, 'monotonic', clock), \
                patch.object(namespace, 'write', transient_then_success), self.capture() as captured:
            result = projector.drain(tenant_id=TENANT, max_months=1)
        metrics = self.fields(captured)
        self.assertEqual((metrics['sdk_ms'], metrics['sdk_calls']), (500, 2))
        self.assertEqual((metrics['backoff_ms'], metrics['backoff_calls']), (1000, 1))
        self.assertEqual(metrics['month_ms'], 1500)
        self.assertEqual((result['rows'], result['rate_limited']), (1, 1))
        self.assertEqual(calls[0], calls[1])
        self.assertNotIn('private body', '\n'.join(captured.output))
        self.assertFalse(catalog.outbox)

    def test_failure_logs_partial_cost_without_identity_or_error_text(self):
        clock = Clock()
        catalog, projector, namespace = self.fixture()
        failure = ValueError('private body source:secret namespace:secret')
        def fail(**_kwargs):
            clock.advance(.5)
            raise failure
        claim = projector.claim_months(projector.store.connect(), tenant_id=TENANT, limit=1)[0]
        with patch.object(owner.time, 'monotonic', clock), \
                patch.object(namespace, 'write', fail), self.capture() as captured:
            with self.assertRaises(ValueError) as raised:
                projector.project_month(claim)
        self.assertIs(raised.exception, failure)
        metrics = self.fields(captured)
        self.assertEqual((metrics['sdk_ms'], metrics['sdk_calls']), (500, 1))
        self.assertEqual(metrics['catalog_calls'], 1)
        self.assertEqual(metrics['page_calls'], 1)
        self.assertEqual(metrics['commit_calls'], 0)  # no completion write after failure
        self.assertIn('succeeded=0', captured.output[0])
        self.assertNotIn('secret', '\n'.join(captured.output))
        self.assertNotIn(TENANT, '\n'.join(captured.output))
        self.assertEqual(len(catalog.outbox), 1)

    def test_concurrent_call_sums_overlap_month_wall_and_keep_all_counts(self):
        clock = Clock()
        catalog, projector, namespace = self.fixture(count=4, batch_rows=1, write_concurrency=4)
        entering = threading.Barrier(4)
        leaving = threading.Barrier(4)
        write = namespace.write
        def concurrent_write(**kwargs):
            if entering.wait(timeout=5) == 0:
                clock.advance(.5)
            leaving.wait(timeout=5)
            return write(**kwargs)
        with patch.object(owner.time, 'monotonic', clock), \
                patch.object(namespace, 'write', concurrent_write), self.capture() as captured:
            result = projector.drain(tenant_id=TENANT, max_months=1)
        metrics = self.fields(captured)
        self.assertEqual((metrics['sdk_calls'], metrics['sdk_ms']), (4, 2000))
        self.assertEqual((metrics['month_ms'], metrics['concurrency']), (500, 4))
        self.assertEqual(metrics['page_calls'], 2)  # exact full page + empty terminator
        self.assertEqual(metrics['pacer_calls'], 4)
        self.assertEqual(result['rows'], 4)
        self.assertEqual(len(namespace.rows), 4)
        self.assertFalse(catalog.outbox)

    def test_catalog_page_and_completion_failure_keep_phase_costs_and_queue(self):
        for method, phase in (("read_watermark", "catalog"), ("passage_page", "page"), ("finish_month", "commit")):
            with self.subTest(phase=phase):
                clock = Clock()
                catalog, projector, _namespace = self.fixture()
                claim = projector.claim_months(projector.store.connect(), tenant_id=TENANT, limit=1)[0]
                original = ValueError("private catalog receipt")

                def fail(*_args, **_kwargs):
                    clock.advance(.375)
                    raise original

                with patch.object(owner.time, "monotonic", clock), \
                        patch.object(projector, method, fail), self.capture() as captured:
                    with self.assertRaises(ValueError) as raised:
                        projector.project_month(claim)
                self.assertIs(raised.exception, original)
                metrics = self.fields(captured)
                self.assertEqual((metrics[phase + "_ms"], metrics[phase + "_calls"]), (375, 1))
                self.assertEqual(metrics["month_ms"], 375)
                self.assertIn("succeeded=0", captured.output[0])
                self.assertNotIn("private", "\n".join(captured.output))
                self.assertEqual(len(catalog.outbox), 1)

    def test_actual_token_pacer_wait_is_separate_from_sdk_time(self):
        clock = Clock()
        catalog, projector, _namespace = self.fixture(count=2, batch_rows=1)
        # A one-token window forces each subsequent nonempty row to wait.
        projector.pacer = owner.TokenPacer(12, clock=clock, sleep=clock.advance)
        with patch.object(owner.time, "monotonic", clock), self.capture() as captured:
            result = projector.drain(tenant_id=TENANT, max_months=1)
        metrics = self.fields(captured)
        self.assertEqual((metrics["pacer_ms"], metrics["pacer_calls"]), (5000, 2))
        self.assertEqual((metrics["sdk_ms"], metrics["sdk_calls"]), (0, 2))
        self.assertEqual(metrics["month_ms"], 5000)
        self.assertEqual(result["rows"], 2)
        self.assertFalse(catalog.outbox)

    def test_delete_only_and_empty_month_counts_include_completion(self):
        for deleted in (False, True):
            with self.subTest(deleted=deleted):
                clock = Clock()
                catalog, projector, namespace = self.fixture(count=0)
                if deleted:
                    catalog.tombstones.append(dict(source_id="source:codex:test", month=JULY, passage_id="gone"))
                write = namespace.write

                def delayed_write(**kwargs):
                    clock.advance(.25)
                    return write(**kwargs)

                with patch.object(owner.time, "monotonic", clock), \
                        patch.object(namespace, "write", delayed_write), self.capture() as captured:
                    result = projector.drain(tenant_id=TENANT, max_months=1)
                metrics = self.fields(captured)
                self.assertEqual(metrics["sdk_calls"], int(deleted))
                self.assertEqual(metrics["sdk_ms"], 250 if deleted else 0)
                self.assertEqual(metrics["pacer_calls"], 0)
                self.assertEqual((metrics["catalog_calls"], metrics["page_calls"], metrics["commit_calls"]), (1, 1, 1))
                self.assertEqual(result["deleted"], int(deleted))
                self.assertIn("succeeded=1", captured.output[0])
                self.assertFalse(catalog.outbox)

    def test_timing_logger_failure_does_not_mask_writer_error_or_success(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                catalog, projector, namespace = self.fixture()
                original = ValueError('original private failure')
                claim = projector.claim_months(projector.store.connect(), tenant_id=TENANT, limit=1)[0]
                with patch.object(owner.LOG, 'info', side_effect=RuntimeError('logger private failure')):
                    if fail:
                        with patch.object(namespace, 'write', side_effect=original):
                            with self.assertRaises(ValueError) as raised:
                                projector.project_month(claim)
                        self.assertIs(raised.exception, original)
                        self.assertEqual(len(catalog.outbox), 1)
                    else:
                        result = projector.project_month(claim)
                        self.assertEqual(result['rows'], 1)
                        self.assertFalse(catalog.outbox)


if __name__ == '__main__':
    unittest.main()
