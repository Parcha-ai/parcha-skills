"""Real Arrow multipart packing and the bounded upload lifecycle."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server import parquet_scan as scan
from tests.central_brain.test_parquet_scan import _candidate


class UploadLifecycle(unittest.TestCase):
    def upload(self, put):
        self.cleaned = []
        owner = SimpleNamespace(archive=SimpleNamespace(put_raw=put),
                                _schedule_cleanup=lambda refs: self.cleaned.extend(refs))
        return scan._StreamingUpload(owner, _candidate(), generation='a' * 64,
            created_at=datetime(2026, 8, 1, tzinfo=timezone.utc))

    def test_actual_multipart_encoding_precedes_first_put_and_backpressures(self):
        blocked, release = threading.Event(), threading.Event()
        calls, encoded = [], []
        native_encode = scan._parquet_table_bytes
        native_parts = scan._parquet_parts

        def encode(table):
            self.assertFalse(blocked.is_set() and not release.is_set())
            encoded.append(table.num_rows)
            return native_encode(table)

        def put(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                blocked.set()
                if not release.wait(5):
                    raise RuntimeError('synthetic missing release')
            return {'artifact_id': kwargs['native_id'], 'content_sha256':
                    hashlib.sha256(kwargs['payload']).hexdigest()}

        upload = self.upload(put)
        rows = [{'ordinal': i, 'search_text': hashlib.shake_256(str(i).encode()).hexdigest(4096)}
                for i in range(6)]
        member = scan.FragmentMember('document', 1, 'b' * 64)
        with patch.object(scan, '_parquet_table_bytes', encode), \
             patch.object(scan, '_parquet_parts',
                          lambda rows, schema: native_parts(rows, schema, maximum_bytes=12000)), \
             ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(upload._emit, 'records', rows, (member,), (None, None))
            try:
                self.assertTrue(blocked.wait(3))
                self.assertGreater(len(encoded), 1, 'fixture did not encode multiple objects')
                self.assertEqual(len(calls), 1)
                self.assertFalse(future.done())
            finally:
                release.set()
            future.result(timeout=5)
            result = upload.finish(first=None, last=None, ensure_datasets=())
        self.assertGreater(len(calls), 1)
        self.assertTrue(all(len(x['payload']) <= 12000 for x in calls))
        self.assertEqual(sum(result.row_counts.values()), 6)
        self.assertTrue(all(m == (member,) for m in result.members.values()))
        self.assertIsNone(upload.pending_upload)
        self.assertIsNone(upload.upload_executor)

    def test_finish_failed_last_put_closes_threads_without_retry(self):
        calls = []

        def put(**kwargs):
            calls.append(kwargs)
            raise RuntimeError('synthetic final PUT failure')

        upload = self.upload(put)
        upload._emit('records', [{'ordinal': 0}], (), (None, None))
        executor = upload.upload_executor
        with self.assertRaisesRegex(RuntimeError, 'synthetic final PUT failure'):
            upload.finish(first=None, last=None, ensure_datasets=())
        self.assertIsNone(upload.upload_executor)
        self.assertTrue(all(not t.is_alive() for t in executor._threads))
        upload.abort()
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.cleaned, [])
        self.assertIsNone(upload.pending_upload)

    def test_abort_collects_previous_success_when_pending_worker_raises_baseexception(self):
        calls = []

        def put(**kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                raise KeyboardInterrupt('synthetic archive stop')
            return {'artifact_id': 'successful-first'}

        upload = self.upload(put)
        upload._emit('records', [{'ordinal': 0}], (), (None, None))
        upload._emit('records', [{'ordinal': 1}], (), (None, None))
        executor = upload.upload_executor
        upload.abort()
        self.assertEqual(self.cleaned, [{'artifact_id': 'successful-first'}])
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(not t.is_alive() for t in executor._threads))
        self.assertIsNone(upload.pending_upload)
        self.assertIsNone(upload.upload_executor)

    def test_interrupted_reference_bookkeeping_keeps_success_for_abort(self):
        upload = self.upload(lambda **kwargs: {'artifact_id': 'successful-first'})

        class Interrupted(dict):
            interrupt = True

            def __setitem__(self, key, value):
                if self.interrupt:
                    self.interrupt = False
                    raise KeyboardInterrupt('synthetic owner interruption')
                super().__setitem__(key, value)

        upload.references = Interrupted()
        upload._emit('records', [{'ordinal': 0}], (), (None, None))
        with self.assertRaisesRegex(KeyboardInterrupt, 'synthetic owner interruption'):
            upload.finish(first=None, last=None, ensure_datasets=())
        upload.abort()
        self.assertEqual(self.cleaned, [{'artifact_id': 'successful-first'}])
        self.assertIsNone(upload.pending_upload)
        self.assertIsNone(upload.upload_executor)
