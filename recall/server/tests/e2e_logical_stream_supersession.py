#!/usr/bin/env python3
"""Stop a superseded PostgreSQL stream without borrowing another connection."""
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from e2e_logical_periodic_supersession import PeriodicSupersession
import recall_server.logical_evidence_projection as module


class StreamSupersession(PeriodicSupersession):
    def exercise_stream(self, mutation):
        candidate = next(c for c in self.logical._pending(limit=2, tenant_id=self.tenant)
                         if c.native_parent_id == 'large')
        self.logical.cursor_fetch_rows = 1
        original_connect = self.store.connect
        consumed, queue_checks = [], []
        active = 0
        outer = self

        class Cursor:
            def __init__(self, actual): self.actual = actual
            @property
            def itersize(self): return self.actual.itersize
            @itersize.setter
            def itersize(self, value): self.actual.itersize = value
            def execute(self, *args): return self.actual.execute(*args)
            def __iter__(self):
                for row in self.actual:
                    consumed.append(row['native_id'])
                    if len(consumed) == 2:
                        # A separate committed writer changes the queue while
                        # the named cursor retains its original SELECT snapshot.
                        with patch.object(outer.store, 'connect', original_connect):
                            if mutation:
                                outer.mutate(mutation)
                        outer.clock[0] = module.PROJECTION_PROGRESS_INTERVAL_SECONDS + 0.1
                    yield row

        class Connection:
            def __init__(self, actual): self.actual = actual
            def __getattr__(self, name): return getattr(self.actual, name)
            def execute(self, sql, *args, **kwargs):
                if 'SELECT generation,changed_at' in sql and consumed:
                    queue_checks.append(len(consumed))
                return self.actual.execute(sql, *args, **kwargs)
            @contextmanager
            def cursor(self, *args, **kwargs):
                with self.actual.cursor(*args, **kwargs) as cursor:
                    yield Cursor(cursor) if kwargs.get('name') == 'logical_evidence_batch_stream' else cursor

        @contextmanager
        def one_lease():
            nonlocal active
            self.assertEqual(active, 0, 'stream checkpoint borrowed another SQL connection')
            active += 1
            try:
                with original_connect() as connection:
                    yield Connection(connection)
            finally:
                active -= 1

        opened, temp_patch = self.observe_temps()
        with (patch.object(self.store, 'connect', one_lease),
              patch.object(module, 'time', SimpleNamespace(monotonic=lambda: self.clock[0])),
              temp_patch):
            if mutation:
                with self.assertRaises(module._LogicalWorkSuperseded):
                    self.logical._prepare_batch_and_upload((candidate,), require_current_queue=True)
            else:
                uploads = self.logical._prepare_batch_and_upload((candidate,), require_current_queue=True)
                self.assertEqual(uploads[0].manifest.record_count, 12)
                self.logical._schedule_upload_cleanup(uploads)
                for upload in uploads:
                    upload.body_locators.close()
        if mutation:
            self.assertLessEqual(len(consumed), 2, 'obsolete SQL stream read the remaining parent')
            self.assertNotIn('large', self.uploaded)
            self.assertNotIn('large', self.encoded)
            with original_connect() as connection:
                row = connection.execute('SELECT generation,attempts,next_attempt_at FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s',
                                         (self.tenant, self.sources['large'])).fetchone()
            self.assertEqual((row['generation'], row['attempts'], row['next_attempt_at']), (2, 0, None))
        else:
            self.assertEqual(len(consumed), 12)
            self.assertEqual(queue_checks.count(2), 1, 'unchanged checkpoint must rearm, not query per row')
        self.assertTrue(all(value.closed for value in opened))

    def test_append_during_sql_stops_before_remaining_rows(self): self.exercise_stream('append')
    def test_forget_during_sql_stops_before_remaining_rows(self): self.exercise_stream('forget')
    def test_unchanged_stream_retains_all_records(self): self.exercise_stream(None)


if __name__ == '__main__':
    suite = unittest.TestSuite(StreamSupersession(n) for n in StreamSupersession.__dict__ if n.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
