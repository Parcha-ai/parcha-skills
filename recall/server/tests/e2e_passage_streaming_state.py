#!/usr/bin/env python3
"""Prepared parent payloads remain disk-backed through atomic publication."""
import unittest
import weakref
import tempfile
from unittest.mock import patch

from e2e_passage_append_tail import PassageAppendTail
from recall_server import passage_index


class PassageStreamingState(PassageAppendTail):
    def test_full_preparation_keeps_payload_off_heap_and_commits_exact_rows(self):
        for i in range(12):
            self.ingest(f'event-{i:03}', text=('exact visible evidence é🙂 ' + str(i) + ' ') * 200,
                        occurred=f'2026-10-13T12:00:{i:02}Z')
        candidate = self.next_candidate()
        prepared = self.passages._prepare(candidate, incremental=False)
        # The iterator seam is useful only if this runtime result stays lazy.
        self.assertNotIsInstance(prepared.passages, (tuple, list))
        expected = {p.passage_id: (p.ordinal, p.text, list(p.receipts)) for p in prepared.passages}
        self.assertGreater(len(expected), 5)
        live, peak = [], [0]
        get = passage_index._DiskSequence.__getitem__
        def tracked(sequence, index):
            value = get(sequence, index)
            if isinstance(value, passage_index.LosslessPassage):
                live[:] = [ref for ref in live if ref() is not None]
                live.append(weakref.ref(value))
                peak[0] = max(peak[0], len(live))
            return value
        with patch.object(passage_index._DiskSequence, '__getitem__', tracked):
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        self.assertLessEqual(peak[0], 4, 'commit retained a parent of decoded passage payloads')
        actual = {p['passage_id']: (p['ordinal'], p['text_redacted'], p['receipts']) for p in self.projected()}
        self.assertEqual(actual, expected)
        self.assertEqual(list(prepared.passages), list(prepared.passages))
        prepared.close()
        self.assertTrue(prepared.passages.stream.closed)

    def test_spooled_candidate_rollback_keeps_previous_publication_and_queue(self):
        self.seed()
        candidate = self.append()
        before = self.projected()
        prepared = self.passages._prepare(candidate, incremental=False)
        self.assertNotIsInstance(prepared.passages, (tuple, list))
        self.assertGreater(len(before), 2)
        with patch.object(passage_index, 'MAX_PASSAGE_PROJECTION_BATCH', 2), patch(
            'recall_server.passage_index.enqueue_search_outbox', side_effect=RuntimeError('synthetic publication failure')
        ):
            with self.assertRaisesRegex(RuntimeError, 'synthetic publication failure'):
                self.passages._commit(prepared)
        self.assertEqual(self.projected(), before)
        self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1)[0].generation, candidate.generation)
        with patch.object(passage_index, 'MAX_PASSAGE_PROJECTION_BATCH', 2):
            self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        prepared.close()

    def test_worker_closes_attempt_files_on_commit_interrupt_and_healthy_retry(self):
        self.ingest('event-000', text='Exact source remains queued until publication.')
        self.next_candidate()
        streams, real = [], tempfile.TemporaryFile
        def tracked(*args, **kwargs):
            stream = real(*args, **kwargs)
            streams.append(stream)
            return stream
        with patch.object(passage_index.tempfile, 'TemporaryFile', side_effect=tracked):
            with patch.object(self.passages, '_commit', side_effect=KeyboardInterrupt('synthetic stop')):
                with self.assertRaises(KeyboardInterrupt):
                    self.passages.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1)
            self.assertTrue(streams)
            self.assertTrue(all(stream.closed for stream in streams))
            self.assertEqual(len(self.passages._pending(tenant_id=self.tenant, limit=1)), 1)
            result = self.passages.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1)
            self.assertEqual(result['documents'], 1)
            self.assertTrue(all(stream.closed for stream in streams))


if __name__ == '__main__':
    suite = unittest.TestSuite(PassageStreamingState(name) for name in PassageStreamingState.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
