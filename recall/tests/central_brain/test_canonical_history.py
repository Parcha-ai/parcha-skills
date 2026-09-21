"""Outgoing revisions retain exact bodies; preparation never owns a DB lease."""
import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.canonical_history import HistoryUnavailable, StagedHistory, prepare_history
from recall_server.canonical import CanonicalLifecycleError
from recall_server.app import Handler


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.row = dict(tenant_id='tenant:test', source_id='source:test',
                        document_id='doc_test', revision=1, text_sha256=digest('α\nβ'))
        self.chunks = [dict(ordinal=0, receipt='r0', text_redacted='α\n'),
                       dict(ordinal=1, receipt='r1', text_redacted='β')]

    def test_private_spool_keeps_exact_boundaries_and_closes(self):
        with StagedHistory() as staged:
            staged.add(self.row, self.chunks)
            self.assertEqual(staged.read(self.row), self.chunks)
            spool = staged.file
            self.assertIsNotNone(spool)
        self.assertTrue(spool.closed)

    def test_stale_revision_or_hash_cannot_restore(self):
        with StagedHistory() as staged:
            staged.add(self.row, self.chunks)
            for change in ({'revision': 2}, {'text_sha256': '0' * 64}, {'source_id': 'source:other'}, {'tenant_id': 'tenant:other'}):
                with self.subTest(change=change), self.assertRaises(HistoryUnavailable):
                    staged.read({**self.row, **change})

    def test_changed_bytes_and_oversized_stage_fail_closed(self):
        with StagedHistory() as staged:
            with self.assertRaises(HistoryUnavailable):
                staged.add(self.row, [{**self.chunks[0], 'text_redacted': 'bad'}])
            with mock.patch('recall_server.canonical_history.MAX_STAGE_BYTES', 1):
                with self.assertRaises(HistoryUnavailable):
                    staged.add(self.row, self.chunks)

    def test_staging_refuses_to_fill_worker_disk(self):
        available = mock.Mock(f_bavail=1, f_frsize=4096)
        with StagedHistory() as staged, mock.patch('os.fstatvfs', return_value=available):
            with self.assertRaises(HistoryUnavailable):
                staged.add(self.row, self.chunks)
            self.assertEqual(staged.size, 0)

    def test_retryable_failure_is_not_bad_collector_input(self):
        self.assertEqual(Handler.legacy_write_error_status(
            CanonicalLifecycleError('canonical_history_unavailable')), 503)

    def test_canonical_http_returns_retryable_503_without_ack(self):
        from .test_webhook_http import FakeStore, WebhookServer
        plane = mock.Mock()
        plane.ingest_batch.side_effect = CanonicalLifecycleError('canonical_history_unavailable')
        with mock.patch.dict(os.environ, {'RECALL_HTTP_PROFILE': '', 'RECALL_AUTH_REQUIRED': '0'}), \
             mock.patch.object(Handler, 'canonical_plane', plane), \
             mock.patch.object(Handler, 'canonical_authority', return_value=('tenant:test', 'principal:test', 'source:test')), \
             mock.patch.object(Handler, 'require', return_value={'principal_id': 'principal:test'}), \
             WebhookServer(FakeStore()) as server:
            status, body = server.request('POST', '/v2/ingest/canonical',
                body={'events': [{'source_id': 'source:test'}]}, token=None)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {'error': 'canonical_history_unavailable'})

    def test_managed_worker_passes_archive_to_its_real_writer(self):
        from recall_server.managed_worker import ManagedConnectorWorker
        with tempfile.TemporaryDirectory() as root:
            worker = ManagedConnectorWorker(mock.Mock(), mock.Mock(), mock.Mock(),
                state_root=Path(root), chunk_body_archive=mock.sentinel.evidence_archive)
        self.assertIs(worker.plane.chunk_body_archive, mock.sentinel.evidence_archive)

    def test_large_parent_is_staged_in_bounded_reads_after_sql_closes(self):
        rows = [{**self.row, 'document_id': f'doc_{number}', 'native_parent_id': 'parent'} for number in range(9)]
        active = [False]
        connection = mock.Mock()
        connection.execute.return_value.fetchall.side_effect = [
            [{'source_id': 'source:test', 'owner_principal_id': 'principal:test'}], rows]
        store = mock.Mock()
        @contextmanager
        def connect():
            active[0] = True
            try:
                yield connection
            finally:
                active[0] = False
        store.connect = connect
        calls = []
        def archived(*_, **kwargs):
            self.assertFalse(active[0])
            self.assertLessEqual(len(kwargs['document_ids']), 8)
            calls.append(kwargs['document_ids'])
            return {('source:test', document): self.chunks for document in kwargs['document_ids']}
        candidates = [dict(source_id='source:test', native_id='native', content_sha256='a' * 64,
                           principal_id='principal:test')]
        with mock.patch('recall_server.canonical_history.read_archived_chunks', side_effect=archived):
            with prepare_history(store, object(), tenant_id='tenant:test', candidates=candidates) as staged:
                self.assertEqual([staged.read(row) for row in rows], [self.chunks] * 9)
        self.assertEqual(list(map(len, calls)), [8, 1])


if __name__ == '__main__':
    unittest.main()
