#!/usr/bin/env python3
"""A failed archive admission must not discard an independent ready parent."""
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest

from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError
import psycopg

import e2e_logical_parent_progress as progress
from recall_server.archive import ArchiveCorruption, ArchiveError


def provider_error(cause):
    error = ArchiveError('archive provider request failed')
    error.__cause__ = cause
    return error


class AdmissionFailureIsolation(progress.ParentProgress):
    def _admission_failure(self, error, *, expected=None, before_error=None):
        original = self.projection.archive.verify_raw_bounded
        failed = threading.Event()
        failure_lock = threading.Lock()

        def verify(reference, *, deadline_at):
            if reference['source_id'] == self.sources['small']:
                self.assertTrue(self.blocked.wait(5))
                with failure_lock:
                    if not failed.is_set() and before_error is not None:
                        before_error()
                    failed.set()
                raise error
            return original(reference, deadline_at=deadline_at)

        self.projection.archive.verify_raw_bounded = verify
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(failed.wait(5))
            finally:
                self.release.set()
            if expected is not None:
                with self.assertRaises(expected):
                    future.result(timeout=10)
                return None
            return future.result(timeout=10)

    def _assert_transport_failure_retains_peer_publication(self, cause):
        result = self._admission_failure(provider_error(cause))
        self.assertEqual(result['documents'], 1)
        self.assertEqual(result['logical_failed'], 1)
        with self.store.connect() as connection:
            published = connection.execute(
                'SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,),
            ).fetchall()
            queued = connection.execute(
                '''SELECT native_parent_id,generation,attempts,last_error_code,
                          next_attempt_at>clock_timestamp() AS backed_off
                   FROM canonical_evidence_document_queue WHERE tenant_id=%s''',
                (self.tenant,),
            ).fetchall()
        self.assertEqual([row['native_parent_id'] for row in published], ['large'])
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]['native_parent_id'], 'small')
        self.assertEqual(queued[0]['attempts'], 1)
        self.assertTrue(queued[0]['backed_off'])
        self.assertEqual(queued[0]['last_error_code'], 'ArchiveError')
        self.assertEqual(progress.archive_object_count(self.archive_root), 2)

    def test_transient_admission_failure_retains_peer_publication(self):
        self._assert_transport_failure_retains_peer_publication(
            ConnectionClosedError(endpoint_url='https://synthetic.invalid'))

    def _assert_no_failed_backoff(self):
        with self.store.connect() as connection:
            rows = connection.execute(
                'SELECT attempts FROM canonical_evidence_document_queue WHERE tenant_id=%s',
                (self.tenant,),
            ).fetchall()
        self.assertTrue(all(row['attempts'] == 0 for row in rows))

    def test_authorization_failure_still_aborts(self):
        self._admission_failure(provider_error(ClientError(
            {'Error': {'Code': 'AccessDenied'}, 'ResponseMetadata': {'HTTPStatusCode': 403}},
            'HeadObject')), expected=ArchiveError)
        self._assert_no_failed_backoff()

    def test_later_generation_is_not_penalized_by_failed_parent(self):
        def dirty_again():
            with self.store.connect() as connection:
                progress.mark_logical_evidence_dirty(
                    connection, tenant_id=self.tenant, source_id=self.sources['small'],
                    native_ids=['small'], reason='ingest')
        result = self._admission_failure(
            provider_error(ConnectionClosedError(endpoint_url='https://synthetic.invalid')),
            before_error=dirty_again)
        self.assertEqual(result['documents'], 1)
        with self.store.connect() as connection:
            queued = connection.execute(
                '''SELECT generation,attempts,last_error_code FROM canonical_evidence_document_queue
                   WHERE tenant_id=%s AND source_id=%s''',
                (self.tenant, self.sources['small']),
            ).fetchone()
        self.assertGreater(queued['generation'], 1)
        self.assertEqual(queued['attempts'], 0)
        self.assertIsNone(queued['last_error_code'])

    def test_corruption_still_aborts(self):
        self._admission_failure(ArchiveCorruption('archive metadata mismatch'), expected=ArchiveCorruption)
        self._assert_no_failed_backoff()

    def test_admission_read_timeout_retains_peer_publication(self):
        # A bounded HEAD can time out during TLS setup before any publication.
        # Keep its failed parent queued without discarding independent work.
        self._assert_transport_failure_retains_peer_publication(
            ReadTimeoutError(endpoint_url='https://synthetic.invalid'))

    def test_cleanup_failure_is_not_reported_as_isolated(self):
        original = self.logical._schedule_cleanup
        def cleanup(references, *args, **kwargs):
            if any(ref['source_id'] == self.sources['small'] for ref in references):
                raise psycopg.OperationalError('synthetic cleanup database failure')
            return original(references, *args, **kwargs)
        self.logical._schedule_cleanup = cleanup
        self._admission_failure(provider_error(ConnectionClosedError(endpoint_url='https://synthetic.invalid')), expected=psycopg.OperationalError)
        self._assert_no_failed_backoff()

    def test_backoff_failure_is_not_reported_as_isolated(self):
        def mark_failed(*args):
            raise psycopg.OperationalError('synthetic retry bookkeeping failure')
        self.logical._mark_failed = mark_failed
        self._admission_failure(provider_error(ConnectionClosedError(endpoint_url='https://synthetic.invalid')), expected=psycopg.OperationalError)
        self._assert_no_failed_backoff()

    def test_lost_commit_ack_still_escapes_and_keeps_live_archive(self):
        original = self.logical._commit
        committed = threading.Event()
        def commit(candidate, upload):
            result = original(candidate, upload)
            if candidate.native_parent_id == 'small':
                self.assertTrue(self.blocked.wait(5))
                committed.set()
                raise psycopg.OperationalError('synthetic lost commit acknowledgement')
            return result
        self.logical._commit = commit
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.run_worker)
            try:
                self.assertTrue(committed.wait(5))
            finally:
                self.release.set()
            with self.assertRaises(psycopg.OperationalError):
                future.result(timeout=10)
        with self.store.connect() as connection:
            published = connection.execute(
                'SELECT native_parent_id FROM canonical_evidence_documents WHERE tenant_id=%s',
                (self.tenant,),
            ).fetchall()
        self.assertIn('small', [row['native_parent_id'] for row in published])
        self._assert_no_failed_backoff()
        self.assertEqual(progress.archive_object_count(self.archive_root), 2 * len(published))


if __name__ == '__main__':
    unittest.main()
