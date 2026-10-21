#!/usr/bin/env python3
"""Pending Archil deletion cannot race a new PostgreSQL artifact owner."""
# ruff: noqa: E402
import base64
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

import e2e_archive_reference_lifetime as original_fixture
from recall_server.canonical import (
    CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane,
    _guard_artifact_admission, _lock_artifacts,
)
from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from server.recall_server.archive_runtime import build_evidence_archive_store
from tests.test_archil_archive_deletion import FakeArchil, FakeR2


class ArchilCleanupAdmissionTests(unittest.TestCase):
    # Reuse the original isolated database and source fixture, without modifying
    # or silently rerunning its existing verifier methods.
    setUpClass = classmethod(original_fixture.ArchiveReferenceTests.setUpClass.__func__)
    tearDownClass = classmethod(original_fixture.ArchiveReferenceTests.tearDownClass.__func__)
    setUp = original_fixture.ArchiveReferenceTests.setUp
    event = original_fixture.ArchiveReferenceTests.event
    write = original_fixture.ArchiveReferenceTests.write

    def guard(self, reference):
        with self.store.connect() as connection, connection.transaction():
            _guard_artifact_admission(connection, self.tenant, self.source,
                                      [reference['artifact_id']])

    def queue_rows(self):
        with self.store.connect() as connection:
            return connection.execute(
                'SELECT * FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',
                (self.tenant,),
            ).fetchall()

    def claim_cleanup(self, reference):
        with self.store.connect() as connection, connection.transaction():
            connection.execute(
                'UPDATE canonical_evidence_cleanup_queue SET attempts=1 '
                'WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s',
                (self.tenant, self.source, reference['artifact_id']),
            )

    def use_archil_archive(self):
        self.origin, self.disk = FakeR2(), FakeArchil()
        values = {
            'RECALL_EVIDENCE_ARCHIVE_BACKEND': 's3-unversioned',
            'RECALL_EVIDENCE_ARCHIVE_BUCKET': 'recall-evidence-test',
            'RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL': 'https://s3.us-west-2.amazonaws.com',
            'RECALL_EVIDENCE_ARCHIVE_REGION': 'us-west-2',
            'RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID': 'synthetic',
            'RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY': 'synthetic',
            'RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY': base64.b64encode(b'h' * 32).decode(),
            'RECALL_EVIDENCE_DELETE_MODE': 'archil',
            'ARCHIL_API_KEY': 'key-synthetic',
            'RECALL_ARCHIL_DISK_ID': 'dsk-0123456789abcdef',
            'RECALL_ARCHIL_REGION': 'aws-us-west-2',
        }
        with patch('server.recall_server.archil_archive.ArchilDisk', return_value=self.disk):
            self.archive = build_evidence_archive_store(values, client_factory=lambda **_: self.origin)
        self.evidence = CanonicalEvidenceProjector(
            self.store, EvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant)
        self.logical = CanonicalLogicalEvidenceProjector(
            self.store, LogicalEvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant, raw_archive=self.archive)
        self.plane = CanonicalPlane(self.store, self.archive, evidence_projector=self.evidence,
                                    chunk_body_archive=self.archive)
        self.gateway = CanonicalArchiveGateway(
            self.store, self.archive, tenant_id=self.tenant, principal_id=self.principal)

    def test_unowned_claimed_object_cannot_acquire_a_new_owner(self):
        event = self.event()
        reference = event['provenance']['artifact_ref']
        self.logical._schedule_cleanup((reference,))
        self.claim_cleanup(reference)
        with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_artifact_cleanup_pending'):
            self.write(event)
        self.assertEqual(len(self.queue_rows()), 1)
        self.assertGreater(len(self.archive.read_raw(reference)), 0)

    def test_unclaimed_queued_object_can_gain_a_live_owner_before_cleanup(self):
        event = self.event()
        reference = event['provenance']['artifact_ref']
        self.logical._schedule_cleanup((reference,))
        self.assertEqual(self.queue_rows()[0]['attempts'], 0)
        self.write(event)
        result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual((result['completed'], result['deleted'], result['pending']), (1, 0, 0))
        self.assertGreater(len(self.archive.read_raw(reference)), 0)

    def test_existing_owner_stays_admissible_and_cleanup_preserves_its_object(self):
        event = self.event()
        self.write(event)
        reference = event['provenance']['artifact_ref']
        self.logical._schedule_cleanup((reference,))
        self.claim_cleanup(reference)
        self.guard(reference)
        result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual((result['completed'], result['deleted'], result['pending']), (1, 0, 0))
        self.assertGreater(len(self.archive.read_raw(reference)), 0)

    def test_waiting_admission_reads_cleanup_committed_while_artifact_lock_is_held(self):
        reference = self.event()['provenance']['artifact_ref']
        started = threading.Event()
        waiter_pid = []

        def admit():
            with self.store.connect() as connection, connection.transaction():
                waiter_pid.append(connection.info.backend_pid)
                started.set()
                _guard_artifact_admission(connection, self.tenant, self.source,
                                          [reference['artifact_id']])

        with ThreadPoolExecutor(max_workers=1) as executor:
            with self.store.connect() as holder, holder.transaction():
                _lock_artifacts(holder, self.tenant, self.source, [reference['artifact_id']])
                future = executor.submit(admit)
                self.assertTrue(started.wait(5), 'admission connection did not start')
                deadline = time.monotonic() + 5
                blocked = False
                while time.monotonic() < deadline:
                    with self.store.connect() as observer:
                        blocked = observer.execute(
                            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s "
                            "AND locktype='advisory' AND NOT granted) AS blocked",
                            (waiter_pid[0],),
                        ).fetchone()['blocked']
                    if blocked:
                        break
                    time.sleep(0.01)
                self.assertTrue(blocked, 'admission did not actually wait on the artifact lock')
                self.logical._schedule_cleanup((reference,))
                self.claim_cleanup(reference)
                self.assertFalse(future.done())
            with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_artifact_cleanup_pending'):
                future.result(timeout=5)
        self.assertEqual(len(self.queue_rows()), 1)

    def test_api_ack_keeps_cleanup_and_admission_blocked_until_origin_absence(self):
        self.use_archil_archive()
        event = self.event()
        reference = event['provenance']['artifact_ref']
        key = reference['object_key']
        self.disk.lengths[key] = reference['size_bytes']
        self.logical._schedule_cleanup((reference,))
        committed_attempts_at_delete = []
        delete = self.disk.delete

        def observe_committed_intent_then_delete(object_key):
            # A distinct connection must see the claim before any provider call;
            # the transaction performing deletion can still fail afterward.
            committed_attempts_at_delete.append(self.queue_rows()[0]['attempts'])
            delete(object_key)

        with patch.object(self.disk, 'delete', side_effect=observe_committed_intent_then_delete):
            result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual((result['completed'], result['deleted'], result['failures'], result['pending']),
                         (0, 0, 1, 1))
        self.assertIn(('DELETE', key), self.disk.calls)
        self.assertIn((self.archive.bucket, key), self.origin.objects)
        self.assertEqual(len(committed_attempts_at_delete), 1)
        self.assertGreaterEqual(committed_attempts_at_delete[0], 1)
        self.assertGreaterEqual(self.queue_rows()[0]['attempts'], 1)
        with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_artifact_cleanup_pending'):
            self.write(event)

        # Simulate only the provider's asynchronous origin-delete propagation.
        self.origin.objects.pop((self.archive.bucket, key))
        result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual((result['completed'], result['deleted'], result['failures'], result['pending']),
                         (1, 0, 0, 0))
        self.assertEqual(self.queue_rows(), [])
        self.assertEqual(self.disk.calls.count(('DELETE', key)), 1)

    def test_unrelated_new_origin_upload_is_admitted_before_archil_discovery(self):
        self.use_archil_archive()
        event = self.event('fresh')
        reference = event['provenance']['artifact_ref']
        self.assertNotIn(reference['object_key'], self.disk.lengths)
        self.write(event)
        with self.store.connect() as connection:
            row = connection.execute(
                'SELECT artifact_id FROM raw_artifacts WHERE tenant_id=%s AND source_id=%s '
                'AND artifact_id=%s', (self.tenant, self.source, reference['artifact_id']),
            ).fetchone()
        self.assertEqual(row['artifact_id'], reference['artifact_id'])
        self.assertEqual(self.disk.calls, [])

    def test_external_delete_followed_by_transaction_abort_keeps_committed_claim(self):
        self.use_archil_archive()
        event = self.event()
        reference = event['provenance']['artifact_ref']
        key = reference['object_key']
        self.disk.lengths[key] = reference['size_bytes']
        self.logical._schedule_cleanup((reference,))
        delete = self.disk.delete

        class CleanupInterrupted(BaseException):
            """Simulate interruption after the provider effect, before SQL commit."""

        def delete_then_interrupt(object_key):
            delete(object_key)
            raise CleanupInterrupted()

        with patch.object(self.disk, 'delete', side_effect=delete_then_interrupt):
            with self.assertRaises(CleanupInterrupted):
                self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertIn(('DELETE', key), self.disk.calls)
        self.assertNotIn(key, self.disk.lengths)
        self.assertIn((self.archive.bucket, key), self.origin.objects)
        self.assertGreaterEqual(self.queue_rows()[0]['attempts'], 1)
        with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_artifact_cleanup_pending'):
            self.write(event)

        self.origin.objects.pop((self.archive.bucket, key))
        result = self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual((result['completed'], result['pending']), (1, 0))
        self.assertEqual(self.disk.calls.count(('DELETE', key)), 1)


if __name__ == '__main__':
    unittest.main()
