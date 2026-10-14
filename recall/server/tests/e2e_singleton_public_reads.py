#!/usr/bin/env python3
"""Public reader parity after separately verified singleton admission."""
import unittest
from contextlib import contextmanager
from unittest.mock import patch
import e2e_receipt_membership as fixtures
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.chunk_bodies import ChunkBodyError


class SingletonPublicReads(fixtures.ReceiptMembershipTests):
    def bound(self, **kwargs):
        return BoundCanonicalRetrieval(self.store, **{
            'tenant_id': self.tenant, 'principal_id': self.principal,
            'authorized_sources': (self.source,), 'chunk_body_archive': self.archive, **kwargs})

    def test_public_show_preserves_exact_response_and_physical_fallback(self):
        receipt = self.ingest('turn/show', 'Exact public show α 🧠')['receipt']
        physical = self.ingest('turn/multi', 'Physical multi-chunk text α ' * 2000)['receipt']
        bound = self.bound()
        original, original_physical = bound.show(receipt), bound.show(physical)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        self.assertIsNone(bound.show(receipt))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(bound.show(receipt), original)
            self.assertEqual(bound.show(physical), original_physical)

    def test_public_show_does_not_cross_tenant_or_source_authority(self):
        receipt = self.ingest('turn/scoped', 'Scoped archived public body')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertIsNone(self.bound(authorized_sources=()).show(receipt))
            self.assertIsNone(self.bound(tenant_id='tenant:other').show(receipt))

    def test_public_show_requires_archive_reader_for_admitted_body(self):
        receipt = self.ingest('turn/archive-required', 'Sole archived body')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.assertRaisesRegex(ChunkBodyError, 'archived_chunk_body_unavailable'):
                self.bound(chunk_body_archive=None).show(receipt)

    def test_public_show_deletion_during_archive_read_is_not_returned(self):
        receipt = self.ingest('turn/read-race', 'Deleted during exact public read')['receipt']
        document = self.baseline(receipt)
        self.admit_fixture(document['document_id'])
        connect, read = self.store.connect, self.archive.read_raw
        active, observed = [0], []
        @contextmanager
        def counted_connection():
            with connect() as connection:
                active[0] += 1
                try:
                    yield connection
                finally:
                    active[0] -= 1
        def delete_during_read(reference):
            self.assertEqual(active[0], 0, 'archive I/O must remain outside SQL leases')
            payload = read(reference)
            if not observed:
                observed.append(True)
                with connect() as connection:
                    connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                                       (self.tenant, document['document_id']))
            return payload
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.store, 'connect', counted_connection), patch.object(self.archive, 'read_raw', delete_during_read):
                with self.assertRaisesRegex(ChunkBodyError, 'archived_chunk_body_unavailable'):
                    self.bound().show(receipt)
        self.assertEqual(observed, [True])

    def test_public_context_preserves_mixed_neighbors_and_anchor_limits(self):
        receipts = [self.ingest('turn/' + suffix, 'Exact context ' + suffix)['receipt']
                    for suffix in ('a', 'b', 'c')]
        bound = self.bound()
        expected = {(before, after): bound.session_context(receipts[1], before=before, after=after)
                    for before, after in ((0, 0), (1, 1), (20, 20))}
        for receipt in receipts[1:]:
            self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            for (before, after), baseline in expected.items():
                self.assertEqual(bound.session_context(receipts[1], before=before, after=after), baseline)

    def test_public_context_cannot_skip_a_profile_neighbor_without_archive_reader(self):
        anchor = self.ingest('turn/a', 'Physical context anchor')['receipt']
        neighbor = self.ingest('turn/b', 'Archived context neighbor')['receipt']
        self.admit_fixture(self.baseline(neighbor)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with self.assertRaisesRegex(ChunkBodyError, 'archived_chunk_body_unavailable'):
                self.bound(chunk_body_archive=None).session_context(anchor, before=0, after=1)

    def test_public_context_refuses_profile_contradictions_before_archive_io(self):
        from recall_server.receipt_membership import ReceiptMembershipError

        anchor = self.ingest('turn/a', 'Physical context anchor')['receipt']
        neighbor = self.ingest('turn/b', 'Archived context neighbor')['receipt']
        document = self.baseline(neighbor)
        self.admit_fixture(document['document_id'])
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND document_id=%s",
                               (self.tenant, document['document_id']))
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.archive, 'read_raw', side_effect=AssertionError('unverified archive read')):
                with self.assertRaises(ReceiptMembershipError):
                    self.bound().session_context(anchor, before=0, after=1)

    def test_public_context_excludes_deleted_profile_neighbors(self):
        anchor = self.ingest('turn/a', 'Physical context anchor')['receipt']
        neighbor = self.ingest('turn/b', 'Archived context neighbor')['receipt']
        document = self.baseline(neighbor)
        self.admit_fixture(document['document_id'])
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s",
                               (self.tenant, document['document_id']))
        baseline = self.bound().session_context(anchor, before=0, after=1)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            actual = self.bound().session_context(anchor, before=0, after=1)
        self.assertEqual(actual, baseline)
        self.assertEqual(len(actual['events']), 1)


if __name__ == '__main__':
    names = [name for name in dir(SingletonPublicReads) if name.startswith('test_public_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonPublicReads(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
