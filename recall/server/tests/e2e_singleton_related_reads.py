#!/usr/bin/env python3
"""Related-result ordering parity across physical and admitted singleton rows."""
import unittest
from unittest.mock import patch
import e2e_receipt_membership as fixtures
from recall_server.canonical_retrieval import BoundCanonicalRetrieval


class SingletonRelatedReads(fixtures.ReceiptMembershipTests):
    def test_related_exact_tied_order_and_limit_survives_admission(self):
        for index in range(6):
            self.ingest(f'turn/{index}', f'Exact related body {index} α 🧠')
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        expected = {limit: bound.related(limit=limit) for limit in (1, 3, 20)}
        # Choose the first and last actual canonical chunk IDs in the baseline
        # ordering, so limiting before merging admitted rows cannot pass.
        for row in (expected[20]['results'][0], expected[20]['results'][-1]):
            self.admit_fixture(self.baseline(row['receipt'])['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            for limit, baseline in expected.items():
                self.assertEqual(bound.related(limit=limit), baseline)

    def test_related_keeps_physical_multichunk_filters_and_scope(self):
        self.ingest('turn/multi', 'Physical multi-chunk body α ' * 1000)
        receipt = self.ingest('turn/single', 'Exact admitted matching branch')['receipt']
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{provenance}',%s::jsonb) WHERE tenant_id=%s",
                               ('{"cwd":"/synthetic/project","branch":"feature/one"}', self.tenant))
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        expected = bound.related(cwd='/synthetic/project', branch='feature/one', limit=20)
        self.assertGreater(len(expected['results']), 2)
        self.admit_fixture(self.baseline(receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(bound.related(cwd='/synthetic/project', branch='feature/one', limit=20), expected)
            self.assertEqual(bound.related(branch='different')['results'], [])
            with patch.object(bound, 'authorized_sources', ()):
                self.assertEqual(bound.related()['results'], [])
            with patch.object(bound, 'tenant_id', 'tenant:other'):
                self.assertEqual(bound.related()['results'], [])

    def test_related_never_uses_profile_to_hide_retained_deleted_chunk(self):
        receipt = self.ingest('turn/deleted', 'Retained deleted chunk fence')['receipt']
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s', (self.tenant,))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND receipt=%s', (self.tenant, receipt))
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(bound.related()['results'], [])

    def test_related_refuses_corrupt_profile_and_missing_archive(self):
        from recall_server.chunk_bodies import ChunkBodyError
        from recall_server.receipt_membership import ReceiptMembershipError
        receipt = self.ingest('turn/profile', 'Exact admitted related body')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(bound, 'chunk_body_archive', None):
                with self.assertRaises(ChunkBodyError):
                    bound.related()
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s', (self.tenant,))
            with self.assertRaises(ReceiptMembershipError):
                bound.related()

    def test_related_deletion_during_archive_read_prevents_response(self):
        from recall_server.chunk_bodies import ChunkBodyError
        receipt = self.ingest('turn/race', 'Exact admitted read race')['receipt']
        self.admit_fixture(self.baseline(receipt)['document_id'])
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        read, deleted = self.archive.read_raw, []
        def delete_after_read(reference):
            payload = read(reference)
            if not deleted:
                deleted.append(True)
                with self.store.connect() as connection:
                    connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s', (self.tenant,))
            return payload
        with patch.object(self.store, 'verified_singleton_receipts', True):
            with patch.object(self.archive, 'read_raw', delete_after_read):
                with self.assertRaises(ChunkBodyError):
                    bound.related()
        self.assertEqual(deleted, [True])


if __name__ == '__main__':
    names = [name for name in dir(SingletonRelatedReads) if name.startswith('test_related_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonRelatedReads(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
