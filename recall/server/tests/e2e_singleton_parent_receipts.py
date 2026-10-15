#!/usr/bin/env python3
"""Parent-ranked receipts retain canonical membership after singleton admission."""
import unittest
from unittest.mock import patch
from e2e_singleton_search_publication import SingletonSearchPublication


class SingletonParentReceipts(SingletonSearchPublication):
    def prepare_parent(self,mixed=False):
        self.prepare_passages(mixed=mixed)
        writer,self.namespace=self.writer()
        with self.store.connect() as connection:
            ids=[row['passage_id'] for row in connection.execute('SELECT passage_id FROM canonical_passages WHERE tenant_id=%s',(self.tenant,)).fetchall()]
        writer.upsert_passages(self.tenant,ids)
        self.store.turbopuffer=writer.settings;self.store.turbopuffer_client=writer.client;self.store.search_plane='turbopuffer'

    def lookup(self,filters=None):
        return self.bound._parent_scoped_receipts(source_id=self.source,parent_id='session',terms=['archived','passage'],filters=filters,limit=20)

    def test_parent_receipt_exact_identity_survives_admission(self):
        self.prepare_parent();baseline=self.lookup();self.assertEqual(baseline,(self.receipt,))
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):self.assertEqual(self.lookup(),baseline)

    def test_parent_receipt_filters_each_member_without_reviving_deleted_rows(self):
        self.prepare_parent(mixed=True);self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.ingest('turn/physical','',tombstone=True)
            self.assertEqual(self.lookup(),(self.receipt,))
            self.assertEqual(self.lookup({'since':'2026-10-15'}),())

    def test_parent_receipt_rejects_profile_contradiction_and_stale_vendor(self):
        self.prepare_parent();doc=self.baseline(self.receipt)['document_id'];self.admit_fixture(doc)
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            self.assertEqual(self.lookup(),())
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            for row in self.namespace.rows.values():row['manifest_content_sha256']='f'*64
            self.assertEqual(self.lookup(),())

    def test_parent_receipt_catalog_and_membership_share_one_snapshot(self):
        from recall_server import receipt_membership
        self.prepare_parent();doc=self.baseline(self.receipt)['document_id'];self.admit_fixture(doc)
        owner=receipt_membership.lookup_receipt_metadata
        def delete_after_owner(*args,**kwargs):
            result=owner(*args,**kwargs)
            with self.store.connect() as other:
                other.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            return result
        with patch.object(self.store,'verified_singleton_receipts',True):
            with patch.object(receipt_membership,'lookup_receipt_metadata',delete_after_owner):self.assertEqual(self.lookup(),(self.receipt,))
            self.assertEqual(self.lookup(),())


if __name__=='__main__':
    suite=unittest.TestSuite(SingletonParentReceipts(name) for name in SingletonParentReceipts.__dict__ if name.startswith('test_parent_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
