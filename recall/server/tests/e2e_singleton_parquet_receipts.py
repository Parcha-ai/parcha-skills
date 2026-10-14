#!/usr/bin/env python3
"""Keep emitted scan receipt authority after separately admitted metadata removal."""
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from e2e_singleton_passage_reads import SingletonPassageReads
from recall_server.deep_inspection import DeepInspectionError


class SingletonParquetReceipts(SingletonPassageReads):
    def verify(self, receipts=None, **overrides):
        args=dict(sources=[self.source],since=None,until=None,person=None,relation=None)
        args.update(overrides)
        return self.bound._verify_parquet_receipts(receipts or [self.receipt],**args)

    def test_scan_receipt_exact_scope_survives_admission(self):
        self.prepare_passages(mixed=True)
        self.verify([self.receipt,self.physical_receipt])
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.verify([self.receipt,self.physical_receipt])

    def test_scan_receipt_preserves_actor_time_and_evidence_scope(self):
        self.prepare_passages()
        with self.store.connect() as connection:
            connection.execute("INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES(%s,'actor_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','human','Sample Person')",(self.tenant,))
            connection.execute("INSERT INTO canonical_source_actor_bindings(tenant_id,source_id,actor_id,relation) VALUES(%s,%s,'actor_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','contributor')",(self.tenant,self.source))
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.verify(person='Sample Person',relation='contributor')
            for filters in (dict(person='Other Person'),dict(person='Sample Person',relation='author'),
                dict(since=datetime(2026,10,15,tzinfo=timezone.utc)),dict(until=datetime(2026,10,13,tzinfo=timezone.utc)),
                dict(sources=[self.source+':wrong'])):
                with self.subTest(filters=filters),self.assertRaises(DeepInspectionError):self.verify(**filters)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s',(self.tenant,self.source))
            with self.assertRaises(DeepInspectionError):self.verify()

    def test_scan_receipt_keeps_physical_and_profile_contradictions_closed(self):
        self.prepare_passages(mixed=True)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND receipt=%s',(self.tenant,self.physical_receipt))
            with self.assertRaises(DeepInspectionError):self.verify([self.receipt,self.physical_receipt])
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND chunk_receipt_profile=1',(self.tenant,))
            with self.assertRaises(DeepInspectionError):self.verify()

    def test_scan_receipt_real_tombstone_revokes_admitted_output(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.ingest('turn/passage','',tombstone=True)
            with self.assertRaises(DeepInspectionError):self.verify()

    def test_scan_receipt_authority_and_filters_share_one_snapshot(self):
        from recall_server import receipt_membership
        self.prepare_passages();doc=self.baseline(self.receipt)['document_id'];self.admit_fixture(doc)
        owner=receipt_membership.lookup_receipt_metadata
        def delete_after_owner(*args,**kwargs):
            result=owner(*args,**kwargs)
            with self.store.connect() as other:
                other.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            return result
        with patch.object(self.store,'verified_singleton_receipts',True):
            with patch.object(receipt_membership,'lookup_receipt_metadata',delete_after_owner):self.verify()
            with self.assertRaises(DeepInspectionError):self.verify()

    def test_scan_receipt_default_needs_no_future_profile_column(self):
        self.prepare_passages()
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_documents DROP COLUMN chunk_receipt_profile')
        try:self.verify()
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_documents ADD COLUMN chunk_receipt_profile smallint')

    def test_scan_public_execution_accepts_only_verified_emitted_receipt(self):
        from types import SimpleNamespace
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.parquet_scan import CanonicalParquetScanProjector
        from recall_server.canonical import CanonicalArchiveGateway
        self.source=self.source.replace('source:membership:','claude:membership:')
        self.gateway=CanonicalArchiveGateway(self.store,self.archive,tenant_id=self.tenant,principal_id=self.principal)
        self.prepare_passages()
        projector=CanonicalParquetScanProjector(self.store,LogicalEvidenceProjectionStore(self.archive))
        result=projector.project_pending(tenant_id=self.tenant,batch_size=4,max_batches=1,compaction_budget=0)
        self.assertGreater(result['shards'],0)
        seen=[]
        def execute_scan(**kwargs):
            seen.append(kwargs)
            record=dict(event_native_id='turn/passage',occurred_at='2026-10-14T00:00:00Z',ordinal=0,receipts=[self.receipt],text='Exact archived passage α 🧠 '*16)
            return dict(stdout=json.dumps(record)+'\nRECALL_EVIDENCE '+self.receipt+'\n',complete=True,stopped_reason='completed',output_truncated=False)
        self.bound.deep_inspector=SimpleNamespace(execute_scan=execute_scan)
        baseline=self.bound.execute_parquet_scan('synthetic local scanner',filters={},timeout_seconds=10)
        self.assertEqual(baseline['opened_receipts'],[self.receipt],(baseline,len(seen),result))
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            result=self.bound.execute_parquet_scan('synthetic local scanner',filters={},timeout_seconds=10)
        self.assertEqual(result['opened_receipts'],baseline['opened_receipts'])
        self.assertEqual(len(seen),2);self.assertEqual(seen[0]['objects'],seen[1]['objects'])


if __name__=='__main__':
    suite=unittest.TestSuite(SingletonParquetReceipts(name) for name in SingletonParquetReceipts.__dict__ if name.startswith('test_scan_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
