#!/usr/bin/env python3
"""Preserve complete parent proof after an admitted singleton loses its row."""
import os
import unittest
from unittest.mock import patch
from e2e_receipt_membership import ReceiptMembershipTests
from recall_server.locator_backfill_plan import plan_parent
from recall_server.streaming_locators import publish_parent_locators
from recall_server.chunk_retirement import retire_parent_chunks, set_parent_retirement_enabled


class SingletonMaintenance(ReceiptMembershipTests):
    def prepare(self, *, mixed=False):
        receipt = self.ingest('turn/one', 'Exact locator compatibility evidence')['receipt']
        if mixed:
            self.ingest('turn/two', 'Retained physical source body')
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.scope = dict(tenant_id=self.tenant, source_id=self.source, native_parent_id='session')
        return self.baseline(receipt)['document_id']

    def test_maintenance_plan_keeps_admitted_member_in_full_byte_proof(self):
        doc = self.prepare()
        baseline = plan_parent(self.store,self.archive,**self.scope)
        self.assertEqual(baseline['eligible_documents'],1)
        self.admit_fixture(doc)
        with patch.object(self.store,'verified_singleton_receipts',True):
            result = plan_parent(self.store,self.archive,**self.scope)
        self.assertEqual(result['eligible_documents'],1)
        self.assertEqual(result['unchanged_locators'],1)
        self.assertEqual(result['verified_current_pg_chunk_bytes'],0)

    def test_maintenance_streaming_repairs_admitted_missing_locator(self):
        doc = self.prepare()
        self.admit_fixture(doc)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND document_id=%s RETURNING document_id',
                                          (self.tenant,doc)).fetchone()
        with patch.object(self.store,'verified_singleton_receipts',True):
            preview = publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal)
            self.assertEqual(preview['proposed_documents'],1)
            result = publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal,
                                            apply=True,reviewed_plan=preview['plan'])
        self.assertEqual(result['published_documents'],1)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s',
                                                (self.tenant,doc)).fetchone()['n'],0)

    def test_maintenance_retirement_clears_only_physical_peer_after_manifest_reset(self):
        doc = self.prepare()
        self.admit_fixture(doc)
        set_parent_retirement_enabled(self.store,**self.scope,enabled=True)
        with patch.object(self.store,'verified_singleton_receipts',True), patch.dict(os.environ,{'RECALL_CHUNK_BODY_READS':'archive'}):
            self.ingest('turn/two', 'Retained physical source body')
            self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
            preview = retire_parent_chunks(self.store,self.archive,**self.scope)
            self.assertEqual(preview['eligible_documents'],2)
            result = retire_parent_chunks(self.store,self.archive,**self.scope,apply=True,reviewed_plan=preview['plan'],
                                          owner_principal_id=self.principal)
        self.assertTrue(result['complete'])
        self.assertEqual(result['cleared_documents'],1)
        self.assertEqual(result['cleared_chunks'],1)
        self.assertEqual(result['cleared_utf8_bytes'],len('Retained physical source body'.encode()))

    def test_maintenance_unmarked_missing_rows_are_never_inferred_from_archive(self):
        from recall_server.locator_backfill_plan import LocatorPlanError
        doc = self.prepare()
        self.admit_fixture(doc)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=NULL WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.assertRaises(LocatorPlanError):
                plan_parent(self.store,self.archive,**self.scope)

    def test_maintenance_invalid_profile_and_archive_hash_still_refuse(self):
        from recall_server.locator_backfill_plan import LocatorPlanError
        doc = self.prepare()
        self.admit_fixture(doc)
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
            with self.assertRaises(LocatorPlanError):
                plan_parent(self.store,self.archive,**self.scope)
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1,text_sha256=%s WHERE tenant_id=%s AND document_id=%s', ('0'*64,self.tenant,doc))
            with self.assertRaises(LocatorPlanError):
                plan_parent(self.store,self.archive,**self.scope)

    def test_maintenance_locked_locator_check_rejects_changed_profile(self):
        from recall_server import streaming_locators
        doc = self.prepare()
        self.admit_fixture(doc)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
        original = streaming_locators._publish_batch
        def corrupt_after_proof(*args,**kwargs):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
            return original(*args,**kwargs)
        with patch.object(self.store,'verified_singleton_receipts',True):
            plan = publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal)
            with patch.object(streaming_locators,'_publish_batch',corrupt_after_proof):
                with self.assertRaises(streaming_locators.LocatorPublicationError) as caught:
                    publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal,
                                            apply=True,reviewed_plan=plan['plan'])
        self.assertEqual(caught.exception.committed,dict(batches=0,published_documents=0))
        with self.store.connect() as connection:
            row = connection.execute('SELECT body_record_ordinal,body_record_count FROM canonical_documents WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc)).fetchone()
        self.assertEqual(tuple(row.values()),(None,None))

    def test_maintenance_locked_retirement_check_rejects_changed_profile_before_clear(self):
        from recall_server import chunk_retirement
        doc = self.prepare(mixed=True)
        self.admit_fixture(doc)
        set_parent_retirement_enabled(self.store,**self.scope,enabled=True)
        original = chunk_retirement._retire_parent_batch
        def remove_admission_after_proof(*args,**kwargs):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=NULL WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
            return original(*args,**kwargs)
        with patch.object(self.store,'verified_singleton_receipts',True), patch.dict(os.environ,{'RECALL_CHUNK_BODY_READS':'archive'}):
            plan = retire_parent_chunks(self.store,self.archive,**self.scope)
            with patch.object(chunk_retirement,'_retire_parent_batch',remove_admission_after_proof):
                with self.assertRaises(chunk_retirement.ChunkRetirementError) as caught:
                    retire_parent_chunks(self.store,self.archive,**self.scope,apply=True,reviewed_plan=plan['plan'],owner_principal_id=self.principal)
        self.assertEqual(caught.exception.committed['cleared_utf8_bytes'],0)
        with self.store.connect() as connection:
            body = connection.execute('SELECT text_redacted FROM canonical_chunks WHERE tenant_id=%s', (self.tenant,)).fetchone()['text_redacted']
        self.assertEqual(body,'Retained physical source body')

    def test_maintenance_locator_lost_commit_ack_keeps_durable_position_unknown(self):
        from contextlib import contextmanager
        import psycopg
        from recall_server import streaming_locators
        doc = self.prepare()
        self.admit_fixture(doc)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET body_record_ordinal=NULL,body_record_count=NULL WHERE tenant_id=%s AND document_id=%s', (self.tenant,doc))
        transaction = psycopg.Connection.transaction
        publish = streaming_locators._publish_batch
        armed = False
        def arm(*args,**kwargs):
            nonlocal armed
            armed=True
            try:
                return publish(*args,**kwargs)
            finally:
                armed=False
        @contextmanager
        def commit_then_disconnect(connection,*args,**kwargs):
            with transaction(connection,*args,**kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic lost commit acknowledgement')
        with patch.object(self.store,'verified_singleton_receipts',True):
            plan = publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal)
            with patch.object(streaming_locators,'_publish_batch',arm), patch.object(psycopg.Connection,'transaction',commit_then_disconnect):
                with self.assertRaises(streaming_locators.LocatorPublicationError) as caught:
                    publish_parent_locators(self.store,self.archive,**self.scope,owner_principal_id=self.principal,
                                            apply=True,reviewed_plan=plan['plan'])
        self.assertTrue(caught.exception.commit_unknown)
        self.assertEqual(caught.exception.committed,dict(batches=0,published_documents=0))
        with self.store.connect() as connection:
            row=connection.execute('SELECT body_record_ordinal,body_record_count FROM canonical_documents WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc)).fetchone()
        self.assertEqual(tuple(row.values()),(0,1))


if __name__=='__main__':
    names=[n for n in dir(SingletonMaintenance) if n.startswith('test_maintenance_')]
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonMaintenance(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
