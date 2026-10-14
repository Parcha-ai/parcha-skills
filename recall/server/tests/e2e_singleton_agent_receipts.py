#!/usr/bin/env python3
"""Agent output retains exact admitted-document receipt scope."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from e2e_singleton_passage_reads import SingletonPassageReads
from recall_server.deep_inspection import DeepInspectionError


class SingletonAgentReceipts(SingletonPassageReads):
    def execute(self, receipt=None):
        receipt=receipt or self.receipt
        record=dict(event_native_id='turn/passage',occurred_at='2026-10-14T00:00:00Z',ordinal=0,
                    receipts=[receipt],text='Exact archived passage α 🧠 '*16)
        self.bound.deep_inspector=SimpleNamespace(execute=lambda **kwargs:dict(stdout=json.dumps(record)+'\nRECALL_EVIDENCE '+receipt+'\n',complete=True))
        return self.bound.execute_agent_program('synthetic local inspector',
            logical_document_ids=(self.args['logical_document_id'],),record_spans={},routing_receipts={},timeout_seconds=10)

    def test_agent_output_receipt_survives_admission(self):
        self.prepare_passages()
        self.assertEqual(self.execute()['opened_receipts'],[self.receipt])
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.assertEqual(self.execute()['opened_receipts'],[self.receipt])

    def test_agent_output_cannot_cite_another_parent_after_admission(self):
        self.prepare_passages();ingest=self.plane.ingest_document
        def other_parent(**kwargs):
            kwargs['envelope']={**kwargs['envelope'],'native_parent_id':'other-session'}
            return ingest(**kwargs)
        with patch.object(self.plane,'ingest_document',other_parent):
            other=self.ingest('turn/other','Other parent evidence')['receipt']
        self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1,upload_concurrency=1)
        self.admit_fixture(self.baseline(other)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.assertRaises(DeepInspectionError):self.execute(other)

    def test_agent_output_rechecks_after_execution_tombstones_source(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        record=dict(event_native_id='turn/passage',occurred_at='2026-10-14T00:00:00Z',ordinal=0,receipts=[self.receipt],text='Exact archived passage α 🧠 '*16)
        def execute_then_delete(**kwargs):
            self.ingest('turn/passage','',tombstone=True)
            return dict(stdout=json.dumps(record),complete=True)
        self.bound.deep_inspector=SimpleNamespace(execute=execute_then_delete)
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.assertRaises(DeepInspectionError):
                self.bound.execute_agent_program('synthetic local inspector',logical_document_ids=(self.args['logical_document_id'],),record_spans={},routing_receipts={},timeout_seconds=10)

    def test_agent_output_invalid_profile_is_not_a_verified_citation(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s',(self.tenant,))
        with patch.object(self.store,'verified_singleton_receipts',True):
            with self.assertRaises(DeepInspectionError):self.execute()

    def test_agent_output_default_needs_no_future_profile_column(self):
        self.prepare_passages()
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_documents DROP COLUMN chunk_receipt_profile')
        try:self.assertEqual(self.execute()['opened_receipts'],[self.receipt])
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE canonical_documents ADD COLUMN chunk_receipt_profile smallint')


if __name__=='__main__':
    suite=unittest.TestSuite(SingletonAgentReceipts(name) for name in SingletonAgentReceipts.__dict__ if name.startswith('test_agent_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
