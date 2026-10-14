#!/usr/bin/env python3
"""Real catalog and owner publication of admitted receipts to a fake vendor."""
import unittest
from unittest.mock import patch
from e2e_singleton_passage_reads import SingletonPassageReads
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class SingletonSearchPublication(SingletonPassageReads):
    def writer(self):
        client = FakeTurbopuffer()
        settings = TurbopufferSettings(api_key='synthetic-local-only')
        writer = TurbopufferProjector(self.store, settings, client=client, batch_rows=1,
                                     write_concurrency=1, tokens_per_minute=0)
        return writer, client.namespace(settings.namespace(self.tenant))

    def test_publication_repair_by_id_preserves_exact_vendor_row_after_admission(self):
        self.prepare_passages()
        writer, namespace = self.writer()
        ids = self.args['passage_ids']
        self.assertEqual(writer.upsert_passages(self.tenant, ids), 1)
        baseline = dict(namespace.rows)
        namespace.rows.clear()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store, 'verified_singleton_receipts', True):
            self.assertEqual(writer.upsert_passages(self.tenant, ids), 1)
        self.assertEqual(namespace.rows, baseline)

    def test_publication_month_crosses_empty_valid_page_to_later_profile(self):
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        from recall_server.passage_index import CanonicalPassageProjector
        from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY

        ingest = self.plane.ingest_document
        for index in range(3):
            def with_parent(*, _index=index, **kwargs):
                kwargs['envelope'] = {**kwargs['envelope'], 'native_parent_id': f'session/{_index}'}
                return ingest(**kwargs)
            with patch.object(self.plane, 'ingest_document', with_parent):
                self.ingest(f'turn/{index}', f'Exact isolated source body {index}')
        self.projector.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        CanonicalPassageProjector(self.store, LogicalEvidenceProjectionStore(self.archive),
            bound_tenant_id=self.tenant, policy=DEFAULT_PASSAGE_POLICY).project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        with self.store.connect() as connection:
            passages = connection.execute('SELECT passage_id,receipts FROM canonical_passages WHERE tenant_id=%s ORDER BY first_occurred_at,passage_id',
                                          (self.tenant,)).fetchall()
        self.assertEqual(len(passages), 3)
        missing = self.baseline(passages[0]['receipts'][0])
        self.admit_fixture(self.baseline(passages[-1]['receipts'][0])['document_id'])
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                               (self.tenant, missing['document_id']))
            connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                               (self.tenant, missing['document_id']))
        writer, namespace = self.writer()
        with patch.object(self.store, 'verified_singleton_receipts', True):
            first = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(first['failed'], 0)
            self.assertEqual(first['rows'], 0)
            self.assertEqual(first['months'], 0)
            self.assertEqual(first['pending'], 1)
            for _ in range(5):
                report = writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                self.assertEqual(report['failed'], 0)
                if report['pending'] == 0:
                    break
            else:
                self.fail('examined-page cursor did not reach the end of the finite source month')
        self.assertEqual(set(namespace.rows), {row['passage_id'] for row in passages[1:]})

    def test_publication_invalid_profile_stops_before_vendor_and_keeps_outbox(self):
        self.prepare_passages()
        doc=self.baseline(self.receipt)['document_id'];self.admit_fixture(doc)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=2 WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
        writer,namespace=self.writer()
        with patch.object(self.store,'verified_singleton_receipts',True):
            result=writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertEqual(result['failed'],1);self.assertEqual(result['pending'],1)
        self.assertEqual(namespace.rows,{})

    def test_publication_retained_deleted_row_is_not_overridden_by_profile(self):
        self.prepare_passages()
        doc=self.baseline(self.receipt)['document_id']
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
        writer,namespace=self.writer()
        with patch.object(self.store,'verified_singleton_receipts',True):
            result=writer.drain_quantum(tenant_id=self.tenant,max_months=1)
        self.assertEqual(result['failed'],1);self.assertEqual(result['pending'],1)
        self.assertEqual(namespace.rows,{})

    def test_publication_authority_snapshot_matches_catalog_then_next_request_denies(self):
        self.prepare_passages()
        doc=self.baseline(self.receipt)['document_id'];self.admit_fixture(doc)
        writer,namespace=self.writer();owner=writer._authorized_publication_rows
        def delete_between(connection,tenant,rows,**kwargs):
            with self.store.connect() as other:
                other.execute('UPDATE canonical_documents SET deleted_at=now(),is_current=false WHERE tenant_id=%s AND document_id=%s',(self.tenant,doc))
            return owner(connection,tenant,rows,**kwargs)
        with patch.object(self.store,'verified_singleton_receipts',True):
            with patch.object(writer,'_authorized_publication_rows',delete_between):
                self.assertEqual(writer.upsert_passages(self.tenant,self.args['passage_ids']),1)
            namespace.rows.clear()
            self.assertEqual(writer.upsert_passages(self.tenant,self.args['passage_ids']),0)
        self.assertEqual(namespace.rows,{})

    def test_publication_lost_vendor_ack_keeps_outbox_and_replays_exact_row(self):
        self.prepare_passages()
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        writer,namespace=self.writer();write=namespace.write
        def commit_then_lose(**kwargs):
            write(**kwargs)
            raise RuntimeError('synthetic lost vendor acknowledgment')
        with patch.object(self.store,'verified_singleton_receipts',True):
            with patch.object(namespace,'write',commit_then_lose):
                result=writer.drain_quantum(tenant_id=self.tenant,max_months=1)
            self.assertEqual(result['failed'],1);self.assertEqual(result['pending'],1)
            self.assertEqual(len(namespace.rows),1);baseline=dict(namespace.rows)
            for _ in range(3):
                result=writer.drain_quantum(tenant_id=self.tenant,max_months=1)
                if not result['pending']:break
            self.assertEqual(result['pending'],0);self.assertEqual(namespace.rows,baseline)


if __name__ == '__main__':
    names = [name for name in dir(SingletonSearchPublication) if name.startswith('test_publication_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(SingletonSearchPublication(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
