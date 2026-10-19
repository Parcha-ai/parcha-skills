"""Native streamed metadata parity and refusal against the receipt owner."""
import json
import time
import unittest
from unittest.mock import patch

import e2e_retirement_stream_capture as original
from recall_server import parent_chunk_proof as proof, receipt_membership
from e2e_logical_evidence_projection import legacy_canonical_state


class StreamMembership(unittest.TestCase):
    setUp = original.StreamCapture.setUp
    capture = original.StreamCapture.capture

    def test_complete_capture_does_not_repeat_receipt_sql(self):
        queries = []
        execute = self.store._execute_bounded

        def observe(connection, statement, parameters, deadline):
            if 'AS wanted(receipt,source_id,native_id,revision,ordinal)' in statement:
                queries.append(statement)
            return execute(connection, statement, parameters, deadline)

        with patch.object(self.store, '_execute_bounded', observe):
            with proof.prove_parent_chunks(self.store, self.archive, scope=self.scope,
                    limits=self.limits, deadline_at=time.monotonic() + 10) as complete:
                self.assertEqual(complete['current_documents'], 12)
                self.assertEqual(complete['current_chunks'], 12)
                self.assertEqual(complete['eligible_documents'], 12)
                self.assertEqual(complete['excluded'], {})
                self.assertEqual(complete['eligible_utf8_bytes'], sum(len(t.encode()) for t in self.texts.values()))
        self.assertEqual(queries, [], 'complete streamed metadata must not be reread in receipt batches')


    def assert_native_rows_equal(self):
        result, payloads = self.capture()
        documents = [json.loads(row[0]) for row in payloads]
        with self.store.connect() as connection, connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            native = receipt_membership.verified_document_chunks(self.store, connection,
                tenant_id=self.tenant, source_id=self.source, documents=documents,
                deadline_at=time.monotonic() + 10)
        self.assertEqual({row['document_id']: row['chunks'] for row in documents}, native)
        return result, documents

    def assert_native_refusal_equal(self):
        # Capture physical rows through the unchanged disabled path, then ask
        # the original full SQL receipt owner about this exact database state.
        with patch.object(self.store, 'verified_singleton_receipts', False):
            _, payloads = self.capture()
        documents = [json.loads(row[0]) for row in payloads]
        with self.store.connect() as connection, connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            with self.assertRaises(receipt_membership.ReceiptMembershipError) as baseline:
                receipt_membership.verified_document_chunks(self.store, connection,
                    tenant_id=self.tenant, source_id=self.source, documents=documents,
                    deadline_at=time.monotonic() + 10)
        with self.assertRaises(receipt_membership.ReceiptMembershipError) as candidate:
            self.capture()
        self.assertEqual(str(candidate.exception), str(baseline.exception))

    def test_physical_and_deleted_chunks_match_native_owner(self):
        self.assertEqual(self.assert_native_rows_equal()[0][2:], (12, 12))
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.docs[0]['document_id']))
        self.assertEqual(self.assert_native_rows_equal()[0][2:], (12, 11))

    def test_malformed_and_foreign_receipts_refuse(self):
        document = self.docs[0]['document_id']
        with self.store.connect() as connection:
            old = connection.execute('SELECT receipt FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, document)).fetchone()['receipt']
        for receipt in (old + 'invalid', old.replace('?rev=1', '?rev=01'),
                'recall://' + self.source + '/missing-foreign-native?rev=1#item=0'):
            with self.subTest(receipt_kind=receipt.rsplit('#', 1)[-1]):
                with self.store.connect() as connection, legacy_canonical_state(connection):
                    connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND document_id=%s',
                        (receipt, self.tenant, document))
                self.assert_native_refusal_equal()
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute('UPDATE canonical_chunks SET receipt=%s WHERE tenant_id=%s AND document_id=%s',
                (old, self.tenant, document))
        self.assertEqual(self.assert_native_rows_equal()[0][2:], (12, 12))

    def test_profile_cannot_hide_deleted_or_other_ordinal_physical_row(self):
        document = self.docs[0]['document_id']
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, document))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, document))
        self.assert_native_refusal_equal()
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute("UPDATE canonical_chunks SET deleted_at=NULL,ordinal=1,receipt=replace(receipt,'#item=0','#item=1') WHERE tenant_id=%s AND document_id=%s",
                (self.tenant, document))
        self.assert_native_refusal_equal()

    def test_current_document_and_event_fences_remain(self):
        document = self.docs[0]['document_id']
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, document))
        self.assertEqual(self.assert_native_rows_equal()[0][2:], (11, 11))
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=true WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, document))
            connection.execute('UPDATE canonical_events SET is_tombstone=true WHERE tenant_id=%s AND event_id=(SELECT event_id FROM canonical_documents WHERE tenant_id=%s AND document_id=%s)',
                (self.tenant, self.tenant, document))
        self.assertEqual(self.assert_native_rows_equal()[0][2:], (11, 11))

    def test_actual_admitted_singleton_matches_native_owner(self):
        from e2e_singleton_metadata_admission import SingletonMetadataAdmission
        f = SingletonMetadataAdmission()
        f.store = self.store
        f.setUp()
        try:
            original_metadata = f.prepare()
            preview = f.admit(original_metadata)
            self.assertEqual(f.admit(original_metadata, apply=True, reviewed_plan=preview['plan'])['retired_chunks'], 1)
            with proof.ParentMetadataSpool(max_bytes=self.limits.max_spool_bytes) as spool:
                captured = proof._capture(self.store, spool, (f.tenant, f.source, 'session'),
                    self.limits, time.monotonic() + 10)
                documents = [json.loads(row[0]) for row in spool.index.execute('SELECT payload FROM documents')]
            self.assertEqual(captured[2:], (1, 1))
            with self.store.connect() as connection:
                native = receipt_membership.verified_document_chunks(self.store, connection,
                    tenant_id=f.tenant, source_id=f.source, documents=documents,
                    deadline_at=time.monotonic() + 10)
            self.assertEqual(documents[0]['chunks'], native[original_metadata['document_id']])
            self.assertEqual(documents[0]['pg_body_bytes'], 0)
        finally:
            f.doCleanups()


if __name__ == '__main__':
    unittest.main(verbosity=2)
