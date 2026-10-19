"""Same-snapshot row contract against the pre-change native capture query."""
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

import psycopg

import e2e_retirement_stream_capture as original
from e2e_logical_evidence_projection import insert_record, legacy_canonical_state
from recall_server import parent_chunk_proof as proof


class CaptureDocumentIdentity(unittest.TestCase):
    capture = original.StreamCapture.capture

    def setUp(self):
        original.StreamCapture.setUp(self)
        stream = psycopg.Cursor.stream
        queries = []

        def observe(cursor, query, parameters=None, **kwargs):
            queries.append((query, parameters))
            yield from stream(cursor, query, parameters, **kwargs)

        with patch.object(psycopg.Cursor, 'stream', observe):
            self.capture()
        self.assertEqual(len(queries), 1)
        self.query, self.parameters = queries[0]
        self.before = Path(__file__).with_name('capture_document_before_join.sql').read_text()

    def assert_same_rows(self, scope=None):
        parameters = self.parameters if scope is None else (*scope, *self.parameters[3:])
        with self.store.connect() as connection, connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            before = self.store._execute_bounded(connection, self.before, parameters,
                time.monotonic() + 10).fetchall()
            after = self.store._execute_bounded(connection, self.query, parameters,
                time.monotonic() + 10).fetchall()
        self.assertEqual(after, before)
        self.assertEqual(json.dumps(after, default=str), json.dumps(before, default=str))
        return after

    def test_full_rows_preserve_scope_currentness_deleted_and_profile_fields(self):
        self.assertEqual(len(self.assert_same_rows()), 12)
        with self.store.connect() as connection:
            insert_record(connection, tenant=self.tenant, source=self.source,
                parent='another-parent', native='another-native', text='Other parent body',
                role='assistant', byte_start=999)
        self.assertEqual(len(self.assert_same_rows()), 12)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.docs[0]['document_id']))
            connection.execute('UPDATE canonical_documents SET is_current=false, deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.docs[1]['document_id']))
            connection.execute('UPDATE canonical_events SET is_tombstone=true WHERE tenant_id=%s AND event_id=(SELECT event_id FROM canonical_documents WHERE tenant_id=%s AND document_id=%s)',
                (self.tenant, self.tenant, self.docs[2]['document_id']))
        self.assertEqual(len(self.assert_same_rows()), 9)
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute('UPDATE canonical_documents SET chunk_receipt_profile=1 WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.docs[3]['document_id']))
            connection.execute('UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND document_id=%s',
                (self.tenant, self.docs[3]['document_id']))
        rows = self.assert_same_rows()
        row = next(r for r in rows if r['document_id'] == self.docs[3]['document_id'])
        self.assertTrue(row['membership_physical'])
        self.assertEqual(row['chunks'], [])

    def test_admitted_chunkless_singleton_preserves_complete_native_capture(self):
        from e2e_singleton_metadata_admission import SingletonMetadataAdmission
        f = SingletonMetadataAdmission()
        f.store = self.store
        f.setUp()
        try:
            metadata = f.prepare()
            preview = f.admit(metadata)
            self.assertEqual(f.admit(metadata, apply=True,
                reviewed_plan=preview['plan'])['retired_chunks'], 1)
            scope = (f.tenant, f.source, 'session')
            rows = self.assert_same_rows(scope)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['membership_profile'], 1)
            self.assertFalse(rows[0]['membership_physical'])
            with proof.ParentMetadataSpool(max_bytes=self.limits.max_spool_bytes) as spool:
                captured = proof._capture(self.store, spool, scope, self.limits,
                    time.monotonic() + 10)
                payloads = [json.loads(r[0]) for r in spool.index.execute('SELECT payload FROM documents')]
            self.assertEqual(captured[2:], (1, 1))
            self.assertEqual(payloads[0]['pg_body_bytes'], 0)
            self.assertEqual(payloads[0]['chunks'][0]['receipt'], metadata['receipt'])
        finally:
            f.doCleanups()


if __name__ == '__main__':
    unittest.main(verbosity=2)
