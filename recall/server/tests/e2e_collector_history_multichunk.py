"""Recover bounded original canonical chunks without changing body authority."""
import hashlib
import json
from contextlib import nullcontext
import unittest
from unittest.mock import patch


import e2e_collector_history_recovery as fixture
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.projectors import canonical_json, item_receipt
from recall_server.collector_history_recovery import CollectorHistoryRecoveryError
from recall_server.logical_evidence_projection import LogicalEvidenceError


class MultichunkRecovery(unittest.TestCase):
    setUpClass = classmethod(fixture.CollectorHistoryRecovery.setUpClass.__func__)
    tearDownClass = classmethod(fixture.CollectorHistoryRecovery.tearDownClass.__func__)
    setUp = fixture.CollectorHistoryRecovery.setUp
    ingest = fixture.CollectorHistoryRecovery.ingest
    delete = fixture.CollectorHistoryRecovery.delete
    before = fixture.CollectorHistoryRecovery.before
    recover = fixture.CollectorHistoryRecovery.recover
    assert_no_correction = fixture.CollectorHistoryRecovery.assert_no_correction

    def prepare_chunks(self, text, *, generation=0, total_bytes=None):
        root = dict(self.root, ts='1784332800.000200', text=text)
        prepared = self.ingest(root, path='prepare', connector_id='claude.jsonl')
        event = prepared['envelope']
        event['kind'] = 'transcript_record'
        event['native_parent_id'] = 'parent:recovery'
        event['provenance']['connector_schema_version'] = 1
        event['content']['_recall_collector_generation'] = generation
        if total_bytes is not None:
            padding = total_bytes - len(canonical_json(event['content']))
            self.assertGreaterEqual(padding, 0)
            event['content']['text'] += 'x' * padding
        self.original_text = canonical_json(event['content']).decode()
        event['content_sha256'] = hashlib.sha256(self.original_text.encode()).hexdigest()
        prepared['text_redacted'] = self.original_text
        self.original = self.plane.ingest_document(**prepared)
        with self.store.connect() as connection:
            self.captured = connection.execute('''SELECT c.ordinal,c.text_redacted,c.receipt,c.text_sha256
                FROM canonical_chunks c JOIN canonical_documents d USING(tenant_id,source_id,document_id)
                WHERE d.tenant_id=%s AND d.source_id=%s AND d.native_id=%s ORDER BY c.ordinal''',
                (self.tenant, self.source, event['native_id'])).fetchall()
        marker = self.ingest(self.delete(root), path='prepare', connector_id='claude.jsonl')
        marker['envelope']['content']['deletion_id'] = 'legacy-filesystem-cleanup'
        marker['envelope']['content_sha256'] = hashlib.sha256(canonical_json(marker['envelope']['content'])).hexdigest()
        with patch('recall_server.canonical._filesystem_cleanup', return_value=False):
            self.plane.ingest_document(**marker)
        with self.store.connect() as connection:
            document = connection.execute('''SELECT document_id,text_sha256 FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND native_id=%s''',
                (self.tenant, self.source, event['native_id'])).fetchone()
            # Reproduce retained historical bytes using actual writer boundaries.
            for chunk in self.captured:
                connection.execute('''UPDATE canonical_chunks SET text_redacted=%s
                    WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND ordinal=%s''',
                    (chunk['text_redacted'], self.tenant, self.source, document['document_id'], chunk['ordinal']))
            events = connection.execute('''SELECT event_id FROM canonical_events
                WHERE tenant_id=%s AND source_id=%s AND native_id=%s ORDER BY revision''',
                (self.tenant, self.source, event['native_id'])).fetchall()
        self.expected = dict(source_id=self.source, native_id=event['native_id'],
            original_event_id=events[0]['event_id'], marker_event_id=events[1]['event_id'],
            document_id=document['document_id'], text_sha256=document['text_sha256'],
            text_bytes=len(self.original_text.encode()))
        return self.captured

    def assert_recovered(self):
        historical = self.before()
        result = self.recover()
        self.assertEqual(result['revision'], 3)
        self.assertEqual(self.before(), historical)
        resolved = self.store.resolve(result['receipt'], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        shown = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.admin,
            authorized_sources=(self.source,), chunk_body_archive=self.archive).show(result['receipt'])
        items = resolved['items']
        self.assertEqual([row['ordinal'] for row in items], list(range(len(items))))
        receipts = [item_receipt(self.source, self.expected['native_id'], 3, n) for n in range(len(items))]
        self.assertEqual([row['receipt'] for row in items], receipts)
        self.assertEqual(shown['opened_receipts'], receipts)
        self.assertEqual([row['text_redacted'] for row in items], [row['text'] for row in shown['chunks']])
        body = ''.join(row['text_redacted'] for row in items)
        original = json.loads(self.original_text)
        original['_recall_collector_generation'] += 1
        self.assertEqual(body.encode(), canonical_json(original))
        with self.store.connect() as connection:
            audit = connection.execute('SELECT status FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone()
        self.assertEqual(audit, {'status': 'success'})
        return items

    def test_two_original_chunks_recover_full_body_and_every_public_receipt(self):
        self.assertEqual(len(self.prepare_chunks('x' * 30000)), 2)
        self.assert_recovered()

    def test_three_utf8_chunks_preserve_exact_body(self):
        self.assertEqual(len(self.prepare_chunks('α😀' * 8300)), 3)
        self.assert_recovered()

    def test_generation_increment_can_add_a_new_chunk(self):
        self.assertEqual(len(self.prepare_chunks('x', generation=9, total_bytes=48000)), 2)
        self.assertEqual(len(self.assert_recovered()), 3)

    def alter_chunk(self, assignment, params=(), *, ordinal=1, historical=False):
        from e2e_logical_evidence_projection import legacy_canonical_state
        with self.store.connect() as connection:
            # Only explicit pre-invariant corruption fixtures use the legacy
            # writer; the exact installed collector is restored before recovery.
            with legacy_canonical_state(connection) if historical else nullcontext():
                connection.execute(f'''UPDATE canonical_chunks SET {assignment}
                    WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND ordinal=%s''',
                    (*params, self.tenant, self.source, self.expected['document_id'], ordinal))

    def test_later_chunk_corruption_cannot_reach_raw_or_write(self):
        self.prepare_chunks('x' * 30000)
        self.alter_chunk("text_redacted='z'||substr(text_redacted,2)")
        with patch.object(self.archive, 'verify_raw_bounded') as head:
            with self.assertRaises(LogicalEvidenceError):
                self.recover()
        head.assert_not_called()
        self.assert_no_correction()

    def test_wrong_later_receipt_refuses_before_raw(self):
        self.prepare_chunks('x' * 30000)
        self.alter_chunk('receipt=%s', ('recall://wrong/identity?rev=1#item=1',), historical=True)
        with patch.object(self.archive, 'verify_raw_bounded') as head:
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
                self.recover()
        head.assert_not_called()
        self.assert_no_correction()

    def test_missing_later_chunk_refuses(self):
        self.prepare_chunks('x' * 30000)
        with self.store.connect() as connection:
            connection.execute('''DELETE FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s
                AND document_id=%s AND ordinal=1''', (self.tenant, self.source, self.expected['document_id']))
        with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
            self.recover()
        self.assert_no_correction()

    def test_ordinal_gap_refuses(self):
        self.prepare_chunks('x' * 30000)
        self.alter_chunk('ordinal=2', historical=True)
        with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
            self.recover()
        self.assert_no_correction()

    def test_empty_chunk_refuses(self):
        self.prepare_chunks('x' * 30000)
        self.alter_chunk("text_redacted=''")
        with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
            self.recover()
        self.assert_no_correction()

    def test_expected_byte_limit_matches_canonical_ingestion_domain(self):
        self.expected['text_bytes'] = 8_000_001
        with patch.object(self.archive, 'verify_raw_bounded') as head:
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'scope_invalid'):
                self.recover()
        head.assert_not_called()
        self.assert_no_correction()

    def test_later_chunk_snapshot_change_during_head_refuses(self):
        self.prepare_chunks('x' * 30000)
        original_head = self.archive.verify_raw_bounded
        def changed(*args, **kwargs):
            result = original_head(*args, **kwargs)
            self.alter_chunk("deleted_at=deleted_at+interval '1 second'")
            return result
        with patch.object(self.archive, 'verify_raw_bounded', side_effect=changed):
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'snapshot_changed'):
                self.recover()
        self.assert_no_correction()

    def test_later_chunk_body_change_during_head_refuses(self):
        self.prepare_chunks('x' * 30000)
        original_head = self.archive.verify_raw_bounded
        def changed(*args, **kwargs):
            result = original_head(*args, **kwargs)
            self.alter_chunk("text_redacted='z'||substr(text_redacted,2)")
            return result
        with patch.object(self.archive, 'verify_raw_bounded', side_effect=changed):
            with self.assertRaises(LogicalEvidenceError):
                self.recover()
        self.assert_no_correction()

    def test_overlapping_bytes_with_valid_chunk_hash_still_refuse_full_body(self):
        self.prepare_chunks('x' * 30000)
        overlap = self.captured[0]['text_redacted'][:len(self.captured[1]['text_redacted'])]
        self.alter_chunk('text_redacted=%s,text_sha256=%s',
                         (overlap, hashlib.sha256(overlap.encode()).hexdigest()))
        with self.assertRaises(LogicalEvidenceError):
            self.recover()
        self.assert_no_correction()

    def test_reordered_valid_chunks_refuse_full_body(self):
        self.prepare_chunks('x' * 30000)
        for ordinal, chunk in enumerate(reversed(self.captured)):
            self.alter_chunk('text_redacted=%s,text_sha256=%s',
                             (chunk['text_redacted'], chunk['text_sha256']), ordinal=ordinal)
        with self.assertRaises(LogicalEvidenceError):
            self.recover()
        self.assert_no_correction()

    def test_single_chunk_metadata_refusal_stays_explicit(self):
        self.alter_chunk('text_sha256=%s', ('0' * 64,), ordinal=0)
        with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
            self.recover()
        self.assert_no_correction()

    def test_metadata_overflow_refuses_before_raw(self):
        self.prepare_chunks('x' * 30000)
        with self.store.connect() as connection:
            connection.execute('''INSERT INTO canonical_chunks
                (tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256,deleted_at)
                SELECT tenant_id,source_id,chunk_id||':overflow:'||n::text,document_id,n,
                       %s||n::text,'x',%s,deleted_at
                FROM canonical_chunks CROSS JOIN generate_series(2,507) n
                WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND ordinal=0''',
                (item_receipt(self.source, self.expected['native_id'], 1, 0)[:-1],
                 hashlib.sha256(b'x').hexdigest(), self.tenant, self.source, self.expected['document_id']))
        with patch.object(self.archive, 'verify_raw_bounded') as head:
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'chunk_changed'):
                self.recover()
        head.assert_not_called()
        self.assert_no_correction()


if __name__ == '__main__':
    unittest.main()
