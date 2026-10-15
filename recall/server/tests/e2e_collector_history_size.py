"""Recovery obeys the same encoded document size contract as canonical ingest."""
import unittest
from unittest.mock import patch

import e2e_collector_history_multichunk as fixture
from recall_server.collector_history_recovery import CollectorHistoryRecoveryError
from recall_server.chunk_retirement import ChunkRetirementError
from recall_server.logical_evidence_projection import LogicalEvidenceError


class CanonicalSizeRecovery(unittest.TestCase):
    setUpClass = classmethod(fixture.MultichunkRecovery.setUpClass.__func__)
    tearDownClass = classmethod(fixture.MultichunkRecovery.tearDownClass.__func__)
    setUp = fixture.MultichunkRecovery.setUp
    ingest = fixture.MultichunkRecovery.ingest
    delete = fixture.MultichunkRecovery.delete
    before = fixture.MultichunkRecovery.before
    recover = fixture.MultichunkRecovery.recover
    assert_no_correction = fixture.MultichunkRecovery.assert_no_correction
    assert_recovered = fixture.MultichunkRecovery.assert_recovered
    alter_chunk = fixture.MultichunkRecovery.alter_chunk

    def prepare_chunks(self, text, **kwargs):
        # Exercise canonical ingest's domain independently of the synthetic
        # Slack connector fixture's smaller input limit. The original helper
        # already adjusts canonical content after connector normalization.
        ingest = self.ingest
        def canonical_input(value, **options):
            prepared = ingest(value, **options)
            if prepared['envelope']['kind'] != 'tombstone':
                prepared['envelope']['content']['text'] = text
            return prepared
        with patch.object(self, 'ingest', canonical_input):
            return fixture.MultichunkRecovery.prepare_chunks(self, 'x', **kwargs)

    def test_twenty_chunk_retained_record_recovers_exact_public_body(self):
        self.assertEqual(len(self.prepare_chunks('x', total_bytes=472540)), 20)
        self.assert_recovered()

    def test_generation_expansion_above_ingest_limit_refuses_before_mutation(self):
        self.prepare_chunks('x', generation=9, total_bytes=8_000_000)
        with patch.object(self.plane, '_write_authorized_document',
                          wraps=self.plane._write_authorized_document) as write:
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'content_too_large'):
                self.recover()
        write.assert_not_called()
        self.assert_no_correction()

    def test_utf8_paragraph_boundaries_recover_at_exact_post_generation_limit(self):
        # Spaces trigger the actual writer's paragraph-aware splits. A ceiling
        # based on fixed 24KB pieces would reject this valid canonical body.
        chunks = self.prepare_chunks(('😀' * 4000 + ' ') * 400,
                                     generation=9, total_bytes=7_999_999)
        self.assertGreater(len(chunks), 334)
        self.assertEqual(len(self.original_text.encode()), 7_999_999)
        self.assertLess(len(self.original_text), 7_999_999)
        items = self.assert_recovered()
        self.assertEqual(sum(len(row['text_redacted'].encode()) for row in items), 8_000_000)

    def test_exact_ingestion_limit_without_generation_growth_is_accepted(self):
        self.assertGreater(len(self.prepare_chunks('x', total_bytes=8_000_000)), 128)
        self.assert_recovered()

    def test_forged_expected_size_above_domain_refuses_before_connection(self):
        self.expected['text_bytes'] = 8_000_001
        with patch.object(self.store, 'connect', wraps=self.store.connect) as connect:
            with self.assertRaisesRegex(CollectorHistoryRecoveryError, 'scope_invalid'):
                self.recover()
        connect.assert_not_called()
        self.assert_no_correction()

    def test_large_last_chunk_corruption_refuses_before_raw_or_write(self):
        chunks = self.prepare_chunks('x', total_bytes=3_200_000)
        self.assertGreater(len(chunks), 128)
        self.alter_chunk("text_redacted='z'||substr(text_redacted,2)", ordinal=len(chunks)-1)
        with patch.object(self.archive, 'verify_raw_bounded') as head:
            with self.assertRaises(LogicalEvidenceError):
                self.recover()
        head.assert_not_called()
        self.assert_no_correction()

    def test_large_source_revocation_during_raw_check_refuses(self):
        self.prepare_chunks('x', total_bytes=472540)
        head = self.archive.verify_raw_bounded
        def revoke(*args, **kwargs):
            head(*args, **kwargs)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                                   (self.tenant, self.source, self.admin))
        with patch.object(self.archive, 'verify_raw_bounded', revoke):
            with self.assertRaises(ChunkRetirementError):
                self.recover()
        self.assert_no_correction()

    def test_large_explicit_forget_during_raw_check_vetoes_recovery(self):
        self.prepare_chunks('x', total_bytes=472540)
        head = self.archive.verify_raw_bounded
        def forget(*args, **kwargs):
            head(*args, **kwargs)
            # Forget also verifies raw metadata; avoid recursively injecting a
            # second forget while the first holds its native advisory locks.
            with patch.object(self.archive, 'verify_raw_bounded', head):
                self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=self.original['receipt'], mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-15T00:00:00Z', idempotency_key='size-recovery-forget'))
        with patch.object(self.archive, 'verify_raw_bounded', forget):
            with self.assertRaisesRegex(RuntimeError, 'forgotten'):
                self.recover()
        self.assert_no_correction()


if __name__ == '__main__':
    unittest.main()
