"""Native Codex capture, historical cleanup, and explicit admin recovery."""
import copy
import hashlib
import json
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
import e2e_slack_deletion_ordering as fixture
from e2e_collector_history_recovery import CollectorHistoryRecovery
from e2e_collector_history_compacted import LegacyCompactedRecovery, legacy_compact
from collector.collector import Collector
from recall_server.canonical import CanonicalArchiveGateway
from recall_server.projectors import canonical_json, validate_envelope


class CodexCleanupRecovery(CollectorHistoryRecovery):
    source_prefix = 'codex:linux:'
    compact = staticmethod(legacy_compact)
    bulk = False
    def setUp(self):
        fixture.SlackDeletionOrdering.setUp(self)
        self.source = self.source_prefix + uuid.uuid4().hex
        directory = tempfile.TemporaryDirectory(prefix='real-sdk-recovery-')
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        captured = []
        plane, tenant, principal = self.plane, self.tenant, self.principal

        class Writer:
            def ingest(self, events):
                captured.extend(copy.deepcopy(events))
                return plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=events)

        gateway = CanonicalArchiveGateway(self.store, self.archive, tenant_id=self.tenant, principal_id=self.principal)
        collector = Collector(root=root, harness='codex', source_id=self.source,
            spool_path=root/'collector.db', endpoint='http://127.0.0.1:1', token='synthetic',
            principal_id=self.principal, visibility='shared', brain_writer=Writer(), archive=gateway,
            tenant_id=self.tenant, defer_scan_flush=True, bulk_manifest_archive=self.bulk)
        transcript = root/'rollout-2026-07-14T00-00-00-12345678-1234-1234-1234-123456789abc.jsonl'
        transcript.write_text(json.dumps({'type':'response_item','timestamp':'2026-07-14T00:00:00Z','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'Retain this exact captured transcript.'}]}})+'\n')
        self.assertEqual(collector.scan()['records_queued'],1)
        self.assertEqual(collector.flush()['acked'],1)
        event = captured[0]
        self.assertEqual(event['kind'],'transcript_record')
        self.assertEqual(validate_envelope(event),event)
        self.original_event = event
        self.original_text = canonical_json(event['content']).decode()
        self.original = {'receipt':f"recall://{self.source}/{event['native_id']}?rev=1#item=0"}
        with self.store.connect() as conn:
            conn.execute(f"UPDATE canonical_events e SET canonical_redacted={self.compact('e')},body_location='raw' WHERE tenant_id=%s AND source_id=%s AND revision=1",(self.tenant,self.source))
        try:
            artifact = collector._archive_raw(native_id=event['native_id'],
                payload=b'{"deleted":true}', occurred_at='2026-10-14T00:00:00Z')
            marker = collector._envelope(root/'rollout-2026-07-14T00-00-00-12345678-1234-1234-1234-123456789abc.jsonl', event['native_id'], 'tombstone',
                {'target_native_id': event['native_id'], 'deletion_id': 'old-filesystem-cleanup'},
                '2026-10-14T00:00:00Z', 0, 17, artifact)
            # Seed a historical pre-policy marker through the unchanged mutation
            # owner. Current collector/server policy never emits/applies it.
            with patch('recall_server.canonical._filesystem_cleanup', return_value=False):
                self.assertEqual(Writer().ingest([marker])['inserted'], 1)
        finally:
            collector.close()
        self.admin = 'principal:recovery-admin'
        self.operation_id = str(uuid.uuid4())
        with self.store.connect() as conn:
            # Match observed retained history. This is not a body-backup inference.
            conn.execute('UPDATE canonical_chunks SET text_redacted=%s WHERE tenant_id=%s AND source_id=%s',
                         (self.original_text, self.tenant, self.source))
            rows = conn.execute('SELECT event_id,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY revision', (self.tenant,self.source)).fetchall()
            doc = conn.execute('SELECT document_id,text_sha256 FROM canonical_documents WHERE tenant_id=%s AND source_id=%s', (self.tenant,self.source)).fetchone()
            self.expected = dict(source_id=self.source,native_id=event['native_id'],original_event_id=rows[0]['event_id'],marker_event_id=rows[1]['event_id'],document_id=doc['document_id'],text_sha256=doc['text_sha256'],text_bytes=len(self.original_text.encode()))
            organization = 'organization:' + uuid.uuid4().hex
            conn.execute('INSERT INTO brain_principals VALUES(%s,%s,now())',(self.tenant,self.admin))
            conn.execute("INSERT INTO brain_organizations VALUES(%s,'company','SDK recovery',now())",(organization,))
            conn.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','sdk-recovery')",(self.tenant,organization))
            conn.execute("INSERT INTO brain_memberships VALUES(%s,%s,'admin',now())",(organization,self.admin))
            conn.execute("INSERT INTO brain_access_grants VALUES(%s,%s,'admin',now())",(self.tenant,self.admin))
            conn.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'read',now())",(self.tenant,self.admin,self.source))

    test_native_codex_compacted_restore = LegacyCompactedRecovery.test_old_compacted_collector_recovers_private_new_generation
    test_same_length_body_corruption_refused = LegacyCompactedRecovery.test_same_length_body_corruption_refused
    test_unknown_compact_shape_refuses = LegacyCompactedRecovery.test_unknown_compact_shape_refuses
    test_present_mismatching_principal_still_refuses = LegacyCompactedRecovery.test_present_mismatching_principal_still_refuses

    def test_wrong_original_harness_refused(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{provenance,harness}','\"claude\"') WHERE tenant_id=%s AND revision=1", (self.tenant,))
        with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
            self.recover()
        self.assert_no_correction()

    def test_wrong_original_or_marker_job_connector_refused(self):
        for revision in (1, 2):
            with self.subTest(revision=revision):
                with self.store.connect() as conn:
                    conn.execute("UPDATE canonical_ingest_jobs j SET connector_id='claude.jsonl' FROM canonical_events e WHERE e.tenant_id=%s AND e.revision=%s AND (j.tenant_id,j.source_id,j.job_id)=(e.tenant_id,e.source_id,e.job_id)", (self.tenant, revision))
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    self.recover()
                self.assert_no_correction()
                with self.store.connect() as conn:
                    conn.execute("UPDATE canonical_ingest_jobs SET connector_id='codex.jsonl' WHERE tenant_id=%s", (self.tenant,))

    def test_parent_explicit_forget_vetoes_codex_recovery(self):
        from recall_server.projectors import canonical_json
        parent = copy.deepcopy(self.original_event)
        parent['native_id'] = parent.pop('native_parent_id')
        parent['content'] = {'type':'session_meta','payload':{'id':'synthetic-parent'}}
        parent['content_sha256'] = hashlib.sha256(canonical_json(parent['content'])).hexdigest()
        result = self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[parent])
        self.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            target_receipt=result['receipts'][0],mode='explicit_forget',reason='owner_requested',
            requested_at='2026-10-14T00:00:00Z',idempotency_key='codex-parent-forget'))
        forgotten = self.before()
        with self.assertRaisesRegex(RuntimeError,'forgotten'):
            self.recover()
        self.assertEqual(self.before(), forgotten)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s',(self.operation_id,)).fetchone())

    def test_codex_restore_keeps_native_parent_and_connector(self):
        result = self.recover()
        self.assertEqual(result['revision'],3)
        with self.store.connect() as conn:
            row = conn.execute("SELECT e.native_id,e.native_parent_id,j.connector_id FROM canonical_events e JOIN canonical_ingest_jobs j USING(tenant_id,source_id,job_id) WHERE e.tenant_id=%s AND e.source_id=%s AND e.revision=3",(self.tenant,self.source)).fetchone()
        self.assertEqual(row['native_id'],self.original_event['native_id'])
        self.assertEqual(row['native_parent_id'],self.original_event['native_parent_id'])
        self.assertEqual(row['connector_id'],'codex.jsonl')

    def test_restoration_does_not_authorize_old_receipts(self):
        from recall_server.receipt_membership import lookup_current_search_membership
        result=self.recover()
        with self.store.connect() as connection,connection.transaction():
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            members=lookup_current_search_membership(self.store,tenant_id=self.tenant,
                authorized_sources=(self.source,),receipts=(self.original['receipt'],result['receipt']),
                connection=connection,deadline_at=time.monotonic()+3)
        self.assertEqual(members,{(self.source,result['receipt'])})
        # An old receipt copied into another parent's passages remains unproved.
        self.assertNotIn((self.source,self.original['receipt']),members)

    def test_new_revision_during_archive_check_is_not_overwritten(self):
        verify=self.archive.verify_raw_bounded
        newer=copy.deepcopy(self.original_event)
        newer['content']['_recall_collector_generation']=1
        newer['content']['new_information']='ordinary concurrent publication'
        newer['content_sha256']=hashlib.sha256(canonical_json(newer['content'])).hexdigest()
        def publish(*args,**kwargs):
            verify(*args,**kwargs)
            self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[newer])
        with patch.object(self.archive,'verify_raw_bounded',publish):
            with self.assertRaisesRegex(RuntimeError,'history_changed'):
                self.recover()
        with self.store.connect() as conn:
            row=conn.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s AND revision=3',
                (self.tenant,self.source,self.expected['native_id'])).fetchone()
            self.assertEqual(row['canonical_redacted']['content']['new_information'],'ordinary concurrent publication')
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s',(self.operation_id,)).fetchone())

    def test_noncommitted_original_or_marker_job_refused(self):
        for revision in (1,2):
            with self.subTest(revision=revision):
                with self.store.connect() as conn:
                    conn.execute("UPDATE canonical_ingest_jobs j SET status='failed' FROM canonical_events e WHERE e.tenant_id=%s AND e.revision=%s AND (j.tenant_id,j.source_id,j.job_id)=(e.tenant_id,e.source_id,e.job_id)",(self.tenant,revision))
                with self.assertRaisesRegex(RuntimeError,'lineage_changed'):
                    self.recover()
                self.assert_no_correction()
                with self.store.connect() as conn:
                    conn.execute("UPDATE canonical_ingest_jobs SET status='committed' WHERE tenant_id=%s",(self.tenant,))


class WrongSourceRecovery(CodexCleanupRecovery):
    source_prefix = 'claude:linux:'

    def test_codex_connector_on_non_codex_source_refused(self):
        with self.assertRaisesRegex(RuntimeError,'marker_refused'):
            self.recover()
        self.assert_no_correction()


class CodexFullProvenanceRecovery(CodexCleanupRecovery):
    from e2e_collector_history_full_provenance import FullProvenanceRecovery as Original
    from e2e_collector_history_full_provenance import _compact_event_expression
    compact = staticmethod(_compact_event_expression)
    bulk = True
    metadata = Original.metadata
    change_metadata = Original.change_metadata
    def setUp(self):
        super().setUp()
        self.original_metadata = self.metadata()

    test_bulk_manifest_restore = Original.test_exact_historical_bulk_collector_restores_through_public_reader
    test_old_compacted_collector_recovers_private_new_generation = LegacyCompactedRecovery.test_old_compacted_collector_recovers_private_new_generation
    test_member_identity_or_normalized_hash_mismatch_refuses = Original.test_member_identity_or_normalized_hash_mismatch_refuses
    test_member_extra_fields_and_unknown_provenance_refuse = Original.test_member_extra_fields_and_unknown_provenance_refuse
    test_missing_provenance_or_member_keys_refuse = Original.test_missing_provenance_or_member_keys_refuse


if __name__ == '__main__':
    names = [
        'test_native_codex_compacted_restore',
        'test_codex_restore_keeps_native_parent_and_connector',
        'test_restoration_does_not_authorize_old_receipts',
        'test_new_revision_during_archive_check_is_not_overwritten',
        'test_parent_explicit_forget_vetoes_codex_recovery',
        'test_noncommitted_original_or_marker_job_refused',
        'test_authority_remains_locked_through_real_canonical_write',
        'test_wrong_original_harness_refused',
        'test_wrong_original_or_marker_job_connector_refused',
        'test_unknown_compact_shape_refuses',
        'test_present_mismatching_principal_still_refuses',
        'test_read_grant_without_company_admin_refused',
        'test_source_revoked_during_archive_metadata_check_refused',
        'test_explicit_forget_always_vetoes_correction',
        'test_changed_latest_marker_is_not_reinterpreted',
        'test_same_length_body_corruption_refused',
        'test_marker_payload_must_match_pinned_content_address',
        'test_competing_native_lock_refuses_without_waiting',
        'test_failure_after_canonical_write_rolls_back_event_queue_and_audit',
        'test_actual_commit_lost_ack_is_observed_without_replay',
        'test_archive_metadata_check_has_no_database_lease',
    ]
    suite = unittest.TestSuite(CodexCleanupRecovery(n) for n in names)
    suite.addTest(WrongSourceRecovery('test_codex_connector_on_non_codex_source_refused'))
    for name in ('test_bulk_manifest_restore','test_member_identity_or_normalized_hash_mismatch_refuses','test_member_extra_fields_and_unknown_provenance_refuse','test_missing_provenance_or_member_keys_refuse'):
        suite.addTest(CodexFullProvenanceRecovery(name))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
