"""Actual collector history survives the exact pre-792 event compactor."""
import copy
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import e2e_slack_deletion_ordering as fixture
from e2e_collector_history_recovery import CollectorHistoryRecovery
from collector.collector import Collector
from recall_server.canonical import CanonicalArchiveGateway
from recall_server.projectors import canonical_json, validate_envelope


# Exact pre-792 SQL oracle; never changed to match recovery implementation.
def legacy_compact(alias: str = "event") -> str:
    """Retain only routing metadata used after the full body moves to raw storage."""

    def structural(path: str) -> str:
        return f"jsonb_strip_nulls(jsonb_build_object('role',{alias}.canonical_redacted #> '{{{path},role}}','type',{alias}.canonical_redacted #> '{{{path},type}}'))"

    content_metadata = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted #> '{{content,role}}',"
        f"'type',{alias}.canonical_redacted #> '{{content,type}}',"
        f"'message',{structural('content,message')},"
        f"'payload',{structural('content,payload')}))"
    )
    oversized_pointer = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'contract',{alias}.canonical_redacted #> '{{content,contract}}',"
        f"'schema_version',{alias}.canonical_redacted #> '{{content,schema_version}}',"
        f"'full_record_available',{alias}.canonical_redacted #> "
        "'{content,full_record_available}',"
        f"'full_content_sha256',{alias}.canonical_redacted #> "
        "'{content,full_content_sha256}',"
        f"'full_size_bytes',{alias}.canonical_redacted #> "
        "'{content,full_size_bytes}',"
        f"'archive_encoding',{alias}.canonical_redacted #> "
        "'{content,archive_encoding}'))"
    )
    content = (
        f"CASE WHEN {alias}.canonical_redacted #>> '{{content,contract}}'="
        "'recall.oversized-projection.v1' "
        f"THEN {oversized_pointer} ELSE {content_metadata} END"
    )
    provenance = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'connector_id',{alias}.canonical_redacted #> '{{provenance,connector_id}}',"
        f"'connector_schema_version',{alias}.canonical_redacted #> "
        "'{provenance,connector_schema_version}',"
        f"'collector_version',{alias}.canonical_redacted #> "
        "'{provenance,collector_version}',"
        f"'privacy_policy_version',{alias}.canonical_redacted #> "
        "'{provenance,privacy_policy_version}',"
        f"'harness',{alias}.canonical_redacted #> '{{provenance,harness}}',"
        f"'cwd',{alias}.canonical_redacted #> '{{provenance,cwd}}',"
        f"'branch',{alias}.canonical_redacted #> '{{provenance,branch}}',"
        f"'slot',{alias}.canonical_redacted #> '{{provenance,slot}}',"
        f"'byte_start',{alias}.canonical_redacted #> '{{provenance,byte_start}}',"
        f"'byte_end',{alias}.canonical_redacted #> '{{provenance,byte_end}}'))"
    )
    return (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted->'role',"
        f"'type',{alias}.canonical_redacted->'type',"
        f"'provenance',{provenance},"
        f"'content',{content},"
        f"'message',{structural('message')},"
        f"'payload',{structural('payload')}))"
    )

class LegacyCompactedRecovery(CollectorHistoryRecovery):
    def setUp(self):
        fixture.SlackDeletionOrdering.setUp(self)
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
        collector = Collector(root=root, harness='claude', source_id=self.source,
            spool_path=root/'collector.db', endpoint='http://127.0.0.1:1', token='synthetic',
            principal_id=self.principal, visibility='shared', brain_writer=Writer(), archive=gateway,
            tenant_id=self.tenant, defer_scan_flush=True)
        transcript = root/'session.jsonl'
        transcript.write_text(json.dumps({'type':'user','timestamp':'2026-07-14T00:00:00Z','message':{'content':'Retain this exact captured transcript.'}})+'\n')
        self.assertEqual(collector.scan()['records_queued'],1)
        self.assertEqual(collector.flush()['acked'],1)
        event = captured[0]
        self.assertEqual(event['kind'],'transcript_record')
        self.assertEqual(validate_envelope(event),event)
        self.original_event = event
        self.original_text = canonical_json(event['content']).decode()
        self.original = {'receipt':f"recall://{self.source}/{event['native_id']}?rev=1#item=0"}
        with self.store.connect() as conn:
            conn.execute(f"UPDATE canonical_events e SET canonical_redacted={legacy_compact('e')},body_location='raw' WHERE tenant_id=%s AND source_id=%s AND revision=1",(self.tenant,self.source))
        try:
            artifact = collector._archive_raw(native_id=event['native_id'],
                payload=b'{"deleted":true}', occurred_at='2026-10-14T00:00:00Z')
            marker = collector._envelope(root/'session.jsonl', event['native_id'], 'tombstone',
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

    def test_old_compacted_collector_recovers_private_new_generation(self):
        before = self.before()
        result = self.recover()
        self.assertEqual(result['revision'],3)
        self.assertEqual(self.before(),before)
        resolved = self.store.resolve(result['receipt'],tenant_id=self.tenant,authorized_sources=(self.source,),chunk_body_archive=self.archive)
        expected = {**self.original_event['content'],'_recall_collector_generation':1}
        self.assertEqual(json.loads(resolved['items'][0]['text_redacted']),expected)
        with self.store.connect() as conn:
            event=conn.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND revision=3',(self.tenant,self.source)).fetchone()['canonical_redacted']
        self.assertEqual(event['principal_id'],self.principal)
        self.assertEqual(event['visibility'],'private')
        self.assertEqual(self.original_event['visibility'],'shared')
        self.assertEqual(validate_envelope(event),event)

    def test_present_mismatching_principal_still_refuses(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_events SET canonical_redacted=canonical_redacted||%s::jsonb WHERE tenant_id=%s AND revision=1",(json.dumps({'principal_id':'different-owner'}),self.tenant))
        with self.assertRaisesRegex(RuntimeError,'lineage_changed'):
            self.recover()
        self.assert_no_correction()

    def test_unknown_compact_shape_refuses(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_events SET canonical_redacted=canonical_redacted||%s::jsonb WHERE tenant_id=%s AND revision=1",(json.dumps({'unrecognized_metadata':True}),self.tenant))
        with self.assertRaisesRegex(RuntimeError,'lineage_changed'):
            self.recover()
        self.assert_no_correction()

    def test_partially_missing_or_null_attribution_is_not_legacy_shape(self):
        with self.store.connect() as conn:
            original=conn.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND revision=1',(self.tenant,)).fetchone()['canonical_redacted']
        for extra in ({'principal_id':None},{'visibility':'shared'},{'content_type':'application/json'}):
            with self.store.connect() as conn:
                conn.execute('UPDATE canonical_events SET canonical_redacted=%s::jsonb WHERE tenant_id=%s AND revision=1',(json.dumps({**original,**extra}),self.tenant))
            with self.assertRaisesRegex(RuntimeError,'lineage_changed'):
                self.recover()
            self.assert_no_correction()

    test_read_grant_without_company_admin_refused = CollectorHistoryRecovery.test_read_grant_without_company_admin_refused
    test_source_revoked_during_archive_metadata_check_refused = CollectorHistoryRecovery.test_source_revoked_during_archive_metadata_check_refused
    def test_same_length_body_corruption_refused(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_chunks SET text_redacted=replace(text_redacted,'Retain','RETAIN') WHERE tenant_id=%s AND source_id=%s",(self.tenant,self.source))
        with self.assertRaisesRegex(ValueError,'source_integrity_invalid'):
            self.recover()
        self.assert_no_correction()

    test_competing_native_lock_refuses_without_waiting = CollectorHistoryRecovery.test_competing_native_lock_refuses_without_waiting
    test_failure_after_canonical_write_rolls_back_event_queue_and_audit = CollectorHistoryRecovery.test_failure_after_canonical_write_rolls_back_event_queue_and_audit
    test_actual_commit_lost_ack_is_observed_without_replay = CollectorHistoryRecovery.test_actual_commit_lost_ack_is_observed_without_replay
    test_explicit_forget_always_vetoes_correction = CollectorHistoryRecovery.test_explicit_forget_always_vetoes_correction

if __name__ == '__main__':
    names = [n for n in LegacyCompactedRecovery.__dict__ if n.startswith('test_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(LegacyCompactedRecovery(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
