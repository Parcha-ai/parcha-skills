"""Explicit company-admin correction of a sealed obsolete collector marker."""
import hashlib
import copy
import json
import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import psycopg

import e2e_slack_deletion_ordering as fixture
from recall_server.projectors import canonical_json
from recall_server.chunk_retirement import ChunkRetirementError


class CollectorHistoryRecovery(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    ingest = fixture.SlackDeletionOrdering.ingest
    delete = fixture.SlackDeletionOrdering.delete

    def setUp(self):
        fixture.SlackDeletionOrdering.setUp(self)
        self.root['text'] = 'Captured history must survive local cleanup. α'
        self.prepared = self.ingest(self.root, path='prepare', connector_id='claude.jsonl')
        event = self.prepared['envelope']
        event['kind'] = 'transcript_record'
        event['native_parent_id'] = 'parent:recovery'
        event['provenance']['connector_schema_version'] = 1
        event['content']['_recall_collector_generation'] = 0
        self.original_text = canonical_json(event['content']).decode()
        event['content_sha256'] = hashlib.sha256(self.original_text.encode()).hexdigest()
        self.prepared['text_redacted'] = self.original_text
        self.original = self.plane.ingest_document(**self.prepared)
        marker = self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')
        marker['envelope']['content']['deletion_id'] = 'legacy-filesystem-cleanup'
        marker['envelope']['content_sha256'] = hashlib.sha256(canonical_json(marker['envelope']['content'])).hexdigest()
        with patch('recall_server.canonical._filesystem_cleanup', return_value=False):
            self.plane.ingest_document(**marker)
        self.admin = 'principal:recovery-admin'
        self.operation_id = str(uuid.uuid4())
        with self.store.connect() as conn:
            # Reproduce the observed pre-policy retained historical chunk, not a
            # current collector deletion or an archive restoration assumption.
            conn.execute('UPDATE canonical_chunks SET text_redacted=%s WHERE tenant_id=%s AND source_id=%s',
                         (self.original_text, self.tenant, self.source))
            rows = conn.execute('SELECT event_id,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY revision',
                                (self.tenant, self.source)).fetchall()
            document = conn.execute('SELECT document_id,text_sha256 FROM canonical_documents WHERE tenant_id=%s AND source_id=%s',
                                    (self.tenant, self.source)).fetchone()
            self.expected = dict(source_id=self.source, native_id=event['native_id'],
                original_event_id=rows[0]['event_id'], marker_event_id=rows[1]['event_id'],
                document_id=document['document_id'], text_sha256=document['text_sha256'],
                text_bytes=len(self.original_text.encode()))
            organization = 'organization:' + uuid.uuid4().hex
            conn.execute('INSERT INTO brain_principals VALUES(%s,%s,now())', (self.tenant, self.admin))
            conn.execute("INSERT INTO brain_organizations VALUES(%s,'company','Recovery test',now())", (organization,))
            conn.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','recovery')", (self.tenant, organization))
            conn.execute("INSERT INTO brain_memberships VALUES(%s,%s,'admin',now())", (organization, self.admin))
            conn.execute("INSERT INTO brain_access_grants VALUES(%s,%s,'admin',now())", (self.tenant, self.admin))
            conn.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'read',now())", (self.tenant, self.admin, self.source))

    def before(self):
        with self.store.connect() as conn:
            return dict(
                events=conn.execute('SELECT to_jsonb(e) AS row FROM canonical_events e WHERE tenant_id=%s AND source_id=%s AND revision<=2 ORDER BY revision', (self.tenant, self.source)).fetchall(),
                source=conn.execute('SELECT * FROM canonical_sources WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone(),
                grants=conn.execute('SELECT * FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s ORDER BY principal_id', (self.tenant, self.source)).fetchall())

    def recover(self):
        from recall_server.collector_history_recovery import recover_collector_history
        return recover_collector_history(self.plane, tenant_id=self.tenant,
            principal_id=self.admin, expected=self.expected, operation_id=self.operation_id)

    def test_admin_recovers_new_generation_with_original_attribution_and_audit(self):
        before = self.before()
        result = self.recover()
        self.assertEqual(result['status'], 'committed')
        self.assertEqual(result['revision'], 3)
        self.assertEqual(result['receipt'], self.original['receipt'].replace('?rev=1', '?rev=3'))
        self.assertEqual(self.before(), before)
        resolved = self.store.resolve(result['receipt'], tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)
        self.assertIsNotNone(resolved)
        with self.store.connect() as conn:
            row = conn.execute('SELECT e.canonical_redacted,c.text_redacted FROM canonical_events e JOIN canonical_documents d USING(tenant_id,source_id,event_id) JOIN canonical_chunks c USING(tenant_id,source_id,document_id) WHERE e.tenant_id=%s AND e.source_id=%s AND e.revision=3', (self.tenant, self.source)).fetchone()
            audit = conn.execute('SELECT principal_id,operation,status,target_sha256 FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone()
        expected_content = json.loads(self.original_text)
        expected_content['_recall_collector_generation'] = 1
        self.assertEqual(json.loads(row['text_redacted']), expected_content)
        self.assertEqual(json.loads(resolved['items'][0]['text_redacted']), expected_content)
        self.assertEqual(row['canonical_redacted']['principal_id'], self.principal)
        self.assertEqual(audit['principal_id'], self.admin)
        self.assertEqual(audit['operation'], 'collector.history.recover')
        self.assertEqual(audit['status'], 'success')
        self.assertEqual(len(audit['target_sha256']), 64)

    def assert_no_correction(self):
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT max(revision) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['n'], 2)
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone())

    def test_ordinary_ingest_still_refuses_admin_impersonation(self):
        from recall_server.canonical import CanonicalLifecycleError
        with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_lineage_invalid'):
            self.plane.ingest_document(**{**self.prepared, 'principal_id': self.admin})
        self.assert_no_correction()

    def test_read_grant_without_company_admin_refused(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE brain_access_grants SET permission='read' WHERE tenant_id=%s AND principal_id=%s", (self.tenant, self.admin))
        with self.assertRaises(ChunkRetirementError):
            self.recover()
        self.assert_no_correction()

    def test_source_revoked_during_archive_metadata_check_refused(self):
        verify = self.archive.verify_raw_bounded
        def revoke(*args, **kwargs):
            verify(*args, **kwargs)
            with self.store.connect() as conn:
                conn.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s', (self.tenant, self.source, self.admin))
        with patch.object(self.archive, 'verify_raw_bounded', revoke):
            with self.assertRaises(ChunkRetirementError):
                self.recover()
        self.assert_no_correction()

    def test_explicit_forget_always_vetoes_correction(self):
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=self.original['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-14T00:00:00Z', idempotency_key='recovery-forget'))
        forgotten = self.before()
        with self.assertRaisesRegex(RuntimeError, 'forgotten'):
            self.recover()
        self.assertEqual(self.before(), forgotten)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone())

    def test_changed_latest_marker_is_not_reinterpreted(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{provenance,connector_id}','\"slack.messages\"') WHERE tenant_id=%s AND source_id=%s AND revision=2", (self.tenant, self.source))
        with self.assertRaisesRegex(RuntimeError, 'marker_refused'):
            self.recover()
        self.assert_no_correction()

    def test_same_length_body_corruption_refused(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_chunks SET text_redacted=replace(text_redacted,'Captured','CAPTURED') WHERE tenant_id=%s AND source_id=%s", (self.tenant, self.source))
        with self.assertRaisesRegex(ValueError, 'source_integrity_invalid'):
            self.recover()
        self.assert_no_correction()

    def test_marker_payload_must_match_pinned_content_address(self):
        with self.store.connect() as conn:
            conn.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{content,deletion_id}','\"different-deletion\"') WHERE tenant_id=%s AND source_id=%s AND revision=2", (self.tenant, self.source))
        with self.assertRaisesRegex(RuntimeError, 'marker_refused'):
            self.recover()
        self.assert_no_correction()

    def test_competing_native_lock_refuses_without_waiting(self):
        with self.store.connect() as conn, conn.transaction():
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                (f'v2\x1f{self.tenant}\x1f{self.source}\x1f{self.expected["native_id"]}',))
            with self.assertRaisesRegex(RuntimeError, 'native_busy'):
                self.recover()
        self.assert_no_correction()

    def test_failure_after_canonical_write_rolls_back_event_queue_and_audit(self):
        write = self.plane._write_authorized_document
        before = self.before()
        def fail(*args, **kwargs):
            write(*args, **kwargs)
            raise RuntimeError('synthetic failure after real write')
        with patch.object(self.plane, '_write_authorized_document', fail):
            with self.assertRaisesRegex(RuntimeError, 'synthetic failure'):
                self.recover()
        self.assertEqual(self.before(), before)
        self.assert_no_correction()

    def test_actual_commit_lost_ack_is_observed_without_replay(self):
        from recall_server import collector_history_recovery as owner
        write, transaction = self.plane._write_authorized_document, psycopg.Connection.transaction
        armed = False
        def record(*args, **kwargs):
            nonlocal armed
            result = write(*args, **kwargs)
            armed = True
            return result
        @contextmanager
        def lost_ack(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                raise psycopg.OperationalError('synthetic acknowledgment lost')
        with patch.object(self.plane, '_write_authorized_document', record), patch.object(psycopg.Connection, 'transaction', lost_ack):
            with self.assertRaises(psycopg.OperationalError):
                self.recover()
        observed = owner.inspect_collector_history_recovery(self.store, tenant_id=self.tenant,
            principal_id=self.admin, expected=self.expected, operation_id=self.operation_id)
        self.assertEqual(observed['status'], 'recorded_applied')
        self.assertEqual(observed['original_commit_outcome'], 'unknown')
        self.assertFalse(observed['mutation_replayed'])
        with self.assertRaisesRegex(RuntimeError, 'history_changed'):
            self.recover()

    def test_new_revision_during_archive_check_is_not_overwritten(self):
        verify = self.archive.verify_raw_bounded
        newer = copy.deepcopy(self.prepared)
        newer['envelope']['content']['new_information'] = 'ordinary concurrent publication'
        newer['envelope']['content']['_recall_collector_generation'] = 1
        newer['text_redacted'] = canonical_json(newer['envelope']['content']).decode()
        newer['envelope']['content_sha256'] = hashlib.sha256(newer['text_redacted'].encode()).hexdigest()
        def publish(*args, **kwargs):
            verify(*args, **kwargs)
            self.plane.ingest_document(**newer)
        with patch.object(self.archive, 'verify_raw_bounded', publish):
            with self.assertRaisesRegex(RuntimeError, 'history_changed'):
                self.recover()
        with self.store.connect() as conn:
            self.assertEqual(conn.execute('SELECT max(revision) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['n'], 3)
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone())

    def test_archive_metadata_check_has_no_database_lease(self):
        connect, verify = self.store.connect, self.archive.verify_raw_bounded
        active = 0
        @contextmanager
        def tracked(*args, **kwargs):
            nonlocal active
            with connect(*args, **kwargs) as conn:
                active += 1
                try:
                    yield conn
                finally:
                    active -= 1
        def checked(*args, **kwargs):
            self.assertEqual(active, 0)
            return verify(*args, **kwargs)
        with patch.object(self.store, 'connect', tracked), patch.object(self.archive, 'verify_raw_bounded', checked):
            self.assertEqual(self.recover()['revision'], 3)

    def test_authority_remains_locked_through_real_canonical_write(self):
        write = self.plane._write_authorized_document
        checked = False
        def check(*args, **kwargs):
            nonlocal checked
            with self.store.connect() as conn:
                conn.execute("SET LOCAL lock_timeout='20ms'")
                with self.assertRaises(psycopg.errors.LockNotAvailable):
                    conn.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s', (self.tenant, self.source, self.admin))
                conn.rollback()
            checked = True
            return write(*args, **kwargs)
        with patch.object(self.plane, '_write_authorized_document', check):
            self.assertEqual(self.recover()['revision'], 3)
        self.assertTrue(checked)

    def test_parent_explicit_forget_vetoes_even_when_record_is_not_target(self):
        parent = copy.deepcopy(self.prepared)
        parent['envelope']['native_id'] = 'parent:recovery'
        parent['envelope'].pop('native_parent_id')
        parent['envelope']['content'] = {'text': 'parent identity'}
        parent['text_redacted'] = canonical_json(parent['envelope']['content']).decode()
        parent['envelope']['content_sha256'] = hashlib.sha256(parent['text_redacted'].encode()).hexdigest()
        target = self.plane.ingest_document(**parent)
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=target['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-14T00:00:00Z', idempotency_key='recovery-parent-forget'))
        before = self.before()
        with self.assertRaises(RuntimeError):
            self.recover()
        self.assertEqual(self.before(), before)
        with self.store.connect() as conn:
            self.assertIsNone(conn.execute('SELECT 1 FROM control_audit_events WHERE id=%s', (self.operation_id,)).fetchone())


if __name__ == '__main__':
    unittest.main()
