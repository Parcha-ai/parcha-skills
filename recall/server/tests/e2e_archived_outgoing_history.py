#!/usr/bin/env python3
"""Retired outgoing revisions remain archived across ordinary edits and cleanup."""
import os
import hashlib
import unittest
from contextlib import contextmanager
from unittest.mock import patch

import psycopg

from e2e_singleton_metadata_admission import SingletonMetadataAdmission
from recall_server.chunk_retirement import retire_current_chunks
from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore


class ArchivedOutgoingHistory(SingletonMetadataAdmission):
    def setUp(self):
        super().setUp()
        # Same revision-specific forget owner as the production serving app.
        self.plane.evidence_projector = CanonicalEvidenceProjector(
            self.store, EvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant)

    def resolve_archive(self, receipt):
        return self.store.resolve(receipt, tenant_id=self.tenant,
            authorized_sources=(self.source,), chunk_body_archive=self.archive)

    def prepare_retired(self, *, singleton=False, native='turn/archived-history'):
        text = 'Original retained history α 🧠 ' * (1 if singleton else 2000)
        first = self.ingest(native, text)
        original = self.baseline(first['receipt'])
        before = self.resolve_archive(first['receipt'])
        self.assertEqual(len(before['items']) == 1, singleton)
        projected = self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        self.assertEqual(projected['failed'], 0)
        scope = dict(tenant_id=self.tenant, source_id=self.source,
            document_ids=(original['document_id'],))
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            plan = retire_current_chunks(self.store, self.archive, **scope)
            retired = retire_current_chunks(self.store, self.archive, **scope,
                apply=True, reviewed_plan=plan['plan'])
        self.assertEqual(retired['status'], 'applied')
        if singleton:
            preview = self.admit(original)
            self.assertEqual(self.admit(original, apply=True,
                reviewed_plan=preview['plan'])['retired_chunks'], 1)
        return native, first, original, before

    def assert_archived_edit(self, *, singleton):
        native, first, original, before = self.prepare_retired(singleton=singleton)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            second = self.ingest(native, 'Second revision, independently retained β')
            current = self.resolve_archive(second['receipt'])
            projected = self.projector.project_pending(tenant_id=self.tenant,
                batch_size=10, max_batches=1, upload_concurrency=1)
            self.assertEqual(projected['failed'], 0)
            self.projector.drain_cleanup(tenant_id=self.tenant)
            self.assertEqual(self.resolve_archive(first['receipt']), before)
            self.assertEqual(self.resolve_archive(second['receipt']), current)
        with self.store.connect() as connection:
            state = connection.execute('''SELECT count(*) AS chunks,
                sum(octet_length(text_redacted)) AS body_bytes
                FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND document_id=%s''',
                (self.tenant, self.source, original['document_id'])).fetchone()
        self.assertEqual(state['chunks'], len(before['items']))
        self.assertEqual(state['body_bytes'], 0,
            'an archived outgoing revision was rehydrated into PostgreSQL')
        return native, first, second, before

    def test_archived_singleton_edit_retains_exact_receipts_without_sql_body(self):
        self.assert_archived_edit(singleton=True)

    def test_archived_multichunk_edit_retains_exact_receipts_without_sql_body(self):
        self.assert_archived_edit(singleton=False)

    def test_upload_failure_refuses_edit_without_changing_original_revision(self):
        from recall_server.canonical import CanonicalLifecycleError
        native, first, original, before = self.prepare_retired()
        with patch.object(self.plane.evidence_projector.projection, 'put',
                side_effect=OSError('synthetic upload failure')):
            with self.assertRaises(CanonicalLifecycleError) as error:
                self.ingest(native, 'Must not acknowledge a failed upload')
        self.assertEqual(error.exception.error_code, 'canonical_history_unavailable')
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_deleted_upload_before_admission_cannot_strand_history(self):
        native, first, original, before = self.prepare_retired()
        put = self.plane.evidence_projector.projection.put
        removed = []
        def put_then_delete(**kwargs):
            reference = put(**kwargs)
            self.assertTrue(self.archive.delete_raw(reference))
            removed.append(reference)
            return reference
        with patch.object(self.plane.evidence_projector.projection, 'put', put_then_delete):
            with self.assertRaises(Exception):
                self.ingest(native, 'Deleted bundle must refuse admission')
        self.assertEqual(len(removed), 1)
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_actual_commit_lost_ack_preserves_bundle_through_cleanup(self):
        native, first, original, before = self.prepare_retired()
        admit = self.plane.evidence_projector.admit_outgoing
        transaction = psycopg.Connection.transaction
        armed = False
        def admitted(*args, **kwargs):
            nonlocal armed
            result = admit(*args, **kwargs)
            armed = True
            return result
        @contextmanager
        def lost_ack(connection, *args, **kwargs):
            nonlocal armed
            with transaction(connection, *args, **kwargs) as value:
                yield value
            if armed:
                armed = False
                raise psycopg.OperationalError('synthetic lost commit acknowledgement')
        with patch.object(self.plane.evidence_projector, 'admit_outgoing', admitted), \
                patch.object(psycopg.Connection, 'transaction', lost_ack), \
                patch.object(self.plane.evidence_projector, '_schedule_cleanup',
                    side_effect=RuntimeError('synthetic cleanup scheduling failure')):
            with self.assertRaisesRegex(psycopg.OperationalError, 'lost commit acknowledgement') as error:
                self.ingest(native, 'Committed revision with a lost acknowledgement')
        self.assertIn('outgoing history cleanup scheduling failed', error.exception.__notes__)
        self.projector.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        self.assertIsNotNone(self.resolve_archive(first['receipt'].replace('?rev=1', '?rev=2')))
        with self.store.connect() as connection:
            row = connection.execute('''SELECT sum(octet_length(text_redacted)) AS n
                FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s''',
                (self.tenant, original['document_id'])).fetchone()
        self.assertEqual(row['n'], 0)

    def assert_replay_cleanup_is_durable(self, *, fallback):
        from recall_server.archive import ArchiveNotFound
        native, first, original, before = self.prepare_retired()
        projector = self.plane.evidence_projector
        put = projector.projection.put
        nested, uploaded = [], []
        text = 'Concurrent identical edit committed before outer writer admission'
        def upload_then_commit(**kwargs):
            reference = put(**kwargs)
            uploaded.append(reference)
            with patch.object(projector.projection, 'put', put), \
                    patch.object(self.plane, 'evidence_projector', self.projector if fallback else projector):
                nested.append(self.ingest(native, text))
            return reference
        def fail_unused(references):
            if references:
                raise psycopg.OperationalError('synthetic cleanup queue unavailable')
        with patch.object(projector.projection, 'put', upload_then_commit), \
                patch.object(projector, '_schedule_cleanup', fail_unused):
            replay = self.ingest(native, text)
        self.assertTrue(replay['replay'])
        self.assertEqual(replay['receipt'], nested[0]['receipt'])
        reference = uploaded[0]
        with self.store.connect() as connection:
            queued = connection.execute('''SELECT count(*) AS n FROM canonical_evidence_cleanup_queue
                WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s''',
                (self.tenant, self.source, reference['artifact_id'])).fetchone()
        self.assertEqual(queued['n'], 1, 'successful replay must retain durable cleanup intent')
        self.projector.drain_cleanup(tenant_id=self.tenant)
        if fallback:
            with self.assertRaises(ArchiveNotFound):
                self.archive.read_raw(reference)
        else:
            self.assertTrue(self.archive.read_raw(reference))
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        self.assertIsNotNone(self.resolve_archive(replay['receipt']))

    def test_archive_writer_replay_commits_cleanup_intent_and_preserves_owned_bundle(self):
        self.assert_replay_cleanup_is_durable(fallback=False)

    def test_fallback_writer_replay_commits_cleanup_intent_and_removes_unused_bundle(self):
        self.assert_replay_cleanup_is_durable(fallback=True)

    def test_cleanup_intent_failure_rolls_back_edit_and_original_error_survives(self):
        native, first, original, before = self.prepare_retired()
        with self.store.connect() as connection:
            connection.execute('''CREATE FUNCTION reject_history_cleanup_intent() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic cleanup intent failure'; END $$''')
            connection.execute('''CREATE TRIGGER reject_history_cleanup_intent
                BEFORE INSERT ON canonical_evidence_cleanup_queue FOR EACH ROW
                EXECUTE FUNCTION reject_history_cleanup_intent()''')
        try:
            with self.assertRaisesRegex(psycopg.errors.RaiseException, 'cleanup intent failure') as error:
                self.ingest(native, 'Edit requires durable cleanup intent')
            self.assertIn('outgoing history cleanup scheduling failed', error.exception.__notes__)
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_history_cleanup_intent ON canonical_evidence_cleanup_queue')
                connection.execute('DROP FUNCTION reject_history_cleanup_intent()')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        second = self.ingest(native, 'Edit requires durable cleanup intent')
        self.projector.drain_cleanup(tenant_id=self.tenant)
        self.assertIsNotNone(self.resolve_archive(second['receipt']))
        self.assertEqual(self.resolve_archive(first['receipt']), before)

    def test_body_reads_and_uploads_do_not_hold_sql_connections(self):
        native, first, original, before = self.prepare_retired()
        connect, read, put = self.store.connect, self.archive.read_raw, self.archive.put_raw
        active, observed = 0, []
        @contextmanager
        def tracked_connect():
            nonlocal active
            with connect() as connection:
                active += 1
                try:
                    yield connection
                finally:
                    active -= 1
        def tracked_read(reference):
            self.assertEqual(active, 0, 'body read held a SQL connection')
            observed.append('read')
            return read(reference)
        def tracked_put(**kwargs):
            if kwargs['media_type'] == 'application/vnd.recall.evidence+json':
                self.assertEqual(active, 0, 'evidence upload held a SQL connection')
                observed.append('put')
            return put(**kwargs)
        with patch.object(self.store, 'connect', tracked_connect), \
                patch.object(self.archive, 'read_raw', tracked_read), \
                patch.object(self.archive, 'put_raw', tracked_put):
            self.ingest(native, 'Edit with bounded staging outside SQL')
        self.assertIn('read', observed)
        self.assertIn('put', observed)
        self.assertEqual(self.resolve_archive(first['receipt']), before)

    def test_original_default_fallback_without_revision_archive_owner(self):
        native, first, original, before = self.prepare_retired()
        self.plane.evidence_projector = self.projector  # Existing logical-only constructor.
        self.ingest(native, 'Edit through original fallback owner')
        self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        self.projector.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        with self.store.connect() as connection:
            self.assertGreater(connection.execute('''SELECT sum(octet_length(text_redacted)) AS n
                FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s''',
                (self.tenant, original['document_id'])).fetchone()['n'], 0)

    def test_forget_during_staging_never_publishes_or_restores_history(self):
        native, first, original, before = self.prepare_retired()
        put = self.plane.evidence_projector.projection.put
        def forget_after_upload(**kwargs):
            reference = put(**kwargs)
            self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
                requested_at='2026-10-15T00:00:00Z', idempotency_key='staged-history-forget'))
            return reference
        with patch.object(self.plane.evidence_projector.projection, 'put', forget_after_upload):
            with self.assertRaises(Exception):
                self.ingest(native, 'Must not resurrect a forgotten revision')
        self.assertIsNone(self.resolve_archive(first['receipt']))
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_failed_new_document_insert_rolls_back_history_admission(self):
        native, first, original, before = self.prepare_retired()
        with self.store.connect() as connection:
            connection.execute('''CREATE FUNCTION reject_outgoing_history_revision() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                    IF NEW.revision=2 THEN RAISE EXCEPTION 'synthetic document failure'; END IF;
                    RETURN NEW;
                END $$''')
            connection.execute('''CREATE TRIGGER reject_outgoing_history_revision
                BEFORE INSERT ON canonical_documents FOR EACH ROW
                EXECUTE FUNCTION reject_outgoing_history_revision()''')
        try:
            with self.assertRaises(psycopg.errors.RaiseException):
                self.ingest(native, 'The whole edit must roll back')
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_outgoing_history_revision ON canonical_documents')
                connection.execute('DROP FUNCTION reject_outgoing_history_revision()')
        self.projector.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.resolve_archive(first['receipt']), before)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)

    def test_existing_revision_bundle_is_reused_and_cannot_be_relabelled(self):
        from recall_server.canonical import CanonicalLifecycleError
        written = self.ingest('turn/preexisting', 'Exact pre-existing historical bundle')
        self.plane.evidence_projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        original = self.baseline(written['receipt'])
        before = self.resolve_archive(written['receipt'])
        self.projector.project_pending(tenant_id=self.tenant,
            batch_size=10, max_batches=1, upload_concurrency=1)
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            scope = dict(tenant_id=self.tenant, source_id=self.source,
                document_ids=(original['document_id'],))
            plan = retire_current_chunks(self.store, self.archive, **scope)
            retire_current_chunks(self.store, self.archive, **scope, apply=True, reviewed_plan=plan['plan'])
        with self.store.connect() as connection:
            reference = connection.execute('SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()
            connection.execute('UPDATE canonical_evidence_objects SET revision=77 WHERE tenant_id=%s', (self.tenant,))
        with self.assertRaises((CanonicalLifecycleError, ValueError)):
            self.ingest('turn/preexisting', 'Must reject a relabelled historical catalog')
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_objects SET revision=1 WHERE tenant_id=%s', (self.tenant,))
        self.ingest('turn/preexisting', 'Second revision with a proven existing bundle')
        self.assertEqual(self.resolve_archive(written['receipt']), before)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone(), reference)

    def test_revoked_owner_grant_during_staging_cannot_be_recreated(self):
        from recall_server.canonical import REGISTRATION_CACHE, CanonicalLifecycleError
        native, first, original, before = self.prepare_retired()
        put = self.plane.evidence_projector.projection.put
        def revoke_after_upload(**kwargs):
            reference = put(**kwargs)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.source))
            REGISTRATION_CACHE.invalidate_tenant(self.tenant)
            return reference
        with patch.object(self.plane.evidence_projector.projection, 'put', revoke_after_upload):
            with self.assertRaises(CanonicalLifecycleError):
                self.ingest(native, 'Revoked owner must not recover write authority')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_source_grants WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)

    def test_pending_parent_forget_after_staging_refuses_history_publication(self):
        from recall_server.canonical import CanonicalLifecycleError
        native, first, original, before = self.prepare_retired()
        put = self.plane.evidence_projector.projection.put
        def forget_parent_after_upload(**kwargs):
            reference = put(**kwargs)
            with self.store.connect() as connection:
                connection.execute('''INSERT INTO forget_tombstones(
                    tenant_id,source_id,target_identity_sha256,mode,reason,deleted_at,status)
                    VALUES (%s,%s,%s,'explicit_forget','owner_requested',now(),'deleting')''',
                    (self.tenant, self.source, hashlib.sha256(
                        '\x1f'.join((self.tenant, self.source, 'session')).encode()).hexdigest()))
            return reference
        with patch.object(self.plane.evidence_projector.projection, 'put', forget_parent_after_upload):
            with self.assertRaises(CanonicalLifecycleError):
                self.ingest(native, 'Pending parent forget must block new historical ownership')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)
            self.assertEqual(connection.execute('SELECT max(revision) AS revision FROM canonical_events WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['revision'], 1)

    def test_ownerless_forget_revokes_reads_but_requires_wired_resume_for_archive(self):
        from recall_server.canonical import CanonicalPlane, CanonicalLifecycleError
        from recall_server.archive import ArchiveNotFound
        native, first, second, before = self.assert_archived_edit(singleton=True)
        with self.store.connect() as connection:
            reference = self.plane.evidence_projector._reference(connection.execute(
                'SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s', (self.tenant,)).fetchone())
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-15T00:00:00Z', idempotency_key='ownerless-history-forget')
        ownerless = CanonicalPlane(self.store, self.archive)
        with self.assertRaises(CanonicalLifecycleError) as error:
            ownerless.forget(request)
        self.assertEqual(error.exception.error_code, 'archive_delete_failed')
        for receipt in (first['receipt'], second['receipt']):
            result = self.resolve_archive(receipt)
            self.assertTrue(result is None or result['items'] == [],
                'partial forget must revoke all readable body items')
        self.assertTrue(self.archive.read_raw(reference))
        self.plane.forget(request)
        with self.assertRaises(ArchiveNotFound):
            self.archive.read_raw(reference)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_evidence_objects WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['n'], 0)

    def test_managed_ingest_stays_fallback_but_its_forget_removes_serving_history(self):
        import tempfile
        from pathlib import Path
        from recall_server.control import SecretBox
        from recall_server.managed_worker import ManagedConnectorWorker
        from recall_server.archive import ArchiveNotFound
        native, first, second, before = self.assert_archived_edit(singleton=True)
        with self.store.connect() as connection:
            reference = self.plane.evidence_projector._reference(connection.execute(
                'SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s', (self.tenant,)).fetchone())
        with tempfile.TemporaryDirectory() as state:
            worker = ManagedConnectorWorker(self.store, self.archive, SecretBox(b'h' * 32),
                state_root=Path(state), chunk_body_archive=self.archive)
            self.assertIsNone(worker.plane.evidence_projector)
            retrieval = worker.retrieval.bind(dict(credential_kind='mcp', principal_kind='workload',
                role='owner', audience='recall-mcp', scopes=['read'], tenant_id=self.tenant,
                principal_id=self.principal, authorized_sources=[self.source]))
            retrieval.forget(first['receipt'])
        self.assertIsNone(self.resolve_archive(first['receipt']))
        with self.assertRaises(ArchiveNotFound):
            self.archive.read_raw(reference)

    def write_batch_revision(self, native, text, path):
        from recall_server.projectors import canonical_json
        from recall_server.legacy_plane import LegacyIngestBridge
        content = dict(text=text, role='user')
        payload = canonical_json(content)
        reference = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=native, payload=payload, media_type='application/json', created_at='2026-10-14T00:00:00Z')
        event = dict(schema_version=1, source_id=self.source, native_id=native, native_parent_id='session',
            kind='connector_record', occurred_at='2026-10-14T00:00:00Z', observed_at='2026-10-14T00:00:00Z',
            principal_id=self.principal, visibility='private', content_type='application/json', content=content,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            provenance=dict(connector_id='synthetic.membership', artifact_ref=reference))
        if path == 'legacy':
            return LegacyIngestBridge(self.store, self.plane, self.archive)._ingest_canonical(
                [event], principal={'tenant_id': self.tenant, 'principal_id': self.principal},
                raw_payload=None, media_type='application/json')
        return self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
            events=[event, event] if path == 'mixed' else [event])

    def test_set_mixed_and_legacy_writers_preserve_archived_history(self):
        for path in ('set', 'mixed', 'legacy'):
            with self.subTest(path=path):
                native, first, original, before = self.prepare_retired(native='turn/' + path)
                self.write_batch_revision(native, 'Changed through ' + path, path)
                projected = self.projector.project_pending(tenant_id=self.tenant,
                    batch_size=10, max_batches=1, upload_concurrency=1)
                self.assertEqual(projected['failed'], 0)
                self.projector.drain_cleanup(tenant_id=self.tenant)
                self.assertEqual(self.resolve_archive(first['receipt']), before)
                self.assertIsNotNone(self.resolve_archive(first['receipt'].replace('?rev=1', '?rev=2')))
                with self.store.connect() as connection:
                    self.assertEqual(connection.execute('''SELECT sum(octet_length(text_redacted)) AS n
                        FROM canonical_chunks WHERE tenant_id=%s AND document_id=%s''',
                        (self.tenant, original['document_id'])).fetchone()['n'], 0)

    def test_public_resolve_rechecks_revoked_source_and_forget_deletes_history_bundle(self):
        from recall_server.app import Handler
        from urllib.parse import urlencode
        native, first, second, before = self.assert_archived_edit(singleton=True)
        handler = Handler.__new__(Handler)
        handler.path = '/v1/receipts/resolve?' + urlencode({'receipt': first['receipt']})
        handler.store, handler.evidence_archive_store = self.store, self.archive
        handler.hide_non_public_route = lambda *args: False
        handler.require = lambda *args: {'kind': 'collector', 'tenant_id': self.tenant,
            'principal_id': self.principal}
        with self.store.connect() as connection:
            connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Synthetic history',now())", (self.tenant,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company','history')", (self.tenant, self.tenant))
            connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')", (self.tenant, self.principal))
        responses = []
        handler.send_json = lambda status, payload: responses.append((status, payload))
        with patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': 'archive'}):
            handler.do_GET()
            self.assertEqual(responses[-1], (200, before))
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant, self.source))
            with patch.object(self.archive, 'read_raw', side_effect=AssertionError('revoked source read archive')):
                handler.do_GET()
            self.assertEqual(responses[-1][0], 404)
        # Restore only the fixture's grant, then exercise the original explicit forget owner.
        with self.store.connect() as connection:
            self.plane.register_source(connection, tenant_id=self.tenant, principal_id=self.principal, source_id=self.source)
            reference = self.plane.evidence_projector._reference(connection.execute(
                'SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s', (self.tenant,)).fetchone())
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-15T00:00:00Z', idempotency_key='archived-history-forget'))
        self.assertIsNone(self.resolve_archive(first['receipt']))
        self.assertIsNone(self.resolve_archive(second['receipt']))
        from recall_server.archive import ArchiveNotFound
        with self.assertRaises(ArchiveNotFound):
            self.archive.read_raw(reference)


if __name__ == '__main__':
    suite = unittest.TestSuite(ArchivedOutgoingHistory(name) for name in
        ArchivedOutgoingHistory.__dict__ if name.startswith('test_'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
