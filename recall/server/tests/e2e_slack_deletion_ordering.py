#!/usr/bin/env python3
"""Real PG/archive checks for Slack deletes, delayed revisions, and identity scope."""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
import threading
import time
from unittest.mock import patch
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]

from client.mac import canonical_envelope
from connectors.slack_source import normalize_slack_message
from recall_server.archive import ArchiveNotFound, FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane
from recall_server.db import BrainStore
from recall_server.legacy_plane import LegacyIngestBridge
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.projectors import canonical_json


class SlackDeletionOrdering(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        suffix = uuid.uuid4().hex
        self.tenant = 'tenant:slack-delete:' + suffix
        self.source = 'source:slack-delete:' + suffix
        self.principal = 'principal:slack-delete:' + suffix
        directory = tempfile.TemporaryDirectory(prefix='recall-slack-delete-')
        self.addCleanup(directory.cleanup)
        self.archive = FilesystemArchiveStore(Path(directory.name), namespace_key=b's' * 32)
        self.plane = CanonicalPlane(self.store, self.archive)
        self.bridge = LegacyIngestBridge(self.store, self.plane, self.archive,
                                        environment={'RECALL_LEGACY_WRITES': '0'})
        self.root = {'ts': '1784332800.000100', 'user': 'U111', 'text': 'Root payload'}
        self.reply = {'ts': '1784332801.000100', 'thread_ts': self.root['ts'],
                      'user': 'U222', 'text': 'Reply payload'}

    def ingest(self, value, *, tenant=None, source=None, path='legacy', connector_id='slack.messages', principal=None):
        tenant, source = tenant or self.tenant, source or self.source
        principal = principal or self.principal
        record = normalize_slack_message(workspace_id='T123', channel_id='C123', value=value,
                                         provenance_surface='events')
        event = canonical_envelope(
            source_id=source, native_id=record.native_id,
            kind='tombstone' if record.deleted else 'connector_record',
            content={'target_native_id': record.native_id} if record.deleted else record.content,
            principal_id=principal, visibility='private', occurred_at=record.occurred_at,
            parent=record.native_parent_id,
            provenance={**record.provenance, 'connector_id': connector_id, 'connector_schema_version': 2},
        )
        if path != 'legacy':
            gateway = CanonicalArchiveGateway(self.store, self.archive, tenant_id=tenant, principal_id=principal)
            artifact = gateway.put_raw(tenant_id=tenant, source_id=source, native_id=record.native_id,
                payload=canonical_json(event), media_type='application/json', created_at=record.occurred_at)
            event['provenance']['artifact_ref'] = artifact
            if path == 'prepare':
                return dict(tenant_id=tenant, principal_id=principal, connector_id=connector_id,
                            artifact_ref=artifact, envelope=event,
                            text_redacted='' if record.deleted else canonical_json(record.content).decode())
            if path == 'batch':
                return self.plane.ingest_batch(tenant_id=tenant, principal_id=principal, events=[event])
            return self.plane.ingest_document(tenant_id=tenant, principal_id=principal,
                connector_id=connector_id, artifact_ref=artifact, envelope=event,
                text_redacted='' if record.deleted else canonical_json(record.content).decode())
        return self.bridge.ingest('synthetic-delete-' + uuid.uuid4().hex, [event],
                                 principal={'tenant_id': tenant, 'source_id': source,
                                            'principal_id': principal}, notification=True)[0]

    def delete(self, value):
        return {'subtype': 'message_deleted', 'deleted_ts': value['ts'], 'event_ts': '1784505600.000100'}

    def current(self, value, *, tenant=None, source=None):
        with self.store.connect() as connection:
            return connection.execute('''SELECT count(*) AS n FROM canonical_documents
                WHERE tenant_id=%s AND source_id=%s AND native_id=%s
                  AND is_current AND deleted_at IS NULL''',
                (tenant or self.tenant, source or self.source, 'slack:T123:C123:' + value['ts'])).fetchone()['n']

    def project(self):
        logical_store = LogicalEvidenceProjectionStore(self.archive)
        logical = CanonicalLogicalEvidenceProjector(self.store, logical_store,
                    bound_tenant_id=self.tenant, raw_archive=self.archive)
        logical.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=2, upload_concurrency=1)
        passages = CanonicalPassageProjector(self.store, logical_store,
                    bound_tenant_id=self.tenant, policy=DEFAULT_PASSAGE_POLICY)
        passages.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=2, concurrency=1)
        with self.store.connect() as connection:
            return '\n'.join(row['text_redacted'] for row in connection.execute(
                'SELECT text_redacted FROM canonical_passages WHERE tenant_id=%s AND source_id=%s',
                (self.tenant, self.source)).fetchall())

    def test_root_delete_removes_root_passage_and_preserves_reply(self):
        self.ingest(self.root)
        self.ingest(self.reply)
        self.assertIn('Root payload', self.project())
        self.ingest(self.delete(self.root))
        self.assertEqual(self.current(self.root), 0)
        self.assertEqual(self.current(self.reply), 1)
        projected = self.project()
        self.assertNotIn('Root payload', projected)
        self.assertIn('Reply payload', projected)

    def test_reply_delete_and_replay_remain_scoped_to_exact_source_and_tenant(self):
        other_tenant, other_source = self.tenant + ':other', self.source + ':other'
        self.ingest(self.reply)
        self.ingest(self.reply, source=other_source)
        self.ingest(self.reply, tenant=other_tenant)
        self.ingest(self.delete(self.reply))
        replay = self.ingest(self.reply)
        self.assertEqual(replay['duplicate_events'], 1)
        self.assertEqual(self.current(self.reply), 0)
        self.assertEqual(self.current(self.reply, source=other_source), 1)
        self.assertEqual(self.current(self.reply, tenant=other_tenant), 1)

    def test_delayed_unseen_edit_cannot_resurrect_a_deleted_message(self):
        self.ingest(self.root)
        self.ingest(self.delete(self.root))
        delayed = {**self.root, 'text': 'Delayed pre-delete edit', 'edited': {'ts': '1784400000.000100'}}
        self.ingest(delayed)
        self.assertEqual(self.current(self.root), 0)
        self.assertNotIn('Delayed pre-delete edit', self.project())

    def test_batch_terminal_ack_is_distinct_from_exact_hash_replay_and_retains_raw_audit(self):
        self.ingest(self.root, path='batch')
        terminal = self.ingest(self.delete(self.root), path='batch')
        with self.store.connect() as connection:
            artifacts_before = connection.execute('SELECT count(*) AS n FROM raw_artifacts WHERE tenant_id=%s',
                                                  (self.tenant,)).fetchone()['n']
        delayed = {**self.root, 'text': 'Unseen delayed batch edit', 'edited': {'ts': '1784400000.000100'}}
        suppressed = self.ingest(delayed, path='batch')
        self.assertEqual(self.current(self.root), 0)
        self.assertEqual(suppressed['inserted'], 0)
        self.assertEqual(suppressed['duplicate_events'], 1)
        self.assertEqual(suppressed['suppressed_deleted_events'], 1)
        self.assertEqual(suppressed['receipts'], terminal['receipts'])
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM raw_artifacts WHERE tenant_id=%s',
                                                 (self.tenant,)).fetchone()['n'], artifacts_before + 1)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s',
                                                 (self.tenant,)).fetchone()['n'], 2)
            audit = connection.execute("SELECT item_count,byte_count FROM canonical_audit_events WHERE tenant_id=%s AND operation='ingest.slack_suppressed_deleted'",
                                       (self.tenant,)).fetchall()
            self.assertEqual(len(audit), 1)
            self.assertEqual(audit[0]['item_count'], 1)
            self.assertGreater(audit[0]['byte_count'], 0)
        exact_replay = self.ingest(self.root, path='batch')
        self.assertEqual(exact_replay.get('suppressed_deleted_events', 0), 0)
        self.assertEqual(exact_replay['duplicate_events'], 1)

    def test_terminal_ack_never_bypasses_source_owner_authority(self):
        self.ingest(self.delete(self.root))
        for path in ('legacy', 'document', 'batch'):
            with self.subTest(path=path), self.assertRaises(CanonicalLifecycleError) as error:
                self.ingest(self.root, path=path, principal=self.principal + ':outsider')
            self.assertEqual(error.exception.error_code, 'canonical_authority_forbidden')
        self.assertEqual(self.current(self.root), 0)

    def test_other_connector_restoration_semantics_are_unchanged(self):
        self.ingest(self.root, connector_id='synthetic.other', path='batch')
        self.ingest(self.delete(self.root), connector_id='synthetic.other', path='batch')
        result = self.ingest({**self.root, 'text': 'Restored generic record'},
                             connector_id='synthetic.other', path='batch')
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(self.current(self.root), 1)

    def test_deleted_identity_remains_terminal_after_outgoing_bodies_were_retired(self):
        self.ingest(self.root)
        self.project()
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_documents SET text_redacted='',body_location='chunks' WHERE tenant_id=%s AND is_current", (self.tenant,))
            connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s", (self.tenant,))
        self.ingest(self.delete(self.root))
        self.ingest({**self.root, 'text': 'Delayed after body retirement'}, path='batch')
        self.assertEqual(self.current(self.root), 0)

    def test_owner_committed_before_forget_keeps_shared_raw(self):
        first = self.ingest(self.root, path='document')
        peer = self.ingest(self.reply, path='prepare')
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex)
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.store.connect() as holder:
                original = self.plane._archive_references(holder, tenant_id=self.tenant,
                    source_id=self.source, native_ids=['slack:T123:C123:' + self.root['ts']])[0]
                peer['artifact_ref'] = original
                peer['envelope']['provenance']['artifact_ref'] = original
                pid = holder.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
                forgetting = pool.submit(self.plane.forget, request)
                blocked = False
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    with self.store.connect() as observer:
                        blocked = observer.execute('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE %s=ANY(pg_blocking_pids(pid))) AS waiting', (pid,)).fetchone()['waiting']
                    if blocked:
                        break
                    time.sleep(0.01)
                self.assertTrue(blocked, 'forget did not wait for artifact admission transaction')
                self.plane.ingest_document(**peer, _connection=holder)
            forgetting.result(timeout=10)
        self.assertEqual(self.current(self.reply), 1)
        self.assertIn(b'Root payload', self.archive.read_raw(original))

    def test_concurrent_forget_of_two_suppressed_owners_does_not_strand_raw(self):
        first = self.ingest(self.delete(self.root), path='document')
        second = self.ingest(self.delete(self.reply), path='document')
        stale = self.ingest(self.root, path='prepare')
        peer = self.ingest(self.reply, path='prepare')
        peer['envelope']['provenance']['artifact_ref'] = stale['artifact_ref']
        self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                               events=[stale['envelope'], peer['envelope']])
        barrier = threading.Barrier(2)
        select = self.plane._archive_references
        def start_together(connection, **kwargs):
            barrier.wait(timeout=5)
            return select(connection, **kwargs)
        def forget(receipt):
            return self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex))
        with patch.object(self.plane, '_archive_references', side_effect=start_together), ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(forget, item['receipt']) for item in (first, second)]
            for future in futures:
                self.assertEqual(future.result(timeout=10)['status'], 'deleted')
        with self.assertRaises(ArchiveNotFound):
            self.archive.read_raw(stale['artifact_ref'])
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_suppressed_artifact_owners WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)

    def test_completed_forget_rejects_new_identity_using_old_shared_raw_reference(self):
        first = self.ingest(self.root, path='document')
        peer = self.ingest(self.reply, path='prepare')
        with self.store.connect() as connection:
            artifact = connection.execute('SELECT artifact_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()['artifact_id']
            original = self.plane._archive_references(connection, tenant_id=self.tenant,
                source_id=self.source, native_ids=['slack:T123:C123:' + self.root['ts']])[0]
        peer['artifact_ref'] = original
        peer['envelope']['provenance']['artifact_ref'] = original
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex))
        for path in ('document', 'batch'):
            with self.subTest(path=path), self.assertRaises(CanonicalLifecycleError) as error:
                if path == 'document':
                    self.plane.ingest_document(**peer)
                else:
                    self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[peer['envelope']])
            self.assertEqual(error.exception.error_code, 'canonical_artifact_forgotten')
        self.assertEqual(self.current(self.reply), 0)

    def test_forget_serializes_with_new_shared_raw_owner_before_deleting(self):
        first = self.ingest(self.root, path='document')
        peer = self.ingest(self.reply, path='prepare')
        with self.store.connect() as connection:
            original = self.plane._archive_references(connection, tenant_id=self.tenant,
                source_id=self.source, native_ids=['slack:T123:C123:' + self.root['ts']])[0]
        peer['artifact_ref'] = original
        peer['envelope']['provenance']['artifact_ref'] = original
        selected, release = threading.Event(), threading.Event()
        select = self.plane._archive_references
        holder_pid = []
        def pause_after_selection(connection, **kwargs):
            result = select(connection, **kwargs)
            holder_pid.append(connection.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid'])
            selected.set()
            if not release.wait(5):
                raise AssertionError('test did not release forget selection')
            return result
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex)
        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(self.plane, '_archive_references', side_effect=pause_after_selection):
            forgetting = pool.submit(self.plane.forget, request)
            try:
                self.assertTrue(selected.wait(5))
                writer = pool.submit(self.plane.ingest_document, **peer)
                blocked = False
                deadline = time.monotonic() + 3
                while not writer.done() and time.monotonic() < deadline:
                    with self.store.connect() as observer:
                        blocked = observer.execute('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE %s=ANY(pg_blocking_pids(pid))) AS waiting', (holder_pid[0],)).fetchone()['waiting']
                    if blocked:
                        break
                    time.sleep(0.01)
                self.assertTrue(blocked, 'new shared owner bypassed artifact lifetime lock')
            finally:
                release.set()
            forgetting.result(timeout=10)
            with self.assertRaises(CanonicalLifecycleError) as error:
                writer.result(timeout=10)
            self.assertEqual(error.exception.error_code, 'canonical_artifact_forgotten')
        self.assertEqual(self.current(self.reply), 0)

    def test_missing_optional_ownership_schema_fails_only_suppression_closed(self):
        with self.store.connect() as connection:
            connection.execute('ALTER TABLE canonical_suppressed_artifact_owners RENAME TO absent_slack_ownership')
        try:
            self.ingest(self.root)
            self.ingest(self.delete(self.root))
            for path in ('document', 'batch'):
                with self.subTest(path=path), self.assertRaises(CanonicalLifecycleError) as error:
                    self.ingest({**self.root, 'text': 'No untracked terminal ACK'}, path=path)
                self.assertEqual(error.exception.error_code, 'slack_deletion_schema_required')
            self.assertEqual(self.current(self.root), 0)
            self.ingest(self.reply)
            self.assertEqual(self.current(self.reply), 1)
        finally:
            with self.store.connect() as connection:
                connection.execute('ALTER TABLE absent_slack_ownership RENAME TO canonical_suppressed_artifact_owners')
        self.assertEqual(self.ingest({**self.root, 'text': 'No untracked terminal ACK'}, path='batch')['suppressed_deleted_events'], 1)

    def test_suppressed_ownership_foreign_key_cascades_on_raw_row_deletion(self):
        self.ingest(self.delete(self.root))
        self.ingest(self.root, path='batch')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_suppressed_artifact_owners WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 1)
            connection.execute('DELETE FROM raw_artifacts WHERE tenant_id=%s AND artifact_id IN (SELECT artifact_id FROM canonical_suppressed_artifact_owners WHERE tenant_id=%s)', (self.tenant, self.tenant))
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_suppressed_artifact_owners WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)

    def test_shared_suppressed_raw_survives_partial_forget_until_final_owner(self):
        for retained_second in (False, True):
            with self.subTest(retained_second=retained_second):
                self.source = 'source:slack-delete:' + uuid.uuid4().hex
                first = self.ingest(self.delete(self.root), path='document')
                second = self.ingest(self.reply if retained_second else self.delete(self.reply), path='document')
                stale = self.ingest({**self.root, 'text': 'Shared raw canary'}, path='prepare')
                peer = self.ingest({**self.reply, 'text': 'Shared second identity'}, path='prepare')
                peer['artifact_ref'] = stale['artifact_ref']
                peer['envelope']['provenance']['artifact_ref'] = stale['artifact_ref']
                self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                       events=[stale['envelope'], peer['envelope']])
                def forget(receipt):
                    self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                        tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                        target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
                        requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex))
                forget(first['receipt'])
                self.assertIn(b'Shared raw canary', self.archive.read_raw(stale['artifact_ref']))
                forget(second['receipt'])
                with self.assertRaises(ArchiveNotFound):
                    self.archive.read_raw(stale['artifact_ref'])

    def test_forget_removes_suppressed_raw_and_preserves_other_source(self):
        for path in ('document', 'batch'):
            with self.subTest(path=path):
                self.source = 'source:slack-delete:' + uuid.uuid4().hex
                first = self.ingest(self.root, path='document')
                self.ingest(self.delete(self.root))
                stale = self.ingest({**self.root, 'text': 'Suppressed raw canary'}, path='prepare')
                other = self.ingest({**self.root, 'text': 'Other source raw canary'}, source=self.source + ':other', path='prepare')
                if path == 'batch':
                    self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal,
                                            events=[stale['envelope']])
                else:
                    self.plane.ingest_document(**stale)
                self.plane.ingest_document(**other)
                self.assertIn(b'Suppressed raw canary', self.archive.read_raw(stale['artifact_ref']))
                request = dict(contract='recall.forget-request.v1', schema_version=1,
                    tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
                    target_receipt=first['receipt'], mode='explicit_forget', reason='owner_requested',
                    requested_at='2026-10-13T05:00:00Z', idempotency_key=uuid.uuid4().hex)
                delete_raw = self.archive.delete_raw
                failed = []
                def lose_suppressed_delete_ack(reference):
                    result = delete_raw(reference)
                    if reference['artifact_id'] == stale['artifact_ref']['artifact_id'] and not failed:
                        failed.append(True)
                        raise OSError('synthetic lost suppression delete acknowledgement')
                    return result
                with patch.object(self.archive, 'delete_raw', side_effect=lose_suppressed_delete_ack):
                    with self.assertRaises(CanonicalLifecycleError) as error:
                        self.plane.forget(request)
                    self.assertEqual(error.exception.error_code, 'archive_delete_failed')
                    self.assertTrue(failed)
                    self.plane.forget(request)
                self.plane.forget(request)
                with self.assertRaises(ArchiveNotFound):
                    self.archive.read_raw(stale['artifact_ref'])
                with self.store.connect() as connection:
                    self.assertIsNone(connection.execute('SELECT state FROM raw_artifacts WHERE tenant_id=%s AND source_id=%s AND artifact_id=%s',
                        (self.tenant, self.source, stale['artifact_ref']['artifact_id'])).fetchone())
                self.assertIn(b'Other source raw canary', self.archive.read_raw(other['artifact_ref']))
                with self.assertRaises(CanonicalLifecycleError):
                    self.plane.ingest_document(**stale)

    def test_concurrent_edit_and_delete_serialize_on_native_identity_in_both_orders(self):
        for first_is_delete in (True, False):
            for queued_path in ('document', 'batch'):
                with self.subTest(first_is_delete=first_is_delete, queued_path=queued_path):
                    self.source = 'source:slack-delete:' + uuid.uuid4().hex
                    self.ingest(self.root)
                    edit = self.ingest({**self.root, 'text': 'Concurrent late edit'}, path='prepare')
                    deletion = self.ingest(self.delete(self.root), path='prepare')
                    first, second = (deletion, edit) if first_is_delete else (edit, deletion)
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        with self.store.connect() as holder:
                            holder.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',
                                (f"v2\x1f{self.tenant}\x1f{self.source}\x1fslack:T123:C123:{self.root['ts']}",))
                            holder_pid = holder.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
                            if queued_path == 'batch':
                                pending = pool.submit(self.plane.ingest_batch, tenant_id=self.tenant,
                                    principal_id=self.principal, events=[second['envelope']])
                            else:
                                pending = pool.submit(self.plane.ingest_document, **second)
                            deadline = time.monotonic() + 5
                            waiting = False
                            while time.monotonic() < deadline:
                                with self.store.connect() as observer:
                                    waiting = observer.execute("""SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                                        WHERE %s=ANY(pg_blocking_pids(pid))) AS waiting""",
                                        (holder_pid,)).fetchone()['waiting']
                                if waiting:
                                    break
                                time.sleep(0.01)
                            self.assertTrue(waiting, 'competing ingest did not reach the real native lock')
                            self.plane.ingest_document(**first, _connection=holder)
                        result = pending.result(timeout=10)
                    self.assertEqual(self.current(self.root), 0)
                    self.assertEqual(result.get('suppressed_deleted_events', 0), int(first_is_delete))

    def test_revoked_provider_routes_no_new_signed_webhook_or_raw_artifact(self):
        from e2e_webhook_ingest import seed_slack_route, post_slack, slack_event, SLACK_SECRET, TENANT, OWNER
        from recall_server.app import Handler
        seed_slack_route(self.store)
        with patch.dict(os.environ, {'RECALL_AUTH_REQUIRED': '1',
                'RECALL_TRUST_TAILSCALE_HEADERS': '0', 'RECALL_SLACK_SIGNING_SECRET': SLACK_SECRET,
                'RECALL_LEGACY_WRITES': '0', 'RECALL_HTTP_PROFILE': 'private'}), \
             patch.object(Handler, 'store', self.store, create=True), \
             patch.object(Handler, 'archive_store', self.archive, create=True), \
             patch.object(Handler, 'canonical_plane', self.plane, create=True), \
             patch.object(self.store, 'legacy_ingest_bridge', self.bridge, create=True):
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                status, before = post_slack(server, slack_event('Authorized message'))
                self.assertEqual(status, 200)
                self.assertEqual(before['routes'], 1)
                with self.store.connect() as connection:
                    raw_before = connection.execute('SELECT count(*) AS n FROM raw_artifacts WHERE tenant_id=%s', (TENANT,)).fetchone()['n']
                    events_before = connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s', (TENANT,)).fetchone()['n']
                    connection.execute("UPDATE provider_connections SET status='revoked',encrypted_credentials='' WHERE principal_id=%s", (OWNER,))
                    connection.execute("UPDATE connector_installations SET state='revoked',revision=revision+1 WHERE tenant_id=%s", (TENANT,))
                status, after = post_slack(server, slack_event('Unauthorized later message', ts='1721332801.000100'))
                self.assertEqual(status, 200)
                self.assertEqual(after['routes'], 0)
                with self.store.connect() as connection:
                    self.assertEqual(connection.execute('SELECT count(*) AS n FROM raw_artifacts WHERE tenant_id=%s', (TENANT,)).fetchone()['n'], raw_before)
                    self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s', (TENANT,)).fetchone()['n'], events_before)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_delete_arriving_before_original_message_still_prevents_resurrection(self):
        self.ingest(self.delete(self.root))
        self.ingest(self.root)
        self.assertEqual(self.current(self.root), 0)


if __name__ == '__main__':
    unittest.main()
