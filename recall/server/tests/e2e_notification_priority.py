#!/usr/bin/env python3
"""Signed notifications must publish while the same source replays history."""
from pathlib import Path
import hashlib
import os
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from e2e_webhook_ingest import (
    Handler, BrainStore, CanonicalPlane, FilesystemArchiveStore, LEGACY_TRUNCATE,
    LegacyIngestBridge, TENANT, OWNER, SLACK_SOURCE, SLACK_WORKSPACE, SLACK_SECRET,
    canonical_json, post_slack, seed_slack_route, slack_event, post_json, request, body,
)
from connectors.slack_source import normalize_slack_message
from recall_server.canonical import CanonicalArchiveGateway
from recall_server.identity_cache import REGISTRATION_CACHE
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector, LogicalEvidenceError, mark_logical_evidence_dirty,
)
from recall_server.managed_worker import _DirectCanonicalWriter
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.turbopuffer_plane import TurbopufferSettings
from recall_server.turbopuffer_projection import TurbopufferProjector
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class NotificationPriority(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=8)
        self.addCleanup(self.store.close)
        self.store.migrate()
        with self.store.connect() as connection:
            connection.execute(LEGACY_TRUNCATE)
        self.addCleanup(self.clear_fixture)
        REGISTRATION_CACHE.clear()
        seed_slack_route(self.store)
        environment = patch.dict(os.environ, {
            'RECALL_AUTH_REQUIRED': '1', 'RECALL_HTTP_PROFILE': 'public-edge',
            'RECALL_TRUST_TAILSCALE_HEADERS': '0',
            'RECALL_SLACK_SIGNING_SECRET': SLACK_SECRET,
            'RECALL_SLACK_CLIENT_ID': 'synthetic-client',
            'RECALL_SLACK_CLIENT_SECRET': 'synthetic-client-secret',
            'RECALL_SLACK_REDIRECT_URI': 'https://recall.example/admin/oauth/callback/slack',
            'RECALL_LEGACY_INGEST_TENANT_ID': TENANT,
            'RECALL_LEGACY_WRITES': '0', 'RECALL_LEGACY_READS': '0',
        })
        environment.start()
        self.addCleanup(environment.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.archive = FilesystemArchiveStore(Path(temporary.name), namespace_key=b'n'*32)
        self.plane = CanonicalPlane(self.store, self.archive)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=TENANT, principal_id=OWNER)
        self.writer = _DirectCanonicalWriter(self.plane, tenant_id=TENANT, principal_id=OWNER)
        for name, value in [('store', self.store), ('archive_store', self.archive),
                            ('canonical_plane', self.plane)]:
            override = patch.object(Handler, name, value, create=True)
            override.start()
            self.addCleanup(override.stop)
        self.store.legacy_ingest_bridge = LegacyIngestBridge(self.store, self.plane, self.archive)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        logical_store = LogicalEvidenceProjectionStore(self.archive)
        self.logical = CanonicalLogicalEvidenceProjector(self.store, logical_store,
            bound_tenant_id=TENANT, raw_archive=self.archive)
        self.passages = CanonicalPassageProjector(self.store, logical_store,
            bound_tenant_id=TENANT, policy=DEFAULT_PASSAGE_POLICY)
        client = FakeTurbopuffer()
        settings = TurbopufferSettings(api_key='synthetic-local-only')
        self.search_writer = TurbopufferProjector(self.store, settings, client=client)
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=TENANT,
            sources=[SLACK_SOURCE], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=settings, client=client)

    def clear_fixture(self):
        with self.store.connect() as connection:
            connection.execute(LEGACY_TRUNCATE)

    def history(self, start, count, *, text=None, source=SLACK_SOURCE, ingest=True, uri=None):
        events = []
        for number in range(start, start + count):
            record = normalize_slack_message(workspace_id=SLACK_WORKSPACE,
                channel_id='C0E2E', value={'ts': f'{1700000000+number}.000100',
                    'user': 'U0E2E', 'text': text or f'Synthetic historical entry {number}.'})
            payload = canonical_json(record.content)
            artifact = self.gateway.put_raw(tenant_id=TENANT, source_id=source,
                native_id=record.native_id, payload=payload, media_type='application/json',
                created_at=record.occurred_at)
            events.append({'schema_version': 1, 'source_id': source,
                'native_id': record.native_id, 'native_parent_id': record.native_parent_id,
                'kind': 'connector_record', 'occurred_at': record.occurred_at,
                'observed_at': record.occurred_at, 'principal_id': OWNER,
                'visibility': 'private', 'content_type': 'application/json',
                'content': record.content,
                'provenance': {**record.provenance, 'connector_id': 'slack.messages',
                    'connector_schema_version': 2, 'artifact_ref': artifact,
                    **({'uri': uri} if uri is not None else {})},
                'content_sha256': hashlib.sha256(payload).hexdigest()})
        if ingest:
            self.assertEqual(self.writer.ingest(events)['inserted'], count)
        return events

    def publish(self):
        self.passages.project_pending(batch_size=10, max_batches=1, concurrency=1)
        self.search_writer.drain(tenant_id=TENANT, max_months=4)

    def test_signed_notification_is_searchable_before_later_history_drains(self):
        older = self.history(0, 10)
        timestamp = f'{int(time.time())}.000100'
        status, acknowledgement = post_slack(self.server,
            slack_event('Synthetic notification sapphire rendezvous.', timestamp))
        self.assertEqual(status, 200)
        self.assertEqual(acknowledgement['routes'], 1)
        native = f'slack:{SLACK_WORKSPACE}:C0E2E:{timestamp}'
        with self.store.connect() as connection:
            receipt = connection.execute('''SELECT chunk.receipt FROM canonical_chunks chunk
                JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND document.native_id=%s AND document.is_current
                  AND document.deleted_at IS NULL AND chunk.deleted_at IS NULL''',
                (TENANT, SLACK_SOURCE, native)).fetchone()['receipt']
        self.history(10, 10)  # Later arrival, older provider time, same source.
        result = self.logical.project_pending(batch_size=1, max_batches=2,
            upload_concurrency=1, on_progress=self.publish)
        self.assertEqual(result['documents'], 2)
        with self.store.connect() as connection:
            old_published = connection.execute('''SELECT count(*) AS n
                FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s
                  AND native_parent_id=%s''',
                (TENANT, SLACK_SOURCE, older[0]['native_parent_id'])).fetchone()['n']
            remaining = connection.execute('''SELECT count(*) AS n
                FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s''',
                (TENANT, SLACK_SOURCE)).fetchone()['n']
        self.assertEqual(old_published, 1, 'oldest history must retain progress')
        self.assertGreater(remaining, 0, 'proof requires an unfinished history backlog')
        hits = self.retrieval.search('notification sapphire rendezvous',
            lexical_query='notification sapphire rendezvous', since=None, until=None,
            limit=10)['results']
        self.assertTrue(any(receipt in item.get('receipts', ()) for hit in hits
            for item in hit['matching_ranges']),
            'signed live notification was stranded behind later historical replay')


    def notify(self, number, text):
        raw = slack_event(text, f'{1700000000+number}.000100')
        status, result = post_slack(self.server, raw)
        self.assertEqual(status, 200, result)
        self.assertEqual(result['routes'], 1)
        return raw

    def queue(self, native, source=SLACK_SOURCE):
        with self.store.connect() as connection:
            return connection.execute("""SELECT queue.* FROM canonical_evidence_document_queue queue
                WHERE queue.tenant_id=%s AND queue.source_id=%s AND queue.native_parent_id=(
                  SELECT coalesce(event.native_parent_id,event.native_id) FROM canonical_events event
                  WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id=%s
                  ORDER BY event.revision DESC LIMIT 1)""",
                (TENANT, source, TENANT, source, native)).fetchone()

    def test_notification_fifo_survives_cost_dispatch_and_later_history(self):
        first = self.history(40, 1, text='Synthetic larger original. ' * 1000)[0]
        second = self.history(41, 1)[0]
        self.logical.project_pending(batch_size=2, max_batches=1, upload_concurrency=1)
        self.notify(40, 'Synthetic notification first, larger prior archive.')
        self.notify(41, 'Synthetic notification second.')
        self.history(50, 10)
        candidates = self.logical._pending(tenant_id=TENANT, limit=3, prefer_recent=True)
        first_parent, second_parent = first['native_parent_id'], second['native_parent_id']
        self.assertEqual([c.native_parent_id for c in candidates[:2]], [first_parent, second_parent])
        self.assertGreater(candidates[0].estimated_bytes, candidates[1].estimated_bytes)
        dispatched = []
        prepare = self.logical._prepare_batch_and_upload
        def observe(candidates, **kwargs):
            dispatched.extend(c.native_parent_id for c in candidates)
            return prepare(candidates, **kwargs)
        self.logical._prefer_recent_admission = True
        with patch.object(self.logical, '_prepare_batch_and_upload', observe):
            self.logical.project_pending(batch_size=3, max_batches=1,
                upload_concurrency=1, on_progress=self.publish)
        self.assertEqual(dispatched[:2], [first_parent, second_parent])
        self.assertIsNone(self.queue(first['native_id']))
        self.assertIsNone(self.queue(second['native_id']))

    def test_duplicate_only_promotes_pending_work_and_history_cannot_demote(self):
        raw = self.notify(60, 'Synthetic notification duplicate.')
        native = f'slack:{SLACK_WORKSPACE}:C0E2E:1700000060.000100'
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_document_queue SET notification_queued_at=NULL')
        before = self.queue(native)
        status, replay = post_slack(self.server, raw, retry='1')
        self.assertEqual(status, 200)
        self.assertEqual(replay['duplicate_events'], 1)
        promoted = self.queue(native)
        self.assertIsNotNone(promoted['notification_queued_at'])
        for field in ('generation', 'changed_at', 'first_queued_at', 'reason', 'attempts', 'next_attempt_at'):
            self.assertEqual(promoted[field], before[field], field)
        self.history(60, 1, text='Synthetic later historical correction.')
        self.assertEqual(self.queue(native)['notification_queued_at'], promoted['notification_queued_at'])
        corrected = self.queue(native)
        with self.store.connect() as connection:
            current_before = connection.execute('SELECT document_id FROM canonical_documents WHERE is_current').fetchone()['document_id']
        self.assertEqual(post_slack(self.server, raw, retry='3')[0], 200)
        self.assertEqual(self.queue(native)['generation'], corrected['generation'])
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT document_id FROM canonical_documents WHERE is_current').fetchone()['document_id'], current_before)
        self.logical.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertIsNone(self.queue(native))
        with self.store.connect() as connection:
            before_revisions = connection.execute('SELECT count(*) AS n FROM canonical_events').fetchone()['n']
        self.assertEqual(post_slack(self.server, raw, retry='2')[0], 200)
        self.assertIsNone(self.queue(native), 'completed duplicate must not create new work')
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events').fetchone()['n'], before_revisions)

    def test_oldest_round_preserves_other_source_progress_under_notifications(self):
        other = self.history(70, 3, source='synthetic:peer')
        self.history(80, 5)
        for number in range(90, 95):
            self.notify(number, f'Synthetic notification {number}.')
        result = self.logical.project_pending(batch_size=2, max_batches=2,
            upload_concurrency=1, on_progress=self.publish)
        self.assertEqual(result['documents'], 4)
        self.assertIsNone(self.queue(other[0]['native_id'], source='synthetic:peer'))
        notified = [self.queue(f'slack:{SLACK_WORKSPACE}:C0E2E:{1700000000+n}.000100')
                    for n in range(90, 95)]
        self.assertEqual(sum(row is None for row in notified), 2)
        self.assertEqual(notified[:2], [None, None])

    def test_notification_priority_keeps_forget_and_eligibility_gates(self):
        history = self.history(110, 1)[0]
        for number in range(111, 116):
            self.notify(number, f'Synthetic eligibility notification {number}.')
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_evidence_document_queue SET first_queued_at=clock_timestamp()-interval '20 minutes',changed_at=clock_timestamp()-interval '2 minutes'")
            connection.execute("UPDATE canonical_evidence_document_queue SET reason='forget' WHERE native_parent_id=%s", (history['native_parent_id'],))
            connection.execute("UPDATE canonical_evidence_document_queue SET attempts=8 WHERE native_parent_id LIKE '%%1700000112.000100'")
            connection.execute("UPDATE canonical_evidence_document_queue SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE native_parent_id LIKE '%%1700000113.000100'")
            connection.execute("UPDATE canonical_evidence_document_queue SET changed_at=clock_timestamp(),first_queued_at=clock_timestamp() WHERE native_parent_id LIKE '%%1700000114.000100'")
            # An active notification remains eligible once the existing max-wait expires.
            connection.execute("UPDATE canonical_evidence_document_queue SET changed_at=clock_timestamp() WHERE native_parent_id LIKE '%%1700000115.000100'")
        candidates = self.logical._pending(tenant_id=TENANT, limit=10, prefer_recent=True,
            quiet_seconds=90, max_wait_seconds=600)
        self.assertEqual([c.native_parent_id for c in candidates], [history['native_parent_id'],
            f'slack-thread:{SLACK_WORKSPACE}:C0E2E:1700000111.000100',
            f'slack-thread:{SLACK_WORKSPACE}:C0E2E:1700000115.000100'])
        with self.assertRaisesRegex(LogicalEvidenceError, 'logical_evidence_tenant_not_configured'):
            self.logical.project_pending(tenant_id='synthetic:foreign')

    def test_notification_repair_preserves_stamp_and_final_cas_rejects_changed_input(self):
        event = self.history(120, 1)[0]
        self.logical.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.notify(120, 'Synthetic notification pending repair.')
        stamped = self.queue(event['native_id'])
        self.assertEqual(self.logical.seed_backfill(source_id=SLACK_SOURCE, include_existing=True), 1)
        self.assertEqual(self.queue(event['native_id'])['notification_queued_at'], stamped['notification_queued_at'])
        original = self.logical._commit_upload
        def forget_before_commit(candidate, upload):
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s', (TENANT, SLACK_SOURCE))
                connection.execute('UPDATE canonical_documents SET is_current=false,deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s', (TENANT, SLACK_SOURCE))
                mark_logical_evidence_dirty(connection, tenant_id=TENANT, source_id=SLACK_SOURCE,
                    native_ids=[event['native_id']], reason='forget')
            return original(candidate, upload)
        self.logical._prefer_recent_admission = True
        with patch.object(self.logical, '_commit_upload', forget_before_commit):
            result = self.logical.project_pending(batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual((result['documents'], result['source_races'], result['failed']), (0, 1, 0))
        queued = self.queue(event['native_id'])
        self.assertEqual((queued['reason'], queued['attempts']), ('forget', 0))
        self.assertEqual(queued['notification_queued_at'], stamped['notification_queued_at'])
        self.assertGreater(queued['generation'], stamped['generation'])

    def test_forgotten_notification_cannot_be_promoted_or_resurrected(self):
        raw = self.notify(130, 'Synthetic forgotten notification.')
        native = f'slack:{SLACK_WORKSPACE}:C0E2E:1700000130.000100'
        self.plane.forget({'contract': 'recall.forget-request.v1', 'schema_version': 1,
            'tenant_id': TENANT, 'principal_id': OWNER, 'source_id': SLACK_SOURCE,
            'target_receipt': f'recall://{SLACK_SOURCE}/{native}?rev=1#item=0',
            'mode': 'explicit_forget', 'reason': 'owner_requested',
            'requested_at': '2026-09-25T00:00:00Z', 'idempotency_key': 'synthetic-notification-forget'})
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_document_queue SET notification_queued_at=NULL')
            before = connection.execute('SELECT * FROM canonical_evidence_document_queue').fetchall()
        status, _ = post_slack(self.server, raw, retry='1')
        self.assertEqual(status, 409)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT * FROM canonical_evidence_document_queue').fetchall(), before)
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_documents WHERE is_current AND deleted_at IS NULL').fetchone()['n'], 0)

    def test_public_json_and_provenance_cannot_request_notification_priority(self):
        token = self.store.create_collector_token('synthetic-canonical', SLACK_SOURCE, ['write'],
            tenant_id=TENANT, principal_id=OWNER)['token']
        events = self.history(100, 1, ingest=False, uri='connector://slack/events')
        with patch.dict(os.environ, {'RECALL_CANONICAL_INGEST_PUBLIC': '1'}):
            status, response = post_json(self.server, '/v2/ingest/canonical', token,
                {'tenant_id': TENANT, 'principal_id': OWNER, 'source_id': SLACK_SOURCE,
                 'events': events, 'notification': True, 'notification_queued_at': '2000-01-01T00:00:00Z'})
        self.assertEqual(status, 201, response)
        self.assertIsNone(self.queue(events[0]['native_id'])['notification_queued_at'])

    def test_notification_ingress_keeps_signature_scope_and_closed_json_controls(self):
        status, _ = request(self.server, 'POST', '/webhooks/v1/slack',
            payload=slack_event('Synthetic rejected.'), headers={
                'X-Slack-Request-Timestamp': str(int(time.time())), 'X-Slack-Signature': 'v0=wrong'})
        self.assertEqual(status, 400)
        webhook = self.store.create_collector_token('synthetic-notify', 'synthetic:webhook', ['webhook'],
            tenant_id=TENANT, principal_id=OWNER, webhook_privacy_mode='scrub')['token']
        reader = self.store.create_collector_token('synthetic-reader', 'synthetic:webhook', ['read'],
            tenant_id=TENANT, principal_id=OWNER)['token']
        self.assertEqual(post_json(self.server, '/webhooks/v1/events', reader, body())[0], 401)
        self.assertEqual(post_json(self.server, '/webhooks/v1/events', webhook,
            {**body(), 'notification': True})[0], 400)
        self.assertEqual(post_json(self.server, '/webhooks/v1/events', webhook,
            {**body(), 'source_id': SLACK_SOURCE})[0], 400)
        self.assertEqual(post_json(self.server, '/webhooks/v1/events', webhook, body())[0], 201)
        self.assertIsNotNone(self.queue('synthetic-event', source='synthetic:webhook')['notification_queued_at'])
        self.assertTrue(self.store.revoke_collector_token('synthetic-notify'))
        self.assertEqual(post_json(self.server, '/webhooks/v1/events', webhook, body())[0], 401)
        with self.store.connect() as connection:
            rows = connection.execute('SELECT source_id FROM canonical_evidence_document_queue').fetchall()
        self.assertEqual([row['source_id'] for row in rows], ['synthetic:webhook'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
