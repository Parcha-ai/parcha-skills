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
    canonical_json, post_slack, seed_slack_route, slack_event,
)
from connectors.slack_source import normalize_slack_message
from recall_server.canonical import CanonicalArchiveGateway
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
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

    def history(self, start, count):
        events = []
        for number in range(start, start + count):
            record = normalize_slack_message(workspace_id=SLACK_WORKSPACE,
                channel_id='C0E2E', value={'ts': f'{1700000000+number}.000100',
                    'user': 'U0E2E', 'text': f'Synthetic historical entry {number}.'})
            payload = canonical_json(record.content)
            artifact = self.gateway.put_raw(tenant_id=TENANT, source_id=SLACK_SOURCE,
                native_id=record.native_id, payload=payload, media_type='application/json',
                created_at=record.occurred_at)
            events.append({'schema_version': 1, 'source_id': SLACK_SOURCE,
                'native_id': record.native_id, 'native_parent_id': record.native_parent_id,
                'kind': 'connector_record', 'occurred_at': record.occurred_at,
                'observed_at': record.occurred_at, 'principal_id': OWNER,
                'visibility': 'private', 'content_type': 'application/json',
                'content': record.content,
                'provenance': {**record.provenance, 'connector_id': 'slack.messages',
                    'connector_schema_version': 2, 'artifact_ref': artifact},
                'content_sha256': hashlib.sha256(payload).hexdigest()})
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
