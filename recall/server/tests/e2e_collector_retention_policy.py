#!/usr/bin/env python3
"""Filesystem cleanup cannot revoke captured canonical history."""
import unittest
import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import patch

import e2e_slack_deletion_ordering as fixture


class CollectorRetentionPolicy(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    setUp = fixture.SlackDeletionOrdering.setUp
    ingest = fixture.SlackDeletionOrdering.ingest
    delete = fixture.SlackDeletionOrdering.delete
    current = fixture.SlackDeletionOrdering.current

    def test_scalar_and_set_batch_suppress_collector_deletion_without_history_io(self):
        for connector in ('claude.jsonl', 'codex.jsonl'):
            for path in ('document', 'batch'):
                with self.subTest(connector=connector, path=path):
                    first = self.ingest(self.root, path='document', connector_id=connector)
                    with patch('recall_server.canonical_history.read_archived_chunks') as history:
                        result = self.ingest(self.delete(self.root), path=path, connector_id=connector)
                        self.assertEqual(result['suppressed_filesystem_events'], 1)
                        self.assertEqual(result['inserted'], 0)
                        history.assert_not_called()
                    self.assertEqual(self.current(self.root), 1)
                    self.assertEqual(self.ingest(self.root, path='document', connector_id=connector)['receipt'], first['receipt'])

    def test_mixed_batch_preserves_history_and_commits_live_member(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        deleted = self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')['envelope']
        result = self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[live, deleted])
        self.assertEqual(result['suppressed_filesystem_events'], 1)
        self.assertEqual(self.current(self.root), 1)
        self.assertEqual(self.current(self.reply), 1)

    def test_legacy_route_suppresses_before_raw_archive_in_both_compatibility_modes(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        for enabled in ('0', '1'):
            self.bridge.environment = {'RECALL_LEGACY_WRITES': enabled}
            with patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as upload:
                result = self.ingest(self.delete(self.root), path='legacy', connector_id='claude.jsonl')
                self.assertEqual(result['suppressed_filesystem_events'], 1)
                upload.assert_not_called()
            self.assertEqual(self.current(self.root), 1)

    def test_old_collector_http_gets_committed_audit_ack_without_document_receipt(self):
        from recall_server.app import Handler
        first = self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')['envelope']
        principal = dict(tenant_id=self.tenant, principal_id=self.principal, source_id=self.source, kind='collector')
        with patch.object(Handler, 'require', return_value=principal), \
             patch.object(Handler, 'legacy_ingest', return_value=self.bridge):
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                request = urllib.request.Request(f'http://127.0.0.1:{server.server_port}/v1/ingest/batches',
                    data=json.dumps({'events': [marker]}).encode(),
                    headers={'Content-Type': 'application/json', 'Idempotency-Key': 'obsolete-marker'})
                with urllib.request.urlopen(request, timeout=3) as response:
                    self.assertEqual(response.status, 201)
                    result = json.loads(response.read())
                self.assertEqual(result['status'], 'committed')
                self.assertEqual(result['inserted'], 0)
                self.assertEqual(result['suppressed_filesystem_events'], 1)
                self.assertTrue(result['receipts'][0].startswith('recall-audit://'))
                self.assertEqual(self.current(self.root), 1)
                self.assertEqual(self.ingest(self.root, path='document', connector_id='claude.jsonl')['receipt'], first['receipt'])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)

    def test_explicit_forget_still_deletes_and_denies_replay(self):
        for connector in ('claude.jsonl', 'codex.jsonl'):
            first = self.ingest(self.root, path='document', connector_id=connector, source=self.source+connector)
            request = dict(contract='recall.forget-request.v1',schema_version=1,tenant_id=self.tenant,
                principal_id=self.principal,source_id=self.source+connector,target_receipt=first['receipt'],
                mode='explicit_forget',reason='owner_requested',requested_at='2026-10-14T00:00:00Z',idempotency_key=connector)
            self.assertEqual(self.plane.forget(request)['status'], 'deleted')
            self.assertTrue(self.plane.forget(request)['replay'])
            self.assertEqual(self.current(self.root, source=self.source+connector), 0)
            with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'archive_identity_forgotten'):
                self.ingest(self.root, path='document', connector_id=connector, source=self.source+connector)

    def test_slack_explicit_deletion_remains_terminal(self):
        self.ingest(self.root, path='batch')
        self.ingest(self.delete(self.root), path='batch')
        self.assertEqual(self.current(self.root), 0)
        self.ingest(self.root, path='batch')
        self.assertEqual(self.current(self.root), 0)


if __name__ == '__main__':
    unittest.main()
