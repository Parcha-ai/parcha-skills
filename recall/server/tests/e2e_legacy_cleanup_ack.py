"""Legacy cleanup ACKs record suppression while captured history stays current."""
import unittest
import tempfile
import threading
from pathlib import Path
from http.server import ThreadingHTTPServer
from contextlib import contextmanager
from unittest.mock import patch

import psycopg

import e2e_slack_deletion_ordering as fixture
from fixtures.legacy_collector_flush import flush as old_flush
from collector.collector import Collector
from recall_server.app import Handler
from recall_server.canonical import CanonicalArchiveGateway, REGISTRATION_CACHE


class LegacyCleanupAck(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    setUp = fixture.SlackDeletionOrdering.setUp
    ingest = fixture.SlackDeletionOrdering.ingest
    delete = fixture.SlackDeletionOrdering.delete
    current = fixture.SlackDeletionOrdering.current

    def credential(self):
        return dict(tenant_id=self.tenant, principal_id=self.principal, source_id=self.source)

    def marker(self):
        return self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')['envelope']

    def audit_count(self):
        with self.store.connect() as c:
            return c.execute("SELECT count(*) AS n FROM canonical_audit_events WHERE tenant_id=%s AND operation='ingest.filesystem_cleanup_suppressed'", (self.tenant,)).fetchone()['n']

    def test_mixed_batch_suppresses_cleanup_and_commits_following_live_record(self):
        original = self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')['envelope']
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        ack, replay = self.bridge.ingest('old-cleanup-mixed', [marker, live], principal={
            'tenant_id': self.tenant, 'principal_id': self.principal, 'source_id': self.source})
        self.assertEqual(ack['status'], 'committed')
        self.assertEqual(ack['inserted'], 1)
        self.assertEqual(ack['suppressed_filesystem_events'], 1)
        self.assertTrue(ack['receipts'][0].startswith('recall-audit://'))
        self.assertTrue(ack['receipts'][1].startswith('recall://'))
        self.assertFalse(replay)
        self.assertEqual(self.current(self.root), 1)
        self.assertEqual(self.current(self.reply), 1)
        self.assertEqual(self.ingest(self.root, path='document', connector_id='claude.jsonl')['receipt'], original['receipt'])
        repeated, replay = self.bridge.ingest('old-cleanup-mixed', [marker, live], principal={
            'tenant_id': self.tenant, 'principal_id': self.principal, 'source_id': self.source})
        self.assertTrue(replay)
        self.assertEqual(repeated['receipts'], ack['receipts'])
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 2)
            self.assertEqual(c.execute("SELECT count(*) AS n FROM canonical_audit_events WHERE tenant_id=%s AND operation='ingest.filesystem_cleanup_suppressed'", (self.tenant,)).fetchone()['n'], 1)

    def test_forgotten_cleanup_does_not_upload_or_revive_and_next_identity_commits(self):
        first = self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            **self.credential(), target_receipt=first['receipt'], mode='explicit_forget',
            reason='owner_requested', requested_at='2026-10-14T00:00:00Z', idempotency_key='forget'))
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        with patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as upload:
            ack, _ = self.bridge.ingest('forgotten-cleanup', [marker, live], principal=self.credential())
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(ack['suppressed_filesystem_events'], 1)
        self.assertEqual(self.current(self.root), 0)
        self.assertEqual(self.current(self.reply), 1)
        with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'archive_identity_forgotten'):
            self.ingest(self.root, path='document', connector_id='claude.jsonl')

    def test_scalar_and_homogeneous_batch_record_audit_and_artifact_forget_ownership(self):
        for connector in ('claude.jsonl', 'codex.jsonl'):
            first = self.ingest(self.root, path='document', connector_id=connector)
            for mode in ('document', 'batch'):
                ack = self.ingest(self.delete(self.root), path=mode, connector_id=connector)
                self.assertEqual(ack['inserted'], 0)
                self.assertEqual(ack['suppressed_filesystem_events'], 1)
                self.assertNotIn('revision', ack)
                receipt = ack.get('receipt', ack.get('receipts', [None])[0])
                self.assertTrue(receipt.startswith('recall-audit://'))
                self.assertEqual(self.current(self.root), 1)
            with self.store.connect() as c:
                self.assertGreater(c.execute('SELECT count(*) AS n FROM canonical_suppressed_artifact_owners WHERE tenant_id=%s', (self.tenant,)).fetchone()['n'], 0)
            self.assertTrue(first['receipt'].startswith('recall://'))

    def test_revoked_grant_mixed_batch_refuses_without_regrant_or_upload(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
        with patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as upload:
            with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'canonical_authority_forbidden'):
                self.bridge.ingest('revoked', [marker, live], principal=self.credential())
            upload.assert_not_called()
        self.assertEqual(self.audit_count(), 0)
        self.assertEqual(self.current(self.reply), 0)

    def test_changed_credential_scope_or_owner_cannot_ack(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        for field in ('source_id', 'principal_id'):
            with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'canonical_authority_forbidden'):
                self.bridge.ingest('wrong-scope', [marker], principal={**self.credential(), field: 'wrong'})
        self.assertEqual(self.audit_count(), 0)

    def test_malformed_batch_member_and_marker_refuse_without_audit(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        for invalid in (None, [], {**marker, 'content_sha256': '0'*64}):
            with self.assertRaises(ValueError):
                self.bridge.ingest('malformed', [invalid], principal=self.credential())
        self.assertEqual(self.audit_count(), 0)

    def test_authority_revoked_after_preflight_refuses_final_audit_and_live_mutation(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        original = CanonicalArchiveGateway.put_raw
        def revoke(gateway, **kwargs):
            result = original(gateway, **kwargs)
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
            return result
        with patch.object(CanonicalArchiveGateway, 'put_raw', revoke):
            with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'canonical_authority_forbidden'):
                self.bridge.ingest('revoke-during-upload', [marker, live], principal=self.credential())
        self.assertEqual(self.audit_count(), 0)
        self.assertEqual(self.current(self.reply), 0)

    def test_revoke_between_preflight_and_raw_gateway_cannot_be_recreated_on_cache_miss(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        original = CanonicalArchiveGateway.put_raw
        def revoke(gateway, **kwargs):
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
            REGISTRATION_CACHE.invalidate_tenant(self.tenant)
            return original(gateway, **kwargs)
        with patch.object(CanonicalArchiveGateway, 'put_raw', revoke), patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as upload:
            with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'canonical_authority_forbidden'):
                self.bridge.ingest('revoke-before-gateway', [marker, live], principal=self.credential())
            upload.assert_not_called()
        self.assertEqual(self.audit_count(), 0)
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT 1 FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone())

    def test_reversed_direct_mixed_batch_checks_marker_authority_before_ordinary_registration(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source))
        REGISTRATION_CACHE.invalidate_tenant(self.tenant)
        with self.assertRaisesRegex(fixture.CanonicalLifecycleError, 'canonical_authority_forbidden'):
            self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[live, marker])
        self.assertEqual(self.audit_count(), 0)
        self.assertEqual(self.current(self.reply), 0)

    def test_direct_suppressed_artifact_is_deleted_by_original_forget_owner(self):
        first = self.ingest(self.root, path='document', connector_id='claude.jsonl')
        prepared = self.ingest(self.delete(self.root), path='prepare', connector_id='claude.jsonl')
        reference = prepared['artifact_ref']
        self.plane.ingest_document(**prepared)
        self.assertTrue(self.archive.read_raw(reference))
        result = self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            **self.credential(), target_receipt=first['receipt'], mode='explicit_forget',
            reason='owner_requested', requested_at='2026-10-14T00:00:00Z', idempotency_key='forget-owned-marker'))
        self.assertEqual(result['status'], 'deleted')
        with self.assertRaises(fixture.ArchiveNotFound):
            self.archive.read_raw(reference)
        self.assertEqual(self.current(self.root), 0)

    def test_failure_after_audit_rolls_back_and_actual_commit_lost_ack_replays_once(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        original = self.plane.suppress_filesystem_cleanup
        touched = set()
        def recorded(connection, **kwargs):
            result = original(connection, **kwargs)
            touched.add(connection)
            return result
        def fail(connection, **kwargs):
            recorded(connection, **kwargs)
            raise RuntimeError('synthetic precommit failure')
        with patch.object(self.plane, 'suppress_filesystem_cleanup', fail):
            with self.assertRaises(RuntimeError):
                self.bridge.ingest('crash', [marker], principal=self.credential())
        self.assertEqual(self.audit_count(), 0)
        touched.clear()
        transaction = psycopg.Connection.transaction
        @contextmanager
        def lost_ack(connection, *args, **kwargs):
            with transaction(connection, *args, **kwargs) as tx:
                yield tx
            if connection in touched:
                touched.remove(connection)
                raise psycopg.OperationalError('synthetic lost COMMIT ACK')
        with patch.object(self.plane, 'suppress_filesystem_cleanup', recorded), patch.object(psycopg.Connection, 'transaction', lost_ack):
            with self.assertRaises(psycopg.OperationalError):
                self.bridge.ingest('crash', [marker], principal=self.credential())
        self.assertEqual(self.audit_count(), 1)
        ack, replay = self.bridge.ingest('crash', [marker], principal=self.credential())
        self.assertTrue(replay)
        self.assertEqual(ack['inserted'], 0)
        self.assertEqual(self.audit_count(), 1)
        self.assertEqual(self.current(self.root), 1)

    def test_frozen_old_collector_drains_cleanup_then_live_over_real_http(self):
        self.ingest(self.root, path='document', connector_id='claude.jsonl')
        marker = self.marker()
        live = self.ingest(self.reply, path='prepare', connector_id='claude.jsonl')['envelope']
        with tempfile.TemporaryDirectory() as temp, \
             patch.object(Handler, 'require', return_value={**self.credential(), 'kind': 'collector'}), \
             patch.object(Handler, 'legacy_ingest', return_value=self.bridge):
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            collector = Collector(root=Path(temp), harness='claude', source_id=self.source,
                principal_id=self.principal, spool_path=Path(temp)/'spool.db',
                endpoint=f'http://127.0.0.1:{server.server_port}', token='synthetic')
            try:
                for index, event in enumerate((marker, live)):
                    event['provenance']['byte_start'] = index
                    collector._queue(Path(temp)/'old-transcript.jsonl', event, index+1)
                collector.db.commit()
                outcome = old_flush(collector)
                self.assertEqual(outcome['errors'], 0)
                self.assertEqual(outcome['acked'], 2)
                self.assertEqual(outcome['batches'], 2)
                self.assertEqual(collector.pending_envelopes(), [])
                receipts = [r['receipt'] for r in collector.db.execute('SELECT receipt FROM outbox ORDER BY id')]
                self.assertTrue(receipts[0].startswith('recall-audit://'))
                self.assertTrue(receipts[1].startswith('recall://'))
                self.assertEqual(self.current(self.root), 1)
                self.assertEqual(self.current(self.reply), 1)
            finally:
                collector.close()
                server.shutdown()
                server.server_close()
                thread.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
