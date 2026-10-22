#!/usr/bin/env python3
"""Accepted Slack callbacks remain readable without rebuilding their raw input."""
import hashlib
import hmac
import json
import time
import unittest
import uuid
from unittest.mock import patch

from e2e_managed_slack_identity import ManagedSlackIdentity
from connectors.slack_events import slack_event_to_webhook
from recall_server import slack_captured_attribution as captured
from recall_server.control import ControlError
from recall_server.canonical import CanonicalLifecycleError
from recall_server.legacy_plane import LegacyIngestBridge
from recall_server.webhooks import build_webhook_event


class CanonicalCapturedRead(ManagedSlackIdentity):
    def callback(self, text):
        raw = json.dumps(dict(type='event_callback', event_id='EvSynthetic', team_id='T123',
            event=dict(type='message', channel='C123', channel_type='channel', user='U123',
                       text=text, ts='1789981201.000001'))).encode()
        timestamp = str(int(time.time()))
        secret = 'synthetic-signing-secret'
        signature = 'v0=' + hmac.new(secret.encode(), b'v0:' + timestamp.encode() + b':' + raw,
                                    hashlib.sha256).hexdigest()
        adapted = slack_event_to_webhook(body=raw, timestamp=timestamp,
            signature=signature, signing_secret=secret)
        prepared = build_webhook_event(adapted.webhook, dict(source_id=self.source,
            principal_id=self.principal, webhook_privacy_mode='scrub', connector_id='slack.messages'))
        acknowledgement, _ = LegacyIngestBridge(self.store, self.plane, self.archive).ingest(
            'synthetic-webhook-' + uuid.uuid4().hex, [prepared.event],
            principal=dict(tenant_id=self.tenant, principal_id=self.principal, source_id=self.source),
            raw_payload=raw, notification=True)
        self.assertEqual(acknowledgement['inserted'], 1)
        return prepared.event

    def captured_callback(self):
        worker = self.managed()
        self.assertEqual(self.step(worker)['failed'], 0)
        event = self.callback('Synthetic webhook question ' + self.email)
        with self.store.connect() as connection:
            connection.execute('UPDATE connector_installations SET run_after=now() WHERE id=%s',
                               (self.installation,))
        row = worker._claim()
        self.assertIsNotNone(row)
        selected = captured._page(worker, row, '', None, 1, native_ids=[event['native_id']])[0]
        self.assertEqual(selected['body_location'], 'inline')
        return worker, row, selected, event

    def snapshot(self):
        with self.store.connect() as connection:
            installation = connection.execute('SELECT to_jsonb(i) AS v FROM connector_installations i WHERE id=%s',
                                              (self.installation,)).fetchone()['v']
            events = connection.execute('SELECT event_id,content_sha256,canonical_redacted FROM canonical_events '
                'WHERE tenant_id=%s AND source_id=%s ORDER BY event_id', (self.tenant, self.source)).fetchall()
        return hashlib.sha256(json.dumps([installation, events, self.counts()], sort_keys=True,
                                         default=str).encode()).hexdigest()

    def test_signed_callback_reuses_accepted_content_without_raw_reparse(self):
        worker, row, selected, event = self.captured_callback()
        before = self.snapshot()
        with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must_not_read')) as read:
            restored = captured._record(worker, row, selected, time.monotonic() + 10)
        read.assert_not_called()
        self.assertEqual(restored['native_id'], event['native_id'])
        self.assertEqual(restored['content_sha256'], event['content_sha256'])
        self.assertEqual(restored['content'], event['content'])
        self.assertNotIn(self.email, json.dumps(restored))
        self.assertEqual(self.snapshot(), before)

    def test_revised_current_event_refuses_the_old_nomination(self):
        worker, row, selected, _ = self.captured_callback()
        self.callback('Synthetic revised webhook question')
        before = self.snapshot()
        with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must_not_read')) as read:
            with self.assertRaisesRegex(ControlError, 'connector_record_changed'):
                captured._record(worker, row, selected, time.monotonic() + 10)
        read.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_revoked_source_refuses_canonical_content(self):
        worker, row, selected, _ = self.captured_callback()
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s '
                               'AND principal_id=%s', (self.tenant, self.source, self.principal))
        before = self.snapshot()
        with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must_not_read')) as read:
            with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_authority_forbidden'):
                captured._record(worker, row, selected, time.monotonic() + 10)
        read.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_forget_after_nomination_never_returns_or_recreates_content(self):
        worker, row, selected, event = self.captured_callback()
        with self.store.connect() as connection:
            receipt = connection.execute('SELECT receipt FROM canonical_chunks chunk JOIN canonical_documents '
                'document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s '
                'AND document.source_id=%s AND document.native_id=%s',
                (self.tenant, self.source, event['native_id'])).fetchone()['receipt']
        self.plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-22T03:00:00Z', idempotency_key=uuid.uuid4().hex))
        before = self.snapshot()
        with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must_not_read')) as read:
            self.assertIsNone(captured._record(worker, row, selected, time.monotonic() + 10))
        read.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_contacts_keep_native_sdk_parsing_and_the_raw_byte_bound(self):
        worker, row, _, _ = self.captured_callback()
        selected = captured._page(worker, row, '', None, 1,
                                  native_ids=[self.contacts[0].native_id])[0]
        before = self.snapshot()
        with patch.object(worker.archive, 'read_raw', wraps=worker.archive.read_raw) as read:
            record = captured._record(worker, row, selected, time.monotonic() + 10,
                contacts_only=True, archive_budget=captured._CapturedArchive(
                    worker.archive, max_bytes=selected['size_bytes']))
        read.assert_called_once()
        self.assertEqual(record.schema_version, 2)
        self.assertEqual(record.native_id, self.contacts[0].native_id)
        with patch.object(worker.archive, 'read_raw', side_effect=AssertionError('must_not_read')) as read:
            with self.assertRaisesRegex(ValueError, 'captured_archive_unavailable'):
                captured._record(worker, row, selected, time.monotonic() + 10,
                    contacts_only=True, archive_budget=captured._CapturedArchive(
                        worker.archive, max_bytes=selected['size_bytes'] - 1))
        read.assert_not_called()
        self.assertEqual(self.snapshot(), before)


if __name__ == '__main__':
    unittest.main(defaultTest=[
        'CanonicalCapturedRead.test_signed_callback_reuses_accepted_content_without_raw_reparse',
        'CanonicalCapturedRead.test_revised_current_event_refuses_the_old_nomination',
        'CanonicalCapturedRead.test_revoked_source_refuses_canonical_content',
        'CanonicalCapturedRead.test_forget_after_nomination_never_returns_or_recreates_content',
        'CanonicalCapturedRead.test_contacts_keep_native_sdk_parsing_and_the_raw_byte_bound'], verbosity=2)
