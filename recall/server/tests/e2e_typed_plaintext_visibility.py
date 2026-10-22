#!/usr/bin/env python3
"""Typed provider plaintext survives the real canonical and passage producers."""
import hashlib
import unittest
from unittest.mock import patch

from connectors.slack_source import normalize_slack_message
import e2e_handback_projection as publication_fixture
import e2e_slack_canonical_capture_read as capture_fixture
import e2e_archive_reprojection as archive_fixture
from recall_server import passage_projection
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json
from recall_server.webhooks import build_webhook_event

TEXT = 'Synthetic Slack evidence is retained.'


class TypedPlaintextVisibility(unittest.TestCase):
    setUp = publication_fixture.HandbackPublication.setUp
    project = publication_fixture.HandbackPublication.project
    projected = publication_fixture.HandbackPublication.projected

    def plaintext_event(self, text=TEXT, *, retrieved_text=None, content=None):
        record = normalize_slack_message(workspace_id='TSYN', channel_id='CSYN',
            value=dict(ts='1789981200.000001', user='USYN', text=text))
        event = build_webhook_event(dict(schema_version=1, event_id=record.native_id,
            parent_id=record.native_parent_id, occurred_at=record.occurred_at,
            record=record.content if content is None else content, deleted=False), dict(source_id=self.source,
            principal_id=self.principal, webhook_privacy_mode='scrub',
            connector_id='slack.messages')).event
        payload = canonical_json(event['content'])
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=event['native_id'], payload=payload, media_type='application/json',
            created_at=event['occurred_at'])
        event['provenance']['artifact_ref'] = artifact
        self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id='slack.messages', artifact_ref=artifact, envelope=event,
            text_redacted=text if retrieved_text is None else retrieved_text)
        return event

    def document(self, native):
        with self.store.connect() as connection:
            return connection.execute('''SELECT document.document_id,document.text_sha256,
                event.content_sha256,document.revision,chunk.text_redacted,chunk.receipt,
                (SELECT count(*) FROM canonical_event_actors actor
                 WHERE actor.tenant_id=event.tenant_id AND actor.source_id=event.source_id
                   AND actor.event_id=event.event_id AND actor.relation='author') AS author_links
                FROM canonical_documents document JOIN canonical_events event
                  USING(tenant_id,source_id,event_id)
                JOIN canonical_chunks chunk USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=%s
                  AND document.native_id=%s AND document.is_current
                  AND document.deleted_at IS NULL ORDER BY chunk.ordinal''',
                (self.tenant, self.source, native)).fetchall()

    def test_supported_plaintext_typed_message_has_exact_search_passage(self):
        event = self.plaintext_event()
        before, = self.document(event['native_id'])
        self.assertEqual(before['text_redacted'], TEXT)
        self.assertEqual(before['text_sha256'], hashlib.sha256(TEXT.encode()).hexdigest())
        self.assertNotEqual(before['text_sha256'], before['content_sha256'])
        self.assertEqual(before['author_links'], 0)
        self.project()
        passages = self.projected()
        self.assertEqual([row['text_redacted'] for row in passages], [TEXT])
        self.assertEqual(passages[0]['receipts'], [before['receipt']])
        self.assertEqual(self.document(event['native_id']), [before])

    def test_typed_document_plaintext_has_exact_passage_without_an_actor(self):
        event = self.plaintext_event(content=dict(kind='document.v1', document_id='synthetic-doc',
            name='Synthetic document', mime_type='text/plain', content_fidelity='complete', text=TEXT))
        self.project()
        self.assertEqual([row['text_redacted'] for row in self.projected()], [TEXT])
        self.assertEqual(self.document(event['native_id'])[0]['author_links'], 0)

    def test_typed_display_text_must_match_the_current_document_hash(self):
        text = 'Different retained bytes are not typed display text.'
        event = self.plaintext_event(retrieved_text=text)
        before, = self.document(event['native_id'])
        self.project()
        self.assertEqual(self.projected(), [])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertEqual([record.text for record in records], [text])
        self.assertFalse(records[0].literal_text)
        self.assertEqual(self.document(event['native_id']), [before])

    def test_segmented_literal_provider_text_replays_exactly(self):
        text = '{"type":"token_count","role":"system","text":"α whole message 🧠"}'
        event = self.plaintext_event(text)
        before, = self.document(event['native_id'])
        with patch('recall_server.logical_evidence_projection.TEXT_SEGMENT_BYTES', 16):
            self.project()
        rows = self.projected()
        self.assertEqual([row['text_redacted'] for row in rows], [text])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertGreater(len(records), 1)
        self.assertTrue(all(record.literal_text and not record.roles and not record.actor_links
                            for record in records))
        self.assertEqual(''.join(record.text for record in records), text)
        self.assertEqual(records[0].receipts, (before['receipt'],))
        self.assertTrue(all(not record.receipts for record in records[1:]))

    def test_source_backfill_rebuilds_old_plaintext_after_inline_retirement(self):
        event = self.plaintext_event()
        before, = self.document(event['native_id'])
        original = CanonicalLogicalEvidenceProjector._record_stream

        def previous_representation(projector, cursor, **kwargs):
            rows = (dict(row, literal_text=False) for row in cursor)
            yield from original(projector, rows, **kwargs)

        with patch.object(CanonicalLogicalEvidenceProjector, '_record_stream', previous_representation):
            self.project()
        self.assertEqual(self.projected(), [])
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_chunks SET text_redacted=\'\' WHERE tenant_id=%s',
                               (self.tenant,))
        self.logical.append_enabled = True
        self.assertEqual(self.logical.seed_backfill(tenant_id=self.tenant,
            source_id=self.source, include_existing=True), 1)
        self.project()
        rows = self.projected()
        self.assertEqual([row['text_redacted'] for row in rows], [TEXT])
        self.assertEqual(rows[0]['receipts'], [before['receipt']])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertEqual([(record.text, record.literal_text) for record in records], [(TEXT, True)])

    def test_current_signed_callback_json_representation_still_has_exact_passage(self):
        event = capture_fixture.CanonicalCapturedRead.callback(self, TEXT)
        before, = self.document(event['native_id'])
        self.assertEqual(before['text_sha256'], before['content_sha256'])
        self.assertEqual(before['author_links'], 0)
        self.project()
        passages = self.projected()
        self.assertEqual([row['text_redacted'] for row in passages], [TEXT])
        self.assertEqual(passages[0]['receipts'], [before['receipt']])
        self.assertEqual(self.document(event['native_id']), [before])

    def test_json_inside_typed_plaintext_remains_literal_through_archive_and_spans(self):
        text = '{"role":"system","text":"This nested field is only part of the Slack message."}'
        event = self.plaintext_event(text)
        before, = self.document(event['native_id'])
        self.assertEqual(before['text_redacted'], text)
        self.assertEqual(before['author_links'], 0)
        self.project()
        rows = self.projected()
        self.assertEqual([row['text_redacted'] for row in rows], [text])
        self.assertEqual(rows[0]['receipts'], [before['receipt']])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertEqual([record.text for record in records], [text])
        messages = passage_projection.visible_messages(records)
        replay = passage_projection.build_passages(tenant_id=self.tenant,
            source_id=self.source, logical_document_id=rows[0]['logical_document_id'],
            revision=rows[0]['revision'], messages=messages,
            policy=passage_projection.DEFAULT_PASSAGE_POLICY)
        self.assertEqual([passage.passage_id for passage in replay],
                         [row['passage_id'] for row in rows])
        self.assertEqual(passage_projection.reconstruct_passage(replay[0], messages), text)
        self.assertEqual(self.document(event['native_id']), [before])


if __name__ == '__main__':
    unittest.main(verbosity=2)
