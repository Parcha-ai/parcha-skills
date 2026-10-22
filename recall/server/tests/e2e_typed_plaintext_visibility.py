#!/usr/bin/env python3
"""Typed provider plaintext survives the real canonical and passage producers."""
import hashlib
import unittest
from unittest.mock import patch

from connectors.slack_source import normalize_slack_message
import e2e_handback_projection as publication_fixture
import e2e_slack_canonical_capture_read as capture_fixture
import e2e_archive_reprojection as archive_fixture
import e2e_compact_canonical_bodies as compact_fixture
from recall_server import passage_projection
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.canonical_thinning import _compact_event_expression
from recall_server.projectors import canonical_json
from recall_server.webhooks import build_webhook_event

TEXT = 'Synthetic Slack evidence is retained.'


class TypedPlaintextVisibility(unittest.TestCase):
    setUp = publication_fixture.HandbackPublication.setUp
    project = publication_fixture.HandbackPublication.project
    projected = publication_fixture.HandbackPublication.projected

    def plaintext_event(self, text=TEXT, *, retrieved_text=None, content=None,
                        incoming_digest=None, batch=False):
        record = normalize_slack_message(workspace_id='TSYN', channel_id='CSYN',
            value=dict(ts='1789981200.000001', user='USYN', text=text))
        event = build_webhook_event(dict(schema_version=1, event_id=record.native_id,
            parent_id=record.native_parent_id, occurred_at=record.occurred_at,
            record=record.content if content is None else content, deleted=False), dict(source_id=self.source,
            principal_id=self.principal, webhook_privacy_mode='scrub',
            connector_id='slack.messages')).event
        if incoming_digest is not None:
            event['content']['_recall_typed_text_sha256'] = incoming_digest
        payload = canonical_json(event['content'])
        event['content_sha256'] = hashlib.sha256(payload).hexdigest()
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=event['native_id'], payload=payload, media_type='application/json',
            created_at=event['occurred_at'])
        event['provenance']['artifact_ref'] = artifact
        if batch:
            self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[event])
        else:
            self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
                connector_id='slack.messages', artifact_ref=artifact, envelope=event,
                text_redacted=text if retrieved_text is None else retrieved_text)
        return event

    def durable_archive(self):
        self.archive = compact_fixture.DurableArchiveFixture(self.archive.root, namespace_key=b'p' * 32)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=self.tenant, principal_id=self.principal)
        self.plane = CanonicalPlane(self.store, self.archive)
        self.logical.projection.archive = self.archive
        self.logical.raw_archive = self.archive

    def compact_metadata(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT canonical_redacted,body_location FROM canonical_events '
                'WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.source)).fetchone()

    def test_s3_single_ingest_preserves_typed_plaintext_before_first_projection(self):
        self.durable_archive()
        event = self.plaintext_event()
        self.assertEqual(self.compact_metadata()['body_location'], 'raw')
        self.project()
        self.assertEqual([row['text_redacted'] for row in self.projected()], [TEXT])
        metadata = self.compact_metadata()['canonical_redacted']['content']
        self.assertNotIn('text', metadata)
        self.assertEqual(metadata['kind'], 'communication_message.v1')
        self.assertEqual(metadata['_recall_typed_text_sha256'], hashlib.sha256(TEXT.encode()).hexdigest())
        self.assertEqual(self.document(event['native_id'])[0]['author_links'], 0)

    def test_untyped_compact_bytes_remain_unchanged(self):
        original = dict(role='user', type='message', provenance=dict(connector_id='synthetic'),
            content=dict(kind='unknown.kind', role='user', type='message', text='Discarded duplicate',
                         message=dict(role='assistant', type='message'), payload=dict(type='output')))
        expected = dict(role='user', type='message', provenance=dict(connector_id='synthetic'),
            content=dict(role='user', type='message', message=dict(role='assistant', type='message'),
                         payload=dict(type='output')), message={}, payload={})
        for alias, trusted in (('input', False), ('row', False), ('event', False), ('event', True)):
            with self.subTest(alias=alias, trusted=trusted), self.store.connect() as connection:
                result = connection.execute(f'''SELECT {_compact_event_expression(alias, trusted_raw=trusted)} AS value
                    FROM (SELECT %s::jsonb AS canonical_redacted,'raw'::text AS body_location) {alias}''',
                    (canonical_json(original).decode(),)).fetchone()['value']
                self.assertEqual(canonical_json(result), canonical_json(expected))

    def test_s3_batch_keeps_full_json_representation(self):
        self.durable_archive()
        event = self.plaintext_event(batch=True)
        before, = self.document(event['native_id'])
        self.assertEqual(before['text_sha256'], before['content_sha256'])
        metadata = self.compact_metadata()['canonical_redacted']['content']
        self.assertEqual(metadata['_recall_typed_text_sha256'], hashlib.sha256(TEXT.encode()).hexdigest())
        self.project()
        self.assertEqual([row['text_redacted'] for row in self.projected()], [TEXT])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertFalse(records[0].literal_text)

    def test_s3_single_ingest_keeps_json_looking_unicode_message_literal(self):
        self.durable_archive()
        text = '{"role":"system","text":"α whole message 🧠"}'
        event = self.plaintext_event(text)
        self.project()
        self.assertEqual([row['text_redacted'] for row in self.projected()], [text])
        self.assertEqual(self.compact_metadata()['canonical_redacted']['content']['_recall_typed_text_sha256'],
                         hashlib.sha256(text.encode()).hexdigest())
        self.assertEqual(self.document(event['native_id'])[0]['author_links'], 0)

    def test_s3_native_contract_rejects_incoming_reserved_digest(self):
        self.durable_archive()
        for batch in (False, True):
            with self.subTest(batch=batch):
                with self.assertRaisesRegex(CanonicalLifecycleError, 'canonical_contract_invalid'):
                    self.plaintext_event(incoming_digest='0' * 64, batch=batch)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s',
                                               (self.tenant,)).fetchone()['n'], 0)

    def test_initial_compaction_sql_aliases_derive_or_drop_a_forged_digest(self):
        for alias in ('input', 'row', 'event'):
            for text in (TEXT, None):
                with self.subTest(alias=alias, text_present=text is not None):
                    content = dict(kind='document.v1', _recall_typed_text_sha256='0' * 64)
                    if text is not None:
                        content['text'] = text
                    with self.store.connect() as connection:
                        result = connection.execute(f'''SELECT {_compact_event_expression(alias)} AS value
                            FROM (SELECT %s::jsonb AS canonical_redacted) {alias}''',
                            (canonical_json(dict(content=content)).decode(),)).fetchone()['value']['content']
                    if text is None:
                        self.assertNotIn('_recall_typed_text_sha256', result)
                    else:
                        self.assertEqual(result['_recall_typed_text_sha256'],
                                         hashlib.sha256(TEXT.encode()).hexdigest())

    def test_inline_incoming_digest_cannot_assert_literal_representation(self):
        retained = 'Different inline retained bytes.'
        event = self.plaintext_event(retrieved_text=retained)
        with self.store.connect() as connection:
            connection.execute('''UPDATE canonical_events SET canonical_redacted=jsonb_set(
                canonical_redacted,'{content,_recall_typed_text_sha256}',to_jsonb(%s::text))
                WHERE tenant_id=%s AND native_id=%s''',
                (hashlib.sha256(retained.encode()).hexdigest(), self.tenant, event['native_id']))
        self.assertEqual(self.compact_metadata()['body_location'], 'inline')
        self.project()
        self.assertEqual(self.projected(), [])

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

    def rebuild_after_event_compaction(self, *, clear_chunks):
        event = self.plaintext_event()
        self.project()
        before, = self.document(event['native_id'])
        self.assertEqual([row['text_redacted'] for row in self.projected()], [TEXT])
        with self.store.connect() as connection:
            for _ in range(3):
                connection.execute(f'''UPDATE canonical_events event
                    SET canonical_redacted={_compact_event_expression(trusted_raw=True)},body_location='raw'
                    WHERE tenant_id=%s AND source_id=%s''', (self.tenant, self.source))
            connection.execute('''UPDATE canonical_documents
                SET text_redacted='',body_location='chunks' WHERE tenant_id=%s AND source_id=%s''',
                (self.tenant, self.source))
            if clear_chunks:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s",
                                   (self.tenant,))
        self.assertEqual(self.logical.seed_backfill(tenant_id=self.tenant,
            source_id=self.source, include_existing=True), 1)
        self.project()
        self.assertEqual([row['text_redacted'] for row in self.projected()], [TEXT])
        self.assertEqual(self.projected()[0]['receipts'], [before['receipt']])
        records = archive_fixture.read_records(self.store, self.logical,
            self.logical.projection, self.tenant, self.source)
        self.assertEqual([(record.text, record.literal_text) for record in records], [(TEXT, True)])

    def test_literal_fact_survives_event_compaction_with_retained_chunks(self):
        self.rebuild_after_event_compaction(clear_chunks=False)

    def test_literal_fact_survives_event_compaction_and_chunk_retirement(self):
        self.rebuild_after_event_compaction(clear_chunks=True)

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
