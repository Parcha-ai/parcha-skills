#!/usr/bin/env python3
"""Compacted visible history through native projection, append and authority."""
import json
import unittest
from unittest.mock import patch

import e2e_codex_tool_publication as fixture
from contracts.source_units import resolve_codex_history_source_unit
from recall_server import passage_append, passage_projection
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.passage_projection import PassagePolicy, canonical_spans_json


def history():
    def message(role, text, identity):
        return dict(type='message', role=role, id=identity,
                    internal_chat_message_metadata_passthrough={'turn_id': 'turn-' + identity},
                    content=[dict(type='input_text' if role == 'user' else 'output_text', text=text)])
    return dict(type='compacted', payload=dict(message='', replacement_history=[
        message('user', 'Keep the café window closed until the inspection ends.', 'human'),
        dict(type='message', role='developer', content=[dict(type='input_text', text='PRIVATE INSTRUCTIONS')]),
        message('assistant', 'The window stays closed; no inspection completion is claimed. 🪟', 'reply'),
        dict(type='compaction', encrypted_content='OPAQUE SYNTHETIC BYTES'),
    ]))


class CodexCompactedHistory(unittest.TestCase):
    setUp = fixture.CodexToolPublication.setUp
    ingest = fixture.CodexToolPublication.ingest
    project = fixture.CodexToolPublication.project
    projected = fixture.CodexToolPublication.projected
    document = fixture.CodexToolPublication.document
    grant_and_open = fixture.CodexToolPublication.grant_and_open
    prepare = fixture.CodexToolPublication.prepare

    def test_native_capture_kind_publishes_exact_history_and_refuses_revoked_reader(self):
        raw = self.ingest('history', value=history())
        self.project()
        rows = self.projected()
        self.assertTrue(rows, 'native transcript_record compaction emitted no visible history')
        self.assertEqual(self.document()['applied_extraction_version'], fixture.VERSION)
        for row in rows:
            self.assertNotIn('PRIVATE INSTRUCTIONS', row['text_redacted'])
            self.assertNotIn('OPAQUE SYNTHETIC BYTES', row['text_redacted'])
            self.assertIn(raw, [chunk['text'] for chunk in self.grant_and_open(row)['chunks']])
            for span in row['spans']:
                text, _, origin = resolve_codex_history_source_unit(raw, span['source_unit'])
                self.assertEqual(text.encode()[span['source_byte_start']:span['source_byte_end']],
                                 row['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
                self.assertEqual(span['origin'], {key: origin[key] for key in
                    ('message_id', 'turn_id', 'role', 'text_sha256')})
        target = {key: rows[0][key] for key in
                  ('source_id', 'logical_document_id', 'revision', 'manifest_content_sha256', 'passage_id')}
        reader = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,))
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s', (self.tenant,))
        with self.assertRaises(ValueError):
            reader.show(target)

    def test_old_epoch_requeues_once_without_changing_extraction_mode(self):
        self.ingest('history', value=history())
        with patch.object(passage_projection, 'iter_codex_history_source_units', return_value=iter(())), \
             patch.object(passage_append, 'CODEX_CHECKPOINT_VERSION', 5):
            self.project()
        self.assertEqual(self.projected(), [])
        self.assertEqual(self.document()['append_checkpoint']['version'], 5)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.project()
        self.assertTrue(self.projected())
        self.assertEqual(self.document()['applied_extraction_version'], fixture.VERSION)
        self.assertEqual(self.document()['append_checkpoint']['version'], 6)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_native_append_matches_full_oracle_across_history_windows(self):
        self.passages.policy = PassagePolicy(8, 2)
        self.ingest('before', value=dict(type='response_item', payload=dict(type='message', role='user',
            content=[dict(type='input_text', text='An earlier ordinary message starts this window.')])))
        self.ingest('history-one', second=1, value=history())
        self.ingest('history-two', second=2, value=history())
        self.project()
        self.assertGreater(len(self.projected()), 2)
        self.ingest('after', second=3, value=dict(type='response_item', payload=dict(type='message', role='user',
            content=[dict(type='input_text', text='A new ordinary message follows both captured history records.')])))
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        actual = self.passages._prepare(candidate, incremental=True)
        self.addCleanup(actual.close)
        oracle = self.passages._prepare(candidate, incremental=False)
        self.addCleanup(oracle.close)
        self.assertIsNotNone(actual.previous_checkpoint)
        offset = actual.previous_checkpoint['passage_ordinal']
        self.assertEqual(tuple(actual.passages[offset:]), tuple(oracle.passages[offset:]))
        self.assertEqual([(p.passage_id, p.ordinal, p.receipts) for p in actual.passages],
                         [(p.passage_id, p.ordinal, p.receipts) for p in oracle.passages])
        self.assertEqual((actual.dense_message_count, actual.dense_message_bytes),
                         (oracle.dense_message_count, oracle.dense_message_bytes))
        self.assertEqual(self.passages._commit(actual)['status'], 'committed')
        self.assertEqual([row['spans'] for row in self.projected()],
                         [json.loads(canonical_spans_json(p.spans)) for p in oracle.passages])

    def test_pending_forget_cannot_commit_prepared_history(self):
        self.ingest('history', value=history())
        prepared = self.prepare()
        self.addCleanup(prepared.close)
        self.assertTrue(prepared.passages)
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_evidence_document_queue
                (tenant_id,source_id,native_parent_id,reason) VALUES(%s,%s,'session-one','forget')""",
                (self.tenant, self.source))
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertEqual(self.projected(), [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
