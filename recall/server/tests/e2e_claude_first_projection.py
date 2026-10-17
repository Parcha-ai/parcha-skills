#!/usr/bin/env python3
"""New Claude parents publish typed tools through the existing projector."""
import json
import unittest
from dataclasses import replace
from unittest.mock import patch

import psycopg

import e2e_tool_group_publication as fixture
from recall_server import passage_index


class ClaudeFirstProjection(unittest.TestCase):
    ingest = fixture.ToolGroupPublication.ingest
    project = fixture.ToolGroupPublication.project
    projected = fixture.ToolGroupPublication.projected
    document = fixture.ToolGroupPublication.document
    grant_and_open = fixture.ToolGroupPublication.grant_and_open
    enroll = fixture.ToolGroupPublication.enroll

    def setUp(self):
        fixture.ToolGroupPublication.setUp(self)
        self.source = 'claude:' + self.source

    def test_first_projection_opens_input_and_result_without_enrollment(self):
        originals = [self.ingest('input'), self.ingest('result', result=True, second=1)]
        self.project()
        tools = [row for row in self.projected() if 'claude.tool-' in json.dumps(row['spans'])]
        self.assertEqual(len(tools), 2)
        document = self.document()
        self.assertEqual(document['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(document['applied_extraction_version'], 'tool_groups_v1')
        for tool in tools:
            opened = self.grant_and_open(tool)
            self.assertTrue(opened['complete'])
            raw = opened['chunks'][0]['text']
            self.assertIn(raw, originals)
            for span in tool['spans']:
                leaf, _ = fixture.resolve_claude_tool_source_unit(raw, span['source_unit'])
                self.assertEqual(leaf.encode()[span['source_byte_start']:span['source_byte_end']],
                    tool['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
        self.assertEqual(self.project()['documents'], 0)

    def prepare_first(self):
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
            max_batches=1, upload_concurrency=1)
        return self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0])

    def test_failed_first_commit_retries_without_partial_version_or_passages(self):
        self.ingest('input')
        prepared = self.prepare_first()
        with self.store.connect() as connection:
            connection.execute("""CREATE FUNCTION reject_first_projection() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture first commit failure'; END $$""")
            connection.execute("""CREATE TRIGGER reject_first_projection BEFORE INSERT
                ON canonical_passage_documents FOR EACH ROW EXECUTE FUNCTION reject_first_projection()""")
        try:
            with self.assertRaises(psycopg.Error):
                self.passages._commit(prepared)
            self.assertIsNone(self.document())
            self.assertEqual(self.projected(), [])
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_first_projection ON canonical_passage_documents')
                connection.execute('DROP FUNCTION reject_first_projection()')
        self.project()
        self.assertEqual(self.document()['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.project()['documents'], 0)

    def test_existing_legacy_stays_legacy_until_explicit_request(self):
        self.ingest('input')
        # Reproduce a document created by the previous writer. The actual
        # legacy preparation and transaction run; only its old default differs.
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            self.project()
        before = self.projected()
        self.ingest('result', result=True, second=1)
        self.project()
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertFalse(any('claude.tool-' in json.dumps(p['spans']) for p in self.projected()))
        self.assertTrue(all(any(p['text_redacted'] in current['text_redacted']
                                for current in self.projected()) for p in before))
        self.assertEqual(self.enroll()['status'], 'queued')
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertEqual(sum('claude.tool-' in json.dumps(p['spans']) for p in self.projected()), 2)

    def test_normal_revision_preserves_default_and_rejects_legacy_preparation(self):
        self.ingest('input')
        self.project()
        before = {p['passage_id'] for p in self.projected() if 'claude.tool-' in json.dumps(p['spans'])}
        self.ingest('result', result=True, second=1)
        prepared = self.prepare_first()
        legacy = self.passages._prepare(replace(prepared.candidate, extraction_version='legacy'))
        self.assertEqual(self.passages._commit(legacy)['status'], 'stale')
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        self.assertEqual(self.document()['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertTrue(before <= {p['passage_id'] for p in self.projected()})
        self.assertIsNone(self.document()['append_checkpoint'])

    def test_pending_forget_refuses_first_projection(self):
        self.ingest('input')
        prepared = self.prepare_first()
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_evidence_document_queue(
                tenant_id,source_id,native_parent_id,reason) VALUES (%s,%s,'session-one','forget')""",
                (self.tenant, self.source))
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertIsNone(self.document())
        self.assertEqual(self.projected(), [])

    def test_codex_first_projection_keeps_legacy_and_raw_tool_event(self):
        self.source = self.source.replace('claude:', 'codex:', 1)
        raw = self.ingest('input', value={'type': 'response_item', 'payload': {
            'type': 'function_call', 'name': 'shell', 'arguments': '{"command":"echo Sapphire"}'}})
        self.project()
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertIsNone(self.document()['extraction_coverage'])
        self.assertEqual(self.projected(), [])
        # Raw capture has not been relabeled as a supported typed tool unit.
        with self.store.connect() as connection:
            receipt = connection.execute('SELECT receipt FROM canonical_chunks WHERE tenant_id=%s',
                                         (self.tenant,)).fetchone()['receipt']
        self.ingest('visible', second=1, value={'type': 'response_item', 'payload': {
            'type': 'message', 'role': 'user', 'content': [{'type': 'input_text', 'text': 'Sapphire context'}]}})
        self.project()
        self.grant_and_open(self.projected()[0])
        reader = fixture.BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,))
        opened = reader.show(receipt)
        self.assertIn(raw, [chunk['text'] for chunk in opened['chunks']])


if __name__ == '__main__':
    unittest.main(verbosity=2)
