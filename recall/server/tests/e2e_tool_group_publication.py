#!/usr/bin/env python3
"""Document-scoped tool extraction through the native publisher and authority."""
import hashlib
import json
import unittest
from dataclasses import replace

import e2e_passage_origin as fixture
from contracts.source_units import resolve_claude_tool_source_unit
from recall_server.passage_retrieval import rerank_document
from recall_server.projectors import canonical_json
from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.control import ControlError


class ToolGroupPublication(unittest.TestCase):
    setUp = fixture.PassageOrigin.setUp
    project = fixture.PassageOrigin.project
    projected = fixture.PassageOrigin.projected
    grant_and_open = fixture.PassageOrigin.grant_and_open

    def ingest(self, native, *, result=False, second=0, value=None, parent='session-one'):
        role = 'user' if result else 'assistant'
        if value is None:
            block = ({'type': 'tool_result', 'tool_use_id': 'call-one',
                      'content': 'Sapphire checks failed: the scoped secret remains in the POC.'}
                     if result else {'type': 'tool_use', 'id': 'call-one', 'name': 'SendMessage',
                      'input': {'message': 'Sapphire fix is verified locally; deployment remains untested.',
                                'options': [{'message': 'Approve deployment'}, {'message': 'Hold off'}]}})
            value = {'type': role, 'message': {'role': role, 'content': [
                {'type': 'text', 'text': 'Visible Sapphire context stays unchanged.'}, block]}}
        payload = canonical_json(value)
        occurred = f'2026-10-01T12:00:{second:02}Z'
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=native, payload=payload, media_type='application/json', created_at=occurred)
        envelope = dict(schema_version=1, source_id=self.source, native_id=native,
            native_parent_id=parent, kind='transcript_record', occurred_at=occurred,
            observed_at=occurred, principal_id=self.principal, visibility='private',
            content_type='application/json', content=value,
            provenance=dict(connector_id='synthetic.tool-group', connector_schema_version=1,
                            artifact_ref=artifact), content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id='synthetic.tool-group', artifact_ref=artifact, envelope=envelope,
            text_redacted=payload.decode())
        return payload.decode()

    def document(self):
        with self.store.connect() as connection:
            return connection.execute("""SELECT projected.* FROM canonical_passage_documents projected
                JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,logical_document_id)
                WHERE tenant_id=%s AND evidence.native_parent_id='session-one'""",
                                      (self.tenant,)).fetchone()

    def enroll(self, **extra):
        row = self.document()
        return self.passages.request_extraction_version(source_id=self.source,
            logical_document_id=row['logical_document_id'], revision=row['revision'],
            source_document_sha256=row['source_document_sha256'],
            extraction_version='tool_groups_v1', principal_id=self.principal, **extra)

    def test_enrollment_keeps_one_complete_handback_owner(self):
        report = 'The checked schema permission caused the deploy failure.'
        value = {'type': 'assistant', 'message': {'role': 'assistant', 'content': [
            {'type': 'tool_use', 'name': 'SubagentHandback', 'input': {'message': report}}]}}
        original = self.ingest('handback', value=value)
        self.project()
        before = self.projected()
        self.assertEqual(len(before), 1)
        self.assertEqual(before[0]['spans'][0]['source_unit'], 'subagent_handback.complete.v1')
        self.assertEqual(self.enroll()['status'], 'queued')
        self.project()
        after = self.projected()
        self.assertEqual(len(after), 1)
        for field in ('passage_id', 'text_sha256', 'receipts', 'spans'):
            self.assertEqual(after[0][field], before[0][field])
        self.assertIn(original, [chunk['text'] for chunk in self.grant_and_open(after[0])['chunks']])

    def test_long_handback_keeps_every_original_window_and_byte(self):
        report = 'Verified café finding and remaining action. ' * 600
        self.ingest('long-handback', value={'type': 'assistant', 'message': {
            'role': 'assistant', 'content': [{'type': 'tool_use', 'name': 'SubagentHandback',
                'input': {'message': report}}]}})
        self.project()
        before = self.projected()
        self.assertGreater(len(before), 1)
        self.enroll()
        self.project()
        after = self.projected()
        self.assertEqual(len(after), len(before))
        for old, current in zip(before, after):
            for field in ('passage_id', 'text_sha256', 'receipts', 'spans', 'text_redacted'):
                self.assertEqual(current[field], old[field])
        covered = set()
        for row in after:
            for span in row['spans']:
                self.assertEqual(report.encode()[span['source_byte_start']:span['source_byte_end']],
                    row['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
                covered.update(range(span['source_byte_start'], span['source_byte_end']))
        self.assertEqual(len(covered), len(report.encode()))
        self.assertEqual(self.document()['extraction_coverage']['dispositions']['already_projected_handback'], 1)

    def test_handback_with_preamble_keeps_generic_fallback(self):
        report = 'The tool report is not present in the visible preamble.'
        raw = self.ingest('preamble', value={'type': 'assistant', 'message': {
            'role': 'assistant', 'content': [{'type': 'text', 'text': 'Separate visible context.'},
                {'type': 'tool_use', 'name': 'SubagentHandback', 'input': {'message': report}}]}})
        self.project()
        before = self.projected()
        self.enroll()
        self.project()
        after = self.projected()
        self.assertTrue({row['passage_id'] for row in before} <= {row['passage_id'] for row in after})
        tool = next(row for row in after if 'claude.tool-input' in json.dumps(row['spans']))
        self.assertIn(report, tool['text_redacted'])
        self.assertIn(raw, [chunk['text'] for chunk in self.grant_and_open(tool)['chunks']])
        self.assertNotIn('already_projected_handback', self.document()['extraction_coverage']['dispositions'])

    def test_extra_input_and_unrelated_equal_text_are_not_owned(self):
        report = 'Exact equal text does not establish a shared semantic owner.'
        raw = self.ingest('mixed', value={'type': 'assistant', 'message': {
            'role': 'assistant', 'content': [
                {'type': 'tool_use', 'name': 'SubagentHandback', 'input': {'message': report}},
                {'type': 'tool_use', 'name': 'SubagentHandback', 'input': {'message': report, 'attempt': 2}},
                {'type': 'tool_use', 'name': 'SubagentHandback', 'input': {'message': report, 'detail': 'Preserve the other leaf.'}},
                {'type': 'tool_use', 'name': 'SendMessage', 'input': {'message': report}}]}})
        self.project()
        before = self.projected()
        self.enroll()
        self.project()
        after = self.projected()
        self.assertTrue({row['passage_id'] for row in before} <= {row['passage_id'] for row in after})
        units = {span.get('source_unit') for row in after for span in row['spans']}
        self.assertNotIn('claude.tool-input.string.v1#/message/content/0/input/message', units)
        for index in (1, 2, 3):
            self.assertIn(f'claude.tool-input.string.v1#/message/content/{index}/input/message', units)
        self.assertIn('claude.tool-input.string.v1#/message/content/2/input/detail', units)
        for row in after:
            self.assertIn(raw, [chunk['text'] for chunk in self.grant_and_open(row)['chunks']])
        coverage = self.document()['extraction_coverage']
        self.assertEqual(coverage['groups'], 3)
        self.assertEqual(coverage['included_string_leaves'], 4)
        self.assertEqual(coverage['dispositions']['already_projected_handback'], 1)

    def test_explicit_enrollment_recovers_inputs_results_and_keeps_legacy_identity(self):
        self.store.search_plane = 'turbopuffer'
        original = [self.ingest('input'), self.ingest('result', result=True, second=1)]
        self.project(); before = self.projected()
        self.assertFalse(any('tool-' in json.dumps(p['spans']) for p in before))
        self.assertEqual(self.enroll()['status'], 'queued')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        report = self.project(); after = self.projected()
        self.assertEqual(report['inserted'], 2)
        self.assertTrue({p['passage_id'] for p in before} <= {p['passage_id'] for p in after})
        self.assertEqual(self.document()['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertIsNone(self.document()['append_checkpoint'])
        for prior in before:
            current = next(p for p in after if p['passage_id'] == prior['passage_id'])
            for field in ('text_redacted', 'text_sha256', 'embed_sha256', 'spans'):
                self.assertEqual(current[field], prior[field])
        tools = [p for p in after if p['spans'][0].get('source_unit', '').startswith('claude.tool-')]
        self.assertEqual(len(tools), 2)
        for passage in tools:
            opened = self.grant_and_open(passage)
            raw = opened['chunks'][0]['text']
            self.assertIn(raw, original)
            for span in passage['spans']:
                leaf, kind = resolve_claude_tool_source_unit(raw, span['source_unit'])
                self.assertEqual(leaf.encode()[span['source_byte_start']:span['source_byte_end']],
                    passage['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
                self.assertEqual(span['origin']['evidence_kind'], kind)
            ranked = rerank_document(passage, ['Sapphire'], 4096)
            self.assertIn('Tool input:' if 'tool-input' in passage['spans'][0]['source_unit'] else 'Tool result:', ranked)
            self.assertIn('does not prove execution, selection, or human approval.' if 'tool-input' in passage['spans'][0]['source_unit'] else 'reported output, not verified fact or human speech.', ranked)
            self.assertIn('Open source for context.', ranked)
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        self.assertEqual(set(namespace.rows), {p['passage_id'] for p in after})
        result = self.retrieval.search('Sapphire', lexical_query='Sapphire', since=None, until=None, limit=10)
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        ranges = result['results'][0]['matching_ranges']
        self.assertEqual(sum('evidence_context' in item for item in ranges), 2)
        from recall_server.mcp import omit_search_spans
        compact = omit_search_spans(result)
        self.assertEqual(sum('evidence_context' in item for item in compact['results'][0]['matching_ranges']), 2)
        parity = self.passages.shadow_diff(tenant_id=self.tenant, source_id=self.source)
        self.assertEqual(parity['documents'][0]['status'], 'compared')
        self.assertEqual(parity['documents'][0]['passages_recomputed'], len(after))
        from evals.passage_index_audit import _exact_sample, _span_coverage
        self.assertEqual(_exact_sample(self.store, self.passages.logical_projection, after,
            policy=self.passages.policy, tenant_id=self.tenant), len(after))
        self.assertEqual(_span_coverage([self.document()], after)['uncovered_bytes'], 0)
        self.assertEqual(self.enroll()['status'], 'current')
        self.assertEqual(self.project()['documents'], 0)

    def test_request_invalidates_inflight_legacy_and_revision_keeps_enrollment(self):
        self.ingest('input'); self.project()
        fixture.PassageOrigin.requeue(self)
        legacy = self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0])
        self.enroll()
        self.assertEqual(self.passages._commit(legacy)['status'], 'stale')
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        prepared = self.passages._prepare(candidate, incremental=True)
        self.assertIsNone(prepared.previous_checkpoint)
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        ids = {p['passage_id'] for p in self.projected()}
        self.ingest('result', result=True, second=1)
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertTrue(any('tool-result' in json.dumps(p['spans']) for p in self.projected()))
        # The input's group-local coordinates survive a later visible message.
        self.assertTrue(any(p['passage_id'] in ids and 'tool-input' in json.dumps(p['spans'])
                            for p in self.projected()))
        self.assertIsNone(self.document()['append_checkpoint'])

    def test_wrong_tenant_stale_manifest_and_unknown_version_do_not_enqueue(self):
        self.ingest('input'); self.project(); before = self.document()
        with self.assertRaises(PermissionError):
            self.enroll(tenant_id='tenant:other')
        with self.assertRaises((ValueError, LogicalEvidenceError)):
            self.passages.request_extraction_version(source_id=self.source,
                logical_document_id=before['logical_document_id'], revision=before['revision'] + 1,
                source_document_sha256=before['source_document_sha256'], extraction_version='tool_groups_v1', principal_id=self.principal)
        with self.assertRaises(ValueError):
            self.passages.request_extraction_version(source_id=self.source,
                logical_document_id=before['logical_document_id'], revision=before['revision'],
                source_document_sha256=before['source_document_sha256'], extraction_version='future', principal_id=self.principal)
        self.assertEqual(self.document(), before)
        self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1), ())

    def test_unsupported_result_preserves_raw_and_records_disposition(self):
        raw = self.ingest('unsupported', value={'type': 'user', 'message': {'role': 'user', 'content': [
            {'type': 'text', 'text': 'Sapphire image context'},
            {'type': 'tool_result', 'content': [{'type': 'image', 'source': {'data': 'opaque'}}]}]}})
        self.project(); self.enroll(); self.project()
        coverage = self.document()['extraction_coverage']
        self.assertEqual(coverage['dispositions']['unsupported_result_content'], 1)
        self.assertEqual(coverage['groups'], 0)
        opened = self.grant_and_open(self.projected()[0])
        self.assertEqual(opened['chunks'][0]['text'], raw)

    def test_queue_failure_rolls_back_enrollment_and_retry_is_atomic(self):
        import psycopg
        self.ingest('input'); self.project()
        with self.store.connect() as connection:
            connection.execute("""CREATE OR REPLACE FUNCTION reject_test_tool_queue() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture queue failure'; END $$""")
            connection.execute("""CREATE TRIGGER reject_test_tool_queue BEFORE INSERT
                ON canonical_passage_projection_queue FOR EACH ROW EXECUTE FUNCTION reject_test_tool_queue()""")
        try:
            with self.assertRaises(psycopg.Error):
                self.enroll()
            self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
            self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1), ())
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_test_tool_queue ON canonical_passage_projection_queue')
                connection.execute('DROP FUNCTION reject_test_tool_queue()')
        self.assertEqual(self.enroll()['status'], 'queued')
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')

    def test_current_authority_refuses_enrollment_and_reads_after_revocation_or_forget(self):
        self.ingest('input'); self.project(); self.enroll(); self.project()
        tool = next(p for p in self.projected() if 'tool-input' in json.dumps(p['spans']))
        self.grant_and_open(tool)
        target = {key: tool[key] for key in ('source_id', 'logical_document_id', 'revision',
                                            'manifest_content_sha256', 'passage_id')}
        reader = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,))
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s', (self.tenant,))
        with self.assertRaises(ValueError):
            reader.show(target)
        with self.assertRaises(ValueError):
            self.enroll()
        self.grant_and_open(tool)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_source_grants SET permission='owner' WHERE tenant_id=%s",
                               (self.tenant,))
        fixture.PassageOrigin.requeue(self)
        prepared = self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0])
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_evidence_document_queue(
                tenant_id,source_id,native_parent_id,reason) VALUES (%s,%s,'session-one','forget')""",
                (self.tenant, self.source))
        with self.assertRaises(ValueError):
            reader.show(target)
        with self.assertRaises(ValueError):
            self.enroll()
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')

    def test_read_grant_alone_cannot_enroll(self):
        self.ingest('input'); self.project()
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_source_grants SET permission='read' WHERE tenant_id=%s",
                               (self.tenant,))
        with self.assertRaises(ControlError):
            self.enroll()
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')

    def test_targeted_reseed_does_not_enroll_existing_or_new_parent_controls(self):
        self.ingest('input'); self.ingest('control', parent='control', second=1); self.project()
        self.enroll()
        with self.store.connect() as connection:
            queued = connection.execute('SELECT logical_document_id FROM canonical_passage_projection_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
            self.assertEqual([q['logical_document_id'] for q in queued], [self.document()['logical_document_id']])
            connection.execute('DELETE FROM canonical_passage_projection_queue WHERE tenant_id=%s', (self.tenant,))
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 1)
        self.project()
        self.ingest('new-control', parent='new-control', second=2); self.project()
        with self.store.connect() as connection:
            controls = connection.execute("""SELECT projected.*,evidence.native_parent_id
                FROM canonical_passage_documents projected JOIN canonical_evidence_documents evidence
                USING(tenant_id,source_id,logical_document_id)
                WHERE tenant_id=%s AND evidence.native_parent_id<>'session-one'""", (self.tenant,)).fetchall()
        self.assertEqual(len(controls), 2)
        self.assertTrue(all(p['requested_extraction_version'] == p['applied_extraction_version'] == 'legacy' for p in controls))
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_enrolled_preparation_cannot_replace_a_newer_source_revision(self):
        self.ingest('input'); self.project(); self.enroll()
        prepared = self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0])
        self.ingest('input', result=True, second=1)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1)
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.project()
        tools = [p for p in self.projected() if 'tool-' in json.dumps(p['spans'])]
        self.assertEqual(len(tools), 1)
        self.assertIn('tool-result', tools[0]['spans'][0]['source_unit'])

    def test_same_generation_legacy_preparation_cannot_erase_enrolled_projection(self):
        self.ingest('input'); self.project(); self.enroll(); self.project()
        before = self.projected()
        fixture.PassageOrigin.requeue(self)
        candidate = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        legacy = self.passages._prepare(replace(candidate, extraction_version='legacy'))
        self.assertEqual(self.passages._commit(legacy)['status'], 'stale')
        self.assertEqual(self.projected(), before)
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.passages._commit(self.passages._prepare(candidate))['status'], 'committed')


if __name__ == '__main__':
    unittest.main(verbosity=2)
