#!/usr/bin/env python3
"""Historical indexed tool results migrate with exact attribution on normal rebuild."""
import json
import hashlib
import sys
import unittest
from unittest.mock import patch

import psycopg
from e2e_typed_passage_visibility import TypedPassageVisibility
from recall_server import passage_index, passage_projection
from recall_server.canonical import CanonicalLifecycleError
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.projectors import canonical_json


class LegacyToolMigration(unittest.TestCase):
    ingest_blocks = TypedPassageVisibility.ingest_blocks
    project = TypedPassageVisibility.project
    projected = TypedPassageVisibility.projected
    grant_and_open = TypedPassageVisibility.grant_and_open

    def setUp(self):
        TypedPassageVisibility.setUp(self)
        self.source = 'claude:' + self.source

    def seed(self, *, historical=True, mixed=False):
        blocks = [{'type': 'tool_result', 'content': 'Verified historical result.'}]
        if mixed:
            blocks.insert(0, {'type': 'text', 'text': 'Original human request.'})
        raw = self.ingest_blocks('result', blocks)
        native = passage_projection._visible_message
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            if historical:
                with patch.object(passage_projection, '_visible_message', side_effect=lambda text: native(text, include_tool_results=True)):
                    self.project()
            else:
                self.project()
        return raw

    def ingest_final(self):
        value = {'type': 'assistant', 'message': {'role': 'assistant', 'stop_reason': 'end_turn',
            'content': [{'type': 'text', 'text': 'Complete verified final finding.'}]}}
        payload = canonical_json(value)
        occurred = '2026-10-13T12:00:00Z'
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id='final', payload=payload, media_type='application/json', created_at=occurred)
        envelope = dict(schema_version=1, source_id=self.source, native_id='final',
            native_parent_id='session-one', kind='transcript_record', occurred_at=occurred,
            observed_at=occurred, principal_id=self.principal, visibility='private',
            content_type='application/json', content=value,
            provenance=dict(connector_id='synthetic.migration', connector_schema_version=1, artifact_ref=artifact),
            content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id='synthetic.migration', artifact_ref=artifact, envelope=envelope,
            text_redacted=payload.decode())

    def test_missing_optional_append_state_defers_then_migrates(self):
        self.seed()
        before = self.published_rows()
        self.ingest_blocks('arrival', [{'type': 'text', 'text': 'Ordinary new human request.'}])
        with patch.object(self.passages, '_append_state', return_value=None):
            report = self.project()
        self.assertEqual(report['documents'], 0)
        self.assertEqual(report['unavailable'], 1)
        self.assertEqual(self.published_rows(), before)
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)
        self.assertEqual(self.project()['documents'], 1)
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertTrue(any(p['text_redacted'] == 'Verified historical result.' for p in self.projected()))
        self.assertEqual(self.pending(), 0)

    def test_tool_migration_preserves_complete_final_unit(self):
        self.ingest_final()
        self.seed()
        old_final = next(p for p in self.projected() if p['text_redacted'] == 'Complete verified final finding.')
        self.assertEqual(old_final['spans'][0]['source_unit'], 'assistant_final.complete.v1')
        prepared = self.append_and_prepare()
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        final = next(p for p in self.projected() if p['text_redacted'] == old_final['text_redacted'])
        self.assertEqual(final['receipts'], old_final['receipts'])
        self.assertEqual(final['spans'][0]['source_unit'], 'assistant_final.complete.v1')
        self.assertTrue(any(p['text_redacted'] == 'Verified historical result.' for p in self.projected()))
        self.grant_and_open(final)

    def document(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT * FROM canonical_passage_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()

    def published_rows(self):
        return [{k: v for k, v in row.items() if k != "manifest_content_sha256"} for row in self.projected()]

    def pending(self):
        with self.store.connect() as connection:
            return connection.execute("SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s AND reason<>'certification'", (self.tenant,)).fetchone()['n']

    def append_and_prepare(self, *, native="arrival", incremental=True):
        self.ingest_blocks(native, [{'type': 'text', 'text': 'Ordinary new human request.'}])
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2, max_batches=1, upload_concurrency=1)
        return self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0], incremental=incremental)

    def test_normal_append_preserves_indexed_result_with_typed_attribution(self):
        raw = self.seed()
        old = self.projected()[0]
        self.grant_and_open(old)
        prepared = self.append_and_prepare()
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        current = self.projected()
        tool = next((p for p in current if old['receipts'][0] in p['receipts']), None)
        self.assertIsNotNone(tool, 'normal append silently dropped previously indexed tool evidence')
        self.assertEqual(tool['text_redacted'], 'Verified historical result.')
        self.assertTrue(all(s['source_unit'].startswith('claude.tool-result.string.v1#') for s in tool['spans']))
        self.assertIn('not verified fact or human speech', tool['header_redacted'])
        self.assertTrue(any(p['text_redacted'] == 'Ordinary new human request.' for p in current))
        self.assertEqual(self.document()['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        self.assertIsNone(self.document()['append_checkpoint'])
        self.assertEqual(self.pending(), 0)
        opened = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal, authorized_sources=(self.source,)).show(old['receipts'][0])
        self.assertIn(raw, [c['text'] for c in opened['chunks']])
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.assertEqual(set(self.client.namespace(self.settings.namespace(self.tenant)).rows), {p['passage_id'] for p in current})
        self.assertEqual(self.project()['documents'], 0)

    def test_unindexed_tools_do_not_enroll_modern_legacy(self):
        for mixed in (False, True):
            with self.subTest(mixed=mixed):
                # Separate native parent scopes make both stored-zero and visible-only cases real.
                if mixed:
                    self.setUp()
                self.seed(historical=False, mixed=mixed)
                prepared = self.append_and_prepare()
                self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
                self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
                self.assertFalse(any('claude.tool-' in json.dumps(p['spans']) for p in self.projected()))

    def test_no_historical_tool_overlap_skips_parent_maps(self):
        for unindexed_tool in (False, True):
            with self.subTest(unindexed_tool=unindexed_tool):
                if unindexed_tool:
                    self.setUp()
                    self.seed(historical=False)
                else:
                    self.ingest_blocks('original', [{'type': 'text', 'text': 'Original human request.'}])
                    with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
                        self.project()
                before = self.projected()
                observed = []
                code = passage_index._legacy_result_intervals.__code__

                def observe(frame, event, result):
                    if frame.f_code is code and event == 'return':
                        observed.append({name: len(frame.f_locals[name])
                            for name in ('normal', 'normal_units', 'historical')
                            if name in frame.f_locals})

                previous = sys.getprofile()
                try:
                    sys.setprofile(observe)
                    prepared = self.append_and_prepare()
                finally:
                    sys.setprofile(previous)
                self.assertIsNone(prepared.legacy_migration)
                self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
                self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
                self.assertEqual(self.pending(), 0)
                current = self.projected()
                self.assertTrue(any('Ordinary new human request.' in p['text_redacted'] for p in current))
                for old in before:
                    self.assertTrue(any(set(old['receipts']) <= set(p['receipts']) for p in current))
                for row in current:
                    self.grant_and_open(row)
                self.assertEqual(observed, [{}], 'unused whole-parent migration maps were allocated')

    def test_mixed_old_byte_domain_refuses_without_destructive_commit(self):
        self.seed(mixed=True)
        before = self.published_rows()
        with self.assertRaisesRegex(LogicalEvidenceError, 'legacy_tool_migration_required'):
            self.append_and_prepare()
        self.assertEqual(self.published_rows(), before)
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)
        self.ingest_blocks('unaffected', [{'type': 'text', 'text': 'Another parent progresses.'}], parent='unaffected')
        report = self.project()
        self.assertEqual(report['documents'], 1)
        self.assertEqual(report['unavailable'], 1)
        self.assertEqual(self.pending(), 1)

    def test_changed_source_refuses_stale_migration(self):
        self.seed()
        prepared = self.append_and_prepare()
        self.ingest_blocks('result', [{'type': 'tool_result', 'content': 'Current corrected result.'}])
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2, max_batches=1, upload_concurrency=1)
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertFalse(any('Verified historical' in p['text_redacted'] for p in self.projected()))

    def test_pending_forget_refuses_migration(self):
        self.seed()
        old = self.projected()[0]
        prepared = self.append_and_prepare()
        request = dict(contract='recall.forget-request.v1', schema_version=1, tenant_id=self.tenant,
            principal_id=self.principal, source_id=self.source, target_receipt=old['receipts'][0],
            mode='explicit_forget', reason='owner_requested', requested_at='2026-10-13T00:00:00Z', idempotency_key='migration-forget')
        with patch.object(self.plane.archive, 'delete_internal_raw_bounded', side_effect=RuntimeError('offline')):
            with self.assertRaises(CanonicalLifecycleError):
                self.plane.forget(request)
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')

    def test_failed_commit_rolls_back_versions_and_passages(self):
        self.seed()
        before = self.published_rows()
        prepared = self.append_and_prepare()
        with self.store.connect() as connection:
            connection.execute("CREATE FUNCTION reject_migration_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture migration failure'; END $$")
            connection.execute('CREATE TRIGGER reject_migration_insert BEFORE INSERT ON canonical_passages FOR EACH ROW EXECUTE FUNCTION reject_migration_insert()')
        try:
            with self.assertRaises(psycopg.Error):
                self.passages._commit(prepared)
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_migration_insert ON canonical_passages')
                connection.execute('DROP FUNCTION reject_migration_insert()')
        self.assertEqual(self.published_rows(), before)
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')

    def test_explicit_native_enrollment_supersedes_automatic_preparation(self):
        self.seed()
        document = self.document()
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_passage_projection_queue
                (tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                VALUES (%s,%s,%s,%s,1,'backfill',clock_timestamp())""",
                (self.tenant,self.source,document['logical_document_id'],document['revision']))
        prepared = self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0], incremental=True)
        self.assertEqual(prepared.candidate.extraction_version, 'tool_groups_v1')
        enrolled = self.passages.request_extraction_version(tenant_id=self.tenant, source_id=self.source,
            logical_document_id=document['logical_document_id'], revision=document['revision'],
            source_document_sha256=document['source_document_sha256'], extraction_version='tool_groups_v1',
            principal_id=self.principal)
        self.assertEqual(enrolled['status'], 'queued')
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertEqual(self.project()['documents'], 1)
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')

    def test_prior_passage_change_refuses_captured_migration(self):
        self.seed()
        prepared = self.append_and_prepare()
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passages SET ordinal=ordinal+1 WHERE tenant_id=%s', (self.tenant,))
        self.assertEqual(self.passages._commit(prepared)['status'], 'stale')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)

    def test_explicit_full_preview_keeps_requested_legacy_semantics(self):
        self.seed()
        old = self.projected()[0]
        prepared = self.append_and_prepare(incremental=False)
        self.assertEqual(prepared.candidate.extraction_version, 'legacy')
        self.assertIsNone(prepared.legacy_migration)
        self.assertFalse(any(old['receipts'][0] in p.receipts for p in prepared.passages))
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(self.pending(), 1)

    def test_append_full_fallback_keeps_worker_migration_intent(self):
        self.seed()
        with patch.object(self.passages, '_prepare', wraps=self.passages._prepare) as prepare:
            prepared = self.append_and_prepare(native='zz-arrival')
        self.assertTrue(any(call.kwargs.get('_migrate_legacy') for call in prepare.call_args_list))
        self.assertEqual(prepared.candidate.extraction_version, 'tool_groups_v1')
        self.assertEqual(self.passages._commit(prepared)['status'], 'committed')


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(LegacyToolMigration)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
