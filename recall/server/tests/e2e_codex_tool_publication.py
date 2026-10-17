#!/usr/bin/env python3
"""Codex typed publication through native queue, archive, authority and vendor."""
import json
import unittest
from dataclasses import replace
from unittest.mock import patch

import psycopg
import e2e_tool_group_publication as fixture
from contracts.source_units import resolve_codex_tool_source_unit
from recall_server import passage_index
from recall_server.passage_retrieval import rerank_document
from evals.passage_index_audit import _exact_sample, _span_coverage

VERSION = 'codex_tool_groups_v1'


class CodexToolPublication(unittest.TestCase):
    project = fixture.ToolGroupPublication.project
    projected = fixture.ToolGroupPublication.projected
    document = fixture.ToolGroupPublication.document
    grant_and_open = fixture.ToolGroupPublication.grant_and_open

    def setUp(self):
        fixture.ToolGroupPublication.setUp(self)
        self.source = 'codex:' + self.source
        self.retrieval.sources = [self.source]

    def ingest(self, native, *, result=False, second=0, value=None):
        if value is None:
            value = dict(type='response_item', payload=(
                dict(type='function_call_output', call_id='call-one', output='Sapphire tests failed; deployment remains untested.')
                if result else dict(type='function_call', name='UnknownTool', call_id='call-one',
                    arguments='{"report":"Sapphire fix is ready for review","options":["Approve deployment","Hold off"]}')))
        return fixture.ToolGroupPublication.ingest(self, native, second=second, value=value)

    def prepare(self):
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1)
        return self.passages._prepare(self.passages._pending(tenant_id=self.tenant, limit=1)[0])

    def enroll(self, version=VERSION):
        row = self.document()
        return self.passages.request_extraction_version(source_id=self.source,
            logical_document_id=row['logical_document_id'], revision=row['revision'],
            source_document_sha256=row['source_document_sha256'], extraction_version=version,
            principal_id=self.principal)

    def test_first_projection_exact_source_attribution_vendor_and_audit(self):
        self.store.search_plane = 'turbopuffer'
        originals = [self.ingest('input'), self.ingest('result', result=True, second=1)]
        self.project()
        rows = self.projected()
        self.assertEqual(len(rows), 2)
        self.assertEqual(self.document()['requested_extraction_version'], VERSION)
        self.assertEqual(self.document()['applied_extraction_version'], VERSION)
        self.assertIsNone(self.document()['append_checkpoint'])
        for row in rows:
            opened = self.grant_and_open(row)
            self.assertTrue(opened['complete'])
            raw = opened['chunks'][0]['text']; self.assertIn(raw, originals)
            for span in row['spans']:
                leaf, kind, provenance = resolve_codex_tool_source_unit(raw, span['source_unit'])
                self.assertEqual(leaf.encode()[span['source_byte_start']:span['source_byte_end']],
                    row['text_redacted'].encode()[span['passage_byte_start']:span['passage_byte_end']])
                self.assertEqual(span['origin']['source_tool'], provenance)
                self.assertEqual(row['roles'], [provenance['role']])
            ranked = rerank_document(row, ['Sapphire'], 4096)
            self.assertIn('does not prove execution, selection, or human approval' if kind=='tool_input'
                          else 'reported output, not verified fact or human speech', ranked)
        self.writer.drain(tenant_id=self.tenant, max_months=10)
        self.assertEqual(set(self.client.namespace(self.settings.namespace(self.tenant)).rows),
                         {row['passage_id'] for row in rows})
        result = self.retrieval.search('Sapphire', lexical_query='Sapphire', since=None, until=None, limit=10)
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        self.assertEqual(sum('evidence_context' in r for r in result['results'][0]['matching_ranges']), 2)
        self.assertEqual(_exact_sample(self.store,self.passages.logical_projection,rows,
            policy=self.passages.policy,tenant_id=self.tenant),len(rows))
        self.assertEqual(_span_coverage([self.document()],rows)['uncovered_bytes'],0)
        self.assertEqual(self.passages.shadow_diff(tenant_id=self.tenant,source_id=self.source)['documents'][0]['status'],'compared')

    def test_existing_legacy_and_claude_version_stay_until_explicit_upgrade(self):
        for old in ['legacy','tool_groups_v1']:
            with self.subTest(old=old):
                self.setUp()
                self.ingest('input')
                with patch.object(passage_index,'_document_extraction_version',return_value=old):self.project()
                self.ingest('result',result=True,second=1);self.project()
                self.assertEqual(self.document()['applied_extraction_version'],old)
                self.assertEqual(self.projected(),[])
                self.assertEqual(self.enroll()['status'],'queued')
                self.project(); self.assertEqual(len(self.projected()),2)
                self.assertEqual(self.document()['applied_extraction_version'],VERSION)
                self.assertEqual(self.enroll()['status'],'current')
                with self.assertRaises(ValueError):self.enroll('tool_groups_v1')

    def test_revision_and_same_generation_reject_stale_version_without_append(self):
        self.ingest('input');self.project()
        prior={p['passage_id'] for p in self.projected()}
        self.ingest('result',result=True,second=1)
        prepared=self.prepare()
        legacy=self.passages._prepare(replace(prepared.candidate,extraction_version='legacy'))
        self.assertEqual(self.passages._commit(legacy)['status'],'stale')
        self.assertEqual(self.passages._commit(prepared)['status'],'committed')
        self.assertTrue(prior <= {p['passage_id'] for p in self.projected()})
        self.assertIsNone(self.document()['append_checkpoint'])

    def test_failed_commit_rolls_back_and_retries_first_projection(self):
        self.ingest('input');prepared=self.prepare()
        with self.store.connect() as connection:
            connection.execute("CREATE FUNCTION reject_codex_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture failure'; END $$")
            connection.execute('CREATE TRIGGER reject_codex_commit BEFORE INSERT ON canonical_passage_documents FOR EACH ROW EXECUTE FUNCTION reject_codex_commit()')
        try:
            with self.assertRaises(psycopg.Error):self.passages._commit(prepared)
            self.assertIsNone(self.document());self.assertEqual(self.projected(),[])
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_codex_commit ON canonical_passage_documents')
                connection.execute('DROP FUNCTION reject_codex_commit()')
        self.project();self.assertEqual(self.document()['applied_extraction_version'],VERSION)

    def test_changed_input_replaces_old_passage_and_vendor_identity(self):
        self.store.search_plane='turbopuffer'
        self.ingest('input');self.project()
        before={p['passage_id'] for p in self.projected()}
        self.writer.drain(tenant_id=self.tenant,max_months=10)
        self.ingest('input',value=dict(type='response_item',payload=dict(type='function_call',
            name='UnknownTool',call_id='call-one',arguments='Updated Sapphire request; do not deploy.')))
        self.project()
        after={p['passage_id'] for p in self.projected()}
        self.assertTrue(after);self.assertFalse(before & after)
        self.assertTrue(all('Updated Sapphire' in p['text_redacted'] for p in self.projected()))
        self.writer.drain(tenant_id=self.tenant,max_months=10)
        self.assertEqual(set(self.client.namespace(self.settings.namespace(self.tenant)).rows),after)
        self.assertIsNone(self.document()['append_checkpoint'])

    def test_explicit_request_queue_failure_rolls_back_and_retry_is_atomic(self):
        self.ingest('input')
        with patch.object(passage_index,'_document_extraction_version',return_value='legacy'):self.project()
        with self.store.connect() as connection:
            connection.execute("CREATE FUNCTION reject_codex_request() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture failure'; END $$")
            connection.execute('CREATE TRIGGER reject_codex_request BEFORE INSERT ON canonical_passage_projection_queue FOR EACH ROW EXECUTE FUNCTION reject_codex_request()')
        try:
            with self.assertRaises(psycopg.Error):self.enroll()
            self.assertEqual(self.document()['requested_extraction_version'],'legacy')
            self.assertEqual(self.passages._pending(tenant_id=self.tenant,limit=1),())
        finally:
            with self.store.connect() as connection:
                connection.execute('DROP TRIGGER reject_codex_request ON canonical_passage_projection_queue')
                connection.execute('DROP FUNCTION reject_codex_request()')
        self.assertEqual(self.enroll()['status'],'queued')
        self.project();self.assertEqual(self.document()['applied_extraction_version'],VERSION)

    def test_revocation_and_forget_keep_native_authority(self):
        self.ingest('input');self.project();row=self.projected()[0]
        self.grant_and_open(row)
        target={key:row[key] for key in ('source_id','logical_document_id','revision','manifest_content_sha256','passage_id')}
        reader=fixture.BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,authorized_sources=(self.source,))
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s',(self.tenant,))
        with self.assertRaises(ValueError):reader.show(target)
        with self.assertRaises(ValueError):self.enroll()
        self.grant_and_open(row)
        self.ingest('result',result=True,second=1);prepared=self.prepare()
        with self.store.connect() as connection:
            connection.execute("INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason) VALUES(%s,%s,'session-one','forget')",(self.tenant,self.source))
        self.assertEqual(self.passages._commit(prepared)['status'],'stale')
        with self.assertRaises(ValueError):reader.show(target)

    def test_unsupported_provider_form_remains_raw_with_explicit_disposition(self):
        raw=self.ingest('unsupported',value=dict(type='response_item',payload=dict(type='function_call_output',output={'text':'Unsupported nested output'})))
        self.ingest('visible',second=1,value=dict(type='response_item',payload=dict(type='message',role='user',content=[dict(type='input_text',text='Sapphire context')])) )
        self.project()
        self.assertEqual(self.document()['extraction_coverage']['dispositions']['source_unit_refused'],1)
        self.assertEqual(len(self.projected()),1)
        self.grant_and_open(self.projected()[0])
        with self.store.connect() as connection:
            receipt=connection.execute("SELECT receipt FROM canonical_chunks WHERE tenant_id=%s AND receipt LIKE '%%/unsupported?rev=%%'",(self.tenant,)).fetchone()['receipt']
        reader=fixture.BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,authorized_sources=(self.source,))
        self.assertIn(raw,[chunk['text'] for chunk in reader.show(receipt)['chunks']])


if __name__=='__main__':unittest.main(verbosity=2)
