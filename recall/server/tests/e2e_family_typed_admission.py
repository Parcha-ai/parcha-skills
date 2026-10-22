#!/usr/bin/env python3
"""Ordinary bounded admission includes permitted historical Claude tool evidence."""
import os
from pathlib import Path
import time
import unittest
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.conninfo import make_conninfo

from e2e_tool_group_publication import ToolGroupPublication
from recall_server import passage_index


class FamilyTypedAdmission(unittest.TestCase):
    project = ToolGroupPublication.project
    projected = ToolGroupPublication.projected
    grant_and_open = ToolGroupPublication.grant_and_open
    ingest = ToolGroupPublication.ingest

    @classmethod
    def setUpClass(cls):
        admin_dsn = os.environ['RECALL_DATABASE_URL']
        database = 'family_admission_' + uuid.uuid4().hex
        with psycopg.connect(admin_dsn, autocommit=True) as connection:
            connection.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)))
        def drop_database():
            with psycopg.connect(admin_dsn, autocommit=True) as connection:
                connection.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))
        cls.addClassCleanup(drop_database)
        environment = patch.dict(os.environ, {'RECALL_DATABASE_URL': make_conninfo(admin_dsn, dbname=database)})
        environment.start()
        cls.addClassCleanup(environment.stop)

    def setUp(self):
        ToolGroupPublication.setUp(self)
        self.source = 'claude:' + self.source

    def legacy(self, *, parent='session-one', mixed=False):
        blocks = [{'type': 'tool_result', 'tool_use_id': 'call-native',
                   'content': 'Synthetic compiler checked 17 modules; two need imports.'}]
        if mixed:
            blocks.insert(0, {'type': 'text', 'text': 'Please preserve this original human context.'})
        raw = self.ingest(parent+'-result', parent=parent, result=True,
            value={'type': 'user', 'message': {'role': 'user', 'content': blocks}})
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            self.assertEqual(self.project()['documents'], 1)
        self.assertFalse(any('Synthetic compiler' in row['text_redacted'] for row in self.projected()))
        return raw

    def document(self):
        with self.store.connect() as connection:
            return connection.execute("""SELECT projected.* FROM canonical_passage_documents projected
                JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,logical_document_id)
                WHERE tenant_id=%s AND source_id=%s AND evidence.native_parent_id='session-one'""",
                (self.tenant, self.source)).fetchone()

    def admit(self, limit=2):
        return self.passages.admit_typed_pending(tenant_id=self.tenant,
            limit=limit, deadline_at=time.monotonic()+3)

    def test_finished_tool_only_legacy_admits_without_body_read_and_publishes_after_restart(self):
        raw = self.legacy()
        self.assertEqual(self.projected(), [])
        with patch.object(self.passages.logical_projection, 'read_part',
                wraps=self.passages.logical_projection.read_part) as archive_read:
            self.assertEqual(self.admit()['queued'], 1)
            self.assertEqual(archive_read.call_count, 0)
        self.assertEqual(self.document()['requested_extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        pending = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        restarted = passage_index.CanonicalPassageProjector(self.store,
            self.passages.logical_projection, policy=self.passages.policy, bound_tenant_id=self.tenant)
        self.assertEqual(restarted.admit_typed_pending(tenant_id=self.tenant,
            limit=2, deadline_at=time.monotonic()+3)['queued'], 0)
        self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1)[0].generation, pending.generation)
        self.assertEqual(self.project()['documents'], 1)
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        rows = self.projected()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['text_redacted'], 'Synthetic compiler checked 17 modules; two need imports.')
        self.assertTrue(all(span['source_unit'].startswith('claude.tool-result.string.v1#') for span in rows[0]['spans']))
        self.assertIn(raw, [chunk['text'] for chunk in self.grant_and_open(rows[0])['chunks']])
        self.assertEqual(self.admit()['queued'], 0)


    def test_ordinary_worker_enrolls_mixed_legacy_and_preserves_visible_bytes(self):
        from recall_server.projection_worker import run_projection_worker
        self.legacy(mixed=True)
        before = self.projected()
        self.assertEqual(len(before), 1)
        run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
            logical_batch_size=1, passage_batch_size=2, embedding_batch_size=1,
            max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
            interval_seconds=1, once=True, skip_embedding=True)
        self.assertEqual(self.document()['applied_extraction_version'], 'tool_groups_v1')
        after = self.projected()
        visible = next(row for row in after if row['text_redacted'] == before[0]['text_redacted'])
        for field in ('text_sha256', 'receipts', 'spans'):
            self.assertEqual(visible[field], before[0][field])
        self.assertTrue(any(row['text_redacted'].startswith('Synthetic compiler') for row in after))

    def test_mixed_families_share_capacity_and_fairness_without_foreign_sources(self):
        from e2e_codex_tool_publication import CodexToolPublication
        claude = self.source
        self.legacy()
        self.legacy(parent='claude-sibling')
        self.source = 'codex:' + claude
        codex = self.source
        CodexToolPublication.ingest(self, 'codex-result', result=True)
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            self.assertEqual(self.project()['documents'], 1)
        self.source = 'other:' + claude
        self.legacy(parent='unrelated-family')
        first = self.admit()
        self.assertEqual(first['queued'], 2)
        self.assertEqual({row['source_id'] for row in first['outcomes']}, {claude, codex})
        self.assertEqual({row['extraction_version'] for row in first['outcomes']},
                         {'tool_groups_v1', 'codex_tool_groups_v1'})
        self.assertEqual(self.admit()['queued'], 0, 'The two families share one outstanding window')
        self.assertEqual(self.project()['documents'], 2)
        last = self.admit()
        self.assertEqual(last['queued'], 1)
        self.assertEqual(last['outcomes'][0]['source_id'], claude)
        self.assertEqual(last['outcomes'][0]['extraction_version'], 'tool_groups_v1')
        self.assertEqual(self.project()['documents'], 1)
        self.assertEqual(self.admit()['queued'], 0, 'Typed Claude must not enroll again')
        with self.store.connect() as connection:
            other = connection.execute("SELECT requested_extraction_version FROM canonical_passage_documents WHERE tenant_id=%s AND source_id=%s", (self.tenant, self.source)).fetchone()
        self.assertEqual(other['requested_extraction_version'], 'legacy')

    def test_claude_owner_revocation_and_pending_forget_after_nomination_refuse(self):
        self.legacy()
        original = self.passages._request_extraction_version
        def revoke(**kwargs):
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s', (self.tenant,))
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=revoke):
            outcome = self.admit()
        self.assertEqual(outcome['outcomes'][0]['reason'], 'source_or_projected_document_unavailable')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        with self.store.connect() as connection:
            connection.execute("INSERT INTO canonical_source_grants(tenant_id,source_id,principal_id,permission) VALUES(%s,%s,%s,'read')", (self.tenant,self.source,self.principal))
        self.assertEqual(self.admit()['selected'], 0)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_source_grants SET permission='owner' WHERE tenant_id=%s", (self.tenant,))
        def forget(**kwargs):
            with self.store.connect() as connection:
                connection.execute("""INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,reason)
                    VALUES(%s,%s,'session-one','forget')""", (self.tenant,self.source))
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=forget):
            outcome = self.admit()
        self.assertEqual(outcome['outcomes'][0]['reason'], 'document_extraction_forget_pending')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')

    def test_claude_changed_revision_refuses_stale_request(self):
        self.legacy()
        original = self.passages._request_extraction_version
        def append(**kwargs):
            self.ingest('later-human', second=2, value={'type':'user','message':{
                'role':'user','content':[{'type':'text','text':'A later captured correction.'}]}})
            self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
                max_batches=1, upload_concurrency=1)
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=append):
            result = self.admit()
        self.assertEqual(result['outcomes'][0]['reason'], 'document_extraction_catalog_changed')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(len(self.passages._pending(tenant_id=self.tenant, limit=2)), 1)

    def test_optional_family_index_exact_readiness_parity_and_rollback(self):
        from e2e_typed_upgrade_capacity_index import NATIVE
        from e2e_codex_tool_publication import CodexToolPublication
        operations = Path(__file__).resolve().parents[1] / 'operations'
        build = (operations / 'family_typed_candidate_index_concurrent.sql').read_text()
        ready = (operations / 'family_typed_candidate_index_readiness.sql').read_text()
        index = 'canonical_passage_documents_family_candidate_idx'
        self.legacy()
        claude = self.source
        self.source = 'codex:' + claude
        CodexToolPublication.ingest(self, 'codex-index-result', result=True)
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            self.assertEqual(self.project()['documents'], 1)

        def outputs():
            with self.store.connect() as connection:
                return [self.store._execute_bounded(connection, NATIVE,
                    (self.tenant, limit, self.passages.policy.fingerprint, limit,
                     self.tenant, cursor, cursor, limit), time.monotonic()+3).fetchall()
                    for limit in (1, 2, 100) for cursor in (None, claude, self.source, 'zzzz')]

        before = outputs()
        self.assertEqual({r['source_id'] for r in before[-1]}, {claude, self.source})
        with psycopg.connect(self.store.dsn, autocommit=True, row_factory=dict_row,
                options='-c statement_timeout=10000 -c lock_timeout=1000') as connection:
            versions = connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()
            existing = connection.execute("SELECT indexrelid FROM pg_index WHERE indrelid='canonical_passage_documents'::regclass ORDER BY indexrelid").fetchall()
            connection.execute('CREATE INDEX ' + index + ' ON canonical_passage_documents(tenant_id)')
            self.store._migrate_concurrently(connection, build)
            connection.autocommit = True
            with self.assertRaisesRegex(psycopg.errors.InvalidTextRepresentation, 'family_typed_candidate_index_unavailable'):
                connection.execute(ready)
            connection.execute('DROP INDEX CONCURRENTLY public.' + index)
            self.store._migrate_concurrently(connection, build)
            connection.autocommit = True
            connection.execute(ready)
            state = connection.execute("SELECT indexrelid,pg_relation_size(indexrelid) AS bytes,pg_get_indexdef(indexrelid) AS definition FROM pg_index WHERE indexrelid=to_regclass(%s)", ('public.'+index,)).fetchone()
            self.assertEqual(outputs(), before)
            self.store._migrate_concurrently(connection, build)
            connection.autocommit = True
            connection.execute(ready)
            self.assertEqual(connection.execute('SELECT to_regclass(%s)::oid AS id', ('public.'+index,)).fetchone()['id'], state['indexrelid'])
            self.assertEqual(self.admit()['queued'], 2)
            self.assertEqual(self.project()['documents'], 2)
            self.assertEqual(outputs(), [[] for _ in before])
            # Completed Claude must leave this index, unlike the older Codex index.
            predicate = build.split('WHERE ', 1)[1].rstrip().removesuffix(';')
            self.assertEqual(connection.execute('SELECT count(*) AS n FROM canonical_passage_documents WHERE tenant_id=%s AND ' + predicate.replace('%', '%%'), (self.tenant,)).fetchone()['n'], 0)
            connection.execute('DROP INDEX CONCURRENTLY public.' + index)
            self.assertEqual(outputs(), [[] for _ in before])
            self.assertEqual(connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall(), versions)
            self.assertEqual(connection.execute("SELECT indexrelid FROM pg_index WHERE indrelid='canonical_passage_documents'::regclass ORDER BY indexrelid").fetchall(), existing)
            print(dict(family_index_bytes=state['bytes'], exact_definition=state['definition'], schema_and_old_indexes_unchanged=True), flush=True)

    def test_worker_scope_cannot_cross_tenant_or_select_the_wrong_family_target(self):
        self.legacy()
        row = self.document()
        with self.assertRaises(PermissionError):
            self.passages.admit_typed_pending(tenant_id='tenant:foreign', limit=2,
                deadline_at=time.monotonic()+3)
        args=dict(tenant_id=self.tenant,source_id=self.source,
            logical_document_id=row['logical_document_id'],revision=row['revision'],
            source_document_sha256=row['source_document_sha256'],worker=True,deadline_at=time.monotonic()+3)
        for principal,version,source in ((self.principal,'tool_groups_v1',self.source),
                (None,'codex_tool_groups_v1',self.source),
                (None,'tool_groups_v1','foreign:'+self.source)):
            with self.subTest(principal=principal,version=version,source=source):
                with self.assertRaises(ValueError):
                    self.passages._request_extraction_version(**dict(args,source_id=source),
                        principal_id=principal,extraction_version=version)
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')


if __name__ == '__main__':
    unittest.main(defaultTest='FamilyTypedAdmission')
