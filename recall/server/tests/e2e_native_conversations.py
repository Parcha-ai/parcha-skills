#!/usr/bin/env python3
"""Source-authorized native conversation grouping through real projection and search."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import insert_source, insert_record  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_thinning import _compact_event_expression  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings, passage_row  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402


class NativeConversations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store._pool.close()

    def setUp(self):
        self.tenant = 'tenant:conversation:' + uuid.uuid4().hex
        self.native = str(uuid.uuid4())
        self.sources = ['codex:mac', 'codex:greppy', 'codex:segment', 'codex:fork', 'codex:denied']
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.archive = FilesystemArchiveStore(Path(tmp.name), namespace_key=b'n'*32)
        self.projection = LogicalEvidenceProjectionStore(self.archive)
        self.logical = CanonicalLogicalEvidenceProjector(self.store, self.projection,
            bound_tenant_id=self.tenant, raw_archive=self.archive)
        self.passages = CanonicalPassageProjector(self.store, self.projection,
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        self.client = FakeTurbopuffer()
        self.settings = TurbopufferSettings(api_key='synthetic-local-only')

    def add(self, source, *, session=None, tail='shared deployment evidence', strand='root', provenance=True):
        session = session or self.native
        header = {'type': 'session_meta', 'payload': {'id': session}}
        if strand.startswith('segment:'):
            header['payload']['history_base'] = {'thread_id': self.native}
        texts = [json.dumps(header), json.dumps({'type': 'response_item', 'payload': {
            'role': 'assistant', 'content': [{'type': 'output_text', 'text': tail}]}})]
        with self.store.connect() as c:
            insert_source(c, self.tenant, 'principal:test', source)
            for index, text in enumerate(texts):
                native = 'turn:' + str(index)
                insert_record(c, tenant=self.tenant, source=source, parent='physical:session',
                    native=native, text=text, role='assistant', byte_start=index * 100)
                identity = {'conversation_id': 'codex:' + session, 'strand_id': strand}
                prov = {'harness': 'codex', 'byte_start': index * 100}
                if provenance:
                    prov['native_conversation'] = identity
                c.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{provenance}',%s::jsonb) WHERE tenant_id=%s AND source_id=%s AND native_id=%s",
                          (json.dumps(prov), self.tenant, source, native))
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=source,
                native_ids=['turn:0', 'turn:1'], reason='ingest')

    def project(self):
        self.logical.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        self.passages.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1)
        with self.store.connect() as c:
            rows = c.execute('''SELECT passage.*, evidence.native_parent_id,evidence.manifest_object_key,
                evidence.manifest_content_sha256,evidence.first_occurred_at AS doc_first_occurred_at,
                evidence.last_occurred_at AS doc_last_occurred_at,'[]'::jsonb AS actors
                FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                USING(tenant_id,source_id,logical_document_id) WHERE passage.tenant_id=%s''', (self.tenant,)).fetchall()
        self.client.namespace(self.settings.namespace(self.tenant)).write(upsert_rows=[passage_row(row) for row in rows])

    def search(self, sources=None):
        retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=sources or self.sources[:-1], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=self.settings, client=self.client)
        return retrieval.search('deployment evidence', lexical_query='deployment evidence',
            since=None, until=None, limit=10)['results']

    def test_copies_unique_tails_forks_and_scope(self):
        self.add(self.sources[0], provenance=False)  # Existing native header, before collector upgrade.
        self.add(self.sources[1], tail='unique remote deployment evidence')
        self.add(self.sources[2], tail='continuation deployment evidence', strand='segment:' + str(uuid.uuid4()))
        fork = str(uuid.uuid4())
        self.add(self.sources[3], session=fork)
        self.add(self.sources[4], tail='denied deployment evidence')
        self.project()
        results = self.search()
        self.assertEqual(len(results), 2)
        grouped = next(row for row in results if row.get('conversation_id') == 'codex:' + self.native)
        self.assertEqual({row['source_id'] for row in grouped['conversation_documents']}, set(self.sources[:3]))
        self.assertNotIn(self.sources[4], json.dumps(results))
        self.assertIn('unique remote', json.dumps(grouped['matching_ranges']))
        self.assertIn('continuation deployment', json.dumps(grouped['matching_ranges']))
        self.assertEqual(len(self.search([self.sources[0]])), 1)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s', (self.tenant, self.sources[1]))
        # Deliberately keep its stale vendor row. Canonical authority must remove every member first.
        self.assertNotIn(self.sources[1], json.dumps(self.search()))

    def test_same_content_repair_backfills_identity_without_revising_passages(self):
        self.add(self.sources[0], provenance=False)
        self.project()
        with self.store.connect() as c:
            before = c.execute('SELECT revision FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()['revision']
            c.execute('UPDATE canonical_evidence_documents SET conversation_id=NULL,conversation_strand_id=NULL WHERE tenant_id=%s', (self.tenant,))
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources[0], native_ids=['turn:0'], reason='ingest')
        self.logical.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        with self.store.connect() as c:
            row = c.execute('SELECT revision,conversation_id,conversation_strand_id FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertEqual(row, {'revision': before, 'conversation_id': 'codex:' + self.native, 'conversation_strand_id': 'root'})

    def test_conflicting_native_header_cannot_be_overridden_by_later_retained_metadata(self):
        self.add(self.sources[0])
        wrong = 'codex:' + str(uuid.uuid4())
        with self.store.connect() as c:
            c.execute("UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{provenance,native_conversation,conversation_id}',%s::jsonb) WHERE tenant_id=%s",
                      (json.dumps(wrong), self.tenant))
        self.project()
        with self.store.connect() as c:
            row = c.execute('SELECT conversation_id FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()
        self.assertIsNone(row['conversation_id'])

    def test_thinning_does_not_add_empty_identity_to_legacy_events(self):
        self.add(self.sources[0], provenance=False)
        with self.store.connect() as c:
            rows = c.execute('SELECT ' + _compact_event_expression('event') + ' AS compact FROM canonical_events event WHERE tenant_id=%s', (self.tenant,)).fetchall()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertNotIn('native_conversation', row['compact']['provenance'])

    def test_thinning_preserves_explicit_identity_metadata(self):
        self.add(self.sources[0])
        with self.store.connect() as c:
            rows = c.execute('SELECT ' + _compact_event_expression('event') + ' AS compact FROM canonical_events event WHERE tenant_id=%s', (self.tenant,)).fetchall()
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row['compact']['provenance'].get('native_conversation'),
                {'conversation_id': 'codex:' + self.native, 'strand_id': 'root'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
