#!/usr/bin/env python3
"""Real catalog/receipts plus a deliberately stale in-process search projection."""
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
import uuid

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval, arm_row
from recall_server.turbopuffer_plane import TurbopufferSettings, passage_row
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer


class ForgetFence(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store._pool.close()

    def setUp(self):
        self.populate()

    def populate(self, text="Synthetic forgotten deployment phrase."):
        nonce = uuid.uuid4().hex
        self.tenant, self.source = 'tenant:forget:' + nonce, 'codex:forget:' + nonce
        self.principal = 'principal:forget:' + nonce
        with self.store.connect() as c:
            insert_source(c, self.tenant, self.principal, self.source)
            self.receipt = insert_record(c, tenant=self.tenant, source=self.source,
                parent='parent:synthetic', native='turn:synthetic',
                text=text, role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                native_ids=['turn:synthetic'], reason='ingest')
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        archive = FilesystemArchiveStore(Path(tmp.name) / 'archive', namespace_key=b'f'*32)
        projection = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(self.store, projection, bound_tenant_id=self.tenant, raw_archive=archive)
        logical.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1, upload_concurrency=1)
        passages = CanonicalPassageProjector(self.store, projection, policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        passages.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1)
        with self.store.connect() as c:
            rows = c.execute('''SELECT passage.*, evidence.native_parent_id,evidence.manifest_object_key,
                evidence.manifest_content_sha256,evidence.first_occurred_at AS doc_first_occurred_at,
                evidence.last_occurred_at AS doc_last_occurred_at, '[]'::jsonb AS actors
                FROM canonical_passages passage JOIN canonical_evidence_documents evidence
                USING(tenant_id,source_id,logical_document_id)
                WHERE passage.tenant_id=%s AND passage.source_id=%s''', (self.tenant,self.source)).fetchall()
        self.assertGreaterEqual(len(rows), 1)
        self.client = FakeTurbopuffer()
        self.settings = TurbopufferSettings(api_key='synthetic-local-only')
        self.ns = self.client.namespace(self.settings.namespace(self.tenant))
        self.vendor_rows = [passage_row(row) for row in rows]
        self.ns.write(upsert_rows=self.vendor_rows)
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=[self.source], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=self.settings, client=self.client)

    def search(self, *, include_arms=True):
        return self.retrieval.search('forgotten deployment phrase', lexical_query='deployment phrase',
            since=None, until=None, limit=10, include_arms=include_arms)

    def test_body_retired_then_deleting_and_deleted_receipt(self):
        with self.store.connect() as c:
            c.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (self.tenant,self.source))
        live = self.search()
        self.assertEqual(len(live['results']), 1)
        self.assertEqual(live['diagnostics']['authority_status'], 'ok')
        self.assertIn('Synthetic forgotten', live['results'][0]['matching_ranges'][0]['text'])
        self.assertEqual(len(self.search(include_arms=False)['results']), 1)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s', (self.tenant,self.source))
        for phase in ('deleting', 'deleted'):
            with self.subTest(phase=phase):
                if phase == 'deleted':
                    with self.store.connect() as c:
                        c.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s', (self.tenant,self.source))
                # A worker can finish an old TP write after the canonical fence.
                self.ns.write(upsert_rows=self.vendor_rows)
                self.ns.queries.clear()
                response = self.search()
                self.assertEqual(response['results'], [])
                self.assertTrue(all(not rows for rows in response['arms'].values()))
                self.assertEqual(response['diagnostics']['authority_status'], 'ok')
                self.assertFalse(any(q['rank_by'] == ('id','asc') for q in self.ns.queries))
                self.assertEqual(self.search(include_arms=False)['results'], [])

    def test_authorized_vendor_row_without_current_canonical_document_is_hidden(self):
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=false WHERE tenant_id=%s AND source_id=%s', (self.tenant,self.source))
        self.assertEqual(self.search()['results'], [])

    def test_overlapping_passages_check_each_receipt_once(self):
        # Many genuine overlapping windows share one canonical receipt. The
        # authority owner should probe its metadata once, not once per window.
        self.populate(' '.join('deploymentword' + str(i) for i in range(12000)))
        self.assertGreater(len(self.vendor_rows), 10)
        plans = []
        owner = self.store
        class ObservedStore:
            def __getattr__(self, name):
                return getattr(owner, name)
            def _execute_bounded(self, connection, statement, values, deadline):
                if 'cardinality(passage.receipts)>0' in statement:
                    plans.append(connection.execute(
                        'EXPLAIN (ANALYZE, FORMAT JSON) ' + statement, values,
                    ).fetchone()['QUERY PLAN'][0]['Plan'])
                return owner._execute_bounded(connection, statement, values, deadline)
        self.retrieval.store = ObservedStore()
        result = self.search()
        self.assertEqual(len(result['results']), 1)
        self.assertEqual(len(plans), 1)
        def nodes(plan):
            yield plan
            for child in plan.get('Plans', []):
                yield from nodes(child)
        chunk_scans = [node for node in nodes(plans[0])
                       if node.get('Relation Name') == 'canonical_chunks']
        self.assertTrue(chunk_scans)
        unique_receipts = {receipt for row in self.vendor_rows for receipt in row['receipts']}
        self.assertLessEqual(sum(node['Actual Loops'] for node in chunk_scans), len(unique_receipts))
        # One missing member must reject every passage containing it. Keep
        # this content-level assertion beside the physical-work assertion.
        forgotten = sorted(unique_receipts)[0]
        with self.store.connect() as c:
            c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s AND receipt=%s',
                      (self.tenant, forgotten))
        rows = [arm_row(row, 1.0) for row in self.vendor_rows]
        expected = {row['id'] for row in self.vendor_rows if forgotten not in row['receipts']}
        diagnostics = self.retrieval._authorize_ranges([], (('dense',1.0,rows),), deadline_at=time.monotonic()+5, include_arms=True)
        self.assertEqual(diagnostics['authority_status'], 'ok')
        self.assertEqual({row['passage_id'] for row in rows}, expected)

    def test_vendor_namespace_cannot_authorize_foreign_source_or_tenant(self):
        # Keep a real live passage in PG, then put its ID in an unrelated namespace.
        other = TurbopufferSettings(api_key='synthetic-local-only')
        fake_tenant = self.tenant + ':other'
        self.client.namespace(other.namespace(fake_tenant)).write(upsert_rows=self.vendor_rows)
        retrieval = TurbopufferHintRetrieval(self.store, tenant_id=fake_tenant,
            sources=[self.source], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=other, client=self.client)
        result = retrieval.search('deployment phrase', lexical_query='deployment phrase', since=None, until=None, limit=10)
        self.assertEqual(result['results'], [])


if __name__ == '__main__':
    unittest.main()
