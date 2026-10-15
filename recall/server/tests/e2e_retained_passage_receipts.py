#!/usr/bin/env python3
"""Canonical metadata revisions preserve searchable, current passage receipts."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]

from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY  # noqa: E402
from recall_server.projectors import canonical_json  # noqa: E402
from recall_server.turbopuffer_plane import TurbopufferSettings  # noqa: E402
from recall_server.turbopuffer_projection import TurbopufferProjector  # noqa: E402
from recall_server.turbopuffer_retrieval import TurbopufferHintRetrieval  # noqa: E402
from tests.central_brain.fake_turbopuffer import FakeTurbopuffer  # noqa: E402


class RetainedPassageReceipts(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        self.addCleanup(self.store.close)
        self.store.migrate()
        nonce = uuid.uuid4().hex
        self.tenant, self.source, self.principal = (
            f'{kind}:retained:{nonce}' for kind in ('tenant', 'source', 'principal')
        )
        directory = tempfile.TemporaryDirectory(prefix='recall-retained-receipts-')
        self.addCleanup(directory.cleanup)
        self.archive = FilesystemArchiveStore(Path(directory.name), namespace_key=b'p'*32)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=self.tenant, principal_id=self.principal)
        self.plane = CanonicalPlane(self.store, self.archive)
        projection = LogicalEvidenceProjectionStore(self.archive)
        self.logical = CanonicalLogicalEvidenceProjector(self.store, projection,
            bound_tenant_id=self.tenant, raw_archive=self.archive)
        self.passages = CanonicalPassageProjector(self.store, projection,
            policy=DEFAULT_PASSAGE_POLICY, bound_tenant_id=self.tenant)
        self.client = FakeTurbopuffer()
        self.settings = TurbopufferSettings(api_key='synthetic-local-only')
        self.writer = TurbopufferProjector(self.store, self.settings, client=self.client)
        self.retrieval = TurbopufferHintRetrieval(self.store, tenant_id=self.tenant,
            sources=[self.source], policy_fingerprint=DEFAULT_PASSAGE_POLICY.fingerprint,
            settings=self.settings, client=self.client)

    def ingest(self, revision, *, native='event-one', text='Sapphire archive receipt remains searchable.'):
        occurred = '2026-10-01T12:00:00Z'
        content = {'text': text, 'role': 'user', 'metadata_revision': revision}
        payload = canonical_json(content)
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=native, payload=payload, media_type='application/json', created_at=occurred)
        envelope = dict(schema_version=1, source_id=self.source, native_id=native,
            native_parent_id='session-one', kind='connector_record', occurred_at=occurred,
            observed_at=occurred, principal_id=self.principal, visibility='private',
            content_type='application/json', content=content,
            provenance=dict(connector_id='synthetic.retained-receipts', connector_schema_version=1,
                            artifact_ref=artifact),
            content_sha256=hashlib.sha256(payload).hexdigest())
        return self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id='synthetic.retained-receipts', artifact_ref=artifact,
            envelope=envelope, text_redacted=text)

    def project(self):
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2,
            max_batches=1, upload_concurrency=1)
        return self.passages.project_pending(tenant_id=self.tenant, batch_size=2,
            max_batches=1, concurrency=1)

    def search(self):
        result = self.retrieval.search('Sapphire archive receipt',
            lexical_query='Sapphire archive receipt', since=None, until=None,
            limit=10, include_arms=True)
        self.assertEqual(result['diagnostics']['authority_status'], 'ok')
        return result

    def rows(self):
        with self.store.connect() as connection:
            return connection.execute('''SELECT passage_id,receipts,revision,text_sha256,
                embed_sha256 FROM canonical_passages WHERE tenant_id=%s ORDER BY ordinal''',
                (self.tenant,)).fetchall()

    def test_metadata_revision_refreshes_retained_receipts_and_search(self):
        self.assertEqual(self.ingest(1)['inserted'], 1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        initial_writes = len(namespace.writes)
        before = self.rows()
        self.assertEqual(len(before), 1)
        self.assertEqual(len(self.search()['results']), 1)
        old_receipt = before[0]['receipts'][0]

        self.assertEqual(self.ingest(2)['inserted'], 1)
        # The asynchronous vendor copy must lose authority immediately.
        self.assertEqual(self.search()['results'], [])
        projected = self.project()
        self.assertEqual(projected['retained'], 1)
        after = self.rows()
        self.assertEqual(after[0]['passage_id'], before[0]['passage_id'])
        self.assertEqual(after[0]['text_sha256'], before[0]['text_sha256'])
        self.assertEqual(after[0]['embed_sha256'], before[0]['embed_sha256'])
        self.assertNotIn(old_receipt, after[0]['receipts'],
            'publisher retained a superseded receipt despite a new canonical revision')
        # An unchanged external body may keep its old receipt metadata. The
        # canonical authority response supplies current receipts before and
        # after an otherwise idle outbox drain, without another embedding.
        self.assertEqual(namespace.rows[after[0]['passage_id']]['receipts'], [old_receipt])
        result = self.search()
        self.assertEqual(len(result['results']), 1)
        self.assertNotIn(old_receipt, str(result['results']))
        with self.store.connect() as connection:
            organization = 'org:' + uuid.uuid4().hex
            connection.execute("INSERT INTO brain_organizations VALUES (%s,'company','Receipt test',now())", (organization,))
            connection.execute("INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES (%s,%s,'company',%s)",
                (self.tenant, organization, organization.replace(':', '-')))
            connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read')",
                (self.tenant, self.principal))
            connection.execute("INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES (%s,%s,%s,'read') ON CONFLICT DO NOTHING",
                (self.tenant, self.principal, self.source))
        hit = result['results'][0]
        target = {field: hit[field] for field in
                  ('source_id', 'logical_document_id', 'revision', 'manifest_content_sha256')}
        target['passage_id'] = hit['matching_ranges'][0]['passage_id']
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,))
        opened = bound.show(target)
        self.assertTrue(opened['complete'])
        self.assertEqual(opened['opened_receipts'], after[0]['receipts'])
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.assertEqual(len(namespace.writes), initial_writes)
        result = self.search()
        self.assertEqual(len(result['results']), 1)
        self.assertNotIn(old_receipt, str(result['results']))
        self.assertIn(after[0]['receipts'][0], str(result['results']))
        self.assertEqual(self.ingest(2)['inserted'], 0)
        self.assertEqual(self.project()['documents'], 0)
        self.assertEqual(self.rows(), after)

    def test_stale_vendor_body_hash_cannot_borrow_current_receipts(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.ingest(2)
        self.project()
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        for row in namespace.rows.values():
            row['text_sha256'] = hashlib.sha256(b'a different body').hexdigest()
        self.assertEqual(self.search()['results'], [])

    def test_pending_forget_rejects_refreshed_receipts_before_archive_cleanup(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.ingest(2)
        self.project()
        receipt = self.rows()[0]['receipts'][0]
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-13T00:00:00Z', idempotency_key='forget-one')
        with patch.object(self.plane.archive, 'delete_internal_raw_bounded', side_effect=RuntimeError('offline')):
            with self.assertRaises(CanonicalLifecycleError):
                self.plane.forget(request)
        self.assertEqual(self.search()['results'], [])
        self.assertEqual(self.plane.forget(request)['status'], 'deleted')
        self.assertEqual(self.search()['results'], [])

    def test_superseded_passage_build_cannot_publish_old_receipts(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        before = self.rows()
        self.ingest(2)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2,
            max_batches=1, upload_concurrency=1)
        projection = self.passages.logical_projection
        read_part = projection.read_part
        superseded = False

        def revise_after_archive_read(*args, **kwargs):
            nonlocal superseded
            payload = read_part(*args, **kwargs)
            if not superseded:
                superseded = True
                self.ingest(3)
                self.logical.project_pending(tenant_id=self.tenant, batch_size=2,
                    max_batches=1, upload_concurrency=1)
            return payload

        with patch.object(projection, 'read_part', side_effect=revise_after_archive_read):
            self.passages.project_pending(tenant_id=self.tenant, batch_size=2,
                max_batches=1, concurrency=1)
        self.assertTrue(superseded)
        self.assertEqual(self.rows(), before, 'obsolete build changed published receipts')
        self.assertEqual(self.search()['results'], [])
        self.project()
        current = self.rows()[0]['receipts'][0]
        self.assertIn('?rev=3#', current)
        result = self.search()
        self.assertEqual(len(result['results']), 1)
        self.assertIn(current, str(result['results']))
        self.assertNotIn('?rev=2#', str(result['results']))


if __name__ == '__main__':
    unittest.main()
