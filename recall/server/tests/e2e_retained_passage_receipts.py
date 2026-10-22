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

    def test_retained_missing_vendor_row_is_reconciled_after_native_rebuild(self):
        from recall_server.search_outbox import enqueue_search_outbox

        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        before = self.rows()
        self.assertEqual(len(before), 1)
        missing_id = before[0]['passage_id']
        # Reproduce the observed absence, without asserting how it arose.
        del namespace.rows[missing_id]
        self.ingest(2)
        rebuilt = self.project()
        self.assertEqual(rebuilt['retained'], 1)
        self.assertEqual(self.rows()[0]['passage_id'], missing_id)
        with self.store.connect() as connection:
            month = connection.execute(
                "SELECT month FROM search_projection_shards WHERE tenant_id=%s",
                (self.tenant,)).fetchone()['month']
            enqueue_search_outbox(connection, tenant_id=self.tenant,
                source_id=self.source, months=[month], reason='logical-update')
        drained = self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.assertEqual(drained['failed'], 0)
        self.assertIn(missing_id, namespace.rows,
            'native incremental publication skipped retained missing evidence')

    def reconciliation_row(self):
        with self.store.connect() as connection:
            return connection.execute("SELECT * FROM search_parent_reconciliation WHERE tenant_id=%s",
                (self.tenant,)).fetchone()

    def test_reconciliation_reuses_nomination_routes_without_a_third_authority_read(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        self.store.verified_singleton_receipts = True
        writer = self.restart_writer()
        with patch.object(writer, 'passages_by_id', wraps=writer.passages_by_id) as hydrate, \
                patch.object(search_parent_reconciliation, 'measure_metadata',
                             wraps=search_parent_reconciliation.measure_metadata) as measure:
            outcome = search_parent_reconciliation.step(writer, self.tenant)
        self.assertEqual(outcome, dict(attempted=1, examined=1, written=1))
        self.assertEqual(set(namespace.rows), {row['passage_id'] for row in self.rows()})
        self.assertEqual(hydrate.call_count, 2,
            'presence comparison and final locked authority are sufficient; routing needs no reread')
        self.assertEqual(measure.call_count, 1,
            'the final locked read retains its metadata budget')

    def test_reconciliation_uses_current_body_after_native_nomination(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        self.store.verified_singleton_receipts = True
        query = namespace.query
        def change_header(**kwargs):
            result = query(**kwargs)
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_passages SET header_redacted=%s WHERE tenant_id=%s',
                    ('Current header after nomination.', self.tenant))
            return result
        with patch.object(namespace, 'query', side_effect=change_header):
            outcome = search_parent_reconciliation.step(self.restart_writer(), self.tenant)
        self.assertEqual(outcome['written'], 1)
        self.assertEqual(next(iter(namespace.rows.values()))['header'], 'Current header after nomination.')

    def test_reconciliation_revision_change_after_nomination_refuses_write(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        self.store.verified_singleton_receipts = True
        query = namespace.query
        def revise(**kwargs):
            result = query(**kwargs)
            self.ingest(3, text='Changed current source evidence.')
            return result
        writes = len(namespace.writes)
        # Reach the revision race independently of the default search latency budget.
        with patch.object(self.store, 'search_deadline_ms', 5000), \
                patch.object(namespace, 'query', side_effect=revise):
            with self.assertRaisesRegex(ValueError, 'write_incomplete'):
                search_parent_reconciliation.step(self.restart_writer(), self.tenant)
        self.assertEqual(len(namespace.writes), writes)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)

    def test_reconciliation_month_move_after_nomination_keeps_work_owed(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        self.store.verified_singleton_receipts = True
        query = namespace.query
        def move_month(**kwargs):
            result = query(**kwargs)
            with self.store.connect() as connection:
                connection.execute("""UPDATE canonical_passages
                    SET first_occurred_at='2026-11-01T12:00:00Z',last_occurred_at='2026-11-01T12:00:00Z'
                    WHERE tenant_id=%s""", (self.tenant,))
            return result
        writes = len(namespace.writes)
        with patch.object(namespace, 'query', side_effect=move_month):
            with self.assertRaisesRegex(ValueError, 'write_incomplete'):
                search_parent_reconciliation.step(self.restart_writer(), self.tenant)
        self.assertEqual(len(namespace.writes), writes)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)
        # A new attempt nominates the new route and can settle the same work.
        outcome = search_parent_reconciliation.step(self.restart_writer(), self.tenant)
        self.assertEqual(outcome['written'], 1)

    def restart_writer(self, *, batch_rows=1):
        return TurbopufferProjector(self.store, self.settings, client=self.client,
            batch_rows=batch_rows, write_concurrency=1, tokens_per_minute=0)

    def test_parent_reconciliation_survives_hot_rebuilds_and_restarts(self):
        text = ' '.join(f'Synthetic archive segment {index} preserves useful evidence.' for index in range(500))
        self.ingest(1, text=text)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        ids = {row['passage_id'] for row in self.rows()}
        self.assertGreater(len(ids), 2)
        namespace.rows.clear()
        observed = []
        for index in range(len(ids)):
            self.ingest(index+2, text=text)
            self.project()
            outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
            self.assertEqual(outcome['failed'], 0)
            observed.append(namespace.writes[-1]['upsert_rows'][0]['id'])
            self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], index)
        self.assertEqual(set(observed), ids)
        self.assertEqual(len(observed), len(set(observed)))
        self.assertGreater(self.reconciliation_row()['generation'],
                           self.reconciliation_row()['page_checkpoint']['generation'])
        writes = len(namespace.writes)
        # EOF observes a newer generation, then the next pass skips correct rows.
        for _ in range(len(ids)+2):
            self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(len(namespace.writes), writes)

    def test_unknown_vendor_ack_rechecks_presence_without_reembedding(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        write = namespace.write
        def unknown(**kwargs):
            write(**kwargs)
            raise RuntimeError('synthetic ACK loss after vendor write')
        with patch.object(namespace, 'write', side_effect=unknown):
            outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 1)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)
        writes = len(namespace.writes)
        outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 0)
        self.assertEqual(len(namespace.writes), writes)
        self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertIsNone(self.reconciliation_row())

    def test_reconciliation_forget_between_presence_and_native_write(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        receipt = self.rows()[0]['receipts'][0]
        request = dict(contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-13T00:00:00Z', idempotency_key='forget-reconcile')
        query = namespace.query
        def forget(**kwargs):
            response = query(**kwargs)
            self.plane.forget(request)
            return response
        writes = len(namespace.writes)
        with patch.object(namespace, 'query', side_effect=forget):
            outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 1)
        self.assertEqual(len(namespace.writes), writes)
        self.assertEqual(namespace.rows, {})

    def test_parent_reconciliation_rolls_back_with_native_commit(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.ingest(2)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=2,
            max_batches=1, upload_concurrency=1)
        before = self.rows()
        enqueue = search_parent_reconciliation.enqueue
        def fail_after_enqueue(connection, *args):
            enqueue(connection, *args)
            self.assertEqual(connection.execute(
                'SELECT count(*) AS count FROM search_parent_reconciliation WHERE tenant_id=%s',
                (self.tenant,)).fetchone()['count'], 1)
            raise RuntimeError('synthetic failure after durable obligation')
        with patch.object(search_parent_reconciliation, 'enqueue', side_effect=fail_after_enqueue):
            with self.assertRaisesRegex(RuntimeError, 'after durable obligation'):
                self.passages.project_pending(tenant_id=self.tenant, batch_size=2,
                    max_batches=1, concurrency=1)
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(self.rows(), before)

    def test_unknown_sql_ack_resumes_committed_parent_cursor(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        execute = self.store._execute_bounded
        def lose_ack(connection, sql, args, deadline):
            result = execute(connection, sql, args, deadline)
            if 'SET page_checkpoint=%s WHERE' in sql:
                connection.commit()
                raise RuntimeError('synthetic SQL commit ACK loss')
            return result
        with patch.object(self.store, '_execute_bounded', side_effect=lose_ack):
            outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 1)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], 0)
        writes = len(namespace.writes)
        self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(len(namespace.writes), writes)

    def test_metadata_budget_refuses_before_hydration_without_advancing(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.ingest(2)
        self.project()
        writer = self.restart_writer()
        with patch.object(search_parent_reconciliation, 'MAX_METADATA_BYTES', 1):
            with patch.object(writer, 'passages_by_id', wraps=writer.passages_by_id) as hydrate:
                outcome = writer.drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 1)
        hydrate.assert_not_called()
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)

    def test_metadata_growth_after_presence_is_bounded_by_native_write_owner(self):
        from recall_server import search_parent_reconciliation
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        self.ingest(2)
        self.project()
        query = namespace.query
        def grow(**kwargs):
            response = query(**kwargs)
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_passages SET header_redacted=%s WHERE tenant_id=%s',
                    ('Synthetic metadata growth. '*2000, self.tenant))
            return response
        writes = len(namespace.writes)
        with patch.object(search_parent_reconciliation, 'MAX_METADATA_BYTES', 8192), \
                patch.object(namespace, 'query', side_effect=grow):
            outcome = self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 1)
        self.assertEqual(len(namespace.writes), writes)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], -1)

    def test_metadata_byte_prefix_advances_across_restart(self):
        from recall_server.passage_projection import PassagePolicy
        self.passages.policy = PassagePolicy(8, 0)
        text = ' '.join(f'segment{index}' for index in range(1100))
        self.ingest(1, text=text)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        ids = {row['passage_id'] for row in self.rows()}
        self.assertGreaterEqual(len(ids), 128)
        # Each native row fits8MiB; the first128 rows together do not.
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_passages SET header_redacted=%s WHERE tenant_id=%s',
                ('Synthetic large header. '*3200, self.tenant))
        self.ingest(2, text=text)
        self.project()
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        first = self.restart_writer(batch_rows=128).drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(first['failed'], 0, 'feasible metadata prefix must make progress')
        self.assertGreater(first['rows'], 0)
        self.assertLess(first['rows'], 128)
        self.assertEqual(self.reconciliation_row()['page_checkpoint']['after'], first['rows']-1)
        writes = first['rows']
        for _ in range(len(ids)):
            outcome = self.restart_writer(batch_rows=128).drain_quantum(tenant_id=self.tenant, max_months=2)
            self.assertEqual(outcome['failed'], 0)
            writes += outcome['rows']
            if self.reconciliation_row() is None:
                break
        self.assertIsNone(self.reconciliation_row())
        self.assertEqual(set(namespace.rows), ids)
        self.assertEqual(writes, len(ids), 'acknowledged prefixes must not be re-embedded')

    def test_destination_change_restarts_parent_presence_pass(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.ingest(2)
        self.project()
        self.restart_writer().drain_quantum(tenant_id=self.tenant, max_months=2)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        namespace.rows.clear()
        writer = self.restart_writer()
        writer.settings = TurbopufferSettings(api_key='synthetic-rotated-account')
        outcome = writer.drain_quantum(tenant_id=self.tenant, max_months=2)
        self.assertEqual(outcome['failed'], 0)
        self.assertEqual(len(namespace.rows), 1)

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
