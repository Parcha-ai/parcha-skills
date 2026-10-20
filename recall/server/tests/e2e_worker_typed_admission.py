#!/usr/bin/env python3
"""Ordinary native admission upgrades historical modes without replaying bodies."""
import time
import unittest
from unittest.mock import patch
from e2e_codex_tool_publication import CodexToolPublication
from e2e_tool_group_publication import ToolGroupPublication
from recall_server import passage_index, passage_append


class WorkerTypedAdmission(CodexToolPublication):
    def legacy(self, mode='legacy'):
        self.ingest('visible', value={'type': 'response_item', 'payload': {
            'type': 'message', 'role': 'user', 'content': [
                {'type': 'input_text', 'text': 'Historical visible context.'}]}})
        self.ingest('result', result=True, second=1)
        with patch.object(passage_index, '_document_extraction_version', return_value=mode):
            self.assertEqual(self.project()['documents'], 1)
        row = self.document()
        self.assertEqual(row['append_checkpoint']['version'], passage_append.CODEX_CHECKPOINT_VERSION)
        self.assertEqual(row['applied_extraction_version'], mode)
        self.assertFalse(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))
        return row

    def admit(self, **kwargs):
        return self.passages.admit_typed_pending(tenant_id=self.tenant,
            limit=2, deadline_at=time.monotonic()+3, **kwargs)

    def legacy_parent(self, parent):
        ToolGroupPublication.ingest(self, parent, parent=parent, value={
            'type': 'response_item', 'payload': {'type': 'message', 'role': 'user',
                'content': [{'type': 'input_text', 'text': 'Archived visible context.'}]}})
        with patch.object(passage_index, '_document_extraction_version', return_value='legacy'):
            self.project()

    def test_native_admission_preserves_restart_progress_then_publishes_tool_output(self):
        self.legacy()
        with patch.object(self.passages.logical_projection, 'read_part',
                wraps=self.passages.logical_projection.read_part) as archive_read:
            self.assertEqual(self.admit()['queued'], 1)
            self.assertEqual(archive_read.call_count, 0)
        self.assertEqual(self.document()['requested_extraction_version'], 'codex_tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        queued = self.passages._pending(tenant_id=self.tenant, limit=1)[0]
        restarted = passage_index.CanonicalPassageProjector(self.store,
            self.passages.logical_projection, policy=self.passages.policy,
            bound_tenant_id=self.tenant)
        self.assertEqual(restarted.admit_typed_pending(tenant_id=self.tenant,
            limit=2, deadline_at=time.monotonic()+3)['queued'], 0)
        self.assertEqual(self.passages._pending(tenant_id=self.tenant, limit=1)[0].generation,
                         queued.generation)
        self.assertEqual(self.project()['documents'], 1)
        self.assertTrue(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))
        self.assertEqual(self.admit()['queued'], 0)
        for row in self.projected():
            self.assertTrue(self.grant_and_open(row)['complete'])

    def test_ordinary_worker_discovers_finished_legacy_without_manual_enrollment(self):
        from recall_server.projection_worker import run_projection_worker
        self.legacy()
        run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
            logical_batch_size=1, passage_batch_size=2, embedding_batch_size=1,
            max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
            interval_seconds=1, once=True, skip_embedding=True)
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        self.assertTrue(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))

    def test_current_tool_groups_mode_also_upgrades_without_old_checkpoint(self):
        self.legacy(mode='tool_groups_v1')
        self.assertEqual(self.admit()['queued'], 1)
        self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        self.assertTrue(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))

    def test_admission_sql_deadline_does_not_block_ready_publication(self):
        from recall_server.projection_worker import run_projection_worker
        self.ingest('result', result=True)
        self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
            max_batches=1, upload_concurrency=1)
        def cancelled(**kwargs):
            with self.store.connect() as connection:
                self.store._execute_bounded(connection, 'SELECT pg_sleep(1)', (),
                                            time.monotonic() + .02)
        with patch.object(self.passages, 'admit_typed_pending', side_effect=cancelled) as admission:
            result = run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=2, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=1, once=True, skip_embedding=True)
        self.assertEqual(admission.call_count, 1)
        self.assertEqual(result['passage_documents'], 1)
        self.assertEqual(result['typed_admission_failed'], 1)
        self.assertTrue(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))

    def test_unknown_admission_ack_yields_without_blocking_queued_projection(self):
        from recall_server.projection_worker import run_projection_worker
        self.legacy()
        original = self.passages.admit_typed_pending
        def lost_ack(**kwargs):
            self.assertEqual(original(**kwargs)['queued'], 1)
            raise ConnectionError('synthetic lost admission ACK')
        with patch.object(self.passages, 'admit_typed_pending', side_effect=lost_ack) as admission:
            result = run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=2, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=1, once=True, skip_embedding=True)
        self.assertEqual(admission.call_count, 1)
        self.assertEqual(result['typed_admission_failed'], 1)
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        self.assertTrue(any('Sapphire tests failed' in p['text_redacted'] for p in self.projected()))

    def test_source_turns_and_requested_mode_survive_restart_without_duplicate_queue(self):
        bulk = self.source
        for i in range(3):
            self.legacy_parent(f'bulk-{i}')
        self.source = bulk + '-peer'
        self.legacy_parent('peer')
        first = self.admit()
        self.assertEqual(first['queued'], 2)
        self.assertEqual({r['source_id'] for r in first['outcomes']}, {bulk, self.source})
        with self.store.connect() as connection:
            before = connection.execute("""SELECT source_id,logical_document_id,generation
                FROM canonical_passage_projection_queue WHERE tenant_id=%s ORDER BY 1,2""",
                (self.tenant,)).fetchall()
        restarted = passage_index.CanonicalPassageProjector(self.store,
            self.passages.logical_projection, policy=self.passages.policy, bound_tenant_id=self.tenant)
        self.assertEqual(restarted.admit_typed_pending(tenant_id=self.tenant,
            limit=2, deadline_at=time.monotonic()+3)['queued'], 0)
        with self.store.connect() as connection:
            self.assertEqual(connection.execute("""SELECT source_id,logical_document_id,generation
                FROM canonical_passage_projection_queue WHERE tenant_id=%s ORDER BY 1,2""",
                (self.tenant,)).fetchall(), before)
        self.project()
        self.assertEqual(restarted.admit_typed_pending(tenant_id=self.tenant,
            limit=2, deadline_at=time.monotonic()+3)['queued'], 2)

    def test_revoked_owner_and_forget_between_selection_and_request_refuse(self):
        self.legacy()
        original = self.passages._request_extraction_version
        def revoke(**kwargs):
            with self.store.connect() as connection:
                connection.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s',
                                   (self.tenant,))
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=revoke):
            outcome = self.admit()
        self.assertEqual(outcome['outcomes'][0]['status'], 'refused')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.grant_and_open(self.projected()[0])
        self.assertEqual(self.admit()['queued'], 0, 'read permission is not worker owner authority')
        with self.store.connect() as connection:
            connection.execute("""UPDATE canonical_source_grants SET permission='owner'
                WHERE tenant_id=%s AND source_id=%s AND principal_id=%s""",
                (self.tenant,self.source,self.principal))
        def forget(**kwargs):
            with self.store.connect() as connection:
                connection.execute("""INSERT INTO canonical_evidence_document_queue(
                    tenant_id,source_id,native_parent_id,reason)
                    VALUES(%s,%s,'session-one','forget')""", (self.tenant,self.source))
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=forget):
            outcome = self.admit()
        self.assertEqual(outcome['outcomes'][0]['reason'], 'document_extraction_forget_pending')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')

    def test_lost_commit_ack_stops_batch_and_durable_mode_prevents_replay(self):
        self.legacy_parent('first')
        self.legacy_parent('second')
        original = self.passages._request_extraction_version
        committed = []
        def lost_ack(**kwargs):
            committed.append(kwargs['logical_document_id'])
            self.assertEqual(original(**kwargs)['status'], 'queued')
            raise ConnectionError('synthetic lost commit ACK')
        with patch.object(self.passages, '_request_extraction_version', side_effect=lost_ack):
            with self.assertRaises(ConnectionError):
                self.admit()
        self.assertEqual(len(committed), 1)
        next_turn = self.admit()
        self.assertEqual(next_turn['queued'], 1)
        self.assertNotEqual(next_turn['outcomes'][0]['logical_document_id'], committed[0])
        with self.store.connect() as connection:
            rows = connection.execute("""SELECT generation FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND reason='backfill'""", (self.tenant,)).fetchall()
        self.assertEqual([row['generation'] for row in rows], [1,1])

    def test_changed_revision_after_selection_refuses_without_replacing_queue(self):
        self.legacy()
        original = self.passages._request_extraction_version
        def append(**kwargs):
            self.ingest('appended', second=2, value={'type': 'response_item', 'payload': {
                'type': 'message', 'role': 'user', 'content': [
                    {'type': 'input_text', 'text': 'New current input remains authoritative.'}]}})
            self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
                max_batches=1, upload_concurrency=1)
            return original(**kwargs)
        with patch.object(self.passages, '_request_extraction_version', side_effect=append):
            result = self.admit()
        self.assertEqual(result['outcomes'][0]['reason'], 'document_extraction_catalog_changed')
        self.assertEqual(self.document()['requested_extraction_version'], 'legacy')
        self.assertEqual(len(self.passages._pending(tenant_id=self.tenant, limit=2)), 1)
        self.project()
        self.assertEqual(self.admit()['queued'], 1)

    def test_internal_scope_cannot_cross_tenant_or_claim_public_principal(self):
        row = self.legacy()
        with self.assertRaisesRegex(PermissionError, 'tenant'):
            self.passages.admit_typed_pending(tenant_id='another-tenant',
                limit=2, deadline_at=time.monotonic()+3)
        with self.assertRaisesRegex(ValueError, 'worker extraction scope'):
            self.passages._request_extraction_version(tenant_id=self.tenant,
                source_id=self.source, logical_document_id=row['logical_document_id'],
                revision=row['revision'],source_document_sha256=row['source_document_sha256'],
                extraction_version='codex_tool_groups_v1',principal_id=self.principal,worker=True)
        with self.assertRaisesRegex(TypeError, 'worker'):
            self.passages.request_extraction_version(tenant_id=self.tenant,
                source_id=self.source, logical_document_id=row['logical_document_id'],
                revision=row['revision'],source_document_sha256=row['source_document_sha256'],
                extraction_version='codex_tool_groups_v1',principal_id=self.principal,worker=True)


if __name__ == '__main__':
    suite = unittest.TestSuite(WorkerTypedAdmission(name) for name in
        WorkerTypedAdmission.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
