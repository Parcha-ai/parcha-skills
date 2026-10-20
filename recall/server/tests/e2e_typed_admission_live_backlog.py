#!/usr/bin/env python3
"""Native typed admission progresses alongside unrelated live publication."""
import unittest
import time

from e2e_worker_typed_admission import WorkerTypedAdmission
from e2e_tool_group_publication import ToolGroupPublication
from recall_server import passage_index, parent_membership_enrollment
from recall_server.projection_worker import run_projection_worker


class TypedAdmissionLiveBacklog(WorkerTypedAdmission):
    def queue_live(self, name):
        original_source = self.source
        self.source = 'claude:' + original_source
        try:
            ToolGroupPublication.ingest(self, name, parent=name, value={
                'type': 'assistant', 'message': {'role': 'assistant',
                    'content': [{'type': 'text', 'text': 'Current live work remains queued.'}]}})
            self.logical.project_pending(tenant_id=self.tenant, batch_size=1,
                max_batches=1, upload_concurrency=1)
        finally:
            self.source = original_source

    def test_unrelated_live_backlog_does_not_starve_legacy_upgrade(self):
        self.legacy()
        self.queue_live('live-one')
        self.queue_live('live-two')
        self.assertEqual(self.passages.count_pending(tenant_id=self.tenant)['pending'], 2)
        self.assertEqual(self.admit()['queued'], 1)
        self.assertEqual(self.document()['requested_extraction_version'], 'codex_tool_groups_v1')
        self.assertEqual(self.document()['applied_extraction_version'], 'legacy')
        self.assertEqual(self.passages.count_pending(tenant_id=self.tenant)['pending'], 3)

    def test_upgrade_window_survives_restart_and_refills_during_live_arrivals(self):
        for index in range(4):
            self.legacy_parent(f'legacy-{index}')
        self.queue_live('live-one')
        self.queue_live('live-two')
        first = self.admit()
        self.assertEqual(first['queued'], 2)
        restarted = passage_index.CanonicalPassageProjector(self.store,
            self.passages.logical_projection, policy=self.passages.policy,
            bound_tenant_id=self.tenant)

        def admit():
            return restarted.admit_typed_pending(tenant_id=self.tenant,
                limit=2, deadline_at=time.monotonic()+3)

        def outstanding():
            with self.store.connect() as connection:
                return connection.execute("""SELECT count(*) AS n FROM canonical_passage_documents
                    WHERE tenant_id=%s AND requested_extraction_version<>applied_extraction_version""",
                    (self.tenant,)).fetchone()['n']

        self.assertEqual(admit()['queued'], 0)
        self.assertEqual(outstanding(), 2)
        for index in range(8):
            self.queue_live(f'arrival-{index}')
            restarted.project_pending(tenant_id=self.tenant,
                batch_size=1, max_batches=1, concurrency=1)
            if outstanding() < 2:
                break
        self.assertEqual(outstanding(), 1, 'native bounded projection must complete an upgrade')
        self.assertGreater(self.passages.count_pending(tenant_id=self.tenant)['pending'], 1)
        self.assertEqual(admit()['queued'], 1)
        self.assertEqual(outstanding(), 2)
        self.assertEqual(admit()['queued'], 0)

    def test_ordinary_worker_publishes_upgrade_with_existing_live_backlog(self):
        self.legacy()
        self.queue_live('live-one')
        self.queue_live('live-two')
        for _ in range(4):
            run_projection_worker(self.logical, self.passages, tenant_id=self.tenant,
                logical_batch_size=1, passage_batch_size=2, embedding_batch_size=1,
                max_batches_per_cycle=1, upload_concurrency=1, passage_concurrency=1,
                interval_seconds=1, once=True, skip_embedding=True)
            if self.document()['applied_extraction_version'] == 'codex_tool_groups_v1':
                break
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        self.assertTrue(any('Sapphire tests failed' in row['text_redacted']
                            for row in self.projected()))

    def test_certification_queued_legacy_upgrades_then_certifies_new_passages(self):
        row = self.legacy()
        self.assertTrue(self.grant_and_open(self.projected()[0])['complete'])
        self.queue_live('live-one')
        self.queue_live('live-two')
        with self.store.connect() as connection:
            connection.execute("""INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason)
                VALUES(%s,%s,%s,%s,1,'certification')""",
                (self.tenant,self.source,row['logical_document_id'],row['revision']))
        self.assertEqual(self.admit()['queued'], 1)
        with self.store.connect() as connection:
            queued = connection.execute("""SELECT reason,generation FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                (self.tenant,self.source,row['logical_document_id'])).fetchone()
        self.assertEqual(queued, {'reason': 'backfill', 'generation': 2})
        self.passages.membership_certification = parent_membership_enrollment.CertificationSettings(
            enabled=True, binding_epoch='synthetic-live-backlog')
        report = self.project()
        self.assertEqual(self.document()['applied_extraction_version'], 'codex_tool_groups_v1')
        self.assertGreater(report['membership_certified_passages'], 0, report)
        with self.store.connect() as connection:
            passages = connection.execute("""SELECT membership_generation,membership_binding_sha256
                FROM canonical_passages WHERE tenant_id=%s AND source_id=%s
                AND logical_document_id=%s""",
                (self.tenant,self.source,row['logical_document_id'])).fetchall()
        self.assertTrue(passages)
        self.assertTrue(all(p['membership_generation'] is not None
                            and p['membership_binding_sha256'] for p in passages))


if __name__ == '__main__':
    suite = unittest.TestSuite(TypedAdmissionLiveBacklog(name) for name in
        TypedAdmissionLiveBacklog.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
