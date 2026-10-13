#!/usr/bin/env python3
"""Real PostgreSQL capacity sharing between old and recently changed parents."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL/'server')]
from e2e_logical_evidence_projection import insert_source, insert_record
from recall_server.archive import FilesystemArchiveStore
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector, mark_logical_evidence_dirty


class AdmissionSharing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = BrainStore(os.environ['RECALL_DATABASE_URL'], pool_max_size=4)
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.tenant = 'tenant:admission:' + uuid.uuid4().hex
        self.source = 'slack:synthetic'
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.archive = FilesystemArchiveStore(Path(tmp.name), namespace_key=b'f'*32)
        self.projection = LogicalEvidenceProjectionStore(self.archive)
        self.logical = self.projector()
        self.sources = set()
        self.selected = []
        prepare = self.logical._prepare_batch_and_upload
        def record(candidates, **kwargs):
            self.selected.extend(item.native_parent_id for item in candidates)
            return prepare(candidates, **kwargs)
        self.logical._prepare_batch_and_upload = record

    def projector(self):
        return CanonicalLogicalEvidenceProjector(self.store, self.projection,
            bound_tenant_id=self.tenant, raw_archive=self.archive)

    def add(self, parent, *, age, source=None, reason='ingest'):
        source = source or self.source
        with self.store.connect() as c:
            if source not in self.sources:
                insert_source(c, self.tenant, 'principal:test', source)
                self.sources.add(source)
            insert_record(c, tenant=self.tenant, source=source, parent=parent, native=parent,
                text='Synthetic capacity sharing proof.', role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=source,
                native_ids=[parent], reason=reason)
            # Synthetic clock setup only; production queue ages remain untouched.
            c.execute('''UPDATE canonical_evidence_document_queue
                SET first_queued_at=clock_timestamp()-%s*interval '1 second',
                    changed_at=clock_timestamp()-%s*interval '1 second'
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''',
                (age, age, self.tenant, source, parent))

    def run_round(self):
        return self.logical.project_pending(batch_size=1, max_batches=1,
            upload_concurrency=1, quiet_seconds=90, max_wait_seconds=600)

    def test_single_slot_across_calls_shares_continuous_changes_and_history(self):
        for index in range(4):
            self.add(f'history-{index}', age=86400-index)
        for index in range(4):
            # Historical replay and live edits both dirty parents. Selection
            # promises recent-change capacity, never source-event freshness.
            self.add(f'replay-{index}', age=240)
            self.add(f'changed-{index}', age=120)
            self.assertEqual(self.run_round()['documents'], 1)
        self.assertEqual(self.selected, ['history-0', 'changed-1', 'history-1', 'changed-3'])

    def test_empty_round_does_not_consume_recent_turn_and_restart_begins_oldest(self):
        self.add('first', age=86400)
        self.assertEqual(self.run_round()['documents'], 1)
        self.assertEqual(self.run_round()['documents'], 0)
        self.add('old', age=86400)
        self.add('recent', age=120)
        self.assertEqual(self.run_round()['documents'], 1)
        self.assertEqual(self.selected, ['first', 'recent'])
        self.add('newest', age=100)
        fresh_process = self.projector()
        selected = []
        prepare = fresh_process._prepare_batch_and_upload
        def record(candidates, **kwargs):
            selected.extend(item.native_parent_id for item in candidates)
            return prepare(candidates, **kwargs)
        fresh_process._prepare_batch_and_upload = record
        self.assertEqual(fresh_process.project_pending(batch_size=1, max_batches=1,
            upload_concurrency=1)['documents'], 1)
        self.assertEqual(selected, ['old'])

    def test_recent_mode_keeps_source_round_robin_and_eligibility(self):
        self.add('a-old', age=86400)
        self.add('a-recent', age=120)
        self.add('b-old', age=80000, source='slack:other')
        self.add('b-recent', age=180, source='slack:other')
        fair = self.logical._pending(tenant_id=self.tenant, limit=2,
            quiet_seconds=90, max_wait_seconds=600, prefer_recent=True)
        self.assertEqual({row.native_parent_id for row in fair}, {'a-recent', 'b-recent'})
        self.add('not-quiet', age=1)
        self.add('retry', age=110)
        self.add('quarantined', age=115)
        self.add('forget', age=0, reason='forget')
        with self.store.connect() as c:
            c.execute('''UPDATE canonical_evidence_document_queue
                SET next_attempt_at=clock_timestamp()+interval '1 hour'
                WHERE tenant_id=%s AND native_parent_id='retry' ''', (self.tenant,))
            c.execute('''UPDATE canonical_evidence_document_queue SET attempts=8
                WHERE tenant_id=%s AND native_parent_id='quarantined' ''', (self.tenant,))
            # Max-wait still admits an old parent being actively changed.
            c.execute('''UPDATE canonical_evidence_document_queue SET changed_at=clock_timestamp()
                WHERE tenant_id=%s AND native_parent_id='a-old' ''', (self.tenant,))
        pending = self.logical._pending(tenant_id=self.tenant, limit=10,
            quiet_seconds=90, max_wait_seconds=600, prefer_recent=True)
        self.assertEqual(pending[0].native_parent_id, 'forget')
        self.assertEqual({row.native_parent_id for row in pending},
            {'forget', 'a-recent', 'a-old', 'b-recent', 'b-old'})
        with self.assertRaisesRegex(LogicalEvidenceError, '^logical_evidence_tenant_not_configured$'):
            self.logical.project_pending(tenant_id='tenant:outside', batch_size=1, max_batches=1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
