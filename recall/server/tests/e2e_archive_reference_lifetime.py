#!/usr/bin/env python3
"""Shared archive objects survive cleanup and concurrent publication failures."""
# ruff: noqa: E402
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.db import BrainStore
from recall_server.evidence_projection import CanonicalEvidenceProjector, EvidenceProjectionStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector
from recall_server.projectors import canonical_json


class ArchiveReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = os.environ['RECALL_DATABASE_URL']
        cls.database = 'recall_historical_owner_' + uuid.uuid4().hex
        settings = conninfo_to_dict(cls.admin)
        settings['dbname'] = cls.database
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{cls.database}"')
        cls.store = BrainStore(make_conninfo(**settings))
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'DROP DATABASE "{cls.database}"')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='recall-historical-owner-')
        self.addCleanup(self.temporary.cleanup)
        self.tenant = 'tenant:history-' + uuid.uuid4().hex[:8]
        self.source, self.principal = 'source:history', 'principal:history'
        self.archive = FilesystemArchiveStore(Path(self.temporary.name), namespace_key=b'h'*32)
        self.evidence = CanonicalEvidenceProjector(self.store, EvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant)
        self.logical = CanonicalLogicalEvidenceProjector(self.store, LogicalEvidenceProjectionStore(self.archive), bound_tenant_id=self.tenant, raw_archive=self.archive)
        self.plane = CanonicalPlane(self.store, self.archive, evidence_projector=self.evidence, chunk_body_archive=self.archive)
        self.gateway = CanonicalArchiveGateway(self.store, self.archive, tenant_id=self.tenant, principal_id=self.principal)
        self.environment = patch.dict(os.environ, RECALL_HISTORICAL_BODY_WRITES='inline')
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def event(self, native='turn', revision=1):
        content = {'text': (f'{native} revision{revision}: α 🧠\n'*100), 'role': 'user'}
        reference = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source, native_id=native,
            payload=canonical_json(content), media_type='application/json', created_at='2026-10-13T00:00:00Z')
        return dict(schema_version=1,source_id=self.source,native_id=native,native_parent_id=native,
            kind='connector_record',occurred_at='2026-10-13T00:00:00Z',observed_at='2026-10-13T00:00:00Z',
            principal_id=self.principal,visibility='private',content_type='application/json',content=content,
            provenance={'connector_id':'synthetic.history','artifact_ref':reference},
            content_sha256=hashlib.sha256(canonical_json(content)).hexdigest())

    def write(self, event):
        return self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[event])

    def test_cleanup_preserves_existing_evidence_catalog_reference(self):
        self.write(self.event())
        self.evidence.project_pending(tenant_id=self.tenant, batch_size=10,
                                      max_batches=1, upload_concurrency=1)
        reference = CanonicalEvidenceProjector._reference(self.references()[0])
        before = self.archive.read_raw(reference)
        self.logical._schedule_cleanup((reference,))
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.archive.read_raw(reference), before)

    def test_cleanup_preserves_existing_raw_artifact_reference(self):
        event = self.event()
        self.write(event)
        reference = event['provenance']['artifact_ref']
        before = self.archive.read_raw(reference)
        self.logical._schedule_cleanup((reference,))
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(self.archive.read_raw(reference), before)

    def references(self):
        with self.store.connect() as c:
            return c.execute('SELECT * FROM canonical_evidence_objects WHERE tenant_id=%s',(self.tenant,)).fetchall()

    def test_losing_publisher_does_not_delete_concurrent_winners_object(self):
        self.write(self.event('first'))
        self.write(self.event('second'))
        original_put=self.evidence.projection.put
        uploads=[]
        def loser_put(**kwargs):
            if uploads:
                raise OSError('synthetic second upload failure')
            reference=original_put(**kwargs)
            uploads.append(reference)
            # The winning publisher independently commits the same immutable
            # first object while this upload result has not yet been registered.
            with patch.object(self.evidence.projection,'put',side_effect=original_put):
                winner=self.evidence.project_pending(tenant_id=self.tenant,
                    batch_size=10,max_batches=1,upload_concurrency=1)
            self.assertEqual(winner['processed'],2)
            return reference
        with patch.object(self.evidence.projection,'put',side_effect=loser_put):
            with self.assertRaises(OSError):
                self.evidence.project_pending(tenant_id=self.tenant,
                    batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(len(self.references()),2)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_evidence_cleanup_queue WHERE tenant_id=%s',(self.tenant,)).fetchone()['n'],1)
        self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertGreater(len(self.archive.read_raw(uploads[0])),0)

    def test_cleanup_defers_an_artifact_held_by_existing_admission_guard(self):
        from recall_server.canonical import _lock_artifacts
        event=self.event()
        reference=event['provenance']['artifact_ref']
        self.logical._schedule_cleanup((reference,))
        with self.store.connect() as c, c.transaction():
            _lock_artifacts(c,self.tenant,self.source,[reference['artifact_id']])
            result=self.logical.drain_cleanup(tenant_id=self.tenant)
            self.assertEqual(result['deleted'],0)
            self.assertEqual(result['pending'],1)
            self.assertGreater(len(self.archive.read_raw(reference)),0)
        result=self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(result['deleted'],1)
        self.assertEqual(result['pending'],0)


    def test_reference_committed_before_artifact_lock_is_seen_by_fresh_snapshot(self):
        from recall_server import canonical
        event=self.event()
        reference=event['provenance']['artifact_ref']
        self.logical._schedule_cleanup((reference,))
        original=canonical._lock_artifacts
        committed=[]
        def commit_then_lock(connection,tenant,source,artifacts,**kwargs):
            if not committed:
                committed.append(True)
                self.write(event)
            return original(connection,tenant,source,artifacts,**kwargs)
        with patch.object(canonical,'_lock_artifacts',side_effect=commit_then_lock):
            result=self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(result['deleted'],0)
        self.assertEqual(result['completed'],1)
        self.assertGreater(len(self.archive.read_raw(reference)),0)

    def test_failed_unowned_upload_is_durably_queued_then_reclaimed(self):
        self.write(self.event('first'))
        self.write(self.event('second'))
        original=self.evidence.projection.put
        uploaded=[]
        def put(**kwargs):
            if uploaded:
                raise OSError('synthetic second upload failure')
            reference=original(**kwargs)
            uploaded.append(reference)
            return reference
        with patch.object(self.evidence.projection,'put',side_effect=put):
            with self.assertRaises(OSError):
                self.evidence.project_pending(tenant_id=self.tenant,
                    batch_size=10,max_batches=1,upload_concurrency=1)
        self.assertEqual(self.references(),[])
        self.assertGreater(len(self.archive.read_raw(uploaded[0])),0)
        result=self.logical.drain_cleanup(tenant_id=self.tenant)
        self.assertEqual(result['deleted'],1)
        self.assertEqual(result['pending'],0)



if __name__ == '__main__':
    unittest.main()
