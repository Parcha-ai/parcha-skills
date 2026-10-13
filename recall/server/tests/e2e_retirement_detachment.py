#!/usr/bin/env python3
"""Real PostgreSQL: catalog replacement cannot resurrect retirement authority."""
# ruff: noqa: E402
import json
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
from e2e_archive_reprojection import fixture
from e2e_logical_source_integrity import TrackedStore
from recall_server import retirement_runner as runner
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane
from recall_server.chunk_retirement import (
    ChunkRetirementError, retire_current_chunks, retire_parent_chunks, set_parent_retirement_enabled,
)
from recall_server.parent_chunk_proof import parent_retirement_plan


class DetachmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = os.environ['RECALL_DATABASE_URL']
        cls.database = 'recall_detach_' + uuid.uuid4().hex
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'CREATE DATABASE "{cls.database}"')
        cls.store = TrackedStore(make_conninfo(**(
            conninfo_to_dict(cls.admin) | {'dbname': cls.database})))
        cls.store.migrate()

    @classmethod
    def tearDownClass(cls):
        cls.store.close()
        with psycopg.connect(cls.admin, autocommit=True) as c:
            c.execute(f'DROP DATABASE "{cls.database}" WITH (FORCE)')

    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.tenant, self.source, self.archive, _, self.projector, _ = fixture(
            self.store, Path(self.root.name), count=2)
        self.scope = runner.RetirementScope(
            self.tenant, 'principal:reprojection', (self.source,))
        self.key = self.tenant, self.source, 'session'
        self.target = dict(tenant_id=self.tenant, source_id=self.source,
                           native_parent_id='session')
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'owner',now())",
                      (self.tenant, self.scope.principal_id, self.source))
        runner.enroll_cohort(self.store, scope=self.scope, reviewed_plan=self.plan())

    def plan(self):
        return runner.plan_cohort(self.store, scope=self.scope, limit=1)

    def state(self, connection=None):
        sql = '''SELECT * FROM canonical_chunk_retirement_progress
                 WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s'''
        if connection is not None:
            return connection.execute(sql, self.key).fetchone()
        with self.store.connect() as c:
            return self.state(c)

    def replace(self, connection=None):
        if connection is None:
            with self.store.connect() as c, c.transaction():
                return self.replace(c)
        c = connection
        saved = c.execute('''SELECT to_jsonb(e) AS value
            FROM canonical_evidence_documents e
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''', self.key).fetchone()['value']
        parts = c.execute('''SELECT to_jsonb(p) AS value FROM canonical_evidence_document_parts p
            WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal''', self.key[:2]).fetchall()
        c.execute('''DELETE FROM canonical_evidence_documents
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s''', self.key)
        c.execute('''INSERT INTO canonical_evidence_documents
            SELECT * FROM jsonb_populate_record(NULL::canonical_evidence_documents,
                %s::jsonb || jsonb_build_object('created_at',clock_timestamp()))''',
                  (json.dumps(saved),))
        for part in parts:
            c.execute('''INSERT INTO canonical_evidence_document_parts
                SELECT * FROM jsonb_populate_record(NULL::canonical_evidence_document_parts, %s::jsonb)''',
                      (json.dumps(part['value']),))

    def rebind(self, epoch=None, plan=None):
        return runner.rebind_detached_parent(
            self.store, scope=self.scope, reviewed_plan=self.plan() if plan is None else plan,
            expected_scope_epoch=self.state()['scope_epoch'] if epoch is None else epoch)

    def test_enabled_catalog_deletion_revokes_direct_and_runner_authority(self):
        previous = self.state()
        self.replace()
        row = self.state()
        self.assertIsNotNone(row, 'catalog cascade erased the original scope')
        self.assertIsNone(row['logical_document_id'])
        self.assertFalse(row['enabled'])
        self.assertEqual(row['status'], 'detached')
        self.assertEqual(row['scope_epoch'], previous['scope_epoch'] + 1)
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            with self.assertRaisesRegex(ChunkRetirementError, 'parent_retirement_disabled'):
                retire_parent_chunks(self.store, self.archive, **self.target, apply=True,
                    reviewed_plan=parent_retirement_plan(self.key, self.plan()['parents'][0]['manifest']))
            report = runner.run_retirement(self.store, self.archive, scope=self.scope, apply=True)
        self.assertEqual(report['attempted_parents'], 0)
        self.assertEqual(sum(self.archive.reads.values()), 0)
        self.assertEqual(runner.enroll_cohort(self.store, scope=self.scope,
                         reviewed_plan=self.plan())['inserted_parents'], 0)
        self.assertEqual(self.state(), row)

    def test_disabled_intent_survives_catalog_recreation_and_explicit_rebind_refuses(self):
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        self.replace()
        row = self.state()
        self.assertIsNotNone(row)
        self.assertFalse(row['enabled'])
        self.assertEqual(row['status'], 'disabled')
        self.assertIsNone(row['logical_document_id'])
        with self.assertRaises(ChunkRetirementError):
            self.rebind()
        self.assertEqual(self.state(), row)

    def test_explicit_rebind_requires_current_catalog_epoch_and_starts_fresh_grace(self):
        old_plan = self.plan()
        self.replace()
        detached = self.state()
        with self.assertRaises(ChunkRetirementError):
            self.rebind(plan=old_plan)
        with self.assertRaises(ChunkRetirementError):
            self.rebind(epoch=detached['scope_epoch'] - 1)
        result = self.rebind()
        self.assertEqual(result['status'], 'rebound')
        current = self.state()
        self.assertTrue(current['enabled'])
        self.assertEqual(current['status'], 'pending')
        self.assertEqual(current['scope_epoch'], detached['scope_epoch'] + 1)
        self.assertEqual(current['last_record_ordinal'], -1)
        self.assertIsNone(current['manifest_artifact_id'])
        self.assertIsNotNone(current['logical_document_id'])
        with self.assertRaises(ChunkRetirementError):
            self.rebind(epoch=detached['scope_epoch'])
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            self.assertEqual(runner.run_retirement(self.store, self.archive,
                             scope=self.scope, apply=True)['attempted_parents'], 0)

    def test_disable_after_detach_cancels_rebind_permission(self):
        self.replace()
        detached = self.state()
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        with self.assertRaises(ChunkRetirementError):
            self.rebind(epoch=detached['scope_epoch'])
        with self.assertRaises(ChunkRetirementError):
            self.rebind()
        self.assertEqual(self.state()['status'], 'disabled')

    def test_restore_after_detach_revokes_rebind_and_preserves_body(self):
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            preview = retire_parent_chunks(self.store, self.archive, **self.target)
            cleared = retire_parent_chunks(self.store, self.archive, **self.target,
                apply=True, reviewed_plan=preview['plan'])
            self.assertTrue(cleared['complete'])
        self.replace()
        with self.store.connect() as c:
            documents = c.execute("SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s", self.key[:2]).fetchall()
        targets = dict(tenant_id=self.tenant, source_id=self.source,
                       document_ids=tuple(row['document_id'] for row in documents))
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            plan = retire_current_chunks(self.store, self.archive, **targets, restore=True)
            retire_current_chunks(self.store, self.archive, **targets, restore=True,
                                  apply=True, reviewed_plan=plan['plan'])
        self.assertEqual(self.state()['status'], 'disabled')
        with self.store.connect() as c:
            restored = c.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted<>''", self.key[:2]).fetchone()['n']
        self.assertEqual(restored, 2)
        with self.assertRaises(ChunkRetirementError):
            self.rebind()

    def test_delete_during_archive_proof_prevents_inflight_clear(self):
        plan = parent_retirement_plan(self.key, self.plan()['parents'][0]['manifest'])
        self.archive.on_read = self.replace
        with patch.dict(os.environ, RECALL_CHUNK_BODY_READS='archive'):
            with self.assertRaises(ChunkRetirementError):
                retire_parent_chunks(self.store, self.archive, **self.target,
                                     apply=True, reviewed_plan=plan)
        self.assertEqual(self.state()['status'], 'detached')
        self.assertFalse(self.state()['enabled'])
        with self.store.connect() as c:
            cleared = c.execute("SELECT count(*) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND text_redacted=''", self.key[:2]).fetchone()['n']
        self.assertEqual(cleared, 0)

    def test_legacy_enable_cannot_bind_a_detached_catalog(self):
        self.replace()
        before = self.state()
        with self.assertRaises(psycopg.errors.CheckViolation):
            set_parent_retirement_enabled(self.store, **self.target, enabled=True)
        self.assertEqual(self.state(), before)

    def test_rebind_requires_current_owner_grant(self):
        self.replace()
        with self.store.connect() as c:
            c.execute("DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s", self.key[:2])
        with self.assertRaises(ChunkRetirementError):
            self.rebind()
        self.assertEqual(self.state()['status'], 'detached')

    def test_detachment_migration_can_repeat_without_changing_intent(self):
        self.replace()
        before = self.state()
        with self.store.connect() as c:
            c.execute((SERVER / 'schema/075_retirement_catalog_detachment.sql').read_text())
        self.assertEqual(self.state(), before)

    def test_partial_forget_preserves_hold_full_forget_removes_it_and_replay_stays_denied(self):
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        plane = CanonicalPlane(self.store, self.archive, evidence_projector=self.projector,
                               chunk_body_archive=self.archive)
        def forget(native):
            return plane.forget(dict(contract='recall.forget-request.v1', schema_version=1,
                tenant_id=self.tenant, principal_id=self.scope.principal_id, source_id=self.source,
                target_receipt=f'recall://{self.source}/{native}?rev=1#item=0',
                mode='explicit_forget', reason='owner_requested',
                requested_at='2026-10-13T00:00:00Z', idempotency_key='forget-' + native))
        self.assertEqual(forget('event-0000')['status'], 'deleted')
        self.assertEqual(self.state()['status'], 'disabled')
        self.assertIsNone(self.state()['logical_document_id'])
        self.assertEqual(forget('event-0001')['status'], 'deleted')
        self.assertIsNone(self.state(), 'fully forgotten parent retained plaintext scope')
        self.assertTrue(forget('event-0001')['replay'])
        gateway = CanonicalArchiveGateway(self.store, self.archive,
            tenant_id=self.tenant, principal_id=self.scope.principal_id)
        with self.assertRaises(CanonicalLifecycleError) as error:
            gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
                native_id='event-0000', payload=b'{}', media_type='application/json',
                created_at='2026-10-13T00:00:00Z')
        self.assertEqual(error.exception.error_code, 'archive_identity_forgotten')
        self.assertIsNone(self.state())
        self.assertEqual(self.plan()['parents'], [])

    def test_detached_metadata_cannot_outlive_its_source(self):
        source = 'codex:empty:' + uuid.uuid4().hex
        with self.store.connect() as c:
            c.execute("INSERT INTO canonical_sources(tenant_id,source_id,owner_principal_id) VALUES(%s,%s,%s)",
                      (self.tenant, source, self.scope.principal_id))
            c.execute("INSERT INTO canonical_chunk_retirement_progress(tenant_id,source_id,native_parent_id) VALUES(%s,%s,'held')", (self.tenant, source))
            c.execute("DELETE FROM canonical_sources WHERE tenant_id=%s AND source_id=%s", (self.tenant, source))
            count = c.execute("SELECT count(*) AS n FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s", (self.tenant, source)).fetchone()['n']
        self.assertEqual(count, 0)

    def test_delete_rollback_restores_ledger_exactly(self):
        before = self.state()
        with self.assertRaisesRegex(RuntimeError, 'rollback'):
            with self.store.connect() as c, c.transaction():
                self.replace(c)
                self.assertFalse(self.state(c)['enabled'])
                raise RuntimeError('rollback')
        self.assertEqual(self.state(), before)

    def test_rebind_refuses_concurrent_disable_and_observes_committed_epoch(self):
        self.replace()
        stale = self.state()['scope_epoch']
        with self.store.connect() as c, c.transaction():
            c.execute('''UPDATE canonical_chunk_retirement_progress SET enabled=false,
                status='disabled',scope_epoch=scope_epoch+1 WHERE tenant_id=%s
                AND source_id=%s AND native_parent_id=%s''', self.key)
            with self.assertRaises((ChunkRetirementError, psycopg.errors.LockNotAvailable)):
                self.rebind(epoch=stale)
        with self.assertRaises(ChunkRetirementError):
            self.rebind(epoch=stale)
        self.assertEqual(self.state()['status'], 'disabled')


if __name__ == '__main__':
    unittest.main()
