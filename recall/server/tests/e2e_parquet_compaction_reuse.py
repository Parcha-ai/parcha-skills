"""Compaction repacks proven fragments without reopening unchanged source parts."""
import hashlib
import json
import unittest
from dataclasses import replace
from contextlib import contextmanager
from unittest.mock import patch

from e2e_parquet_sparse_planning import SparsePlanning
from recall_server import parquet_scan
from recall_server.archive import ArchiveNotFound
from recall_server.db import BrainStore


class CompactionReuse(SparsePlanning):
    def fragmented(self):
        for parent in ('alpha', 'beta'):
            self.ingest_parent(parent, parent + '/one', 'Synthetic fragmented ' + parent)
            self.project()
            self.scan.project_pending(tenant_id=self.tenant, batch_size=4,
                                      max_batches=1, compaction_budget=0)
        self.parts.stop()
        self.scan.compaction_fragments = 1
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        # Native compaction sweep distinguishes its marker from forced backfill.
        with self.store.connect() as c:
            c.execute("UPDATE canonical_parquet_scan_dirty_documents SET reason='compaction' "
                      "WHERE tenant_id=%s AND logical_document_id=%s",
                      (self.tenant, parquet_scan.SCAN_DIRTY_ALL))
        self.assertTrue(self.scan._catalog(candidate).fragmented(1))
        return candidate

    def assert_full_parity(self, candidate, expected_rewrite):
        original_read = self.archive.read_raw
        reads = []
        def read(ref):
            reads.append(ref)
            return original_read(ref)
        with patch.object(self.archive, 'read_raw', side_effect=read), \
             patch.object(self.scan, '_project_document', wraps=self.scan._project_document) as project:
            compacted = self.scan._build(candidate)
        self.assertEqual(compacted.mode, 'compaction')
        self.assertEqual({call.args[1]['logical_document_id'] for call in project.call_args_list},
                         expected_rewrite)
        self.assertEqual(compacted.documents_rewritten, len(expected_rewrite))
        if not expected_rewrite:
            self.assertTrue(reads)
            self.assertTrue(all(r['media_type'] == parquet_scan.PARQUET_MEDIA_TYPE for r in reads))
        oracle = self.scan._build(candidate, _rebuild=True)
        actual_rows, expected_rows = self.rows(compacted), self.rows(oracle)
        self.assertEqual(actual_rows, expected_rows)
        self.assertEqual(hashlib.sha256(json.dumps(actual_rows, sort_keys=True).encode()).hexdigest(),
                         hashlib.sha256(json.dumps(expected_rows, sort_keys=True).encode()).hexdigest())
        self.assertEqual(compacted.row_counts, oracle.row_counts)
        self.assertEqual(set(compacted.removed), set(self.scan._catalog(candidate).shards))
        self.assertLess(len(compacted.references), len(compacted.removed))
        self.assertEqual(
            {m for members in compacted.members.values() for m in members},
            {m for members in oracle.members.values() for m in members})
        return compacted

    def test_unchanged_compaction_never_reopens_raw_and_matches_full_oracle(self):
        candidate = self.fragmented()
        result = self.assert_full_parity(candidate, set())
        self.assertEqual(self.scan._commit(candidate, result), "committed")
        self.assertEqual(self.scan._pending(tenant_id=self.tenant, limit=1), [])

    def test_changed_deleted_and_sibling_rows_match_full_oracle(self):
        self.fragmented()
        self.ingest_parent('alpha', 'alpha/one', 'Synthetic current changed alpha')
        self.ingest_parent('beta', 'beta/one', '', tombstone=True)
        self.project()
        candidate = self.candidate()
        result = self.assert_full_parity(candidate, {self.ids['alpha']})
        self.assertNotIn(self.ids['beta'], {json.loads(r)['logical_document_id']
                                          for r in self.rows(result)['documents']})

    def test_missing_dataset_or_membership_keeps_canonical_full_rewrite(self):
        candidate = self.fragmented()
        catalog = self.scan._catalog(candidate)
        documents = self.scan._documents(candidate)
        one = next(iter(catalog.members))
        missing_dataset = replace(catalog, shards={k: v for k, v in catalog.shards.items()
                                                   if k[0] != 'records'})
        missing_member = replace(catalog, members={k: v for k, v in catalog.members.items() if k != one})
        zero_rows = {**catalog.shards, one: {**catalog.shards[one], 'row_count': 0}}
        zero_without_members = replace(missing_member, shards=zero_rows)
        dangling_member = replace(catalog, members={**catalog.members, ('actors', 9999): ()})
        for broken in (missing_dataset, missing_member, dangling_member):
            with self.subTest(keys=len(broken.shards)):
                mode, _, rewrite, _ = self.scan._plan(candidate, documents, broken)
                self.assertIn(mode, ('compaction', 'full'))
                self.assertEqual(rewrite, {d['logical_document_id'] for d in documents})
        # Native empty parts have no membership rows. Both representations
        # are complete, unlike a positive-row part without membership.
        complete_empty = replace(catalog, shards=zero_rows,
                                 members={**catalog.members, one: ()})
        self.assertEqual(self.scan._plan(candidate, documents, complete_empty)[2], set())
        self.assertEqual(self.scan._plan(candidate, documents, zero_without_members)[2], set())

    def test_explicit_forget_and_later_generation_keep_native_fences(self):
        self.fragmented()
        with self.store.connect() as c:
            receipt = c.execute(
                "SELECT chunk.receipt FROM canonical_chunks chunk JOIN canonical_documents document "
                "USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.native_id=%s",
                (self.tenant, 'beta/one')).fetchone()['receipt']
        forgotten = self.plane.forget(dict(
            contract='recall.forget-request.v1', schema_version=1,
            tenant_id=self.tenant, principal_id=self.principal, source_id=self.source,
            target_receipt=receipt, mode='explicit_forget', reason='owner_requested',
            requested_at='2026-10-15T00:00:00Z', idempotency_key='compaction-forget'))
        self.assertEqual(forgotten['status'], 'deleted')
        self.project()
        candidate = self.candidate()
        result = self.assert_full_parity(candidate, set())
        self.assertNotIn(self.ids['beta'], {json.loads(r)['logical_document_id']
                                          for r in self.rows(result)['documents']})
        self.ingest_parent('alpha', 'alpha/one', 'Synthetic edit after compaction preparation')
        self.project()
        self.assertEqual(self.scan._commit(candidate, result), 'stale')
        self.assertGreater(self.candidate().generation, candidate.generation)

    def test_unavailable_fragment_uses_existing_canonical_recovery(self):
        candidate = self.fragmented()
        original_read = self.archive.read_raw
        failed = []
        def read(ref):
            if ref['media_type'] == parquet_scan.PARQUET_MEDIA_TYPE and not failed:
                failed.append(ref)
                raise ArchiveNotFound('synthetic_derived_missing')
            return original_read(ref)
        with patch.object(self.archive, 'read_raw', side_effect=read), \
             patch.object(self.scan, '_project_document', wraps=self.scan._project_document) as project:
            recovered = self.scan._build(candidate)
        self.assertEqual(len(failed), 1)
        self.assertEqual(recovered.mode, 'full')
        self.assertEqual(project.call_count, 3)
        oracle = self.scan._build(candidate, _rebuild=True)
        self.assertEqual(self.rows(recovered), self.rows(oracle))

    def test_explicit_backfill_still_reads_every_current_document(self):
        self.fragmented()
        # Exercise explicit backfill with no preexisting marker separately from
        # the compaction-marker replacement control below.
        with self.store.connect() as c:
            c.execute("DELETE FROM canonical_parquet_scan_dirty_documents "
                      "WHERE tenant_id=%s AND logical_document_id=%s",
                      (self.tenant, parquet_scan.SCAN_DIRTY_ALL))
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        with patch.object(self.scan, '_project_document', wraps=self.scan._project_document) as project:
            result = self.scan._build(candidate)
        self.assertEqual(result.mode, 'full')
        self.assertEqual(project.call_count, 3)

    def test_explicit_backfill_overrides_existing_compaction_marker(self):
        before = self.fragmented()
        self.assertTrue(self.scan._catalog(before).compaction)
        self.scan.seed_backfill(tenant_id=self.tenant)
        candidate = self.candidate()
        with self.store.connect() as c:
            marker = c.execute("SELECT reason,queued_at FROM canonical_parquet_scan_dirty_documents "
                               "WHERE tenant_id=%s AND logical_document_id=%s",
                               (self.tenant, parquet_scan.SCAN_DIRTY_ALL)).fetchone()
        self.assertEqual(marker['reason'], 'backfill')
        self.assertEqual(marker['queued_at'], candidate.changed_at)
        self.assertGreater(candidate.generation, before.generation)
        self.assertFalse(self.scan._catalog(candidate).compaction)
        with patch.object(self.scan, '_project_document', wraps=self.scan._project_document) as project:
            result = self.scan._build(candidate)
        self.assertEqual(result.mode, 'full')
        self.assertEqual(project.call_count, 3)
        self.assertEqual(self.scan._commit(candidate, result), 'committed')
        self.assertEqual(self.scan._pending(tenant_id=self.tenant, limit=1), [])

    def test_sweep_losing_to_explicit_seed_does_not_replace_its_intent(self):
        self.fragmented()
        with self.store.connect() as c:
            c.execute("DELETE FROM canonical_parquet_scan_queue WHERE tenant_id=%s", (self.tenant,))
            c.execute("DELETE FROM canonical_parquet_scan_dirty_documents WHERE tenant_id=%s", (self.tenant,))
        connect = self.store.connect
        seeded = []
        fixture = self
        class Rows:
            def __init__(self, rows): self.rows = rows
            def fetchall(self): return self.rows
        class Interleave:
            def __init__(self, raw): self.raw = raw
            def __getattr__(self, name): return getattr(self.raw, name)
            def execute(self, statement, *args, **kwargs):
                cursor = self.raw.execute(statement, *args, **kwargs)
                if statement.lstrip().startswith('WITH builds AS'):
                    rows = cursor.fetchall()
                    fixture.assertTrue(rows)
                    fixture.scan.seed_backfill(tenant_id=fixture.tenant)
                    seeded.append(fixture.candidate())
                    return Rows(rows)
                return cursor
        @contextmanager
        def interleave():
            with connect() as raw:
                yield Interleave(raw)
        with patch.object(self.store, 'connect', side_effect=interleave):
            admitted = self.scan._over_fragmented(tenant_id=self.tenant, limit=1)
        self.assertEqual(len(seeded), 1)
        self.assertEqual(admitted, [])
        self.assertEqual(self.candidate(), seeded[0])
        self.assertFalse(self.scan._catalog(seeded[0]).compaction)
        with self.store.connect() as c:
            marker = c.execute("SELECT reason,queued_at FROM canonical_parquet_scan_dirty_documents "
                               "WHERE tenant_id=%s AND logical_document_id=%s",
                               (self.tenant, parquet_scan.SCAN_DIRTY_ALL)).fetchone()
        self.assertEqual(marker, dict(reason='backfill', queued_at=seeded[0].changed_at))


class AnonymousCompaction(CompactionReuse):
    def setUp(self):
        # Leave fixture sources anonymous before the unchanged native writers
        # create their records, passages and initial Parquet fragments.
        with patch.object(BrainStore, 'bind_coding_sources_to_employee', return_value=None):
            super().setUp()

    def test_native_empty_actors_compacts_without_raw_and_matches_full(self):
        candidate = self.fragmented()
        catalog = self.scan._catalog(candidate)
        actors = {key: row for key, row in catalog.shards.items() if key[0] == 'actors'}
        self.assertTrue(actors)
        self.assertTrue(all(row['row_count'] == 0 for row in actors.values()))
        self.assertTrue(all(key not in catalog.members for key in actors))
        compacted = self.assert_full_parity(candidate, set())
        self.assertEqual(self.rows(compacted)['actors'], [])
        self.assertEqual(self.scan._commit(candidate, compacted), 'committed')
        self.assertEqual(self.scan._pending(tenant_id=self.tenant, limit=1), [])


if __name__ == '__main__':
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        CompactionReuse(name) for name in (
            'test_unchanged_compaction_never_reopens_raw_and_matches_full_oracle',
            'test_changed_deleted_and_sibling_rows_match_full_oracle',
            'test_missing_dataset_or_membership_keeps_canonical_full_rewrite',
            'test_explicit_forget_and_later_generation_keep_native_fences',
            'test_unavailable_fragment_uses_existing_canonical_recovery',
            'test_explicit_backfill_still_reads_every_current_document',
            'test_explicit_backfill_overrides_existing_compaction_marker',
            'test_sweep_losing_to_explicit_seed_does_not_replace_its_intent',
        )))
    empty = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite([
        AnonymousCompaction('test_native_empty_actors_compacts_without_raw_and_matches_full')]))
    raise SystemExit(not (result.wasSuccessful() and empty.wasSuccessful()))
