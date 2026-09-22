"""Busy queued builds preserve exact output without optional consolidation."""
import contextlib
from dataclasses import replace
import unittest

from recall_server.parquet_scan import ScanCatalog, SCAN_DIRTY_ALL, MAX_SHARD_INDEX, ParquetScanError
from tests.central_brain.test_parquet_scan import _candidate
from tests.central_brain.test_parquet_cross_dataset_delta import Archive, Probe
from tests.central_brain import test_parquet_cross_dataset_delta as delta


class QueuedProbe(Probe):
    def _pending(self, **kwargs):
        return [_candidate()]

    def _candidate_lease(self, candidate):
        return contextlib.nullcontext(True)

    def _commit(self, candidate, upload):
        self.committed = upload
        return 'committed'

    def _over_fragmented(self, **kwargs):
        self.swept = True
        return []

    def _fragment_total(self, **kwargs):
        return len(self.catalog.shards)


class QueuedCompactionTests(unittest.TestCase):
    def fixture(self):
        helper = delta.CrossDatasetDeltaTests()
        docs, changed, catalog, archive = helper.fixture(records=10)
        # Distinct build generations cross the unchanged cap. Immutable object
        # bytes and each document's recorded fingerprint remain exact.
        shards = {key: {**row, 'generation_sha256': str(index % 2) * 64}
                  for index, (key, row) in enumerate(catalog.shards.items())}
        catalog = replace(catalog, shards=shards, compaction=True,
                          dirty=catalog.dirty | {SCAN_DIRTY_ALL})
        self.assertTrue(catalog.fragmented(1))
        return helper, docs, changed, catalog, archive

    def run_queue(self, documents, catalog, archive, budget):
        probe = QueuedProbe(documents, catalog, archive, compaction_fragments=1)
        result = probe.project_pending(batch_size=1, max_batches=1, compaction_budget=budget)
        self.assertEqual(result['shards'], 1)
        self.assertEqual(bool(getattr(probe, 'swept', False)), bool(budget))
        return probe.committed

    def test_busy_actual_queued_path_delta_has_full_output_parity(self):
        helper, _, changed, catalog, archive = self.fixture()
        result = self.run_queue(changed, catalog, archive, 0)
        self.assertEqual(result.mode, 'delta')
        self.assertEqual(result.documents_rewritten, 1)
        self.assertEqual(archive.reads, ['document:0'])
        after = helper.after(catalog, result)
        full_archive = Archive(archive.records)
        full = Probe(changed, ScanCatalog({}, {}, frozenset()), full_archive)._build(_candidate())
        self.assertEqual(helper.rows(after.shards, archive), helper.rows(full.references, full_archive))

    def test_idle_actual_queued_path_retains_compaction(self):
        _, _, changed, catalog, archive = self.fixture()
        result = self.run_queue(changed, catalog, archive, 1)
        self.assertEqual(result.mode, 'compaction')
        self.assertEqual(result.documents_rewritten, len(changed))

    def test_busy_compaction_only_hint_reuses_identical_base(self):
        _, docs, _, catalog, archive = self.fixture()
        result = self.run_queue(docs, catalog, archive, 0)
        self.assertEqual(result.mode, 'reuse')
        self.assertEqual(archive.reads, [])

    def test_busy_dead_majority_excludes_deleted_members(self):
        helper, docs, _, catalog, archive = self.fixture()
        remaining = docs[-2:]
        catalog = replace(catalog, compaction=False, dirty=frozenset(), shards={
            key: {**row, 'generation_sha256': 'a' * 64} for key, row in catalog.shards.items()
        })
        self.assertFalse(catalog.fragmented(1))
        self.assertEqual(Probe(remaining, catalog, archive)._plan(_candidate(), remaining, catalog)[0], 'compaction')
        result = self.run_queue(remaining, catalog, archive, 0)
        self.assertEqual(result.mode, 'delta')
        self.assertEqual(result.documents_rewritten, 0)
        after = helper.after(catalog, result)
        full_archive = Archive(archive.records)
        full = Probe(remaining, ScanCatalog({}, {}, frozenset()), full_archive)._build(_candidate())
        self.assertEqual(helper.rows(after.shards, archive), helper.rows(full.references, full_archive))

    def test_busy_mandatory_backfill_and_missing_dataset_remain_full(self):
        for missing in (False, True):
            with self.subTest(missing=missing):
                _, _, changed, catalog, archive = self.fixture()
                if missing:
                    catalog = replace(catalog, shards={k:v for k,v in catalog.shards.items() if k[0]!='actors'})
                else:
                    catalog = replace(catalog, compaction=False)
                result = self.run_queue(changed, catalog, archive, 0)
                self.assertEqual(result.mode, 'full')
                self.assertEqual(result.documents_rewritten, len(changed))

    def test_busy_corrupt_derived_part_still_recovers_once(self):
        helper, _, changed, catalog, archive = self.fixture()
        identity = next(k for k,v in catalog.members.items() if k[0]=='documents' and len(v)>1)
        helper.replace_payload(catalog, archive, identity, b'not parquet')
        result = self.run_queue(changed, catalog, archive, 0)
        self.assertEqual(result.mode, 'full')
        self.assertEqual(result.documents_rewritten, len(changed))
        helper.assert_full_parity(changed, catalog, archive, result)

    def test_busy_does_not_raise_shard_index_limit(self):
        _, _, changed, catalog, archive = self.fixture()
        identity = next(key for key, members in catalog.members.items()
                        if key[0] == 'records' and all(m.logical_document_id != 'document:0' for m in members))
        target = (identity[0], MAX_SHARD_INDEX)
        shards, members = dict(catalog.shards), dict(catalog.members)
        shards[target] = {**shards.pop(identity), 'shard_index': MAX_SHARD_INDEX}
        members[target] = members.pop(identity)
        catalog = replace(catalog, shards=shards, members=members)
        probe = QueuedProbe(changed, catalog, archive, compaction_fragments=1)
        with self.assertRaisesRegex(ParquetScanError, 'parquet_scan_shard_index_exhausted'):
            probe.project_pending(batch_size=1, max_batches=1, compaction_budget=0)
        self.assertFalse(hasattr(probe, 'committed'))
