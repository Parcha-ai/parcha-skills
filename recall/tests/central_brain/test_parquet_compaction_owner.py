"""Compaction pressure counts independent owners, not replacement spans."""
import unittest
from unittest import mock

from recall_server.parquet_scan import FragmentMember, ScanCatalog
from tests.central_brain.test_parquet_cross_dataset_delta import Archive, Probe
from tests.central_brain import test_parquet_cross_dataset_delta as delta_tests
from tests.central_brain.test_parquet_scan import _candidate, _live_shard, _month_document


def catalog(groups, *, dataset='records'):
    """groups: (hex generation, list of member-ID lists for its fragments)."""
    shards, members = {}, {}
    for generation, ownership in groups:
        for owners in ownership:
            identity = (dataset, len(shards))
            shards[identity] = {**_live_shard(*identity), 'size_bytes': 100,
                                'generation_sha256': generation * 64}
            members[identity] = tuple(FragmentMember(owner, 1, 'c' * 64) for owner in owners)
    return ScanCatalog(shards, members, frozenset())


def cases():
    large = catalog([('a', [['base']] * 100), ('b', [['large']] * 20)])
    useful = catalog([(f'{i:x}', [[f'doc:{i}']]) for i in range(8)])
    tied = catalog([('a', [[f'small:{i}'] for i in range(4)]), ('b', [['large']] * 4)])
    unknown = catalog([('a', [['base']] * 5), ('b', [[] for _ in range(4)])])
    empty = catalog([('a', [['base']] * 5), ('b', [[''] for _ in range(4)])])
    shared = catalog([('a', [['base']] * 5), ('b', [['same', 'superset']] * 4)])
    collision = catalog([('a', [['base']] * 3), ('b', [['fragment:records:4'], []])])
    exact_owner = catalog([('a', [['base']] * 3), ('b', [['Owner'], ['owner']])])
    actors = catalog([('a', [['base']] * 8)], dataset='actors')
    dominant_tie = ScanCatalog({**actors.shards, **tied.shards}, {**actors.members, **tied.members}, frozenset())
    return [('large_replacement', large, 16, False), ('useful_small', useful, 4, True),
            ('generation_tie', tied, 4, True), ('unknown', unknown, 4, True),
            ('empty_owner', empty, 4, True), ('shared_superset', shared, 4, True),
            ('identity_domains', collision, 2, True), ('exact_owner', exact_owner, 2, True), ('dataset_tie', dominant_tie, 4, True),
            ('empty_catalog', ScanCatalog({}, {}, frozenset()), 4, False)]


class CompactionOwnerTests(unittest.TestCase):
    def test_owner_pressure_cases_and_input_order(self):
        for name, value, cap, expected in cases():
            with self.subTest(case=name):
                self.assertEqual(value.fragmented(cap), expected)
                reversed_value = ScanCatalog(dict(reversed(list(value.shards.items()))), value.members, value.dirty)
                self.assertEqual(reversed_value.fragmented(cap), expected)

    def test_same_size_large_replacement_reuses_without_full_rebuild(self):
        check = delta_tests.CrossDatasetDeltaTests()
        _, changed, before, archive = check.fixture()
        with mock.patch('recall_server.parquet_scan.PARQUET_RAW_SLICE_BYTES', 8192), mock.patch(
            'recall_server.parquet_scan.FRAGMENT_TARGET_BYTES', 8192
        ):
            delta = Probe(changed, before, archive)._build(_candidate())
            after = check.after(before, delta)
            self.assertEqual(len(before.shards), len(after.shards))
            self.assertGreater(sum(1 for dataset, _ in delta.references if dataset == 'records'), 16)
            archive.reads.clear()
            reused = Probe(changed, after, archive)._build(_candidate())
        self.assertEqual(reused.mode, 'reuse')
        self.assertFalse(reused.created)
        self.assertEqual(archive.reads, [])
        self.assertEqual(check.rows(after.shards, archive), check.rows(reused.references, archive))

    def test_seventeen_small_additions_still_consolidate_with_exact_output(self):
        check = delta_tests.CrossDatasetDeltaTests()
        documents = [_month_document(f'control:{i}') for i in range(17)]
        archive = Archive({doc['logical_document_id']: 1 for doc in documents})
        value = ScanCatalog({}, {}, frozenset())
        with mock.patch('recall_server.parquet_scan.PARQUET_RAW_SLICE_BYTES', 8192), mock.patch(
            'recall_server.parquet_scan.FRAGMENT_TARGET_BYTES', 8192
        ):
            for count in range(1, 18):
                upload = Probe(documents[:count], value, archive)._build(_candidate())
                value = check.after(value, upload)
            before_rows = check.rows(value.shards, archive)
            compacted = Probe(documents, value, archive)._build(_candidate())
            after = check.after(value, compacted)
        self.assertEqual(compacted.mode, 'compaction')
        self.assertEqual(len(value.shards), 68)
        self.assertEqual(len(after.shards), 16)
        self.assertEqual(before_rows, check.rows(after.shards, archive))


if __name__ == '__main__':
    unittest.main()
