"""Actual Arrow output parity for independently packed dataset fragments."""
import hashlib
import json
import unittest
from dataclasses import replace
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

from recall_server.archive import ArchiveCorruption, ArchiveError, ArchiveNotFound
from recall_server.parquet_scan import (
    FragmentMember, ParquetScanError, ScanCatalog, SCAN_DATASETS, _schemas,
)
from tests.central_brain.test_parquet_scan import (
    _candidate, _DocumentArchive, _FragmentProbe, _live_shard, _month_document,
)


def decode(payload):
    return pq.ParquetFile(pa.BufferReader(payload)).read(use_threads=False).to_pylist()


def encode(rows, schema):
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink)
    return sink.getvalue().to_pybytes()


class Archive(_DocumentArchive):
    def __init__(self, records):
        super().__init__(records)
        self.fragment_reads = []

    def put_raw(self, **values):
        reference = super().put_raw(**values)
        reference['content_sha256'] = hashlib.sha256(values['payload']).hexdigest()
        return reference

    def read_raw(self, reference):
        if reference['artifact_id'].startswith('new:'):
            self.fragment_reads.append(reference['artifact_id'])
            return self.uploads[int(reference['artifact_id'].split(':')[1])-1]['payload']
        raw = super().read_raw(reference)
        # Real record attribution and full nested JSON must survive copying.
        records = [json.loads(line) for line in raw.splitlines()]
        for record in records:
            record['actor_links'] = [{'actor_id': 'actor:test', 'relation': 'speaker'}]
            record['extra'] = {'nested': ['exact', 12, None, True]}
        return b''.join(json.dumps(record).encode() + b'\n' for record in records)


class Probe(_FragmentProbe):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = []

    def _schedule_cleanup(self, references):
        self.cleanup.extend(references)

    def _passages(self, candidate, document_ids):
        for document in self.month_documents:
            doc_id = document['logical_document_id']
            if doc_id in document_ids and self.archive.records[doc_id]:
                yield {
                    'logical_document_id': doc_id, 'revision': document['revision'],
                    'passage_id': 'passage:' + doc_id, 'ordinal': 0,
                    'first_occurred_at': '2026-08-05T12:00:00Z',
                    'last_occurred_at': '2026-08-05T12:00:00Z',
                    'token_count': 2, 'roles': ['user'],
                    'receipts': ['recall://source:test/' + doc_id],
                    'actor_ids': ['actor:test'], 'actor_names': ['A'],
                    'actor_relations': ['speaker'], 'text_redacted': 'text ' + doc_id,
                }


def catalog_from_upload(upload, dirty=()):
    shards = {
        identity: {**_live_shard(*identity), **reference,
                   'row_count': upload.row_counts[identity],
                   'generation_sha256': upload.generation_sha256}
        for identity, reference in upload.references.items()
    }
    return ScanCatalog(shards, upload.members, frozenset(dirty))


class CrossDatasetDeltaTests(unittest.TestCase):
    def fixture(self, *, records=100, empty=False):
        documents = [_month_document(f'document:{i}') for i in range(6)]
        for document in documents:
            document['actor_links'] = [{'actor_id': 'actor:test', 'display_name': 'A',
                                        'relation': 'speaker'}]
        archive = Archive({d['logical_document_id']: records for d in documents})
        if empty:
            archive.records['document:5'] = 0
        with mock.patch('recall_server.parquet_scan.PARQUET_RAW_SLICE_BYTES', 8192), mock.patch(
            'recall_server.parquet_scan.FRAGMENT_TARGET_BYTES', 8192
        ):
            initial = Probe(documents, ScanCatalog({}, {}, frozenset()), archive)._build(_candidate())
        catalog = catalog_from_upload(initial, {'document:0'})
        changed = [{**documents[0], 'document_content_sha256': 'e'*64}, *documents[1:]]
        archive.reads.clear()
        return documents, changed, catalog, archive

    def rows(self, references, archive):
        result = {dataset: [] for dataset in SCAN_DATASETS}
        for (dataset, _), reference in references.items():
            result[dataset].extend(decode(archive.read_raw(reference)))
        return {dataset: sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)
                for dataset, rows in result.items()}

    def after(self, catalog, result):
        shards = {identity: value for identity, value in catalog.shards.items()
                  if identity not in result.removed}
        shards.update(catalog_from_upload(result).shards)
        members = {identity: value for identity, value in catalog.members.items()
                   if identity not in result.removed}
        members.update(result.members)
        return ScanCatalog(shards, members, frozenset())

    def assert_full_parity(self, documents, catalog, archive, result):
        after = self.after(catalog, result)
        full_archive = Archive(archive.records)
        full = Probe(documents, ScanCatalog({}, {}, frozenset()), full_archive)._build(_candidate())
        self.assertEqual(self.rows(after.shards, archive), self.rows(full.references, full_archive))
        self.assertEqual(Probe(documents, after, archive)._plan(_candidate(), documents, after)[0], 'reuse')
        return after

    def replace_payload(self, catalog, archive, identity, payload):
        shard = catalog.shards[identity]
        archive.uploads[int(shard['artifact_id'].split(':')[1])-1]['payload'] = payload
        shard['size_bytes'] = len(payload)
        shard['content_sha256'] = hashlib.sha256(payload).hexdigest()

    def test_current_writer_packs_shared_metadata_but_isolated_record_parts(self):
        _, _, catalog, _ = self.fixture()
        metadata = [members for (dataset, _), members in catalog.members.items() if dataset == 'documents']
        self.assertEqual(len(metadata), 1)
        self.assertEqual(len(metadata[0]), 6)
        records = [members for (dataset, _), members in catalog.members.items() if dataset == 'records']
        self.assertGreater(len(records), 6)
        self.assertTrue(all(len(members) == 1 for members in records))
        self.assertFalse(catalog.fragmented(16))

    def test_one_dirty_document_does_not_reread_unrelated_record_bodies(self):
        _, changed, catalog, archive = self.fixture()
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, ['document:0'])
        self.assertEqual(result.documents_rewritten, 1)
        self.assertLess(len(result.references), len(catalog.shards))
        self.assertEqual(result.first_occurred_at, result.last_occurred_at)
        self.assert_full_parity(changed, catalog, archive, result)

    def test_deleted_document_does_not_regenerate_siblings(self):
        documents, _, catalog, archive = self.fixture()
        result = Probe(documents[1:], catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, [])
        after = self.assert_full_parity(documents[1:], catalog, archive, result)
        for rows in self.rows(after.shards, archive).values():
            self.assertTrue(all(json.loads(row)['logical_document_id'] != 'document:0' for row in rows))

    def test_revision_change_and_multiple_dirty_documents_have_exact_parity(self):
        documents, changed, catalog, archive = self.fixture(records=3)
        changed[0] = {**changed[0], 'revision': 2}
        changed[1] = {**documents[1], 'document_content_sha256': 'f'*64, 'actor_links': []}
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, ['document:0', 'document:1'])
        self.assert_full_parity(changed, catalog, archive, result)

    def test_provider_or_unclassified_failures_do_not_rebuild(self):
        for error in (RuntimeError('unavailable'), ArchiveError('provider request failed'), TimeoutError()):
            with self.subTest(error=type(error).__name__):
                _, changed, catalog, archive = self.fixture()
                original = archive.read_raw
                def bad_read(reference):
                    if reference['artifact_id'].startswith('new:'):
                        raise error
                    return original(reference)
                archive.read_raw = bad_read
                with self.assertRaises(type(error)):
                    Probe(changed, catalog, archive)._build(_candidate())
                self.assertEqual(archive.reads, [])

    def test_catalog_scope_identity_size_and_count_refuse(self):
        changes = {'tenant_id': 'other', 'source_id': 'other', 'bucket_start': None,
                   'dataset': 'other', 'shard_index': 99, 'size_bytes': 0,
                   'media_type': 'text/plain'}
        for key, value in changes.items():
            with self.subTest(key=key):
                _, changed, catalog, archive = self.fixture()
                catalog.shards[('documents', 0)][key] = value
                with self.assertRaises(ParquetScanError):
                    Probe(changed, catalog, archive)._build(_candidate())

    def test_exact_schema_row_scope_membership_revision_and_version_refuse(self):
        for key, value in [('tenant_id', 'other'), ('source_id', 'other'),
                           ('logical_document_id', 'other'), ('revision', 9), ('schema_version', 9)]:
            with self.subTest(key=key):
                _, changed, catalog, archive = self.fixture()
                identity = ('documents', 0)
                rows = decode(archive.read_raw(catalog.shards[identity]))
                rows[1][key] = value
                self.replace_payload(catalog, archive, identity, encode(rows, _schemas()['documents']))
                with self.assertRaises(ParquetScanError):
                    Probe(changed, catalog, archive)._build(_candidate())
    def test_stale_member_fingerprint_regenerates_sibling_not_copies_it(self):
        _, changed, catalog, archive = self.fixture(records=3)
        identity = ('documents', 0)
        catalog.members[identity] = tuple(
            replace(member, generation_sha256='0'*64) if member.logical_document_id == 'document:1'
            else member for member in catalog.members[identity])
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, ['document:0', 'document:1'])
        self.assert_full_parity(changed, catalog, archive, result)

    def test_partial_preservation_uploads_are_cleaned_if_next_part_missing(self):
        _, changed, catalog, archive = self.fixture(records=3)
        original = archive.read_raw
        documents_artifact = catalog.shards[('documents', 0)]['artifact_id']
        def bad_read(reference):
            if reference['artifact_id'] == documents_artifact:
                raise RuntimeError('unavailable')
            return original(reference)
        archive.read_raw = bad_read
        probe = Probe(changed, catalog, archive)
        initial_count = len(archive.uploads)
        # Actor copies precede documents and this boundary forces real uploads.
        with mock.patch('recall_server.parquet_scan.FRAGMENT_TARGET_BYTES', 1):
            with self.assertRaises(RuntimeError):
                probe._build(_candidate())
        self.assertGreater(len(archive.uploads), initial_count)
        self.assertEqual(len(probe.cleanup), len(archive.uploads) - initial_count)
        self.assertTrue(all(ref['artifact_id'].startswith('new:') for ref in probe.cleanup))

    def test_zero_row_document_claim_survives_and_next_plan_reuses(self):
        _, changed, catalog, archive = self.fixture(records=3, empty=True)
        self.assertTrue(any(member.logical_document_id == 'document:5'
                            for member in catalog.members[('documents', 0)]))
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, ['document:0'])
        after = self.assert_full_parity(changed, catalog, archive, result)
        self.assertTrue(any(member.logical_document_id == 'document:5'
                            for members in after.members.values() for member in members))

    def test_shared_tail_and_safe_superset_copy_only_actual_rows_once(self):
        documents, changed, catalog, archive = self.fixture(records=3)
        all_records = []
        for identity in list(catalog.shards):
            if identity[0] == 'records':
                all_records.extend(decode(archive.read_raw(catalog.shards.pop(identity))))
                catalog.members.pop(identity)
        layouts = [
            [row for row in all_records if row['logical_document_id'] == 'document:1' and row['ordinal'] == 0],
            [row for row in all_records if row['logical_document_id'] == 'document:0' or
             (row['logical_document_id'] == 'document:1' and row['ordinal'] > 0)],
            [row for row in all_records if row['logical_document_id'] not in {'document:0', 'document:1'}],
        ]
        for index, rows in enumerate(layouts):
            identity = ('records', index)
            reference = archive.put_raw(
                tenant_id='tenant:test', source_id='source:test', native_id='fixture',
                payload=encode(rows, _schemas()['records']), media_type='application/vnd.apache.parquet',
                created_at='2026-08-01T00:00:00Z')
            catalog.shards[identity] = {**_live_shard(*identity), **reference, 'row_count': len(rows)}
            actual = {row['logical_document_id'] for row in rows}
            if index == 1:
                actual.add('document:2')  # Valid split-membership superset, no row here.
            catalog.members[identity] = tuple(FragmentMember(
                document['logical_document_id'], document['revision'], Probe._fingerprint(document))
                for document in documents if document['logical_document_id'] in actual)
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(archive.reads, ['document:0'])
        self.assertNotIn(('records', 0), result.removed)
        self.assertNotIn(('records', 2), result.removed)
        self.assertIn(('records', 1), result.removed)
        self.assert_full_parity(changed, catalog, archive, result)

    def test_upload_generation_includes_preserved_provenance(self):
        _, changed, catalog, archive = self.fixture(records=3)
        result = Probe(changed, catalog, archive)._build(_candidate())
        self.assertEqual(result.generation_sha256, Probe._generation(_candidate(), changed))
        self.assertNotEqual(result.generation_sha256, Probe._generation(_candidate(), changed[:1]))

    def test_copied_records_consume_existing_record_budget(self):
        _, changed, catalog, archive = self.fixture(records=3)
        probe = Probe(changed, catalog, archive)
        with mock.patch('recall_server.parquet_scan.MAX_SCAN_RECORDS', 2):
            with self.assertRaises(ParquetScanError):
                probe._build(_candidate())

    def test_commit_unknown_keeps_uploaded_objects_for_existing_resolution(self):
        from contextlib import nullcontext
        _, changed, catalog, archive = self.fixture(records=3)
        probe = Probe(changed, catalog, archive)
        with mock.patch.object(probe, '_candidate_lease', return_value=nullcontext(True)), mock.patch.object(
            probe, '_commit', side_effect=ConnectionError('commit acknowledgment lost')
        ):
            with self.assertRaises(ConnectionError):
                probe._process(_candidate(), {})
        self.assertEqual(probe.cleanup, [])

    def test_known_stale_publication_cleans_only_new_objects(self):
        from contextlib import nullcontext
        _, changed, catalog, archive = self.fixture(records=3)
        probe = Probe(changed, catalog, archive)
        initial_count = len(archive.uploads)
        with mock.patch.object(probe, '_candidate_lease', return_value=nullcontext(True)), mock.patch.object(
            probe, '_commit', return_value='stale'
        ):
            totals = {'stale': 0}
            probe._process(_candidate(), totals)
        self.assertEqual(totals['stale'], 1)
        self.assertEqual(len(probe.cleanup), len(archive.uploads) - initial_count)

    def test_closed_fragment_corruption_rebuilds_once_with_exact_full_output(self):
        for failure in ('missing', 'archive_digest', 'wrong_bytes', 'schema', 'count', 'arrow'):
            with self.subTest(failure=failure):
                _, changed, catalog, archive = self.fixture(records=3)
                identity = ('documents', 0)
                original = archive.read_raw
                bad_id = catalog.shards[identity]['artifact_id']
                if failure == 'schema':
                    self.replace_payload(catalog, archive, identity, encode([], pa.schema([('extra', pa.string())])))
                elif failure == 'count':
                    catalog.shards[identity]['row_count'] += 1
                elif failure == 'arrow':
                    self.replace_payload(catalog, archive, identity, b'not parquet')
                def bad_read(reference):
                    if reference['artifact_id'] == bad_id:
                        if failure == 'missing':
                            raise ArchiveNotFound('missing')
                        if failure == 'archive_digest':
                            raise ArchiveCorruption('invalid')
                        if failure == 'wrong_bytes':
                            return b'changed bytes'
                    return original(reference)
                archive.read_raw = bad_read
                probe = Probe(changed, catalog, archive)
                with mock.patch.object(probe, '_documents', wraps=probe._documents) as metadata:
                    result = probe._build(_candidate())
                self.assertEqual(metadata.call_count, 2)
                self.assertEqual(result.mode, 'full')
                self.assertEqual(archive.reads, [d['logical_document_id'] for d in changed])
                self.assertNotEqual(result.generation_sha256, Probe._generation(_candidate(), changed))
                self.assert_full_parity(changed, catalog, archive, result)

    def test_rebuild_aborts_partial_output_before_raw_read_and_separates_identity(self):
        _, changed, catalog, archive = self.fixture(records=3)
        probe = Probe(changed, catalog, archive)
        original = archive.read_raw
        bad_id = catalog.shards[('documents', 0)]['artifact_id']
        def bad_read(reference):
            if reference['artifact_id'] == bad_id:
                raise ArchiveCorruption('invalid')
            if reference['artifact_id'].startswith('raw:'):
                self.assertGreater(len(probe.cleanup), 0)
            return original(reference)
        archive.read_raw = bad_read
        with mock.patch('recall_server.parquet_scan.FRAGMENT_TARGET_BYTES', 1):
            result = probe._build(_candidate())
        queued_native = {archive.uploads[int(ref['artifact_id'].split(':')[1])-1]['native_id']
                         for ref in probe.cleanup}
        committed_native = {archive.uploads[int(ref['artifact_id'].split(':')[1])-1]['native_id']
                            for ref in result.references.values()}
        self.assertTrue(queued_native.isdisjoint(committed_native))
        self.assert_full_parity(changed, catalog, archive, result)

    def test_cleanup_failure_blocks_rebuild_and_raw_corruption_never_retries(self):
        for cleanup_fails in (True, False):
            with self.subTest(cleanup_fails=cleanup_fails):
                _, changed, catalog, archive = self.fixture(records=3)
                probe = Probe(changed, catalog, archive)
                def bad_read(reference):
                    if reference['artifact_id'].startswith('new:'):
                        raise ArchiveCorruption('derived invalid')
                    archive.reads.append(reference['artifact_id'])
                    raise ArchiveCorruption('canonical invalid')
                archive.read_raw = bad_read
                if cleanup_fails:
                    probe._schedule_cleanup = mock.Mock(side_effect=RuntimeError('cleanup unavailable'))
                with mock.patch.object(probe, '_documents', wraps=probe._documents) as metadata:
                    with self.assertRaises((ArchiveCorruption, ParquetScanError)):
                        probe._build(_candidate())
                self.assertEqual(metadata.call_count, 1 if cleanup_fails else 2)
                self.assertEqual(len(archive.reads), 0 if cleanup_fails else 1)

    def test_cross_month_passage_does_not_widen_document_bounds(self):
        passages = Probe._passages
        def crossing(probe, candidate, document_ids):
            for passage in passages(probe, candidate, document_ids):
                yield {**passage, 'first_occurred_at': '2026-07-31T23:00:00Z',
                       'last_occurred_at': '2026-09-01T01:00:00Z'}
        with mock.patch.object(Probe, '_passages', crossing):
            _, changed, catalog, archive = self.fixture(records=3)
            result = Probe(changed, catalog, archive)._build(_candidate())
            full = Probe(changed, ScanCatalog({}, {}, frozenset()), Archive(archive.records))._build(_candidate())
            self.assertEqual((result.first_occurred_at, result.last_occurred_at),
                             (full.first_occurred_at, full.last_occurred_at))
            self.assert_full_parity(changed, catalog, archive, result)


if __name__ == '__main__':
    unittest.main()
