"""Fetched certificates avoid a second lookup without changing native membership."""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import unittest
import copy
import time

from recall_server import parent_membership_read as reader
from recall_server.parent_membership_enrollment import _binding
from recall_server.parquet_scan import CanonicalParquetScanProjector, ScanCandidate
from recall_server.projectors import item_receipt
from test_parquet_receipt_slices import Store


def certified_row():
    row = dict(tenant_id='tenant:test', source_id='source:test', logical_document_id='document',
               passage_id='certified', revision=1, text_sha256='f'*64,
               receipts=[item_receipt('source:test', 'event', 1, 0)],
               manifest_content_sha256='a'*64)
    metadata = dict(source_id=row['source_id'], passage_id=row['passage_id'],
                    policy_fingerprint='b'*64, passage_revision=1, membership_generation=2,
                    membership_binding_sha256=None, native_parent_id='event', manifest_revision=1,
                    projected_revision=1, source_document_sha256='c'*64, generation=2)
    metadata['membership_binding_sha256'] = _binding({**row, **metadata}, {'binding_epoch':'fixture'})
    return {**row, 'membership_metadata':metadata, 'membership_metadata_bytes':500,
            'membership_metadata_complete':True}


class MetadataStore(Store):
    @contextmanager
    def transaction(self):
        yield

    def _execute_bounded(self, connection, statement, parameters, deadline):
        if statement == reader._METADATA_SQL:
            self.queries.append((statement, parameters, deadline))
            self.result = [{**self.saved['membership_metadata'], 'selected_count':1, 'metadata_bytes':500}]
            return self
        return super()._execute_bounded(connection, statement, parameters, deadline)

    def __init__(self, row):
        super().__init__([row])
        self.saved = row


class InlineCertificates(unittest.TestCase):
    def test_certified_stream_needs_no_second_metadata_query_and_keeps_output(self):
        row = certified_row()
        store = MetadataStore(row)
        owner = CanonicalParquetScanProjector(store, SimpleNamespace(archive=None))
        candidate = ScanCandidate('tenant:test', 'source:test', date(2026,1,1), 1,
                                  datetime(2026,1,1,tzinfo=timezone.utc))
        with patch.object(reader.ReadSettings, 'from_env',
                          return_value=reader.ReadSettings(True, 'fixture')):
            result = list(owner._passages(candidate, ['document']))
        self.assertEqual(store.queries, [])
        self.assertEqual(result, [dict(logical_document_id='document', passage_id='certified', revision=1,
                                      receipts=[item_receipt('source:test', 'event', 1, 0)])])
        self.assertEqual(store.fetch_sizes, [2000,2000])
        # Both the data FETCH and the final EOF FETCH own a bounded allowance.
        self.assertEqual(len(store.restored_deadlines), 2)

    def partition(self, rows):
        return reader.partition_fetched(selected=rows, settings=reader.ReadSettings(True, 'fixture'),
                                        deadline_at=time.monotonic()+5)

    def test_missing_join_is_batch_fallback_but_missing_authority_is_per_passage(self):
        good = certified_row()
        other = certified_row()
        other['passage_id'] = other['membership_metadata']['passage_id'] = 'other'
        other['membership_metadata']['membership_binding_sha256'] = _binding(
            {**other, **other['membership_metadata']}, {'binding_epoch':'fixture'})
        for field in ('membership_generation', 'generation'):
            changed = copy.deepcopy(other)
            changed['membership_metadata'][field] = None
            keys, stats = self.partition([good, changed])
            self.assertEqual(keys, {('source:test','certified')})
            self.assertEqual(stats['status'], 'partitioned')
        other['membership_metadata_complete'] = False
        keys, stats = self.partition([good, other])
        self.assertEqual(keys, set())
        self.assertEqual(stats['status'], 'incomplete')

    def test_each_binding_field_and_generation_refuse_changed_certificate(self):
        from recall_server.parent_membership_enrollment import _BINDING_FIELDS
        for field in (*_BINDING_FIELDS, 'generation'):
            with self.subTest(field=field):
                row = certified_row()
                owner = row['membership_metadata'] if field in row['membership_metadata'] else row
                value = owner[field]
                owner[field] = value+1 if isinstance(value,int) else value+['extra'] if isinstance(value,list) else value+'changed'
                keys, stats = self.partition([row])
                self.assertEqual(keys, set())
                self.assertIn(stats['status'], ('partitioned','incomplete'))

    def test_oversize_and_duplicate_batch_fall_back_without_partial_success(self):
        row = certified_row()
        row['membership_metadata_bytes'] = 8388609
        keys, stats = self.partition([row])
        self.assertEqual((keys,stats['status']), (set(),'metadata-budget'))
        row = certified_row()
        keys, stats = self.partition([row, copy.deepcopy(row)])
        self.assertEqual((keys,stats['status']), (set(),'incomplete'))

    def test_separate_reader_still_executes_original_metadata_query(self):
        row = certified_row()
        store = MetadataStore(row)
        keys, stats = reader.partition(store, store, selected=[row], settings=reader.ReadSettings(True,'fixture'),
                                       deadline_at=time.monotonic()+5)
        self.assertEqual(keys, {('source:test','certified')})
        self.assertEqual(stats['status'], 'partitioned')
        self.assertEqual(len(store.queries),1)
        self.assertEqual(store.queries[0][0],reader._METADATA_SQL)



if __name__ == '__main__':
    unittest.main()
