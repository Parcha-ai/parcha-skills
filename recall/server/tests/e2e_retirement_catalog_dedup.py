#!/usr/bin/env python3
"""Native catalog rows retain every fence without repeated parent wire payload."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from e2e_singleton_metadata_admission import SingletonMetadataAdmission
from recall_server import chunk_bodies


LEGACY_SQL = Path(__file__).with_name('catalog_before_dedup.sql').read_text()


class CustomCatalogObject(dict):
    pass


def custom_catalog_loads(value):
    return json.loads(value, object_hook=CustomCatalogObject)


class CatalogDedup(SingletonMetadataAdmission):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Install future-field fixtures before the pool prepares any part reads.
        with cls.store.connect() as c:
            c.execute("""ALTER TABLE canonical_evidence_document_parts
                ADD COLUMN copy_number numeric, ADD COLUMN copy_flag boolean,
                ADD COLUMN copy_null text, ADD COLUMN copy_nested jsonb""")

    @classmethod
    def tearDownClass(cls):
        try:
            with cls.store.connect() as c:
                c.execute("""ALTER TABLE canonical_evidence_document_parts
                    DROP COLUMN copy_number, DROP COLUMN copy_flag,
                    DROP COLUMN copy_null, DROP COLUMN copy_nested""")
        finally:
            super().tearDownClass()

    def populate(self):
        ingest = self.plane.ingest_document
        put = self.projector.projection.put_records
        for parent in ('parent/a', 'parent/b'):
            def write(**kwargs):
                return ingest(**dict(kwargs, envelope=dict(kwargs['envelope'], native_parent_id=parent)))
            with patch.object(self.plane, 'ingest_document', side_effect=write):
                for number in range(4):
                    self.ingest(f'{parent}/{number}', f'Body α 🧠 {parent} {number} ' * 60)
        with patch.object(self.projector.projection, 'put_records',
                          side_effect=lambda **kw: put(**dict(kw, part_bytes=1024))):
            report = self.projector.project_pending(tenant_id=self.tenant, batch_size=10,
                max_batches=1, upload_concurrency=1)
        self.assertEqual(report['failed'], 0, report)
        with self.store.connect() as c:
            self.ids = tuple(r['document_id'] for r in c.execute(
                'SELECT document_id FROM canonical_documents WHERE tenant_id=%s ORDER BY document_id',
                (self.tenant,)))

    def equivalent(self, *, located_only=False, sources=None):
        sources = sources or (self.source,)
        with self.store.connect() as c, c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            rows = c.execute(LEGACY_SQL, (self.tenant, list(sources), list(self.ids), located_only)).fetchall()
            expected = {(r['source_id'], r['document_id']): r for r in rows}
            actual = chunk_bodies.chunk_catalog_snapshot(self.store, tenant_id=self.tenant,
                source_ids=sources, document_ids=self.ids, located_only=located_only,
                connection=c, deadline_at=time.monotonic()+10)
        self.assertEqual(actual, expected)
        self.assertEqual(list(actual), list(expected))
        for key in actual:
            self.assertEqual(list(actual[key]), list(expected[key]))
            self.assertEqual({k: type(v) for k, v in actual[key].items()},
                             {k: type(v) for k, v in expected[key].items()})
        def encode(value):
            return json.dumps(list(value.values()), sort_keys=True, default=str).encode()
        self.assertEqual(hashlib.sha256(encode(actual)).digest(), hashlib.sha256(encode(expected)).digest())
        return actual

    def test_multiple_parents_rows_and_wire_inventory(self):
        self.populate()
        self.equivalent()
        execute = self.store._execute_bounded
        wire = []
        class Rows:
            def __init__(self, cursor): self.cursor = cursor
            def fetchall(self):
                rows = self.cursor.fetchall()
                wire.extend(deepcopy(rows))
                return rows
        def measured(c, sql, params, deadline):
            cursor = execute(c, sql, params, deadline)
            return Rows(cursor) if 'AS structural_types' in sql else cursor
        with patch.object(self.store, '_execute_bounded', side_effect=measured):
            actual = chunk_bodies.chunk_catalog_snapshot(self.store, tenant_id=self.tenant,
                source_ids=(self.source,), document_ids=self.ids)
        parents = {r['native_parent_id']: r for r in actual.values()}
        expected_entries = sum(len(r['parts']) for r in parents.values())
        self.assertGreater(expected_entries, 0)
        self.assertEqual(sum(len(r.get('parts') or []) for r in wire), expected_entries)
        self.assertEqual(sum(r.get('manifest') is not None for r in wire), len(parents))
        same_parent = [r for r in actual.values() if r['native_parent_id']=='parent/a']
        same_parent[0]['parts'][0]['size_bytes'] = -1
        self.assertNotEqual(same_parent[1]['parts'][0]['size_bytes'], -1)
        same_parent[0]['manifest']['revision'] = -1
        self.assertNotEqual(same_parent[1]['manifest']['revision'], -1)

    def test_same_parent_names_in_different_sources_remain_separate(self):
        self.populate()
        first_ids = self.ids
        second_source = self.source + ':second'
        with patch.object(self, 'source', second_source):
            self.populate()
        self.ids += first_ids
        rows = self.equivalent(sources=(self.source, second_source))
        self.assertEqual(len(rows), 16)
        self.assertEqual({row['source_id'] for row in rows.values()}, {self.source, second_source})
        for row in rows.values():
            self.assertEqual(row['manifest']['source_id'], row['source_id'])
            self.assertTrue(all(part['source_id']==row['source_id'] for part in row['parts']))

    def test_pending_missing_manifest_and_deleted_documents_match(self):
        self.populate()
        self.ingest('unpublished', 'An unlocated document has no published manifest')
        with self.store.connect() as c:
            self.ids += (c.execute('SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND native_id=%s',
                (self.tenant, 'unpublished')).fetchone()['document_id'],)
        actual = self.equivalent()
        missing = [r for r in actual.values() if r['native_id']=='unpublished'][0]
        self.assertIsNone(missing['manifest'])
        self.assertEqual(missing['parts'], [])
        self.assertTrue(missing['pending'])
        located = self.equivalent(located_only=True)
        self.assertNotIn((self.source, missing['document_id']), located)
        self.ingest('parent/a/0', '', tombstone=True)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND native_id=%s',
                (self.tenant, 'parent/b/0'))
        actual = self.equivalent()
        self.assertFalse(any(r['native_id'] in ('parent/a/0', 'parent/b/0') for r in actual.values()))
        self.assertEqual(chunk_bodies.chunk_catalog_snapshot(self.store, tenant_id=self.tenant,
            source_ids=(self.source,), document_ids=()), {})

    def test_singleton_admission_preserves_exact_catalog_and_native_receipt(self):
        original = self.prepare()
        self.ids = (original['document_id'],)
        before = self.equivalent()
        preview = self.admit(original)
        self.assertEqual(self.admit(original, apply=True, reviewed_plan=preview['plan'])['retired_chunks'], 1)
        with patch.object(self.store, 'verified_singleton_receipts', True):
            actual = chunk_bodies.chunk_catalog_snapshot(self.store, tenant_id=self.tenant,
                source_ids=(self.source,), document_ids=self.ids)
            for row in before.values():
                row['chunk_receipt_profile'] = 1
            self.assertEqual(actual, before)
            self.assertEqual(self.store.resolve(original['receipt'], tenant_id=self.tenant,
                authorized_sources=(self.source,), chunk_body_archive=self.archive), self.public_response)

    def test_flat_parts_preserve_large_scalars_and_independent_rows(self):
        self.populate()
        large = 10**80 + 17
        with self.store.connect() as c:
            c.execute("""UPDATE canonical_evidence_document_parts SET copy_number=%s,copy_flag=true
                          WHERE tenant_id=%s""", (large, self.tenant))
        actual = self.equivalent()
        siblings = [r for r in actual.values() if r['native_parent_id'] == 'parent/a']
        for row in siblings:
            self.assertEqual(row['parts'][0]['copy_number'], large)
            self.assertIs(type(row['parts'][0]['copy_number']), int)
            self.assertIs(row['parts'][0]['copy_flag'], True)
            self.assertIsNone(row['parts'][0]['copy_null'])
        siblings[0]['parts'][0]['copy_number'] = -1
        self.assertEqual(siblings[1]['parts'][0]['copy_number'], large)

    def test_future_nested_part_values_remain_independent(self):
        self.populate()
        with self.store.connect() as c:
            c.execute("""UPDATE canonical_evidence_document_parts
                            SET copy_nested='{"items":[{"value":"original"}]}'::jsonb
                          WHERE tenant_id=%s""", (self.tenant,))
        actual = self.equivalent()
        siblings = [r for r in actual.values() if r['native_parent_id'] == 'parent/a']
        siblings[0]['parts'][0]['copy_nested']['items'][0]['value'] = 'changed'
        self.assertEqual(siblings[1]['parts'][0]['copy_nested']['items'][0]['value'], 'original')

    def test_custom_json_objects_retain_type_and_independence(self):
        from psycopg.types.json import set_json_loads

        self.populate()
        with self.store.connect() as c, c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            set_json_loads(custom_catalog_loads, c)
            expected = c.execute(LEGACY_SQL,
                (self.tenant, [self.source], list(self.ids), False)).fetchall()
            actual = chunk_bodies.chunk_catalog_snapshot(self.store, tenant_id=self.tenant,
                source_ids=(self.source,), document_ids=self.ids, connection=c)
        self.assertEqual(list(actual.values()), expected)
        siblings = [r for r in actual.values() if r['native_parent_id'] == 'parent/a']
        for row in siblings:
            self.assertIs(type(row['parts'][0]), CustomCatalogObject)
            self.assertIs(type(row['manifest']), CustomCatalogObject)
        siblings[0]['parts'][0]['size_bytes'] = -1
        self.assertNotEqual(siblings[1]['parts'][0]['size_bytes'], -1)


if __name__ == '__main__':
    names = [name for name in CatalogDedup.__dict__ if name.startswith('test_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(CatalogDedup(n) for n in names))
    raise SystemExit(not result.wasSuccessful())
