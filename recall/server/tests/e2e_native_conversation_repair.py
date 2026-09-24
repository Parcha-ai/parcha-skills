#!/usr/bin/env python3
"""Metadata-only repair for idle acknowledged sessions, using real PG and archives."""
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
import e2e_native_conversations as fixtures  # noqa:E402
from recall_server.canonical_thinning import _compact_event_expression  # noqa:E402
from recall_server.logical_evidence_projection import mark_logical_evidence_dirty  # noqa:E402


class NativeMetadataRepair(unittest.TestCase):
    setUpClass = classmethod(fixtures.NativeConversations.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.NativeConversations.tearDownClass.__func__)
    setUp = fixtures.NativeConversations.setUp
    add = fixtures.NativeConversations.add
    project = fixtures.NativeConversations.project
    search = fixtures.NativeConversations.search

    def historical(self, source, tail='idle deployment evidence'):
        self.add(source, provenance=False, tail=tail)
        self.project()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_evidence_documents SET conversation_id=NULL,conversation_strand_id=NULL WHERE tenant_id=%s AND source_id=%s', (self.tenant, source))
            c.execute("UPDATE canonical_documents SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (self.tenant, source))
            c.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (self.tenant, source))
            c.execute('UPDATE canonical_events event SET canonical_redacted=' + _compact_event_expression('event') + ' WHERE tenant_id=%s AND source_id=%s', (self.tenant, source))

    def repair(self, *, sources=None, apply=True, limit=100, after=None):
        from recall_server.native_conversation_repair import NativeConversationRepair
        return NativeConversationRepair(self.store, self.projection).run(
            tenant_id=self.tenant, source_ids=sources or self.sources[:2], harness='codex',
            apply=apply, limit=limit, after=after)

    def catalog(self):
        with self.store.connect() as c:
            return c.execute('SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s ORDER BY source_id', (self.tenant,)).fetchall()

    def immutable_state(self):
        with self.store.connect() as c:
            return {table: c.execute('SELECT md5(coalesce(string_agg(row_to_json(t)::text,\'\' ORDER BY row_to_json(t)::text),\'\')) AS value FROM '+table+' t WHERE tenant_id=%s', (self.tenant,)).fetchone()['value']
                    for table in ('canonical_events','canonical_documents','canonical_chunks','canonical_ingest_jobs','canonical_passages','canonical_evidence_document_parts','canonical_evidence_document_queue')}

    def test_idle_thinned_root_repairs_without_upload_reprojection_or_new_ack(self):
        self.historical(self.sources[0])
        before, catalog = self.immutable_state(), self.catalog()
        with patch.object(self.archive, 'put_raw', wraps=self.archive.put_raw) as upload:
            dry = self.repair(apply=False)
            self.assertEqual(dry['would_update'], 1)
            self.assertEqual(self.catalog(), catalog)
            repaired = self.repair()
            self.assertEqual(repaired['updated'], 1)
            self.assertEqual(self.immutable_state(), before)
            upload.assert_not_called()
        after = self.catalog()[0]
        self.assertEqual(after['conversation_id'], 'codex:' + self.native)
        self.assertEqual(after['conversation_strand_id'], 'root')
        self.assertEqual({k:v for k,v in after.items() if not k.startswith('conversation_')},
                         {k:v for k,v in catalog[0].items() if not k.startswith('conversation_')})
        self.assertEqual(self.repair()['selected'], 0)

    def test_scoped_copied_roots_group_and_cursor_reaches_other_source(self):
        self.historical(self.sources[0], tail='local idle deployment evidence')
        self.historical(self.sources[1], tail='unique remote deployment evidence')
        self.assertEqual(len(self.search(self.sources[:2])), 2)
        first = self.repair(limit=1)
        self.assertEqual(first['updated'], 1)
        self.assertFalse(first['scope_exhausted'])
        second = self.repair(limit=1, after=first['next_after'])
        self.assertEqual(second['updated'], 1)
        self.assertTrue(second['scope_exhausted'])
        results = self.search(self.sources[:2])
        self.assertEqual(len(results), 1)
        self.assertEqual({item['source_id'] for item in results[0]['conversation_documents']}, set(self.sources[:2]))
        denied = self.search([self.sources[0]])
        self.assertNotIn(self.sources[1], json.dumps(denied))

    def test_revision_race_between_archive_read_and_update_is_skipped(self):
        self.historical(self.sources[0])
        original = self.archive.read_raw
        changed = False
        def read(reference):
            nonlocal changed
            payload = original(reference)
            if not changed:
                changed = True
                with self.store.connect() as c:
                    c.execute('UPDATE canonical_evidence_documents SET revision=revision+1 WHERE tenant_id=%s', (self.tenant,))
            return payload
        with patch.object(self.archive, 'read_raw', side_effect=read):
            report = self.repair()
        self.assertEqual(report['updated'], 0)
        self.assertEqual(report['raced'], 1)
        self.assertIsNone(self.catalog()[0]['conversation_id'])

    def test_pending_forget_between_archive_read_and_update_cannot_fill_metadata(self):
        self.historical(self.sources[0])
        original = self.archive.read_raw
        changed = False
        def read(reference):
            nonlocal changed
            payload = original(reference)
            if not changed:
                changed = True
                with self.store.connect() as c:
                    c.execute('UPDATE canonical_chunks SET deleted_at=clock_timestamp() WHERE tenant_id=%s', (self.tenant,))
                    mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.sources[0], native_ids=['turn:0'], reason='forget')
            return payload
        with patch.object(self.archive, 'read_raw', side_effect=read):
            report = self.repair()
        self.assertEqual(report['updated'], 0)
        self.assertIsNone(self.catalog()[0]['conversation_id'])


    def test_deleted_catalog_is_not_recreated_after_archive_read(self):
        self.historical(self.sources[0])
        original = self.archive.read_raw
        def read(reference):
            payload = original(reference)
            with self.store.connect() as c:
                c.execute('DELETE FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,))
            return payload
        with patch.object(self.archive, 'read_raw', side_effect=read):
            report = self.repair()
        self.assertEqual(report['updated'], 0)
        self.assertEqual(report['raced'], 1)
        self.assertEqual(self.catalog(), [])

    def test_unknown_and_corrupt_records_are_reported_without_fabricated_identity(self):
        self.add(self.sources[0], session='unknown-legacy-identity', provenance=False)
        self.project()
        report = self.repair()
        self.assertEqual((report['unknown'], report['updated']), (1, 0))
        before = self.catalog()
        with patch.object(self.archive, 'read_raw', return_value=b'{}\n'):
            corrupt = self.repair()
        self.assertEqual((corrupt['unavailable'], corrupt['updated']), (1, 0))
        self.assertEqual(self.catalog(), before)

    def test_commit_failure_propagates_even_when_metadata_committed(self):
        from contextlib import contextmanager
        self.historical(self.sources[0])
        connect = self.store.connect
        @contextmanager
        def interrupted_commit():
            changed = False
            with connect() as connection:
                class Proxy:
                    def __getattr__(self, name):
                        return getattr(connection, name)
                    def execute(self, query, values=None):
                        nonlocal changed
                        changed |= query.startswith('UPDATE canonical_evidence_documents evidence')
                        return connection.execute(query, values)
                yield Proxy()
            if changed:
                raise RuntimeError('synthetic commit acknowledgement lost')
        with patch.object(self.store, 'connect', side_effect=interrupted_commit):
            with self.assertRaisesRegex(RuntimeError, 'acknowledgement lost'):
                self.repair()
        self.assertEqual(self.catalog()[0]['conversation_id'], 'codex:' + self.native)

    def mutate_pin_during_read(self, field):
        self.historical(self.sources[0])
        original = self.archive.read_raw
        changed = False
        def read(reference):
            nonlocal changed
            payload = original(reference)
            if not changed:
                changed = True
                with self.store.connect() as c:
                    c.execute('UPDATE canonical_evidence_documents SET ' + field + '=%s WHERE tenant_id=%s AND source_id=%s', ('f'*64, self.tenant, self.sources[0]))
            return payload
        with patch.object(self.archive, 'read_raw', side_effect=read):
            report = self.repair()
        self.assertEqual(report['updated'], 0)
        self.assertEqual(report['raced'], 1)
        self.assertIsNone(self.catalog()[0]['conversation_id'])

    def test_same_revision_manifest_hash_race(self):
        self.mutate_pin_during_read('manifest_content_sha256')

    def test_same_revision_document_hash_race(self):
        self.mutate_pin_during_read('document_content_sha256')


if __name__ == '__main__':
    unittest.main(verbosity=2)
