#!/usr/bin/env python3
"""An admitted append must not starve while sibling parents occupy the worker."""
import unittest
from unittest.mock import patch

import e2e_logical_append_capability as fixtures


class DispatchRefresh(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish

    def seed_waiting_parents(self):
        self.parent = 'small'
        self.ingest('small-base', 1, text='Small base.')
        self.parent = 'large'
        self.ingest('large-base', 1, text='Large base. ' * 1000)
        self.assertEqual(self.publish()['documents'], 2)
        self.parent = 'small'
        self.ingest('small-new', 2)
        self.parent = 'large'
        self.ingest('large-new', 2)
        self.logical.append_enabled = True

    def test_queued_append_refreshes_after_sibling_commits_without_another_admission(self):
        self.seed_waiting_parents()
        commit = self.logical._commit_upload
        dispatched = []

        def finish_sibling(candidate, upload):
            dispatched.append(candidate.native_parent_id)
            result = commit(candidate, upload)
            if candidate.native_parent_id == 'small':
                self.assertEqual(result, 'committed')
                self.parent = 'large'
                self.ingest('large-later', 3)
            return result

        with patch.object(self.logical, '_commit_upload', finish_sibling):
            result = self.publish()
        self.assertEqual(dispatched[0], 'small')
        self.assertEqual(result['documents'], 2)
        self.assertEqual(result['source_races'], 0)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(result['records'], 5)
        with self.store.connect() as connection:
            queued = connection.execute('SELECT native_parent_id FROM canonical_evidence_document_queue WHERE tenant_id=%s', (self.tenant,)).fetchall()
            current = connection.execute('SELECT record_count,append_source_ordinal FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s', (self.tenant, self.source, 'large')).fetchone()
        self.assertEqual(queued, [])
        self.assertEqual((current['record_count'], current['append_source_ordinal']), (3, 3))


    def refused_successor(self, mutation):
        self.seed_waiting_parents()
        commit, prepare = self.logical._commit_upload, self.logical._prepare_batch_and_upload
        prepared = []

        def observed_prepare(candidates, **kwargs):
            prepared.extend(candidate.native_parent_id for candidate in candidates)
            return prepare(candidates, **kwargs)

        def finish_sibling(candidate, upload):
            result = commit(candidate, upload)
            if candidate.native_parent_id == 'small':
                self.parent = 'large'
                mutation()
            return result

        with patch.object(self.logical, '_commit_upload', finish_sibling), patch.object(
            self.logical, '_prepare_batch_and_upload', observed_prepare
        ):
            result = self.publish()
        self.assertEqual((result['documents'], result['source_races'], result['failed']), (1, 1, 0))
        self.assertNotIn('large', prepared)
        with self.store.connect() as connection:
            queued = connection.execute('SELECT generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s', (self.tenant, self.source, 'large')).fetchone()
        self.assertIsNotNone(queued)

    def change_queue(self, assignment):
        self.ingest('large-later', 3)
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_evidence_document_queue SET '+assignment+' WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s', (self.tenant, self.source, 'large'))

    def test_edit_poison_is_not_refreshed(self):
        self.refused_successor(lambda: self.ingest('large-base', 1, text='Edited original body.'))

    def test_tombstone_poison_is_not_refreshed(self):
        self.refused_successor(lambda: self.ingest('large-base', 3, kind='tombstone'))

    def test_future_retry_is_not_admitted_by_refresh(self):
        self.refused_successor(lambda: self.change_queue("next_attempt_at=clock_timestamp()+interval '1 hour'"))

    def test_quarantined_parent_is_not_admitted_by_refresh(self):
        self.refused_successor(lambda: self.change_queue('attempts=100'))

    def test_unclassified_append_is_not_refreshed(self):
        self.refused_successor(lambda: self.change_queue('append_generation=NULL,append_base_manifest_id=NULL'))

    def test_missing_capability_is_not_refreshed(self):
        def mutation():
            self.ingest('large-later', 3)
            with self.store.connect() as connection:
                connection.execute('DELETE FROM schema_migrations WHERE version=79')
        try:
            self.refused_successor(mutation)
        finally:
            with self.store.connect() as connection:
                connection.execute('INSERT INTO schema_migrations(version) VALUES(79) ON CONFLICT DO NOTHING')

    def test_same_generation_changed_timestamp_is_not_refreshed(self):
        def mutation():
            with self.store.connect() as connection:
                connection.execute('UPDATE canonical_evidence_document_queue SET changed_at=clock_timestamp() WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s', (self.tenant, self.source, 'large'))
        self.refused_successor(mutation)

    def test_disabled_append_keeps_original_stale_fence(self):
        def mutation():
            self.ingest('large-later', 3)
            self.logical.append_enabled = False
        self.refused_successor(mutation)

    def test_replaced_base_manifest_binding_is_not_refreshed(self):
        self.refused_successor(lambda: self.change_queue("append_base_manifest_id='not-the-committed-manifest'"))


if __name__ == '__main__':
    unittest.main()
