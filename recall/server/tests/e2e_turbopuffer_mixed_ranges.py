#!/usr/bin/env python3
"""A stale vendor range must not erase authorized evidence in the same parent."""
import hashlib
import json
import unittest
import os
import time
from unittest.mock import patch
from recall_server.rerank import RerankRuntime
from recall_server.db import SearchDeadlineExceeded
from recall_server.temporal_hints import TemporalHintSettings
from tests.central_brain.test_rerank_matched_ranges import RecordingProvider

import e2e_handback_projection as fixture


class MixedRanges(unittest.TestCase):
    setUp = fixture.HandbackPublication.setUp
    ingest = fixture.HandbackPublication.ingest
    project = fixture.HandbackPublication.project

    def seed(self):
        self.ingest('current', text='Deployment finding: keep the current authorized evidence.')
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=20)
        ns = self.client.namespace(self.settings.namespace(self.tenant))
        self.assertEqual(len(ns.rows), 1)
        current = next(iter(ns.rows.values()))
        stale = dict(current, id='psg_stale_missing_catalog', text='deployment finding STALE_PRIVATE_MARKER',
                     text_sha256=hashlib.sha256(b'deployment finding STALE_PRIVATE_MARKER').hexdigest(),
                     manifest_object_key='STALE_PRIVATE_MANIFEST', manifest_content_sha256='f'*64,
                     native_parent_id='STALE_PRIVATE_PARENT', header='STALE_PRIVATE_HEADER', revision=999)
        ns.write(upsert_rows=[stale])
        return ns, current, stale

    def test_mixed_current_and_stale_parent_keeps_only_authorized_evidence(self):
        ns, current, stale = self.seed()
        result = self.retrieval.search('deployment finding', lexical_query='deployment finding',
                                       since=None, until=None, limit=10, include_arms=False)
        self.assertEqual(len(result['results']), 1)
        document = result['results'][0]
        self.assertEqual({r['passage_id'] for r in document['matching_ranges']}, {current['id']})
        self.assertIn('current authorized evidence', document['matching_ranges'][0]['text'])
        self.assertNotIn('STALE_PRIVATE', json.dumps(result))
        self.assertTrue(all(stale['id'] not in json.dumps(q.get('filters', [])) for q in ns.queries
                            if q['rank_by'] == ('id', 'asc')))


    def test_survivor_matches_clean_search_with_actual_fusion_options(self):
        ns, current, stale = self.seed()
        for singleton in (False, True):
            self.store.verified_singleton_receipts = singleton
            for mode in ('rrf', 'convex'):
                self.store.fusion_mode = mode
                self.store.fusion_alphas = {'dense': .2, 'passage-lexical': .7, 'sparse-exact': .1}
                self.store.temporal_hints = TemporalHintSettings(enabled=True)
                for arms in (False, True):
                    with self.subTest(singleton=singleton, mode=mode, include_arms=arms):
                        ns.rows.pop(stale['id'], None)
                        clean = self.retrieval.search('deployment finding October 2026',
                            lexical_query='deployment finding', since=None, until=None,
                            limit=10, include_arms=arms)
                        ns.write(upsert_rows=[stale])
                        mixed = self.retrieval.search('deployment finding October 2026',
                            lexical_query='deployment finding', since=None, until=None,
                            limit=10, include_arms=arms)
                        self.assertEqual(mixed['results'], clean['results'])
                        self.assertEqual(mixed['diagnostics']['fusion'], clean['diagnostics']['fusion'])
                        self.assertEqual(mixed['results'][0]['temporal_boost'], clean['results'][0]['temporal_boost'])
                        self.assertNotIn('STALE_PRIVATE', json.dumps(mixed))

    def test_only_current_authorized_body_reaches_original_provider_boundary(self):
        ns, current, stale = self.seed()
        provider = RecordingProvider()
        self.store.rerank_runtime = RerankRuntime(protocol='voyage', key_env='RERANK_TEST_KEY',
            max_candidates=50, max_doc_chars=20000, timeout_seconds=2.5, transport=provider)
        with patch.dict(os.environ, {'RERANK_TEST_KEY': 'synthetic-only'}):
            result = self.retrieval.search('deployment finding', lexical_query='deployment finding',
                since=None, until=None, limit=10, deadline_at=time.monotonic()+5)
        self.assertEqual(len(provider.calls), 1)
        documents = provider.calls[0][0]['documents']
        self.assertEqual(len(documents), 1)
        self.assertIn('current authorized evidence', documents[0])
        self.assertNotIn('STALE_PRIVATE', json.dumps(provider.calls))
        self.assertEqual(len(result['results']), 1)

    def test_all_stale_never_hydrates_or_calls_provider(self):
        ns, current, stale = self.seed()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s', (self.tenant,))
        provider = RecordingProvider()
        self.store.rerank_runtime = RerankRuntime(protocol='voyage', key_env='RERANK_TEST_KEY',
            max_candidates=50, transport=provider)
        ns.queries.clear()
        with patch.dict(os.environ, {'RERANK_TEST_KEY': 'synthetic-only'}):
            result = self.retrieval.search('deployment finding', lexical_query='deployment finding',
                since=None, until=None, limit=10, include_arms=True, deadline_at=time.monotonic()+5)
        self.assertEqual(result['results'], [])
        self.assertTrue(all(not values for values in result['arms'].values()))
        self.assertFalse(provider.calls)
        self.assertFalse(any(q['rank_by'] == ('id', 'asc') for q in ns.queries))

    def test_diagnostic_arms_cannot_expand_original_document_admission(self):
        from e2e_logical_evidence_projection import insert_record
        from recall_server.logical_evidence_projection import mark_logical_evidence_dirty
        ns, current, stale = self.seed()
        with self.store.connect() as c:
            insert_record(c, tenant=self.tenant, source=self.source,
                parent='other-parent', native='other-current',
                text='Ocean current unrelated background.', role='user', byte_start=0)
            mark_logical_evidence_dirty(c, tenant_id=self.tenant, source_id=self.source,
                native_ids=['other-current'], reason='ingest')
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=20)
        before = []
        arm_parents = set()
        hydrate = self.retrieval._hydrate_ranges
        def observed(results, legs, **kwargs):
            arm_parents.update(row['logical_document_id'] for _name, _weight, rows in legs for row in rows)
            before.extend((row['source_id'], row['logical_document_id'], item['passage_id'])
                for row in results for item in row['matching_ranges'])
            return hydrate(results, legs, **kwargs)
        with patch.object(self.retrieval, '_hydrate_ranges', side_effect=observed):
            result = self.retrieval.search('deployment finding', lexical_query='deployment finding',
                since=None, until=None, limit=1, include_arms=True)
        self.assertEqual(len(result['results']), 1)
        self.assertEqual(result['results'][0]['logical_document_id'], current['logical_document_id'])
        after = {(row['source_id'], row['logical_document_id'], item['passage_id'])
                 for row in result['results'] for item in row['matching_ranges']}
        self.assertTrue(after.issubset(set(before)))
        self.assertTrue(arm_parents - {current['logical_document_id']})

    def test_authority_deadline_still_fails_closed(self):
        ns, current, stale = self.seed()
        execute = self.store._execute_bounded
        def unavailable(connection, sql, values, deadline):
            if 'cardinality(passage.receipts)>0' in sql:
                raise SearchDeadlineExceeded()
            return execute(connection, sql, values, deadline)
        ns.queries.clear()
        with patch.object(self.store, '_execute_bounded', side_effect=unavailable):
            result = self.retrieval.search('deployment finding', lexical_query='deployment finding',
                since=None, until=None, limit=10)
        self.assertEqual(result['results'], [])
        self.assertEqual(result['diagnostics']['authority_status'], 'deadline-exceeded')
        self.assertFalse(any(q['rank_by'] == ('id', 'asc') for q in ns.queries))


if __name__ == '__main__':
    unittest.main(verbosity=2)
