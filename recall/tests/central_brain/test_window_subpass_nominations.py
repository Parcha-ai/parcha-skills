"""Window priority shares existing dense nomination slots, without new fusion weight."""
import copy
import os
import unittest
from unittest import mock

from recall_server import passage_retrieval as native
from tests.central_brain.test_balanced_passage_selection import row, keys


class WindowNominations(unittest.TestCase):
    def test_full_parent_envelope_does_not_expand(self):
        legs = tuple((name, weight, [row(f'{name}{i}', f'{name}{i}', 100-i) for i in range(5)] +
            [row(f'common{i}', f'{name}-common{i}', 90-i) for i in range(30)])
            for name,weight in (('dense', .15), ('passage-lexical', .30), ('sparse-exact', .55)))
        nominations = native.dense_subpass_nominations(legs[0][2], [legs[0][2][4]], limit=5)
        candidate = native.collapse_document_candidates(legs, limit=30, nominate_per_arm=5,
            passage_budget='admitted', dense_nominations=nominations)
        self.assertEqual(len(candidate), 45)
        self.assertLessEqual(len(keys(candidate)), 135)

    def test_actual_search_wires_only_successful_window_into_balanced_rrf(self):
        from tests.central_brain.test_temporal_hints import SearchWiringTests
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        owner = SearchWiringTests()
        for fusion, balanced, rerank in (('rrf','1',True), ('rrf','0',True), ('convex','1',True), ('rrf','1',False)):
            with self.subTest(fusion=fusion,balanced=balanced,rerank=rerank):
                store=owner._Store(); store.fusion_mode=fusion
                if rerank: store.rerank_runtime=RerankWiringTests._FakeRerank()
                calls=[];retrieval=owner._retrieval(store,dense_calls=calls)
                with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES':balanced}), \
                        mock.patch.object(native,'collapse_document_candidates',wraps=native.collapse_document_candidates) as collapse:
                    owner._search(retrieval, 'the deploy on 2026-05-03 failed')
                self.assertEqual(len(calls),2)
                options=collapse.call_args_list[0].kwargs
                if fusion=='rrf' and balanced=='1' and rerank:
                    self.assertIsNotNone(options['dense_nominations'])
                    self.assertEqual(len(options['dense_nominations']),5)
                else:
                    self.assertNotIn('dense_nominations',options)

    def test_failed_window_does_not_replace_merged_dense_nominations(self):
        from tests.central_brain.test_temporal_hints import SearchWiringTests
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        owner=SearchWiringTests();store=owner._Store();store.fusion_mode='rrf'
        store.rerank_runtime=RerankWiringTests._FakeRerank()
        retrieval=owner._retrieval(store); original=retrieval._dense_candidates
        def failed(query,**kwargs):
            if kwargs.get('since') is not None:return [],'deadline-exceeded','unavailable',None
            return original(query,**kwargs)
        retrieval._dense_candidates=failed
        with mock.patch.object(native,'collapse_document_candidates',wraps=native.collapse_document_candidates) as collapse:
            owner._search(retrieval,'the deploy on 2026-05-03 failed')
        self.assertIsNone(collapse.call_args_list[0].kwargs['dense_nominations'])

    def test_duplicate_parents_share_slots_and_inputs_stay_unchanged(self):
        full = [row('a', 'a1', 1), row('a', 'a2', .9), row('b', 'b', .8), row('c', 'c', .7)]
        window = [row('a', 'a1', 1), row('w', 'w', .6), row('w', 'w2', .5), row('x', 'x', .4)]
        before = copy.deepcopy((full, window))
        actual = native.dense_subpass_nominations(full, window, limit=5)
        self.assertEqual(actual, frozenset(('source:test', p) for p in ('a', 'w', 'b', 'x', 'c')))
        self.assertEqual((full, window), before)

    def test_absent_window_keeps_original_nomination_and_scores(self):
        full = [row(str(i), str(i), 1-i/100) for i in range(50)]
        self.assertIsNone(native.dense_subpass_nominations(full, [], limit=5))
        legs = (('dense', .15, full),)
        original = native.collapse_document_candidates(legs, limit=30, nominate_per_arm=5, passage_budget='admitted')
        self.assertEqual(original, native.collapse_document_candidates(legs, limit=30,
            nominate_per_arm=5, passage_budget='admitted', dense_nominations=None))

    def test_window_top_reaches_existing_budget_without_reranking_fused_head(self):
        full = [row(f'd{i}', f'd{i}', 1-i/100) for i in range(20)]
        window = [full[13]]
        legs = (('dense', .15, full),
            ('passage-lexical', .30, [row(f'l{i}', f'l{i}', 20-i/100) for i in range(40)]),
            ('sparse-exact', .55, [row(f's{i}', f's{i}', 30-i/100) for i in range(40)]))
        options = dict(limit=30, nominate_per_arm=5, passage_budget='admitted')
        original = native.collapse_document_candidates(legs, **options)
        nominations = native.dense_subpass_nominations(full, window, limit=5)
        candidate = native.collapse_document_candidates(legs, **options, dense_nominations=nominations)
        self.assertNotIn('d13', keys(original))
        self.assertIn('d13', keys(candidate))
        self.assertEqual([(r['logical_document_id'], r['rank']) for r in original[:30]],
                         [(r['logical_document_id'], r['rank']) for r in candidate[:30]])
        self.assertLessEqual(len(candidate), 45)
        self.assertLessEqual(len(keys(candidate)), 135)
        # Authority rebuilds cannot reintroduce denied window nominations.
        survivors = set(keys(candidate)) - {'d13'}
        filtered = tuple((name, weight, [r for r in rows if r['passage_id'] in survivors]) for name,weight,rows in legs)
        rebuilt = native.collapse_document_candidates(filtered, limit=len(candidate), nominate_per_arm=0,
            passage_budget=len(keys(candidate)), dense_nominations=nominations)
        self.assertNotIn('d13', keys(rebuilt))
        self.assertLessEqual(set(keys(rebuilt)), survivors)


if __name__ == '__main__':
    unittest.main()
