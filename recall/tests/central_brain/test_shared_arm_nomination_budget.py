"""Unused arm nomination slots can preserve evidence outside a consensus head."""
import copy
import os
import unittest
from unittest import mock

from recall_server.passage_retrieval import collapse_document_candidates
from tests.central_brain.test_balanced_passage_selection import keys, row
from tests.central_brain.test_canonical_retrieval import RerankWiringTests


def crowded():
    goals = [row(f'goal-{i}', f'GOAL request {i}', 100-i) for i in range(30)]
    findings = [row(f'result-{i}', f'completed result {i}', 90-i) for i in range(25)]
    return (('dense', .15, copy.deepcopy(goals[:5] + findings + goals[5:])),
            ('passage-lexical', .30, copy.deepcopy(goals)),
            ('sparse-exact', .55, copy.deepcopy(goals)))


def select(legs, **extra):
    return collapse_document_candidates(legs, limit=30, nominate_per_arm=5,
                                       passage_budget='admitted', preserve_arm_passages=True,
                                       **extra)


class SharedArmNominationTests(unittest.TestCase):
    def test_native_search_scores_new_parent_and_returns_its_exact_receipt(self):
        legs = crowded()
        baseline = select(legs)
        self.assertEqual(len(baseline), 30)
        self.assertNotIn('completed result 9', keys(baseline))
        owner = RerankWiringTests()
        store = owner._Store()
        store.fusion_mode = 'rrf'
        store.rerank_runtime = owner._FakeRerank({'completed result 9': 1})
        store.rerank_runtime.max_candidates = 30
        retrieval = owner._retrieval(store)
        retrieval._dense_candidates = lambda *a, **k: (copy.deepcopy(legs[0][2]), 'ok', 'fixture', 55)
        retrieval._lexical_candidates = lambda *a, **k: (copy.deepcopy(legs[1][2]), 'ok')
        retrieval._sparse_candidates = lambda *a, **k: (copy.deepcopy(legs[2][2]), 'ok')
        with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': '1'}):
            result = owner._search(retrieval, limit=5)
        self.assertEqual(len(store.rerank_runtime.calls), 1)
        texts = store.rerank_runtime.calls[0]['documents']
        self.assertIn('completed result 9', [text.rsplit('\n\n', 1)[-1] for text in texts])
        self.assertEqual(len(texts), 45)
        self.assertEqual(result['results'][0]['logical_document_id'], 'result-9')
        self.assertEqual(result['results'][0]['matching_ranges'][0]['receipts'],
                         ['receipt:completed result 9'])
        self.assertEqual(store.rerank_runtime.max_candidates, 30)
        self.assertLessEqual(store.rerank_runtime.calls[0]['deadline_seconds'], 20)

    def test_shared_budget_keeps_original_head_and_stops_at_its_bound(self):
        legs = crowded(); before = copy.deepcopy(legs)
        baseline = select(legs)
        result = select(legs, share_arm_nominations=True)
        self.assertEqual(result[:30], baseline)
        self.assertEqual(len(result), 45)
        self.assertTrue(all(r['nominated'] for r in result[30:]))
        self.assertIn('completed result 14', keys(result))
        self.assertNotIn('completed result 15', keys(result))
        self.assertEqual(legs, before)
        self.assertEqual(result, select(legs, share_arm_nominations=True))

    def test_old_nominees_and_consensus_survive_disjoint_arm_competition(self):
        legs = tuple((name,weight,[row(f'{name}-{i}',f'{name}-{i}',100-i) for i in range(45)])
                     for name,weight in (('dense',.15),('passage-lexical',.30),('sparse-exact',.55)))
        baseline = select(legs); result = select(legs, share_arm_nominations=True)
        ids = lambda rows: [r['logical_document_id'] for r in rows]
        self.assertEqual(ids(result[:30]), ids(baseline[:30]))
        self.assertTrue(set(ids(baseline)) <= set(ids(result)))
        self.assertLessEqual(len(result),45)
        self.assertLessEqual(len(keys(result)),135)

    def test_repeated_rows_do_not_spend_slots_or_allow_unbounded_prefix(self):
        legs=crowded(); dense=legs[0][2]
        dense[5:5]=[copy.deepcopy(dense[0]) for _ in range(50)]
        result=select(legs,share_arm_nominations=True)
        self.assertEqual(len(result),45)
        self.assertIn('completed result 14',keys(result))
        self.assertNotIn('completed result 24',keys(result))

    def test_authority_rebuild_does_not_refill_removed_parent(self):
        legs=crowded(); selected=select(legs,share_arm_nominations=True)
        survivors=set(keys(selected))-{'completed result 9'}
        filtered=tuple((name,weight,[r for r in rows if r['passage_id'] in survivors])
                       for name,weight,rows in legs)
        rebuilt=collapse_document_candidates(filtered,limit=len(selected),nominate_per_arm=0,
            passage_budget=len(keys(selected)),preserve_arm_passages=True,share_arm_nominations=True)
        self.assertEqual(set(keys(rebuilt)),survivors)

    def test_disabled_and_convex_paths_are_unchanged(self):
        legs=crowded()
        for mode,nominate in [('rrf',0),('convex',5)]:
            args=dict(limit=30,fusion=mode,nominate_per_arm=nominate)
            self.assertEqual(collapse_document_candidates(legs,**args),
                             collapse_document_candidates(legs,**args,share_arm_nominations=True))

if __name__=='__main__':unittest.main()
