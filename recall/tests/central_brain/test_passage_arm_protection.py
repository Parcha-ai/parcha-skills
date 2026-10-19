"""Strong single-arm evidence survives the bounded native passage selector."""
import copy
import os
import unittest
from unittest import mock

from tests.central_brain.test_balanced_passage_selection import keys, row
from tests.central_brain.test_canonical_retrieval import RerankWiringTests
from recall_server.passage_retrieval import collapse_document_candidates


def sibling_competition():
    dense = [row('answer-parent', key, 1 - i / 100)
             for i, key in enumerate(('dense-first', 'dense-second', 'answer'))]
    siblings = [row('answer-parent', f'generic-{i}', 100 - i) for i in range(5)]
    competitors = [row(f'other-{i // 3}', f'other-passage-{i}', 90 - i)
                   for i in range(30)]
    # Lexical agreement dominates single-arm dense evidence under weighted RRF.
    dense.extend(copy.deepcopy(siblings))
    return (('dense', .15, dense), ('passage-lexical', .30, siblings + competitors),
            ('sparse-exact', .55, []))


class PassageArmProtectionTests(unittest.TestCase):
    def select(self, legs, **kwargs):
        return collapse_document_candidates(
            legs, **dict(limit=50, nominate_per_arm=5, passage_budget='admitted',
                         preserve_arm_passages=True, **kwargs))

    def test_native_search_keeps_dense_third_passage_without_more_provider_inputs(self):
        legs = sibling_competition()
        baseline = collapse_document_candidates(legs, limit=50, nominate_per_arm=5,
                                                passage_budget='admitted')
        self.assertNotIn('answer', keys(baseline))
        owner = RerankWiringTests()
        store = owner._Store()
        store.fusion_mode = 'rrf'
        store.rerank_runtime = owner._FakeRerank({'answer': 1})
        retrieval = owner._retrieval(store)
        retrieval._dense_candidates = lambda *a, **k: (legs[0][2], 'ok', 'fixture', 8)
        retrieval._lexical_candidates = lambda *a, **k: (legs[1][2], 'ok')
        retrieval._sparse_candidates = lambda *a, **k: (legs[2][2], 'ok')
        with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': '1'}):
            result = owner._search(retrieval)
        self.assertEqual(len(store.rerank_runtime.calls), 1)
        documents = store.rerank_runtime.calls[0]['documents']
        self.assertEqual(len(documents), len(keys(baseline)))
        self.assertIn('answer', [document.rsplit('\n\n', 1)[-1] for document in documents])
        self.assertEqual(result['results'][0]['matching_ranges'][0]['passage_id'], 'answer')

    def test_competing_parents_keep_their_floor_and_original_capacity(self):
        legs = sibling_competition()
        before = copy.deepcopy(legs)
        baseline = collapse_document_candidates(legs, limit=50, nominate_per_arm=5,
                                                passage_budget='admitted')
        selected = self.select(legs)
        self.assertEqual(len(keys(selected)), len(keys(baseline)))
        self.assertEqual([item['logical_document_id'] for item in selected],
                         [item['logical_document_id'] for item in baseline])
        self.assertTrue(all(item['matching_ranges'] for item in selected))
        self.assertEqual(legs, before)
        self.assertEqual(selected, self.select(legs))

    def test_shared_or_repeated_arm_nominees_do_not_consume_extra_slots(self):
        legs = sibling_competition()
        selected = self.select(legs)
        legs[0][2].append(copy.deepcopy(legs[0][2][0]))
        self.assertEqual(keys(selected), keys(self.select(legs)))
        self.assertEqual(len(keys(selected)), len(set(keys(selected))))

    def test_parent_floor_wins_when_nominations_exceed_remaining_capacity(self):
        legs = (('dense', .15, [row('a', f'a-{i}', 10 - i) for i in range(8)] +
                 [row('b', 'b-only', .1)]),
                ('passage-lexical', .30, [row('a', f'lex-{i}', 10 - i) for i in range(8)]))
        selected = self.select(legs)
        self.assertEqual(len(keys(selected)), 4)
        self.assertIn('b-only', keys(selected))
        self.assertIn('lex-1', keys(selected))
        self.assertIn('lex-0', keys(selected))
        self.assertNotIn('a-2', keys(selected))

    def test_authority_rebuild_cannot_refill_rejected_nominees(self):
        legs = sibling_competition()
        selected = self.select(legs)
        before = set(keys(selected))
        self.assertIn('answer', before)
        survivors = before - {'answer', 'dense-first'}
        filtered = tuple((name, weight, [item for item in rows
                                        if item['passage_id'] in survivors])
                         for name, weight, rows in legs)
        rebuilt = collapse_document_candidates(
            filtered, limit=50, nominate_per_arm=5, passage_budget=len(before),
            preserve_arm_passages=True)
        self.assertEqual(set(keys(rebuilt)), survivors)
        self.assertNotIn('answer', keys(rebuilt))

    def test_disabled_and_no_nomination_paths_preserve_existing_selection(self):
        legs = sibling_competition()
        for budget in (None, 'admitted'):
            baseline = collapse_document_candidates(legs, limit=50, passage_budget=budget)
            candidate = collapse_document_candidates(legs, limit=50, passage_budget=budget,
                                                     preserve_arm_passages=True)
            self.assertEqual(candidate, baseline)

    def test_strong_lexical_and_exact_identifier_survive_semantic_siblings(self):
        dense = [row('a', f'semantic-{i}', 1 - i / 100) for i in range(12)]
        lexical = [row('a', 'exact-phrase', 100), row('b', 'b-only', 90)]
        sparse = [row('a', 'release-identifier', 100), row('c', 'c-only', 90)]
        legs = (('dense', .15, dense), ('passage-lexical', .30, lexical),
                ('sparse-exact', .55, sparse))
        selected = self.select(legs)
        self.assertEqual(len(keys(selected)), 5)
        self.assertIn('exact-phrase', keys(selected))
        self.assertIn('release-identifier', keys(selected))
        self.assertIn('b-only', keys(selected))
        self.assertIn('c-only', keys(selected))

    def test_later_correction_text_and_receipt_survive_without_rewriting(self):
        from tests.central_brain.test_balanced_passage_selection import fixture
        legs = fixture()
        report = 'Earlier investigation. ' * 100 + 'Final correction: rollback the release.'
        for _, _, rows in legs:
            for item in rows:
                if item['passage_id'] == 'answer':
                    item['text_redacted'] = report
                    item['receipts'] = ['recall://source:test/final-correction?rev=2#item=0']
        selected = self.select(legs)
        correction = next(item for parent in selected for item in parent['matching_ranges']
                          if item['passage_id'] == 'answer')
        self.assertEqual(correction['text'], report)
        self.assertFalse(correction['text_clipped'])
        self.assertEqual(correction['receipts'],
                         ['recall://source:test/final-correction?rev=2#item=0'])


if __name__ == '__main__':
    unittest.main()
