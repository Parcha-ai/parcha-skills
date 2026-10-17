"""One bounded passage-allocation experiment, preserving weighted parent floors."""
import copy
import os
import time
import unittest
from unittest import mock

from tests.central_brain.test_balanced_passage_selection import keys, row
from recall_server.passage_retrieval import collapse_document_candidates


def evidence_legs():
    return (
        ('dense', .15, [row('parent', p, 10-i) for i, p in enumerate(('dense-1', 'dense-2', 'answer'))]),
        ('passage-lexical', .30, [row('parent', p, 10-i) for i, p in enumerate(('lexical-1', 'lexical-2', 'answer'))]),
        ('sparse-exact', .55, [row('parent', p, 10-i) for i, p in enumerate(('identifier-1', 'identifier-2', 'identifier-3'))]),
    )


class EqualExtraPassageTests(unittest.TestCase):
    def test_actual_native_search_scores_cross_arm_evidence_at_same_three_inputs(self):
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        owner = RerankWiringTests()
        store = owner._Store()
        store.fusion_mode = 'rrf'
        store.rerank_runtime = owner._FakeRerank({'answer': 1})
        retrieval = owner._retrieval(store)
        legs = evidence_legs()
        retrieval._dense_candidates = lambda *a, **k: (legs[0][2], 'ok', 'synthetic', 3)
        retrieval._lexical_candidates = lambda *a, **k: (legs[1][2], 'ok')
        retrieval._sparse_candidates = lambda *a, **k: (legs[2][2], 'ok')
        with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': '1'}):
            result = owner._search(retrieval)
        self.assertEqual(len(store.rerank_runtime.calls), 1)
        self.assertEqual(len(store.rerank_runtime.calls[0]['documents']), 3)
        self.assertIn('answer', keys(result['results']))
        self.assertIn('identifier-1', keys(result['results']), 'original weighted mandatory floor')

    def test_identifier_tradeoff_is_explicit_without_query_specific_exception(self):
        legs = (
            ('sparse-exact', .55, [row('p', 'mandatory', 2), row('p', 'exact-identifier', 1)]),
            ('dense', .15, [row('p', 'correlated-generic', 1)]),
            ('passage-lexical', .30, [row('p', 'correlated-generic', 1)]),
        )
        result = collapse_document_candidates(legs, limit=1, passage_budget=2)
        self.assertEqual(set(keys(result)), {'mandatory', 'correlated-generic'})

    def test_sparse_only_order_and_nominated_floor_are_retained(self):
        legs = (('sparse-exact', .55, [row('big', f'id-{i}', 10-i) for i in range(6)]
                  + [row('small', 'small-identifier', .1)]),)
        result = collapse_document_candidates(legs, limit=1, nominate_per_arm=2, passage_budget='admitted')
        self.assertEqual(set(keys(result)), {'id-0', 'id-1', 'id-2', 'small-identifier'})
        self.assertEqual(len(result), 2)

    def test_duplicate_hits_do_not_create_extra_support(self):
        legs = evidence_legs()
        before = collapse_document_candidates(legs, limit=1, passage_budget='admitted')
        legs[0][2].extend(copy.deepcopy(legs[0][2][:2]))
        self.assertEqual(before, collapse_document_candidates(legs, limit=1, passage_budget='admitted'))

    def test_frozen_integer_rebuild_cannot_refill_denied_passage(self):
        legs = evidence_legs()
        selected = collapse_document_candidates(legs, limit=1, passage_budget='admitted')
        capacity = len(keys(selected))
        survivors = set(keys(selected)) - {'answer'}
        self.assertNotIn('identifier-2', survivors)
        authorized = tuple((name, weight, [r for r in rows if r['passage_id'] in survivors])
                           for name, weight, rows in legs)
        rebuilt = collapse_document_candidates(authorized, limit=1, passage_budget=capacity)
        self.assertEqual(set(keys(rebuilt)), survivors)
        self.assertEqual(len(keys(rebuilt)), capacity-1)


    def test_sparse_request_keeps_exact_eligibility_but_ranks_full_question(self):
        from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
        from tests.central_brain.test_turbopuffer_retrieval import ArmTests
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        client = FakeTurbopuffer()
        owner = ArmTests()
        namespace = owner._seed(client)
        retrieval, store = owner._retrieval(client)
        store.fusion_mode = 'rrf'
        store.rerank_runtime = RerankWiringTests._FakeRerank()
        arguments = dict(since='2026-05-01', until='2026-05-04', candidate_limit=40,
                         actor_ids=None, actor_relations=None, deadline_at=time.monotonic() + 5,
                         original_query='what did Greptile flag on PR #6076?')
        for setting in ('0', '1'):
            with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': setting}):
                _, status = retrieval._sparse_candidates('greptile flagged 6076', **arguments)
            self.assertEqual(status, 'ok')
        baseline, candidate = namespace.queries[-2:]
        self.assertEqual(candidate['rank_by'], ('text', 'BM25', 'greptile flagged 6076'))
        self.assertNotEqual(candidate['rank_by'], baseline['rank_by'])
        self.assertEqual({k: v for k, v in candidate.items() if k not in {'rank_by', 'timeout'}},
                         {k: v for k, v in baseline.items() if k not in {'rank_by', 'timeout'}})
        self.assertGreater(candidate['timeout'], 0)
        self.assertLessEqual(candidate['timeout'], baseline['timeout'])
        self.assertLessEqual(baseline['timeout'], 5)
        self.assertEqual(len(namespace.queries), 2, 'one arm call per variant')

    def test_disabled_and_convex_sparse_wire_remains_identifier_only(self):
        from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
        from tests.central_brain.test_turbopuffer_retrieval import ArmTests
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        client = FakeTurbopuffer()
        owner = ArmTests()
        namespace = owner._seed(client)
        retrieval, store = owner._retrieval(client)
        arguments = dict(since=None, until=None, candidate_limit=40, actor_ids=None,
                         actor_relations=None, deadline_at=time.monotonic() + 5)
        for fusion, runtime in (('rrf', None), ('convex', RerankWiringTests._FakeRerank())):
            store.fusion_mode, store.rerank_runtime = fusion, runtime
            with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': '1'}):
                retrieval._sparse_candidates('greptile flagged 6076', **arguments)
            self.assertEqual(namespace.queries[-1]['rank_by'], ('text', 'BM25', '6076'))

if __name__ == '__main__':
    unittest.main()
