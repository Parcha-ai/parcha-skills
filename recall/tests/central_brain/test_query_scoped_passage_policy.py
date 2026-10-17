"""Query allowance survives sparse parent fragmentation before native scoring."""
import os
import time
import unittest
from unittest import mock

from tests.central_brain.test_balanced_passage_selection import keys, row
from recall_server.passage_retrieval import collapse_document_candidates


class QueryScopedPassageTests(unittest.TestCase):
    def test_singleton_parent_does_not_shrink_configured_rerank_allowance(self):
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        owner = RerankWiringTests()
        store = owner._Store()
        store.fusion_mode = 'rrf'
        store.rerank_runtime = owner._FakeRerank({'target-4': 1})
        store.rerank_runtime.max_candidates = 2
        retrieval = owner._retrieval(store)
        dense = [row('target', f'target-{i}', 10 - i) for i in range(8)]
        dense.append(row('small', 'small-evidence', .01))
        retrieval._dense_candidates = lambda *a, **k: (dense, 'ok', 'synthetic', 9)
        retrieval._lexical_candidates = lambda *a, **k: ([], 'ok')
        retrieval._sparse_candidates = lambda *a, **k: ([], 'ok')
        with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': '1'}):
            result = owner._search(retrieval, limit=2)
        self.assertEqual(len(store.rerank_runtime.calls), 1)
        self.assertEqual(len(store.rerank_runtime.calls[0]['documents']), 6)
        self.assertIn('target-4', keys(result['results']))
        self.assertIn('small-evidence', keys(result['results']))
        self.assertEqual({d['logical_document_id'] for d in result['results']}, {'target', 'small'})

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

    def test_nominations_keep_parent_guarantee_without_multiplying_every_parent(self):
        legs = tuple((name, 1.0, [row(f'{name}-{i}', f'{name}-{i}-range-{j}', 10 - j)
                                  for i in range(2) for j in range(4)])
                     for name in ('dense', 'passage-lexical', 'sparse-exact'))
        output = collapse_document_candidates(legs, limit=1, nominate_per_arm=2,
                                              passage_budget='query')
        self.assertEqual(len(output), 6)
        self.assertEqual(len(keys(output)), 6, 'one per nominee, not three per actual parent')

    def test_default_query_budget_stays_90_with_many_admitted_parents(self):
        legs = (('dense', 1.0, [row(f'p-{i}', f'p-{i}-r-{j}', 1000 - i * 10 - j)
                                for i in range(40) for j in range(6)]),)
        output = collapse_document_candidates(legs, limit=30, nominate_per_arm=35,
                                              passage_budget='query')
        self.assertEqual(len(output), 35)
        self.assertEqual(len(keys(output)), 90)
        self.assertTrue(all(d['matching_ranges'] for d in output))

    def test_query_budget_ceiling_uses_existing_legacy_fallback(self):
        legs = (('dense', 1.0, [row(f'p-{i // 4}', f'r-{i}', 2000 - i)
                                for i in range(1400)]),)
        self.assertEqual(collapse_document_candidates(legs, limit=350),
                         collapse_document_candidates(legs, limit=350, passage_budget='query'))

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
