"""Fixed presentation diversity after native ranking, before final response fitting."""
from __future__ import annotations

import copy
import math
import os
import random
import unittest
from types import SimpleNamespace
from unittest import mock

from recall_server import mcp, passage_retrieval as passage
from tests.central_brain.test_canonical_retrieval import ActorRecordingStore
from tests.central_brain.test_passage_fusion import candidate


class PresentationTests(unittest.TestCase):
    class Store(ActorRecordingStore):
        search_deadline_ms = 20000
        temporal_hints = None
        query_clauses = False
        rerank_blend = 1.0

    def search(self, *, mode="ok", blend=1., grouped=False, heterogeneous=False):
        rows = []
        for index, name in enumerate('abcdef'):
            row = candidate(name, 'dense', .9 - index * .01)
            row['text_redacted'] = ('same recurring incident alert has reached quota threshold ' * 6
                                    if name != 'f' else 'independent analysis explains why the quota was reached')
            if heterogeneous and name == 'e':
                row['text_redacted'] = 'a separate product plan explains different implementation tradeoffs'
            if grouped and name in 'de':
                row['native_parent_id'] = 'shared-native-parent'
            rows.append(row)
        calls = []
        def rerank(query, documents, **kwargs):
            calls.append(documents)
            if mode == 'unavailable':
                raise passage.RerankUnavailable('fixture_unavailable')
            scored = [(i, .93 if 'independent analysis' in text else .95) for i, text in enumerate(documents)]
            return scored[:1] if mode == 'partial' else scored
        store = self.Store()
        store.rerank_blend = blend
        if mode == 'budget':
            store.rerank_min_budget_seconds = 1000
        store.rerank_runtime = SimpleNamespace(max_candidates=10, max_doc_chars=2000,
                                               fingerprint='fixture', rerank=rerank)
        if mode == 'disabled':
            store.rerank_runtime = None
        retrieval = passage.PassageHintRetrieval(store, tenant_id='tenant:test',
                                                  sources=['source:test'], policy_fingerprint='fp-policy')
        retrieval._dense_candidates = lambda query, **kwargs: (rows, 'ok', 'ann', None)
        retrieval._lexical_candidates = lambda query, **kwargs: ([], 'ok')
        retrieval._sparse_candidates = lambda query, original_query=None, **kwargs: ([], 'skipped-prose-query')
        with mock.patch.dict(os.environ, {}, clear=True):
            result = retrieval.search('why did quota reach threshold', lexical_query='quota threshold',
                                      since=None, until=None, limit=5)
        return result, calls

    def test_heterogeneous_native_top_set_keeps_original_order(self):
        result, _ = self.search(heterogeneous=True)
        with mock.patch.object(passage, 'diversify_reranked_results', side_effect=lambda rows, **kwargs: rows):
            baseline, _ = self.search(heterogeneous=True)
        self.assertEqual(result['results'], baseline['results'])

    def test_native_search_diversifies_after_scoring_before_topfive_and_fit(self):
        result, calls = self.search()
        self.assertEqual(len(calls), 1)
        fitted = mcp.fit_search_result(mcp.clip_search_snippets(mcp.omit_search_spans(result), 32))
        self.assertEqual([r['logical_document_id'][5] for r in fitted['results']], ['a', 'f', 'b', 'c', 'd'])
        self.assertEqual(fitted['results'][1]['matching_ranges'][0]['receipts'],
                         ['recall://source:test/f?rev=1#item=0'])
        self.assertEqual(fitted['diagnostics']['rerank_status'], 'ok')

    def test_native_grouping_runs_before_crowding_decision(self):
        result, calls = self.search(grouped=True)
        self.assertEqual(len(calls), 1)
        self.assertEqual([r['logical_document_id'][5] for r in result['results']], ['a', 'b', 'c', 'd', 'f'])
        grouped = result['results'][3]
        self.assertEqual(grouped['similar_documents'][0]['logical_document_id'][5], 'e')
        self.assertEqual(result['diagnostics']['near_duplicates_folded'], 1)

    def test_native_failure_partial_budget_disabled_and_nondefault_blend_keep_baseline(self):
        for mode, blend in [('unavailable', 1.), ('partial', 1.), ('budget', 1.), ('disabled', 1.), ('ok', .6)]:
            with self.subTest(mode=mode, blend=blend):
                result, _ = self.search(mode=mode, blend=blend)
                with mock.patch.object(passage, 'diversify_reranked_results', side_effect=lambda rows, **kwargs: rows):
                    baseline, _ = self.search(mode=mode, blend=blend)
                self.assertEqual(result['results'], baseline['results'])


def reference(rows):
    # Frozen pilot mmr.py policy, direct all-selected formulation (not the optimization).
    relevance = [row.get('rerank_score') for row in rows]
    signatures = [passage.text_shingles(((row.get('matching_ranges') or [{}])[0].get('text') or '')) for row in rows]
    if not rows:
        return []
    selected = [0]
    remaining = list(range(1, len(rows)))
    while remaining:
        chosen = max(remaining, key=lambda i: (.9 * relevance[i] - .1 * max(
            passage.shingle_similarity(signatures[i], signatures[j]) for j in selected), -i))
        selected.append(chosen)
        remaining.remove(chosen)
    return [rows[i] for i in selected]


def ranked(index, text, score=.95):
    return dict(source_id='source:' + str(index % 3), logical_document_id=str(index),
                rerank_score=score, matching_ranges=[dict(passage_id='p' + str(index), text=text,
                rerank_score=score, receipts=['receipt:' + str(index)], spans=[{'record_ordinal': index}])])


class PermutationTests(unittest.TestCase):
    def test_running_max_matches_frozen_reference_with_ties_and_empty_signatures(self):
        randomizer = random.Random(57)
        texts = ['short', '', 'same five words recur here with extra context',
                 'same five words recur here with different context',
                 'incident 123 status open reached 90 percent capacity',
                 'incident 456 status closed reached 99 percent capacity']
        for count in (0, 1, 2, 6, 16, 32, 75):
            rows = [ranked(i, 'same fully recurring incident alert repeated here' if i < 5 else randomizer.choice(texts), randomizer.choice([.95, .94, .9, .8])) for i in range(count)]
            before = copy.deepcopy(rows)
            output = passage.diversify_reranked_results(rows)
            self.assertEqual(list(map(id, output)), list(map(id, reference(rows) if count >= 5 else rows)))
            self.assertEqual(sorted(map(id, output)), sorted(map(id, rows)))
            self.assertEqual(rows, before)
            if rows:
                self.assertIs(output[0], rows[0])

    def test_every_pair_must_be_similar_not_only_first_anchor(self):
        words = [f'word{i}' for i in range(24)]
        texts = [' '.join(words), ' '.join(words[:18]), ' '.join(words[6:])]
        rows = [ranked(i, text, .95 - i * .01) for i, text in enumerate(texts)]
        rows.append(ranked(3, 'unrelated complementary finding with distinct words', .92))
        self.assertAlmostEqual(passage.shingle_similarity(passage.text_shingles(texts[0]), passage.text_shingles(texts[1])), .7)
        self.assertAlmostEqual(passage.shingle_similarity(passage.text_shingles(texts[1]), passage.text_shingles(texts[2])), .4)
        self.assertIs(passage.diversify_reranked_results(rows, limit=3), rows)

    def test_caller_limit_defines_full_crowded_set_and_short_sets_stay(self):
        rows = [ranked(i, 'same repeated incident alert reached quota again') for i in range(3)]
        rows.extend([ranked(3, 'independent report explains the actual cause', .93),
                     ranked(4, 'different verified recommendation from another source', .92)])
        self.assertEqual([r['logical_document_id'] for r in passage.diversify_reranked_results(rows, limit=3)][:2], ['0', '3'])
        self.assertIs(passage.diversify_reranked_results(rows, limit=5), rows)
        self.assertIs(passage.diversify_reranked_results(rows, limit=6), rows)
        self.assertIs(passage.diversify_reranked_results(rows, limit=1), rows)
        empty = [ranked(i, '') for i in range(6)]
        self.assertIs(passage.diversify_reranked_results(empty), empty)

    def test_all_ranges_and_receipts_stay_on_their_original_parent(self):
        rows = [ranked(i, 'same five words repeated here') for i in range(6)]
        rows[-1]['matching_ranges'].append(dict(passage_id='secondary', text='other evidence',
                                               receipts=['second-source-receipt'], rerank_score=.2))
        before = copy.deepcopy(rows)
        output = passage.diversify_reranked_results(rows)
        self.assertEqual(rows, before)
        self.assertEqual(sorted(map(id, rows)), sorted(map(id, output)))
        self.assertIs(next(r for r in output if r['logical_document_id']=='5'), rows[-1])

    def test_unscored_nonfinite_and_boolean_parent_or_secondary_range_returns_same_baseline(self):
        def eligible():
            rows = [ranked(i, 'same complete recurring incident evidence repeated here') for i in range(5)]
            rows.append(ranked(5, 'independent explanation identifies another verified cause', .93))
            return rows
        healthy = eligible()
        self.assertIs(passage.diversify_reranked_results(healthy)[1], healthy[5])
        for value in (None, math.nan, math.inf, -math.inf, True):
            for parent in (True, False):
                with self.subTest(value=value, parent=parent):
                    rows = eligible()
                    if parent:
                        rows[1]['rerank_score'] = value
                    else:
                        rows[1]['matching_ranges'].append(dict(text='secondary', rerank_score=value, receipts=['secondary']))
                    self.assertIs(passage.diversify_reranked_results(rows), rows)
        rows = eligible()
        rows[1]['matching_ranges'] = []
        self.assertIs(passage.diversify_reranked_results(rows), rows)

    def test_deadline_before_or_during_selection_returns_unmodified_baseline(self):
        rows = [ranked(i, 'one two three four five six') for i in range(5)]
        before = copy.deepcopy(rows)
        for clock in ([2], [0] * 6 + [2]):
            with mock.patch.object(passage.time, 'monotonic', side_effect=iter(clock)):
                self.assertIs(passage.diversify_reranked_results(rows, deadline_at=1), rows)
        self.assertEqual(rows, before)

    def test_crowding_check_adds_only_topset_pairs_to_running_max(self):
        rows = [ranked(i, 'same incident event has reached capacity with status open') for i in range(10)]
        with mock.patch.object(passage, 'shingle_similarity', wraps=passage.shingle_similarity) as similarity:
            output = passage.diversify_reranked_results(rows)
        self.assertEqual(similarity.call_count, 55)
        self.assertEqual(list(map(id, output)), list(map(id, reference(rows))))


if __name__ == '__main__':
    unittest.main()
