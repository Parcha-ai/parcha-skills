"""Offline counterexamples for reallocating the existing passage budget."""
import copy
import sys
import unittest
import time
import hashlib
import json
import os
import random
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.passage_retrieval import collapse_document_candidates, apply_rerank_scores


def row(parent, passage, score):
    return dict(source_id='source:test', logical_document_id=parent, revision=1,
        native_parent_id=parent, first_occurred_at='2026-08-01T00:00:00Z',
        last_occurred_at='2026-08-01T00:10:00Z', manifest_object_key='object',
        manifest_content_sha256='a' * 64, text_redacted=passage, score=score,
        passage_id=passage, passage_ordinal=0, spans=[], receipts=[f'receipt:{passage}'])


def fixture():
    # Agreement on an answer passage loses to three independent arm tops.
    return (
        ('dense', .15, [row('a', 'dense-top', 1), row('a', 'answer', .9), row('b', 'b', .1)]),
        ('passage-lexical', .30, [row('a', 'lexical-top', 20), row('a', 'answer', 19)]),
        ('sparse-exact', .55, [row('a', 'identifier-top', 100)]),
    )


def keys(results):
    return [r['passage_id'] for d in results for r in d['matching_ranges']]


def experimental(legs, *, limit):
    return collapse_document_candidates(legs, limit=limit, passage_budget='admitted')


def parity_pools():
    for seed in range(128):
        rng = random.Random(seed)
        legs = []
        for name, weight in (('dense', .15), ('passage-lexical', .30), ('sparse-exact', .55)):
            rows = [row(f'parent-{rng.randrange(15)}', f'p-{i}', rng.random())
                    for i in range(rng.randrange(1, 80))]
            # Shared identities across arms have consistent parent ownership.
            for item in rows:
                item['logical_document_id'] = f"parent-{int(item['passage_id'][2:]) % 15}"
            legs.append((name, weight, rows))
        yield tuple(legs), dict(limit=1 + seed % 14, nominate_per_arm=seed % 6)


class PassageBudgetTests(unittest.TestCase):
    def test_frozen_128_pool_legacy_and_measured_candidate_parity(self):
        legacy, balanced = [], []
        for legs, options in parity_pools():
            for mode in ('rrf', 'convex'):
                legacy.append(collapse_document_candidates(legs, fusion=mode, **options))
            balanced.append(collapse_document_candidates(legs, passage_budget='admitted', **options))
        digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest(legacy), '9a7aa9208a6b08490cc3c1b6bccb05892b7f89073e6a1895a0d3e32d8bc24360')
        self.assertEqual(digest(balanced), 'd1258590e1fb3904220b842c65d44fd0e40ea2717019256d1b6fa59fb45978af')

    def test_actual_search_selects_once_and_rollback_preserves_legacy(self):
        from recall_server import passage_retrieval as native
        from tests.central_brain.test_canonical_retrieval import RerankWiringTests
        owner = RerankWiringTests()
        outputs = {}
        for setting in ('1', '0'):
            store = owner._Store()
            store.fusion_mode = 'rrf'
            store.rerank_runtime = owner._FakeRerank({'answer': 1})
            retrieval = owner._retrieval(store)
            legs = fixture()
            retrieval._dense_candidates = lambda *a, **k: (legs[0][2], 'ok', 'synthetic', 3)
            retrieval._lexical_candidates = lambda *a, **k: (legs[1][2], 'ok')
            retrieval._sparse_candidates = lambda *a, **k: (legs[2][2], 'ok')
            with mock.patch.dict(os.environ, {'RECALL_SEARCH_BALANCED_PASSAGES': setting}), \
                    mock.patch.object(native, 'collapse_document_candidates', wraps=collapse_document_candidates) as collapse:
                outputs[setting] = owner._search(retrieval)['results']
            self.assertEqual(collapse.call_count, 1)
            self.assertEqual(len(store.rerank_runtime.calls), 1)
        self.assertIn('answer', keys(outputs['1']))
        self.assertNotIn('answer', keys(outputs['0']))
        # The query allowance can use a fourth range from one parent. Rollback
        # still returns the original four ranges; neither admits another parent.
        self.assertEqual(len(keys(outputs['1'])), 5)
        self.assertEqual(len(keys(outputs['0'])), 4)
        self.assertEqual({d['logical_document_id'] for d in outputs['1']},
                         {d['logical_document_id'] for d in outputs['0']})

    def test_oversized_admitted_pool_preserves_legacy_instead_of_refusing_search(self):
        legs = (('dense', 1.0, [row(f'p-{i // 4}', f'r-{i}', 1 - i / 2000) for i in range(1400)]),)
        self.assertEqual(collapse_document_candidates(legs, limit=350),
                         collapse_document_candidates(legs, limit=350, passage_budget='admitted'))
    def test_agreement_answer_reaches_native_rerank_without_more_inputs(self):
        baseline = collapse_document_candidates(fixture(), limit=30)
        self.assertNotIn('answer', keys(baseline))
        candidate = experimental(fixture(), limit=30)
        self.assertIn('answer', keys(candidate))
        self.assertLessEqual(len(keys(candidate)), len(keys(baseline)))
        scored = apply_rerank_scores(candidate, {'answer': 1.0, 'b': .2})
        self.assertEqual(scored[0]['matching_ranges'][0]['passage_id'], 'answer')

    def test_no_new_parent_or_mutated_raw_rows(self):
        legs = fixture()
        saved = copy.deepcopy(legs)
        baseline = collapse_document_candidates(legs, limit=1)
        candidate = experimental(legs, limit=1)
        self.assertLessEqual({r['logical_document_id'] for r in candidate},
                             {r['logical_document_id'] for r in baseline})
        self.assertEqual(legs, saved)

    def test_duplicate_in_one_arm_does_not_gain_another_vote(self):
        legs = fixture()
        expected = experimental(legs, limit=30)
        legs[0][2].append(copy.deepcopy(legs[0][2][0]))
        actual = experimental(legs, limit=30)
        self.assertEqual(keys(actual), keys(expected))

    def test_default_is_explicit_legacy_and_ties_are_deterministic(self):
        self.assertEqual(collapse_document_candidates(fixture(), limit=30),
                         collapse_document_candidates(fixture(), limit=30, passage_budget=None))
        first = experimental(fixture(), limit=30)
        self.assertEqual(first, experimental(fixture(), limit=30))
        self.assertTrue(all(not any(k.startswith('_') for k in d) for d in first))

    def test_large_parent_cannot_remove_small_admitted_parent(self):
        large = [row('large', f'large-{i}', 100 - i) for i in range(20)]
        legs = (('dense', 1.0, large + [row('small', 'small-answer', .01)]),
                ('passage-lexical', 1.0, copy.deepcopy(large)))
        baseline = collapse_document_candidates(legs, limit=30)
        candidate = experimental(legs, limit=30)
        self.assertEqual(len(keys(candidate)), len(keys(baseline)))
        self.assertIn('small-answer', keys(candidate))
        self.assertEqual({d['logical_document_id'] for d in candidate}, {'large', 'small'})

    def test_slots_can_move_to_fourth_passage_without_parent_or_cost_expansion(self):
        dense = [row('large', f'large-{i}', 100 - i) for i in range(10)]
        small = [row('small', f'small-{i}', .1 - i / 100) for i in range(3)]
        legs = (('dense', 1.0, dense + small), ('passage-lexical', 1.0, copy.deepcopy(dense)))
        baseline = collapse_document_candidates(legs, limit=30)
        candidate = experimental(legs, limit=30)
        self.assertEqual(len(keys(candidate)), len(keys(baseline)))
        large = next(d for d in candidate if d['logical_document_id'] == 'large')
        self.assertGreater(len(large['matching_ranges']), 3)
        # Scored evidence remains available to the native response fitter.
        output = apply_rerank_scores(candidate, {'large-3': 1, 'small-0': .1})
        self.assertEqual(output[0]['matching_ranges'][0]['passage_id'], 'large-3')
        self.assertEqual(len(keys(output)), len(keys(candidate)))

    def test_native_provider_dispatch_contains_answer_with_unchanged_count(self):
        from tests.central_brain.test_rerank_matched_ranges import MatchedRangeRerankTests, RecordingProvider
        owner = MatchedRangeRerankTests()
        owner.setUp()
        self.addCleanup(owner.doCleanups)
        legs = fixture()
        for _, _, rows in legs:
            for item in rows:
                if item['passage_id'] == 'answer':
                    item['text_redacted'] = 'DIRECT ANSWER with exact source facts'
        provider = RecordingProvider()
        selected = experimental(legs, limit=30)
        output, diagnostic, _, _ = owner.run_rerank(provider, selected, legs)
        self.assertEqual(diagnostic['rerank_status'], 'ok')
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(len(provider.calls[0][0]['documents']), len(keys(selected)))
        self.assertEqual(output[0]['matching_ranges'][0]['passage_id'], 'answer')

    def test_native_authority_still_refuses_forgotten_or_expired_candidate(self):
        from tests.central_brain.test_turbopuffer_forget_fence import ForgetAuthorityTests
        from recall_server.db import SearchDeadlineExceeded
        for kwargs in ({'live': False}, {'foreign': True}, {'error': SearchDeadlineExceeded()}):
            owner = ForgetAuthorityTests()
            retrieval, namespace, legs, _, calls, _ = owner.fixture(**kwargs)
            selected = experimental(legs, limit=30)
            retrieval._hydrate_ranges(selected, legs, deadline_at=time.monotonic() + 5)
            self.assertEqual(selected, [])
            self.assertEqual(legs[0][2], [])
            self.assertEqual(namespace.queries, [])
            self.assertEqual(len(calls), 1)

    def test_authority_rebuild_only_removes_and_keeps_frozen_capacity(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        from tests.central_brain.fake_turbopuffer import FakeTurbopuffer
        from tests.central_brain.test_turbopuffer_retrieval import ArmTests, SETTINGS, TENANT, catalog_passage
        from recall_server.turbopuffer_plane import passage_row
        from recall_server.turbopuffer_retrieval import arm_row
        client = FakeTurbopuffer()
        namespace = client.namespace(SETTINGS.namespace(TENANT))
        canonical = [catalog_passage(i, f'evidence {i}', ldoc='large' if i < 10 else 'small')
                     for i in range(13)]
        vendor = [passage_row(p) for p in canonical]
        namespace.write(upsert_rows=vendor)
        legs = (('dense', .15, [arm_row(p, 100 - i) for i, p in enumerate(vendor)]),
                ('passage-lexical', .30, [arm_row(p, 100 - i) for i, p in enumerate(vendor[:10])]))
        for _, _, rows in legs:
            for candidate in rows:
                candidate['text_redacted'] = ''
        initial = collapse_document_candidates(legs, limit=30)
        capacity = len(keys(initial))
        selected = collapse_document_candidates(legs, limit=30, passage_budget=capacity)
        before = set(keys(selected))
        self.assertEqual(len(before), 6)
        self.assertEqual(len(selected[0]['matching_ranges']), 5)
        retrieval, store = ArmTests()._retrieval(client)
        store.connect = lambda: nullcontext(object())
        requested = []
        def execute(connection, sql, params, deadline):
            requested.extend(params[4])
            return SimpleNamespace(fetchall=lambda: [dict(p, tenant_id=TENANT, chunk_text='')
                for p in canonical if p['passage_id'] in params[4] and p['logical_document_id'] == 'large'])
        store._execute_bounded = execute
        diagnostic = retrieval._hydrate_ranges(selected, legs, deadline_at=time.monotonic() + 5,
            collapse_options={'limit': 30, 'fusion': 'rrf', 'passage_budget': capacity})
        self.assertEqual(diagnostic['authority_rejected'], 1)
        self.assertEqual(set(requested), before)
        self.assertEqual(len(keys(selected)), 5, 'do not reimpose three-range limit after authority')
        self.assertLess(set(keys(selected)), before)
        self.assertEqual(diagnostic['hydrated_passages'], 5)
        self.assertEqual(len(namespace.queries), 1, 'one existing bulk body query')

    def test_invalid_capacity_is_refused(self):
        for capacity in (True, 0, 1001, -1):
            with self.subTest(capacity=capacity), self.assertRaises(ValueError):
                collapse_document_candidates(fixture(), limit=30, passage_budget=capacity)


if __name__ == '__main__':
    unittest.main()
