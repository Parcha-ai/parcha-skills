"""Parent-admission sensitivity to extra passages from an earlier parent."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.passage_retrieval import collapse_document_candidates


def passage(parent, name, score):
    return dict(source_id='source:synthetic', logical_document_id=parent,
        revision=1, native_parent_id=parent, first_occurred_at='2026-01-01T00:00:00Z',
        last_occurred_at='2026-01-01T00:00:00Z', manifest_object_key='synthetic',
        manifest_content_sha256='a' * 64, text_redacted=name, score=score,
        passage_id=name, passage_ordinal=0, spans=[],
        receipts=[f'recall://source:synthetic/{name}?rev=1#item=0'])


def pool(prefix=1, *, sparse=False):
    # Same distinct-parent best scores/order in both variants. Only the earlier
    # lexical parent has additional equally ranked, distinct passage records.
    return (
        ('dense', .15, [passage('dense-only', 'dense-only-0', .9),
                        passage('shared', 'shared-0', .8)]),
        ('passage-lexical', .30,
         [passage('shared', f'shared-{i}', 30.) for i in range(prefix)] +
         [passage('lexical-only', 'lexical-only-0', 20.)]),
        ('sparse-exact', .55,
         [passage('lexical-only', 'lexical-only-0', 100.)] if sparse else []),
    )


def parent_signature(legs):
    signature = []
    for name, weight, rows in legs:
        scores = {}
        for row in rows:
            key = row['logical_document_id']
            scores[key] = max(scores.get(key, float('-inf')), row['score'])
        signature.append((name, weight, list(scores.items())))
    return signature


def parents(results):
    return [row['logical_document_id'] for row in results]


def select(legs, *, nominations=0, capacity=2):
    return collapse_document_candidates(legs, limit=2, fusion='rrf',
        nominate_per_arm=nominations, passage_budget=capacity,
        preserve_arm_passages=True)


class ParentRrfDuplicatePrefixTests(unittest.TestCase):
    def test_duplicate_prefix_does_not_displace_another_parent_at_fixed_capacity(self):
        original, repeated = pool(), pool(101)
        self.assertEqual(parent_signature(original), parent_signature(repeated))
        before = copy.deepcopy((original, repeated))
        baseline, expanded = select(original), select(repeated)
        self.assertEqual(parents(baseline), ['shared', 'lexical-only'])
        for result in (baseline, expanded):
            self.assertEqual(len(result), 2, 'fixed parent capacity B=2')
            self.assertEqual(sum(len(x['matching_ranges']) for x in result), 2,
                             'fixed passage capacity C=2')
        self.assertEqual((original, repeated), before)
        self.assertEqual(parents(expanded), parents(baseline))

    def test_no_duplicate_fixture_is_stable_and_pure(self):
        legs = pool(); saved = copy.deepcopy(legs)
        first = select(legs)
        self.assertEqual(parents(first), ['shared', 'lexical-only'])
        self.assertEqual(first, select(legs))
        self.assertEqual(legs, saved)

    def test_sparse_agreement_still_protects_the_matching_parent(self):
        original, repeated = pool(sparse=True), pool(101, sparse=True)
        self.assertEqual(parent_signature(original), parent_signature(repeated))
        for legs in (original, repeated):
            result = select(legs)
            self.assertEqual(parents(result), ['lexical-only', 'shared'])
            self.assertEqual(sum(len(x['matching_ranges']) for x in result), 2)

    def test_existing_distinct_parent_nominations_can_protect_tail(self):
        # A nomination breadth of two already covers both lexical parents;
        # duplicate passages must not consume that existing nomination budget.
        for legs in (pool(), pool(101)):
            result = select(legs, nominations=2, capacity=3)
            self.assertEqual(set(parents(result)),
                             {'shared', 'lexical-only', 'dense-only'})
            self.assertEqual(len(result), 3)
            self.assertEqual(sum(len(x['matching_ranges']) for x in result), 3)

    def test_each_arm_parent_normalization_ignores_sibling_prefix_density(self):
        for kind, weight in (('dense', .15), ('passage-lexical', .30),
                             ('sparse-exact', .55)):
            for exact_duplicate in (False, True):
                with self.subTest(kind=kind, exact_duplicate=exact_duplicate):
                    first = passage('earlier', 'earlier-0', 30.)
                    later = passage('later', 'later-0', 20.)
                    original = ((kind, weight, [first, later]),)
                    prefix = ([copy.deepcopy(first) for _ in range(101)]
                              if exact_duplicate else
                              [passage('earlier', f'earlier-{i}', 30.)
                               for i in range(101)])
                    repeated = ((kind, weight, [*prefix, later]),)
                    self.assertEqual(parent_signature(original),
                                     parent_signature(repeated))
                    baseline, expanded = select(original), select(repeated)
                    self.assertEqual(parents(baseline), ['earlier', 'later'])
                    self.assertEqual(parents(expanded), parents(baseline))
                    self.assertEqual([r['rank'] for r in expanded],
                                     [r['rank'] for r in baseline])
                    for before, after in zip(baseline, expanded):
                        self.assertEqual(set(before), set(after))
                        self.assertEqual(set(after['arm_scores']), {kind})
                        self.assertEqual(set(after['arm_scores'][kind]),
                                         {'score', 'rank', 'normalized'})
                        self.assertEqual(before['arm_scores'][kind]['normalized'],
                                         after['arm_scores'][kind]['normalized'])
                        self.assertEqual(before['arm_scores'][kind]['score'],
                                         after['arm_scores'][kind]['score'])
                    # Public provenance continues reporting the actual first
                    # raw passage position, despite distinct-parent scoring.
                    self.assertEqual(baseline[1]['arm_scores'][kind]['rank'], 2)
                    self.assertEqual(expanded[1]['arm_scores'][kind]['rank'], 102)
                    self.assertEqual(sum(len(x['matching_ranges']) for x in expanded), 2)

    def test_exact_duplicate_prefix_preserves_parent_admission(self):
        original = pool()
        repeated = copy.deepcopy(original)
        earlier = copy.deepcopy(repeated[1][2][0])
        repeated[1][2][1:1] = [copy.deepcopy(earlier) for _ in range(100)]
        self.assertEqual(parent_signature(original), parent_signature(repeated))
        self.assertEqual(parents(select(repeated)), parents(select(original)))


if __name__ == '__main__':
    unittest.main()
