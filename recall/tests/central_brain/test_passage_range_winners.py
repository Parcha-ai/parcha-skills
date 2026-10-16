"""Preserve each arm's best passage before merging duplicate identities."""
from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'server'))
from recall_server.passage_retrieval import collapse_document_candidates


def row(key, score):
    return dict(source_id='source:test', logical_document_id='ldoc_'+'a'*32,
                revision=1, native_parent_id='session:synthetic',
                first_occurred_at='2026-01-01', last_occurred_at='2026-01-01',
                manifest_object_key='objects/synthetic', manifest_content_sha256='b'*64,
                passage_id=key, passage_ordinal=0, spans=[{'record_ordinal': 0}],
                receipts=[f'recall://source:test/{key}?rev=1#item=0'],
                text_redacted=f'synthetic {key}', score=score)


def collapse(dense=(), lexical=(), sparse=()):
    legs = tuple((kind, weight, [row(key, score) for key, score in values])
                 for kind, weight, values in (
                     ('dense', .65, dense), ('passage-lexical', .1, lexical),
                     ('sparse-exact', .25, sparse)))
    return collapse_document_candidates(legs, limit=30, fusion='convex',
        alphas={'dense': .65, 'passage-lexical': .1, 'sparse-exact': .25},
        nominate_per_arm=5)


class PassageRangeWinnersTests(unittest.TestCase):
    def test_lexical_score_does_not_hide_the_dense_arm_winner(self):
        document = collapse([('A', .99), ('D', .5)], [('C', 10), ('A', 9)], [('S', 20)])[0]
        ranges = {item['passage_id']: item for item in document['matching_ranges']}
        self.assertEqual(set(ranges), {'A', 'C', 'S'})
        self.assertEqual(len(document['matching_ranges']), 3)
        self.assertEqual((ranges['A']['kind'], ranges['A']['score']), ('dense', .99))
        self.assertEqual((ranges['C']['kind'], ranges['C']['score']), ('passage-lexical', 10))
        self.assertEqual((ranges['S']['kind'], ranges['S']['score']), ('sparse-exact', 20))
        self.assertEqual(ranges['A']['receipts'], ['recall://source:test/A?rev=1#item=0'])
        self.assertEqual(document['rank'], 1.0)
        self.assertEqual(document['arm_scores']['dense']['score'], .99)
        self.assertFalse(any(key.startswith('_') for key in document))

    def test_duplicate_arm_winners_take_one_slot_then_existing_fill_runs(self):
        document = collapse([('A', .9), ('D', .8)], [('A', 10), ('L', 9)], [('A', 20), ('S', 19)])[0]
        ranges = document['matching_ranges']
        self.assertEqual({item['passage_id'] for item in ranges}, {'A', 'L', 'S'})
        self.assertEqual(len(ranges), 3)
        winner = next(item for item in ranges if item['passage_id'] == 'A')
        self.assertEqual((winner['kind'], winner['score']), ('dense', .9))

    def test_fewer_than_three_distinct_passages_stays_small(self):
        document = collapse([('A', .9)], [('A', 10)], [('A', 20)])[0]
        self.assertEqual(len(document['matching_ranges']), 1)
        self.assertEqual(document['matching_ranges'][0]['passage_id'], 'A')
        self.assertEqual(document['matching_ranges'][0]['kind'], 'dense')
        self.assertEqual(collapse(), [])

    def test_single_arm_fills_three_without_changing_tie_order_or_inputs(self):
        legs = (('dense', 1., [row(key, .9) for key in 'ABCD']),)
        before = copy.deepcopy(legs)
        first = collapse_document_candidates(legs, limit=30)
        second = collapse_document_candidates(legs, limit=30)
        self.assertEqual(first, second)
        self.assertEqual(legs, before)
        self.assertEqual([item['passage_id'] for item in first[0]['matching_ranges']], ['A', 'B', 'C'])

    def test_repeated_passage_identity_uses_best_score_within_its_arm(self):
        document = collapse([('A', .1), ('A', .9), ('D', .8)], [('C', 10)], [('S', 20)])[0]
        ranges = document['matching_ranges']
        self.assertEqual({item['passage_id'] for item in ranges}, {'A', 'C', 'S'})
        self.assertEqual(len(ranges), 3)
        winner = next(item for item in ranges if item['passage_id'] == 'A')
        self.assertEqual((winner['kind'], winner['score']), ('dense', .9))

    def test_arm_priority_is_stable_when_shared_winner_leg_order_changes(self):
        legs = (('passage-lexical', .1, [row('A', 10)]), ('dense', .65, [row('A', .9)]))
        result = collapse_document_candidates(legs, limit=30)[0]['matching_ranges']
        self.assertEqual(len(result), 1)
        self.assertEqual((result[0]['kind'], result[0]['score']), ('dense', .9))


if __name__ == '__main__':
    unittest.main()
