"""A retrieved passage survives subpass merging before native admission."""
import copy
import unittest

from recall_server.passage_retrieval import merge_dense_pools, collapse_document_candidates
from tests.central_brain.test_balanced_passage_selection import row


class PassageSubpassIdentity(unittest.TestCase):
    def test_clause_passage_from_existing_parent_reaches_native_admission(self):
        primary = [row('parent', 'overview', .9), row('other', 'other', .8)]
        clause = [row('parent', 'answer', .4)]
        original = copy.deepcopy((primary, clause))
        merged, added = merge_dense_pools(primary, clause, align_by_rank=True)
        admitted = collapse_document_candidates(
            (('dense', .15, merged),), limit=30, nominate_per_arm=5,
            passage_budget='admitted',
        )
        selected = {item['passage_id'] for parent in admitted for item in parent['matching_ranges']}
        self.assertIn('answer', selected)
        self.assertEqual(added, 1)
        self.assertEqual([(item['passage_id'], item['score']) for item in merged],
                         [('overview', .9), ('answer', .9), ('other', .8)])
        self.assertEqual((primary, clause), original)

    def test_repeat_keeps_primary_score_and_metadata_even_if_secondary_is_stronger(self):
        primary = [row('parent', 'overview', .9), row('parent', 'answer', .2)]
        primary[1]['receipts'] = ['primary-receipt']
        secondary = [row('parent', 'answer', 100), row('parent', 'new', 90)]
        secondary[0]['receipts'] = ['secondary-receipt']
        for align in (False, True):
            with self.subTest(align=align):
                original = copy.deepcopy((primary, secondary))
                merged, added = merge_dense_pools(primary, secondary, align_by_rank=align)
                answer = next(item for item in merged if item['passage_id'] == 'answer')
                self.assertIs(answer, primary[1])
                self.assertEqual(answer['score'], .2)
                self.assertEqual(answer['receipts'], ['primary-receipt'])
                self.assertEqual(added, 1)
                self.assertEqual((primary, secondary), original)

    def test_repeated_new_passage_keeps_first_secondary_row_without_score_upgrade(self):
        first = row('parent', 'answer', .2)
        secondary = [first, row('parent', 'answer', .9), row('parent', 'other', .1)]
        merged, added = merge_dense_pools([], secondary, align_by_rank=True)
        self.assertEqual(added, 2)
        self.assertEqual([(item['passage_id'], item['score']) for item in merged],
                         [('answer', .2), ('other', .1)])
        self.assertIs(merged[0], first)

    def test_window_preserves_distinct_source_and_parent_passage_identities(self):
        primary = [row('parent', 'same', .9)]
        other_source = {**row('parent', 'same', .8), 'source_id': 'other-source'}
        secondary = [other_source, row('other-parent', 'same', .7),
                     row('parent', 'distinct', .6)]
        merged, added = merge_dense_pools(primary, secondary)
        self.assertEqual(added, 3)
        self.assertEqual(merged, primary + secondary)

    def test_legacy_document_fallback_and_rank_alignment_are_unchanged(self):
        primary = [dict(logical_document_id='a', score=.9),
                   dict(logical_document_id='b', score=.1)]
        secondary = [dict(logical_document_id='b', score=100),
                     dict(logical_document_id='c', score=90)]
        merged, added = merge_dense_pools(primary, secondary, align_by_rank=True)
        self.assertEqual(merged, primary + [dict(logical_document_id='c', score=.1)])
        self.assertEqual(added, 1)
        self.assertIs(merged[1], primary[1])
        self.assertIs(merge_dense_pools(primary, [secondary[0]])[0], primary)
        self.assertIs(merge_dense_pools(primary, [])[0], primary)


if __name__ == '__main__':
    unittest.main()
