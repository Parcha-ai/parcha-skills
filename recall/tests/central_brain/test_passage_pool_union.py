import copy
import unittest

from recall_server.passage_retrieval import merge_dense_pools


class PassagePoolUnionTests(unittest.TestCase):
    def row(self, parent, passage, score, source="source-a"):
        return dict(source_id=source, logical_document_id=parent,
                    passage_id=passage, score=score)

    def test_clause_keeps_distinct_passages_from_an_existing_parent(self):
        primary = [self.row("a", "earlier", .9), self.row("b", "other", .8)]
        clause = [self.row("a", "correction", .4), self.row("a", "detail", .3)]
        merged, added = merge_dense_pools(primary, clause, align_by_rank=True)
        self.assertEqual([row["passage_id"] for row in merged],
                         ["earlier", "correction", "other", "detail"])
        self.assertEqual([row["score"] for row in merged], [.9, .9, .8, .8])
        self.assertEqual(added, 2)

    def test_same_passage_uses_aligned_support_not_raw_clause_score(self):
        primary = [self.row("a", "head", .9), self.row("b", "middle", .8),
                   self.row("c", "target", .1)]
        primary[-1]["receipts"] = ["current-primary-pointer"]
        clause = [self.row("x", "new", 100), self.row("c", "target", 90)]
        clause[-1]["receipts"] = ["secondary-hint"]
        original = copy.deepcopy((primary, clause))
        merged, added = merge_dense_pools(primary, clause, align_by_rank=True)
        target = next(row for row in merged if row["passage_id"] == "target")
        self.assertEqual(target["score"], .8)
        self.assertEqual(target["receipts"], ["current-primary-pointer"])
        self.assertEqual(added, 1)
        self.assertEqual((primary, clause), original)

    def test_window_scores_are_comparable_and_do_not_accumulate(self):
        primary = [self.row("a", "one", .5), self.row("a", "two", .4)]
        window = [self.row("a", "two", .8), self.row("a", "two", .7),
                  self.row("a", "one", .3)]
        merged, added = merge_dense_pools(primary, window)
        self.assertEqual([(row["passage_id"], row["score"]) for row in merged],
                         [("two", .8), ("one", .5)])
        self.assertEqual(added, 0)

    def test_empty_primary_deduplicates_passage_identity_and_keeps_raw_scores(self):
        secondary = [self.row("a", "one", .4), self.row("a", "two", .3),
                     self.row("a", "one", .2)]
        merged, added = merge_dense_pools([], secondary, align_by_rank=True)
        self.assertEqual([(row["passage_id"], row["score"]) for row in merged],
                         [("one", .4), ("two", .3)])
        self.assertEqual(added, 2)

    def test_passage_identity_does_not_cross_source_or_parent(self):
        primary = [self.row("a", "same", .9)]
        secondary = [self.row("a", "same", .8, source="source-b"),
                     self.row("b", "same", .7)]
        merged, added = merge_dense_pools(primary, secondary)
        self.assertEqual(len(merged), 3)
        self.assertEqual(added, 2)

    def test_legacy_document_rows_keep_primary_score_and_metadata(self):
        primary = [dict(logical_document_id="a", score=.9),
                   dict(logical_document_id="b", score=.1)]
        secondary = [dict(logical_document_id="b", score=100),
                     dict(logical_document_id="c", score=90)]
        merged, added = merge_dense_pools(primary, secondary, align_by_rank=True)
        self.assertEqual(merged, [primary[0], primary[1],
                                 dict(logical_document_id="c", score=.1)])
        self.assertEqual(added, 1)
        self.assertEqual(merge_dense_pools(primary, []), (primary, 0))

    def test_corrected_union_keeps_existing_collapse_capacity_rule(self):
        from recall_server.passage_retrieval import collapse_document_candidates
        from tests.central_brain.test_passage_fusion import candidate
        primary = [candidate("a", "dense", .9), candidate("b", "dense", .8)]
        correction = {**primary[0], "passage_id": "correction", "score": .4}
        merged, _ = merge_dense_pools(primary, [correction], align_by_rank=True)
        results = collapse_document_candidates((("dense", 1., merged),),
                                               limit=2, passage_budget="admitted")
        selected = [item["passage_id"] for row in results
                    for item in row["matching_ranges"]]
        self.assertEqual(set(selected), {primary[0]["passage_id"],
                                         primary[1]["passage_id"], "correction"})
        self.assertEqual(len(selected), 3)
        survivors = [row for row in merged if row["passage_id"] != "correction"]
        rebuilt = collapse_document_candidates((("dense", 1., survivors),),
                                               limit=2, passage_budget=3)
        self.assertNotIn("correction", [item["passage_id"] for row in rebuilt
                                        for item in row["matching_ranges"]])


if __name__ == "__main__":
    unittest.main()
