"""Append prefix commitments stay exact while replaying parent-sized metadata."""
import tracemalloc
import unittest
from recall_server.passage_append import RetainedPassage, digest, prefix_digest


class PrefixStreamingTests(unittest.TestCase):
    def test_original_digest_exact_for_empty_unicode_escaping_and_order(self):
        cases = [(), (RetainedPassage('id', 0, ()),)]
        for count in range(1, 65):
            cases.append(tuple(RetainedPassage('id-é🙂\\\"\n' + str(i), i,
                tuple('recall://é🙂/\\\"\n' + str(j) for j in range(i % 7)))
                for i in range(count)))
        for values in cases:
            expected = digest([(p.passage_id, p.ordinal, list(p.receipts)) for p in values])
            self.assertEqual(prefix_digest(iter(values)), expected)
            self.assertEqual(prefix_digest(iter(reversed(values))),
                digest([(p.passage_id, p.ordinal, list(p.receipts)) for p in reversed(values)]))

    def test_replayed_receipts_are_not_retained_for_whole_parent(self):
        def replay():
            for i in range(5000):
                yield RetainedPassage('id-' + str(i), i,
                    tuple(f'recall://source/event-{i}/chunk-{j}/' + 'x' * 180 for j in range(32)))
        tracemalloc.start()
        try:
            actual = prefix_digest(replay())
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        self.assertEqual(len(actual), 64)
        self.assertLess(peak, 4 * 1024 * 1024,
                        f'prefix hashing retained parent receipt payloads: {peak} bytes')


if __name__ == '__main__':
    unittest.main()
