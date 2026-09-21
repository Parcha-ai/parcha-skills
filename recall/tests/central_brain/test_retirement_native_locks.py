"""Native write locks retain their namespace and fail closed on incomplete SQL results."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / 'server')]
from recall_server.chunk_retirement import _try_parent_native_locks  # noqa: E402


class RetirementNativeLocksTests(unittest.TestCase):
    def test_sorted_exact_namespace_one_bounded_query(self):
        query = Mock()
        query.return_value.fetchall.return_value = [
            {'ordinality': n, 'locked': True} for n in range(1, 257)
        ]
        rows = [{'native_id': f'event-{n:04}'} for n in reversed(range(256))]
        self.assertTrue(_try_parent_native_locks(query, ('tenant:a', 'source:b', 'parent:c'), rows))
        query.assert_called_once()
        sql, (keys,) = query.call_args.args
        self.assertEqual(keys, sorted(f'v2\x1ftenant:a\x1fsource:b\x1f{row["native_id"]}' for row in rows))
        self.assertIn('hashtextextended(lock_key,0)', sql)
        self.assertIn('ORDER BY ordinality', sql)

    def test_missing_extra_reordered_or_non_boolean_result_refused(self):
        valid = [{'ordinality': 1, 'locked': True}, {'ordinality': 2, 'locked': True}]
        for result in (valid[:1], valid + valid[:1], list(reversed(valid)),
                       [valid[0], {'ordinality': 2, 'locked': False}],
                       [valid[0], {'ordinality': 2, 'locked': 1}],
                       [valid[0], {'ordinality': 2, 'locked': None}]):
            with self.subTest(result=result):
                query = Mock()
                query.return_value.fetchall.return_value = result
                self.assertFalse(_try_parent_native_locks(query, ('tenant:a', 'source:b', 'parent:c'), [{'native_id': 'a'}, {'native_id': 'b'}]))

    def test_empty_completion_does_not_take_unrelated_locks(self):
        query = Mock()
        self.assertTrue(_try_parent_native_locks(query, ('tenant:a', 'source:b', 'parent:c'), []))
        query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
