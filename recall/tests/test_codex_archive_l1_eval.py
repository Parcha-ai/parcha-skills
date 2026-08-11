from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "eval_codex_archive_l1.py"
SPEC = importlib.util.spec_from_file_location("codex_archive_l1", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CodexArchiveL1EvalTest(unittest.TestCase):
    def test_l1_hard_gates(self) -> None:
        result = MODULE.measure()
        self.assertEqual(result["randomized_moves"], 100)
        self.assertEqual(result["move_identity_invariance"], 1.0)
        self.assertTrue(result["record_content_parity"])
        self.assertEqual(result["move_tombstones"], 0)
        self.assertEqual(result["move_duplicate_records"], 0)
        self.assertLessEqual(result["move_visibility_intervals"], 2)
        self.assertEqual(result["archive_coverage"], 1.0)
        self.assertEqual(result["identity_conflicts"], 0)
        self.assertTrue(result["active_serviced_first"])


if __name__ == "__main__":
    unittest.main()
