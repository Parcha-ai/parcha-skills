from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "eval_codex_archive_baseline.py"
SPEC = importlib.util.spec_from_file_location("codex_archive_baseline", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CodexArchiveBaselineEvalTest(unittest.TestCase):
    def test_baseline_detects_single_root_failure_and_multi_root_fix(self) -> None:
        result = MODULE.measure()
        self.assertEqual(result["contract"], "recall.codex-archive-baseline.v1")
        self.assertEqual(result["stable_identity_coverage"], 1)
        self.assertEqual(result["current_single_root_archive_coverage"], 0.0)
        self.assertEqual(result["single_root_move_tombstones"], 2)
        self.assertEqual(result["move_tombstones"], 0)
        self.assertTrue(result["move_parent_identity_invariant"])
        self.assertTrue(result["move_record_identity_invariant"])


if __name__ == "__main__":
    unittest.main()
