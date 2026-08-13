from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "eval_codex_archive_l2.py"
SPEC = importlib.util.spec_from_file_location("codex_archive_l2", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CodexArchiveL2EvalTest(unittest.TestCase):
    def test_l2_hard_gates(self) -> None:
        result = MODULE.measure()
        self.assertEqual(result["randomized_restores"], 100)
        self.assertEqual(result["operational_tombstones"], 200)
        self.assertEqual(result["restored_records"], 200)
        self.assertEqual(result["restore_identity_invariance"], 1.0)
        self.assertEqual(result["restore_duplicate_records"], 0)
        self.assertEqual(result["restore_generation"], 1)
        self.assertGreater(result["crash_resume_records"], 0)
        self.assertEqual(result["archive_backlog_before_ack"], 100)
        self.assertEqual(result["archive_backlog_after_ack"], 0)
        self.assertEqual(result["restore_acked"], 200)
        self.assertEqual(result["restore_noop_records"], 0)
        self.assertEqual(result["restore_noop_tombstones"], 0)
        self.assertEqual(result["identity_conflicts"], 0)


if __name__ == "__main__":
    unittest.main()
