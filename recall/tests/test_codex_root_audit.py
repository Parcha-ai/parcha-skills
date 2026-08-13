from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.audit_codex_roots import audit_roots


SESSION_ID = "019f1111-2222-7333-8444-555555555555"


def _rollout(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "type": "session_meta",
            "payload": {"id": SESSION_ID},
        })
        + "\n"
        + json.dumps({"type": "event_msg", "payload": {"marker": marker}})
        + "\n"
    )


class CodexRootAuditTest(unittest.TestCase):
    def test_content_free_duplicate_classification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            name = f"rollout-2026-08-10T00-00-00-{SESSION_ID}.jsonl"
            active = root / "sessions"
            archived = root / "archived_sessions"
            _rollout(active / name, "same")
            _rollout(archived / name, "same")

            result = audit_roots(
                source_id="codex:synthetic",
                active_root=active,
                archive_root=archived,
                parquet_queue_depth=3,
            )

            self.assertEqual(result["stable_identity_coverage"], 1.0)
            self.assertEqual(result["duplicates"]["session_ids"], 1)
            self.assertEqual(result["duplicates"]["byte_identical"], 1)
            self.assertEqual(result["duplicates"]["divergent"], 0)
            self.assertEqual(result["parquet_queue_depth"], 3)
            rendered = json.dumps(result)
            self.assertNotIn(SESSION_ID, rendered)
            self.assertNotIn(str(active), rendered)

    def test_divergent_duplicate_fails_inventory_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            name = f"rollout-2026-08-10T00-00-00-{SESSION_ID}.jsonl"
            active = root / "sessions"
            archived = root / "archived_sessions"
            _rollout(active / name, "active")
            _rollout(archived / name, "archived")

            result = audit_roots(
                source_id="codex:synthetic",
                active_root=active,
                archive_root=archived,
            )

            self.assertEqual(result["duplicates"]["byte_identical"], 0)
            self.assertEqual(result["duplicates"]["divergent"], 1)


if __name__ == "__main__":
    unittest.main()
