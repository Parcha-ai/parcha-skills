from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


RECALL_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RECALL_ROOT))

from collector.collector import Collector


SCRIPT = RECALL_ROOT / "skills/recall/scripts/recall.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
spec = importlib.util.spec_from_file_location("recall_session_export_parity", SCRIPT)
engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)


class SessionExportParityTest(unittest.TestCase):
    def test_local_export_evidence_ids_match_collector_projection_identity(self):
        for harness, fixture in (("claude", "claude_sample.jsonl"), ("codex", "codex_rollout.jsonl")):
            with self.subTest(harness=harness), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / harness
                root.mkdir()
                name = (
                    "session.jsonl"
                    if harness == "claude"
                    else "rollout-fixture-019f57ed-74e4-79f1-b140-504e97b05cbe.jsonl"
                )
                session = root / name
                shutil.copy(FIXTURES / fixture, session)
                source_id = f"{harness}:linux:parity"
                collector = Collector(
                    root=root, harness=harness, source_id=source_id,
                    spool_path=Path(temporary) / "spool.db",
                    endpoint="http://127.0.0.1:1", token="unused",
                )
                collector.scan()
                envelopes = collector.pending_envelopes()
                self.assertTrue(envelopes)

                old = {key: os.environ.get(key) for key in (
                    "RECALL_CLAUDE_ROOT", "RECALL_CODEX_ROOT", "RECALL_SESSION_CURSOR_DB",
                    "RECALL_EXPORT_SOURCE_ID", "RECALL_MODE", "RECALL_URL",
                )}
                os.environ.update({
                    "RECALL_CLAUDE_ROOT": str(root if harness == "claude" else Path(temporary) / "empty-claude"),
                    "RECALL_CODEX_ROOT": str(root if harness == "codex" else Path(temporary) / "empty-codex"),
                    "RECALL_SESSION_CURSOR_DB": str(Path(temporary) / "cursors.db"),
                    "RECALL_EXPORT_SOURCE_ID": source_id,
                    "RECALL_MODE": "local",
                })
                os.environ.pop("RECALL_URL", None)
                try:
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        self.assertEqual(engine.main(["session-export", "--target", str(session)]), 0)
                    page = json.loads(output.getvalue())
                finally:
                    for key, value in old.items():
                        if value is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value
                    collector.close()

                parser = engine.claude_record if harness == "claude" else engine.codex_record
                expected = []
                expected_tools = []
                for envelope in envelopes:
                    projected, _metadata = parser(envelope["content"])
                    for ordinal, (_timestamp, _surface, text, entities) in enumerate(projected):
                        expected.append(engine.session_evidence_id(
                            source_id, envelope["native_parent_id"], envelope["native_id"], ordinal, text,
                        )[0])
                        expected_tools.append(sorted(value for kind, value in entities if kind == "tool"))
                self.assertEqual([item["evidence_id"] for item in page["items"]], expected)
                self.assertEqual([
                    sorted(entity["value"] for entity in item.get("entities", []) if entity["kind"] == "tool")
                    for item in page["items"]
                ], expected_tools)
                self.assertEqual(page["session"]["native_session_id"], envelopes[0]["native_parent_id"])


if __name__ == "__main__":
    unittest.main()
