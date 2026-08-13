from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from skills.recall.scripts.codex_identity import (
    resolve_codex_session_identity,
    stable_codex_record_key,
)


CORPUS = Path(__file__).parent / "codex_archive_identity_v1" / "corpus.jsonl"


class CodexArchiveIdentityContractTest(unittest.TestCase):
    def test_closed_corpus(self) -> None:
        cases = [json.loads(line) for line in CORPUS.read_text().splitlines()]
        self.assertEqual(len(cases), 9)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for case in cases:
                target = root / case["case_id"] / case["filename"]
                target.parent.mkdir()
                lines = list(case.get("raw_prefix", ()))
                lines.extend(json.dumps(record) for record in case["records"])
                target.write_text("\n".join(lines) + "\n")
                identity = resolve_codex_session_identity(target)
                self.assertEqual(
                    {
                        "status": identity.status,
                        "native_session_id": identity.native_session_id,
                        "basis": identity.basis,
                    },
                    case["expected"],
                    case["case_id"],
                )

    def test_identity_read_is_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "rollout-synthetic.jsonl"
            target.write_text(
                json.dumps({"type": "event_msg", "payload": {"value": "x" * 1024}})
                + "\n"
                + json.dumps({
                    "type": "session_meta",
                    "payload": {"id": "outside-bound"},
                })
                + "\n"
            )
            identity = resolve_codex_session_identity(
                target,
                max_bytes=256,
                max_records=1,
            )
            self.assertEqual(identity.status, "identity_unavailable")

    def test_invalid_bounds_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "rollout-synthetic.jsonl"
            target.write_text("{}\n")
            with self.assertRaisesRegex(ValueError, "bounds"):
                resolve_codex_session_identity(target, max_records=0)

    def test_stable_record_key_is_case_normalized_and_path_independent(self) -> None:
        session_id = "019F1111-2222-7333-8444-555555555555"
        self.assertEqual(
            stable_codex_record_key(session_id),
            stable_codex_record_key(session_id.casefold()),
        )
        with self.assertRaisesRegex(ValueError, "unsafe"):
            stable_codex_record_key(" unsafe ")


if __name__ == "__main__":
    unittest.main()
