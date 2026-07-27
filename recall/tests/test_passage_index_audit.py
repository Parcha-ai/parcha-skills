from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "server"))

from evals.passage_index_audit import (  # noqa: E402
    PassageIndexAuditError,
    _choose_sample,
    _span_coverage,
)


class PassageIndexAuditTest(unittest.TestCase):
    def test_span_coverage_accepts_complete_overlapping_ranges(self) -> None:
        document = {
            "tenant_id": "tenant:company:test",
            "source_id": "codex:linux:test",
            "logical_document_id": "ldoc_" + "1" * 32,
            "dense_message_count": 2,
            "dense_message_bytes": 13,
            "passage_count": 2,
        }
        identity = {
            key: document[key]
            for key in ("tenant_id", "source_id", "logical_document_id")
        }
        passages = [
            {
                **identity,
                "spans": [
                    {
                        "record_ordinal": 0,
                        "record_count": 1,
                        "source_byte_start": 0,
                        "source_byte_end": 5,
                    },
                    {
                        "record_ordinal": 1,
                        "record_count": 1,
                        "source_byte_start": 0,
                        "source_byte_end": 4,
                    },
                ],
            },
            {
                **identity,
                "spans": [
                    {
                        "record_ordinal": 1,
                        "record_count": 1,
                        "source_byte_start": 2,
                        "source_byte_end": 8,
                    },
                ],
            },
        ]

        report = _span_coverage([document], passages)

        self.assertEqual(report["uncovered_bytes"], 0)
        self.assertEqual(report["messages"], 2)
        self.assertEqual(report["message_bytes"], 13)

    def test_span_coverage_rejects_a_gap(self) -> None:
        document = {
            "tenant_id": "tenant:company:test",
            "source_id": "codex:linux:test",
            "logical_document_id": "ldoc_" + "1" * 32,
            "dense_message_count": 1,
            "dense_message_bytes": 5,
            "passage_count": 1,
        }
        passage = {
            **{
                key: document[key]
                for key in ("tenant_id", "source_id", "logical_document_id")
            },
            "spans": [{
                "record_ordinal": 0,
                "record_count": 1,
                "source_byte_start": 1,
                "source_byte_end": 5,
            }],
        }

        with self.assertRaisesRegex(
            PassageIndexAuditError,
            "uncovered byte",
        ):
            _span_coverage([document], [passage])

    def test_sample_is_deterministic_and_source_stratified(self) -> None:
        rows = [
            {
                "source_id": source,
                "passage_id": f"psg_{ordinal:032x}",
                "token_count": ordinal + 1,
            }
            for source in ("codex:linux:test", "claude:linux:test")
            for ordinal in range(8)
        ]

        first = _choose_sample(rows, sample_size=8, seed="fixed")
        second = _choose_sample(rows, sample_size=8, seed="fixed")

        self.assertEqual(first, second)
        self.assertEqual(
            {row["source_id"] for row in first},
            {"codex:linux:test", "claude:linux:test"},
        )


if __name__ == "__main__":
    unittest.main()
