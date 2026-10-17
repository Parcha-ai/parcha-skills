from __future__ import annotations

import sys
import unittest
from typing import ClassVar
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "server"))

from evals.passage_index_audit import (  # noqa: E402
    PassageIndexAuditError,
    _choose_sample,
    _span_coverage,
    _extraction_arguments,
)


class PassageIndexAuditTest(unittest.TestCase):
    def test_codex_applied_version_requires_declared_support(self):
        @dataclass(frozen=True)
        class VersionedCandidate:
            extraction_version: str = 'legacy'

        with patch('evals.passage_index_audit.PassageCandidate', VersionedCandidate):
            with self.assertRaisesRegex(PassageIndexAuditError, 'version-aware projector'):
                _extraction_arguments('codex_tool_groups_v1')

        @dataclass(frozen=True)
        class CodexCandidate(VersionedCandidate):
            supported_extraction_versions: ClassVar[frozenset[str]] = frozenset(
                {'legacy', 'tool_groups_v1', 'codex_tool_groups_v1'})

        with patch('evals.passage_index_audit.PassageCandidate', CodexCandidate):
            self.assertEqual(_extraction_arguments('codex_tool_groups_v1'),
                             {'extraction_version': 'codex_tool_groups_v1'})

    def test_applied_version_requires_explicit_projector_capability(self):
        @dataclass(frozen=True)
        class LegacyCandidate:
            revision: int = 1
        with patch('evals.passage_index_audit.PassageCandidate', LegacyCandidate):
            self.assertEqual(_extraction_arguments('legacy'), {})
            with self.assertRaisesRegex(PassageIndexAuditError, 'version-aware projector'):
                _extraction_arguments('tool_groups_v1')
        @dataclass(frozen=True)
        class VersionedCandidate(LegacyCandidate):
            extraction_version: str = 'legacy'
        with patch('evals.passage_index_audit.PassageCandidate', VersionedCandidate):
            self.assertEqual(_extraction_arguments('tool_groups_v1'),
                             {'extraction_version': 'tool_groups_v1'})
            self.assertEqual(_extraction_arguments('legacy'), {'extraction_version': 'legacy'})
            with self.assertRaisesRegex(PassageIndexAuditError, 'unsupported applied'):
                _extraction_arguments('tool_groups_future')

    def test_distinct_source_units_at_same_record_keep_independent_byte_coverage(self):
        document = dict(tenant_id='tenant:test', source_id='claude:test',
                        logical_document_id='ldoc_' + '1' * 32,
                        dense_message_count=3, dense_message_bytes=12, passage_count=2)
        identity = {k: document[k] for k in ('tenant_id', 'source_id', 'logical_document_id')}
        first = 'claude.tool-input.string.v1#/message/content/1/input/report'
        second = 'claude.tool-input.string.v1#/message/content/1/input/options/0/message'
        spans = [dict(record_ordinal=0, record_count=1, source_byte_start=0,
                      source_byte_end=4, **({'source_unit': unit} if unit else {}))
                 for unit in (None, first, second)]
        passages = [{**identity, 'spans': spans[:1]}, {**identity, 'spans': spans[1:]}]
        self.assertEqual(_span_coverage([document], passages)['message_bytes'], 12)
        # The legacy field and other sibling cannot fill a missing prefix here.
        spans[2]['source_byte_start'] = 1
        with self.assertRaisesRegex(PassageIndexAuditError, 'uncovered byte'):
            _span_coverage([document], passages)
        spans[2]['source_byte_start'] = 0
        spans[2]['source_byte_end'] = 3
        with self.assertRaisesRegex(PassageIndexAuditError, 'disagrees'):
            _span_coverage([document], passages)

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
