"""Typed report boundaries preserve exact source bytes without changing ordinary views."""

import dataclasses
import json
import unittest

from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    DEFAULT_PASSAGE_POLICY,
    build_passages,
    canonical_spans_json,
    reconstruct_passage,
    visible_messages,
)
from central_brain.test_rerank_matched_ranges import (
    MatchedRangeRerankTests,
    RecordingProvider,
)

KIND = "subagent_handback.complete.v1"


class ReportUnits(unittest.TestCase):
    def fixture(self, report="Result: " + "verified evidence " * 180, ordinary=False):
        texts = ["Earlier context " * 400, report, "Later status " * 40]
        records = []
        for i, text in enumerate(texts):
            blocks = [{"type": "text", "text": text}]
            if i == 1 and not ordinary:
                blocks = [
                    {
                        "type": "tool_use",
                        "name": "SubagentHandback",
                        "input": {"message": text},
                    }
                ]
            records.append(
                LogicalEvidenceRecord(
                    ordinal=i,
                    event_native_id=f"event-{i}",
                    event_kind="transcript_record",
                    occurred_at="2026-10-13T00:00:00Z",
                    roles=("assistant",),
                    receipts=(f"recall://claude:linux:unit/event-{i}?rev=1#item=0",),
                    segment_ordinal=0,
                    segment_count=1,
                    text=json.dumps(
                        {
                            "type": "assistant",
                            "message": {"role": "assistant", "content": blocks},
                        }
                    ),
                )
            )
        messages = visible_messages(records)
        passages = build_passages(
            tenant_id="tenant:unit",
            source_id="claude:linux:unit",
            logical_document_id="ldoc_" + "a" * 32,
            revision=1,
            messages=messages,
            policy=DEFAULT_PASSAGE_POLICY,
        )
        return messages, passages

    def test_complete_report_is_its_own_identity_bound_source_unit(self):
        messages, passages = self.fixture()
        report = [p for p in passages if any(s.record_ordinal == 1 for s in p.spans)]
        self.assertEqual(len(report), 1)
        self.assertEqual(report[0].text, messages[1].text)
        self.assertEqual(report[0].receipts, messages[1].receipts)
        self.assertEqual(
            json.loads(canonical_spans_json(report[0].spans))[0]["source_unit"], KIND
        )
        for p in passages:
            self.assertEqual(reconstruct_passage(p, messages), p.text)

    def test_larger_custom_policy_does_not_mark_above_default_budget(self):
        messages, _ = self.fixture(report="report evidence " * 800)
        passages = build_passages(
            tenant_id="tenant:unit",
            source_id="claude:linux:unit",
            logical_document_id="ldoc_" + "a" * 32,
            revision=1,
            messages=messages,
            policy=dataclasses.replace(DEFAULT_PASSAGE_POLICY, target_tokens=2048),
        )
        report = [p for p in passages if any(s.record_ordinal == 1 for s in p.spans)]
        self.assertEqual(len(report), 1)
        self.assertGreater(report[0].token_count, 1024)
        self.assertIsNone(report[0].spans[0].source_unit)

    def test_ordinary_span_serialization_stays_identical(self):
        _, passages = self.fixture(ordinary=True)
        for p in passages:
            encoded = json.loads(canonical_spans_json(p.spans))
            self.assertTrue(all("source_unit" not in s for s in encoded))
            self.assertTrue(all(s.origin is None for s in p.spans))
            self.assertTrue(all("origin" not in s for s in encoded))
            expected = [
                {k: v for k, v in dataclasses.asdict(s).items() if k not in {"source_unit", "origin"}}
                for s in p.spans
            ]
            self.assertEqual(encoded, expected)

    def test_oversize_report_fragments_are_not_complete_units(self):
        messages, passages = self.fixture(report="λ status " * 5000)
        report = [p for p in passages if any(s.record_ordinal == 1 for s in p.spans)]
        self.assertGreater(len(report), 1)
        for p in report:
            self.assertTrue(all(s.record_ordinal == 1 for s in p.spans))
            self.assertFalse(
                any(
                    s.get("source_unit")
                    for s in json.loads(canonical_spans_json(p.spans))
                )
            )
            self.assertLessEqual(p.token_count, 1024)
            self.assertEqual(reconstruct_passage(p, messages), p.text)


class ReportProvider(MatchedRangeRerankTests):
    def test_complete_wordlike_unit_over_provider_ceiling_keeps_bounded_fallback(self):
        from recall_server.rerank import MAX_DOC_CHARS_CEILING

        _, passages = ReportUnits().fixture(report=("x" * 40 + " ") * 800)
        unit = next(p for p in passages if p.spans[0].source_unit == KIND)
        self.assertLessEqual(unit.token_count, 1024)
        self.assertGreater(len(unit.text), MAX_DOC_CHARS_CEILING)
        pool, legs = self.fixture()
        answer = next(r for r in legs[0][2] if r.get("passage_id") == "answer")
        answer.update(source_unit=unit.spans[0].source_unit, text_redacted=unit.text)
        provider = RecordingProvider()
        self.run_rerank(provider, pool, legs)
        docs = provider.calls[0][0]["documents"]
        self.assertLessEqual(max(map(len, docs)), MAX_DOC_CHARS_CEILING)
        self.assertNotIn(unit.text, docs)
        self.assertIn(unit.text[:2000], docs)

    def test_header_is_included_in_whole_unit_ceiling(self):
        pool, legs = self.fixture()
        answer = next(r for r in legs[0][2] if r.get("passage_id") == "answer")
        answer.update(
            source_unit=KIND,
            text_redacted="x" * 19950,
            header_redacted="source context " * 10,
        )
        provider = RecordingProvider()
        self.run_rerank(provider, pool, legs)
        docs = provider.calls[0][0]["documents"]
        expected = (
            answer["header_redacted"].strip() + "\n\n" + answer["text_redacted"]
        )[:2000]
        self.assertIn(expected, docs)
        self.assertLessEqual(max(map(len, docs)), 2000)

    def test_complete_report_full_ordinary_wire_and_count_unchanged(self):
        pool, legs = self.fixture()
        ordinary = legs[0][2][0]
        ordinary["text_redacted"] = "ordinary " * 500
        answer = next(r for r in legs[0][2] if r.get("passage_id") == "answer")
        answer["source_unit"] = KIND
        provider = RecordingProvider()
        _, diagnostics, _, _ = self.run_rerank(provider, pool, legs)
        body, _ = provider.calls[0]
        self.assertIn(answer["text_redacted"], body["documents"])
        self.assertIn(ordinary["text_redacted"][:2000], body["documents"])
        self.assertEqual(len(body["documents"]), 52)
        self.assertEqual(diagnostics["rerank_candidates"], 52)


if __name__ == "__main__":
    unittest.main()
