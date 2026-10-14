"""Explicit report delivery must survive the existing passage/receipt contract."""

from __future__ import annotations

import unittest

import orjson

from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    PassagePolicy,
    build_passages,
    reconstruct_passage,
    visible_message_text,
    visible_messages,
)


def envelope(report="Verified résumé: two checks passed. No deployment."):
    return {
        "type": "assistant",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_report",
                    "name": "SubagentHandback",
                    "input": {"message": report},
                }
            ],
        },
        "wireToolInputs": {"toolu_report": {"message": report}},
    }


def encoded(value):
    return orjson.dumps(value).decode()


class HandbackProjection(unittest.TestCase):
    def test_exact_explicit_report_without_envelope_or_mirrored_wire_input(self):
        report = "  Verified résumé: two checks passed.\nNo deployment.  "
        value = envelope(report)
        value["message"]["content"].insert(
            0, {"type": "thinking", "thinking": "private deliberation"}
        )
        value["cwd"] = "/private/workspace"
        self.assertEqual(visible_message_text(encoded(value)), report)

    def test_existing_visible_text_wins_without_appending_report(self):
        value = envelope("new tool report must not shift old passage offsets")
        value["message"]["content"].insert(
            0, {"type": "text", "text": "original visible answer"}
        )
        self.assertEqual(
            visible_message_text(encoded(value)), "original visible answer"
        )

    def test_only_exact_named_report_schema_is_visible(self):
        variants = []
        for name in ("Bash", "Read", "StructuredOutput", "SubagentHandbackExtra"):
            value = envelope()
            value["message"]["content"][0]["name"] = name
            variants.append(value)
        for data in (
            {},
            {"message": {}},
            {"message": ["not a string"]},
            {"message": "   "},
            {"message": "report", "token": "private"},
        ):
            value = envelope()
            value["message"]["content"][0]["input"] = data
            variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                self.assertIsNone(visible_message_text(encoded(value)))

    def test_user_tool_result_wire_mirror_and_hidden_field_are_not_reports(self):
        variants = []
        value = envelope()
        value["message"]["role"] = "user"
        variants.append(value)
        value = envelope()
        value["type"] = "user"
        variants.append(value)
        value = envelope()
        value["message"]["content"][0]["type"] = "tool_result"
        variants.append(value)
        value = envelope()
        value["message"]["content"] = []
        variants.append(value)
        value = {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "SubagentHandback report"}
                ],
            },
        }
        variants.append(value)
        for value in variants:
            with self.subTest(value=value):
                self.assertIsNone(visible_message_text(encoded(value)))

    def test_segmented_report_retains_receipts_record_scope_and_utf8_spans(self):
        text = encoded(
            envelope(
                "Verified café and 🧠 findings. Exact source preserved; deployment untested."
            )
        )
        receipt = "recall://claude:test/native:report?rev=3#item=0"
        cut = len(text) // 2
        records = tuple(
            LogicalEvidenceRecord(
                ordinal=i,
                event_native_id="native:report",
                event_kind="transcript_record",
                occurred_at="2026-10-13T12:00:00Z",
                roles=("assistant",),
                receipts=(receipt,) if i == 0 else (),
                segment_ordinal=i,
                segment_count=2,
                text=part,
            )
            for i, part in enumerate((text[:cut], text[cut:]))
        )
        messages = visible_messages(records)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].record_ordinal, 0)
        self.assertEqual(messages[0].record_count, 2)
        self.assertEqual(messages[0].receipts, (receipt,))
        passages = build_passages(
            tenant_id="tenant:test",
            source_id="claude:test",
            logical_document_id="ldoc_" + "a" * 32,
            revision=3,
            messages=messages,
            policy=PassagePolicy(target_tokens=5, overlap_tokens=1),
        )
        self.assertTrue(passages)
        for passage in passages:
            self.assertEqual(reconstruct_passage(passage, messages), passage.text)
            self.assertEqual(passage.receipts, (receipt,))
            self.assertTrue(
                all(
                    s.record_ordinal == 0 and s.record_count == 2 for s in passage.spans
                )
            )


if __name__ == "__main__":
    unittest.main()
