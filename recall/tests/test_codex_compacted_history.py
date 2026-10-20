"""Captured visible history keeps its native message and receipt coordinates."""
from dataclasses import replace
import json
from types import SimpleNamespace
import unittest

from recall_server.logical_evidence import LogicalEvidenceRecord
from recall_server.passage_projection import (
    PassagePolicy, build_passages, iter_visible_messages, reconstruct_passage,
)


from test_codex_history_source_units import SOURCE, RECEIPT, history_text, message


def history_record():
    return LogicalEvidenceRecord(
        ordinal=0, event_native_id="compacted-record", event_kind="transcript_record",
        occurred_at="2026-01-02T03:04:05Z", roles=(), receipts=(RECEIPT,),
        segment_ordinal=0, segment_count=1, text=history_text(),
    )


class CodexCompactedHistoryTests(unittest.TestCase):
    def test_overlapping_history_chain_checkpoint_reads_are_linear(self):
        from collections.abc import Sequence
        from recall_server.passage_append import make_checkpoint

        records = []
        for index in range(40):
            value = {"type": "compacted", "payload": {"replacement_history": [
                message("user", f"Note {index}.", message_id=f"message-{index}",
                        turn_id=f"turn-{index}"),
            ]}}
            records.append(replace(
                history_record(), ordinal=index, event_native_id=f"history-{index}",
                receipts=(f"recall://{SOURCE}/history-{index}?rev=1#item=0",),
                text=json.dumps(value),
            ))
        messages = tuple(iter_visible_messages(records))
        policy = PassagePolicy(4, 3)
        built = build_passages(
            tenant_id="tenant:history", source_id=SOURCE,
            logical_document_id="ldoc_" + "a" * 32, revision=1,
            messages=messages, policy=policy,
        )

        class Counted(Sequence):
            reads = 0

            def __len__(self):
                return len(built)

            def __getitem__(self, index):
                value = built[index]
                self.reads += len(value) if isinstance(index, slice) else 1
                return value

        counted = Counted()
        candidate = SimpleNamespace(
            source_id=SOURCE, revision=1, source_document_sha256="a" * 64,
            manifest_reference={"content_sha256": "b" * 64},
        )
        checkpoint = make_checkpoint(candidate, {"parts": [{"ordinal": 0}]},
                                     messages, counted, policy)
        self.assertEqual(checkpoint["record_ordinal"], 0)
        self.assertLessEqual(counted.reads, 2 * len(built))

    def test_copied_history_origin_matches_native_message_not_equal_text(self):
        from recall_server.passage_projection import _message_origin

        copied = tuple(iter_visible_messages((history_record(),)))
        for item in copied:
            origin = item.origin
            ordinary = {"type": "response_item", "payload": message(
                origin["role"], item.text, message_id=origin["message_id"],
                turn_id=origin["turn_id"],
            )}
            self.assertEqual(origin, _message_origin(json.dumps(ordinary), item.text))
            for field, value in (("id", "independent-message"), ("role", "assistant" if
                                 origin["role"] == "user" else "user")):
                changed = json.loads(json.dumps(ordinary))
                changed["payload"][field] = value
                self.assertNotEqual(origin, _message_origin(json.dumps(changed), item.text))
            ordinary["payload"]["internal_chat_message_metadata_passthrough"]["turn_id"] = "other-turn"
            self.assertNotEqual(origin, _message_origin(json.dumps(ordinary), item.text))

        envelope = json.loads(history_text())
        first = envelope["payload"]["replacement_history"][0]
        first.pop("id")
        duplicate = json.loads(json.dumps(first))
        envelope["payload"]["replacement_history"].append(duplicate)
        actual = tuple(iter_visible_messages((replace(
            history_record(), text=json.dumps(envelope),
        ),)))
        self.assertEqual(len(actual), 3)
        self.assertIsNone(actual[0].origin)
        self.assertIsNone(actual[2].origin)
        self.assertEqual(actual[0].text, actual[2].text)

    def test_native_record_restart_matches_full_append_projection(self):
        from recall_server.passage_append import make_checkpoint, resume_passages

        old_records = (history_record(),)
        old_messages = tuple(iter_visible_messages(old_records))
        arguments = dict(tenant_id="tenant:history", source_id=SOURCE,
                         logical_document_id="ldoc_" + "a" * 32,
                         revision=1, policy=PassagePolicy(4, 1))
        prior = build_passages(messages=old_messages, **arguments)
        candidate = SimpleNamespace(
            source_id=SOURCE, revision=1, source_document_sha256="a" * 64,
            manifest_reference={"content_sha256": "b" * 64},
        )
        checkpoint = make_checkpoint(candidate, {"parts": [{"ordinal": 0}]},
                                     old_messages, prior, arguments["policy"])
        appended = replace(
            history_record(), ordinal=1, event_native_id="next-record",
            event_kind="codex.response_item", roles=("user",),
            receipts=(f"recall://{SOURCE}/next-record?rev=1#item=0",),
            text=json.dumps({"type": "response_item", "payload": message(
                "user", "Now open the other door.", message_id="new-message", turn_id="new-turn",
            )}),
        )
        records = old_records + (appended,)
        # The native projector seeks by record ordinal, not message index.
        tail = tuple(iter_visible_messages(
            (record for record in records if record.ordinal >= checkpoint["record_ordinal"]),
            first_record_ordinal=checkpoint["record_ordinal"],
        ))
        after = dict(arguments, revision=2)
        actual = tuple(replace(passage, revision=2) for passage in prior[:checkpoint["passage_ordinal"]])
        actual += resume_passages(tail, checkpoint, after)
        expected = build_passages(messages=tuple(iter_visible_messages(records)), **after)
        self.assertEqual(actual, expected)


    def test_visible_history_messages_keep_separate_roles_and_original_receipts(self):
        record = history_record()
        before = record.text
        messages = tuple(iter_visible_messages((record,)))
        self.assertEqual(
            [(item.roles, item.text) for item in messages],
            [(("user",), "Keep the café window closed."),
             (("assistant",), "The window remains closed. 🪟")],
        )
        self.assertEqual(
            [item.source_unit for item in messages],
            ["codex.history-message.v1#/payload/replacement_history/0",
             "codex.history-message.v1#/payload/replacement_history/2"],
        )
        for item in messages:
            self.assertEqual(item.receipts, (RECEIPT,))
            self.assertEqual((item.record_ordinal, item.record_count), (0, 1))
        self.assertEqual(record.text, before)

    def test_passages_reconstruct_separate_history_coordinates(self):
        messages = tuple(iter_visible_messages((history_record(),)))
        passages = build_passages(
            tenant_id="tenant:history", source_id=SOURCE,
            logical_document_id="ldoc_" + "a" * 32, revision=1,
            messages=messages, policy=PassagePolicy(100, 10),
        )
        self.assertEqual(len(passages), 1)
        self.assertEqual(passages[0].text,
                         "Keep the café window closed.\nThe window remains closed. 🪟")
        self.assertEqual(reconstruct_passage(passages[0], messages), passages[0].text)
        self.assertEqual(
            [span.source_unit for span in passages[0].spans],
            [item.source_unit for item in messages],
        )


if __name__ == "__main__":
    unittest.main()
