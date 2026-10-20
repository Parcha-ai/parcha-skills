"""Read-compatible visible Codex history source-unit contract."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from contracts.source_units import resolve_codex_history_source_unit

SOURCE = "codex:synthetic:history"
RECEIPT = f"recall://{SOURCE}/compacted-record?rev=1#item=0"


def message(role, text, *, message_id, turn_id):
    return {
        "type": "message",
        "role": role,
        "id": message_id,
        "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
        "content": [{
            "type": "input_text" if role == "user" else "output_text",
            "text": text,
        }],
    }


def history_envelope():
    envelope = {
        "type": "compacted",
        "payload": {
            "message": "",
            "replacement_history": [
                message("user", "Keep the café window closed.",
                        message_id="message-human", turn_id="turn-human"),
                {"type": "message", "role": "developer", "content": [
                    {"type": "input_text", "text": "Private runtime instructions."},
                ]},
                message("assistant", "The window remains closed. 🪟",
                        message_id="message-reply", turn_id="turn-reply"),
                {"type": "compaction", "encrypted_content": "opaque-synthetic-data"},
            ],
        },
    }
    return envelope


def history_text():
    return json.dumps(history_envelope(), ensure_ascii=False)


class CodexHistorySourceUnitTests(unittest.TestCase):
    def test_history_contract_refuses_hidden_metadata_and_arbitrary_coordinates(self):
        unit = "codex.history-message.v1#/payload/replacement_history/0"
        base = json.loads(history_text())
        for change in (
            lambda value: value.update(type="response_item"),
            lambda value: value.update(channel="analysis"),
            lambda value: value.update(role="developer"),
            lambda value: value["payload"].update(channel="analysis"),
            lambda value: value["payload"].update(role="assistant"),
            lambda value: value["payload"]["replacement_history"][0].update(role="developer"),
            lambda value: value["payload"]["replacement_history"][0].update(role="tool"),
            lambda value: value["payload"]["replacement_history"][0].update(type="reasoning"),
            lambda value: value["payload"]["replacement_history"][0].update(channel="analysis"),
            lambda value: value["payload"]["replacement_history"][0]["content"][0].update(type="encrypted_content"),
            lambda value: value["payload"]["replacement_history"][0]["content"][0].update(channel="analysis"),
            lambda value: value["payload"]["replacement_history"][0][
                "internal_chat_message_metadata_passthrough"].update(content_item_kinds=["agents_md.instructions"]),
        ):
            value = copy.deepcopy(base)
            change(value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                resolve_codex_history_source_unit(json.dumps(value), unit)
        for suffix in ("-1", "00", "0/text", "0~1text", "٠", "100", "1", "3"):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                resolve_codex_history_source_unit(
                    history_text(),
                    "codex.history-message.v1#/payload/replacement_history/" + suffix,
                )
        duplicate = history_text().replace('"role": "user"', '"role": "developer", "role": "user"')
        with self.assertRaises(ValueError):
            resolve_codex_history_source_unit(duplicate, unit)

    def test_history_contract_keeps_literal_json_and_native_identity(self):
        envelope = json.loads(history_text())
        literal = '{"role":"developer","text":"This is quoted data."}'
        envelope["payload"]["replacement_history"][0]["content"][0]["text"] = literal
        text, kind, origin = resolve_codex_history_source_unit(
            json.dumps(envelope), "codex.history-message.v1#/payload/replacement_history/0",
        )
        self.assertEqual(text, literal)
        self.assertEqual(kind, "history_message")
        self.assertEqual((origin["message_id"], origin["turn_id"], origin["role"]),
                         ("message-human", "turn-human", "user"))

    def test_mounted_reader_resolves_the_same_history_message_bytes(self):
        from evals.candidate_capture import CandidateCapture
        from test_candidate_capture import LocalClient
        from test_codex_source_units import mount

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mounted = root / "mount"
            mounted.mkdir()
            hit = mount(mounted, value=json.loads(history_text()), split=True)
            expected = "Keep the café window closed."
            hit["matching_ranges"][0]["text"] = expected[:4]
            span = hit["matching_ranges"][0]["spans"][0]
            span.update(
                source_unit="codex.history-message.v1#/payload/replacement_history/0",
                source_byte_end=len(expected.encode()), passage_byte_end=len(expected.encode()),
            )
            client = LocalClient(mounted)
            capture = CandidateCapture(root / "capture", client_factory=lambda: client,
                                       protected_families=set())
            result = capture.capture_case(
                "synthetic-history", "Read captured history", {"results": [hit]},
            )["candidates"][0]
            self.assertTrue(result["complete_selected_passages"], result)
            self.assertEqual(result["text"], expected)
            evidence = result["source_evidence"][0]["spans"][0]
            self.assertEqual(evidence["source_origin"], "history_message")
            self.assertEqual(evidence["source_message"]["message_id"], "message-human")

    def test_mounted_history_refuses_wrong_source_roles_and_pointer(self):
        from evals.candidate_capture import CandidateCapture
        from test_candidate_capture import LocalClient
        from test_codex_source_units import mount

        for conflict in ("source", "role", "mixed_roles", "pointer", "second_role"):
            with self.subTest(conflict=conflict), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                mounted = root / "mount"
                mounted.mkdir()
                roles = (["assistant"] if conflict == "role" else
                         ["user"] if conflict == "second_role" else
                         ["user", "assistant"] if conflict == "mixed_roles" else [])
                hit = mount(mounted, value=history_envelope(), roles=roles, split=True)
                expected = "Keep the café window closed."
                hit["matching_ranges"][0]["text"] = expected[:4]
                span = hit["matching_ranges"][0]["spans"][0]
                span.update(
                    source_unit="codex.history-message.v1#/payload/replacement_history/"
                                + ("0/text" if conflict == "pointer" else "0"),
                    source_byte_end=len(expected.encode()), passage_byte_end=len(expected.encode()),
                )
                if conflict == "source":
                    hit["source_id"] = "codex:another:source"
                if conflict == "second_role":
                    second = copy.deepcopy(hit["matching_ranges"][0])
                    second["passage_id"] = "psg_" + "2" * 32
                    text = "The window remains closed. 🪟"
                    second["text"] = text[:4]
                    second["spans"][0].update(
                        source_unit="codex.history-message.v1#/payload/replacement_history/2",
                        source_byte_end=len(text.encode()), passage_byte_end=len(text.encode()),
                    )
                    hit["matching_ranges"].append(second)
                before = {path.name: path.read_bytes() for path in mounted.iterdir()}
                capture = CandidateCapture(
                    root / "capture", client_factory=lambda: LocalClient(mounted),
                    protected_families=set(),
                )
                result = capture.capture_case(
                    "synthetic-history", "Read captured history", {"results": [hit]},
                )["candidates"][0]
                self.assertFalse(result["complete_selected_passages"], result)
                self.assertNotIn("text", result)
                self.assertEqual(before, {path.name: path.read_bytes() for path in mounted.iterdir()})


if __name__ == "__main__":
    unittest.main()
