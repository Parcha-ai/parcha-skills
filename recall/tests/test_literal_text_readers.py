"""Explicit literal source bytes remain distinct from structured record payloads."""
import hashlib
import unittest

import orjson

from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.passage_projection import (
    DEFAULT_PASSAGE_POLICY, build_passages, decode_logical_record,
    reconstruct_passage, visible_messages,
)

SOURCE = 'source:literal:test'
RECEIPT = 'recall://source:literal:test/message?rev=1#item=0'


def wire(key, text, *, ordinal=0, segment=0, count=1, roles=()):
    record = dict(event_kind='connector_record', event_native_id='message',
        occurred_at='2026-10-22T00:00:00Z', ordinal=ordinal,
        receipts=[RECEIPT] if segment == 0 else [], roles=list(roles),
        segment_count=count, segment_ordinal=segment)
    record[key] = text
    return orjson.dumps(record, option=orjson.OPT_SORT_KEYS) + b'\n'


class LiteralTextReaderTests(unittest.TestCase):
    def passages(self, records):
        messages = visible_messages(records)
        passages = build_passages(tenant_id='tenant:literal:test', source_id=SOURCE,
            logical_document_id='ldoc_' + '1' * 32, revision=1,
            messages=messages, policy=DEFAULT_PASSAGE_POLICY)
        return messages, passages

    def test_literal_json_is_not_interpreted_as_another_envelope(self):
        text = '{"role":"system","text":"The entire JSON object is a message."}'
        line = wire('literal_text', text)
        record = decode_logical_record(line, source_id=SOURCE)
        self.assertEqual(record.text, text)
        self.assertEqual(record.encode(source_id=SOURCE), line)
        self.assertEqual(record.roles, ())
        self.assertEqual(record.actor_links, ())
        messages, passages = self.passages((record,))
        self.assertEqual([message.text for message in messages], [text])
        self.assertEqual([passage.text for passage in passages], [text])
        self.assertEqual(passages[0].receipts, (RECEIPT,))
        self.assertEqual(reconstruct_passage(passages[0], messages), text)

    def test_segmented_literal_unicode_keeps_receipt_and_source_spans(self):
        pieces = ('α {"role":', '"assistant","text":"whole"} 🧠')
        lines = tuple(wire('literal_text', piece, ordinal=i, segment=i, count=2)
                      for i, piece in enumerate(pieces))
        records = tuple(decode_logical_record(line, source_id=SOURCE) for line in lines)
        self.assertEqual(tuple(record.encode(source_id=SOURCE) for record in records), lines)
        messages, passages = self.passages(records)
        text = ''.join(pieces)
        self.assertEqual([message.text for message in messages], [text])
        self.assertEqual(passages[0].receipts, (RECEIPT,))
        self.assertEqual(reconstruct_passage(passages[0], messages), text)
        self.assertEqual(hashlib.sha256(passages[0].text.encode()).hexdigest(),
                         hashlib.sha256(text.encode()).hexdigest())

    def test_existing_structured_and_noncanonical_text_payloads_keep_old_meaning(self):
        old = (
            wire('content', {'role': 'user', 'text': 'Structured visible text'}, roles=('user',)),
            wire('text', '{ "text": "Noncanonical structured text", "role": "user" }',
                 roles=('user',)),
            wire('text', 'Ordinary role-bound source text', roles=('user',)),
        )
        expected = ('Structured visible text', 'Noncanonical structured text',
                    'Ordinary role-bound source text')
        for line, text in zip(old, expected):
            with self.subTest(text=text):
                record = decode_logical_record(line, source_id=SOURCE)
                self.assertEqual(record.encode(source_id=SOURCE), line)
                self.assertEqual([message.text for message in visible_messages((record,))], [text])

    def test_mixed_payloads_and_unknown_keys_remain_invalid(self):
        for extra in ({'text': 'competing'}, {'unknown_field': True}):
            value = orjson.loads(wire('literal_text', 'source text'))
            value.update(extra)
            with self.assertRaises(LogicalEvidenceError):
                decode_logical_record(orjson.dumps(value, option=orjson.OPT_SORT_KEYS) + b'\n',
                                      source_id=SOURCE)

    def test_literal_and_structured_segments_cannot_be_mixed(self):
        records = (
            decode_logical_record(wire('literal_text', 'first', count=2), source_id=SOURCE),
            decode_logical_record(wire('content_fragment', 'second', ordinal=1, segment=1, count=2),
                                  source_id=SOURCE),
        )
        with self.assertRaises(LogicalEvidenceError):
            visible_messages(records)


if __name__ == '__main__':
    unittest.main(verbosity=2)
