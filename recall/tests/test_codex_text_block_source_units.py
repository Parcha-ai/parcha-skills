"""Exact text-block receipts resolve without authorizing other output blocks."""
import copy
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from contracts.source_units import codex_tool_source_unit_role, resolve_codex_tool_source_unit
from evals.candidate_capture import CandidateCapture
from recall_server.passage_projection import PassageMessage
from test_candidate_capture import LocalClient
from test_codex_source_units import FORMS, RECEIPT, TEXT, envelope, mount


class CodexTextBlockContract(unittest.TestCase):
    def test_text_block_declares_only_tool_role_at_native_message_boundary(self):
        unit = 'codex.tool-result.block-text.v1#/payload/output/2/text'
        self.assertEqual(codex_tool_source_unit_role(unit), 'tool')
        message = PassageMessage(record_ordinal=0, occurred_at='2026-09-18T00:00:00Z',
                                 roles=('tool',), receipts=(RECEIPT,), text=TEXT,
                                 source_unit=unit)
        message.validate()
        for role in ('user', 'assistant'):
            with self.assertRaises(ValueError):
                replace(message, roles=(role,)).validate()

    def test_mixed_output_resolves_only_the_selected_literal_text(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / 'archive'
            archive.mkdir()
            value = envelope(FORMS[3], [
                {'type': 'input_text', 'text': TEXT},
                {'type': 'input_image', 'image_url': 'fixture-image', 'detail': 'auto'},
                {'type': 'unknown', 'text': 'Not authorized as text.'},
            ])
            hit = mount(archive, FORMS[3], value=value, split=True)
            unit = 'codex.tool-result.block-text.v1#/payload/output/0/text'
            hit['matching_ranges'][0]['spans'][0]['source_unit'] = unit
            before = {p.name: p.read_bytes() for p in archive.iterdir()}
            client = LocalClient(archive)
            capture = CandidateCapture(root / 'capture', client_factory=lambda: client,
                                       protected_families=set())
            result = capture.capture_case('fixture', 'Exact source', {'results': [hit]})['candidates'][0]
            self.assertTrue(result['complete_selected_passages'], result)
            self.assertEqual(result['text'], TEXT)
            self.assertEqual(result['source_evidence'][0]['spans'][0]['source_unit'], unit)
            self.assertEqual(before, {p.name: p.read_bytes() for p in archive.iterdir()})
            self.assertTrue(all(len(args['program'].encode()) <= 16000 for _, args, _ in client.calls))

    def test_literal_blocks_keep_independent_unicode_byte_coordinates(self):
        value = envelope(FORMS[3], [
            {'type': 'input_text', 'text': 'First.'},
            {'type': 'input_image', 'image_url': 'fixture-image'},
            {'type': 'input_text', 'text': TEXT},
            {'type': 'input_text', 'text': ''},
        ])
        for index, expected in ((0, 'First.'), (2, TEXT), (3, '')):
            unit = f'codex.tool-result.block-text.v1#/payload/output/{index}/text'
            text, kind, provenance = resolve_codex_tool_source_unit(json.dumps(value), unit)
            self.assertEqual(text.encode(), expected.encode())
            self.assertEqual(kind, 'tool_result')
            self.assertEqual(provenance, {'type': 'custom_tool_call_output', 'role': 'tool',
                                         'call_id': 'call-17', 'name': 'ArbitraryTool'})
        with self.assertRaises(ValueError):
            resolve_codex_tool_source_unit(json.dumps(value),
                                           'codex.tool-result.string.v1#/payload/output')

    def test_selected_images_unknown_blocks_and_noncanonical_pointers_refuse(self):
        value = envelope(FORMS[3], [
            {'type': 'input_text', 'text': TEXT},
            {'type': 'input_image', 'text': 'Not a text block.'},
            {'type': 'unknown', 'text': 'Not a text block.'},
        ])
        prefix = 'codex.tool-result.block-text.v1#/payload/output/'
        for suffix in ('1/text', '2/text', '3/text', '-1/text', '+0/text', '00/text',
                       '٠/text', '0', '0/text/extra', '0/other', '*/text', '0~1text'):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                resolve_codex_tool_source_unit(json.dumps(value), prefix + suffix)
        for suffix in ('-1/text', '+0/text', '00/text', '٠/text', '0', '0/text/extra', '*/text'):
            self.assertIsNone(codex_tool_source_unit_role(prefix + suffix))

    def test_block_contract_preserves_envelope_and_exact_leaf_refusals(self):
        unit = 'codex.tool-result.block-text.v1#/payload/output/0/text'
        good = envelope(FORMS[3], [{'type': 'input_text', 'text': TEXT}])
        changes = [
            ('top', 'channel', 'analysis'), ('payload', 'channel', 'analysis'),
            ('top', 'role', 'assistant'), ('payload', 'role', 'user'),
            ('top', 'type', 'event_msg'), ('payload', 'type', 'function_call_output'),
            ('payload', 'call_id', 7), ('payload', 'name', None),
            ('payload', 'output', TEXT), ('payload', 'output', []),
            ('block', 'text', 7), ('block', 'text', '\ud800'),
            ('block', 'channel', 'analysis'), ('block', 'role', 'tool'),
        ]
        for location, key, bad in changes:
            value = copy.deepcopy(good)
            target = value if location == 'top' else value['payload']
            if location == 'block':
                target = target['output'][0]
            target[key] = bad
            with self.subTest(location=location, key=key), self.assertRaises(ValueError):
                resolve_codex_tool_source_unit(json.dumps(value), unit)
        duplicate = json.dumps(good).replace('"call_id": "call-17"',
                                             '"call_id": "other", "call_id": "call-17"')
        with self.assertRaises(ValueError):
            resolve_codex_tool_source_unit(duplicate, unit)

    def test_generated_reader_preserves_receipt_manifest_and_utf8_bounds(self):
        for mutation in ('receipt', 'manifest', 'split_utf8', 'outside_leaf', 'wrong_role'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                archive = root / 'archive'
                archive.mkdir()
                hit = mount(archive, FORMS[3], value=envelope(FORMS[3], [
                    {'type': 'input_text', 'text': TEXT},
                    {'type': 'input_image', 'image_url': 'fixture-image'},
                ]), roles=['user'] if mutation == 'wrong_role' else None)
                span = hit['matching_ranges'][0]['spans'][0]
                span['source_unit'] = 'codex.tool-result.block-text.v1#/payload/output/0/text'
                if mutation == 'receipt':
                    hit['matching_ranges'][0]['receipts'] = [RECEIPT + '-forged']
                elif mutation == 'manifest':
                    hit['manifest_content_sha256'] = 'f' * 64
                elif mutation == 'split_utf8':
                    span['source_byte_end'] = span['passage_byte_end'] = 4
                elif mutation == 'outside_leaf':
                    span['source_byte_end'] = span['passage_byte_end'] = len(TEXT.encode()) + 1
                client = LocalClient(archive)
                capture = CandidateCapture(root / 'capture', client_factory=lambda: client,
                                           protected_families=set())
                result = capture.capture_case('fixture', 'Exact source',
                                              {'results': [hit]})['candidates'][0]
                self.assertFalse(result['complete_selected_passages'], result)
                self.assertNotIn('text', result)


if __name__ == '__main__':
    unittest.main()
