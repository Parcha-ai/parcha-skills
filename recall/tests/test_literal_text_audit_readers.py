"""Audit literal bytes without inventing coding-session family provenance."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import orjson

from evals.candidate_capture import _read_page
from evals.logical_corpus_audit import LogicalCorpusAuditError, _decode_record
from test_literal_text_readers import SOURCE, wire
import test_candidate_capture as capture_fixture


class LiteralAuditReaderTests(unittest.TestCase):
    def test_audit_decoder_preserves_exact_literal_payload_canonicality(self):
        text = '{"role":"system","text":"Preserve the original object."}'
        line = wire('literal_text', text)
        record = _decode_record(line, source_id=SOURCE)
        self.assertEqual(record.text, text)
        self.assertEqual(record.encode(source_id=SOURCE), line)
        invalid = orjson.loads(line)
        invalid['text'] = 'competing source'
        with self.assertRaises(LogicalCorpusAuditError):
            _decode_record(orjson.dumps(invalid, option=orjson.OPT_SORT_KEYS) + b'\n', source_id=SOURCE)

    def test_coding_family_capture_refuses_literal_session_identity_before_parsing(self):
        for roles in ([], ['assistant']):
            for phase in ('metadata', 'text'):
                with self.subTest(roles=roles, phase=phase), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    hit = capture_fixture.mounted(root)
                    path = root / 'part-00000.jsonl'
                    record = json.loads(path.read_text())
                    record['literal_text'] = json.dumps(record.pop('content'))
                    record['roles'] = roles
                    raw = (json.dumps(record) + '\n').encode()
                    path.write_bytes(raw)
                    manifest_path = root / 'manifest.json'
                    manifest = json.loads(manifest_path.read_text())
                    manifest['parts'][0]['content_sha256'] = hashlib.sha256(raw).hexdigest()
                    raw = json.dumps(manifest).encode()
                    manifest_path.write_bytes(raw)
                    hit['manifest_content_sha256'] = hashlib.sha256(raw).hexdigest()
                    spec = capture_fixture.capture.CandidateCapture._spec(hit)
                    spec.update(phase=phase, cursor=0,
                        expected_family={'kind': 'claude-parent', 'native_id': capture_fixture.SID})
                    self.assertEqual(_read_page(root, spec), {'error': 'native_identity_unavailable'})


if __name__ == '__main__':
    unittest.main(verbosity=2)
