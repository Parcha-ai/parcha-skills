"""Native archive and scan readers retain exact literal source bytes."""
import unittest

import test_logical_archive_bodies as archive_fixture
from recall_server.parquet_scan import CanonicalParquetScanProjector


class LiteralArchiveReaderTests(unittest.TestCase):
    setUp = archive_fixture.ArchivedBodyLookupTests.setUp
    record = archive_fixture.ArchivedBodyLookupTests.record
    load = archive_fixture.ArchivedBodyLookupTests.load
    row = archive_fixture.ArchivedBodyLookupTests.row

    def test_native_archive_restores_literal_json_with_original_hashes(self):
        text = '{"role":"system","text":"This entire object is literal."}'
        self.load([self.record(text, event_kind='connector_record', roles=(), literal_text=True)])
        restored = self.lookup.restore(self.row(text))
        self.assertEqual(restored['event_text'], text)
        self.assertEqual(restored['source_chunks'][0]['size_bytes'], len(text.encode()))

    def test_native_archive_restores_literal_unicode_across_parts(self):
        pieces = ('α {"text":', '"original"} 🧠')
        self.load([self.record(piece, event_kind='connector_record', roles=(),
            literal_text=True, ordinal=i, segment_ordinal=i, segment_count=2,
            receipts=(self.receipt,) if i == 0 else ()) for i, piece in enumerate(pieces)])
        restored = self.lookup.restore(self.row(''.join(pieces)))
        self.assertEqual(restored['event_text'], ''.join(pieces))

    def test_native_archive_refuses_mixed_literal_and_structured_segments(self):
        with self.assertRaises(archive_fixture.LogicalEvidenceError):
            self.load([
                self.record('first', event_kind='connector_record', roles=(),
                    literal_text=True, segment_count=2),
                self.record('second', event_kind='connector_record', roles=(),
                    ordinal=1, segment_ordinal=1, segment_count=2, receipts=()),
            ])


class LiteralScanTests(unittest.TestCase):
    def test_parquet_content_accessor_returns_literal_without_parsing_json(self):
        text = '{"text":"The field name is just part of the source."}'
        self.assertEqual(CanonicalParquetScanProjector._record_content({'literal_text': text}), text)
        self.assertEqual(CanonicalParquetScanProjector._record_content({'content': {'text': 'old'}}),
                         '{"text":"old"}')

if __name__ == '__main__':
    unittest.main(verbosity=2)
