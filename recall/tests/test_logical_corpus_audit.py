from __future__ import annotations

import hashlib
import unittest

from evals.logical_corpus_audit import (
    _DigestProjection,
    _choose_sample,
    _decode_record,
)
from recall_server.logical_evidence import LogicalEvidenceRecord


class LogicalCorpusAuditTests(unittest.TestCase):
    def test_digest_projection_and_decoder_preserve_exact_records(self) -> None:
        records = (
            LogicalEvidenceRecord(
                ordinal=0,
                event_native_id="native:one",
                event_kind="message",
                occurred_at="2026-07-27T00:00:00Z",
                roles=("user",),
                receipts=("recall://source:test/native:one?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text="question",
            ),
            LogicalEvidenceRecord(
                ordinal=1,
                event_native_id="native:two",
                event_kind="message",
                occurred_at="2026-07-27T00:00:01Z",
                roles=("assistant",),
                receipts=("recall://source:test/native:two?rev=1#item=0",),
                segment_ordinal=0,
                segment_count=1,
                text='{"answer":true}',
            ),
        )
        payload = b"".join(
            record.encode(source_id="source:test")
            for record in records
        )
        upload = _DigestProjection.put_records(
            source_id="source:test",
            records=records,
        )

        decoded = [
            _decode_record(line, source_id="source:test")
            for line in payload.splitlines(keepends=True)
        ]
        self.assertEqual(decoded, list(records))
        self.assertEqual(
            upload.prepared.document_content_sha256,
            hashlib.sha256(payload).hexdigest(),
        )
        self.assertEqual(upload.prepared.record_count, 2)
        self.assertEqual(upload.prepared.receipt_count, 2)

    def test_sample_is_deterministic_and_spans_sources_and_size_quartiles(
        self,
    ) -> None:
        rows = [
            {
                "source_id": source,
                "logical_document_id": f"ldoc_{index:032x}",
                "record_count": index + 1,
            }
            for source, offset in (("source:a", 0), ("source:b", 100))
            for index in range(offset, offset + 100)
        ]

        first = _choose_sample(rows, sample_size=40, seed="fixed")
        second = _choose_sample(rows, sample_size=40, seed="fixed")

        self.assertEqual(first, second)
        self.assertEqual({row["source_id"] for row in first}, {
            "source:a",
            "source:b",
        })
        self.assertEqual(len({
            row["logical_document_id"] for row in first
        }), 40)


if __name__ == "__main__":
    unittest.main()
