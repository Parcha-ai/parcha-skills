"""Located event pools share part reads under the original byte/time bounds."""

import copy
import time
import unittest
from unittest.mock import patch

from recall_server.chunk_bodies import ChunkBodyError
from recall_server.db import SearchDeadlineExceeded
from tests.central_brain.test_chunk_bodies import ChunkBodyTests


class LocatedChunkPoolTests(unittest.TestCase):
    def fixture(self, count):
        owner = ChunkBodyTests()
        texts = [f"Exact located event {index} α" for index in range(count)]
        rows = [owner.document(f"turn-{index}", text) for index, text in enumerate(texts)]
        records = [owner.record(row, text, index) for index, (row, text) in enumerate(zip(rows, texts))]
        store, archive = owner.fixture(rows, records)
        for index, row in enumerate(rows):
            row.update(body_record_ordinal=index, body_record_count=1)
        return owner, store, archive, rows, texts

    def test_100_and_101_located_events_read_the_same_part_once(self):
        for count in (100, 101):
            with self.subTest(count=count):
                owner, store, archive, rows, texts = self.fixture(count)
                deadline = time.monotonic() + 5
                result = owner.read(store, archive, located_only=True, deadline_at=deadline)
                self.assertEqual([result[("source", row["document_id"])][0]["text_redacted"] for row in rows], texts)
                self.assertEqual(len(archive.calls), 1)
                self.assertEqual(sum(call["size_bytes"] for call in archive.calls),
                                 sum(map(len, archive.payloads.values())))
                self.assertEqual(store.deadlines, [deadline, deadline])

    def test_transitional_or_unbudgeted_reads_keep_existing_count_bound(self):
        for options in ({}, {"deadline_at": time.monotonic() + 5}, {"located_only": True}):
            owner, store, archive, _, _ = self.fixture(101)
            with self.subTest(options=options), self.assertRaisesRegex(ChunkBodyError, "request_invalid"):
                owner.read(store, archive, **options)
            self.assertEqual(store.calls, [])
            self.assertEqual(archive.calls, [])

    def test_large_located_pool_retains_part_and_result_byte_bounds(self):
        for bound in ("MAX_READ_BYTES", "MAX_RESULT_BYTES"):
            owner, store, archive, _, _ = self.fixture(101)
            with self.subTest(bound=bound), patch("recall_server.chunk_bodies." + bound, 1), \
                 self.assertRaises(ChunkBodyError):
                owner.read(store, archive, located_only=True, deadline_at=time.monotonic() + 5)
            self.assertEqual(len(archive.calls), 0 if bound == "MAX_READ_BYTES" else 1)

    def test_large_located_pool_keeps_expired_deadline_and_catalog_fence(self):
        owner, store, archive, rows, _ = self.fixture(101)
        with self.assertRaises(SearchDeadlineExceeded):
            owner.read(store, archive, located_only=True, deadline_at=time.monotonic() - 1)
        self.assertEqual(archive.calls, [])
        after = copy.deepcopy(rows[:-1])  # A formerly current event was forgotten during IO.
        store.snapshots.append(after)
        with self.assertRaisesRegex(ChunkBodyError, "body_unavailable"):
            owner.read(store, archive, located_only=True, deadline_at=time.monotonic() + 5)
        self.assertEqual(len(archive.calls), 1)


if __name__ == "__main__":
    unittest.main()
