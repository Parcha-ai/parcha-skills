"""Interactive hydration never scans a parent to find an unlocated record."""
from pathlib import Path
import sys
import unittest

sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / 'server')]
from tests.central_brain import test_chunk_bodies as fixtures
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks
from recall_server.chunk_hydration import hydrate_chunk_rows


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class Store(fixtures.Store):
    def __init__(self, rows, texts):
        super().__init__(rows)
        self.fallback = {chunk['receipt']: dict(receipt=chunk['receipt'], text_redacted=text,
                        text_sha256=chunk['text_sha256']) for row, text in zip(rows, texts) for chunk in row['chunks']}
        self.fallback_queries = 0
        self.revoked = False

    def _execute_bounded(self, connection, sql, params, deadline_at):
        if 'SELECT chunk.receipt,chunk.text_redacted,chunk.text_sha256' in sql:
            self.fallback_queries += 1
            tenant, sources, receipts = params
            return Rows([] if self.revoked or tenant != 'tenant' or 'source' not in sources else
                        [self.fallback[receipt] for receipt in receipts if receipt in self.fallback])
        return super()._execute_bounded(connection, sql, params, deadline_at)


class LocatedPublicReadTests(unittest.TestCase):
    def setUp(self):
        base = fixtures.ChunkBodyTests()
        self.texts = ['located body α', 'unlocated body 🧠']
        self.documents = [base.document(str(index), text) for index, text in enumerate(self.texts)]
        records = [base.record(row, text, index) for index, (row, text) in enumerate(zip(self.documents, self.texts))]
        base.fixture(self.documents, records)
        template = self.documents[0]['parts'][0]
        parts, payloads = [], {}
        for index, record in enumerate(records):
            payload = record.encode(source_id='source')
            parts.append(dict(template, part_ordinal=index, first_record_ordinal=index,
                last_record_ordinal=index, size_bytes=len(payload), content_sha256=fixtures.digest(payload),
                object_key=str(index), receipt_count=1))
            payloads[str(index)] = payload
        for row in self.documents:
            row.update(parts=parts, body_record_ordinal=None, body_record_count=None)
            row['manifest']['part_count'] = 2
        self.documents[0].update(body_record_ordinal=0, body_record_count=1)
        self.store, self.archive = Store(self.documents, self.texts), fixtures.Archive(payloads)
        self.rows = [dict(source_id='source', document_id=row['document_id'], ordinal=0,
                         receipt=row['chunks'][0]['receipt'], text_redacted=None) for row in self.documents]

    def hydrate(self, sources=('source',)):
        hydrate_chunk_rows(self.store, self.archive, self.rows, tenant_id='tenant', source_ids=sources)

    def test_mixed_parent_reads_only_located_part_and_exact_pg_fallback(self):
        self.hydrate()
        self.assertEqual([row['text_redacted'] for row in self.rows], self.texts)
        self.assertEqual([reference['object_key'] for reference in self.archive.calls], ['0'])
        self.assertEqual(self.store.fallback_queries, 1)

    def test_all_null_locators_do_zero_archive_io(self):
        self.documents[0].update(body_record_ordinal=None, body_record_count=None)
        self.hydrate()
        self.assertEqual([row['text_redacted'] for row in self.rows], self.texts)
        self.assertFalse(self.archive.calls)

    def test_low_level_default_retains_transitional_full_parent_read(self):
        result = read_archived_chunks(self.store, self.archive, tenant_id='tenant', source_ids=('source',),
                                      document_ids=tuple(row['document_id'] for row in self.documents))
        self.assertEqual(len(result), 2)
        self.assertEqual(len(self.archive.calls), 2)

    def test_malformed_locator_never_becomes_fallback(self):
        self.documents[0]['body_record_count'] = None
        with self.assertRaises(ChunkBodyError):
            self.hydrate()
        self.assertFalse(self.archive.calls)
        self.assertEqual(self.store.fallback_queries, 0)

    def test_located_corruption_never_becomes_fallback(self):
        self.archive.payloads['0'] = b'corrupt requested object'
        with self.assertRaises(ChunkBodyError):
            self.hydrate()
        self.assertEqual(self.store.fallback_queries, 0)

    def test_unlocated_blank_or_corrupt_pg_fails_closed(self):
        self.documents[0].update(body_record_ordinal=None, body_record_count=None)
        for text in ('', 'wrong inline bytes'):
            self.store.fallback[self.rows[0]['receipt']]['text_redacted'] = text
            with self.subTest(text=text), self.assertRaises(ChunkBodyError):
                self.hydrate()
        self.assertFalse(self.archive.calls)

    def test_denied_or_revoked_fallback_is_not_returned(self):
        self.documents[0].update(body_record_ordinal=None, body_record_count=None)
        for sources, revoked in (((), False), (('source',), True)):
            self.store.revoked = revoked
            with self.subTest(sources=sources), self.assertRaises(ChunkBodyError):
                self.hydrate(sources)
        self.assertFalse(self.archive.calls)


if __name__ == '__main__':
    unittest.main()
