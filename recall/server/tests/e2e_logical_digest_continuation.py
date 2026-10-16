#!/usr/bin/env python3
"""Committed hash continuations preserve native bytes without rereading old parts."""
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

import e2e_logical_append_capability as fixtures


class DigestContinuation(unittest.TestCase):
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish
    catalog = fixtures.AppendCapability.catalog
    queue = fixtures.AppendCapability.queue
    safe = fixtures.AppendCapability.safe

    def setUp(self):
        fixtures.AppendCapability.setUp(self)
        put = self.logical.projection.put_records
        small = patch.object(self.logical.projection, 'put_records',
                             lambda **kw: put(part_bytes=2300, **kw))
        small.start()
        self.addCleanup(small.stop)

    def seed(self, count=8):
        for index in range(count):
            self.ingest(f'prefix-{index}', index + 1, 'synthetic prefix ' + 'x' * 700)
        self.assertEqual(self.publish()['documents'], 1)
        with self.store.connect() as connection:
            return connection.execute('''SELECT artifact_id FROM canonical_evidence_document_parts
                WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal''',
                (self.tenant, self.source)).fetchall()

    def prepare(self):
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        upload = self.logical._prepare_batch_and_upload((candidate,))[0]
        self.addCleanup(upload.body_locators.close)
        return candidate, upload

    def test_committed_prefix_reads_only_tail_and_matches_full_native_oracle(self):
        parts = self.seed()
        self.assertGreater(len(parts), 2)
        self.ingest('suffix', 20, 'new suffix')
        _, full = self.prepare()
        self.logical.append_enabled = True
        read = self.logical.projection.read_part
        reads = []

        def observe(reference, **kwargs):
            reads.append(reference['artifact_id'])
            return read(reference, **kwargs)

        with patch.object(self.logical.projection, 'read_part', observe):
            candidate, warm = self.prepare()
        self.assertEqual(reads, [parts[-1]['artifact_id']])
        self.assertEqual(warm.prepared, full.prepared)
        self.assertEqual(warm.manifest.encode(), full.manifest.encode())
        self.assertEqual(warm.part_references, full.part_references)
        self.assertEqual(self.logical._commit_upload(candidate, warm), 'committed')

    def test_repeated_appends_to_larger_parent_keep_tail_only_reads(self):
        parts = self.seed(count=64)
        self.assertGreater(len(parts), 20)
        for index in range(3):
            self.ingest(f'suffix-{index}', 100 + index, 'new suffix')
            self.logical.append_enabled = False
            _, full = self.prepare()
            self.logical.append_enabled = True
            reads, observe = self.part_reads()
            with observe:
                candidate, warm = self.prepare()
            self.assertEqual(len(reads), 1)
            self.assertEqual(warm.prepared, full.prepared)
            self.assertEqual(warm.manifest.encode(), full.manifest.encode())
            self.assertEqual(warm.part_references, full.part_references)
            self.assertEqual(self.logical._commit_upload(candidate, warm), 'committed')

    def part_reads(self):
        reads = []
        original = self.logical.projection.read_part
        def read(reference, **kwargs):
            reads.append(reference['artifact_id'])
            return original(reference, **kwargs)
        return reads, patch.object(self.logical.projection, 'read_part', read)

    def test_restart_falls_back_to_complete_prefix_verification(self):
        from recall_server.logical_evidence import LogicalEvidenceProjectionStore
        parts = self.seed()
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        self.logical.projection = LogicalEvidenceProjectionStore(self.archive)
        put = self.logical.projection.put_records
        with patch.object(self.logical.projection, 'put_records',
                          lambda **kw: put(part_bytes=2300, **kw)):
            reads, observe = self.part_reads()
            with observe:
                self.prepare()
        self.assertEqual(reads, [part['artifact_id'] for part in parts])

    def test_eviction_keeps_all_history_and_falls_back(self):
        with patch('recall_server.logical_evidence.DIGEST_CONTINUATION_ENTRIES', 1):
            parts = self.seed()
            parent = self.parent
            self.parent = 'second-parent'
            self.ingest('other-native', 1)
            self.assertEqual(self.publish()['documents'], 1)
            self.parent = parent
            self.ingest('suffix', 20)
            self.logical.append_enabled = True
            reads, observe = self.part_reads()
            with observe:
                candidate, upload = self.prepare()
            self.assertEqual(reads, [part['artifact_id'] for part in parts])
            self.assertEqual(upload.prepared.record_count, 9)
            self.assertEqual(self.logical._commit_upload(candidate, upload), 'committed')

    def test_lost_commit_ack_does_not_admit_new_continuation(self):
        self.seed()
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        candidate, upload = self.prepare()
        commit = self.logical._commit
        def lost_ack(*args):
            self.assertEqual(commit(*args), 'committed')
            raise RuntimeError('synthetic lost commit acknowledgement')
        with patch.object(self.logical, '_commit', lost_ack):
            with self.assertRaisesRegex(RuntimeError, 'lost commit'):
                self.logical._commit_upload(candidate, upload)
        self.ingest('next', 30)
        reads, observe = self.part_reads()
        with observe:
            _, recovered = self.prepare()
        self.assertEqual(len(reads), len(upload.part_references))
        self.assertEqual(recovered.prepared.record_count, 10)

    def test_stale_publication_does_not_admit_continuation(self):
        self.seed()
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        candidate, upload = self.prepare()
        admitted = []
        original = self.logical.projection.remember_committed
        def remember(value):
            admitted.append(value)
            return original(value)
        self.ingest('prefix-0', 1, 'edited old body')
        with patch.object(self.logical.projection, 'remember_committed', remember):
            self.assertEqual(self.logical._commit_upload(candidate, upload), 'stale')
        self.assertEqual(admitted, [])
        self.assertFalse(self.safe())
        self.assertEqual(self.publish()['documents'], 1)

    def test_concurrent_attempts_copy_state_and_match_full_oracle(self):
        self.seed()
        self.ingest('suffix', 20)
        _, full = self.prepare()
        self.logical.append_enabled = True
        candidate = self.logical._pending(tenant_id=self.tenant, limit=1)[0]
        def prepare():
            return self.logical._prepare_batch_and_upload((candidate,))[0]
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = list(pool.map(lambda _: prepare(), range(2)))
        self.addCleanup(first.body_locators.close)
        self.addCleanup(second.body_locators.close)
        self.assertIsNot(first._digest_continuation[1], second._digest_continuation[1])
        for upload in (first, second):
            self.assertEqual(upload.prepared, full.prepared)
            self.assertEqual(upload.manifest.encode(), full.manifest.encode())
            self.assertEqual(upload.part_references, full.part_references)
        self.assertEqual(self.logical._commit_upload(candidate, first), 'committed')
        self.assertEqual(self.logical._commit_upload(candidate, second), 'adopted')
        self.ingest('next', 30)
        reads, observe = self.part_reads()
        with observe:
            _, next_upload = self.prepare()
        self.assertEqual(len(reads), 1)
        self.assertEqual(next_upload.prepared.record_count, 10)

    def corrupt(self, *, manifest=False, interior=False):
        with self.store.connect() as connection:
            if manifest:
                row = connection.execute("""SELECT manifest_object_key AS object_key
                    FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s""",
                    (self.tenant, self.source)).fetchone()
            else:
                direction = 'ASC' if interior else 'DESC'
                row = connection.execute("""SELECT object_key FROM canonical_evidence_document_parts
                    WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal """ + direction + ' LIMIT 1',
                    (self.tenant, self.source)).fetchone()
        path = self.archive.root / row['object_key'] / 'data'
        original = path.read_bytes()
        self.addCleanup(path.write_bytes, original)
        path.write_bytes(bytes([original[0] ^ 1]) + original[1:])

    def assert_warm_corruption_refused(self, *, manifest):
        from recall_server.logical_evidence import LogicalEvidenceError
        self.seed()
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        self.corrupt(manifest=manifest)
        with self.assertRaisesRegex(LogicalEvidenceError, 'corrupt'):
            self.prepare()
        self.assertIsNotNone(self.queue())

    def test_warm_manifest_corruption_is_rejected(self):
        self.assert_warm_corruption_refused(manifest=True)

    def test_warm_tail_corruption_is_rejected(self):
        self.assert_warm_corruption_refused(manifest=False)

    def test_warm_interior_corruption_is_detected_by_unchanged_public_reader(self):
        from recall_server.canonical_retrieval import BoundCanonicalRetrieval
        from recall_server.chunk_bodies import ChunkBodyError
        self.seed()
        receipt = f'recall://{self.source}/prefix-0?rev=1#item=0'
        bound = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant,
            principal_id=self.principal, authorized_sources=(self.source,),
            chunk_body_archive=self.archive)
        with self.store.connect() as connection:
            connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE receipt=%s", (receipt,))
        expected = bound.show(receipt)
        self.assertIsNotNone(expected)
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        self.corrupt(interior=True)
        candidate, upload = self.prepare()
        self.assertEqual(self.logical._commit_upload(candidate, upload), 'committed')
        with self.assertRaises(ChunkBodyError):
            bound.show(receipt)

    def test_cold_interior_corruption_refuses_publication(self):
        from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore
        self.seed()
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        self.corrupt(interior=True)
        self.logical.projection = LogicalEvidenceProjectionStore(self.archive)
        put = self.logical.projection.put_records
        with patch.object(self.logical.projection, 'put_records',
                          lambda **kw: put(part_bytes=2300, **kw)):
            with self.assertRaisesRegex(LogicalEvidenceError, 'corrupt'):
                self.prepare()
        self.assertIsNotNone(self.queue())

    def test_corrupt_cached_state_falls_back_to_native_full_verification(self):
        parts = self.seed()
        for state in self.logical.projection._digest_states.values():
            state.update(b'synthetic cache corruption')
        self.ingest('suffix', 20)
        self.logical.append_enabled = True
        reads, observe = self.part_reads()
        with observe:
            _, warm = self.prepare()
        self.assertEqual(len(reads), len(parts))
        self.logical.append_enabled = False
        _, full = self.prepare()
        self.assertEqual(warm.manifest.encode(), full.manifest.encode())


if __name__ == '__main__':
    unittest.main()
