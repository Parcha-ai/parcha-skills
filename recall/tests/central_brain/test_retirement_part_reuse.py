"""Bounded immutable parts may be reused, never authority or a failed read."""
import hashlib
import time
import tracemalloc
import unittest
from unittest.mock import Mock, patch

from recall_server.archive import ArchiveDeadlineExceeded
from recall_server.chunk_retirement import ChunkRetirementError
from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceProjectionStore, PART_MEDIA_TYPE
from recall_server.retirement_runner import RunArchive


class PartReuseTests(unittest.TestCase):
    def setUp(self):
        self.payload = b'immutable part\n'
        self.reference = dict(tenant_id='tenant', source_id='source', artifact_id='part',
            object_key='key', version_id=None, media_type=PART_MEDIA_TYPE,
            size_bytes=len(self.payload), content_sha256=hashlib.sha256(self.payload).hexdigest())
        self.delegate = Mock(spec=['read_raw', 'read_raw_bounded'])
        self.delegate.read_raw.return_value = self.payload
        self.delegate.read_raw_bounded.return_value = self.payload
        self.meter = RunArchive(self.delegate, max_bytes=1000)

    def test_reuse_is_attempt_local_and_both_surfaces_share_one_part(self):
        with self.meter.reuse_part():
            self.assertEqual(self.meter.read_raw(self.reference), self.payload)
            self.assertEqual(self.meter.read_raw_bounded(dict(self.reference), deadline_at=time.monotonic()+10), self.payload)
        with self.meter.reuse_part():
            self.meter.read_raw(self.reference)
        self.assertEqual(self.delegate.read_raw.call_count, 2)
        self.delegate.read_raw_bounded.assert_not_called()
        self.assertEqual((self.meter.gets, self.meter.bytes), (3, 3*len(self.payload)))
        self.assertEqual((self.meter.reused_gets, self.meter.reused_bytes), (1, len(self.payload)))

    def test_different_reference_fetches_independently_even_with_identical_payload(self):
        with self.meter.reuse_part():
            for field, value in [('tenant_id','other'), ('source_id','other'), ('artifact_id','new'),
                                 ('version_id','new'), ('object_key','new')]:
                self.meter.read_raw(self.reference)
                self.meter.read_raw(dict(self.reference, **{field:value}))
                self.meter.read_raw(self.reference)
        self.assertEqual(self.delegate.read_raw.call_count, 6)

    def test_changed_content_at_same_key_fetches_new_bytes(self):
        changed = b'new version\n'
        reference = dict(self.reference, size_bytes=len(changed), content_sha256=hashlib.sha256(changed).hexdigest())
        self.delegate.read_raw.side_effect = [self.payload, changed]
        with self.meter.reuse_part():
            self.meter.read_raw(self.reference)
            self.assertEqual(self.meter.read_raw(reference), changed)
            self.assertEqual(self.meter.read_raw(reference), changed)
        self.assertEqual(self.delegate.read_raw.call_count, 2)

    def test_corrupt_payload_is_never_retained_and_original_reader_rejects_it(self):
        projection = LogicalEvidenceProjectionStore(self.meter)
        self.delegate.read_raw.return_value = b'x'*len(self.payload)
        with self.meter.reuse_part():
            for _ in range(2):
                with self.assertRaises(LogicalEvidenceError):
                    projection.read_part(self.reference, tenant_id='tenant', source_id='source')
        self.assertEqual(self.delegate.read_raw.call_count, 2)
        self.assertEqual(self.meter.reused_gets, 0)

    def test_failed_fetch_does_not_reuse_previous_entry(self):
        self.delegate.read_raw.side_effect = [self.payload, OSError('failed'), self.payload]
        with self.meter.reuse_part():
            self.meter.read_raw(self.reference)
            with self.assertRaises(OSError):
                self.meter.read_raw(dict(self.reference, artifact_id='other'))
            self.meter.read_raw(self.reference)
        self.assertEqual(self.delegate.read_raw.call_count, 3)

    def test_budget_and_deadline_still_refuse_reuse(self):
        self.meter.max_bytes = len(self.payload)
        with self.meter.reuse_part():
            self.meter.read_raw(self.reference)
            with self.assertRaises(ChunkRetirementError):
                self.meter.read_raw(self.reference)
            self.meter.max_bytes = 1000
            with self.assertRaises(ArchiveDeadlineExceeded):
                self.meter.read_raw_bounded(self.reference, deadline_at=time.monotonic()-1)
        self.delegate.read_raw_bounded.assert_not_called()

    def test_exception_exit_discards_retained_part(self):
        with self.assertRaises(RuntimeError), self.meter.reuse_part():
            self.meter.read_raw(self.reference)
            raise RuntimeError('stop parent')
        self.meter.read_raw(self.reference)
        self.assertEqual(self.delegate.read_raw.call_count, 2)

    def test_late_transport_response_is_not_retained(self):
        clock = [100.0]
        def late(*args, **kwargs):
            clock[0] = 200.0
            return self.payload
        self.delegate.read_raw_bounded.side_effect = late
        with patch('recall_server.retirement_runner.time.monotonic', side_effect=lambda:clock[0]), self.meter.reuse_part():
            with self.assertRaises(ArchiveDeadlineExceeded):
                self.meter.read_raw_bounded(self.reference, deadline_at=150.0)
            self.meter.read_raw(self.reference)
        self.assertEqual(self.delegate.read_raw.call_count, 1)
        self.assertEqual(self.meter.reused_gets, 0)

    def test_nonpart_reads_are_not_retained(self):
        with self.meter.reuse_part():
            for _ in range(2):
                self.meter.read_raw(dict(self.reference, media_type='text/plain'))
        self.assertEqual(self.delegate.read_raw.call_count, 2)

    def test_existing_part_bound_and_exact_size_limit_retention(self):
        with patch('recall_server.retirement_runner.MAX_PART_BYTES', len(self.payload)-1), self.meter.reuse_part():
            for _ in range(2):
                self.meter.read_raw(self.reference)
        with self.meter.reuse_part():
            for _ in range(2):
                self.meter.read_raw(dict(self.reference, size_bytes=len(self.payload)+1))
        self.assertEqual(self.delegate.read_raw.call_count, 4)

    def test_reader_still_hashes_retained_bytes(self):
        projection = LogicalEvidenceProjectionStore(self.meter)
        with self.meter.reuse_part():
            projection.read_part(self.reference, tenant_id='tenant', source_id='source')
            with patch('recall_server.logical_evidence.hashlib.sha256', wraps=hashlib.sha256) as digest:
                projection.read_part(self.reference, tenant_id='tenant', source_id='source')
                digest.assert_called_once_with(self.payload)
        self.assertEqual(self.delegate.read_raw.call_count, 1)

    def test_byte_bound_evicts_before_fetch_and_retains_multiple_small_parts(self):
        size = len(self.payload)
        references = [dict(self.reference, artifact_id=str(i)) for i in range(3)]
        def measured(_reference):
            self.assertLessEqual(self.meter._part_bytes, size)
            return self.payload
        self.delegate.read_raw.side_effect = measured
        with patch('recall_server.retirement_runner.MAX_PART_BYTES', 2*size), self.meter.reuse_part():
            for reference in references[:2]:
                self.meter.read_raw(reference)
            self.assertEqual(self.meter._part_bytes, 2*size)
            self.meter.read_raw(references[0])  # A is newest; C must evict B.
            self.meter.read_raw(references[2])
            self.meter.read_raw(references[0])
            self.assertEqual(self.delegate.read_raw.call_count, 3)
            self.assertEqual(self.meter._part_bytes, 2*size)
            self.meter.read_raw(references[1])
            self.assertEqual(self.delegate.read_raw.call_count, 4)
        self.assertEqual(self.meter._part_bytes, 0)
        self.assertEqual(len(self.meter._parts), 0)

    def test_entry_bound_evicts_only_and_never_rejects_valid_reads(self):
        with patch('recall_server.retirement_runner._MAX_REUSED_PARTS', 2), self.meter.reuse_part():
            for index in range(5):
                self.assertEqual(self.meter.read_raw(dict(self.reference, artifact_id=str(index))), self.payload)
                self.assertLessEqual(len(self.meter._parts), 2)
        self.assertEqual(self.delegate.read_raw.call_count, 5)
        self.assertEqual(self.meter.gets, 5)

    def test_corrupt_second_part_discards_retention_without_accepting_it(self):
        projection = LogicalEvidenceProjectionStore(self.meter)
        self.delegate.read_raw.side_effect = [self.payload, b'x'*len(self.payload), self.payload]
        with self.meter.reuse_part():
            projection.read_part(self.reference, tenant_id='tenant', source_id='source')
            with self.assertRaises(LogicalEvidenceError):
                projection.read_part(dict(self.reference, artifact_id='other'), tenant_id='tenant', source_id='source')
            projection.read_part(self.reference, tenant_id='tenant', source_id='source')
        self.assertEqual(self.delegate.read_raw.call_count, 3)
        self.assertEqual(self.meter.reused_gets, 0)

    def test_two_maximum_parts_retain_only_one_payload(self):
        from recall_server.logical_evidence import MAX_PART_BYTES
        reference = dict(self.reference, size_bytes=MAX_PART_BYTES,
            content_sha256=hashlib.sha256(b'x'*MAX_PART_BYTES).hexdigest())
        self.meter.max_bytes = 2*MAX_PART_BYTES
        self.delegate.read_raw.side_effect = lambda _: b'x'*MAX_PART_BYTES
        tracemalloc.start()
        try:
            with self.meter.reuse_part():
                self.meter.read_raw(reference)
                self.meter.read_raw(dict(reference, artifact_id='second'))
                retained, peak = tracemalloc.get_traced_memory()
                self.assertLess(retained, MAX_PART_BYTES+1024**2)
                self.assertLess(peak, MAX_PART_BYTES+1024**2)
            self.assertLess(tracemalloc.get_traced_memory()[0], 1024**2)
        finally:
            tracemalloc.stop()


if __name__ == '__main__':
    unittest.main()
