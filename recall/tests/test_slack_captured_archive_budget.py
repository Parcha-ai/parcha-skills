"""Actual archive reads share one captured-attribution slice allowance."""
import time
import hashlib
import unittest
from unittest.mock import patch

from recall_server.slack_captured_attribution import _CapturedArchive


class CapturedArchiveBudget(unittest.TestCase):
    def test_cumulative_reservation_refuses_before_second_read(self):
        calls = []

        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                calls.append(reference)
                return b'x' * reference['size_bytes']

        budget = _CapturedArchive(Archive())
        with patch('recall_server.slack_captured_attribution.MAX_PAGE_BYTES', 5):
            self.assertEqual(budget.read_raw_bounded(dict(size_bytes=3),
                deadline_at=time.monotonic() + 1), b'xxx')
            with self.assertRaisesRegex(ValueError, 'captured_archive_unavailable'):
                budget.read_raw_bounded(dict(size_bytes=3), deadline_at=time.monotonic() + 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(budget.consumed, 3)

    def test_failed_read_retains_reservation(self):
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                raise TimeoutError()

        budget = _CapturedArchive(Archive())
        with self.assertRaises(TimeoutError):
            budget.read_raw_bounded(dict(size_bytes=3), deadline_at=time.monotonic() + 1)
        self.assertEqual(budget.consumed, 3)

    def test_cached_bytes_require_same_reference_and_live_deadline(self):
        calls = []
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                calls.append(reference)
                return b'abc'
        reference = dict(artifact_id='a', size_bytes=3,
            content_sha256=hashlib.sha256(b'abc').hexdigest())
        budget = _CapturedArchive(Archive())
        deadline = time.monotonic() + 1
        self.assertEqual(budget.read_raw_bounded(reference, deadline_at=deadline), b'abc')
        self.assertEqual(budget.read_raw_bounded(dict(reference), deadline_at=deadline), b'abc')
        self.assertEqual(len(calls), 1)
        with self.assertRaises(TimeoutError):
            budget.read_raw_bounded(reference, deadline_at=time.monotonic() - 1)
        budget.read_raw_bounded(dict(reference, artifact_id='b'), deadline_at=deadline)
        self.assertEqual(len(calls), 2)
        self.assertEqual(budget.consumed, 6)

    def test_corrupt_bytes_never_enter_cache(self):
        calls = []
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                calls.append(reference)
                return b'bad'
        reference = dict(size_bytes=3, content_sha256=hashlib.sha256(b'abc').hexdigest())
        budget = _CapturedArchive(Archive())
        for _ in range(2):
            with self.assertRaisesRegex(ValueError, 'captured_archive_corrupt'):
                budget.read_raw_bounded(reference, deadline_at=time.monotonic() + 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(budget.cache, {})
        self.assertEqual(budget.consumed, 6)

    def test_logical_and_sdk_allowances_are_separate_and_bounded(self):
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                return b'x' * reference['size_bytes']
        archive = Archive()
        budget = _CapturedArchive(archive, logical_archive=archive)
        with patch('recall_server.slack_captured_attribution.MAX_PAGE_BYTES', 2), patch(
            'recall_server.chunk_bodies.MAX_READ_BYTES', 5):
            with self.assertRaises(ValueError):
                budget.read_raw_bounded(dict(size_bytes=3), deadline_at=time.monotonic()+1)
            self.assertEqual(budget.logical.read_raw_bounded(dict(size_bytes=3),
                deadline_at=time.monotonic()+1), b'xxx')
            with self.assertRaises(ValueError):
                budget.logical.read_raw_bounded(dict(size_bytes=3), deadline_at=time.monotonic()+1)
        self.assertEqual(budget.consumed, 0)
        self.assertEqual(budget.logical.consumed, 3)

    def test_missing_logical_client_never_falls_back_to_raw(self):
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                raise AssertionError('wrong archive owner')
        budget = _CapturedArchive(Archive())
        with self.assertRaisesRegex(ValueError, 'captured_archive_unavailable'):
            _ = budget.logical
        self.assertEqual(budget.consumed, 0)

    def test_expired_deadline_never_calls_archive(self):
        class Archive:
            def read_raw_bounded(self, reference, *, deadline_at):
                raise AssertionError('must not read')

        budget = _CapturedArchive(Archive())
        with self.assertRaises(TimeoutError):
            budget.read_raw_bounded(dict(size_bytes=3), deadline_at=time.monotonic() - 1)
        self.assertEqual(budget.consumed, 0)


if __name__ == '__main__':
    unittest.main()
