"""Actual archive reads share one captured-attribution slice allowance."""
import time
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
