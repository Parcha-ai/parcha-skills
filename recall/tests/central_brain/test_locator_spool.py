"""Locator metadata stays on disk and is released on every publication outcome."""
from types import SimpleNamespace
import tracemalloc
import unittest
from unittest.mock import patch

import psycopg

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector


class LocatorSpoolTests(unittest.TestCase):
    def spool(self):
        from recall_server.logical_evidence_projection import _LocatorSpool
        spool = _LocatorSpool()
        self.addCleanup(spool.close)
        return spool

    def test_large_parent_metadata_is_streamed_with_constant_python_memory(self):
        spool = self.spool()
        tracemalloc.start()
        try:
            for index in range(100_000):
                spool.append((f'doc_{index:032x}', index, 1))
            for index, row in enumerate(spool):
                self.assertEqual(row, (f'doc_{index:032x}', index, 1))
            self.assertEqual(index, 99_999)
            self.assertLess(tracemalloc.get_traced_memory()[1], 2 * 1024 * 1024)
        finally:
            tracemalloc.stop()
        self.assertEqual(next(iter(spool)), ('doc_' + '0' * 32, 0, 1))

    def test_commit_closes_spool_on_all_outcomes_and_cleanup_failures(self):
        for outcome in ('committed', 'repaired', 'adopted', 'stale',
                        RuntimeError('failed'), psycopg.errors.LockNotAvailable('busy'),
                        KeyboardInterrupt()):
            for cleanup_error in (False, True):
                with self.subTest(outcome=outcome, cleanup_error=cleanup_error):
                    spool = self.spool()
                    spool.append(('doc_' + 'a' * 32, 0, 1))
                    upload = SimpleNamespace(body_locators=spool, cleanup_references=())
                    projector = CanonicalLogicalEvidenceProjector(None, None)
                    with patch.object(projector, '_commit', side_effect=outcome if isinstance(outcome, BaseException) else None,
                                      return_value=outcome), patch.object(projector, '_schedule_cleanup',
                                      side_effect=RuntimeError('cleanup failed') if cleanup_error else None):
                        try:
                            projector._commit_upload(None, upload)
                        except BaseException:
                            pass
                    self.assertTrue(spool.closed)

    def test_interrupted_batch_cleanup_closes_all_spools_even_if_enqueue_fails(self):
        spools = [self.spool(), self.spool()]
        uploads = [SimpleNamespace(body_locators=spool, cleanup_references=()) for spool in spools]
        projector = CanonicalLogicalEvidenceProjector(None, None)
        with patch.object(projector, '_schedule_cleanup', side_effect=RuntimeError('enqueue failed')):
            with self.assertRaises(RuntimeError):
                projector._schedule_upload_cleanup(uploads)
        self.assertTrue(all(spool.closed for spool in spools))

    def test_locator_io_failure_is_not_silently_discarded(self):
        spool = self.spool()
        with patch.object(spool, '_file', SimpleNamespace(write=lambda _line: (_ for _ in ()).throw(OSError(28, 'disk full')))):
            with self.assertRaises(OSError):
                spool.append(('doc_' + 'a' * 32, 0, 1))


if __name__ == '__main__':
    unittest.main()
