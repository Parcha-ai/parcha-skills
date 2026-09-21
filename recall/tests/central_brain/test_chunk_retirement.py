"""Exact targets and private reviewed plans are mandatory for chunk retirement."""
import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from recall_server.chunk_retirement import (
    ChunkRetirementError, public_report, read_private_plan,
    retire_current_chunks, write_private_plan,
)


class ChunkRetirementTests(unittest.TestCase):
    def test_invalid_or_wildcard_targets_do_no_io(self):
        store, archive = mock.Mock(), mock.Mock()
        for documents in ((), ([],), ('*',), ('doc_' + 'a' * 32,) * 2,
                          tuple('doc_' + f'{number:032x}' for number in range(9))):
            with self.subTest(documents=documents), self.assertRaises(ChunkRetirementError):
                retire_current_chunks(store, archive, tenant_id='tenant:test',
                    source_id='source:test', document_ids=documents)
        store.connect.assert_not_called()
        archive.read_raw.assert_not_called()

    def test_apply_requires_archive_profile_and_reviewed_plan_before_io(self):
        store = mock.Mock()
        for profile, plan in (('postgres', {}), ('archive', None)):
            with mock.patch.dict(os.environ, {'RECALL_CHUNK_BODY_READS': profile}), \
                 self.assertRaises(ChunkRetirementError):
                retire_current_chunks(store, mock.Mock(), tenant_id='tenant:test',
                    source_id='source:test', document_ids=('doc_' + 'a' * 32,),
                    apply=True, reviewed_plan=plan)
        store.connect.assert_not_called()

    def test_expired_or_invalid_deadline_does_no_io(self):
        store, archive = mock.Mock(), mock.Mock()
        for deadline in (-1, float('nan'), float('inf'), True):
            with self.subTest(deadline=deadline), self.assertRaises(ChunkRetirementError):
                retire_current_chunks(store, archive, tenant_id='tenant:test',
                    source_id='source:test', document_ids=('doc_' + 'a' * 32,), deadline_at=deadline)
        store.connect.assert_not_called()
        archive.read_raw.assert_not_called()

    def test_plan_is_private_exclusive_and_symlinks_are_refused(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'plan.json'
            plan = {'private': 'receipt-and-native-id'}
            write_private_plan(path, plan)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(read_private_plan(path), plan)
            with self.assertRaises(ChunkRetirementError):
                write_private_plan(path, plan)
            linked = Path(root) / 'linked.json'
            linked.symlink_to(path)
            with self.assertRaises(ChunkRetirementError):
                read_private_plan(linked)
            path.chmod(0o644)
            with self.assertRaises(ChunkRetirementError):
                read_private_plan(path)

    def test_stdout_summary_has_counts_and_digest_only(self):
        result = dict(status='dry_run', documents=2, chunks=3, bytes=41,
                      proof_sha256='a' * 64, plan={'native_id': 'private', 'receipt': 'private'})
        report = public_report(result)
        self.assertEqual(set(report), {'status', 'documents', 'chunks', 'bytes', 'proof_sha256'})
        self.assertNotIn('private', json.dumps(report))


if __name__ == '__main__':
    unittest.main()
