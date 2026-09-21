"""Parent proof is streamed once, privately, before any body mutation."""
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock

sys.path[:0] = [str(Path(__file__).resolve().parents[2]), str(Path(__file__).resolve().parents[2] / 'server')]
from tests.central_brain import test_chunk_bodies as fixtures
from recall_server.chunk_retirement import ChunkRetirementError, ParentRetirementLimits, retire_parent_chunks
from recall_server.logical_body_proof import iter_parent_bodies
from recall_server.parent_chunk_proof import ParentMetadataSpool


class ParentChunkProofTests(unittest.TestCase):
    def fixture(self, count=1):
        base = fixtures.ChunkBodyTests()
        rows = [base.document(f'native-{i:04}', f'exact body {i}') for i in range(count)]
        records = [base.record(row, f'exact body {i}', i) for i, row in enumerate(rows)]
        _, archive = base.fixture(rows, records)
        manifest = dict(rows[0]['manifest'], tenant_id='tenant', source_id='source', native_parent_id='session',
                        receipt_count=count)
        return rows, archive, manifest, rows[0]['parts']

    def test_thousand_records_share_one_verified_get(self):
        rows, archive, manifest, parts = self.fixture(1000)
        count = 0
        for first, segments in iter_parent_bodies(archive, tenant_id='tenant', source_id='source',
                native_parent_id='session', manifest=manifest, parts=parts, max_records=2000, max_bytes=1024**2):
            self.assertEqual(first.ordinal, count)
            self.assertEqual(len(segments), 1)
            count += 1
        self.assertEqual(count, len(rows))
        self.assertEqual(len(archive.calls), 1)

    def test_whole_parent_hash_is_checked_before_proof_finishes(self):
        _, archive, manifest, parts = self.fixture()
        manifest['document_content_sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            list(iter_parent_bodies(archive, tenant_id='tenant', source_id='source', native_parent_id='session',
                                   manifest=manifest, parts=parts, max_records=10, max_bytes=1024**2))

    def test_empty_archive_topology_refuses_before_io(self):
        _, archive, manifest, _ = self.fixture()
        manifest.update(record_count=0, part_count=0)
        with self.assertRaisesRegex(ValueError, 'catalog_invalid'):
            list(iter_parent_bodies(archive, tenant_id='tenant', source_id='source', native_parent_id='session',
                                   manifest=manifest, parts=[], max_records=10, max_bytes=1024**2))
        self.assertFalse(archive.calls)

    def test_metadata_spool_rejects_prose_and_is_bounded(self):
        with ParentMetadataSpool(max_bytes=128 * 1024) as spool:
            with self.assertRaises(ChunkRetirementError):
                spool.add_document({'native_id': 'native', 'text_redacted': 'never store prose'})
            spool.claim_native('native')
            with self.assertRaises(ChunkRetirementError):
                spool.claim_native('native')
            with self.assertRaises(ChunkRetirementError):
                for i in range(10000):
                    spool.claim_native(f'{i:08}' + 'x' * 2048)
        self.assertFalse(Path(spool.directory.name).exists())

    def test_invalid_scope_or_limits_do_zero_io(self):
        store, archive = Mock(), Mock()
        for parent in ('', '*', None):
            with self.assertRaises(ChunkRetirementError):
                retire_parent_chunks(store, archive, tenant_id='tenant', source_id='source', native_parent_id=parent)
        for options in ({'batch_documents': 0}, {'hash_bytes': True}, {'batch_documents': 257}, {'hash_bytes': 33 * 1024**2}):
            with self.assertRaises(ChunkRetirementError):
                ParentRetirementLimits(**options)
        store.connect.assert_not_called()
        archive.read_raw.assert_not_called()


class ParentRetirementCliTests(unittest.TestCase):
    def module(self):
        import importlib.util
        path = Path(__file__).resolve().parents[2] / 'scripts/retire_parent_chunks.py'
        spec = importlib.util.spec_from_file_location('parent_retirement_cli', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_dry_run_private_plan_and_content_free_summary(self):
        from contextlib import redirect_stdout
        from io import StringIO
        import os
        import tempfile
        from unittest.mock import patch
        module = self.module()
        result = dict(status='dry_run', eligible_documents=1000, eligible_utf8_bytes=1234,
                      archive_gets=1, archive_bytes=4000, cleared_utf8_bytes=0,
                      plan={'tenant_id': 'tenant:private', 'proof_sha256': 'f' * 64})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'plan.json'
            output = StringIO()
            with patch.dict(os.environ, RECALL_DATABASE_URL='postgresql://synthetic'), \
                 patch.object(module, 'BrainStore'), patch.object(module, 'build_evidence_archive_store'), \
                 patch.object(module, 'retire_parent_chunks', return_value=result), redirect_stdout(output):
                self.assertEqual(module.main(['plan', '--tenant-id', 'tenant:private', '--source-id', 'source:private',
                    '--native-parent-id', 'private-parent', '--plan-file', str(path)]), 0)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('private', output.getvalue())
            self.assertIn('1000', output.getvalue())

    def test_scope_switch_is_explicit_and_does_no_archive_io(self):
        from contextlib import redirect_stdout
        from io import StringIO
        import os
        from unittest.mock import patch
        module = self.module()
        with patch.dict(os.environ, RECALL_DATABASE_URL='postgresql://synthetic'), \
             patch.object(module, 'BrainStore'), patch.object(module, 'build_evidence_archive_store') as archive, \
             patch.object(module, 'set_parent_retirement_enabled') as switch, redirect_stdout(StringIO()):
            self.assertEqual(module.main(['disable', '--tenant-id', 'tenant', '--source-id', 'source',
                                          '--native-parent-id', 'parent']), 0)
        archive.assert_not_called()
        self.assertFalse(switch.call_args.kwargs['enabled'])


if __name__ == '__main__':
    unittest.main()
