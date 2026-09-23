"""Generated scan staging materializes every dataset before isolated execution."""
from pathlib import Path
import unittest
from unittest.mock import patch

from tests.central_brain import test_scan_manifest_staging as fixture


class ScanLocalMaterializationTests(unittest.TestCase):
    def case(self):
        case = fixture.ScanManifestStagingTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_dataset_is_local_exact_bytes_without_per_file_mounts(self):
        case = self.case()
        source = case.root / 'mnt/archil/evidence' / case.data.object_key
        expected = source.read_bytes()
        case.stage()
        self.assertFalse(any(case.data.object_key in str(call) for call in case.mounts))
        source.unlink()
        self.assertEqual((case.root / 'tmp/recall-authorized' / case.data.object_key).read_bytes(), expected)
        self.assertTrue(any(case.tool.object_key in str(call) for call in case.mounts))

    def test_non_document_datasets_keep_lazy_bind_mounts(self):
        for dataset in ('records', 'passages', 'actors'):
            with self.subTest(dataset=dataset):
                case = self.case()
                case.stage(datasets={case.data.object_key:
                    f's1/2026-09/{dataset}-part-00000.parquet'})
                calls = [call for call in case.mounts if case.data.object_key in str(call)]
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0][1], '--bind')
                self.assertEqual(calls[1][2], 'remount,bind,ro')

    def test_large_dataset_streams_every_byte(self):
        case = self.case()
        payload = b'large synthetic parquet block' * 100_000
        obj = case.object(payload)
        case.stage(datasets={obj.object_key: 's1/2026-09/documents-part-00000.parquet'})
        self.assertEqual((case.root / 'tmp/recall-authorized' / obj.object_key).read_bytes(), payload)
        self.assertFalse(any(obj.object_key in str(call) for call in case.mounts))

    def test_original_and_public_roots_are_remounted_readonly_before_agent(self):
        from recall_server.deep_inspection import _agent_exec_command
        case = self.case()
        command = _agent_exec_command(program='true', objects=(case.data,),
            document_aliases={}, record_spans={}, routing_receipts={}, timeout_seconds=10,
            dataset_aliases={case.data.object_key:'s1/2026-09/documents-part-00000.parquet'})
        self_bind = command.index('mount --rbind /tmp/recall-authorized /tmp/recall-authorized')
        original_ro = command.index('mount -o remount,bind,ro /tmp/recall-authorized')
        public_bind = command.index('mount --rbind /tmp/recall-authorized /mnt/archil/evidence')
        public_ro = command.index('mount -o remount,bind,ro /mnt/archil/evidence')
        agent = command.index('exec env -i HOME=/tmp')
        self.assertLess(self_bind, original_ro)
        self.assertLess(original_ro, public_bind)
        self.assertLess(public_bind, public_ro)
        self.assertLess(public_ro, agent)

    def test_corrupt_dataset_prevents_publication(self):
        case = self.case()
        (case.root / 'mnt/archil/evidence' / case.data.object_key).write_bytes(b'changed bytes')
        with self.assertRaises(SystemExit) as error:
            case.stage()
        self.assertEqual(error.exception.code, 66)
        self.assertFalse((case.root / 'tmp/recall-agent/duckdb-real').exists())

    def test_copy_failure_prevents_program_publication(self):
        case = self.case()
        original = Path.open
        def opening(path, mode='r', *args, **kwargs):
            if mode == 'xb' and str(path).endswith(case.data.object_key):
                raise OSError('synthetic local disk failure')
            return original(path, mode, *args, **kwargs)
        with patch.object(type(case.root), 'open', opening), self.assertRaises(OSError):
            case.stage()
        self.assertFalse((case.root / 'tmp/recall-agent/duckdb-real').exists())
