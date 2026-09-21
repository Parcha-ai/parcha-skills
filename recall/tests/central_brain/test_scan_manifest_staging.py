"""Execute the generated staging script with synthetic files and mocked mounts."""
from contextlib import ExitStack, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import shlex
import shutil
import tempfile
import unittest
from unittest.mock import patch

from recall_server.deep_inspection import AgentExecObject, _agent_exec_command


class ScanManifestStagingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.objects = []
        self.mounts = []
        self.reads = []
        self.walks = []
        self.tool = self.object(b'synthetic duckdb executable')
        self.data = self.object(b'PAR1\xffsynthetic parquet')
        self.part = self.object(b'{"text":"synthetic evidence"}\n')
        (self.root / 'tmp/recall-agent').mkdir(parents=True)

    def object(self, body):
        sha = hashlib.sha256(body).hexdigest()
        value = AgentExecObject(object_key=f'objects/{sha[:2]}/{sha}', content_sha256=sha)
        path = self.root / 'mnt/archil/evidence' / value.object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        self.objects.append(value)
        return value

    def stage(self, aliases=None, datasets=None, *, tools=None):
        command = _agent_exec_command(
            program='true', objects=tuple(self.objects), document_aliases=aliases or {},
            record_spans={}, routing_receipts={}, timeout_seconds=10,
            dataset_aliases=datasets if datasets is not None else {self.data.object_key: 's1/2026-09/passages-part-00000.parquet'},
            tool_objects=tools if tools is not None else {'linux-x86_64': self.tool},
        )
        inner = shlex.split(shlex.split(command[command.index('\nunshare ') + 1:])[-1])
        start = inner.index('python3')
        script, arguments = inner[start + 2], inner[start + 3:start + 10]
        self.assertEqual(len(arguments), 7)
        original_path, original_copy = type(self.root), shutil.copyfile
        original_read, original_walk = Path.read_text, Path.rglob

        def mapped_path(*args):
            path = original_path(*args)
            if path.is_absolute() and not path.is_relative_to(self.root):
                return self.root / str(path).lstrip('/')
            return path

        def mounted(command, *, check):
            self.assertTrue(check)
            self.mounts.append(command)
            if command[1] == '--bind':
                original_path(command[3]).chmod(0o600)
                original_copy(command[2], command[3])
                original_path(command[3]).chmod(0o400)
            else:
                self.assertEqual(command[:3], ['mount', '-o', 'remount,bind,ro'])

        def read(path, *args, **kwargs):
            self.reads.append(path)
            return original_read(path, *args, **kwargs)

        def walk(path, *args, **kwargs):
            self.walks.append(path)
            return original_walk(path, *args, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch('pathlib.Path', mapped_path))
            stack.enter_context(patch.object(original_path, 'read_text', read))
            stack.enter_context(patch.object(original_path, 'rglob', walk))
            stack.enter_context(patch('subprocess.run', side_effect=mounted))
            stack.enter_context(patch('platform.machine', return_value='x86_64'))
            stack.enter_context(patch('sys.argv', ['stage', *arguments]))
            stack.enter_context(patch('shutil.copyfile', side_effect=lambda src, dst: original_copy(src, mapped_path(dst))))
            stack.enter_context(redirect_stderr(io.StringIO()))
            exec(compile(script, '<generated-stage>', 'exec'), {})

    def test_scan_skips_manifest_traversal_but_keeps_mounts_dataset_and_tool(self):
        self.stage()
        self.assertEqual(self.walks, [])
        self.assertEqual(self.reads, [])
        self.assertEqual(len(self.mounts), len(self.objects) * 2)
        self.assertEqual((self.root / 'tmp/recall-agent/duckdb-real').read_bytes(), b'synthetic duckdb executable')
        link = self.root / 'tmp/recall-datasets/s1/2026-09/passages-part-00000.parquet'
        self.assertEqual(str(link.readlink()), '/mnt/archil/evidence/' + self.data.object_key)

    def test_scan_still_rejects_tool_hash_mismatch(self):
        bad = AgentExecObject(self.tool.object_key, '0' * 64)
        with self.assertRaises(SystemExit) as error:
            self.stage(tools={'linux-x86_64': bad})
        self.assertEqual(error.exception.code, 66)

    def test_scan_still_rejects_invalid_dataset_alias(self):
        with self.assertRaises(SystemExit) as error:
            self.stage(datasets={self.data.object_key: '../unauthorized'})
        self.assertEqual(error.exception.code, 64)

    def test_scan_still_rejects_missing_admitted_object(self):
        (self.root / 'mnt/archil/evidence' / self.data.object_key).unlink()
        with self.assertRaises(SystemExit) as error:
            self.stage()
        self.assertEqual(error.exception.code, 66)

    def test_document_alias_still_binds_manifest_and_part(self):
        self.object(json.dumps({'logical_document_id': 'synthetic-doc', 'parts': [
            {'object_key': self.part.object_key, 'content_sha256': self.part.content_sha256}
        ]}).encode())
        self.stage({'synthetic-doc': 'd1'})
        self.assertEqual(len(self.walks), 1)
        self.assertTrue(self.reads)
        folder = self.root / 'tmp/recall-docs/d1'
        self.assertTrue((folder / 'manifest.json').is_symlink())
        self.assertEqual(str((folder / 'part-00000.jsonl').readlink()), '/mnt/archil/evidence/' + self.part.object_key)

    def test_document_alias_still_refuses_missing_or_malformed_manifest(self):
        self.object(b'{not valid json')
        with self.assertRaises(SystemExit) as error:
            self.stage({'synthetic-doc': 'd1'})
        self.assertEqual(error.exception.code, 66)
        self.assertTrue(self.reads)


if __name__ == '__main__':
    unittest.main()
