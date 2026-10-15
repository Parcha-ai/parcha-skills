"""Execute the generated staging script with synthetic files and mocked mounts."""
from contextlib import ExitStack, redirect_stderr
import ctypes
import errno
import hashlib
import io
import json
from pathlib import Path
import shlex
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

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

    def stage(self, aliases=None, datasets=None, *, tools=None, inventory=None, allow_missing=False,
              mount_failure=None, machine='x86_64'):
        if inventory is not None:
            local = self.root / "tmp/recall-agent/inventory.json"
            if not local.is_symlink():
                local.write_bytes((self.root / "mnt/archil/evidence" / inventory.object_key).read_bytes())
        command = _agent_exec_command(
            program='true', objects=tuple(self.objects), document_aliases=aliases or {},
            inventory=inventory, inventory_url="https://synthetic.invalid/inventory",
            inventory_size_bytes=1, allow_missing_objects=allow_missing,
            record_spans={}, routing_receipts={}, timeout_seconds=10,
            dataset_aliases=datasets if datasets is not None else {self.data.object_key: 's1/2026-09/documents-part-00000.parquet'},
            tool_objects=tools if tools is not None else {'linux-x86_64': self.tool},
        )
        inner = shlex.split(shlex.split(command[command.index('\nunshare ') + 1:])[-1])
        start = inner.index('python3')
        script, arguments = inner[start + 2], inner[start + 3:start + 10]
        self.assertEqual(len(arguments), 7)
        original_path, original_copy = type(self.root), shutil.copyfile
        original_read, original_walk = Path.read_text, Path.rglob
        original_resolve, original_is_file = Path.resolve, Path.is_file
        self.resolutions, self.file_checks = [], []

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

        def failed(operation):
            if mount_failure == operation:
                ctypes.set_errno(errno.EPERM)
                return True
            if mount_failure == 'unsupported' and operation == 'readonly':
                ctypes.set_errno(errno.ENOSYS)
                return True
            return False

        def bind(source, destination, filesystem, flags, data):
            self.assertEqual((filesystem, flags, data), (None, 4096, None))
            if failed('bind'):
                return -1
            mounted(['mount', '--bind', source.decode(), destination.decode()], check=True)
            return 0

        def readonly(number, directory, destination, flags, pointer, size):
            attr = pointer._obj
            self.assertEqual((number, directory, flags, size), (442, -100, 0, 32))
            self.assertEqual((attr.attr_set, attr.attr_clr, attr.propagation, attr.userns_fd), (1, 0, 0, 0))
            if failed('readonly'):
                return -1
            mounted(['mount', '-o', 'remount,bind,ro', destination.decode()], check=True)
            return 0

        libc = Mock()
        libc.mount.side_effect = bind
        libc.syscall.side_effect = readonly

        def read(path, *args, **kwargs):
            self.reads.append(path)
            return original_read(path, *args, **kwargs)

        def walk(path, *args, **kwargs):
            self.walks.append(path)
            return original_walk(path, *args, **kwargs)

        def resolve(path, *args, **kwargs):
            self.resolutions.append(path)
            return original_resolve(path, *args, **kwargs)

        def is_file(path, *args, **kwargs):
            self.file_checks.append(path)
            return original_is_file(path, *args, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch('pathlib.Path', mapped_path))
            stack.enter_context(patch.object(original_path, 'read_text', read))
            stack.enter_context(patch.object(original_path, 'rglob', walk))
            stack.enter_context(patch.object(original_path, 'resolve', resolve))
            stack.enter_context(patch.object(original_path, 'is_file', is_file))
            stack.enter_context(patch('ctypes.CDLL', return_value=libc))
            stack.enter_context(patch('platform.machine', return_value=machine))
            stack.enter_context(patch('sys.argv', ['stage', *arguments]))
            stack.enter_context(patch('shutil.copyfile', side_effect=lambda src, dst: original_copy(src, mapped_path(dst))))
            stderr = io.StringIO()
            stack.enter_context(redirect_stderr(stderr))
            exec(compile(script, '<generated-stage>', 'exec'), {})
        return stderr.getvalue()

    def inventory(self, datasets):
        body = json.dumps({"objects": [dict(object_key=o.object_key,
                            content_sha256=o.content_sha256) for o in self.objects],
                           "datasets": datasets}).encode()
        digest = hashlib.sha256(body).hexdigest()
        ref = AgentExecObject(f"objects/{digest[:2]}/{digest}", digest)
        path = self.root / "mnt/archil/evidence" / ref.object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return ref, path

    def test_external_inventory_stages_all_600_parts_and_hides_inventory(self):
        parts = [self.data] + [self.object(f"part-{i}".encode()) for i in range(599)]
        datasets = {o.object_key: f"s1000/2026-09/documents-part-{i:05}.parquet"
                    for i, o in enumerate(parts)}
        ref, _path = self.inventory(datasets)
        self.stage(datasets=datasets, inventory=ref)
        self.assertEqual(len(self.mounts), (len(self.objects) - len(parts)) * 2)
        self.assertEqual(len(list((self.root / "tmp/recall-datasets").rglob("*.parquet"))), 600)
        self.assertFalse((self.root / "tmp/recall-authorized" / ref.object_key).exists())
        self.assertFalse(any(ref.object_key in str(call) for call in self.mounts))

    def test_changed_inventory_refuses_before_mounting(self):
        ref, path = self.inventory({self.data.object_key: "s1/2026-09/documents-part-00000.parquet"})
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaises(SystemExit) as error:
            self.stage(inventory=ref)
        self.assertEqual(error.exception.code, 66)
        self.assertEqual(self.mounts, [])

    def test_inventory_symlink_escape_refuses_before_reading(self):
        ref, path = self.inventory({})
        body = path.read_bytes()
        outside = self.root / "private-inventory"
        outside.write_bytes(body)
        (self.root / "tmp/recall-agent/inventory.json").symlink_to(outside)
        with self.assertRaises(SystemExit) as error:
            self.stage(inventory=ref)
        self.assertEqual(error.exception.code, 64)
        self.assertEqual(self.mounts, [])

    def test_scan_skips_manifest_traversal_but_keeps_mounts_dataset_and_tool(self):
        self.stage()
        self.assertEqual(self.walks, [])
        self.assertEqual(self.reads, [])
        self.assertEqual(len(self.mounts), (len(self.objects) - 1) * 2)
        self.assertEqual((self.root / 'tmp/recall-agent/duckdb-real').read_bytes(), b'synthetic duckdb executable')
        link = self.root / 'tmp/recall-datasets/s1/2026-09/documents-part-00000.parquet'
        self.assertEqual(str(link.readlink()), '/mnt/archil/evidence/' + self.data.object_key)

    def test_scan_still_rejects_tool_hash_mismatch(self):
        bad = AgentExecObject(self.tool.object_key, '0' * 64)
        with self.assertRaises(SystemExit) as error:
            self.stage(tools={'linux-x86_64': bad})
        self.assertEqual(error.exception.code, 66)

    def test_failed_mount_or_readonly_attribute_stops_before_aliases(self):
        for operation in ('bind', 'readonly'):
            with self.subTest(operation=operation):
                helper = ScanManifestStagingTests()
                helper.setUp()
                self.addCleanup(helper.doCleanups)
                with self.assertRaises(OSError) as error:
                    helper.stage(mount_failure=operation)
                self.assertEqual(error.exception.errno, errno.EPERM)
                self.assertFalse(any((helper.root / 'tmp/recall-datasets').rglob('*.parquet')))

    def test_unsupported_mount_setattr_stops_before_aliases(self):
        with self.assertRaises(OSError) as error:
            self.stage(mount_failure='unsupported')
        self.assertEqual(error.exception.errno, errno.ENOSYS)
        self.assertFalse(any((self.root / 'tmp/recall-datasets').rglob('*.parquet')))

    def test_unknown_syscall_architecture_stops_before_staging(self):
        with self.assertRaises(SystemExit) as error:
            self.stage(machine='unknown')
        self.assertEqual(error.exception.code, 69)
        self.assertEqual(self.mounts, [])

    def test_dataset_alias_reuses_staged_file_without_metadata_reprobe(self):
        alias = 's1/2026-09/passages-part-00000.parquet'
        self.stage(datasets={self.data.object_key: alias})
        staged = self.root / 'tmp/recall-authorized' / self.data.object_key
        self.assertEqual(self.resolutions.count(staged), 1)
        self.assertNotIn(staged, self.file_checks)
        self.assertEqual(staged.read_bytes(), b'PAR1\xffsynthetic parquet')
        link = self.root / 'tmp/recall-datasets' / alias
        self.assertEqual(str(link.readlink()), '/mnt/archil/evidence/' + self.data.object_key)

    def test_inventory_dataset_without_admitted_object_refuses_alias(self):
        alias = 's1/2026-09/passages-part-00000.parquet'
        self.objects.remove(self.data)
        ref, _path = self.inventory({self.data.object_key: alias})
        # The object exists in Archil, but the signed inventory did not admit it.
        with self.assertRaises(SystemExit) as error:
            self.stage(inventory=ref)
        self.assertEqual(error.exception.code, 66)
        self.assertFalse((self.root / 'tmp/recall-datasets' / alias).is_symlink())

    def test_dataset_source_symlink_escape_still_refuses(self):
        source = self.root / 'mnt/archil/evidence' / self.data.object_key
        source.unlink()
        private = self.root / 'private-data'
        private.write_bytes(b'private')
        source.symlink_to(private)
        with self.assertRaises(SystemExit) as error:
            self.stage()
        self.assertEqual(error.exception.code, 64)

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

    def test_verified_fallback_stages_and_removes_all_writable_aliases(self):
        for family, visible in (("documents", False), ("passages", False),
                                ("documents", True), ("passages", True)):
            with self.subTest(family=family, visible=visible):
                # Each family needs a fresh namespace destination.
                helper = ScanManifestStagingTests()
                helper.setUp()
                self.addCleanup(helper.doCleanups)
                source = helper.root / "mnt/archil/evidence" / helper.data.object_key
                body = source.read_bytes()
                if visible:
                    source.write_bytes(b'stale Archil bytes')
                else:
                    source.unlink()
                fallback = helper.root / "tmp/recall-agent/fallback" / helper.data.object_key
                fallback.parent.mkdir(parents=True)
                fallback.write_bytes(body)
                unused = fallback.parent / "unused"
                unused.write_bytes(b"unused local copy")
                stderr = helper.stage(datasets={helper.data.object_key:
                    f"s1/2026-09/{family}-part-00000.parquet"}, allow_missing=True)
                self.assertIn("objects_unavailable\t0", stderr)
                self.assertEqual((helper.root / "tmp/recall-authorized" / helper.data.object_key).read_bytes(), body)
                self.assertNotIn(source, helper.resolutions)
                self.assertNotIn(source, helper.file_checks)
                self.assertFalse((helper.root / "tmp/recall-agent/fallback").exists())
                mounts = [call for call in helper.mounts if call[1] == "--bind" and call[2] == str(fallback)]
                self.assertEqual(len(mounts), int(family == "passages"))

    def test_local_download_symlink_escape_refuses(self):
        local = self.root / 'tmp/recall-agent/fallback' / self.data.object_key
        local.parent.mkdir(parents=True)
        private = self.root / 'private-data'
        private.write_bytes(b'private')
        local.symlink_to(private)
        with self.assertRaises(SystemExit) as error:
            self.stage()
        self.assertEqual(error.exception.code, 64)

    def test_missing_in_both_stores_keeps_visibility_incomplete(self):
        (self.root / "mnt/archil/evidence" / self.data.object_key).unlink()
        stderr = self.stage(allow_missing=True)
        self.assertIn("objects_unavailable\t1", stderr)
        self.assertFalse((self.root / "tmp/recall-authorized" / self.data.object_key).exists())
        self.assertFalse((self.root / "tmp/recall-datasets/s1/2026-09/documents-part-00000.parquet").exists())


if __name__ == '__main__':
    unittest.main()
