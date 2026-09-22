"""Run the generated stage; gates prove overlap and failure barriers."""
from contextlib import ExitStack, redirect_stderr
import hashlib
import io
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from recall_server.deep_inspection import AgentExecObject, _agent_exec_command


class ScanStagingConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.items = []
        for index in range(9):
            body = f'object-{index}'.encode()
            digest = hashlib.sha256(body).hexdigest()
            item = AgentExecObject(f'objects/{digest[:2]}/{digest}', digest)
            path = self.root / 'mnt/archil/evidence' / item.object_key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            self.items.append(item)
        (self.root / 'tmp/recall-agent').mkdir(parents=True)
        self.tool = self.items[-1]
        self.calls = []
        self.active = self.peak = self.completed = 0
        self.lock = threading.Lock()
        self.four = threading.Event()
        self.stderr = io.StringIO()

    def stage(self, *, delay=0, single=False, fail=None, missing=False,
              duplicate=False, escape=False, scan=True, tools=None):
        items = tuple(self.items + ([self.items[0]] if duplicate else []))
        command = _agent_exec_command(
            program='true', objects=items, document_aliases={}, record_spans={},
            routing_receipts={}, timeout_seconds=10,
            dataset_aliases={item.object_key: f's1/2026-09/passages-part-{i:05}.parquet'
                             for i, item in enumerate(self.items[:-1])} if scan else {},
            tool_objects=tools or {'linux-x86_64': self.tool}, allow_missing_objects=missing,
        )
        inner = shlex.split(shlex.split(command[command.index('\nunshare ') + 1:])[-1])
        start = inner.index('python3')
        script, arguments = inner[start + 2], inner[start + 3:start + 10]
        cls, copy = type(self.root), shutil.copyfile
        original_is_file = cls.is_file

        def is_file(path):
            if fail == 'stat' and str(path).endswith(self.items[0].object_key):
                raise OSError('synthetic metadata failure')
            return original_is_file(path)

        def mapped(*args):
            path = cls(*args)
            return self.root / str(path).lstrip('/') if path.is_absolute() and not path.is_relative_to(self.root) else path

        def mounted(command, *, check):
            self.assertTrue(check)
            bind = command[1] == '--bind'
            with self.lock:
                self.calls.append(command)
                if bind:
                    self.active += 1
                    self.peak = max(self.peak, self.active)
                    if self.active == 4:
                        self.four.set()
            if bind:
                if delay:
                    if not single or command[2].endswith(self.items[0].object_key):
                        time.sleep(delay)
                elif fail is None:
                    self.four.wait(.1)
                if fail == 'bind' and command[2].endswith(self.items[0].object_key):
                    with self.lock:
                        self.active -= 1
                    raise subprocess.CalledProcessError(1, ['mount'])
                cls(command[3]).chmod(0o600)
                copy(command[2], command[3])
                cls(command[3]).chmod(0o400)
            else:
                self.assertEqual(command[:3], ['mount', '-o', 'remount,bind,ro'])
                with self.lock:
                    self.active -= 1
                    self.completed += 1
                if fail == 'remount' and command[3].endswith(self.items[0].object_key):
                    raise subprocess.CalledProcessError(1, ['mount'])

        if escape:
            path = self.root / 'mnt/archil/evidence' / self.items[0].object_key
            path.unlink()
            path.symlink_to(self.root / 'private')
            (self.root / 'private').write_bytes(b'not authorized')
        with ExitStack() as stack:
            stack.enter_context(patch('pathlib.Path', mapped))
            stack.enter_context(patch.object(cls, 'is_file', is_file))
            stack.enter_context(patch('subprocess.run', side_effect=mounted))
            stack.enter_context(patch('platform.machine', return_value='x86_64'))
            stack.enter_context(patch('sys.argv', ['stage', *arguments]))
            stack.enter_context(patch('shutil.copyfile', side_effect=lambda src, dst: copy(src, mapped(dst))))
            stack.enter_context(redirect_stderr(self.stderr))
            exec(compile(script, '<generated-stage>', 'exec'), {})

    def test_four_real_overlapping_tasks_and_readonly_order(self):
        self.stage()
        self.assertTrue(self.four.is_set())
        self.assertEqual(self.peak, 4)
        self.assertEqual(self.active, 0)
        self.assertEqual(self.completed, len(self.items))
        for item in self.items:
            calls = [call for call in self.calls if call[-1].endswith(item.object_key)]
            self.assertEqual([call[1] for call in calls], ['--bind', '-o'])
        self.assertIn('objects_ready', self.stderr.getvalue())

    def test_bind_and_remount_failures_join_before_stage_exits(self):
        for failure in ('bind', 'remount'):
            with self.subTest(failure=failure):
                # A fresh fixture avoids a prior exclusive destination.
                other = type(self)(); other.setUp()
                try:
                    with self.assertRaises(subprocess.CalledProcessError):
                        other.stage(delay=.01, fail=failure)
                    self.assertEqual(other.active, 0)
                    self.assertNotIn('objects_ready', other.stderr.getvalue())
                    self.assertFalse((other.root / 'tmp/recall-agent/duckdb-real').exists())
                finally:
                    other.doCleanups()

    def test_duplicate_refuses_before_any_mount(self):
        with self.assertRaises(SystemExit):
            self.stage(duplicate=True)
        self.assertEqual(self.calls, [])

    def test_symlink_escape_joins_and_never_mounts_outside(self):
        with self.assertRaises(SystemExit) as error:
            self.stage(delay=.01, escape=True)
        self.assertEqual(error.exception.code, 64)
        self.assertEqual(self.active, 0)
        self.assertFalse(any(call[1] == '--bind' and call[2].endswith('/private') for call in self.calls))
        self.assertNotIn('objects_ready', self.stderr.getvalue())

    def test_missing_data_allowed_but_missing_tool_refused(self):
        (self.root / 'mnt/archil/evidence' / self.items[0].object_key).unlink()
        self.stage(missing=True)
        self.assertIn('objects_unavailable\t1', self.stderr.getvalue())
        self.assertEqual(self.completed, len(self.items) - 1)
        other = type(self)(); other.setUp()
        try:
            (other.root / 'mnt/archil/evidence' / other.tool.object_key).unlink()
            with self.assertRaises(SystemExit) as error:
                other.stage(missing=True, delay=.01)
            self.assertEqual(error.exception.code, 66)
            self.assertEqual(other.active, 0)
            self.assertNotIn('objects_ready', other.stderr.getvalue())
        finally:
            other.doCleanups()

    def test_document_execution_keeps_serial_staging(self):
        self.stage(scan=False, delay=.001)
        self.assertEqual(self.peak, 1)

    def test_missing_nonselected_tool_is_still_required(self):
        other_tool = self.items[-2]
        (self.root / 'mnt/archil/evidence' / other_tool.object_key).unlink()
        with self.assertRaises(SystemExit) as error:
            self.stage(missing=True, delay=.01, tools={
                'linux-x86_64': self.tool, 'linux-arm64': other_tool,
            })
        self.assertEqual(error.exception.code, 66)
        self.assertEqual(self.active, 0)
        self.assertNotIn('objects_ready', self.stderr.getvalue())

    def test_metadata_failure_joins_and_prevents_publication(self):
        with self.assertRaises(OSError):
            self.stage(delay=.01, fail='stat')
        self.assertEqual(self.active, 0)
        self.assertNotIn('objects_ready', self.stderr.getvalue())
        self.assertFalse((self.root / 'tmp/recall-agent/duckdb-real').exists())


if __name__ == '__main__':
    unittest.main()
