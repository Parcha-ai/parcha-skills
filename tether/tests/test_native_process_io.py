import importlib.util
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"native_process_io_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class NativeProcessIOTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)

    def tearDown(self):
        self.temp.cleanup()

    def start(self, script: str):
        return subprocess.Popen(
            [sys.executable, "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

    def test_capture_drains_but_does_not_retain_unbounded_output(self):
        extra = self.runtime.NATIVE_STREAM_CHUNK_BYTES
        process = self.start(
            "import sys; "
            f"sys.stdout.buffer.write(b'A' * "
            f"{self.runtime.MAX_NATIVE_STDOUT_BYTES + extra}); "
            f"sys.stderr.buffer.write(b'B' * "
            f"{self.runtime.MAX_NATIVE_STDERR_BYTES + extra})"
        )
        stdout, stderr, truncated = self.runtime._collect_native_output(
            process,
            "",
            time.monotonic() + 10,
            None,
        )
        self.assertEqual(process.returncode, 0)
        self.assertTrue(truncated)
        self.assertEqual(
            len(stdout),
            self.runtime.MAX_NATIVE_STDOUT_BYTES,
        )
        self.assertEqual(
            len(stderr),
            self.runtime.MAX_NATIVE_STDERR_BYTES,
        )

    def test_capture_cancels_the_process_group(self):
        process = self.start(
            "import time; print('started', flush=True); time.sleep(60)"
        )
        cancel = threading.Event()
        threading.Timer(0.1, cancel.set).start()
        with self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "cancelled",
        ):
            self.runtime._collect_native_output(
                process,
                "",
                time.monotonic() + 10,
                cancel,
            )
        self.assertIsNotNone(process.returncode)

    def test_capture_does_not_wait_for_descendants_holding_pipes(self):
        process = self.start(
            "import subprocess, sys; "
            "subprocess.Popen([sys.executable, '-c', "
            "\"import signal,time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "time.sleep(60)\"], stdout=sys.stdout, stderr=sys.stderr); "
            "print('parent exited', flush=True)"
        )
        started = time.monotonic()
        stdout, stderr, truncated = self.runtime._collect_native_output(
            process,
            "",
            time.monotonic() + 10,
            None,
        )
        elapsed = time.monotonic() - started
        self.assertEqual(process.returncode, 0)
        self.assertIn(b"parent exited", stdout)
        self.assertEqual(stderr, b"")
        self.assertFalse(truncated)
        self.assertLess(elapsed, 4)


if __name__ == "__main__":
    unittest.main()
