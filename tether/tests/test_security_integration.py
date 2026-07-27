from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import socket
import sqlite3
import stat
import sys
import tempfile
import threading
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
SECURITY_PATH = ROOT / "runtime" / "security.py"
BOOT_ID = "00000000-0000-4000-8000-000000000001"


def load_security():
    name = "tether_security_integration_security"
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(name, SECURITY_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("security module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


SECURITY = load_security()


def load_runtime(
    home: pathlib.Path,
    *,
    approved_roots: tuple[pathlib.Path, ...] = (),
    staging_directory: pathlib.Path | None = None,
    max_upload_bytes: int = 1024,
):
    environment = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "TETHER_UPLOAD_APPROVED_ROOTS": os.pathsep.join(
            str(path) for path in approved_roots
        ),
        "TETHER_UPLOAD_STAGING_DIRECTORY": str(
            staging_directory or home / ".hermes" / "upload-staging"
        ),
        "TETHER_UPLOAD_MAX_BYTES": str(max_upload_bytes),
    }
    with mock.patch.dict(os.environ, environment, clear=False), mock.patch.dict(
        sys.modules,
        {"security": SECURITY},
        clear=False,
    ):
        name = f"security_integration_runtime_{id(home)}_{os.urandom(4).hex()}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError("runtime module could not be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class UploadProbe:
    def __init__(self, mutate=None):
        self.calls: list[dict[str, object]] = []
        self.mutate = mutate
        self.allocated_filename = ""

    def allocate(self, _token, filename, _size):
        self.allocated_filename = filename
        return (
            "F12345678",
            "https://files.slack.com/upload/v1/test-signed-url",
        )

    def upload_bytes(self, _token, _upload_url, staged):
        if self.mutate is not None:
            self.mutate()
        path = pathlib.Path(staged.path)
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            chunks = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        self.calls.append(
            {
                "kwargs": {},
                "path": path,
                "content": b"".join(chunks),
                "device": opened.st_dev,
                "inode": opened.st_ino,
                "owner": opened.st_uid,
                "mode": stat.S_IMODE(opened.st_mode),
                "links": opened.st_nlink,
                "parent_mode": stat.S_IMODE(path.parent.stat().st_mode),
            }
        )

    def complete(self, _token, _channel, file_id, **kwargs):
        self.calls[-1]["kwargs"] = kwargs
        return {"ok": True, "files": [{"id": file_id}]}


class StoreSecurityIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name) / "home"
        self.home.mkdir(mode=0o700)
        self.runtime = load_runtime(self.home)

    def tearDown(self):
        self.temp.cleanup()

    def test_store_rejects_symlinked_database_without_touching_target(self):
        state = self.home / ".hermes"
        state.mkdir(mode=0o700)
        target = self.home / "target.db"
        with sqlite3.connect(target) as database:
            database.execute("PRAGMA user_version=0")
        target.chmod(0o600)
        link = state / "bridges.db"
        link.symlink_to(target)

        with self.assertRaises(
            (SECURITY.StatePathError, RuntimeError, ValueError)
        ):
            self.runtime.Store(link)

        with sqlite3.connect(target) as database:
            self.assertEqual(database.execute("PRAGMA user_version").fetchone()[0], 0)

    def test_store_tightens_owner_owned_legacy_database_before_migration(self):
        state = self.home / ".hermes"
        state.mkdir(mode=0o700)
        database_path = state / "bridges.db"
        with sqlite3.connect(database_path) as database:
            database.execute("PRAGMA user_version=0")
        database_path.chmod(0o644)

        self.runtime.Store(database_path)
        self.assertEqual(stat.S_IMODE(database_path.stat().st_mode), 0o600)

    def test_store_rejects_database_owned_by_another_uid(self):
        state = self.home / ".hermes"
        state.mkdir(mode=0o700)
        database_path = state / "bridges.db"
        with sqlite3.connect(database_path):
            pass
        database_path.chmod(0o600)

        with mock.patch.object(
            os,
            "geteuid",
            return_value=os.geteuid() + 1,
        ), self.assertRaises(
            (SECURITY.StatePathError, RuntimeError, PermissionError)
        ):
            self.runtime.Store(database_path)

    def test_store_database_wal_and_shm_are_owner_only(self):
        state = self.home / ".hermes"
        state.mkdir(mode=0o777)
        state.chmod(0o777)
        database_path = state / "bridges.db"
        store = self.runtime.Store(database_path)

        with store.connect() as database:
            database.execute("CREATE TABLE IF NOT EXISTS mode_probe(value TEXT)")
            database.execute("INSERT INTO mode_probe(value) VALUES('probe')")
            database.commit()
            database.execute("SELECT * FROM mode_probe").fetchall()
            paths = (
                database_path,
                pathlib.Path(f"{database_path}-wal"),
                pathlib.Path(f"{database_path}-shm"),
            )
            self.assertTrue(all(path.exists() for path in paths))
            for path in paths:
                with self.subTest(path=path.name):
                    info = path.stat()
                    self.assertEqual(info.st_uid, os.geteuid())
                    self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o700)


class BrokerPeerBoundaryIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name) / "home"
        self.home.mkdir(mode=0o700)
        self.runtime = load_runtime(self.home)
        self.socket_path = self.home / ".hermes" / "bridge.sock"

    def tearDown(self):
        self.temp.cleanup()

    def close_server(self, server):
        server.shutdown()
        server.server_close()
        self.socket_path.unlink(missing_ok=True)

    def test_broker_refuses_root_before_creating_state(self):
        with mock.patch.object(self.runtime.os, "geteuid", return_value=0):
            with self.assertRaisesRegex(RuntimeError, "refuses to run as root"):
                self.runtime.start_broker("test-token", self.socket_path)
        self.assertFalse(self.socket_path.exists())

    def test_broker_rejects_a_different_peer_uid(self):
        server = self.runtime.start_broker("test-token", self.socket_path)
        try:
            with mock.patch.object(
                self.runtime,
                "_peer_credentials",
                return_value=(1234, os.geteuid() + 1, os.getegid()),
            ):
                with self.assertRaises(self.runtime.NativeContinuationError) as rejected:
                    self.runtime.broker_call({"op": "status"}, self.socket_path)
            self.assertEqual(rejected.exception.code, "peer_uid_mismatch")
        finally:
            self.close_server(server)

    def test_incomplete_client_is_closed_on_read_timeout(self):
        server = self.runtime.start_broker(
            "test-token",
            self.socket_path,
            request_timeout_seconds=0.1,
        )
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(1)
                client.connect(str(self.socket_path))
                response = json.loads(client.recv(65_536))
            self.assertEqual(response["code"], "request_timeout")
        finally:
            self.close_server(server)

    def test_connection_limit_rejects_a_slow_second_client(self):
        server = self.runtime.start_broker(
            "test-token",
            self.socket_path,
            max_connections=1,
            request_timeout_seconds=2,
        )
        first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        entered = threading.Event()
        real_peer_credentials = self.runtime._peer_credentials

        def mark_handler(connection):
            entered.set()
            return real_peer_credentials(connection)

        try:
            with mock.patch.object(
                self.runtime,
                "_peer_credentials",
                side_effect=mark_handler,
            ):
                first.connect(str(self.socket_path))
                self.assertTrue(entered.wait(timeout=1))
                with self.assertRaises(self.runtime.NativeContinuationError) as rejected:
                    self.runtime.broker_call({"op": "status"}, self.socket_path)
            self.assertEqual(rejected.exception.code, "broker_busy")
        finally:
            first.close()
            self.close_server(server)


class SlackUploadSecurityIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp.name)
        self.home = root / "home"
        self.home.mkdir(mode=0o700)
        self.approved = self.home / "approved"
        self.approved.mkdir(mode=0o700)
        self.staging = self.home / ".hermes" / "upload-staging"
        self.runtime = load_runtime(
            self.home,
            approved_roots=(self.approved,),
            staging_directory=self.staging,
            max_upload_bytes=1024,
        )

    def tearDown(self):
        self.temp.cleanup()

    def source(self, name="report.txt", content=b"original evidence"):
        path = self.approved / name
        path.write_bytes(content)
        path.chmod(0o600)
        return path

    def upload(self, source, probe, *, filename=""):
        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            side_effect=probe.allocate,
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
            side_effect=probe.upload_bytes,
        ), mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            side_effect=probe.complete,
        ):
            return self.runtime.slack_upload(
                "test-token",
                "C12345678",
                "safe comment",
                str(source),
                "1785000000.000001",
                filename=filename,
            )

    def assert_rejected_before_upload(self, source, *, owner_uid=None):
        probe = UploadProbe()
        owner = (
            mock.patch.object(os, "geteuid", return_value=owner_uid)
            if owner_uid is not None
            else mock.patch.object(os, "geteuid", wraps=os.geteuid)
        )
        with owner, self.assertRaises(
            (SECURITY.UploadSecurityError, ValueError, PermissionError)
        ):
            self.upload(source, probe)
        self.assertEqual(probe.calls, [])

    def test_upload_uses_private_detached_snapshot_and_survives_source_mutation(self):
        source = self.source()
        original = source.stat()

        def mutate_source():
            source.write_bytes(b"attacker replacement")

        probe = UploadProbe(mutate_source)
        self.assertEqual(
            self.upload(source, probe),
            "1785000000.000001",
        )
        self.assertEqual(len(probe.calls), 1)
        observed = probe.calls[0]
        self.assertEqual(observed["content"], b"original evidence")
        self.assertNotEqual(
            (observed["device"], observed["inode"]),
            (original.st_dev, original.st_ino),
        )
        self.assertEqual(observed["owner"], os.geteuid())
        self.assertIn(observed["mode"], {0o400, 0o600})
        self.assertEqual(observed["links"], 1)
        self.assertEqual(observed["parent_mode"], 0o700)

    def test_upload_rejects_symlink_source(self):
        target = self.source("target.txt")
        link = self.approved / "link.txt"
        link.symlink_to(target)
        self.assert_rejected_before_upload(link)

    def test_upload_rejects_source_outside_approved_roots(self):
        outside = self.home / "outside.txt"
        outside.write_bytes(b"outside")
        outside.chmod(0o600)
        self.assert_rejected_before_upload(outside)

    def test_upload_rejects_wrong_owner(self):
        source = self.source()
        self.assert_rejected_before_upload(
            source,
            owner_uid=os.geteuid() + 1,
        )

    def test_upload_rejects_lstat_open_symlink_race(self):
        source = self.source()
        outside = self.home / "outside-secret.txt"
        outside.write_bytes(b"outside secret")
        outside.chmod(0o600)
        original_lstat = SECURITY._lstat_at
        raced = False

        def replace_after_lstat(parent_fd, name):
            nonlocal raced
            result = original_lstat(parent_fd, name)
            if name == source.name and not raced:
                source.unlink()
                source.symlink_to(outside)
                raced = True
            return result

        probe = UploadProbe()
        with mock.patch.object(
            SECURITY,
            "_lstat_at",
            side_effect=replace_after_lstat,
        ), self.assertRaises(
            (SECURITY.UploadSecurityError, ValueError, PermissionError)
        ):
            self.upload(source, probe)
        self.assertTrue(raced)
        self.assertEqual(probe.calls, [])

    def test_upload_rejects_oversized_source(self):
        source = self.source("large.bin", b"")
        source.write_bytes(b"x" * 1025)
        self.assert_rejected_before_upload(source)

    def test_upload_rejects_secret_bearing_file_bytes(self):
        source = self.source(
            "credentials.txt",
            ("api_key=sk-" + "S" * 32).encode(),
        )
        self.assert_rejected_before_upload(source)

    def test_upload_sanitizes_explicit_filename_once_for_both_slack_steps(self):
        source = self.source()
        probe = UploadProbe()
        unsafe_name = "../private\\nested/\n report<>.txt"
        self.upload(source, probe, filename=unsafe_name)
        expected = "report-.txt"
        self.assertEqual(probe.allocated_filename, expected)
        self.assertEqual(probe.calls[0]["kwargs"]["filename"], expected)

    def test_upload_filename_has_a_transport_bound(self):
        source = self.source()
        probe = UploadProbe()
        self.upload(source, probe, filename="a" * 400 + ".txt")
        self.assertEqual(
            len(probe.allocated_filename),
            self.runtime.MAX_SLACK_FILENAME,
        )
        self.assertEqual(
            probe.calls[0]["kwargs"]["filename"],
            probe.allocated_filename,
        )


class SlackEgressSecurityIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.temp.name)
        self.home = root / "home"
        self.home.mkdir(mode=0o700)
        self.approved = self.home / "approved"
        self.approved.mkdir(mode=0o700)
        self.source = self.approved / "report.txt"
        self.source.write_text("report", encoding="utf-8")
        self.source.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def test_text_tool_and_upload_egress_use_central_redaction(self):
        calls = []
        real_redactor = SECURITY.redact_egress_text

        def tracked_redactor(value):
            calls.append(value)
            return real_redactor(value)

        with mock.patch.object(
            SECURITY,
            "redact_egress_text",
            side_effect=tracked_redactor,
        ):
            runtime = load_runtime(
                self.home,
                approved_roots=(self.approved,),
                max_upload_bytes=1024,
            )
            provider_key = "sk-" + "P" * 32
            jwt = "eyJ" + "A" * 12 + "." + "B" * 12 + "." + "C" * 12
            raw_values = (
                provider_key,
                jwt,
                "bearer-secret-value",
                "password-secret-value",
                "private-key-material",
                "url-password",
            )
            tool_payload = json.dumps(
                {
                    "tool": "terminal",
                    "input": {
                        "command": (
                            f"Authorization: Bearer bearer-secret-value "
                            f"password=password-secret-value key={provider_key}"
                        )
                    },
                }
            )
            private_marker = "PRIVATE KEY"
            text = (
                f"{tool_payload}\n{jwt}\n"
                f"-----BEGIN {private_marker}-----\n"
                "private-key-material\n"
                f"-----END {private_marker}-----\n"
                "https://alice:url-password@example.test/path"
            )
            posted = {}

            def slack_call(_token, method, payload):
                if method == "chat.postMessage":
                    posted["text"] = payload["text"]
                    return {"ok": True, "ts": "1785000000.000001"}
                if method == "files.getUploadURLExternal":
                    return {
                        "ok": True,
                        "file_id": "F12345678",
                        "upload_url": (
                            "https://files.slack.com/upload/v1/test-signed-url"
                        ),
                    }
                if method == "files.completeUploadExternal":
                    posted["initial_comment"] = payload["initial_comment"]
                    return {"ok": True, "files": [{"id": "F12345678"}]}
                raise AssertionError(f"unexpected Slack method: {method}")

            probe = UploadProbe()
            with mock.patch.object(
                runtime,
                "_slack_call",
                side_effect=slack_call,
            ), mock.patch.object(
                runtime,
                "_upload_slack_bytes",
                side_effect=probe.upload_bytes,
            ):
                runtime.slack_post("test-token", "C12345678", text)
                runtime.slack_upload(
                    "test-token",
                    "C12345678",
                    text,
                    str(self.source),
                    "1785000000.000001",
                )

        self.assertGreaterEqual(len(calls), 2)
        egress = posted["text"] + "\n" + posted["initial_comment"]
        for raw in raw_values:
            with self.subTest(raw=raw):
                self.assertNotIn(raw, egress)


class NativeExecutableTrustIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.untrusted_temp = tempfile.TemporaryDirectory(
            prefix="untrusted-",
            dir="/tmp",
        )
        self.home = pathlib.Path(self.temp.name) / "home"
        self.home.mkdir(mode=0o700)
        self.runtime = load_runtime(self.home)
        self.proc_root = pathlib.Path(self.temp.name) / "proc"
        boot = self.proc_root / "sys" / "kernel" / "random"
        boot.mkdir(parents=True)
        (boot / "boot_id").write_text(BOOT_ID, encoding="utf-8")

    def tearDown(self):
        self.untrusted_temp.cleanup()
        self.temp.cleanup()

    def test_tmp_executable_with_allowlisted_basename_is_not_native_identity(self):
        executable = pathlib.Path(self.untrusted_temp.name) / "codex"
        executable.write_text("#!/bin/sh\n", encoding="utf-8")
        executable.chmod(0o700)
        process = self.proc_root / "200"
        process.mkdir()
        (process / "environ").write_bytes(
            b"ZELLIJ_SESSION_NAME=work\0ZELLIJ_PANE_ID=7\0"
        )
        (process / "cmdline").write_bytes(
            str(executable).encode() + b"\0exec\0resume\0session\0"
        )
        fields = [
            "S",
            "1",
            "200",
            "200",
            "34823",
            "200",
            *(["0"] * 13),
            "20000",
        ]
        (process / "stat").write_text(
            "200 (codex) " + " ".join(fields),
            encoding="utf-8",
        )
        (process / "exe").symlink_to(executable)

        with self.assertRaises(self.runtime.NativeContinuationError) as rejected:
            self.runtime._zellij_agent_process(
                "work",
                "7",
                {"codex"},
                self.proc_root,
                metadata_agent="codex",
            )
        self.assertIn(
            rejected.exception.code,
            {"process_identity_missing", "process_identity_untrusted"},
        )


if __name__ == "__main__":
    unittest.main()
