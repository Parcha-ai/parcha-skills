from __future__ import annotations

import hashlib
import importlib.util
import os
import pathlib
import stat
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
SECURITY_PATH = ROOT / "runtime" / "security.py"


def load_security():
    spec = importlib.util.spec_from_file_location("tether_security_test", SECURITY_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("security module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


security = load_security()


class PrivateStatePathTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.uid = os.geteuid()

    def tearDown(self):
        self.temp.cleanup()

    def test_directory_and_file_modes_are_enforced(self):
        directory = self.root / "state"
        directory.mkdir(mode=0o755)
        state_file = directory / "state.db"
        state_file.write_text("state", encoding="utf-8")
        state_file.chmod(0o644)

        self.assertEqual(
            security.secure_state_directory(directory),
            directory,
        )
        self.assertEqual(security.secure_state_file(state_file), state_file)
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(state_file.stat().st_mode), 0o600)

    def test_missing_private_paths_are_created_with_private_modes(self):
        directory = self.root / "new-state"
        state_file = directory / "token"
        security.secure_state_directory(directory, create=True)
        security.secure_state_file(state_file, create=True)
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(state_file.stat().st_mode), 0o600)

    def test_leaf_and_intermediate_symlinks_are_rejected(self):
        real = self.root / "real"
        real.mkdir()
        leaf_link = self.root / "state-link"
        leaf_link.symlink_to(real, target_is_directory=True)
        with self.assertRaises(security.StatePathError):
            security.secure_state_directory(leaf_link)

        intermediate = self.root / "intermediate-link"
        intermediate.symlink_to(real, target_is_directory=True)
        (real / "state.db").write_text("x", encoding="utf-8")
        with self.assertRaises((security.StatePathError, OSError)):
            security.secure_state_file(intermediate / "state.db")

    def test_wrong_owner_is_rejected_before_mode_repair(self):
        directory = self.root / "state"
        directory.mkdir(mode=0o755)
        directory.chmod(0o755)
        with self.assertRaises(security.StatePathError):
            security.secure_state_directory(
                directory,
                owner_uid=self.uid + 1,
            )
        self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o755)

        state_file = self.root / "state.db"
        state_file.write_text("state", encoding="utf-8")
        state_file.chmod(0o644)
        with self.assertRaises(security.StatePathError):
            security.secure_state_file(
                state_file,
                owner_uid=self.uid + 1,
            )
        self.assertEqual(stat.S_IMODE(state_file.stat().st_mode), 0o644)

    def test_file_symlink_swap_during_validation_is_rejected(self):
        state_file = self.root / "state.db"
        state_file.write_text("state", encoding="utf-8")
        target = self.root / "target"
        target.write_text("target", encoding="utf-8")
        original_lstat = security._lstat_at
        raced = False

        def replace_after_lstat(parent_fd, name):
            nonlocal raced
            result = original_lstat(parent_fd, name)
            if name == state_file.name and not raced:
                state_file.unlink()
                state_file.symlink_to(target)
                raced = True
            return result

        with mock.patch.object(
            security,
            "_lstat_at",
            side_effect=replace_after_lstat,
        ):
            with self.assertRaises(security.StatePathError):
                security.secure_state_file(state_file)
        self.assertTrue(raced)
        self.assertEqual(target.read_text(encoding="utf-8"), "target")

    def test_relative_and_traversal_paths_are_rejected(self):
        with self.assertRaises(security.StatePathError):
            security.secure_state_directory("relative/state")
        with self.assertRaises(security.StatePathError):
            security.secure_state_directory(self.root / ".." / "escape")

    def test_private_executable_rejects_writable_ancestor(self):
        writable = self.root / "writable"
        writable.mkdir()
        writable.chmod(0o777)
        executable = writable / "helper"
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        executable.chmod(0o700)

        with self.assertRaisesRegex(
            security.StatePathError,
            "writable ancestor",
        ):
            security.validate_private_executable(executable)

        writable.chmod(0o700)
        self.assertEqual(
            security.validate_private_executable(executable),
            executable,
        )


class UploadStagingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.approved = self.root / "approved"
        self.approved.mkdir(mode=0o700)
        self.staging = self.root / "staging"
        self.uid = os.geteuid()

    def tearDown(self):
        self.temp.cleanup()

    def write_source(self, name: str = "report.txt", content: bytes = b"safe report"):
        source = self.approved / name
        source.write_bytes(content)
        return source

    def test_regular_file_is_copied_into_private_verified_snapshot(self):
        source = self.write_source(content=b"evidence")
        staged = security.stage_upload(
            source,
            approved_roots=[self.approved],
            staging_directory=self.staging,
            max_bytes=1024,
        )
        self.assertNotEqual(staged.path, source)
        self.assertEqual(staged.path.read_bytes(), b"evidence")
        self.assertEqual(staged.size, 8)
        self.assertEqual(
            staged.sha256,
            hashlib.sha256(b"evidence").hexdigest(),
        )
        self.assertEqual(stat.S_IMODE(self.staging.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(staged.path.stat().st_mode), 0o400)
        self.assertEqual(staged.path.stat().st_nlink, 1)
        staged.verify()
        descriptor = staged.open_verified()
        try:
            staged.path.unlink()
            staged.path.write_bytes(b"replacement")
            self.assertEqual(os.read(descriptor, 1024), b"evidence")
        finally:
            os.close(descriptor)

    def test_non_private_approved_root_is_rejected(self):
        source = self.write_source()
        self.approved.chmod(0o755)
        with self.assertRaisesRegex(
            security.UploadSecurityError,
            "owner-private",
        ):
            security.stage_upload(
                source,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

    def test_filesystem_root_cannot_be_approved(self):
        source = self.write_source()
        with self.assertRaisesRegex(
            security.UploadSecurityError,
            "filesystem root",
        ):
            security.stage_upload(
                source,
                approved_roots=[pathlib.Path("/")],
                staging_directory=self.staging,
            )

    def test_writable_non_sticky_ancestor_is_rejected(self):
        writable = self.root / "writable"
        writable.mkdir(mode=0o777)
        writable.chmod(0o777)
        approved = writable / "private"
        approved.mkdir(mode=0o700)
        source = approved / "report.txt"
        source.write_text("safe", encoding="utf-8")
        with self.assertRaisesRegex(
            security.UploadSecurityError,
            "writable ancestor",
        ):
            security.stage_upload(
                source,
                approved_roots=[approved],
                staging_directory=self.staging,
            )

    def test_path_escape_and_symlinked_root_are_rejected(self):
        outside = self.root / "outside.txt"
        outside.write_text("outside", encoding="utf-8")
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                outside,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                self.approved / ".." / outside.name,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

        root_link = self.root / "approved-link"
        root_link.symlink_to(self.approved, target_is_directory=True)
        source = self.write_source()
        linked_source = root_link / source.name
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                linked_source,
                approved_roots=[root_link],
                staging_directory=self.staging,
            )

    def test_symlink_source_and_lstat_open_race_are_rejected(self):
        secret = self.root / "secret"
        secret.write_text("do not upload", encoding="utf-8")
        source = self.approved / "report"
        source.symlink_to(secret)
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                source,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

        source.unlink()
        source.write_text("safe", encoding="utf-8")
        original_lstat = security._lstat_at
        raced = False

        def replace_after_lstat(parent_fd, name):
            nonlocal raced
            result = original_lstat(parent_fd, name)
            if name == source.name and not raced:
                source.unlink()
                source.symlink_to(secret)
                raced = True
            return result

        with mock.patch.object(
            security,
            "_lstat_at",
            side_effect=replace_after_lstat,
        ):
            with self.assertRaises(security.UploadSecurityError):
                security.stage_upload(
                    source,
                    approved_roots=[self.approved],
                    staging_directory=self.staging,
                )
        self.assertTrue(raced)
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_oversize_fifo_and_hardlink_are_rejected(self):
        oversized = self.write_source("large", b"x" * 33)
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                oversized,
                approved_roots=[self.approved],
                staging_directory=self.staging,
                max_bytes=32,
            )

        fifo = self.approved / "pipe"
        os.mkfifo(fifo)
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                fifo,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

        directory = self.approved / "directory"
        directory.mkdir()
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                directory,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

        original = self.write_source("original", b"content")
        hardlink = self.approved / "hardlink"
        os.link(original, hardlink)
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                hardlink,
                approved_roots=[self.approved],
                staging_directory=self.staging,
            )

    def test_wrong_owner_and_staged_inode_tampering_are_rejected(self):
        source = self.write_source()
        with self.assertRaises(security.UploadSecurityError):
            security.stage_upload(
                source,
                approved_roots=[self.approved],
                staging_directory=self.staging,
                owner_uid=self.uid + 1,
            )

        staged = security.stage_upload(
            source,
            approved_roots=[self.approved],
            staging_directory=self.staging,
        )
        staged.path.unlink()
        staged.path.write_bytes(b"replacement")
        staged.path.chmod(0o400)
        with self.assertRaises(security.UploadSecurityError):
            staged.verify()


class EgressRedactionTest(unittest.TestCase):
    def test_provider_keys_jwt_bearer_and_assignments_are_redacted(self):
        values = {
            "slack": "xo" + "xb-" + "A" * 24,
            "github": "github_pat_" + "B" * 30,
            "openai": "sk-proj-" + "C" * 30,
            "anthropic": "sk-ant-" + "D" * 30,
            "google": "AIza" + "E" * 35,
            "aws": "AKIA" + "F" * 16,
            "huggingface": "hf_" + "G" * 30,
            "jwt": "eyJ" + "H" * 16 + "." + "I" * 16 + "." + "J" * 16,
            "bearer": "Bearer " + "K" * 32,
            "password": 'password="correct horse battery staple"',
            "assignment": "CLIENT_SECRET=" + "L" * 24,
        }
        source = "\n".join(f"{key}: {value}" for key, value in values.items())
        result = security.redact_egress(source)
        for value in values.values():
            secret_value = value.split("=", 1)[-1].strip('"')
            if value.startswith("Bearer "):
                secret_value = value.removeprefix("Bearer ")
            self.assertNotIn(secret_value, result.text)
        self.assertGreaterEqual(result.redaction_count, len(values))
        self.assertNotIn("correct horse battery staple", result.text)

    def test_private_keys_and_credentialed_urls_are_redacted(self):
        marker = "OPENSSH PRIVATE KEY"
        private_key = (
            f"-----BEGIN {marker}-----\n"
            "sensitive-material\n"
            f"-----END {marker}-----"
        )
        source = (
            f"{private_key}\n"
            "postgres://alice:supersecret@db.internal:5432/app\n"
            "https://api.example.test/v1?token=query-secret&safe=yes"
        )
        redacted = security.redact_egress_text(source)
        self.assertNotIn("sensitive-material", redacted)
        self.assertNotIn("alice", redacted)
        self.assertNotIn("supersecret", redacted)
        self.assertNotIn("query-secret", redacted)
        self.assertIn("db.internal:5432", redacted)
        self.assertIn("safe=yes", redacted)

        truncated = (
            "-----BEGIN PRIVATE KEY-----\n"
            "truncated-sensitive-material"
        )
        self.assertNotIn(
            "truncated-sensitive-material",
            security.redact_egress_text(truncated),
        )

    def test_nonsecret_text_is_preserved(self):
        source = "Build 42 passed. See https://example.test/report."
        result = security.redact_egress(source)
        self.assertEqual(result.text, source)
        self.assertEqual(result.redaction_count, 0)

    def test_nested_json_redacts_snake_kebab_and_camel_case_secret_fields(self):
        payload = {
            "api_key": "opaque-one",
            "client-secret": "opaque-two",
            "accessToken": "opaque-three",
            "privateKeyPem": "opaque-four",
            "safeLabel": "preserved",
        }

        result = security.redact_egress_json(payload)

        for key in ("api_key", "client-secret", "accessToken", "privateKeyPem"):
            self.assertEqual(result[key], security.REDACTED)
        self.assertEqual(result["safeLabel"], "preserved")

if __name__ == "__main__":
    unittest.main()
