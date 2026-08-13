from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import platform
import plistlib
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RECALL_ROOT = Path(__file__).resolve().parents[1]
BUILDER = RECALL_ROOT / "scripts" / "build_macos_package.py"
SPEC = importlib.util.spec_from_file_location("recall_macos_builder", BUILDER)
builder = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)


class MacPackageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.runtime = self.root / "runtime.tar.gz"
        self.runtime_lock = self.root / "RUNTIME_LOCK.json"
        self._write_runtime_fixture()

    def _write_runtime_fixture(self, *, include_license: bool = True) -> None:
        members = {
            "python/bin/python3.12": b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01" + b"\x00" * 24,
            "python/lib/python3.12/__pycache__/site.cpython-312.pyc": b"derived cache fixture",
            "python/lib/python3.12/ssl.py": b"# fixture\n",
            "python/lib/python3.12/sqlite3/__init__.py": b"# fixture\n",
        }
        if include_license:
            members["python/lib/python3.12/LICENSE.txt"] = b"Python fixture license\n"
        with tarfile.open(self.runtime, "w:gz") as archive:
            for name, data in sorted(members.items()):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                info.mode = 0o755 if name.endswith("python3.12") else 0o644
                archive.addfile(info, __import__("io").BytesIO(data))
            link = tarfile.TarInfo("python/bin/python3")
            link.type = tarfile.SYMTYPE
            link.linkname = "python3.12"
            link.mode = 0o777
            archive.addfile(link)
        lock = {
            "schema_version": 1,
            "provider": "test/runtime",
            "release": "immutable-test-release",
            "version": "3.12.13",
            "target": "aarch64-apple-darwin",
            "archive_root": "python",
            "capabilities": {
                "implementation": "CPython",
                "language": {"zip_strict": True},
                "machine": "arm64",
                "stdlib_imports": ["ctypes", "ssl", "sqlite3"],
                "system": "Darwin",
                "sqlite": {"fts5": True},
                "tls": {
                    "default_ca_certificates": "nonempty",
                    "default_verify_path": "existing",
                },
            },
            "artifact": {
                "url": "https://example.invalid/runtime.tar.gz",
                "bytes": self.runtime.stat().st_size,
                "sha256": hashlib.sha256(self.runtime.read_bytes()).hexdigest(),
            },
            "license_paths": ["python/lib/python3.12/LICENSE.txt"],
            "required_paths": [
                "python/bin/python3", "python/bin/python3.12",
                "python/lib/python3.12/LICENSE.txt", "python/lib/python3.12/ssl.py",
                "python/lib/python3.12/sqlite3/__init__.py",
            ],
        }
        self.runtime_lock.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")

    def build(self, name: str, *, check: bool = True) -> Path | subprocess.CompletedProcess[str]:
        output = self.root / name
        result = subprocess.run(
            [
                sys.executable, str(BUILDER), "--source-root", str(RECALL_ROOT),
                "--runtime-archive", str(self.runtime), "--runtime-lock", str(self.runtime_lock),
                "--output", str(output),
            ],
            check=check, text=True, capture_output=True,
        )
        return output if check else result

    @staticmethod
    def _use_host_runtime(package: Path) -> None:
        """Replace the immutable Darwin fixture only for a Linux installer E2E."""

        runtime = package / "runtime"
        shutil.rmtree(runtime)
        (runtime / "bin").mkdir(parents=True)
        os.symlink(sys.executable, runtime / "bin" / "python3")
        lock = json.loads((package / "RUNTIME_LOCK.json").read_text())
        lock["version"] = sys.version.split()[0]
        lock["capabilities"] = {
            "implementation": platform.python_implementation(),
            "language": {"zip_strict": True},
            "machine": platform.machine(),
            "stdlib_imports": ["ctypes", "ssl", "sqlite3"],
            "system": platform.system(),
            "sqlite": {"fts5": True},
            "tls": {
                "default_ca_certificates": "nonempty",
                "default_verify_path": "existing",
            },
        }
        (package / "RUNTIME_LOCK.json").write_text(
            json.dumps(lock, sort_keys=True) + "\n"
        )
        builder.refresh_manifest(package)

    def test_production_runtime_lock_is_immutable_arm64_cpython_312(self) -> None:
        lock = json.loads((RECALL_ROOT / "client" / "macos" / "RUNTIME_LOCK.json").read_text())
        self.assertEqual(lock["version"], "3.12.13")
        self.assertEqual(lock["target"], "aarch64-apple-darwin")
        self.assertEqual(lock["release"], "20260510")
        self.assertEqual(lock["artifact"]["bytes"], 25102827)
        self.assertEqual(lock["artifact"]["sha256"], "5a30271f8d345a5b02b0c9e4e31e0f1e1455a8e4a04fba95cd9762472abc3b17")
        self.assertTrue(lock["artifact"]["url"].startswith("https://github.com/astral-sh/python-build-standalone/releases/download/20260510/"))
        self.assertEqual(lock["capabilities"], {
            "implementation": "CPython",
            "language": {"zip_strict": True},
            "machine": "arm64",
            "stdlib_imports": ["ctypes", "ssl", "sqlite3"],
            "system": "Darwin",
            "sqlite": {"fts5": True},
            "tls": {
                "default_ca_certificates": "nonempty",
                "default_verify_path": "existing",
            },
        })

    def test_tampered_runtime_is_rejected_before_package_write(self) -> None:
        self.runtime.write_bytes(self.runtime.read_bytes() + b"tampered")
        result = self.build("tampered.tar.gz", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("runtime artifact", result.stderr)
        self.assertFalse((self.root / "tampered.tar.gz").exists())

    def test_runtime_download_stops_at_pinned_byte_count(self) -> None:
        lock = json.loads(self.runtime_lock.read_text())
        lock["artifact"]["bytes"] = 4

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        destination = self.root / "oversized-runtime.tar.gz"
        with mock.patch.object(builder.urllib.request, "urlopen", return_value=Response(b"12345")):
            with self.assertRaisesRegex(ValueError, "pinned byte count"):
                builder.download_runtime(lock, destination)
        self.assertLessEqual(destination.stat().st_size, 4)

    def test_wrong_target_and_missing_license_are_rejected(self) -> None:
        lock = json.loads(self.runtime_lock.read_text())
        lock["target"] = "x86_64-apple-darwin"
        self.runtime_lock.write_text(json.dumps(lock))
        wrong_target = self.build("wrong-target.tar.gz", check=False)
        self.assertNotEqual(wrong_target.returncode, 0)
        self.assertIn("aarch64-apple-darwin", wrong_target.stderr)

        self._write_runtime_fixture(include_license=False)
        missing_license = self.build("missing-license.tar.gz", check=False)
        self.assertNotEqual(missing_license.returncode, 0)
        self.assertIn("license", missing_license.stderr.lower())

    def test_reproducible_content_free_package_and_clean_install_uninstall(self) -> None:
        first = self.build("first.tar.gz")
        second = self.build("second.tar.gz")
        self.assertEqual(hashlib.sha256(first.read_bytes()).digest(), hashlib.sha256(second.read_bytes()).digest())
        canary = b"C5_TOKEN_CANARY_DO_NOT_RENDER"
        self.assertNotIn(canary, first.read_bytes())

        extracted = self.root / "extracted"
        extracted.mkdir()
        with tarfile.open(first, "r:gz") as archive:
            names = archive.getnames()
            self.assertFalse(any("__pycache__" in name or name.endswith(".pyc") for name in names))
            archive.extractall(extracted, filter="data")
        package = extracted / "recall-brain-macos"
        manifest = json.loads((package / "MANIFEST.json").read_text())
        self.assertEqual(manifest["format"], "recall-macos-v2")
        self.assertEqual(manifest["runtime"]["version"], "3.12.13")
        self.assertEqual(manifest["runtime"]["target"], "aarch64-apple-darwin")
        packaged_lock = json.loads((package / "RUNTIME_LOCK.json").read_text())
        self.assertEqual(packaged_lock["provider"], "test/runtime")
        self.assertEqual(packaged_lock, json.loads(self.runtime_lock.read_text()))
        self.assertTrue((package / "runtime" / "bin" / "python3").is_symlink())
        self.assertTrue((package / "runtime" / "lib" / "python3.12" / "LICENSE.txt").is_file())
        self.assertNotIn("token", json.dumps(manifest).lower())
        packaged_paths = {entry["path"] for entry in manifest["files"]}
        self.assertIn("lib/connectors/sdk.py", packaged_paths)
        self.assertIn("lib/connectors/__init__.py", packaged_paths)
        self.assertIn("lib/connectors/export_inbox.py", packaged_paths)
        self.assertIn("lib/connectors/cowork_local.py", packaged_paths)
        self.assertIn("lib/connectors/grep_ai.py", packaged_paths)
        self.assertIn("lib/connectors/supervisor.py", packaged_paths)
        self.assertIn("lib/connectors/host.py", packaged_paths)
        self.assertIn("lib/connectors/workspace_rail.py", packaged_paths)
        self.assertIn("lib/contracts/connector_v2.json", packaged_paths)
        self.assertIn("lib/contracts/__init__.py", packaged_paths)
        self.assertIn("lib/contracts/v2.py", packaged_paths)
        self.assertIn("lib/connectors/kit.py", packaged_paths)
        self.assertIn("lib/connectors/conformance.py", packaged_paths)
        self.assertIn("lib/connectors/remote_api.py", packaged_paths)
        self.assertIn("lib/connectors/google_workspace.py", packaged_paths)
        self.assertIn("lib/connectors/imessage.py", packaged_paths)
        self.assertIn("lib/connectors/local_sqlite.py", packaged_paths)
        self.assertIn("lib/connectors/local_file.py", packaged_paths)
        self.assertIn("lib/connectors/local_files.py", packaged_paths)
        self.assertIn("lib/connectors/local_activity.py", packaged_paths)
        self.assertIn("lib/connectors/portable_pim.py", packaged_paths)
        self.assertIn("lib/connectors/portable_archives.py", packaged_paths)
        self.assertIn("lib/connectors/feeds.py", packaged_paths)
        self.assertIn("lib/connectors/selected_jsonl.py", packaged_paths)
        self.assertIn("lib/connectors/managed_auth.py", packaged_paths)
        self.assertIn("lib/connectors/whatsapp_export.py", packaged_paths)
        self.assertIn("lib/contracts/connector_page_v1.json", packaged_paths)
        self.assertIn("lib/client/capture.py", packaged_paths)
        self.assertIn("lib/client/mcp.py", packaged_paths)
        self.assertIn("lib/client/macos_utility.py", packaged_paths)
        self.assertIn("macos_admin/RecallBrainAdmin.swift", packaged_paths)
        self.assertIn("macos_admin/Info.plist", packaged_paths)
        self.assertIn("macos_admin/build.sh", packaged_paths)

        wrapper = (package / "bin" / "recall-brain").read_text()
        cli = (package / "lib" / "client" / "cli.py").read_text()
        self.assertIn('exec "$HERE/runtime/bin/python3" -m client.cli', wrapper)
        self.assertNotIn("exec python3", wrapper)
        installer = (package / "install.sh").read_text()
        subprocess.run(["sh", "-n", str(package / "install.sh")], check=True)
        subprocess.run(["sh", "-n", str(package / "uninstall.sh")], check=True)
        subprocess.run(
            ["sh", "-n", str(package / "macos_admin" / "build.sh")],
            check=True,
        )
        self.assertIn('$PREFIX/runtime/bin/python3', installer)
        self.assertNotRegex(installer, r"(?m)(?:^|[ ;])python3(?:[ ;]|$)")
        self.assertIn('RUNTIME_LOCK.json', installer)
        self.assertIn('ssl.get_default_verify_paths()', installer)
        self.assertIn('get_ca_certs()', installer)
        self.assertIn('cp -R "$SOURCE/lib/connectors"', installer)
        self.assertIn('cp -R "$SOURCE/lib/contracts"', installer)
        self.assertIn('"client.cli", "export-inbox-sync"', installer)
        self.assertIn('"client.cli", "cowork-local-sync"', installer)
        self.assertIn('arguments[3] = "imessage-sync"', installer)
        self.assertIn('arguments[3] = "whatsapp-export-sync"', installer)
        self.assertIn('arguments[3] = "selected-text-sync"', installer)
        self.assertIn('arguments[3] = "browser-sync"', installer)
        self.assertIn('arguments[3] = "apple-notes-sync"', installer)
        self.assertIn('arguments[3] = "hermes-session-sync"', installer)
        self.assertIn('claude-code', installer)
        self.assertIn('cowork', installer)
        self.assertIn('whatsapp|whatsapp-export) NORMALIZED=whatsapp', installer)
        self.assertIn('selected-text|obsidian) NORMALIZED=selected-text', installer)
        self.assertIn('codex|chatgpt-codex-desktop) NORMALIZED=codex', installer)
        self.assertIn('mac-claude-surface-preview', cli)
        self.assertIn('--export-inbox', installer)
        self.assertIn('--disable-export-inbox', installer)
        self.assertIn('--connector-supervisor-config', installer)
        self.assertIn('--reserved-export-inbox "$EXPORT_INBOX"', installer)
        self.assertIn('--disable-connector-supervisor', installer)
        self.assertIn('if [ -n "$SOURCES" ]; then\n  case ",$SOURCES,"', installer)
        self.assertIn('"client.cli", "connector-supervisor-run"', installer)
        self.assertIn('"KeepAlive": True', installer)
        self.assertEqual(installer.count('"Umask": 0o077'), 5)
        self.assertIn('while launchctl print "$TARGET"', installer)
        self.assertIn('launch agent stop did not converge', installer)
        self.assertIn('stop_launch_agent "$LABEL"', installer)
        uninstaller = (package / "uninstall.sh").read_text()
        self.assertIn(
            "claude codex cowork chatgpt-export imessage whatsapp selected-text "
            "safari chrome apple-notes hermes connector-supervisor",
            uninstaller,
        )
        self.assertIn('while launchctl print "$TARGET"', uninstaller)
        self.assertIn('launch agent stop did not converge', uninstaller)
        self.assertIn('--delete-state', uninstaller)
        self.assertIn('"state_retained":true', uninstaller)
        self.assertNotIn('rm -rf "$PREFIX"\necho', uninstaller)
        invalid = subprocess.run([
            "sh", str(package / "install.sh"),
            "--endpoint", "https://example.invalid", "--host-id", "test",
            "--keychain-service", "synthetic", "--visibility", "private",
            "--export-inbox", str(self.root), "--disable-export-inbox", "--no-load",
        ], text=True, capture_output=True)
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("mutually exclusive", invalid.stderr)

        # Installation executes and validates the Mach-O runtime, so that E2E belongs
        # on the real Apple-arm64 target. This unit test pins its generated plist contract.
        self.assertIn('"ProgramArguments": [', installer)
        self.assertIn('"-m", "client.cli", "collect"', installer)
        self.assertIn('"PYTHONPATH":', installer)
        self.assertIn(
            'cp -R "$SOURCE/lib/skills" "$TRANSACTION/stage/lib/skills"',
            installer,
        )

    def test_packaged_local_connectors_install_status_disable_and_uninstall(self) -> None:
        archive = self.build("lifecycle.tar.gz")
        extracted = self.root / "lifecycle-package"
        extracted.mkdir()
        with tarfile.open(archive, "r:gz") as package_archive:
            package_archive.extractall(extracted, filter="data")
        package = extracted / "recall-brain-macos"
        self._use_host_runtime(package)

        prefix = self.root / "private-prefix-PATH-CANARY"
        agents = self.root / "private-agents-PATH-CANARY"
        imessage = self.root / "private-imessage-PATH-CANARY.db"
        whatsapp = self.root / "private-whatsapp-PATH-CANARY.txt"
        selected = self.root / "private-selected-PATH-CANARY"
        safari_history = self.root / "private-safari-history-PATH-CANARY.db"
        safari_bookmarks = self.root / "private-safari-bookmarks-PATH-CANARY.plist"
        chrome_history = self.root / "private-chrome-history-PATH-CANARY.db"
        chrome_bookmarks = self.root / "private-chrome-bookmarks-PATH-CANARY.json"
        notes = self.root / "private-notes-PATH-CANARY.db"
        hermes = self.root / "private-hermes-PATH-CANARY.db"
        imessage.write_bytes(b"synthetic")
        whatsapp.write_text("17/07/2026, 12:00 - Synthetic: fixture\n")
        selected.mkdir()
        (selected / "fixture.md").write_text("synthetic")
        for path in (
            safari_history, safari_bookmarks, chrome_history, chrome_bookmarks,
            notes, hermes,
        ):
            path.write_bytes(b"synthetic")

        installed = subprocess.run([
            "sh", str(package / "install.sh"),
            "--prefix", str(prefix), "--launch-agents", str(agents),
            "--endpoint", "https://example.invalid", "--host-id", "synthetic-host",
            "--keychain-service", "synthetic.reference", "--visibility", "private",
            "--privacy-mode", "scrub",
            "--sources",
            "imessage,whatsapp-export,obsidian,safari,chrome,apple-notes,hermes",
            "--imessage-database", str(imessage),
            "--whatsapp-export", str(whatsapp),
            "--whatsapp-conversation-id", "synthetic-conversation",
            "--whatsapp-owner-name", "Synthetic Owner",
            "--whatsapp-date-order", "dmy", "--whatsapp-timezone", "UTC",
            "--selected-text-root", str(selected), "--no-load",
            "--safari-history", str(safari_history),
            "--safari-bookmarks", str(safari_bookmarks),
            "--chrome-history", str(chrome_history),
            "--chrome-bookmarks", str(chrome_bookmarks),
            "--apple-notes-database", str(notes),
            "--hermes-database", str(hermes),
            "--hermes-sources", "cli,slack",
            "--hermes-roles", "assistant,user",
        ], check=True, text=True, capture_output=True)
        self.assertEqual(installed.stderr, "")
        self.assertNotIn("PATH-CANARY", installed.stdout)

        expected = {
            "imessage": ("imessage-sync", "--database", str(imessage)),
            "whatsapp": ("whatsapp-export-sync", "--export", str(whatsapp)),
            "selected-text": ("selected-text-sync", "--root", str(selected)),
            "safari": ("browser-sync", "--history", str(safari_history)),
            "chrome": ("browser-sync", "--history", str(chrome_history)),
            "apple-notes": ("apple-notes-sync", "--database", str(notes)),
            "hermes": ("hermes-session-sync", "--database", str(hermes)),
        }
        for name, (command, path_option, source_path) in expected.items():
            path = agents / f"ai.parcha.recall.{name}.plist"
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with path.open("rb") as source:
                value = plistlib.load(source)
            arguments = value["ProgramArguments"]
            self.assertEqual(arguments[3], command)
            self.assertEqual(arguments[arguments.index(path_option) + 1], source_path)
            self.assertEqual(arguments[arguments.index("--privacy-mode") + 1], "scrub")
            self.assertNotIn("--token", arguments)
            self.assertEqual(value["Umask"], 0o077)
            self.assertEqual(
                value["EnvironmentVariables"]["RECALL_KEYCHAIN_REFERENCE"],
                "Keychain service/account only",
            )

        wrapper = prefix / "bin" / "recall-brain"
        status = subprocess.run([
            str(wrapper), "mac-status", "--prefix", str(prefix),
            "--launch-agents", str(agents), "--now", "200",
        ], check=True, text=True, capture_output=True)
        rendered = status.stdout + status.stderr
        self.assertNotIn("PATH-CANARY", rendered)
        status_value = json.loads(status.stdout)
        self.assertEqual(status_value["enabled"], 7)
        self.assertTrue(all(
            status_value["sources"][name]["health"] == "starting"
            for name in expected
        ))

        disabled = subprocess.run([
            str(wrapper), "mac-disable", "--source", "whatsapp",
            "--launch-agents", str(agents), "--no-load",
        ], check=True, text=True, capture_output=True)
        self.assertTrue(json.loads(disabled.stdout)["state_retained"])
        self.assertFalse((agents / "ai.parcha.recall.whatsapp.plist").exists())
        self.assertTrue((agents / "ai.parcha.recall.imessage.plist").exists())

        retained = subprocess.run([
            "sh", str(package / "uninstall.sh"), "--prefix", str(prefix),
            "--launch-agents", str(agents), "--no-load",
        ], check=True, text=True, capture_output=True)
        self.assertTrue(json.loads(retained.stdout)["state_retained"])
        self.assertTrue((prefix / "state").is_dir())
        self.assertFalse((prefix / "lib").exists())
        self.assertFalse(any(agents.glob("ai.parcha.recall.*.plist")))

        deleted = subprocess.run([
            "sh", str(package / "uninstall.sh"), "--prefix", str(prefix),
            "--launch-agents", str(agents), "--delete-state", "--no-load",
        ], check=True, text=True, capture_output=True)
        self.assertFalse(json.loads(deleted.stdout)["state_retained"])
        self.assertFalse(prefix.exists())

    def test_four_primary_mac_sources_install_as_independent_bounded_schedules(self) -> None:
        archive = self.build("primary-schedules.tar.gz")
        extracted = self.root / "primary-schedules-package"
        extracted.mkdir()
        with tarfile.open(archive, "r:gz") as package_archive:
            package_archive.extractall(extracted, filter="data")
        package = extracted / "recall-brain-macos"
        self._use_host_runtime(package)
        prefix = self.root / "primary-prefix"
        agents = self.root / "primary-agents"
        claude = self.root / "claude-root"
        codex = self.root / "codex-root"
        codex_archive = self.root / "codex-archive-root"
        cowork = self.root / "cowork-root"
        inbox = self.root / "chatgpt-inbox"
        for path in (claude, codex, cowork, inbox):
            path.mkdir()

        subprocess.run([
            "sh", str(package / "install.sh"),
            "--prefix", str(prefix), "--launch-agents", str(agents),
            "--endpoint", "https://example.invalid", "--host-id", "synthetic-host",
            "--keychain-service", "synthetic.reference", "--visibility", "private",
            "--privacy-mode", "scrub", "--sources", "claude,codex,cowork",
            "--claude-root", str(claude), "--codex-root", str(codex),
            "--codex-archive-root", str(codex_archive),
            "--cowork-root", str(cowork), "--export-inbox", str(inbox),
            "--no-load",
        ], check=True, text=True, capture_output=True)

        expected = {
            "claude": ("collect", "claude:mac:synthetic-host"),
            "codex": ("collect", "codex:mac:synthetic-host"),
            "cowork": ("cowork-local-sync", "cowork:mac:synthetic-host"),
            "chatgpt-export": (
                "export-inbox-sync", "chatgpt-export:mac:synthetic-host",
            ),
        }
        spools = set()
        for label, (command, source_id) in expected.items():
            with (agents / f"ai.parcha.recall.{label}.plist").open("rb") as source:
                value = plistlib.load(source)
            arguments = value["ProgramArguments"]
            self.assertEqual(arguments[3], command)
            self.assertEqual(
                arguments[arguments.index("--source-id") + 1],
                source_id,
            )
            spool = arguments[arguments.index("--spool") + 1]
            self.assertNotIn(spool, spools)
            spools.add(spool)
            self.assertTrue(value["RunAtLoad"])
            self.assertEqual(value["StartInterval"], 30)
            if command == "collect":
                self.assertEqual(
                    arguments[arguments.index("--max-scan-records") + 1],
                    "1000",
                )
                self.assertEqual(
                    arguments[arguments.index("--max-scan-seconds") + 1],
                    "20",
                )
            if label == "codex":
                self.assertEqual(
                    arguments[arguments.index("--archive-root") + 1],
                    str(codex_archive),
                )
                self.assertTrue(codex_archive.is_dir())
            elif command == "collect":
                self.assertNotIn("--archive-root", arguments)

        status = subprocess.run([
            str(prefix / "bin" / "recall-brain"), "mac-status",
            "--prefix", str(prefix), "--launch-agents", str(agents),
            "--now", "200",
        ], check=True, text=True, capture_output=True)
        value = json.loads(status.stdout)
        self.assertEqual(value["enabled"], 4)
        self.assertTrue(all(
            value["sources"][name]["health"] == "starting"
            for name in ("claude-code", "codex", "cowork", "chatgpt-export")
        ))

    def test_packaged_cli_resolves_the_installed_root_through_a_symlink(self) -> None:
        archive = self.build("symlink-entrypoint.tar.gz")
        extracted = self.root / "symlink-entrypoint-package"
        extracted.mkdir()
        with tarfile.open(archive, "r:gz") as package_archive:
            package_archive.extractall(extracted, filter="data")
        package = extracted / "recall-brain-macos"
        self._use_host_runtime(package)

        user_bin = self.root / "user-bin"
        user_bin.mkdir()
        entrypoint = user_bin / "recall-brain"
        os.symlink(package / "bin" / "recall-brain", entrypoint)

        result = subprocess.run(
            [str(entrypoint), "mac-route-info", "--help"],
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--source", result.stdout)

    def test_install_verifies_bundle_restores_failed_upgrade_and_supports_rollback(self) -> None:
        archive = self.build("upgrade.tar.gz")
        extracted = self.root / "upgrade-package"
        extracted.mkdir()
        with tarfile.open(archive, "r:gz") as package_archive:
            package_archive.extractall(extracted, filter="data")
        package = extracted / "recall-brain-macos"
        self._use_host_runtime(package)
        prefix = self.root / "upgrade-prefix"
        agents = self.root / "upgrade-agents"
        selected = self.root / "selected"
        selected.mkdir()
        (selected / "fixture.md").write_text("synthetic")
        install = [
            "sh", str(package / "install.sh"),
            "--prefix", str(prefix), "--launch-agents", str(agents),
            "--endpoint", "https://example.invalid", "--host-id", "synthetic-host",
            "--keychain-service", "synthetic.reference", "--visibility", "private",
            "--privacy-mode", "scrub", "--sources", "selected-text",
            "--selected-text-root", str(selected), "--no-load",
        ]
        subprocess.run(install, check=True, text=True, capture_output=True)
        wrapper = prefix / "bin" / "recall-brain"
        subprocess.run([
            str(wrapper), "mac-route-apply", "--source", "selected-text",
            "--tenant-id", "tenant:personal", "--principal-id", "owner",
            "--launch-agents", str(agents),
        ], check=True, text=True, capture_output=True)
        marker = prefix / "lib" / "previous-release-marker"
        marker.write_text("previous")
        plist_before = (
            agents / "ai.parcha.recall.selected-text.plist"
        ).read_bytes()

        tampered = package / "lib" / "client" / "cli.py"
        original = tampered.read_bytes()
        tampered.write_bytes(original + b"\n# tampered\n")
        rejected = subprocess.run(install, text=True, capture_output=True)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("package_integrity_failed", rejected.stderr)
        self.assertEqual(marker.read_text(), "previous")
        self.assertEqual(
            (agents / "ai.parcha.recall.selected-text.plist").read_bytes(),
            plist_before,
        )
        tampered.write_bytes(original)

        invalid_supervisor = self.root / "invalid-supervisor.json"
        invalid_supervisor.write_text("{")
        failed_upgrade = subprocess.run([
            "sh", str(package / "install.sh"),
            "--prefix", str(prefix), "--launch-agents", str(agents),
            "--connector-supervisor-config", str(invalid_supervisor), "--no-load",
        ], text=True, capture_output=True)
        self.assertNotEqual(failed_upgrade.returncode, 0)
        self.assertEqual(marker.read_text(), "previous")
        self.assertEqual(
            (agents / "ai.parcha.recall.selected-text.plist").read_bytes(),
            plist_before,
        )

        subprocess.run(install, check=True, text=True, capture_output=True)
        self.assertFalse(marker.exists())
        with (agents / "ai.parcha.recall.selected-text.plist").open("rb") as source:
            upgraded_plist = plistlib.load(source)
        self.assertEqual(
            upgraded_plist["EnvironmentVariables"]["RECALL_TENANT_ID"],
            "tenant:personal",
        )
        self.assertEqual(
            upgraded_plist["EnvironmentVariables"]["RECALL_PRINCIPAL_ID"],
            "owner",
        )
        self.assertEqual(
            upgraded_plist["EnvironmentVariables"]["RECALL_CANONICAL_V2_ENABLED"],
            "1",
        )
        rolled_back = subprocess.run([
            "sh", str(package / "install.sh"), "--rollback",
            "--prefix", str(prefix), "--launch-agents", str(agents), "--no-load",
        ], check=True, text=True, capture_output=True)
        self.assertEqual(json.loads(rolled_back.stdout), {
            "schema_version": 1, "mode": "mac-rollback",
            "restored": True, "state_retained": True,
        })
        self.assertEqual(marker.read_text(), "previous")
        self.assertEqual(
            (agents / "ai.parcha.recall.selected-text.plist").read_bytes(),
            plist_before,
        )


if __name__ == "__main__":
    unittest.main()
