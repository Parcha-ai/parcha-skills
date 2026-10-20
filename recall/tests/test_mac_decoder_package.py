from __future__ import annotations

import hashlib
import io
import json
import unittest
import zipfile
from pathlib import Path

from scripts import build_macos_package as builder


class DecoderPackageTest(unittest.TestCase):
    def wheel(self, *, extension=None, extra=None):
        root = "backports_zstd-1.0.0.dist-info/licenses/"
        entries = {
            root + "LICENSE.txt": b"synthetic package license",
            root + "LICENSE_zstd.txt": b"synthetic zstd license",
            "backports/zstd/__init__.py": b"# synthetic import\n",
            "backports/zstd/_zstd.cpython-312-darwin.so": (
                extension if extension is not None else
                builder.MACHO_64_LITTLE_ENDIAN + builder.CPU_TYPE_ARM64_LITTLE_ENDIAN
            ),
        }
        entries.update(extra or {})
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            for name, body in entries.items():
                archive.writestr(name, body)
        wheel = data.getvalue()
        lock = {
            "schema_version": 1, "package": "backports.zstd", "version": "1.0.0",
            "target": "cp312-cp312-macosx_11_0_arm64",
            "artifact": {"bytes": len(wheel), "sha256": hashlib.sha256(wheel).hexdigest()},
            "license_paths": [root + "LICENSE.txt", root + "LICENSE_zstd.txt"],
        }
        return wheel, json.dumps(lock).encode()

    def test_pinned_decoder_and_licenses_are_manifested_under_runtime(self):
        wheel, lock = self.wheel()
        payloads = builder.decoder_payloads(wheel, lock)
        self.assertIn(
            "runtime/lib/python3.12/site-packages/backports/zstd/_zstd.cpython-312-darwin.so",
            payloads,
        )
        self.assertEqual(payloads["lib/collector/DECODER_LOCK.json"].data, lock)
        runtime_lock = json.loads(
            (Path(__file__).parents[1] / "client/macos/RUNTIME_LOCK.json").read_bytes(),
        )
        manifest = json.loads(builder.manifest(payloads, runtime_lock))
        entries = {item["path"]: item for item in manifest["files"]}
        for path, payload in payloads.items():
            self.assertEqual(entries[path]["sha256"], hashlib.sha256(payload.data).hexdigest())

    def test_wrong_bytes_target_and_extension_are_refused(self):
        wheel, lock = self.wheel()
        with self.assertRaises(ValueError):
            builder.decoder_payloads(wheel[:-1], lock)
        changed = json.loads(lock)
        changed["target"] = "cp312-cp312-manylinux_x86_64"
        with self.assertRaises(ValueError):
            builder.decoder_payloads(wheel, json.dumps(changed).encode())
        wrong_wheel, wrong_lock = self.wheel(extension=b"ELF is not Mach-O")
        with self.assertRaises(ValueError):
            builder.decoder_payloads(wrong_wheel, wrong_lock)

    def test_wheel_cannot_escape_runtime_namespace(self):
        for path in ("../escape", "/escape", "backports/../../escape",
                     "backports/./alias.py", "unrelated.py"):
            with self.subTest(path=path):
                wheel, lock = self.wheel(extra={path: b"bad"})
                with self.assertRaises(ValueError):
                    builder.decoder_payloads(wheel, lock)
