from __future__ import annotations

import hashlib
import importlib.util
import io
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from connectors.workspace_rail import GWS_RELEASE, GwsRelease


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "install_gws.py"
SPEC = importlib.util.spec_from_file_location("recall_install_gws", SCRIPT)
installer = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(installer)


def archive(*, symlink: bool = False) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as output:
        for name, data in (
            ("CHANGELOG.md", b"synthetic changelog"),
            ("LICENSE", b"synthetic license"),
            ("README.md", b"synthetic readme"),
            ("gws", b"synthetic binary"),
        ):
            member = tarfile.TarInfo("./" + name)
            if symlink and name == "gws":
                member.type = tarfile.SYMTYPE
                member.linkname = "/untrusted"
                output.addfile(member)
            else:
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
    return raw.getvalue()


def release(data: bytes) -> GwsRelease:
    return GwsRelease(
        version=GWS_RELEASE.version,
        tag_commit=GWS_RELEASE.tag_commit,
        sha256={"synthetic": hashlib.sha256(data).hexdigest()},
        bytes={"synthetic": len(data)},
    )


class WorkspaceReleaseTest(unittest.TestCase):
    def test_verified_archive_installs_atomically_with_license(self) -> None:
        data = archive()
        payloads = installer.verified_payloads(data, "synthetic", release(data))
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / GWS_RELEASE.version
            result = installer.install(payloads, prefix)
            self.assertEqual(result["status"], "installed")
            self.assertEqual((prefix / "gws").read_bytes(), b"synthetic binary")
            self.assertTrue((prefix / "gws").stat().st_mode & 0o100)
            self.assertEqual((prefix / "LICENSE").read_bytes(), b"synthetic license")
            self.assertEqual(installer.install(payloads, prefix)["status"], "already_installed")
            (prefix / "LICENSE").unlink()
            with self.assertRaisesRegex(ValueError, "already exists"):
                installer.install(payloads, prefix)

    def test_checksum_shape_and_symlink_fail_before_install(self) -> None:
        data = archive()
        with self.assertRaisesRegex(ValueError, "verification"):
            installer.verified_payloads(data + b"tampered", "synthetic", release(data))
        linked = archive(symlink=True)
        with self.assertRaisesRegex(ValueError, "member"):
            installer.verified_payloads(linked, "synthetic", release(linked))

    def test_download_is_byte_bounded_and_rejects_redirect_host(self) -> None:
        data = b"1234"
        pinned = release(data)

        class Response(io.BytesIO):
            def __init__(self, value: bytes, url: str):
                super().__init__(value); self.url = url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def geturl(self):
                return self.url

        approved = Response(data, "https://release-assets.githubusercontent.com/synthetic")
        with mock.patch.object(installer.urllib.request, "urlopen", return_value=approved):
            self.assertEqual(installer.download("synthetic", pinned), data)
        rejected = Response(data, "https://example.invalid/synthetic")
        with mock.patch.object(installer.urllib.request, "urlopen", return_value=rejected):
            with self.assertRaisesRegex(ValueError, "unapproved"):
                installer.download("synthetic", pinned)

    def test_download_retries_only_transient_transport_failures(self) -> None:
        data = b"1234"
        pinned = release(data)

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def geturl(self):
                return "https://release-assets.githubusercontent.com/synthetic"

        with (
            mock.patch.object(
                installer.urllib.request,
                "urlopen",
                side_effect=[OSError("transient"), OSError("transient"), Response(data)],
            ) as opened,
            mock.patch.object(installer.time, "sleep") as slept,
        ):
            self.assertEqual(installer.download("synthetic", pinned), data)
        self.assertEqual(opened.call_count, 3)
        self.assertEqual([call.args[0] for call in slept.call_args_list], [1, 2])


if __name__ == "__main__":
    unittest.main()
