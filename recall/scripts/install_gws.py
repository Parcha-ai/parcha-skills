#!/usr/bin/env python3
"""Install the checksum-pinned gws release into an immutable version directory."""

from __future__ import annotations

import argparse
import hashlib
import io
import os
import stat
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.workspace_rail import GWS_RELEASE, GwsRelease


ALLOWED_MEMBERS = {"CHANGELOG.md", "LICENSE", "README.md", "gws"}
ALLOWED_DOWNLOAD_HOSTS = {"github.com", "release-assets.githubusercontent.com"}
DOWNLOAD_ATTEMPTS = 4


def release_url(target: str, release: GwsRelease = GWS_RELEASE) -> str:
    if target not in release.sha256 or target not in release.bytes:
        raise ValueError("unsupported gws target")
    name = f"google-workspace-cli-{target}.tar.gz"
    return f"https://github.com/googleworkspace/cli/releases/download/v{release.version}/{name}"


def download(target: str, release: GwsRelease = GWS_RELEASE) -> bytes:
    expected = release.bytes.get(target)
    if expected is None:
        raise ValueError("unsupported gws target")
    request = urllib.request.Request(
        release_url(target, release), headers={"User-Agent": "recall-gws-installer/1"},
    )
    for attempt in range(DOWNLOAD_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                final = urllib.parse.urlparse(response.geturl())
                if final.scheme != "https" or final.hostname not in ALLOWED_DOWNLOAD_HOSTS:
                    raise ValueError("gws release redirected to an unapproved host")
                data = response.read(expected + 1)
            break
        except OSError:
            if attempt + 1 == DOWNLOAD_ATTEMPTS:
                raise
            time.sleep(2**attempt)
    if len(data) != expected:
        raise ValueError("gws release byte count mismatch")
    return data


def verified_payloads(data: bytes, target: str,
                      release: GwsRelease = GWS_RELEASE) -> dict[str, bytes]:
    expected_bytes = release.bytes.get(target)
    expected_sha = release.sha256.get(target)
    if expected_bytes is None or expected_sha is None:
        raise ValueError("unsupported gws target")
    if len(data) != expected_bytes or hashlib.sha256(data).hexdigest() != expected_sha:
        raise ValueError("gws release verification failed")
    result: dict[str, bytes] = {}
    seen: set[str] = set()
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive:
                normalized = str(PurePosixPath(member.name)).removeprefix("./")
                if normalized in {"", "."} and member.isdir():
                    continue
                if normalized not in ALLOWED_MEMBERS or normalized in seen:
                    raise ValueError("gws release archive shape is invalid")
                seen.add(normalized)
                if not member.isfile() or member.issym() or member.islnk():
                    raise ValueError("gws release archive member is invalid")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError("gws release archive member is unreadable")
                result[normalized] = source.read()
    except (tarfile.TarError, OSError) as error:
        raise ValueError("gws release archive is invalid") from error
    if set(result) != ALLOWED_MEMBERS or not result["gws"] or not result["LICENSE"]:
        raise ValueError("gws release archive is incomplete")
    return result


def install(payloads: dict[str, bytes], prefix: Path) -> dict[str, Any]:
    prefix = Path(prefix)
    if not prefix.is_absolute() or prefix.name != GWS_RELEASE.version:
        raise ValueError("gws install prefix must be the pinned absolute version directory")
    if prefix.exists():
        matches = all(
            (prefix / name).is_file()
            and hashlib.sha256((prefix / name).read_bytes()).digest() == hashlib.sha256(data).digest()
            for name, data in payloads.items()
        )
        if matches:
            return {"status": "already_installed", "version": GWS_RELEASE.version}
        raise ValueError("gws install prefix already exists")
    prefix.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    temporary = Path(tempfile.mkdtemp(prefix=".gws-install-", dir=prefix.parent))
    try:
        for name, data in payloads.items():
            path = temporary / name
            path.write_bytes(data)
            os.chmod(path, 0o755 if name == "gws" else 0o644)
        temporary.replace(prefix)
    except Exception:
        for child in temporary.iterdir():
            child.unlink()
        temporary.rmdir()
        raise
    installed = prefix / "gws"
    if not stat.S_ISREG(installed.stat().st_mode) or not os.access(installed, os.X_OK):
        raise ValueError("gws binary installation failed")
    return {"status": "installed", "version": GWS_RELEASE.version}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=tuple(sorted(GWS_RELEASE.sha256)), required=True)
    parser.add_argument("--prefix", type=Path, default=Path(f"/opt/recall/vendor/gws/{GWS_RELEASE.version}"))
    parser.add_argument("--archive", type=Path)
    args = parser.parse_args()
    data = args.archive.read_bytes() if args.archive else download(args.target)
    result = install(verified_payloads(data, args.target), args.prefix)
    print(f"{result['status']} gws {result['version']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
