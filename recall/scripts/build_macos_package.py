#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import posixpath
import stat
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


EPOCH = 0
EXPECTED_TARGET = "aarch64-apple-darwin"
MACHO_64_LITTLE_ENDIAN = b"\xcf\xfa\xed\xfe"
CPU_TYPE_ARM64_LITTLE_ENDIAN = b"\x0c\x00\x00\x01"
EXPECTED_CAPABILITIES = {
    "implementation": "CPython",
    "language": {"zip_strict": True},
    "machine": "arm64",
    "stdlib_imports": ["ctypes", "ssl", "sqlite3"],
    "system": "Darwin",
    "sqlite": {"fts5": True},
    "tls": {"default_ca_certificates": "nonempty", "default_verify_path": "existing"},
}


@dataclass(frozen=True)
class Payload:
    data: bytes | None = None
    linkname: str | None = None
    executable: bool = False


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_lock(path: Path) -> dict[str, Any]:
    try:
        lock = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid runtime lock: {exc}") from exc
    if lock.get("schema_version") != 1:
        raise ValueError("runtime lock schema_version must be 1")
    if lock.get("target") != EXPECTED_TARGET:
        raise ValueError(f"runtime target must be {EXPECTED_TARGET}")
    if not str(lock.get("version", "")).startswith("3.12."):
        raise ValueError("runtime version must be an exact CPython 3.12 patch release")
    artifact = lock.get("artifact", {})
    if not str(artifact.get("url", "")).startswith("https://"):
        raise ValueError("runtime artifact URL must use https")
    if not isinstance(artifact.get("bytes"), int) or artifact["bytes"] <= 0:
        raise ValueError("runtime artifact byte count must be positive")
    digest = artifact.get("sha256", "")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("runtime artifact SHA-256 must be lowercase hexadecimal")
    if lock.get("archive_root") != "python":
        raise ValueError("runtime archive_root must be python")
    if lock.get("capabilities") != EXPECTED_CAPABILITIES:
        raise ValueError("runtime capability contract is missing or unsupported")
    if not lock.get("license_paths") or not lock.get("required_paths"):
        raise ValueError("runtime lock must name license and required paths")
    return lock


def verify_artifact(data: bytes, lock: dict[str, Any]) -> None:
    expected = lock["artifact"]
    if len(data) != expected["bytes"]:
        raise ValueError(f"runtime artifact byte count mismatch: expected {expected['bytes']}, got {len(data)}")
    actual = sha256(data)
    if actual != expected["sha256"]:
        raise ValueError(f"runtime artifact SHA-256 mismatch: expected {expected['sha256']}, got {actual}")


def safe_member_name(name: str, root: str) -> bool:
    path = PurePosixPath(name)
    return bool(name) and not path.is_absolute() and ".." not in path.parts and path.parts[0] == root


def safe_link(name: str, target: str, root: str) -> bool:
    if not target or PurePosixPath(target).is_absolute():
        return False
    resolved = posixpath.normpath(posixpath.join(posixpath.dirname(name), target))
    return resolved == root or resolved.startswith(root + "/")


def runtime_payloads(archive_data: bytes, lock: dict[str, Any]) -> dict[str, Payload]:
    root = lock["archive_root"]
    payloads: dict[str, Payload] = {}
    source_names: set[str] = set()
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_data), mode="r:gz") as archive:
            for member in archive:
                name = member.name.rstrip("/")
                if not safe_member_name(name, root):
                    raise ValueError(f"unsafe runtime archive path: {member.name}")
                path = PurePosixPath(name)
                if "__pycache__" in path.parts or path.suffix == ".pyc":
                    continue
                if name in source_names:
                    raise ValueError(f"duplicate runtime archive path: {name}")
                source_names.add(name)
                destination = "runtime" + name[len(root):]
                if member.isdir():
                    continue
                if member.issym():
                    if not safe_link(name, member.linkname, root):
                        raise ValueError(f"unsafe runtime symlink: {name} -> {member.linkname}")
                    payloads[destination] = Payload(linkname=member.linkname)
                    continue
                if not member.isfile():
                    raise ValueError(f"unsupported runtime archive member type: {name}")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"unreadable runtime archive member: {name}")
                payloads[destination] = Payload(
                    data=source.read(), executable=bool(member.mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)),
                )
    except (tarfile.TarError, OSError) as exc:
        raise ValueError(f"invalid runtime artifact archive: {exc}") from exc

    missing = sorted(set(lock["required_paths"]) - source_names)
    if missing:
        raise ValueError("runtime artifact missing required paths: " + ", ".join(missing))
    missing_licenses = sorted(path for path in lock["license_paths"] if path not in source_names)
    if missing_licenses:
        raise ValueError("runtime artifact missing license paths: " + ", ".join(missing_licenses))
    for path in lock["license_paths"]:
        item = payloads.get("runtime" + path[len(root):])
        if item is None or not item.data:
            raise ValueError(f"runtime license is empty or not a regular file: {path}")

    executable = payloads.get(f"runtime/bin/python{lock['version'][:4]}")
    if executable is None or executable.data is None:
        raise ValueError("runtime artifact is missing its versioned Python executable")
    if executable.data[:8] != MACHO_64_LITTLE_ENDIAN + CPU_TYPE_ARM64_LITTLE_ENDIAN:
        raise ValueError("runtime Python executable is not an arm64 Mach-O binary")
    return payloads


def application_payloads(source_root: Path, runtime_lock_data: bytes) -> dict[str, Payload]:
    selected: dict[str, Payload] = {}
    mappings = {
        source_root / "client" / "mac.py": "lib/client/mac.py",
        source_root / "client" / "cli.py": "lib/client/cli.py",
        source_root / "client" / "capture.py": "lib/client/capture.py",
        source_root / "client" / "mcp.py": "lib/client/mcp.py",
        source_root / "client" / "macos_utility.py": "lib/client/macos_utility.py",
        source_root / "client" / "local_surfaces.py": "lib/client/local_surfaces.py",
        source_root / "client" / "__init__.py": "lib/client/__init__.py",
        source_root / "collector" / "collector.py": "lib/collector/collector.py",
        source_root / "collector" / "__init__.py": "lib/collector/__init__.py",
        source_root / "skills" / "recall" / "scripts" / "codex_identity.py":
            "lib/skills/recall/scripts/codex_identity.py",
        source_root / "connectors" / "sdk.py": "lib/connectors/sdk.py",
        source_root / "connectors" / "export_inbox.py": "lib/connectors/export_inbox.py",
        source_root / "connectors" / "cowork_local.py": "lib/connectors/cowork_local.py",
        source_root / "connectors" / "grep_ai.py": "lib/connectors/grep_ai.py",
        source_root / "connectors" / "registry.py": "lib/connectors/registry.py",
        source_root / "connectors" / "supervisor.py": "lib/connectors/supervisor.py",
        source_root / "connectors" / "host.py": "lib/connectors/host.py",
        source_root / "connectors" / "workspace_rail.py": "lib/connectors/workspace_rail.py",
        source_root / "connectors" / "kit.py": "lib/connectors/kit.py",
        source_root / "connectors" / "conformance.py": "lib/connectors/conformance.py",
        source_root / "connectors" / "remote_api.py": "lib/connectors/remote_api.py",
        source_root / "connectors" / "google_workspace.py": "lib/connectors/google_workspace.py",
        source_root / "connectors" / "imessage.py": "lib/connectors/imessage.py",
        source_root / "connectors" / "local_sqlite.py": "lib/connectors/local_sqlite.py",
        source_root / "connectors" / "local_file.py": "lib/connectors/local_file.py",
        source_root / "connectors" / "local_files.py": "lib/connectors/local_files.py",
        source_root / "connectors" / "local_activity.py": "lib/connectors/local_activity.py",
        source_root / "connectors" / "whatsapp_export.py": "lib/connectors/whatsapp_export.py",
        source_root / "connectors" / "__init__.py": "lib/connectors/__init__.py",
        source_root / "contracts" / "connector_v2.json": "lib/contracts/connector_v2.json",
        source_root / "contracts" / "connector_page_v1.json": "lib/contracts/connector_page_v1.json",
        source_root / "privacy" / "policy.py": "lib/privacy/policy.py",
        source_root / "privacy" / "transport.py": "lib/privacy/transport.py",
        source_root / "privacy" / "__init__.py": "lib/privacy/__init__.py",
        source_root / "client" / "macos" / "recall-brain": "bin/recall-brain",
        source_root / "client" / "macos" / "install.sh": "install.sh",
        source_root / "client" / "macos" / "uninstall.sh": "uninstall.sh",
        source_root / "client" / "macos_admin" / "RecallBrainAdmin.swift":
            "macos_admin/RecallBrainAdmin.swift",
        source_root / "client" / "macos_admin" / "Info.plist":
            "macos_admin/Info.plist",
        source_root / "client" / "macos_admin" / "build.sh":
            "macos_admin/build.sh",
    }
    for source, destination in mappings.items():
        selected[destination] = Payload(
            data=source.read_bytes(),
            executable=destination in {
                "bin/recall-brain", "install.sh", "uninstall.sh",
                "macos_admin/build.sh",
            },
        )
    for module in ("client", "collector", "connectors", "contracts", "privacy"):
        for source in sorted((source_root / module).glob("*.py")):
            selected[f"lib/{module}/{source.name}"] = Payload(data=source.read_bytes())
    for source in sorted((source_root / "contracts").glob("*.json")):
        selected[f"lib/contracts/{source.name}"] = Payload(data=source.read_bytes())
    selected["RUNTIME_LOCK.json"] = Payload(data=runtime_lock_data)
    return selected


def manifest(payloads: dict[str, Payload], lock: dict[str, Any]) -> bytes:
    entries = []
    for path, payload in sorted(payloads.items()):
        if payload.linkname is not None:
            entries.append({"path": path, "type": "symlink", "target": payload.linkname})
        else:
            data = payload.data or b""
            entries.append({"path": path, "type": "file", "bytes": len(data), "sha256": sha256(data)})
    value = {
        "format": "recall-macos-v2",
        "version": 2,
        "runtime": {
            "provider": lock["provider"],
            "release": lock["release"],
            "version": lock["version"],
            "target": lock["target"],
            "artifact_sha256": lock["artifact"]["sha256"],
        },
        "files": entries,
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def refresh_manifest(package_root: Path) -> None:
    """Refresh manifest file entries for a deliberately modified test/package root."""

    manifest_path = package_root / "MANIFEST.json"
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries = []
    for path in sorted(package_root.rglob("*")):
        relative = path.relative_to(package_root).as_posix()
        if relative == "MANIFEST.json" or path.is_dir():
            continue
        if path.is_symlink():
            entries.append({
                "path": relative, "type": "symlink", "target": os.readlink(path),
            })
        else:
            data = path.read_bytes()
            entries.append({
                "path": relative, "type": "file", "bytes": len(data),
                "sha256": sha256(data),
            })
    value["files"] = entries
    manifest_path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def render_package(payloads: dict[str, Payload]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.GNU_FORMAT) as archive:
        root = tarfile.TarInfo("recall-brain-macos")
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        root.mtime = EPOCH
        archive.addfile(root)
        directories = sorted(
            {str(parent) for path in payloads for parent in PurePosixPath(path).parents if str(parent) != "."},
            key=lambda value: (value.count("/"), value),
        )
        for directory in directories:
            info = tarfile.TarInfo("recall-brain-macos/" + directory)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            info.mtime = EPOCH
            archive.addfile(info)
        for path, payload in sorted(payloads.items()):
            info = tarfile.TarInfo("recall-brain-macos/" + path)
            info.mtime = EPOCH
            if payload.linkname is not None:
                info.type = tarfile.SYMTYPE
                info.linkname = payload.linkname
                info.mode = 0o777
                archive.addfile(info)
            else:
                data = payload.data or b""
                info.size = len(data)
                info.mode = 0o755 if payload.executable else 0o644
                archive.addfile(info, io.BytesIO(data))
    destination = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=destination, mtime=EPOCH) as compressed:
        compressed.write(raw.getvalue())
    return destination.getvalue()


def build(source_root: Path, output: Path, runtime_archive: Path, runtime_lock: Path) -> None:
    lock = read_lock(runtime_lock)
    archive_data = runtime_archive.read_bytes()
    verify_artifact(archive_data, lock)
    payloads = application_payloads(source_root, runtime_lock.read_bytes())
    payloads.update(runtime_payloads(archive_data, lock))
    payloads["MANIFEST.json"] = Payload(data=manifest(payloads, lock))
    package = render_package(payloads)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_bytes(package)
    temporary.replace(output)


def download_runtime(lock: dict[str, Any], destination: Path) -> None:
    request = urllib.request.Request(lock["artifact"]["url"], headers={"User-Agent": "recall-macos-package-builder/2"})
    expected_bytes = lock["artifact"]["bytes"]
    with urllib.request.urlopen(request, timeout=120) as response, destination.open("wb") as output:
        downloaded = 0
        while chunk := response.read(min(1024 * 1024, expected_bytes - downloaded + 1)):
            downloaded += len(chunk)
            if downloaded > expected_bytes:
                raise ValueError("runtime artifact exceeds its pinned byte count")
            output.write(chunk)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build byte-reproducible Recall Brain macOS bundle")
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime-lock", type=Path)
    parser.add_argument("--runtime-archive", type=Path)
    args = parser.parse_args()
    source_root = args.source_root.resolve()
    runtime_lock = (args.runtime_lock or source_root / "client" / "macos" / "RUNTIME_LOCK.json").resolve()
    try:
        lock = read_lock(runtime_lock)
        if args.runtime_archive:
            build(source_root, args.output, args.runtime_archive.resolve(), runtime_lock)
        else:
            with tempfile.TemporaryDirectory() as temporary:
                archive = Path(temporary) / "runtime.tar.gz"
                download_runtime(lock, archive)
                build(source_root, args.output, archive, runtime_lock)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")
    print(json.dumps({"output": str(args.output), "sha256": sha256(args.output.read_bytes())}, sort_keys=True))


if __name__ == "__main__":
    main()
