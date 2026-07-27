from __future__ import annotations

import hashlib
import hmac
import math
import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


DEFAULT_UPLOAD_LIMIT = 25 * 1024 * 1024
REDACTED = "[REDACTED]"

_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | os.O_DIRECTORY
    | os.O_CLOEXEC
    | getattr(os, "O_NOFOLLOW", 0)
)
_READ_FLAGS = (
    os.O_RDONLY
    | os.O_CLOEXEC
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
_WRITE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | os.O_CLOEXEC
    | getattr(os, "O_NOFOLLOW", 0)
)


class SecurityError(RuntimeError):
    """Base class for security-boundary failures."""


class StatePathError(SecurityError):
    """A private state path failed ownership, type, or symlink validation."""


class UploadSecurityError(SecurityError):
    """An upload source or staged snapshot failed validation."""


def _absolute_path(path: str | os.PathLike[str], *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise StatePathError(f"{label} must be absolute")
    if any(part in {".", ".."} for part in candidate.parts):
        raise StatePathError(f"{label} contains an unsafe path component")
    return candidate


def _open_directory_chain(path: Path) -> int:
    """Open an absolute directory without following any path-component symlink."""
    path = _absolute_path(path, label="directory")
    descriptor = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                _DIRECTORY_FLAGS,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_parent(path: Path) -> tuple[int, str]:
    if path == Path("/"):
        raise StatePathError("root cannot be used as a private state path")
    return _open_directory_chain(path.parent), path.name


def _lstat_at(parent_fd: int, name: str) -> os.stat_result:
    return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _validate_owner(info: os.stat_result, owner_uid: int, *, label: str) -> None:
    if info.st_uid != owner_uid:
        raise StatePathError(f"{label} has the wrong owner")


def secure_state_directory(
    path: str | os.PathLike[str],
    *,
    owner_uid: int | None = None,
    create: bool = False,
) -> Path:
    """Validate one private state directory and enforce mode 0700.

    Every path component is opened with no-follow semantics. The leaf must be
    owned by ``owner_uid`` (the effective UID by default). Existing symlinks are
    rejected rather than repaired.
    """
    target = _absolute_path(path, label="state directory")
    expected_uid = os.geteuid() if owner_uid is None else owner_uid
    parent_fd, name = _open_parent(target)
    descriptor = -1
    try:
        try:
            before = _lstat_at(parent_fd, name)
        except FileNotFoundError:
            if not create:
                raise StatePathError("state directory does not exist") from None
            try:
                os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            except FileExistsError:
                pass
            before = _lstat_at(parent_fd, name)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            raise StatePathError("state directory is not a real directory")
        descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened):
            raise StatePathError("state directory changed during validation")
        _validate_owner(opened, expected_uid, label="state directory")
        os.fchmod(descriptor, 0o700)
        enforced = os.fstat(descriptor)
        if stat.S_IMODE(enforced.st_mode) != 0o700:
            raise StatePathError("state directory mode could not be enforced")
        return target
    except OSError as exc:
        raise StatePathError("state directory could not be validated safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_fd)


def secure_state_file(
    path: str | os.PathLike[str],
    *,
    owner_uid: int | None = None,
    create: bool = False,
) -> Path:
    """Validate one private regular state file and enforce mode 0600."""
    target = _absolute_path(path, label="state file")
    expected_uid = os.geteuid() if owner_uid is None else owner_uid
    parent_fd, name = _open_parent(target)
    descriptor = -1
    try:
        try:
            before = _lstat_at(parent_fd, name)
        except FileNotFoundError:
            if not create:
                raise StatePathError("state file does not exist") from None
            try:
                descriptor = os.open(
                    name,
                    _WRITE_FLAGS,
                    0o600,
                    dir_fd=parent_fd,
                )
                before = os.fstat(descriptor)
            except FileExistsError:
                before = _lstat_at(parent_fd, name)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise StatePathError("state file is not a real regular file")
        if descriptor < 0:
            descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened):
            raise StatePathError("state file changed during validation")
        _validate_owner(opened, expected_uid, label="state file")
        os.fchmod(descriptor, 0o600)
        enforced = os.fstat(descriptor)
        if stat.S_IMODE(enforced.st_mode) != 0o600:
            raise StatePathError("state file mode could not be enforced")
        return target
    except OSError as exc:
        raise StatePathError("state file could not be validated safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_fd)


def read_private_text(
    path: str | os.PathLike[str],
    *,
    owner_uid: int | None = None,
    max_bytes: int = 1_048_576,
    encoding: str = "utf-8",
) -> str:
    """Read an owner-controlled regular file through a verified no-follow FD."""
    target = _absolute_path(path, label="private file")
    if not 1 <= max_bytes <= 16 * 1024 * 1024:
        raise StatePathError("private file size limit is invalid")
    expected_uid = os.geteuid() if owner_uid is None else owner_uid
    parent_fd, name = _open_parent(target)
    descriptor = -1
    try:
        before = _lstat_at(parent_fd, name)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise StatePathError("private file is not a real regular file")
        descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened):
            raise StatePathError("private file changed during validation")
        _validate_owner(opened, expected_uid, label="private file")
        if opened.st_nlink != 1:
            raise StatePathError("private file has multiple hard links")
        os.fchmod(descriptor, 0o600)
        if opened.st_size > max_bytes:
            raise StatePathError("private file is too large")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > max_bytes:
            raise StatePathError("private file is too large")
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError as exc:
            raise StatePathError("private file has invalid text encoding") from exc
    except OSError as exc:
        raise StatePathError("private file could not be read safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_fd)


def validate_private_executable(
    path: str | os.PathLike[str],
    *,
    owner_uid: int | None = None,
) -> Path:
    """Validate an absolute executable without following path-component symlinks."""
    target = _absolute_path(path, label="executable")
    expected_uid = os.geteuid() if owner_uid is None else owner_uid
    parent_fd = os.open("/", _DIRECTORY_FLAGS)
    descriptor = -1
    try:
        for component in target.parent.parts[1:]:
            next_parent_fd = os.open(
                component,
                _DIRECTORY_FLAGS,
                dir_fd=parent_fd,
            )
            os.close(parent_fd)
            parent_fd = next_parent_fd
            parent_info = os.fstat(parent_fd)
            parent_mode = stat.S_IMODE(parent_info.st_mode)
            if parent_mode & 0o022 and not parent_mode & stat.S_ISVTX:
                raise StatePathError("executable has a writable ancestor")
        name = target.name
        before = _lstat_at(parent_fd, name)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise StatePathError("executable is not a real regular file")
        descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        if not _same_inode(before, opened):
            raise StatePathError("executable changed during validation")
        if opened.st_uid not in {0, expected_uid}:
            raise StatePathError("executable has the wrong owner")
        if opened.st_nlink != 1:
            raise StatePathError("executable has multiple hard links")
        mode = stat.S_IMODE(opened.st_mode)
        if mode & 0o022:
            raise StatePathError("executable is group or world writable")
        if not mode & 0o111:
            raise StatePathError("executable is not executable")
        return target
    except OSError as exc:
        raise StatePathError("executable could not be validated safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_fd)


def _lexical_absolute(path: str | os.PathLike[str], *, label: str) -> Path:
    raw = Path(path)
    if not raw.is_absolute() or any(part in {".", ".."} for part in raw.parts):
        raise UploadSecurityError(f"{label} must be an absolute path without traversal")
    return raw


def _approved_location(source: Path, approved_roots: tuple[Path, ...]) -> tuple[Path, Path]:
    matches: list[tuple[Path, Path]] = []
    for root in approved_roots:
        try:
            relative = source.relative_to(root)
        except ValueError:
            continue
        if relative.parts:
            matches.append((root, relative))
    if not matches:
        raise UploadSecurityError("upload source is outside the approved roots")
    return max(matches, key=lambda item: len(item[0].parts))


def _open_relative_parent(root_fd: int, relative: Path) -> tuple[int, str]:
    descriptor = os.dup(root_fd)
    try:
        for component in relative.parts[:-1]:
            next_descriptor = os.open(
                component,
                _DIRECTORY_FLAGS,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor, relative.name
    except BaseException:
        os.close(descriptor)
        raise


def _open_private_root(path: Path, owner_uid: int) -> int:
    """Open an owner-private root through non-writable ancestors."""
    path = _absolute_path(path, label="approved upload root")
    descriptor = os.open("/", _DIRECTORY_FLAGS)
    components = path.parts[1:]
    if not components:
        os.close(descriptor)
        raise UploadSecurityError("filesystem root cannot be an upload root")
    try:
        for index, component in enumerate(components):
            next_descriptor = os.open(
                component,
                _DIRECTORY_FLAGS,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
            info = os.fstat(descriptor)
            mode = stat.S_IMODE(info.st_mode)
            if mode & 0o022 and not mode & stat.S_ISVTX:
                raise UploadSecurityError(
                    "approved upload root has a writable ancestor"
                )
            if index == len(components) - 1 and (
                info.st_uid != owner_uid
                or mode & 0o077
            ):
                raise UploadSecurityError(
                    "approved upload root must be owner-private"
                )
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


@dataclass(frozen=True)
class StagedUpload:
    """A detached, read-only upload snapshot with an integrity fingerprint."""

    path: Path
    size: int
    sha256: str
    owner_uid: int
    device: int
    inode: int
    source_device: int
    source_inode: int

    def open_verified(self) -> int:
        """Return a verified read FD; the caller owns and must close it.

        Consumers should read this descriptor instead of reopening ``path``.
        The descriptor remains bound to the verified inode if the path is
        replaced after validation.
        """
        parent_fd = descriptor = -1
        try:
            parent_fd, name = _open_parent(self.path)
            before = _lstat_at(parent_fd, name)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise UploadSecurityError("staged upload is not a regular file")
            descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            if (
                not _same_inode(before, opened)
                or (opened.st_dev, opened.st_ino) != (self.device, self.inode)
                or opened.st_uid != self.owner_uid
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o400
                or opened.st_size != self.size
            ):
                raise UploadSecurityError("staged upload identity changed")
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, 1024 * 1024):
                digest.update(chunk)
            if not hmac.compare_digest(digest.hexdigest(), self.sha256):
                raise UploadSecurityError("staged upload content changed")
            os.lseek(descriptor, 0, os.SEEK_SET)
            verified_descriptor = descriptor
            descriptor = -1
            return verified_descriptor
        except (OSError, StatePathError) as exc:
            raise UploadSecurityError("staged upload could not be verified") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if parent_fd >= 0:
                os.close(parent_fd)

    def verify(self) -> None:
        """Fail if the staged snapshot was replaced or modified."""
        descriptor = self.open_verified()
        os.close(descriptor)


def stage_upload(
    source: str | os.PathLike[str],
    *,
    approved_roots: list[str | os.PathLike[str]]
    | tuple[str | os.PathLike[str], ...],
    staging_directory: str | os.PathLike[str],
    owner_uid: int | None = None,
    max_bytes: int = DEFAULT_UPLOAD_LIMIT,
) -> StagedUpload:
    """Copy an approved regular file into a private, integrity-checked snapshot."""
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    expected_uid = os.geteuid() if owner_uid is None else owner_uid
    source_path = _lexical_absolute(source, label="upload source")
    roots = tuple(
        _lexical_absolute(root, label="approved root")
        for root in approved_roots
    )
    if not roots:
        raise UploadSecurityError("at least one approved root is required")
    root, relative = _approved_location(source_path, roots)
    staging_path = _lexical_absolute(staging_directory, label="staging directory")
    try:
        secure_state_directory(
            staging_path,
            owner_uid=expected_uid,
            create=True,
        )
    except StatePathError as exc:
        raise UploadSecurityError("staging directory is not private") from exc

    root_fd = source_parent_fd = source_fd = staging_fd = destination_fd = -1
    suffix = source_path.suffix
    if not re.fullmatch(r"\.[A-Za-z0-9]{1,15}", suffix):
        suffix = ""
    destination_name = f"upload-{secrets.token_hex(20)}{suffix}"
    destination_created = False
    try:
        root_fd = _open_private_root(root, expected_uid)
        source_parent_fd, source_name = _open_relative_parent(root_fd, relative)
        before = _lstat_at(source_parent_fd, source_name)
        if stat.S_ISLNK(before.st_mode):
            raise UploadSecurityError("upload source cannot be a symlink")
        source_fd = os.open(source_name, _READ_FLAGS, dir_fd=source_parent_fd)
        opened = os.fstat(source_fd)
        if not _same_inode(before, opened):
            raise UploadSecurityError("upload source changed while opening")
        if not stat.S_ISREG(opened.st_mode):
            raise UploadSecurityError("upload source is not a regular file")
        if opened.st_uid != expected_uid:
            raise UploadSecurityError("upload source has the wrong owner")
        if opened.st_nlink != 1:
            raise UploadSecurityError("upload source must have exactly one hard link")
        if opened.st_size > max_bytes:
            raise UploadSecurityError("upload source exceeds the size limit")

        staging_fd = _open_directory_chain(staging_path)
        staging_info = os.fstat(staging_fd)
        if staging_info.st_uid != expected_uid:
            raise UploadSecurityError("staging directory has the wrong owner")
        os.fchmod(staging_fd, 0o700)
        if stat.S_IMODE(os.fstat(staging_fd).st_mode) != 0o700:
            raise UploadSecurityError("staging directory could not be made private")
        destination_fd = os.open(
            destination_name,
            _WRITE_FLAGS,
            0o600,
            dir_fd=staging_fd,
        )
        destination_created = True
        digest = hashlib.sha256()
        copied = 0
        while chunk := os.read(source_fd, min(1024 * 1024, max_bytes + 1)):
            copied += len(chunk)
            if copied > max_bytes:
                raise UploadSecurityError("upload source grew beyond the size limit")
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(destination_fd, view)
                if written <= 0:
                    raise UploadSecurityError("staged upload write failed")
                view = view[written:]

        after = os.fstat(source_fd)
        source_identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_uid,
            opened.st_mode,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_uid,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if source_identity != after_identity or copied != opened.st_size:
            raise UploadSecurityError("upload source changed while being copied")

        os.fsync(destination_fd)
        os.fchmod(destination_fd, 0o400)
        staged_info = os.fstat(destination_fd)
        if (
            not stat.S_ISREG(staged_info.st_mode)
            or staged_info.st_uid != expected_uid
            or staged_info.st_nlink != 1
            or stat.S_IMODE(staged_info.st_mode) != 0o400
            or staged_info.st_size != copied
        ):
            raise UploadSecurityError("staged upload could not be made private")
        candidate = StagedUpload(
            path=staging_path / destination_name,
            size=copied,
            sha256=digest.hexdigest(),
            owner_uid=expected_uid,
            device=staged_info.st_dev,
            inode=staged_info.st_ino,
            source_device=opened.st_dev,
            source_inode=opened.st_ino,
        )
        candidate.verify()
        staged = candidate
        return candidate
    except UploadSecurityError:
        raise
    except (OSError, StatePathError) as exc:
        raise UploadSecurityError("upload could not be staged safely") from exc
    finally:
        if destination_created and "staged" not in locals() and staging_fd >= 0:
            try:
                os.unlink(destination_name, dir_fd=staging_fd)
            except OSError:
                pass
        for descriptor in (
            destination_fd,
            staging_fd,
            source_fd,
            source_parent_fd,
            root_fd,
        ):
            if descriptor >= 0:
                os.close(descriptor)


@dataclass(frozen=True)
class RedactionResult:
    text: str
    redaction_count: int


_PRIVATE_KEY = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*PRIVATE KEY)-----.*?"
    r"(?:-----END (?P=label)-----|\Z)",
    re.DOTALL,
)
_CREDENTIALED_URL = re.compile(
    r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)"
    r"[^/\s:@]+:[^/\s@]+@"
    r"(?P<host>\[[^\]]+\]|[^/\s?#]+)"
)
_URL_QUERY_SECRET = re.compile(
    r"(?P<prefix>[?&](?:access[_-]?token|api[_-]?key|password|passwd|"
    r"secret|token|key)=)[^&#\s]+",
    re.IGNORECASE,
)
_BEARER = re.compile(
    r"(?P<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]{8,}",
    re.IGNORECASE,
)
_JWT = re.compile(
    r"(?<![A-Za-z0-9_-])"
    r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
    r"(?![A-Za-z0-9_-])"
)
_PROVIDER_KEY = re.compile(
    r"(?<![A-Za-z0-9_-])(?:"
    r"xox[baprs]-[A-Za-z0-9-]{10,}|"
    r"xa" r"pp-[A-Za-z0-9-]{10,}|"
    r"github_pat_[A-Za-z0-9_]{20,}|"
    r"gh[pousr]_[A-Za-z0-9]{20,}|"
    r"sk-(?:ant-|proj-)?[A-Za-z0-9_-]{20,}|"
    r"AIza[A-Za-z0-9_-]{35}|"
    r"(?:AKIA|ASIA)[A-Z0-9]{16}|"
    r"(?:hf_|gsk_|ops_)[A-Za-z0-9_-]{20,}|"
    r"(?:sk|rk)_live_[A-Za-z0-9]{16,}"
    r")(?![A-Za-z0-9_-])"
)
_QUOTED_ASSIGNMENT = re.compile(
    r"(?P<prefix>(?:[\"']?(?:password|passwd|pwd|secret|api[_-]?key|"
    r"access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key)"
    r"[\"']?\s*[:=]\s*))"
    r"(?P<quote>[\"'])(?P<value>[^\"'\r\n]*)(?P=quote)",
    re.IGNORECASE,
)
_PLAIN_ASSIGNMENT = re.compile(
    r"(?P<prefix>\b(?:password|passwd|pwd|secret|api[_-]?key|"
    r"access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key)"
    r"\b\s*[:=]\s*)"
    r"(?P<value>[^\s,;}\]]+)",
    re.IGNORECASE,
)
_SENSITIVE_FIELD_NAME = re.compile(
    r"(?:^|[_-])(?:"
    r"password|passwd|pwd|secret|token|api[_-]?key|access[_-]?token|"
    r"auth[_-]?token|authorization|client[_-]?secret|credential(?:s)?|"
    r"private[_-]?key"
    r")(?:$|[_-])",
    re.IGNORECASE,
)


def _is_sensitive_field_name(key: str) -> bool:
    segmented = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    return _SENSITIVE_FIELD_NAME.search(segmented) is not None


def redact_egress(text: str) -> RedactionResult:
    """Redact sensitive text without returning or logging matched values."""
    if not isinstance(text, str):
        raise TypeError("egress text must be a string")
    count = 0

    def apply(
        value: str,
        pattern: re.Pattern[str],
        replacement: str | Callable[[re.Match[str]], str],
    ) -> str:
        nonlocal count
        value, substitutions = pattern.subn(replacement, value)
        count += substitutions
        return value

    cleaned = apply(text, _PRIVATE_KEY, "[REDACTED_PRIVATE_KEY]")
    cleaned = apply(
        cleaned,
        _CREDENTIALED_URL,
        lambda match: (
            f"{match.group('scheme')}[REDACTED_CREDENTIALS]@"
            f"{match.group('host')}"
        ),
    )
    cleaned = apply(
        cleaned,
        _URL_QUERY_SECRET,
        lambda match: f"{match.group('prefix')}{REDACTED}",
    )
    cleaned = apply(
        cleaned,
        _QUOTED_ASSIGNMENT,
        lambda match: (
            f"{match.group('prefix')}{match.group('quote')}"
            f"{REDACTED}{match.group('quote')}"
        ),
    )
    cleaned = apply(
        cleaned,
        _PLAIN_ASSIGNMENT,
        lambda match: f"{match.group('prefix')}{REDACTED}",
    )
    cleaned = apply(
        cleaned,
        _BEARER,
        lambda match: f"{match.group('prefix')}{REDACTED}",
    )
    cleaned = apply(cleaned, _JWT, "[REDACTED_JWT]")
    cleaned = apply(cleaned, _PROVIDER_KEY, "[REDACTED_PROVIDER_KEY]")
    return RedactionResult(cleaned, count)


def redact_egress_text(text: str) -> str:
    """Return only the safe egress text."""
    return redact_egress(text).text


def redact_egress_json(
    value: Any,
    *,
    max_depth: int = 24,
    max_nodes: int = 20_000,
) -> Any:
    """Return a redacted, bounded copy of a JSON-compatible value."""
    if max_depth < 1 or max_nodes < 1:
        raise ValueError("egress JSON limits must be positive")
    ancestors: set[int] = set()
    visited = 0

    def walk(item: Any, depth: int) -> Any:
        nonlocal visited
        visited += 1
        if visited > max_nodes:
            raise ValueError("egress JSON contains too many values")
        if depth > max_depth:
            raise ValueError("egress JSON is nested too deeply")
        if item is None or isinstance(item, bool | int):
            return item
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("egress JSON contains a non-finite number")
            return item
        if isinstance(item, str):
            return redact_egress_text(item)
        if isinstance(item, list):
            identity = id(item)
            if identity in ancestors:
                raise ValueError("egress JSON contains a cycle")
            ancestors.add(identity)
            try:
                return [walk(child, depth + 1) for child in item]
            finally:
                ancestors.remove(identity)
        if isinstance(item, dict):
            identity = id(item)
            if identity in ancestors:
                raise ValueError("egress JSON contains a cycle")
            ancestors.add(identity)
            try:
                result: dict[str, Any] = {}
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise ValueError("egress JSON object keys must be strings")
                    if redact_egress(key).redaction_count:
                        raise ValueError(
                            "egress JSON object key contains sensitive data"
                        )
                    result[key] = (
                        REDACTED
                        if child is not None
                        and _is_sensitive_field_name(key)
                        else walk(child, depth + 1)
                    )
                return result
            finally:
                ancestors.remove(identity)
        raise ValueError("egress value is not JSON-compatible")

    return walk(value, 0)


def require_safe_upload_content(
    descriptor: int,
    *,
    max_bytes: int = DEFAULT_UPLOAD_LIMIT,
) -> None:
    """Reject staged file bytes that match the central egress secret policy."""
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode):
        raise UploadSecurityError("staged upload is not a regular file")
    if opened.st_size > max_bytes:
        raise UploadSecurityError("staged upload exceeds the configured size limit")
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = opened.st_size
    chunks: list[bytes] = []
    while remaining:
        chunk = os.read(descriptor, min(remaining, 1024 * 1024))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    os.lseek(descriptor, 0, os.SEEK_SET)
    if remaining:
        raise UploadSecurityError("staged upload changed while being scanned")
    content = b"".join(chunks).decode("latin-1")
    if redact_egress(content).redaction_count:
        raise UploadSecurityError(
            "upload content matches the secret egress policy; remove or redact it first"
        )
