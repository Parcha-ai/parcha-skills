from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


MAX_IDENTITY_BYTES = 1024 * 1024
MAX_IDENTITY_RECORDS = 128
SAFE_NATIVE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@-]{0,255}")
UUID = re.compile(
    r"(?<![0-9A-Fa-f])"
    r"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-"
    r"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"
    r"(?![0-9A-Fa-f])"
)
SECRET_SHAPE = re.compile(
    r"(?:sk-[A-Za-z0-9_-]{20,}|(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}|"
    r"xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[A-Z0-9]{16}|"
    r"AIza[A-Za-z0-9_-]{30,})"
)


@dataclass(frozen=True)
class CodexSessionIdentity:
    status: str
    native_session_id: str | None
    basis: str | None

    @property
    def stable_parent_id(self) -> str | None:
        if self.native_session_id is None:
            return None
        return "codex-session-" + self.native_session_id


def _safe_native_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if (
        candidate != value
        or not SAFE_NATIVE_ID.fullmatch(candidate)
        or SECRET_SHAPE.search(candidate)
    ):
        return None
    return candidate.casefold()


def _filename_uuid(path: Path) -> str | None:
    matches = UUID.findall(path.stem)
    if not matches:
        return None
    return matches[-1].casefold()


def resolve_codex_session_identity(
    path: Path,
    *,
    max_bytes: int = MAX_IDENTITY_BYTES,
    max_records: int = MAX_IDENTITY_RECORDS,
) -> CodexSessionIdentity:
    """Resolve one bounded, content-free Codex session identity.

    The first native session metadata ID names the outer rollout. Codex may add
    later ``session_meta`` records when it forks or resumes work inside that
    rollout, so those later IDs do not replace the rollout identity. When a
    filename UUID exists it must agree with the first metadata ID. The filename
    is otherwise a compatibility fallback. Unsafe or conflicting identity data
    fails closed instead of falling back to a mutable path-derived identity.
    """

    if max_bytes < 1 or max_records < 1:
        raise ValueError("identity bounds must be positive")
    filename_id = _filename_uuid(path)
    metadata_ids: list[str] = []
    unsafe_metadata = False
    consumed = 0
    with path.open("rb") as source:
        for _ in range(max_records):
            remaining = max_bytes - consumed
            if remaining <= 0:
                break
            line = source.readline(remaining + 1)
            consumed += len(line)
            if len(line) > remaining:
                break
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(record, dict) or record.get("type") != "session_meta":
                continue
            payload = record.get("payload")
            if not isinstance(payload, dict) or "id" not in payload:
                continue
            native_id = _safe_native_id(payload.get("id"))
            if native_id is None:
                unsafe_metadata = True
            else:
                if native_id not in metadata_ids:
                    metadata_ids.append(native_id)

    if unsafe_metadata:
        return CodexSessionIdentity("unsafe_metadata", None, None)
    metadata_id = metadata_ids[0] if metadata_ids else None
    if metadata_id is not None and filename_id is not None:
        if metadata_id != filename_id:
            return CodexSessionIdentity("identity_conflict", None, None)
        return CodexSessionIdentity("resolved", metadata_id, "metadata")
    if metadata_id is not None:
        return CodexSessionIdentity("resolved", metadata_id, "metadata")
    if filename_id is not None:
        return CodexSessionIdentity("resolved", filename_id, "filename")
    return CodexSessionIdentity("identity_unavailable", None, None)
