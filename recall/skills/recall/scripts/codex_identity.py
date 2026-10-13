from __future__ import annotations

import hashlib
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
PAGINATED_FILENAME = re.compile(
    r"rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-"
    r"(" + UUID.pattern + r")_(" + UUID.pattern + r")"
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
    segment_id: str | None = None
    history_base_id: str | None = None
    history_base_offset: int | None = None
    history_base_ordinal: int | None = None


def codex_segment_id_from_filename(path: Path) -> str | None:
    match = PAGINATED_FILENAME.fullmatch(path.stem)
    return match.group(2).casefold() if match else None


def stable_codex_segment_key(session_id: str, segment_id: str) -> str:
    values = [_safe_native_id(value) for value in (session_id, segment_id)]
    if any(value is None for value in values):
        raise ValueError("unsafe Codex segment identity")
    return hashlib.sha256(("codex-segment\x1f" + "\x1f".join(values)).encode()).hexdigest()[:24]


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


def codex_session_id_from_filename(path: Path) -> str | None:
    """Return the native session UUID, excluding a paginated segment suffix."""

    segmented = PAGINATED_FILENAME.fullmatch(path.stem)
    if segmented:
        return segmented.group(1).casefold()
    matches = UUID.findall(path.stem)
    if not matches:
        return None
    return matches[-1].casefold()


def codex_session_id_from_record(record: object) -> str | None:
    """Return a safe native ID only for a Codex session metadata record."""

    if not isinstance(record, dict) or record.get("type") != "session_meta":
        return None
    payload = record.get("payload")
    if not isinstance(payload, dict) or "id" not in payload:
        return None
    return _safe_native_id(payload.get("id"))


def stable_codex_record_key(session_id: str) -> str:
    """Return the path-independent key used by every Codex export surface."""

    safe_session_id = _safe_native_id(session_id)
    if safe_session_id is None:
        raise ValueError("unsafe Codex session identity")
    return hashlib.sha256(
        ("codex-session\x1f" + safe_session_id).encode()
    ).hexdigest()[:24]


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
    filename_id = codex_session_id_from_filename(path)
    metadata_ids: list[str] = []
    first_payload: dict | None = None
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
            if first_payload is None:
                first_payload = payload
            native_id = codex_session_id_from_record(record)
            if native_id is None:
                unsafe_metadata = True
            elif native_id not in metadata_ids:
                metadata_ids.append(native_id)

    if unsafe_metadata:
        return CodexSessionIdentity("unsafe_metadata", None, None)
    metadata_id = metadata_ids[0] if metadata_ids else None
    segment_id = codex_segment_id_from_filename(path)
    if segment_id is not None:
        payload = first_payload or {}
        base = payload.get("history_base")
        if (metadata_id != filename_id or payload.get("history_mode") != "paginated"
                or _safe_native_id(payload.get("session_id", metadata_id)) != metadata_id
                or segment_id == metadata_id or not isinstance(base, dict)
                or _safe_native_id(base.get("thread_id")) is None
                or type(base.get("end_byte_offset")) is not int
                or base["end_byte_offset"] < 0
                or type(base.get("end_ordinal_exclusive")) is not int
                or base["end_ordinal_exclusive"] < 0):
            return CodexSessionIdentity("identity_conflict", None, None)
        return CodexSessionIdentity("resolved", metadata_id, "metadata", segment_id,
                                    _safe_native_id(base["thread_id"]), base["end_byte_offset"],
                                    base["end_ordinal_exclusive"])
    if metadata_id is not None and filename_id is not None:
        if metadata_id != filename_id:
            return CodexSessionIdentity("identity_conflict", None, None)
        return CodexSessionIdentity("resolved", metadata_id, "metadata")
    if metadata_id is not None:
        return CodexSessionIdentity("resolved", metadata_id, "metadata")
    if filename_id is not None:
        return CodexSessionIdentity("resolved", filename_id, "filename")
    return CodexSessionIdentity("identity_unavailable", None, None)
