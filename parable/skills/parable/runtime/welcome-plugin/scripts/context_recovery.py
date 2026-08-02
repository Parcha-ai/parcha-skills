#!/usr/bin/env python3
"""Ask Parable's supervisor to preflight or recover one exact session."""

from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import re
import stat
import sys


RECOVERY_FILE_ENV = "PARABLE_CONTEXT_RECOVERY_FILE"
RESUME_PICKER_ENV = "PARABLE_CONTEXT_RESUME_PICKER"
CONTEXT_ERROR = re.compile(
    r"input exceeds the context window|context window of this model|prompt is too long",
    re.IGNORECASE,
)
INTERRUPTED_MESSAGE = "[Request interrupted by user]"
TEAMMATE_MESSAGE_PREFIX = "Another Claude session sent a message:"
TRANSCRIPT_TAIL_BYTES = 1024 * 1024
TEAMMATE_DELIVERY_WINDOW_SECONDS = 2


def _message_text(record: object) -> str | None:
    if not isinstance(record, dict) or record.get("type") != "user":
        return None
    message = record.get("message")
    if not isinstance(message, dict) or message.get("role") != "user":
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return None
    text = "\n".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
    ).strip()
    return text or None


def _timestamp(record: object) -> datetime | None:
    if not isinstance(record, dict):
        return None
    value = record.get("timestamp")
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_transcript_tail(target: Path) -> list[dict[str, object]]:
    metadata = os.lstat(target)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (
            hasattr(os, "getuid")
            and metadata.st_uid != os.getuid()
        )
    ):
        return []
    with target.open("rb") as handle:
        start = max(0, metadata.st_size - TRANSCRIPT_TAIL_BYTES)
        handle.seek(start)
        if start:
            handle.readline()
        lines = handle.readlines()
    records = []
    for line in lines:
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _assistant_made_progress(record: object) -> bool:
    if not isinstance(record, dict) or record.get("type") != "assistant":
        return False
    message = record.get("message")
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return False
    content = message.get("content")
    if not isinstance(content, list):
        return bool(content)
    return any(
        isinstance(block, dict)
        and block.get("type") != "thinking"
        for block in content
    )


def teammate_delivery_interrupted_turn(transcript_path: object) -> bool:
    if not isinstance(transcript_path, str) or not transcript_path:
        return False
    try:
        records = _read_transcript_tail(Path(transcript_path))
    except OSError:
        return False

    interrupted_at: datetime | None = None
    delivery_index: int | None = None
    for index, record in enumerate(records):
        text = _message_text(record)
        if text == INTERRUPTED_MESSAGE:
            interrupted_at = _timestamp(record)
            continue
        if text and text.startswith(TEAMMATE_MESSAGE_PREFIX) and interrupted_at:
            delivered_at = _timestamp(record)
            if (
                delivered_at
                and 0 <= (delivered_at - interrupted_at).total_seconds()
                <= TEAMMATE_DELIVERY_WINDOW_SECONDS
            ):
                delivery_index = index
            interrupted_at = None
        elif text or record.get("type") in {"user", "assistant"}:
            interrupted_at = None

    if delivery_index is None:
        return False
    for record in records[delivery_index + 1:]:
        text = _message_text(record)
        if text is not None:
            if text == INTERRUPTED_MESSAGE or text.startswith(TEAMMATE_MESSAGE_PREFIX):
                continue
            return False
        if record.get("type") == "user":
            # A tool result means the lead recovered and continued doing work.
            return False
        if _assistant_made_progress(record):
            return False
    return True


def recovery_request(event: object) -> dict[str, object] | None:
    if not isinstance(event, dict):
        return None
    # Plugin hooks also run in subagents. Only the main interactive session is
    # owned by the Parable supervisor and can be restarted safely.
    if event.get("agent_id") is not None:
        return None
    session_id = event.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return None
    if (
        event.get("hook_event_name") == "SessionStart"
        and event.get("source") in {"resume", "fork"}
        and os.environ.get(RESUME_PICKER_ENV) == "1"
    ):
        return {
            "version": 1,
            "reason": "resume_picker",
            "session_id": session_id,
        }
    if (
        event.get("hook_event_name") == "Notification"
        and event.get("notification_type") == "idle_prompt"
        and teammate_delivery_interrupted_turn(event.get("transcript_path"))
    ):
        return {
            "version": 1,
            "reason": "teammate_interrupt",
            "session_id": session_id,
        }
    if event.get("hook_event_name") != "StopFailure":
        return None
    if event.get("error") not in {"invalid_request", "unknown"}:
        return None
    detail = "\n".join(
        value for value in (
            event.get("error_details"), event.get("last_assistant_message")
        ) if isinstance(value, str)
    )
    if not CONTEXT_ERROR.search(detail):
        return None
    return {
        "version": 1,
        "reason": "context_failure",
        "session_id": session_id,
    }


def write_request(target: Path, request: dict[str, object]) -> None:
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(request, handle, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main() -> None:
    target = os.environ.get(RECOVERY_FILE_ENV)
    if not target:
        return
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        return
    request = recovery_request(event)
    if request is None:
        return
    try:
        write_request(Path(target), request)
    except OSError:
        # Hook output cannot safely replace the supervisor channel. The
        # original session remains visible and can still be resumed manually.
        return


if __name__ == "__main__":
    main()
