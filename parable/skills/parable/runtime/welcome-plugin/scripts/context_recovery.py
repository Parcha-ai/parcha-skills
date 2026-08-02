#!/usr/bin/env python3
"""Ask Parable's supervisor to recover a context-window API failure."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys


RECOVERY_FILE_ENV = "PARABLE_CONTEXT_RECOVERY_FILE"
CONTEXT_ERROR = re.compile(
    r"input exceeds the context window|context window of this model|prompt is too long",
    re.IGNORECASE,
)


def recovery_request(event: object) -> dict[str, object] | None:
    if not isinstance(event, dict) or event.get("hook_event_name") != "StopFailure":
        return None
    # Plugin hooks also run in subagents. Only the main interactive session is
    # owned by the Parable supervisor and can be restarted safely.
    if event.get("agent_id") is not None:
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
    session_id = event.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        return None
    return {"version": 1, "session_id": session_id}


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
        # StopFailure ignores hook output. The original API error remains
        # visible, and the user can still recover manually or on next resume.
        return


if __name__ == "__main__":
    main()
