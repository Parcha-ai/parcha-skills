#!/usr/bin/env python3
"""Turn proxy authentication failures into actionable Parable guidance."""

import json
import re
import sys


VENDOR_BY_PROVIDER = {
    "claude": "claude",
    "codex": "chatgpt",
    "xai": "xai",
    "kimi": "kimi",
}
LABEL_BY_VENDOR = {
    "claude": "Claude",
    "chatgpt": "ChatGPT",
    "xai": "xAI",
    "kimi": "Kimi",
}


def error_provider(error: str) -> str | None:
    """Return one recognized provider from a proxy selection error."""
    match = re.search(r"\bproviders?=([a-z0-9_.-]+)", error, re.IGNORECASE)
    if match:
        provider = match.group(1).lower()
        if provider in VENDOR_BY_PROVIDER:
            return provider
    return None


def error_model(error: str) -> str | None:
    """Return the model identifier when the proxy included one."""
    match = re.search(r"\bmodel=([a-z0-9_.\[\]-]+)", error, re.IGNORECASE)
    return match.group(1) if match else None


def emit(message: str, context: str) -> None:
    print(json.dumps({
        "systemMessage": message,
        "hookSpecificOutput": {
            "hookEventName": "PostToolUseFailure",
            "additionalContext": context,
        },
    }, separators=(",", ":")))


def main() -> None:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        return
    if (
        event.get("hook_event_name") != "PostToolUseFailure"
        or event.get("tool_name") != "Agent"
        or event.get("is_interrupt") is True
    ):
        return
    error = event.get("error")
    if not isinstance(error, str):
        return

    lowered = error.lower()
    model = error_model(error)
    auth_model_note = f" for {model}" if model else ""
    cooldown_model_note = f" {model}" if model else ""
    if "model_cooldown" in lowered:
        message = (
            f"Parable: the selected model{cooldown_model_note} is cooling down. Wait for its "
            "reset or route this work to another available model; reauthentication is not required."
        )
        emit(message, message)
        return
    if "auth_unavailable" not in lowered and "auth_not_found" not in lowered:
        return

    provider = error_provider(error)
    vendor = VENDOR_BY_PROVIDER.get(provider or "")
    if vendor:
        label = LABEL_BY_VENDOR[vendor]
        command = f"parable auth add {vendor}"
        if "auth_not_found" in lowered:
            action = f"Run `{command}` in a separate terminal, finish authorization, then retry."
        else:
            action = (
                f"Retry once in a moment. If it repeats, run `{command}` in a separate "
                "terminal, finish authorization, then retry."
            )
        message = f"Parable: {label} authentication is unavailable{auth_model_note}. {action}"
    else:
        message = (
            f"Parable: model authentication is unavailable{auth_model_note}. Retry once in a "
            "moment; if it repeats, reauthorize the affected subscription with "
            "`parable auth add <vendor>` in a separate terminal."
        )
    emit(message, message + " Repeated agent retries do not repair expired authentication.")


if __name__ == "__main__":
    main()
