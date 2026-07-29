#!/usr/bin/env python3
"""Keep generated Parable agent frontmatter authoritative."""

import json
import os
import sys

ACTIVE_AGENTS_ENV = "PARABLE_ACTIVE_AGENTS_JSON"


def active_agents() -> set[str] | None:
    """Return the launch snapshot, or None outside a Parable-managed launch."""
    raw = os.environ.get(ACTIVE_AGENTS_ENV)
    if raw is None:
        return None
    try:
        values = json.loads(raw)
    except json.JSONDecodeError:
        return set()
    if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
        return set()
    return set(values)


def main() -> None:
    try:
        event = json.load(sys.stdin)
    except (json.JSONDecodeError, OSError):
        return

    if event.get("hook_event_name") != "PreToolUse" or event.get("tool_name") != "Agent":
        return
    tool_input = event.get("tool_input")
    if not isinstance(tool_input, dict):
        return
    agent_type = tool_input.get("subagent_type")
    if not isinstance(agent_type, str) or not agent_type.startswith("parable-"):
        return

    active = active_agents()
    if active is not None and agent_type not in active:
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"{agent_type} is unavailable in this Parable session; "
                    "restart Parable after its model authentication recovers."
                ),
            }
        }, separators=(",", ":")))
        return

    if "model" not in tool_input:
        return
    updated_input = dict(tool_input)
    updated_input.pop("model", None)
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "permissionDecisionReason": (
                "Parable generated this agent; its checked-in frontmatter owns the exact model."
            ),
            "updatedInput": updated_input,
        }
    }, separators=(",", ":")))


if __name__ == "__main__":
    main()
