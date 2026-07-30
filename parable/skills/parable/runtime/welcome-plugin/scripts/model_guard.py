#!/usr/bin/env python3
"""Keep generated Parable agent frontmatter authoritative."""

import json
import os
import sys

AGENT_STATE_ENV = "PARABLE_AGENT_STATE_JSON"


def agent_state() -> dict[str, set[str]] | None:
    """Return the launch snapshot, or None outside a Parable-managed launch."""
    raw = os.environ.get(AGENT_STATE_ENV)
    if raw is None:
        return None
    try:
        values = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(values, dict):
        return {}
    expected = {"active", "unavailable", "parent"}
    if set(values) != expected:
        return {}
    if any(
        not isinstance(items, list)
        or not all(isinstance(item, str) for item in items)
        for items in values.values()
    ):
        return {}
    return {status: set(items) for status, items in values.items()}


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

    state = agent_state()
    if state is not None and agent_type in state.get("unavailable", set()):
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

    if state is not None and agent_type in state.get("parent", set()):
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"{agent_type} uses the current parent model; work directly "
                    "instead of delegating back to the parent lane."
                ),
            }
        }, separators=(",", ":")))
        return

    if state is not None and agent_type not in state.get("active", set()):
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"{agent_type} is not active in this Parable session."
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
