#!/usr/bin/env python3
"""Keep generated Parable agent frontmatter authoritative."""

import json
import sys


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
    if (
        not isinstance(agent_type, str)
        or not agent_type.startswith("parable-")
        or "model" not in tool_input
    ):
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
