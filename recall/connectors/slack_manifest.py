"""Generate the least-privilege Slack app manifest for one Recall deployment."""

from __future__ import annotations

import json
import sys
from urllib.parse import urlsplit

from connectors.registry import definition
from connectors.slack_source import SLACK_PUBLIC_HISTORY_USER_SCOPES


def slack_app_manifest(public_origin: str, *, events: bool = False) -> dict[str, object]:
    parsed = urlsplit(public_origin)
    if (
        parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
        or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
    ):
        raise ValueError("public origin must be an exact HTTPS origin")
    origin = f"https://{parsed.netloc}"
    scopes = list(definition("slack.messages").auth.minimum_scopes)
    settings: dict[str, object] = {
        "interactivity": {"is_enabled": False},
        "org_deploy_enabled": False,
        "socket_mode_enabled": False,
        "token_rotation_enabled": False,
    }
    if events:
        settings["event_subscriptions"] = {
            "request_url": f"{origin}/webhooks/v1/slack",
            "bot_events": ["message.channels"],
        }
    return {
        "display_information": {
            "name": "Recall",
            "description": "Private workspace memory",
        },
        "features": {
            "bot_user": {"display_name": "Recall", "always_online": False},
        },
        "oauth_config": {
            "redirect_urls": [f"{origin}/admin/oauth/callback/slack"],
            "scopes": {
                "bot": scopes,
                "user": list(SLACK_PUBLIC_HISTORY_USER_SCOPES),
            },
        },
        "settings": settings,
    }


def main(argv: list[str] | None = None) -> int:
    values = sys.argv[1:] if argv is None else argv
    events = False
    if values and values[0] == "--events":
        events = True
        values = values[1:]
    if len(values) != 1:
        print(
            "usage: python -m connectors.slack_manifest [--events] https://recall.example",
            file=sys.stderr,
        )
        return 2
    try:
        value = slack_app_manifest(values[0], events=events)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["slack_app_manifest"]
