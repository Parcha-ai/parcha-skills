"""Slack Events API adapter for the generic Recall webhook sink."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any

from connectors.sdk import ConnectorContractError
from connectors.slack_source import normalize_slack_message


MAX_EVENT_BYTES = 256 * 1024
MAX_EVENT_AGE_SECONDS = 300


@dataclass(frozen=True)
class SlackEventResult:
    event_id: str
    retry_number: int | None
    workspace_id: str | None = None
    webhook: dict[str, Any] | None = None
    challenge: str | None = None


def verify_slack_signature(
    *, body: bytes, timestamp: str, signature: str, signing_secret: str,
    now: int | None = None,
) -> None:
    if (
        not isinstance(body, bytes) or not body or len(body) > MAX_EVENT_BYTES
        or not isinstance(timestamp, str) or not timestamp.isdigit()
        or not isinstance(signature, str) or not signature.startswith("v0=")
        or not isinstance(signing_secret, str) or not signing_secret
    ):
        raise ConnectorContractError("slack event signature is invalid")
    current = int(time.time()) if now is None else now
    if abs(current - int(timestamp)) > MAX_EVENT_AGE_SECONDS:
        raise ConnectorContractError("slack event signature is stale")
    expected = "v0=" + hmac.new(
        signing_secret.encode(), b"v0:" + timestamp.encode() + b":" + body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ConnectorContractError("slack event signature is invalid")


def slack_event_to_webhook(
    *, body: bytes, timestamp: str, signature: str, signing_secret: str,
    expected_workspace_id: str | None = None, retry_number: str | None = None,
    now: int | None = None,
) -> SlackEventResult:
    """Verify Slack, then project a message event to Recall's closed webhook v1."""

    verify_slack_signature(
        body=body, timestamp=timestamp, signature=signature,
        signing_secret=signing_secret, now=now,
    )
    try:
        value = json.loads(body, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise ConnectorContractError("slack event body is invalid") from None
    if not isinstance(value, dict):
        raise ConnectorContractError("slack event body is invalid")
    if value.get("type") == "url_verification":
        challenge = value.get("challenge")
        if not isinstance(challenge, str) or not challenge or len(challenge) > 4096:
            raise ConnectorContractError("slack event challenge is invalid")
        return SlackEventResult(
            event_id="url_verification", retry_number=None, challenge=challenge,
        )
    if value.get("type") != "event_callback":
        raise ConnectorContractError("slack event type is unsupported")
    event_id = value.get("event_id")
    workspace_id = value.get("team_id")
    event = value.get("event")
    if (
        not isinstance(event_id, str) or not event_id
        or not isinstance(workspace_id, str)
        or not isinstance(event, dict)
        or event.get("type") != "message"
        or (expected_workspace_id is not None and workspace_id != expected_workspace_id)
    ):
        raise ConnectorContractError("slack event is invalid")
    channel_id = event.get("channel")
    if not isinstance(channel_id, str):
        raise ConnectorContractError("slack event channel is invalid")
    record = normalize_slack_message(
        workspace_id=workspace_id, channel_id=channel_id, value=event,
        provenance_surface="events",
    )
    retry = None
    if retry_number is not None:
        if not retry_number.isdigit() or int(retry_number) > 100:
            raise ConnectorContractError("slack retry number is invalid")
        retry = int(retry_number)
    return SlackEventResult(
        event_id=event_id,
        retry_number=retry,
        workspace_id=workspace_id,
        webhook={
            "schema_version": 1,
            "event_id": record.native_id,
            "parent_id": record.native_parent_id,
            "occurred_at": record.occurred_at,
            "record": record.content,
            "deleted": record.deleted,
        },
    )


__all__ = ["SlackEventResult", "slack_event_to_webhook", "verify_slack_signature"]
