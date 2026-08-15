"""Canonical Slack identities and records shared by every acquisition path."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlsplit

from connectors.sdk import ConnectorContractError, ConnectorRecordV2


SLACK_PUBLIC_HISTORY_USER_SCOPES = (
    "channels:history",
    "channels:read",
    "files:read",
)


SLACK_ID = re.compile(r"[A-Z][A-Z0-9]{1,31}\Z")
SLACK_TS = re.compile(r"[0-9]{1,16}(?:\.[0-9]{1,6})?\Z")
MAX_TEXT_BYTES = 500_000
MAX_ATTACHMENTS = 20


def _slack_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or SLACK_ID.fullmatch(value) is None:
        raise ConnectorContractError(f"slack {label} is invalid")
    return value


def _slack_ts(value: Any, label: str) -> str:
    if not isinstance(value, str) or SLACK_TS.fullmatch(value) is None:
        raise ConnectorContractError(f"slack {label} is invalid")
    return value


def slack_time(value: Any) -> str:
    raw = _slack_ts(value, "timestamp")
    whole, _, fraction = raw.partition(".")
    try:
        parsed = datetime.fromtimestamp(int(whole), timezone.utc).replace(
            microsecond=int((fraction + "000000")[:6])
        )
    except (OSError, OverflowError, ValueError):
        raise ConnectorContractError("slack timestamp is invalid") from None
    return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")


def slack_message_id(workspace_id: str, channel_id: str, timestamp: str) -> str:
    return (
        f"slack:{_slack_id(workspace_id, 'workspace id')}:"
        f"{_slack_id(channel_id, 'channel id')}:{_slack_ts(timestamp, 'timestamp')}"
    )


def slack_channel_id(workspace_id: str, channel_id: str) -> str:
    return (
        f"slack-channel:{_slack_id(workspace_id, 'workspace id')}:"
        f"{_slack_id(channel_id, 'channel id')}"
    )


def slack_thread_id(workspace_id: str, channel_id: str, timestamp: str) -> str:
    return (
        f"slack-thread:{_slack_id(workspace_id, 'workspace id')}:"
        f"{_slack_id(channel_id, 'channel id')}:{_slack_ts(timestamp, 'thread timestamp')}"
    )


def slack_actor_id(workspace_id: str, user_id: str) -> str:
    return (
        f"slack:{_slack_id(workspace_id, 'workspace id')}:"
        f"{_slack_id(user_id, 'user id')}"
    )


def _text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    encoded = value.encode(errors="replace")[:MAX_TEXT_BYTES]
    return encoded.decode(errors="ignore")


def _https(value: Any) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 4096:
        return None
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        return None
    return value


def _attachments(files: Any) -> list[dict[str, Any]]:
    if files is None:
        return []
    if not isinstance(files, list) or len(files) > MAX_ATTACHMENTS:
        raise ConnectorContractError("slack files are invalid")
    result = []
    for value in files:
        if not isinstance(value, dict):
            raise ConnectorContractError("slack file is invalid")
        file_id = value.get("id")
        if not isinstance(file_id, str) or SLACK_ID.fullmatch(file_id) is None:
            raise ConnectorContractError("slack file id is invalid")
        item: dict[str, Any] = {"file_id": file_id}
        for source, target in (
            ("name", "name"), ("title", "title"), ("mimetype", "mime_type"),
            ("filetype", "file_type"), ("mode", "mode"),
        ):
            raw = value.get(source)
            if isinstance(raw, str) and raw and len(raw.encode()) <= 4096:
                item[target] = raw
        size = value.get("size")
        if type(size) is int and 0 <= size <= 10**12:
            item["size_bytes"] = size
        url = _https(value.get("permalink"))
        if url:
            item["source_url"] = url
        result.append(item)
    return result


def normalize_slack_message(
    *,
    workspace_id: str,
    channel_id: str,
    value: dict[str, Any],
    owner_identifiers: Iterable[str] = (),
    provenance_surface: str = "api",
) -> ConnectorRecordV2:
    """Normalize history, reply, archive, or Events API message payloads."""

    if not isinstance(value, dict):
        raise ConnectorContractError("slack message is invalid")
    event = value
    subtype = event.get("subtype")
    if subtype == "message_deleted":
        timestamp = _slack_ts(event.get("deleted_ts"), "deleted timestamp")
        occurred = slack_time(event.get("event_ts") or event.get("ts") or timestamp)
        native_id = slack_message_id(workspace_id, channel_id, timestamp)
        return ConnectorRecordV2(
            schema_version=2,
            native_id=native_id,
            occurred_at=occurred,
            content={"kind": "communication_message.v1"},
            provenance={"uri": f"connector://slack/{provenance_surface}/{native_id}"},
            deleted=True,
        )
    edited_at = None
    if subtype == "message_changed":
        event = value.get("message")
        if not isinstance(event, dict):
            raise ConnectorContractError("slack changed message is invalid")
        edited = event.get("edited")
        if isinstance(edited, dict):
            edited_at = slack_time(edited.get("ts"))
    timestamp = _slack_ts(event.get("ts"), "message timestamp")
    sent_at = slack_time(timestamp)
    thread_timestamp = event.get("thread_ts") or timestamp
    thread_timestamp = _slack_ts(thread_timestamp, "thread timestamp")
    native_id = slack_message_id(workspace_id, channel_id, timestamp)
    thread_id = slack_thread_id(workspace_id, channel_id, thread_timestamp)
    raw_author = event.get("user") or event.get("bot_id")
    author_id = None
    if raw_author is not None:
        author_id = slack_actor_id(workspace_id, _slack_id(raw_author, "author id"))
    owners = {str(item).casefold() for item in owner_identifiers}
    direction = "system"
    if author_id:
        direction = (
            "outbound"
            if author_id.casefold() in owners or str(raw_author).casefold() in owners
            else "inbound"
        )
    attachments = _attachments(event.get("files"))
    content: dict[str, Any] = {
        "kind": "communication_message.v1",
        "content_fidelity": "complete",
        "conversation_id": thread_id,
        "direction": direction,
        "format": "slack-message",
        "message_id": native_id,
        "sent_at": sent_at,
        "surface": "slack",
        "text": _text(event.get("text")),
    }
    if author_id:
        content["author_id"] = author_id
        content["participant_ids"] = [author_id]
    if thread_timestamp != timestamp:
        content["reply_to_id"] = slack_message_id(
            workspace_id, channel_id, thread_timestamp
        )
    if edited_at:
        content["edited_at"] = edited_at
    if attachments:
        content["attachments"] = attachments
        content["content_fidelity"] = "partial"
        content["content_omissions"] = ["attachment_bytes"]
    permalink = _https(event.get("permalink"))
    if permalink:
        content["source_url"] = permalink
    return ConnectorRecordV2(
        schema_version=2,
        native_id=native_id,
        native_parent_id=thread_id,
        occurred_at=sent_at,
        content=content,
        provenance={"uri": f"connector://slack/{provenance_surface}/{native_id}"},
    )


def normalize_slack_user(
    *, workspace_id: str, value: dict[str, Any], owner_user_ids: Iterable[str] = (),
) -> tuple[ConnectorRecordV2, ...]:
    """Emit stable native-ID and verified-email identity records for actor binding."""

    if not isinstance(value, dict):
        raise ConnectorContractError("slack user is invalid")
    user_id = _slack_id(value.get("id"), "user id")
    actor_id = slack_actor_id(workspace_id, user_id)
    profile = value.get("profile") if isinstance(value.get("profile"), dict) else {}
    display_name = profile.get("real_name") or profile.get("display_name") or value.get("name")
    if not isinstance(display_name, str) or not display_name:
        display_name = user_id
    base = {
        "kind": "contact_identity.v1",
        "content_fidelity": "complete",
        "display_name": _text(display_name),
        "identity_id": actor_id,
        "identifier": user_id,
        "identifier_type": "slack_user_id",
        "role": "self" if user_id in set(owner_user_ids) else "other",
        "surface": "slack",
        "text": _text(profile.get("title")),
    }
    records = [ConnectorRecordV2(
        schema_version=2,
        native_id=f"slack-user:{_slack_id(workspace_id, 'workspace id')}:{user_id}",
        occurred_at="1970-01-01T00:00:00Z",
        content=base,
        provenance={"uri": f"connector://slack/users/{actor_id}"},
    )]
    email = profile.get("email")
    if isinstance(email, str) and email.strip():
        normalized = email.strip().casefold()
        records.append(ConnectorRecordV2(
            schema_version=2,
            native_id=f"slack-email:{_slack_id(workspace_id, 'workspace id')}:{user_id}",
            occurred_at="1970-01-01T00:00:00Z",
            content={
                **base,
                "identity_id": f"{actor_id}:email",
                "identifier": normalized,
                "identifier_type": "email",
            },
            provenance={"uri": f"connector://slack/users/{actor_id}/email"},
        ))
    return tuple(records)


__all__ = [
    "SLACK_PUBLIC_HISTORY_USER_SCOPES",
    "normalize_slack_message",
    "normalize_slack_user",
    "slack_actor_id",
    "slack_channel_id",
    "slack_message_id",
    "slack_thread_id",
    "slack_time",
]
