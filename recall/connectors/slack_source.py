"""Canonical Slack identities and records shared by every acquisition path."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlsplit

from connectors.sdk import ConnectorContractError, ConnectorRecordV2


# Increment when provider replay is required to recover previously omitted content.
SLACK_MESSAGE_CAPTURE_VERSION = 2

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


def _message_text(event: dict[str, Any]) -> tuple[str, list[str]]:
    """Read visible Slack prose, not arbitrary strings from interactive payloads.

    Blocks replace notification text; attachment fallback replaces missing card
    prose. Repeated visible blocks/fields remain repeated occurrences. Size
    enforcement belongs to the record contract, never silent text clipping.
    """
    omissions: set[str] = set()

    def string(value: Any) -> str:
        return value if isinstance(value, str) else ""

    def lines(values: Iterable[str]) -> str:
        return "\n".join(value for value in values if value)

    def items(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        omissions.add("unsupported_slack_blocks")
        return []

    def render(value: Any) -> str:
        if not isinstance(value, dict):
            omissions.add("unsupported_slack_blocks")
            return ""
        kind = value.get("type")
        if kind in {"text", "plain_text", "mrkdwn", "markdown"}:
            return string(value.get("text"))
        if kind == "section":
            return lines(
                [
                    render(value["text"]) if value.get("text") else "",
                    *[render(item) for item in items(value.get("fields"))],
                    render(value["accessory"]) if value.get("accessory") else "",
                ]
            )
        if kind == "header":
            return render(value.get("text"))
        if kind in {"rich_text", "rich_text_list", "context", "actions"}:
            return lines(render(item) for item in items(value.get("elements")))
        if kind in {"rich_text_section", "rich_text_quote", "rich_text_preformatted"}:
            return "".join(render(item) for item in items(value.get("elements")))
        if kind == "table":
            return lines(
                " | ".join(render(cell) for cell in items(row))
                for row in items(value.get("rows"))
            )
        if kind in {"button", "workflow_button"}:
            return render(value.get("text"))
        if kind == "image":
            omissions.add("image_bytes")
            return lines(
                [
                    render(value["title"]) if value.get("title") else "",
                    string(value.get("alt_text")),
                ]
            )
        if kind == "link":
            url, label = string(value.get("url")), string(value.get("text"))
            return f"<{url}|{label}>" if url and label else (url or label)
        if kind in {"user", "channel", "usergroup"}:
            identifier = string(
                value.get(
                    {
                        "user": "user_id",
                        "channel": "channel_id",
                        "usergroup": "usergroup_id",
                    }[kind]
                )
            )
            prefix = {"user": "@", "channel": "#", "usergroup": "!subteam^"}[kind]
            return f"<{prefix}{identifier}>" if identifier else ""
        if kind == "emoji":
            name = string(value.get("name"))
            return f":{name}:" if name else ""
        if kind == "broadcast":
            name = string(value.get("range"))
            return f"<!{name}>" if name else ""
        if kind == "date":
            timestamp, format_text = value.get("timestamp"), string(value.get("format"))
            if type(timestamp) is int and format_text:
                url = string(value.get("url"))
                fallback = string(value.get("fallback"))
                return (
                    f"<!date^{timestamp}^{format_text}"
                    + (f"^{url}" if url else "")
                    + (f"|{fallback}" if fallback else "")
                    + ">"
                )
            omissions.add("unsupported_slack_blocks")
            return string(value.get("fallback"))
        if kind == "divider":
            return ""
        omissions.add("unsupported_slack_blocks")
        return ""

    def blocks(value: Any) -> str:
        if not isinstance(value, list):
            if value is not None:
                omissions.add("unsupported_slack_blocks")
            return ""
        return lines(render(item) for item in value)

    top = blocks(event.get("blocks")) or string(event.get("text"))
    parts = [top]
    cards = event.get("attachments") or []
    if not isinstance(cards, list):
        omissions.add("unsupported_slack_attachments")
        cards = []
    for card in cards:
        if not isinstance(card, dict):
            omissions.add("unsupported_slack_attachments")
            continue
        if card.get("image_url") or card.get("thumb_url"):
            omissions.add("image_bytes")
        body = [string(card.get("pretext")), string(card.get("author_name"))]
        visible_blocks = blocks(card.get("blocks"))
        if visible_blocks:
            body.append(visible_blocks)
        else:
            title, title_link = (
                string(card.get("title")),
                string(card.get("title_link")),
            )
            body.extend(
                [
                    f"<{title_link}|{title}>" if title and title_link else title,
                    string(card.get("text")),
                ]
            )
            for field in items(card.get("fields")):
                if isinstance(field, dict):
                    body.append(
                        ": ".join(
                            item
                            for item in (
                                string(field.get("title")),
                                string(field.get("value")),
                            )
                            if item
                        )
                    )
                else:
                    omissions.add("unsupported_slack_attachments")
        body.append(string(card.get("footer")))
        visible = lines(body)
        if visible:
            parts.append(visible)
        else:
            fallback = string(card.get("fallback"))
            # Top-level text sometimes repeats the exact card notification fallback.
            if fallback != top:
                parts.append(fallback)
    return lines(parts), sorted(omissions)


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
    message_text, omissions = _message_text(event)
    content: dict[str, Any] = {
        "kind": "communication_message.v1",
        "content_fidelity": "complete",
        "conversation_id": thread_id,
        "direction": direction,
        "format": "slack-message",
        "message_id": native_id,
        "sent_at": sent_at,
        "surface": "slack",
        "text": message_text,
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
        omissions.append("attachment_bytes")
    if omissions:
        content["content_fidelity"] = "partial"
        content["content_omissions"] = sorted(set(omissions))
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
    "SLACK_MESSAGE_CAPTURE_VERSION",
    "SLACK_PUBLIC_HISTORY_USER_SCOPES",
    "normalize_slack_message",
    "normalize_slack_user",
    "slack_actor_id",
    "slack_channel_id",
    "slack_message_id",
    "slack_thread_id",
    "slack_time",
]
