"""Workspace-wide Slack backfill and reconciliation connector."""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from connectors.remote_api import RemoteApiError
from connectors.sdk import (
    ConnectorContractError, ConnectorPage, ConnectorRecordV2,
    ConnectorUpstreamError, SOURCE_ID,
)
from connectors.slack_source import normalize_slack_message, normalize_slack_user
from connectors.attachment_extract import extract_attachment_text


EPOCH = "1970-01-01T00:00:00Z"
CHANNEL_BATCH_SIZE = 50
MAX_CONFIGURED_CHANNELS = 128


class JsonRail(Protocol):
    def request(
        self, operation_id: str, *, path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> Any: ...


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _oldest(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return f"{int(parsed.timestamp())}.{parsed.microsecond:06d}"


def _response(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("ok") is not True:
        raise ConnectorUpstreamError("connector_upstream_error")
    return value


def _items(value: Any, label: str, maximum: int = 500) -> list[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ConnectorContractError(f"slack {label} are invalid")
    return value


def _next(response: dict[str, Any]) -> str | None:
    metadata = response.get("response_metadata", {})
    if not isinstance(metadata, dict):
        raise ConnectorContractError("slack response metadata is invalid")
    value = metadata.get("next_cursor") or None
    if value is not None and (not isinstance(value, str) or len(value) > 4096):
        raise ConnectorContractError("slack page cursor is invalid")
    return value


def _cursor(value: dict[str, Any]) -> str:
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise ConnectorContractError("slack workspace cursor is invalid") from None
    if not raw or len(raw.encode()) > 4096:
        raise ConnectorContractError("slack workspace cursor is invalid")
    return raw


def _initial_state(
    *, watermark: str = EPOCH, upper: str | None = None, cycle: int = 0,
    public_history: bool = False,
) -> dict[str, Any]:
    return {
        "v": 3,
        "coverage": "public" if public_history else "member",
        "phase": "users",
        "page": None,
        "discovery_page": None,
        "channels": [],
        "configured_index": 0,
        "channel_index": 0,
        "threads": [],
        "thread_index": 0,
        "thread_page": None,
        "watermark": watermark,
        "upper": upper or _now(),
        "cycle": cycle,
        "found": False,
    }


def _valid_time_bounds(value: Mapping[str, Any]) -> bool:
    try:
        datetime.fromisoformat(value["watermark"].replace("Z", "+00:00"))
        datetime.fromisoformat(value["upper"].replace("Z", "+00:00"))
    except (AttributeError, KeyError, ValueError):
        return False
    return True


def _state(
    raw: str | None,
    configured_channels: tuple[str, ...],
    public_history: bool,
) -> dict[str, Any]:
    if raw is None:
        return _initial_state(public_history=public_history)
    try:
        value = json.loads(raw, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
    except (TypeError, json.JSONDecodeError, ValueError):
        raise ConnectorContractError("slack workspace cursor is invalid") from None

    legacy_expected = {
        "v", "phase", "page", "channels", "channel_index", "threads",
        "thread_index", "thread_page", "watermark", "upper", "cycle",
    }
    if isinstance(value, dict) and set(value) == legacy_expected and value.get("v") == 1:
        if (
            value.get("phase") not in {"discover", "users", "history", "threads"}
            or not isinstance(value.get("channels"), list)
            or len(value["channels"]) > MAX_CONFIGURED_CHANNELS
            or any(not isinstance(item, str) or len(item) > 40 for item in value["channels"])
            or not isinstance(value.get("threads"), list) or len(value["threads"]) > 100
            or any(not isinstance(item, str) or len(item) > 32 for item in value["threads"])
            or type(value.get("channel_index")) is not int
            or not 0 <= value["channel_index"] <= len(value["channels"])
            or type(value.get("thread_index")) is not int
            or not 0 <= value["thread_index"] <= len(value["threads"])
            or type(value.get("cycle")) is not int or value["cycle"] < 0
            or any(value.get(key) is not None and not isinstance(value[key], str)
                   for key in ("page", "thread_page"))
            or not _valid_time_bounds(value)
        ):
            raise ConnectorContractError("slack workspace cursor is invalid")
        # V1 accumulated the entire workspace in one cursor. Restart its bounded
        # interval under V3; stable Slack native IDs make the replay idempotent.
        return _initial_state(
            watermark=EPOCH if public_history else value["watermark"],
            upper=None if public_history else value["upper"],
            cycle=0 if public_history else value["cycle"],
            public_history=public_history,
        )

    v2_expected = {
        "v", "phase", "page", "discovery_page", "channels", "configured_index",
        "channel_index", "threads", "thread_index", "thread_page", "watermark",
        "upper", "cycle", "found",
    }
    if isinstance(value, dict) and set(value) == v2_expected and value.get("v") == 2:
        if (
            value.get("phase") not in {"discover", "users", "history", "threads"}
            or not isinstance(value.get("channels"), list)
            or len(value["channels"]) > CHANNEL_BATCH_SIZE
            or any(not isinstance(item, str) or len(item) > 40 for item in value["channels"])
            or not isinstance(value.get("threads"), list) or len(value["threads"]) > 100
            or any(not isinstance(item, str) or len(item) > 32 for item in value["threads"])
            or type(value.get("configured_index")) is not int
            or not 0 <= value["configured_index"] <= len(configured_channels)
            or type(value.get("channel_index")) is not int
            or not 0 <= value["channel_index"] <= len(value["channels"])
            or type(value.get("thread_index")) is not int
            or not 0 <= value["thread_index"] <= len(value["threads"])
            or type(value.get("cycle")) is not int or value["cycle"] < 0
            or type(value.get("found")) is not bool
            or any(value.get(key) is not None and not isinstance(value[key], str)
                   for key in ("page", "discovery_page", "thread_page"))
            or not _valid_time_bounds(value)
        ):
            raise ConnectorContractError("slack workspace cursor is invalid")
        # Public-history authority broadens coverage to archived and unjoined
        # public channels, so replay from epoch once. Stable native IDs dedupe.
        return _initial_state(
            watermark=EPOCH if public_history else value["watermark"],
            upper=None if public_history else value["upper"],
            cycle=0 if public_history else value["cycle"],
            public_history=public_history,
        )

    expected = v2_expected | {"coverage"}
    expected_coverage = "public" if public_history else "member"
    if (
        not isinstance(value, dict) or set(value) != expected or value.get("v") != 3
        or value.get("coverage") not in {"member", "public"}
        or value.get("phase") not in {"discover", "users", "history", "threads"}
        or not isinstance(value.get("channels"), list)
        or len(value["channels"]) > CHANNEL_BATCH_SIZE
        or any(not isinstance(item, str) or len(item) > 40 for item in value["channels"])
        or not isinstance(value.get("threads"), list) or len(value["threads"]) > 100
        or any(not isinstance(item, str) or len(item) > 32 for item in value["threads"])
        or type(value.get("configured_index")) is not int
        or not 0 <= value["configured_index"] <= len(configured_channels)
        or type(value.get("channel_index")) is not int
        or not 0 <= value["channel_index"] <= len(value["channels"])
        or type(value.get("thread_index")) is not int
        or not 0 <= value["thread_index"] <= len(value["threads"])
        or type(value.get("cycle")) is not int or value["cycle"] < 0
        or type(value.get("found")) is not bool
        or any(value.get(key) is not None and not isinstance(value[key], str)
               for key in ("page", "discovery_page", "thread_page"))
        or not _valid_time_bounds(value)
    ):
        raise ConnectorContractError("slack workspace cursor is invalid")
    if value["phase"] in {"history", "threads"} and (
        not value["channels"] or value["channel_index"] >= len(value["channels"])
    ):
        raise ConnectorContractError("slack workspace cursor is invalid")
    if value["phase"] == "threads" and (
        not value["threads"] or value["thread_index"] >= len(value["threads"])
    ):
        raise ConnectorContractError("slack workspace cursor is invalid")
    if value["coverage"] != expected_coverage:
        return _initial_state(public_history=public_history)
    return value


class SlackWorkspaceConnector:
    """Discover all accessible channels, then backfill each through a fixed upper bound."""

    connector_id = "slack.messages"

    def __init__(
        self, *, rail: JsonRail, source_id: str, workspace_id: str,
        owner_user_ids: tuple[str, ...] = (), channel_ids: tuple[str, ...] = (),
        page_size: int = 20,
    ):
        if not callable(getattr(rail, "request", None)):
            raise ConnectorContractError("remote rail is invalid")
        if not isinstance(source_id, str) or SOURCE_ID.fullmatch(source_id) is None:
            raise ConnectorContractError("source_id is invalid")
        if not isinstance(workspace_id, str) or not workspace_id:
            raise ConnectorContractError("slack workspace is invalid")
        if (
            not isinstance(owner_user_ids, tuple) or not isinstance(channel_ids, tuple)
            or len(channel_ids) > MAX_CONFIGURED_CHANNELS
            or any(not isinstance(item, str) or not item for item in owner_user_ids + channel_ids)
            or len(channel_ids) != len(set(channel_ids))
            or not 1 <= page_size <= 20
        ):
            raise ConnectorContractError("slack workspace configuration is invalid")
        self.rail = rail
        self.source_id = source_id
        self.workspace_id = workspace_id
        self.owner_user_ids = owner_user_ids
        self.channel_ids = channel_ids
        self.page_size = page_size
        self.public_history = getattr(rail, "public_history", False) is True

    def _request(self, operation: str, query: dict[str, Any]) -> dict[str, Any]:
        try:
            return _response(self.rail.request(operation, query=query))
        except RemoteApiError:
            raise ConnectorUpstreamError("connector_upstream_error") from None

    def pull(self, cursor: str | None) -> ConnectorPage:
        state = _state(cursor, self.channel_ids, self.public_history)
        return getattr(self, f"_pull_{state['phase']}")(state)

    def _page(self, records: list[ConnectorRecordV2], state: dict[str, Any], more: bool) -> ConnectorPage:
        return ConnectorPage(records=tuple(records), next_cursor=_cursor(state), has_more=more)

    def _pull_discover(self, state: dict[str, Any]) -> ConnectorPage:
        query: dict[str, Any] = {
            "exclude_archived": not self.public_history,
            "limit": CHANNEL_BATCH_SIZE,
            "types": "public_channel",
        }
        if state["discovery_page"]:
            query["cursor"] = state["discovery_page"]
        response = self._request("channels.list", query)
        channels = []
        for raw in _items(response.get("channels"), "channels", CHANNEL_BATCH_SIZE):
            if not isinstance(raw, dict) or not isinstance(raw.get("id"), str):
                raise ConnectorContractError("slack channel is invalid")
            if raw.get("is_private") or raw.get("is_im") or raw.get("is_mpim"):
                continue
            do_not_join = bool(raw.get("is_archived"))
            encoded = f"{raw['id']}:{1 if do_not_join else 0}"
            if encoded not in channels:
                channels.append(encoded)
        channels.sort()
        next_page = _next(response)
        state = {
            **state, "channels": channels, "channel_index": 0,
            "discovery_page": next_page, "found": state["found"] or bool(channels),
        }
        if channels:
            return self._page([], {**state, "phase": "history", "page": None}, True)
        if next_page:
            return self._page([], state, True)
        if not state["found"]:
            raise ConnectorUpstreamError("slack_no_accessible_channels")
        return self._finish_cycle([], state)

    def _pull_users(self, state: dict[str, Any]) -> ConnectorPage:
        query: dict[str, Any] = {"include_locale": False, "limit": self.page_size}
        if state["page"]:
            query["cursor"] = state["page"]
        response = self._request("users.list", query)
        records: list[ConnectorRecordV2] = []
        for raw in _items(response.get("members"), "users"):
            if not isinstance(raw, dict):
                raise ConnectorContractError("slack user is invalid")
            records.extend(normalize_slack_user(
                workspace_id=self.workspace_id, value=raw,
                owner_user_ids=self.owner_user_ids,
            ))
        next_page = _next(response)
        state = {**state, "page": next_page}
        if next_page:
            return self._page(records, state, True)
        state = {**state, "page": None}
        if self.channel_ids:
            return self._page(records, self._configured_batch(state), True)
        return self._page(records, {**state, "phase": "discover"}, True)

    def _configured_batch(self, state: dict[str, Any]) -> dict[str, Any]:
        start = state["configured_index"]
        stop = min(start + CHANNEL_BATCH_SIZE, len(self.channel_ids))
        return {
            **state,
            "phase": "history",
            "channels": [
                f"{item}:{1 if self.public_history else 0}"
                for item in self.channel_ids[start:stop]
            ],
            "configured_index": stop,
            "channel_index": 0,
            "page": None,
            "threads": [],
            "thread_index": 0,
            "thread_page": None,
        }

    @staticmethod
    def _channel(state: dict[str, Any]) -> tuple[str, bool]:
        encoded = state["channels"][state["channel_index"]]
        channel_id, do_not_join = encoded.rsplit(":", 1)
        return channel_id, do_not_join == "1"

    def _pull_history(self, state: dict[str, Any]) -> ConnectorPage:
        channel_id, do_not_join = self._channel(state)
        if not do_not_join and state["page"] is None:
            self._request("channels.join", {"channel": channel_id})
        query: dict[str, Any] = {
            "channel": channel_id, "inclusive": True, "limit": self.page_size,
            "latest": _oldest(state["upper"]),
        }
        if state["page"]:
            query["cursor"] = state["page"]
        elif state["watermark"] != EPOCH:
            query["oldest"] = _oldest(state["watermark"])
        response = self._request("messages.history", query)
        records: dict[str, ConnectorRecordV2] = {}
        threads = []
        for raw in _items(response.get("messages"), "messages"):
            if not isinstance(raw, dict):
                raise ConnectorContractError("slack message is invalid")
            record = normalize_slack_message(
                workspace_id=self.workspace_id, channel_id=channel_id, value=raw,
                owner_identifiers=self.owner_user_ids, provenance_surface="api",
            )
            record, attachments = self._capture_files(raw, record)
            records[record.native_id] = record
            for attachment in attachments:
                records[attachment.native_id] = attachment
            reply_count = raw.get("reply_count", 0)
            if type(reply_count) is not int or reply_count < 0:
                raise ConnectorContractError("slack reply count is invalid")
            if reply_count:
                threads.append(raw["ts"])
        next_page = _next(response)
        if threads:
            next_state = {
                **state, "phase": "threads", "page": next_page,
                "threads": threads, "thread_index": 0, "thread_page": None,
            }
            return self._page(list(records.values()), next_state, True)
        return self._advance_channel(list(records.values()), state, next_page)

    def _pull_threads(self, state: dict[str, Any]) -> ConnectorPage:
        channel_id, _do_not_join = self._channel(state)
        thread_ts = state["threads"][state["thread_index"]]
        query: dict[str, Any] = {
            "channel": channel_id, "inclusive": True, "limit": self.page_size,
            "ts": thread_ts, "latest": _oldest(state["upper"]),
        }
        if state["thread_page"]:
            query["cursor"] = state["thread_page"]
        response = self._request("messages.replies", query)
        records: dict[str, ConnectorRecordV2] = {}
        for raw in _items(response.get("messages"), "replies"):
            if not isinstance(raw, dict):
                raise ConnectorContractError("slack reply is invalid")
            record = normalize_slack_message(
                workspace_id=self.workspace_id, channel_id=channel_id, value=raw,
                owner_identifiers=self.owner_user_ids, provenance_surface="api",
            )
            record, attachments = self._capture_files(raw, record)
            records[record.native_id] = record
            for attachment in attachments:
                records[attachment.native_id] = attachment
        next_thread_page = _next(response)
        if next_thread_page:
            return self._page(list(records.values()), {
                **state, "thread_page": next_thread_page,
            }, True)
        index = state["thread_index"] + 1
        if index < len(state["threads"]):
            return self._page(list(records.values()), {
                **state, "thread_index": index, "thread_page": None,
            }, True)
        return self._advance_channel(list(records.values()), state, state["page"])

    def _advance_channel(
        self, records: list[ConnectorRecordV2], state: dict[str, Any],
        history_page: str | None,
    ) -> ConnectorPage:
        if history_page:
            return self._page(records, {
                **state, "phase": "history", "page": history_page,
                "threads": [], "thread_index": 0, "thread_page": None,
            }, True)
        index = state["channel_index"] + 1
        if index < len(state["channels"]):
            return self._page(records, {
                **state, "phase": "history", "page": None,
                "channel_index": index, "threads": [], "thread_index": 0,
                "thread_page": None,
            }, True)
        if self.channel_ids and state["configured_index"] < len(self.channel_ids):
            return self._page(records, self._configured_batch(state), True)
        if not self.channel_ids and state["discovery_page"]:
            return self._page(records, {
                **state, "phase": "discover", "page": None, "channels": [],
                "channel_index": 0, "threads": [], "thread_index": 0,
                "thread_page": None,
            }, True)
        return self._finish_cycle(records, state)

    def _finish_cycle(
        self, records: list[ConnectorRecordV2], state: dict[str, Any],
    ) -> ConnectorPage:
        return self._page(records, {
            **_initial_state(
                watermark=state["upper"], upper=_now(), cycle=state["cycle"] + 1,
                public_history=self.public_history,
            ),
        }, False)

    def _file_records(
        self, message: dict[str, Any], parent: ConnectorRecordV2,
    ) -> list[ConnectorRecordV2]:
        files = message.get("files") or []
        if not isinstance(files, list) or len(files) > 20:
            raise ConnectorContractError("slack files are invalid")
        downloader = getattr(self.rail, "download_binary", None)
        if not files or not callable(downloader):
            return []
        result = []
        for value in files:
            if not isinstance(value, dict):
                raise ConnectorContractError("slack file is invalid")
            file_id = value.get("id")
            url = value.get("url_private_download") or value.get("url_private")
            if not isinstance(file_id, str) or not isinstance(url, str):
                continue
            try:
                payload, media_type = downloader(url)
            except RemoteApiError:
                continue
            extraction = extract_attachment_text(payload, media_type)
            native_id = f"slack-file:{self.workspace_id}:{file_id}"
            name = value.get("name") or value.get("title") or file_id
            if not isinstance(name, str) or not name:
                name = file_id
            content: dict[str, Any] = {
                "kind": "document.v1",
                "content_fidelity": "complete",
                "document_id": native_id,
                "mime_type": media_type,
                "name": name[:10_000],
                "parent_id": parent.native_id,
                "surface": "slack",
                "artifact_content_sha256": hashlib.sha256(payload).hexdigest(),
            }
            if extraction.text:
                content["text"] = extraction.text
            author = value.get("user")
            if isinstance(author, str) and author:
                content["owner_ids"] = [f"slack:{self.workspace_id}:{author}"]
            permalink = value.get("permalink")
            if isinstance(permalink, str) and permalink.startswith("https://"):
                content["source_url"] = permalink
            result.append(ConnectorRecordV2(
                schema_version=2,
                native_id=native_id,
                native_parent_id=parent.native_id,
                occurred_at=parent.occurred_at,
                content=content,
                provenance={"uri": f"connector://slack/files/{native_id}"},
                archive_payload=payload,
                archive_media_type=media_type,
            ))
        return result

    def _capture_files(
        self, message: dict[str, Any], record: ConnectorRecordV2,
    ) -> tuple[ConnectorRecordV2, list[ConnectorRecordV2]]:
        files = message.get("files") or []
        attachments = self._file_records(message, record)
        if not files or len(attachments) != len(files):
            return record, attachments
        content = dict(record.content)
        omissions = [
            value for value in content.get("content_omissions", [])
            if value != "attachment_bytes"
        ]
        if omissions:
            content["content_omissions"] = omissions
        else:
            content.pop("content_omissions", None)
            content["content_fidelity"] = "complete"
        return ConnectorRecordV2(
            schema_version=2,
            native_id=record.native_id,
            native_parent_id=record.native_parent_id,
            occurred_at=record.occurred_at,
            content=content,
            provenance=record.provenance,
            deleted=record.deleted,
        ), attachments


__all__ = ["SlackWorkspaceConnector"]
