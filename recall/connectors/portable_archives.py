"""Bounded portable imports for Slack, Notion, and X account archives."""

from __future__ import annotations

import hashlib
import io
import json
import re
import stat
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from connectors.local_file import read_stable_file
from connectors.portable_pim import (
    EPOCH,
    MAX_FILE_BYTES,
    MAX_RECORDS,
    _SnapshotImport,
    _bounded,
)
from connectors.sdk import ConnectorContractError, ConnectorRecordV2
from connectors.slack_source import normalize_slack_message, normalize_slack_user


MAX_ARCHIVE_MEMBERS = 100_000
MAX_MEMBER_BYTES = 16 * 1024 * 1024
MAX_EXPANDED_BYTES = 1024 * 1024 * 1024
MAX_COMPRESSION_RATIO = 100
SLACK_MESSAGE_PATH = re.compile(r"[^/]{1,255}/\d{4}-\d{2}-\d{2}\.json\Z")
NOTION_PAGE_ID = re.compile(r"(?:^|[ _-])([0-9a-fA-F]{32})\Z")
X_ASSIGNMENT = re.compile(r"\s*window\.YTD\.[A-Za-z0-9_.]+\s*=\s*", re.ASCII)


def _finite_json(raw: bytes, label: str) -> Any:
    try:
        return json.loads(
            raw,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        raise ConnectorContractError(f"portable_{label}_json_invalid") from None


def _archive_members(path) -> dict[str, bytes]:
    raw = read_stable_file(path, maximum_bytes=MAX_FILE_BYTES)
    try:
        archive = zipfile.ZipFile(io.BytesIO(raw))
    except (OSError, zipfile.BadZipFile):
        raise ConnectorContractError("portable_archive_invalid") from None
    result: dict[str, bytes] = {}
    expanded = 0
    with archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_MEMBERS:
            raise ConnectorContractError("portable_archive_too_many_members")
        for info in members:
            path_value = PurePosixPath(info.filename)
            unix_mode = info.external_attr >> 16
            file_type = stat.S_IFMT(unix_mode)
            if (
                not info.filename
                or "\x00" in info.filename
                or "\\" in info.filename
                or path_value.is_absolute()
                or ".." in path_value.parts
                or len(path_value.parts) > 32
                or len(info.filename.encode()) > 2048
                or info.filename in result
                or file_type not in {0, stat.S_IFREG, stat.S_IFDIR}
            ):
                raise ConnectorContractError("portable_archive_member_invalid")
            if info.is_dir():
                continue
            expanded += info.file_size
            ratio = info.file_size / max(1, info.compress_size)
            if (
                info.file_size > MAX_MEMBER_BYTES
                or expanded > MAX_EXPANDED_BYTES
                or ratio > MAX_COMPRESSION_RATIO
            ):
                raise ConnectorContractError("portable_archive_member_invalid")
            try:
                value = archive.read(info)
            except (OSError, RuntimeError, zipfile.BadZipFile):
                raise ConnectorContractError("portable_archive_read_invalid") from None
            if len(value) != info.file_size:
                raise ConnectorContractError("portable_archive_read_invalid")
            result[info.filename] = value
    return result


def _slack_time(value: str) -> str:
    try:
        seconds = float(value)
        if not seconds >= 0:
            raise ValueError
        return datetime.fromtimestamp(seconds, timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except (TypeError, ValueError, OverflowError):
        raise ConnectorContractError("portable_slack_timestamp_invalid") from None


class SlackArchiveConnector(_SnapshotImport):
    connector_id = "portable.slack"
    native_prefix = "slack:"
    record_kind = "communication_message.v1"

    def _live_records(self) -> list[ConnectorRecordV2]:
        members = _archive_members(self.path)
        channels: dict[str, str] = {}
        for catalog_name in ("channels.json", "groups.json", "mpims.json", "dms.json"):
            raw_catalog = members.get(catalog_name)
            if raw_catalog is None:
                continue
            catalog = _finite_json(raw_catalog, "slack")
            if not isinstance(catalog, list) or len(catalog) > MAX_RECORDS:
                raise ConnectorContractError("portable_slack_json_invalid")
            for value in catalog:
                if not isinstance(value, dict):
                    raise ConnectorContractError("portable_slack_json_invalid")
                channel_id = value.get("id")
                channel_name = value.get("name") or channel_id
                if not isinstance(channel_id, str) or not isinstance(channel_name, str):
                    raise ConnectorContractError("portable_slack_json_invalid")
                channels[channel_name] = channel_id
        records: dict[str, ConnectorRecordV2] = {}
        users_raw = members.get("users.json")
        if users_raw is not None:
            users = _finite_json(users_raw, "slack")
            if not isinstance(users, list) or len(users) > MAX_RECORDS:
                raise ConnectorContractError("portable_slack_json_invalid")
            for user in users:
                for record in normalize_slack_user(
                    workspace_id=self.archive_id,
                    value=user,
                    owner_user_ids=self.owner_identifiers,
                ):
                    records[record.native_id] = record
        for member, raw in sorted(members.items()):
            if not SLACK_MESSAGE_PATH.fullmatch(member):
                continue
            values = _finite_json(raw, "slack")
            if not isinstance(values, list) or len(values) > MAX_RECORDS:
                raise ConnectorContractError("portable_slack_json_invalid")
            channel_name = member.split("/", 1)[0]
            channel = channels.get(channel_name)
            if channel is None:
                raise ConnectorContractError("portable_slack_channel_identity_missing")
            for value in values:
                if not isinstance(value, dict):
                    raise ConnectorContractError("portable_slack_json_invalid")
                timestamp = value.get("ts")
                text = value.get("text", "")
                author = value.get("user") or value.get("bot_id") or "system"
                if (
                    not isinstance(timestamp, str)
                    or not isinstance(text, str)
                    or not isinstance(author, str)
                ):
                    raise ConnectorContractError("portable_slack_json_invalid")
                record = normalize_slack_message(
                    workspace_id=self.archive_id,
                    channel_id=channel,
                    value=value,
                    owner_identifiers=self.owner_identifiers,
                    provenance_surface="archive",
                )
                records[record.native_id] = record
        return [records[key] for key in sorted(records)]


def _decode_text(raw: bytes, label: str) -> str:
    try:
        return _bounded(raw.decode("utf-8-sig"), label)
    except UnicodeDecodeError:
        raise ConnectorContractError(f"portable_{label}_encoding_invalid") from None


class NotionArchiveConnector(_SnapshotImport):
    connector_id = "portable.notion"
    native_prefix = "notion:"
    record_kind = "document.v1"

    def _live_records(self) -> list[ConnectorRecordV2]:
        records = {}
        for member, raw in sorted(_archive_members(self.path).items()):
            suffix = PurePosixPath(member).suffix.lower()
            if suffix not in {".csv", ".markdown", ".md"}:
                continue
            path_value = PurePosixPath(member)
            name = path_value.stem
            match = NOTION_PAGE_ID.search(name)
            stable = match.group(1).lower() if match else member
            native_id = "notion:" + hashlib.sha256(
                f"{self.archive_id}\0{stable}".encode()
            ).hexdigest()
            parent_id = "notion-parent:" + hashlib.sha256(
                f"{self.archive_id}\0{path_value.parent}".encode()
            ).hexdigest()
            mime_type = "text/csv" if suffix == ".csv" else "text/markdown"
            records[native_id] = ConnectorRecordV2(
                schema_version=2,
                native_id=native_id,
                native_parent_id=parent_id,
                occurred_at=EPOCH,
                content={
                    "kind": self.record_kind,
                    "content_fidelity": "complete",
                    "document_id": native_id,
                    "mime_type": mime_type,
                    "name": _bounded(name, "notion_name", maximum=10_000),
                    "parent_id": parent_id,
                    "surface": "portable_notion",
                    "text": _decode_text(raw, "notion_text"),
                },
                provenance={"uri": f"connector://portable-notion/{native_id}"},
            )
        return [records[key] for key in sorted(records)]


def _x_time(value: Any) -> str:
    if value is None:
        return EPOCH
    if not isinstance(value, str):
        raise ConnectorContractError("portable_x_timestamp_invalid")
    try:
        return datetime.strptime(
            value, "%a %b %d %H:%M:%S %z %Y"
        ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        raise ConnectorContractError("portable_x_timestamp_invalid") from None


def _x_count(value: Any) -> int:
    if value is None:
        return 0
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise ConnectorContractError("portable_x_json_invalid")
    try:
        result = int(value)
    except ValueError:
        raise ConnectorContractError("portable_x_json_invalid") from None
    if result < 0 or result > 9_223_372_036_854_775_807:
        raise ConnectorContractError("portable_x_json_invalid")
    return result


class XArchiveConnector(_SnapshotImport):
    connector_id = "portable.x"
    native_prefix = "xarchive:"
    record_kind = "social_post.v1"

    def _live_records(self) -> list[ConnectorRecordV2]:
        records = {}
        for member, raw in sorted(_archive_members(self.path).items()):
            filename = PurePosixPath(member).name.lower()
            if not (
                member.startswith("data/")
                and filename.endswith(".js")
                and (filename.startswith("tweet") or filename.startswith("tweets"))
            ):
                continue
            try:
                text = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                raise ConnectorContractError("portable_x_encoding_invalid") from None
            payload = X_ASSIGNMENT.sub("", text, count=1)
            values = _finite_json(payload.encode(), "x")
            if not isinstance(values, list) or len(values) > MAX_RECORDS:
                raise ConnectorContractError("portable_x_json_invalid")
            for wrapper in values:
                if not isinstance(wrapper, dict):
                    raise ConnectorContractError("portable_x_json_invalid")
                value = wrapper.get("tweet", wrapper)
                if not isinstance(value, dict):
                    raise ConnectorContractError("portable_x_json_invalid")
                post_id = value.get("id_str") or value.get("id")
                post_text = value.get("full_text") or value.get("text")
                if not isinstance(post_id, str) or not isinstance(post_text, str):
                    raise ConnectorContractError("portable_x_json_invalid")
                native_id = "xarchive:" + hashlib.sha256(
                    f"{self.archive_id}\0{post_id}".encode()
                ).hexdigest()
                occurred_at = _x_time(value.get("created_at"))
                content: dict[str, Any] = {
                    "kind": self.record_kind,
                    "content_fidelity": "complete",
                    "author_id": (
                        self.owner_identifiers[0]
                        if self.owner_identifiers
                        else self.archive_id
                    ),
                    "post_id": post_id,
                    "stream_type": "own",
                    "surface": "portable_x",
                    "text": _bounded(post_text, "x_text"),
                }
                if occurred_at != EPOCH:
                    content["created_at"] = occurred_at
                metrics = {
                    "likes": _x_count(value.get("favorite_count")),
                    "reposts": _x_count(value.get("retweet_count")),
                }
                if any(metrics.values()):
                    content["metrics"] = metrics
                reply_to = value.get("in_reply_to_status_id_str")
                if reply_to is not None:
                    if not isinstance(reply_to, str):
                        raise ConnectorContractError("portable_x_json_invalid")
                    content["reply_to_id"] = reply_to
                records[native_id] = ConnectorRecordV2(
                    schema_version=2,
                    native_id=native_id,
                    occurred_at=occurred_at,
                    content=content,
                    provenance={"uri": f"connector://portable-x/{native_id}"},
                )
        return [records[key] for key in sorted(records)]


__all__ = [
    "NotionArchiveConnector",
    "SlackArchiveConnector",
    "XArchiveConnector",
]
