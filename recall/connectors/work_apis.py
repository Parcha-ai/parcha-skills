"""Typed GitHub, Linear, Slack, and Notion pull connectors."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol
from urllib.parse import urlsplit

from connectors.remote_api import BoundedJsonRail, RemoteApiError, RemoteOperation
from connectors.sdk import (
    ConnectorContractError,
    ConnectorPage,
    ConnectorRecordV2,
    ConnectorUpstreamError,
    SOURCE_ID,
)
from connectors.slack_source import normalize_slack_message, normalize_slack_user


EPOCH = "1970-01-01T00:00:00Z"
MAX_ITEMS = 500
MAX_TEXT_BYTES = 500_000
MAX_VALUE_BYTES = 4_096


class JsonRail(Protocol):
    def request(
        self,
        operation_id: str,
        *,
        path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> Any: ...


def _string(value: Any, label: str, *, maximum: int = MAX_VALUE_BYTES) -> str:
    if not isinstance(value, str) or not value or len(value.encode()) > maximum:
        raise ConnectorContractError(f"{label} is invalid")
    return value


def _optional_string(value: Any, label: str, *, maximum: int = MAX_VALUE_BYTES) -> str | None:
    return None if value is None else _string(value, label, maximum=maximum)


def _source(value: str) -> str:
    if not isinstance(value, str) or not SOURCE_ID.fullmatch(value):
        raise ConnectorContractError("source_id is invalid")
    return value


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConnectorContractError(f"{label} is invalid")
    return value


def _items(value: Any, label: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_ITEMS:
        raise ConnectorContractError(f"{label} is invalid")
    return value


def _text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    encoded = value.encode(errors="replace")[:MAX_TEXT_BYTES]
    return encoded.decode(errors="ignore")


def _timestamp(value: Any, label: str, *, fallback: str | None = None) -> str:
    if not isinstance(value, str) or not value:
        if fallback is not None:
            return fallback
        raise ConnectorContractError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        if fallback is not None:
            return fallback
        raise ConnectorContractError(f"{label} is invalid") from None
    if parsed.tzinfo is None:
        if fallback is not None:
            return fallback
        raise ConnectorContractError(f"{label} is invalid")
    return value


def _slack_timestamp(value: Any, label: str) -> str:
    raw = _string(value, label)
    whole, separator, fraction = raw.partition(".")
    if not whole.isdigit() or (separator and (not fraction.isdigit() or len(fraction) > 6)):
        raise ConnectorContractError(f"{label} is invalid")
    try:
        parsed = datetime.fromtimestamp(
            int(whole),
            timezone.utc,
        ).replace(microsecond=int((fraction + "000000")[:6]))
    except (ValueError, OverflowError, OSError):
        raise ConnectorContractError(f"{label} is invalid") from None
    rendered = parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return rendered


def _slack_oldest(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    seconds = int(parsed.timestamp())
    return f"{seconds}.{parsed.microsecond:06d}"


def _url(value: Any) -> str | None:
    if not isinstance(value, str) or not value or len(value.encode()) > MAX_VALUE_BYTES:
        return None
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        return None
    return value


def _cursor(*, page: Any, watermark: str, max_seen: str, cycle: int) -> str:
    try:
        raw = json.dumps(
            {
                "v": 1,
                "page": page,
                "watermark": watermark,
                "max_seen": max_seen,
                "cycle": cycle,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError):
        raise ConnectorContractError("connector cursor is invalid") from None
    if len(raw.encode()) > MAX_VALUE_BYTES:
        raise ConnectorContractError("connector cursor is invalid")
    return raw


def _state(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {
            "v": 1,
            "page": None,
            "watermark": EPOCH,
            "max_seen": EPOCH,
            "cycle": 0,
        }
    if not isinstance(raw, str) or not raw or len(raw.encode()) > MAX_VALUE_BYTES:
        raise ConnectorContractError("connector cursor is invalid")
    try:
        value = json.loads(
            raw,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (json.JSONDecodeError, ValueError):
        raise ConnectorContractError("connector cursor is invalid") from None
    if (
        not isinstance(value, dict)
        or set(value) != {"v", "page", "watermark", "max_seen", "cycle"}
        or value.get("v") != 1
        or type(value.get("cycle")) is not int
        or not 0 <= value["cycle"] <= 2_147_483_647
    ):
        raise ConnectorContractError("connector cursor is invalid")
    _timestamp(value.get("watermark"), "connector cursor watermark")
    _timestamp(value.get("max_seen"), "connector cursor maximum")
    return value


def _next_cycle(value: int) -> int:
    return 0 if value == 2_147_483_647 else value + 1


def _max_timestamp(current: str, candidate: str) -> str:
    left = datetime.fromisoformat(current.replace("Z", "+00:00"))
    right = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    return candidate if right > left else current


def _record(
    *,
    native_id: str,
    occurred_at: str,
    kind: str,
    provenance_uri: str,
    content: Mapping[str, Any] | None = None,
    parent: str | None = None,
    deleted: bool = False,
) -> ConnectorRecordV2:
    return ConnectorRecordV2.from_mapping({
        "schema_version": 2,
        "native_id": native_id,
        "native_parent_id": parent,
        "occurred_at": occurred_at,
        "content": {"kind": kind} if deleted else {"kind": kind, **dict(content or {})},
        "provenance": {"uri": provenance_uri},
        "deleted": deleted,
    })


def _translate(error: RemoteApiError) -> None:
    code = {
        "authority_revoked": "connector_authority_revoked",
        "authority_forbidden": "connector_authority_forbidden",
        "response_invalid": "connector_schema_drift",
        "content_type_invalid": "connector_schema_drift",
    }.get(error.code, "connector_upstream_error")
    raise ConnectorUpstreamError(code) from None


def _page_size(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 100:
        raise ConnectorContractError("page_size is invalid")
    return value


def github_rail(*, authority_path: Path, **options: Any) -> BoundedJsonRail:
    return BoundedJsonRail(
        origin="https://api.github.com",
        authority_path=authority_path,
        authorization_scheme="Bearer",
        operations={
            "issues.list": RemoteOperation(
                method="GET",
                path_template="/repos/{owner}/{repo}/issues",
                path_fields=("owner", "repo"),
                query_fields=("direction", "page", "per_page", "since", "sort", "state"),
            ),
        },
        fixed_headers={"X-GitHub-Api-Version": "2022-11-28"},
        **options,
    )


class GitHubActivityConnector:
    connector_id = "github.activity"

    def __init__(
        self,
        *,
        rail: JsonRail,
        source_id: str,
        owner: str,
        repository: str,
        page_size: int = 100,
    ):
        if not callable(getattr(rail, "request", None)):
            raise ConnectorContractError("remote rail is invalid")
        self.rail = rail
        self.source_id = _source(source_id)
        self.owner = _string(owner, "github owner")
        self.repository = _string(repository, "github repository")
        self.page_size = _page_size(page_size)

    def pull(self, cursor: str | None) -> ConnectorPage:
        state = _state(cursor)
        if state["page"] is not None and (
            type(state["page"]) is not int or state["page"] < 2
        ):
            raise ConnectorContractError("connector cursor is invalid")
        page_number = state["page"] or 1
        query: dict[str, Any] = {
            "direction": "asc",
            "page": page_number,
            "per_page": self.page_size,
            "sort": "updated",
            "state": "all",
        }
        if state["watermark"] != EPOCH:
            query["since"] = state["watermark"]
        try:
            values = self.rail.request(
                "issues.list",
                path={"owner": self.owner, "repo": self.repository},
                query=query,
            )
        except RemoteApiError as error:
            _translate(error)
        records = []
        maximum = state["max_seen"]
        for raw in _items(values, "github issues response"):
            issue = _mapping(raw, "github issue")
            number = issue.get("number")
            if type(number) is not int or number <= 0:
                raise ConnectorContractError("github issue number is invalid")
            updated = _timestamp(issue.get("updated_at"), "github updated timestamp")
            maximum = _max_timestamp(maximum, updated)
            pull_request = isinstance(issue.get("pull_request"), dict)
            type_name = "pull-request" if pull_request else "issue"
            title = _string(issue.get("title"), "github title", maximum=MAX_TEXT_BYTES)
            state_name = _optional_string(issue.get("state"), "github state") or ""
            labels = []
            for raw_label in _items(issue.get("labels"), "github labels"):
                label = _mapping(raw_label, "github label")
                name = _optional_string(label.get("name"), "github label")
                if name:
                    labels.append(name)
            user = issue.get("user")
            login = (
                _optional_string(user.get("login"), "github user")
                if isinstance(user, dict)
                else None
            )
            native = f"github:{self.owner}/{self.repository}:{type_name}:{number}"
            content: dict[str, Any] = {
                "content_fidelity": "partial",
                "content_omissions": ["comments_not_fetched"],
                "document_id": native,
                "mime_type": f"application/vnd.github.{type_name}+json",
                "name": title,
                "modified_at": updated,
                "surface": "github",
                "text": "\n".join(
                    item for item in (title, state_name, " ".join(labels), _text(issue.get("body")))
                    if item
                ),
            }
            url = _url(issue.get("html_url"))
            if url:
                content["source_url"] = url
            if login:
                content["participant_ids"] = [login]
            records.append(_record(
                native_id=native,
                occurred_at=updated,
                kind="document.v1",
                content=content,
                provenance_uri="connector://github-activity",
            ))
        has_more = len(records) == self.page_size
        return ConnectorPage(
            records=tuple(records),
            next_cursor=_cursor(
                page=page_number + 1 if has_more else None,
                watermark=state["watermark"] if has_more else maximum,
                max_seen=maximum,
                cycle=state["cycle"] if has_more else _next_cycle(state["cycle"]),
            ),
            has_more=has_more,
        )


LINEAR_ISSUES_QUERY = """
query RecallIssues($team_id: ID!, $watermark: DateTimeOrDuration!, $after: String, $first: Int!) {
  issues(
    filter: {team: {id: {eq: $team_id}}, updatedAt: {gte: $watermark}}
    orderBy: updatedAt
    first: $first
    after: $after
  ) {
    nodes {
      id identifier title description url createdAt updatedAt
      state { name }
      assignee { id }
      labels { nodes { name } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
""".strip()


def linear_rail(*, authority_path: Path, **options: Any) -> BoundedJsonRail:
    return BoundedJsonRail(
        origin="https://api.linear.app",
        authority_path=authority_path,
        authorization_scheme="Bearer",
        operations={
            "issues.list": RemoteOperation(
                method="POST",
                path_template="/graphql",
                path_fields=(),
                query_fields=(),
                json_fields=("variables",),
                fixed_json={"query": LINEAR_ISSUES_QUERY},
            ),
        },
        **options,
    )


class LinearActivityConnector:
    connector_id = "linear.activity"

    def __init__(self, *, rail: JsonRail, source_id: str, team_id: str, page_size: int = 100):
        if not callable(getattr(rail, "request", None)):
            raise ConnectorContractError("remote rail is invalid")
        self.rail = rail
        self.source_id = _source(source_id)
        self.team_id = _string(team_id, "linear team")
        self.page_size = _page_size(page_size)

    def pull(self, cursor: str | None) -> ConnectorPage:
        state = _state(cursor)
        if state["page"] is not None and not isinstance(state["page"], str):
            raise ConnectorContractError("connector cursor is invalid")
        try:
            response = _mapping(self.rail.request(
                "issues.list",
                json_body={"variables": {
                    "team_id": self.team_id,
                    "watermark": state["watermark"],
                    "after": state["page"],
                    "first": self.page_size,
                }},
            ), "linear response")
        except RemoteApiError as error:
            _translate(error)
        if response.get("errors") is not None:
            _items(response["errors"], "linear errors")
            raise ConnectorUpstreamError("connector_upstream_error")
        data = _mapping(response.get("data"), "linear data")
        issues = _mapping(data.get("issues"), "linear issues")
        records = []
        maximum = state["max_seen"]
        for raw in _items(issues.get("nodes"), "linear issue nodes"):
            issue = _mapping(raw, "linear issue")
            item_id = _string(issue.get("id"), "linear issue id")
            identifier = _string(issue.get("identifier"), "linear identifier")
            title = _string(issue.get("title"), "linear title", maximum=MAX_TEXT_BYTES)
            updated = _timestamp(issue.get("updatedAt"), "linear updated timestamp")
            maximum = _max_timestamp(maximum, updated)
            state_value = issue.get("state")
            state_name = (
                _optional_string(state_value.get("name"), "linear state")
                if isinstance(state_value, dict)
                else None
            )
            labels = []
            label_value = issue.get("labels")
            if isinstance(label_value, dict):
                for raw_label in _items(label_value.get("nodes"), "linear labels"):
                    label = _mapping(raw_label, "linear label")
                    name = _optional_string(label.get("name"), "linear label")
                    if name:
                        labels.append(name)
            content: dict[str, Any] = {
                "content_fidelity": "partial",
                "content_omissions": ["comments_not_fetched"],
                "document_id": f"linear:{item_id}",
                "mime_type": "application/vnd.linear.issue+json",
                "name": title,
                "modified_at": updated,
                "surface": "linear",
                "text": "\n".join(
                    item for item in (
                        f"{identifier} {title}",
                        state_name or "",
                        " ".join(labels),
                        _text(issue.get("description")),
                    ) if item
                ),
            }
            url = _url(issue.get("url"))
            if url:
                content["source_url"] = url
            assignee = issue.get("assignee")
            if isinstance(assignee, dict):
                assignee_id = _optional_string(assignee.get("id"), "linear assignee")
                if assignee_id:
                    content["participant_ids"] = [assignee_id]
            records.append(_record(
                native_id=f"linear:{item_id}",
                occurred_at=updated,
                kind="document.v1",
                content=content,
                provenance_uri="connector://linear-activity",
            ))
        page_info = _mapping(issues.get("pageInfo"), "linear page info")
        has_more = page_info.get("hasNextPage")
        if type(has_more) is not bool:
            raise ConnectorContractError("linear pagination is invalid")
        next_page = _optional_string(page_info.get("endCursor"), "linear page cursor")
        if has_more != (next_page is not None):
            raise ConnectorContractError("linear pagination is invalid")
        return ConnectorPage(
            records=tuple(records),
            next_cursor=_cursor(
                page=next_page,
                watermark=state["watermark"] if has_more else maximum,
                max_seen=maximum,
                cycle=state["cycle"] if has_more else _next_cycle(state["cycle"]),
            ),
            has_more=has_more,
        )


def _slack_cursor(
    *, phase: str, page: str | None, thread_page: str | None,
    threads: list[str], thread_index: int, watermark: str, max_seen: str, cycle: int,
) -> str:
    value = {
        "v": 2, "phase": phase, "page": page, "thread_page": thread_page,
        "threads": threads, "thread_index": thread_index,
        "watermark": watermark, "max_seen": max_seen, "cycle": cycle,
    }
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        raise ConnectorContractError("connector cursor is invalid") from None
    if len(raw.encode()) > MAX_VALUE_BYTES:
        raise ConnectorContractError("connector cursor is invalid")
    return raw


def _slack_state(raw: str | None) -> dict[str, Any]:
    if raw is None:
        return {
            "v": 2, "phase": "history", "page": None, "thread_page": None,
            "threads": [], "thread_index": 0, "watermark": EPOCH,
            "max_seen": EPOCH, "cycle": 0,
        }
    # Accept the pre-workspace cursor only as a safe history resume.
    try:
        value = json.loads(raw, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
    except (TypeError, json.JSONDecodeError, ValueError):
        raise ConnectorContractError("connector cursor is invalid") from None
    if isinstance(value, dict) and value.get("v") == 1:
        legacy = _state(raw)
        return {
            **legacy, "v": 2, "phase": "history", "thread_page": None,
            "threads": [], "thread_index": 0,
        }
    expected = {
        "v", "phase", "page", "thread_page", "threads", "thread_index",
        "watermark", "max_seen", "cycle",
    }
    if (
        not isinstance(value, dict) or set(value) != expected or value.get("v") != 2
        or value.get("phase") not in {"users", "history", "threads"}
        or not isinstance(value.get("threads"), list)
        or len(value["threads"]) > 100
        or any(not isinstance(item, str) or not item for item in value["threads"])
        or type(value.get("thread_index")) is not int
        or not 0 <= value["thread_index"] <= len(value["threads"])
        or type(value.get("cycle")) is not int
        or not 0 <= value["cycle"] <= 2_147_483_647
        or any(value.get(key) is not None and not isinstance(value.get(key), str)
               for key in ("page", "thread_page"))
    ):
        raise ConnectorContractError("connector cursor is invalid")
    _timestamp(value.get("watermark"), "connector watermark")
    _timestamp(value.get("max_seen"), "connector maximum")
    if value["phase"] == "threads" and value["thread_index"] >= len(value["threads"]):
        raise ConnectorContractError("connector cursor is invalid")
    return value


def slack_rail(*, authority_path: Path, **options: Any) -> BoundedJsonRail:
    return BoundedJsonRail(
        origin="https://slack.com",
        authority_path=authority_path,
        authorization_scheme="Bearer",
        binary_hosts=("files.slack.com",),
        operations={
            "messages.history": RemoteOperation(
                method="GET",
                path_template="/api/conversations.history",
                path_fields=(),
                query_fields=("channel", "cursor", "inclusive", "latest", "limit", "oldest"),
            ),
            "messages.replies": RemoteOperation(
                method="GET",
                path_template="/api/conversations.replies",
                path_fields=(),
                query_fields=("channel", "cursor", "inclusive", "latest", "limit", "oldest", "ts"),
            ),
            "users.list": RemoteOperation(
                method="GET",
                path_template="/api/users.list",
                path_fields=(),
                query_fields=("cursor", "include_locale", "limit"),
            ),
            "channels.list": RemoteOperation(
                method="GET",
                path_template="/api/conversations.list",
                path_fields=(),
                query_fields=("cursor", "exclude_archived", "limit", "types"),
            ),
            "channels.join": RemoteOperation(
                method="POST",
                path_template="/api/conversations.join",
                path_fields=(),
                query_fields=("channel",),
            ),
        },
        **options,
    )


class SlackPublicHistoryRail:
    """Route public history through a user token and live membership through the bot."""

    public_history = True
    _USER_OPERATIONS = frozenset({
        "channels.list",
        "messages.history",
        "messages.replies",
    })

    def __init__(self, *, bot_rail: JsonRail, user_rail: JsonRail):
        if (
            not callable(getattr(bot_rail, "request", None))
            or not callable(getattr(user_rail, "request", None))
            or not callable(getattr(user_rail, "download_binary", None))
        ):
            raise ConnectorContractError("slack public history rail is invalid")
        self.bot_rail = bot_rail
        self.user_rail = user_rail

    def request(
        self,
        operation_id: str,
        *,
        path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> Any:
        rail = self.user_rail if operation_id in self._USER_OPERATIONS else self.bot_rail
        return rail.request(
            operation_id,
            path=path,
            query=query,
            json_body=json_body,
        )

    def download_binary(
        self, url: str, *, maximum_bytes: int = 64 * 1024 * 1024,
    ) -> tuple[bytes, str]:
        return self.user_rail.download_binary(url, maximum_bytes=maximum_bytes)


def slack_public_history_rail(
    *, bot_authority_path: Path, user_authority_path: Path, **options: Any,
) -> SlackPublicHistoryRail:
    return SlackPublicHistoryRail(
        bot_rail=slack_rail(authority_path=bot_authority_path, **options),
        user_rail=slack_rail(authority_path=user_authority_path, **options),
    )


class SlackMessagesConnector:
    connector_id = "slack.messages"

    def __init__(
        self, *, rail: JsonRail, source_id: str, workspace_id: str,
        channel_id: str, owner_user_ids: tuple[str, ...] = (), page_size: int = 100,
    ):
        if not callable(getattr(rail, "request", None)):
            raise ConnectorContractError("remote rail is invalid")
        self.rail = rail
        self.source_id = _source(source_id)
        self.workspace_id = _string(workspace_id, "slack workspace")
        self.channel_id = _string(channel_id, "slack channel")
        if (
            not isinstance(owner_user_ids, tuple)
            or any(not isinstance(value, str) or not value for value in owner_user_ids)
        ):
            raise ConnectorContractError("slack owner users are invalid")
        self.owner_user_ids = owner_user_ids
        self.page_size = _page_size(page_size)

    def pull(self, cursor: str | None) -> ConnectorPage:
        state = _slack_state(cursor)
        if state["phase"] == "users":
            return self._pull_users(state)
        if state["phase"] == "threads":
            return self._pull_thread(state)
        if state["page"] is not None and not isinstance(state["page"], str):
            raise ConnectorContractError("connector cursor is invalid")
        query: dict[str, Any] = {
            "channel": self.channel_id,
            "inclusive": True,
            "limit": self.page_size,
        }
        if state["page"]:
            query["cursor"] = state["page"]
        elif state["watermark"] != EPOCH:
            query["oldest"] = _slack_oldest(state["watermark"])
        try:
            response = _mapping(
                self.rail.request("messages.history", query=query),
                "slack response",
            )
            if response.get("ok") is not True and response.get("error") == "invalid_cursor":
                if state["page"] is None:
                    raise ConnectorUpstreamError("connector_upstream_error")
                query.pop("cursor", None)
                if state["watermark"] != EPOCH:
                    query["oldest"] = _slack_oldest(state["watermark"])
                response = _mapping(
                    self.rail.request("messages.history", query=query),
                    "slack response",
                )
        except RemoteApiError as error:
            _translate(error)
        if response.get("ok") is not True:
            _optional_string(response.get("error"), "slack error")
            raise ConnectorUpstreamError("connector_upstream_error")
        records_by_id: dict[str, ConnectorRecordV2] = {}
        threads: list[str] = []
        maximum = state["max_seen"]
        for raw in _items(response.get("messages"), "slack messages"):
            event = _mapping(raw, "slack message")
            record = normalize_slack_message(
                workspace_id=self.workspace_id,
                channel_id=self.channel_id,
                value=event,
                owner_identifiers=self.owner_user_ids,
                provenance_surface="api",
            )
            records_by_id[record.native_id] = record
            maximum = _max_timestamp(maximum, record.occurred_at)
            reply_count = event.get("reply_count", 0)
            if type(reply_count) is not int or reply_count < 0:
                raise ConnectorContractError("slack reply count is invalid")
            if reply_count:
                threads.append(_string(event.get("ts"), "slack thread timestamp"))
        metadata = _mapping(response.get("response_metadata", {}), "slack response metadata")
        next_page = metadata.get("next_cursor")
        if next_page == "":
            next_page = None
        next_page = _optional_string(next_page, "slack page cursor")
        has_more = response.get("has_more")
        if type(has_more) is not bool or has_more != (next_page is not None):
            raise ConnectorContractError("slack pagination is invalid")
        if threads:
            next_cursor = _slack_cursor(
                phase="threads", page=next_page, thread_page=None,
                threads=threads, thread_index=0, watermark=state["watermark"],
                max_seen=maximum, cycle=state["cycle"],
            )
            more = True
        else:
            next_cursor = _slack_cursor(
                phase="history" if has_more else "users", page=next_page,
                thread_page=None, threads=[],
                thread_index=0,
                watermark=state["watermark"] if has_more else maximum,
                max_seen=maximum,
                cycle=state["cycle"] if has_more else _next_cycle(state["cycle"]),
            )
            more = has_more
        return ConnectorPage(
            records=tuple(records_by_id.values()), next_cursor=next_cursor, has_more=more,
        )

    def _pull_thread(self, state: dict[str, Any]) -> ConnectorPage:
        thread_ts = state["threads"][state["thread_index"]]
        query: dict[str, Any] = {
            "channel": self.channel_id, "inclusive": True,
            "limit": self.page_size, "ts": thread_ts,
        }
        if state["thread_page"]:
            query["cursor"] = state["thread_page"]
        try:
            response = _mapping(
                self.rail.request("messages.replies", query=query), "slack response",
            )
        except RemoteApiError as error:
            _translate(error)
        if response.get("ok") is not True:
            raise ConnectorUpstreamError("connector_upstream_error")
        records: dict[str, ConnectorRecordV2] = {}
        maximum = state["max_seen"]
        for raw in _items(response.get("messages"), "slack replies"):
            message = _mapping(raw, "slack reply")
            record = normalize_slack_message(
                workspace_id=self.workspace_id, channel_id=self.channel_id,
                value=message, owner_identifiers=self.owner_user_ids,
                provenance_surface="api",
            )
            records[record.native_id] = record
            maximum = _max_timestamp(maximum, record.occurred_at)
        metadata = _mapping(response.get("response_metadata", {}), "slack response metadata")
        thread_page = metadata.get("next_cursor") or None
        thread_page = _optional_string(thread_page, "slack thread cursor")
        has_thread_more = response.get("has_more")
        if type(has_thread_more) is not bool or has_thread_more != (thread_page is not None):
            raise ConnectorContractError("slack thread pagination is invalid")
        index = state["thread_index"]
        if not has_thread_more:
            index += 1
        threads_done = index >= len(state["threads"])
        if threads_done:
            history_more = state["page"] is not None
            next_cursor = _slack_cursor(
                phase="history" if history_more else "users", page=state["page"],
                thread_page=None, threads=[],
                thread_index=0,
                watermark=state["watermark"] if history_more else maximum,
                max_seen=maximum,
                cycle=state["cycle"] if history_more else _next_cycle(state["cycle"]),
            )
            more = history_more
        else:
            next_cursor = _slack_cursor(
                phase="threads", page=state["page"],
                thread_page=thread_page if has_thread_more else None,
                threads=state["threads"], thread_index=index,
                watermark=state["watermark"], max_seen=maximum,
                cycle=state["cycle"],
            )
            more = True
        return ConnectorPage(
            records=tuple(records.values()), next_cursor=next_cursor, has_more=more,
        )

    def _pull_users(self, state: dict[str, Any]) -> ConnectorPage:
        query: dict[str, Any] = {
            "include_locale": False, "limit": self.page_size,
        }
        if state["page"]:
            query["cursor"] = state["page"]
        try:
            response = _mapping(
                self.rail.request("users.list", query=query), "slack response",
            )
        except RemoteApiError as error:
            _translate(error)
        if response.get("ok") is not True:
            raise ConnectorUpstreamError("connector_upstream_error")
        records: dict[str, ConnectorRecordV2] = {}
        for raw in _items(response.get("members"), "slack users"):
            for record in normalize_slack_user(
                workspace_id=self.workspace_id,
                value=_mapping(raw, "slack user"),
                owner_user_ids=self.owner_user_ids,
            ):
                records[record.native_id] = record
        metadata = _mapping(response.get("response_metadata", {}), "slack response metadata")
        next_page = metadata.get("next_cursor") or None
        next_page = _optional_string(next_page, "slack users cursor")
        if next_page:
            next_cursor = _slack_cursor(
                phase="users", page=next_page, thread_page=None, threads=[],
                thread_index=0, watermark=state["watermark"],
                max_seen=state["max_seen"], cycle=state["cycle"],
            )
        else:
            next_cursor = _slack_cursor(
                phase="history", page=None, thread_page=None, threads=[],
                thread_index=0, watermark=state["watermark"],
                max_seen=state["max_seen"], cycle=state["cycle"],
            )
        return ConnectorPage(
            records=tuple(records.values()), next_cursor=next_cursor, has_more=True,
        )


def notion_rail(*, authority_path: Path, **options: Any) -> BoundedJsonRail:
    return BoundedJsonRail(
        origin="https://api.notion.com",
        authority_path=authority_path,
        authorization_scheme="Bearer",
        operations={
            "search.list": RemoteOperation(
                method="POST",
                path_template="/v1/search",
                path_fields=(),
                query_fields=(),
                json_fields=("page_size", "start_cursor"),
                fixed_json={
                    "sort": {
                        "direction": "ascending",
                        "timestamp": "last_edited_time",
                    },
                },
            ),
        },
        fixed_headers={"Notion-Version": "2026-03-11"},
        **options,
    )


X_TWEET_FIELDS = (
    "author_id,conversation_id,created_at,edit_history_tweet_ids,"
    "public_metrics,referenced_tweets"
)


def x_rail(*, authority_path: Path, **options: Any) -> BoundedJsonRail:
    common = {
        "path_fields": ("user_id",),
        "fixed_query": {"tweet.fields": X_TWEET_FIELDS},
    }
    return BoundedJsonRail(
        origin="https://api.x.com",
        authority_path=authority_path,
        authorization_scheme="Bearer",
        operations={
            "bookmarks.list": RemoteOperation(
                method="GET",
                path_template="/2/users/{user_id}/bookmarks",
                query_fields=("max_results", "pagination_token"),
                **common,
            ),
            "home.list": RemoteOperation(
                method="GET",
                path_template="/2/users/{user_id}/timelines/reverse_chronological",
                query_fields=("max_results", "pagination_token", "since_id"),
                **common,
            ),
            "mentions.list": RemoteOperation(
                method="GET",
                path_template="/2/users/{user_id}/mentions",
                query_fields=("max_results", "pagination_token", "since_id"),
                **common,
            ),
            "own.list": RemoteOperation(
                method="GET",
                path_template="/2/users/{user_id}/tweets",
                query_fields=("max_results", "pagination_token", "since_id"),
                **common,
            ),
        },
        **options,
    )


def _notion_title(properties: Any) -> str:
    if not isinstance(properties, dict):
        return "Untitled"
    for key in sorted(properties):
        prop = properties[key]
        if not isinstance(prop, dict) or prop.get("type") != "title":
            continue
        parts = []
        for raw in _items(prop.get("title"), "notion title"):
            item = _mapping(raw, "notion title item")
            plain = _optional_string(
                item.get("plain_text"),
                "notion title text",
                maximum=MAX_TEXT_BYTES,
            )
            if plain:
                parts.append(plain)
        return _text("".join(parts)) or "Untitled"
    return "Untitled"


class NotionWorkspaceConnector:
    connector_id = "notion.workspace"

    def __init__(self, *, rail: JsonRail, source_id: str, page_size: int = 100):
        if not callable(getattr(rail, "request", None)):
            raise ConnectorContractError("remote rail is invalid")
        self.rail = rail
        self.source_id = _source(source_id)
        self.page_size = _page_size(page_size)

    def pull(self, cursor: str | None) -> ConnectorPage:
        state = _state(cursor)
        if state["page"] is not None and not isinstance(state["page"], str):
            raise ConnectorContractError("connector cursor is invalid")
        body: dict[str, Any] = {"page_size": self.page_size}
        if state["page"]:
            body["start_cursor"] = state["page"]
        try:
            response = _mapping(
                self.rail.request("search.list", json_body=body),
                "notion response",
            )
        except RemoteApiError as error:
            _translate(error)
        if response.get("object") != "list":
            raise ConnectorContractError("notion response is invalid")
        records = []
        maximum = state["max_seen"]
        for raw in _items(response.get("results"), "notion results"):
            item = _mapping(raw, "notion result")
            object_type = _string(item.get("object"), "notion object type")
            if object_type not in {"page", "data_source"}:
                raise ConnectorContractError("notion object type is invalid")
            item_id = _string(item.get("id"), "notion object id")
            updated = _timestamp(item.get("last_edited_time"), "notion edited timestamp")
            maximum = _max_timestamp(maximum, updated)
            native = f"notion:{item_id}"
            if item.get("in_trash") is True:
                records.append(_record(
                    native_id=native,
                    occurred_at=updated,
                    kind="document.v1",
                    deleted=True,
                    provenance_uri="connector://notion-workspace",
                ))
                continue
            title = _notion_title(item.get("properties"))
            content: dict[str, Any] = {
                "content_fidelity": "partial",
                "content_omissions": ["page_body_not_fetched"],
                "document_id": native,
                "mime_type": f"application/vnd.notion.{object_type}+json",
                "modified_at": updated,
                "name": title,
                "surface": "notion",
                "text": title,
            }
            url = _url(item.get("url"))
            if url:
                content["source_url"] = url
            records.append(_record(
                native_id=native,
                occurred_at=updated,
                kind="document.v1",
                content=content,
                provenance_uri="connector://notion-workspace",
            ))
        has_more = response.get("has_more")
        if type(has_more) is not bool:
            raise ConnectorContractError("notion pagination is invalid")
        next_page = _optional_string(response.get("next_cursor"), "notion page cursor")
        if has_more != (next_page is not None):
            raise ConnectorContractError("notion pagination is invalid")
        return ConnectorPage(
            records=tuple(records),
            next_cursor=_cursor(
                page=next_page,
                watermark=state["watermark"] if has_more else maximum,
                max_seen=maximum,
                cycle=state["cycle"] if has_more else _next_cycle(state["cycle"]),
            ),
            has_more=has_more,
        )


__all__ = [
    "GitHubActivityConnector",
    "LinearActivityConnector",
    "NotionWorkspaceConnector",
    "SlackMessagesConnector",
    "SlackPublicHistoryRail",
    "github_rail",
    "linear_rail",
    "notion_rail",
    "slack_rail",
    "slack_public_history_rail",
    "x_rail",
]
