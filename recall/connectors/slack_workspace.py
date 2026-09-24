"""Workspace-wide Slack backfill and reconciliation connector."""

from __future__ import annotations

import json
import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from connectors.remote_api import RemoteApiError
from connectors.sdk import (
    ConnectorContractError, ConnectorPage, ConnectorRecordV2,
    ConnectorUpstreamError, SOURCE_ID,
)
from connectors.slack_source import (
    SLACK_MESSAGE_CAPTURE_VERSION, normalize_slack_message, normalize_slack_user,
)
from connectors.attachment_extract import extract_attachment_text


EPOCH = "1970-01-01T00:00:00Z"
CHANNEL_BATCH_SIZE = 50
MAX_CONFIGURED_CHANNELS = 128
LOG = logging.getLogger(__name__)
_DIAGNOSTIC_OPERATIONS = frozenset({
    "channels.list", "channels.join", "users.list", "messages.history", "messages.replies",
})
_DIAGNOSTIC_ERRORS = frozenset({
    "not_in_channel", "channel_not_found", "is_archived", "missing_scope",
    "invalid_auth", "not_authed", "token_revoked", "token_expired", "account_inactive",
    "not_allowed_token_type", "access_denied", "no_permission", "restricted_action",
    "team_access_not_granted", "org_login_required", "ekm_access_denied", "ratelimited",
    "invalid_cursor", "invalid_arguments", "internal_error", "fatal_error",
    "service_unavailable", "request_timeout", "method_not_supported_for_channel_type",
    "authority_revoked", "authority_forbidden", "upstream_error", "upstream_unavailable",
    "cursor_expired", "response_invalid", "content_type_invalid", "response_too_large",
    "redirect_rejected", "operation_not_allowed", "parameter_invalid", "parameter_not_allowed",
})


def _log_request_failure(operation: str, kind: str, error: Any) -> None:
    operation = operation if isinstance(operation, str) and operation in _DIAGNOSTIC_OPERATIONS else "unrecognized"
    code = error if isinstance(error, str) and error in _DIAGNOSTIC_ERRORS else "unrecognized"
    LOG.warning(
        "slack request failed operation=%s kind=%s error=%s", operation, kind, code,
        extra={"slack_operation": operation, "slack_error_kind": kind, "slack_error_code": code},
    )


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
    if response.get("has_more") is True and value is None:
        raise ConnectorContractError("slack page continuation is missing")
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
        "v": 4,
        "channel_lower": None,
        "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
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

    # Keep an in-flight V3 page and its original time window. It may not have
    # started at epoch, so finishing it must not certify an unknown baseline.
    if isinstance(value, dict) and value.get("v") == 3 and set(value) == v2_expected | {"coverage"}:
        value = {**value, "v": 4, "capture_version": 0, "channel_lower": (
            value["watermark"] if (value["phase"] == "history" and value["page"])
            or value["phase"] == "threads" else None
        )}
    expected = v2_expected | {"coverage", "channel_lower", "capture_version"}
    expected_coverage = "public" if public_history else "member"
    if (
        not isinstance(value, dict) or set(value) != expected or value.get("v") != 4
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
        or type(value.get("capture_version")) is not int or value["capture_version"] < 0
        or type(value.get("found")) is not bool
        or any(value.get(key) is not None and not isinstance(value[key], str)
               for key in ("page", "discovery_page", "thread_page"))
        or not _valid_time_bounds(value)
    ):
        raise ConnectorContractError("slack workspace cursor is invalid")
    if value["channel_lower"] is not None and not _valid_time_bounds({
        "watermark": value["channel_lower"], "upper": value["upper"],
    }):
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
        self._checkpoint_db: sqlite3.Connection | None = None

    @property
    def _scope(self) -> tuple[str, str]:
        return self.workspace_id, "public" if self.public_history else "member"

    @property
    def _selection(self) -> str:
        return hashlib.sha256(json.dumps(self.channel_ids).encode()).hexdigest()

    def bind_checkpoint_store(self, db: sqlite3.Connection) -> None:
        """Use the runner's identity-pinned spool, never an independent writer."""
        self._checkpoint_db = db
        db.execute("""CREATE TABLE IF NOT EXISTS slack_channel_coverage(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL, channel_id TEXT NOT NULL,
            last_seen_cycle INTEGER NOT NULL, history_through TEXT, scanned_cycle INTEGER,
            capture_version INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(workspace_id,authority,channel_id))""")
        db.execute("""CREATE TABLE IF NOT EXISTS slack_discovery_coverage(
            workspace_id TEXT NOT NULL, authority TEXT NOT NULL,
            started_cycle INTEGER, completed_cycle INTEGER,
            started_selection TEXT, completed_selection TEXT,
            completed_cycles INTEGER NOT NULL DEFAULT 0,
            completed_channels INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(workspace_id,authority))""")
        db.execute("""INSERT OR IGNORE INTO slack_discovery_coverage(workspace_id,authority)
                      VALUES (?,?)""", self._scope)

    def _channel_lower(self, state: dict[str, Any]) -> str:
        if state["channel_lower"] is not None:
            return state["channel_lower"]
        if self._checkpoint_db is not None:
            channel, _ = self._channel(state)
            row = self._checkpoint_db.execute(
                """SELECT history_through,capture_version FROM slack_channel_coverage
                   WHERE workspace_id=? AND authority=? AND channel_id=?""",
                (*self._scope, channel),
            ).fetchone()
            if row and row[0] and row[1] == SLACK_MESSAGE_CAPTURE_VERSION:
                return row[0]
        # No proven channel baseline. A global workspace watermark is not proof
        # that this channel was ever visited. Standalone pulls safely replay.
        return EPOCH

    def commit_checkpoint(self, previous: str | None, current: str) -> None:
        """Called only inside the runner's ACK/cursor transaction. No commit here."""
        db = self._checkpoint_db
        assert db is not None
        before = _state(previous, self.channel_ids, self.public_history)
        after = _state(current, self.channel_ids, self.public_history)
        started = (
            before["phase"] == "discover" and before["discovery_page"] is None
        ) or (bool(self.channel_ids) and before["phase"] == "users" and after["phase"] == "history")
        if started:
            db.execute("""UPDATE slack_discovery_coverage SET started_cycle=?,started_selection=?
                          WHERE workspace_id=? AND authority=?""",
                       (before["cycle"], self._selection, *self._scope))
        # Only discovery/configuration transitions register inventory, not every
        # history page. A channel absent for a full cycle needs a new baseline.
        if after["channels"] and (
            before["phase"] in {"users", "discover"}
            or before["channels"] != after["channels"]
        ):
            db.executemany("""INSERT INTO slack_channel_coverage
                (workspace_id,authority,channel_id,last_seen_cycle) VALUES (?,?,?,?)
                ON CONFLICT(workspace_id,authority,channel_id) DO UPDATE SET
                    history_through=CASE WHEN last_seen_cycle<excluded.last_seen_cycle-1
                        THEN NULL ELSE history_through END,
                    last_seen_cycle=excluded.last_seen_cycle""",
                [(*self._scope, item.rsplit(":", 1)[0], after["cycle"])
                 for item in after["channels"]])
        if before["phase"] in {"history", "threads"}:
            channel, _ = self._channel(before)
            completed = (before["cycle"] != after["cycle"]
                         or after["phase"] not in {"history", "threads"}
                         or self._channel(after)[0] != channel)
            if completed:
                lower = self._channel_lower(before)
                capture_version = (after["capture_version"]
                    if before["phase"] == "history" and before["page"] is None
                    else before["capture_version"])
                # A staged pre-upgrade page was fetched under the old global
                # window, even when its previous cursor had no page token.
                # New pulls produce V4 and choose the per-channel lower bound.
                if json.loads(current).get("v") == 3:
                    lower = before["watermark"]
                    capture_version = 0
                prior = db.execute("""SELECT history_through,capture_version FROM slack_channel_coverage
                    WHERE workspace_id=? AND authority=? AND channel_id=?""",
                    (*self._scope, channel)).fetchone()
                contiguous = lower == EPOCH or (prior and prior[0]
                    and prior[1] == SLACK_MESSAGE_CAPTURE_VERSION and
                    datetime.fromisoformat(lower.replace("Z", "+00:00")) <=
                    datetime.fromisoformat(prior[0].replace("Z", "+00:00")))
                db.execute("""INSERT INTO slack_channel_coverage
                    (workspace_id,authority,channel_id,last_seen_cycle,history_through,scanned_cycle,capture_version)
                    VALUES (?,?,?,?,?,?,?) ON CONFLICT(workspace_id,authority,channel_id)
                    DO UPDATE SET last_seen_cycle=excluded.last_seen_cycle,
                      history_through=excluded.history_through,scanned_cycle=excluded.scanned_cycle,
                      capture_version=excluded.capture_version""",
                    (*self._scope, channel, before["cycle"],
                     before["upper"] if contiguous and capture_version == SLACK_MESSAGE_CAPTURE_VERSION else None,
                     before["cycle"], capture_version))
        if after["cycle"] > before["cycle"]:
            db.execute("""UPDATE slack_discovery_coverage
                SET completed_cycle=?,completed_cycles=completed_cycles+1,
                    completed_selection=started_selection,
                    completed_channels=(SELECT count(*) FROM slack_channel_coverage
                        WHERE workspace_id=? AND authority=? AND last_seen_cycle=?)
                WHERE workspace_id=? AND authority=? AND started_cycle=? AND started_selection=?""",
                (before["cycle"], *self._scope, before["cycle"], *self._scope, before["cycle"], self._selection))

    def checkpoint_status(self) -> dict[str, Any]:
        """Aggregate evidence only: no channel IDs, API cursors, or message text."""
        assert self._checkpoint_db is not None
        discovery = self._checkpoint_db.execute("""SELECT started_cycle,completed_cycle,
            completed_cycles,completed_channels,started_selection,completed_selection
            FROM slack_discovery_coverage WHERE workspace_id=? AND authority=?""", self._scope).fetchone()
        selected = " AND channel_id IN (" + ",".join("?" for _ in self.channel_ids) + ")" if self.channel_ids else ""
        counts = self._checkpoint_db.execute("""WITH channels AS (
            SELECT last_seen_cycle,
                CASE WHEN capture_version=? THEN history_through END AS history_through,
                CASE WHEN capture_version=? THEN scanned_cycle END AS scanned_cycle
            FROM slack_channel_coverage WHERE workspace_id=? AND authority=?""" + selected + """)
            SELECT count(*),
            count(history_through),sum(CASE WHEN last_seen_cycle=? THEN 1 ELSE 0 END),
            sum(CASE WHEN scanned_cycle=? THEN 1 ELSE 0 END), min(history_through)
            FROM channels""",
            (SLACK_MESSAGE_CAPTURE_VERSION, SLACK_MESSAGE_CAPTURE_VERSION,
             *self._scope, *self.channel_ids, discovery[0], discovery[0])).fetchone()
        return {
            "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
            "scope": "public_channels" if self.public_history else "bot_accessible_public_channels",
            "selection": "configured_channels" if self.channel_ids else "workspace_discovery",
            "known_channels": counts[0], "history_baselined_channels": counts[1],
            "history_baseline_pending_channels": counts[0] - counts[1],
            "latest_discovery_channels": (counts[2] or 0) if discovery[4] == self._selection else 0,
            "latest_discovery_scanned_channels": (counts[3] or 0) if discovery[4] == self._selection else 0,
            "last_complete_discovery_channels": discovery[3] if discovery[5] == self._selection else None,
            "discovery_matches_current_selection": discovery[5] == self._selection,
            "completed_discovery_cycles": discovery[2],
            "discovery_in_progress": discovery[0] != discovery[1],
            "oldest_history_through": counts[4],
            "historical_mutations_verified": False,
        }

    def _request(self, operation: str, query: dict[str, Any]) -> dict[str, Any]:
        try:
            if operation == "channels.join":
                response = self.rail.request(operation, json_body=query)
            else:
                response = self.rail.request(operation, query=query)
        except RemoteApiError as error:
            _log_request_failure(operation, "transport", error.code)
            raise ConnectorUpstreamError("connector_upstream_error") from None
        if not isinstance(response, dict):
            _log_request_failure(operation, "response", "response_invalid")
        elif response.get("ok") is not True:
            _log_request_failure(operation, "response", response.get("error"))
        return _response(response)

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
            **state, "channels": channels, "channel_index": 0, "channel_lower": None,
            "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
            "discovery_page": next_page, "found": state["found"] or bool(channels),
        }
        if channels:
            return self._page([], {**state, "phase": "history", "page": None}, True)
        if next_page:
            return self._page([], state, True)
        # A successful empty listing is an observed empty scope, not a provider
        # failure. Keep its cycle so a returning channel is rebaselined.
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
            "channel_lower": None,
            "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
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
        state = {**state, "channel_lower": self._channel_lower(state)}
        if state["page"] is None:
            state["capture_version"] = SLACK_MESSAGE_CAPTURE_VERSION
        query: dict[str, Any] = {
            "channel": channel_id, "inclusive": True, "limit": self.page_size,
            "latest": _oldest(state["upper"]),
        }
        if state["page"]:
            query["cursor"] = state["page"]
        elif state["channel_lower"] != EPOCH:
            query["oldest"] = _oldest(state["channel_lower"])
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
                "thread_page": None, "channel_lower": None,
                "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
            }, True)
        if self.channel_ids and state["configured_index"] < len(self.channel_ids):
            return self._page(records, self._configured_batch(state), True)
        if not self.channel_ids and state["discovery_page"]:
            return self._page(records, {
                **state, "phase": "discover", "page": None, "channels": [],
                "channel_index": 0, "threads": [], "thread_index": 0,
                "thread_page": None, "channel_lower": None,
                "capture_version": SLACK_MESSAGE_CAPTURE_VERSION,
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
