"""Slack protocol primitives shared by Tether transports.

This module deliberately has no Slack SDK or network dependency.  It owns the
parts of Slack's protocol that need identical behavior in synchronous runtime
code and asynchronous Hermes plugin code:

* canonical message edit/delete events,
* workspace-and-method scoped Retry-After coordination, and
* fail-closed cursor page validation.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


__all__ = [
    "CanonicalMessageMutation",
    "CursorPage",
    "CursorPageResult",
    "CursorProtocolError",
    "CursorState",
    "MutationDisposition",
    "MutationKind",
    "MutationNormalization",
    "RetryAfterCoordinator",
    "RetryAfterWindow",
    "SlackMethodKey",
    "canonicalize_message_mutation",
    "parse_retry_after",
    "validate_cursor_page",
]


class MutationKind(str, Enum):
    EDIT = "edit"
    DELETE = "delete"


class MutationDisposition(str, Enum):
    CANONICAL = "canonical"
    IGNORE = "ignore"
    NOT_MUTATION = "not_mutation"
    INVALID = "invalid"


@dataclass(frozen=True)
class CanonicalMessageMutation:
    """A Slack edit/delete with optional context preserved honestly.

    Slack's minimal ``message_deleted`` event does not identify the author or
    original thread.  Those fields therefore remain ``None`` until a durable
    message ledger resolves them.
    """

    kind: MutationKind
    target_ts: str
    event_ts: str | None
    team_id: str | None
    channel_id: str | None
    thread_ts: str | None
    actor_user_id: str | None
    replacement_text: str | None
    event_id: str | None


@dataclass(frozen=True)
class MutationNormalization:
    disposition: MutationDisposition
    reason: str
    mutation: CanonicalMessageMutation | None = None

    def __post_init__(self) -> None:
        has_mutation = self.mutation is not None
        if has_mutation != (self.disposition is MutationDisposition.CANONICAL):
            raise ValueError("only canonical normalization results carry a mutation")


_VISIBLE_MESSAGE_FIELDS = ("text", "blocks", "attachments", "files")


def _optional_identifier(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def _team_identity(
    envelope: Mapping[str, Any],
    event: Mapping[str, Any],
) -> tuple[str | None, bool]:
    candidates: list[Any] = [
        event.get("team"),
        event.get("team_id"),
        envelope.get("team_id"),
    ]
    team = envelope.get("team")
    if isinstance(team, Mapping):
        candidates.append(team.get("id"))
    elif team is not None:
        candidates.append(team)
    authorizations = envelope.get("authorizations")
    if authorizations is not None:
        if not isinstance(authorizations, Sequence) or isinstance(
            authorizations, (str, bytes, bytearray)
        ):
            return None, False
        for authorization in authorizations:
            if not isinstance(authorization, Mapping):
                return None, False
            candidates.append(authorization.get("team_id"))
    return _consistent_identifier(*candidates)


def _event_from_payload(
    payload: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
    nested = payload.get("event")
    if nested is None:
        return payload, payload
    if not isinstance(nested, Mapping):
        return None
    return payload, nested


def _invalid(reason: str) -> MutationNormalization:
    return MutationNormalization(MutationDisposition.INVALID, reason)


def _visible_content(
    message: Mapping[str, Any],
) -> tuple[tuple[str, Any], ...] | None:
    content: list[tuple[str, Any]] = []
    for field_name in _VISIBLE_MESSAGE_FIELDS:
        value = message.get(field_name, "" if field_name == "text" else [])
        if field_name == "text":
            if not isinstance(value, str):
                return None
        elif not isinstance(value, list):
            return None
        content.append((field_name, value))
    return tuple(content)


def _consistent_identifier(*values: Any) -> tuple[str | None, bool]:
    identifiers: list[str] = []
    for value in values:
        if value is None:
            continue
        identifier = _optional_identifier(value)
        if identifier is None:
            return None, False
        identifiers.append(identifier)
    if not identifiers:
        return None, True
    return identifiers[0], all(item == identifiers[0] for item in identifiers)


def canonicalize_message_mutation(
    payload: Mapping[str, Any],
) -> MutationNormalization:
    """Canonicalize Slack ``message_changed`` and ``message_deleted`` payloads.

    ``payload`` may be either the message event itself or an Events API
    envelope containing ``event``.  Known mutation subtypes fail closed when
    identity fields conflict or their documented nested structures are
    malformed.  Unrelated messages are returned as ``NOT_MUTATION``.
    """

    if not isinstance(payload, Mapping):
        return _invalid("payload_not_mapping")
    unpacked = _event_from_payload(payload)
    if unpacked is None:
        return _invalid("event_not_mapping")
    envelope, event = unpacked
    subtype = event.get("subtype")
    if subtype not in {"message_changed", "message_deleted"}:
        return MutationNormalization(
            MutationDisposition.NOT_MUTATION, "unrelated_subtype"
        )
    if event.get("type") not in {None, "message"}:
        return _invalid("mutation_type_not_message")

    channel_id, channel_consistent = _consistent_identifier(
        event.get("channel"), event.get("channel_id")
    )
    if not channel_consistent:
        return _invalid("conflicting_channel_id")
    event_ts = _optional_identifier(event.get("event_ts")) or _optional_identifier(
        event.get("ts")
    )
    event_id = _optional_identifier(envelope.get("event_id"))
    team_id, team_consistent = _team_identity(envelope, event)
    if not team_consistent:
        return _invalid("conflicting_team_id")

    if subtype == "message_deleted":
        previous = event.get("previous_message")
        if previous is not None and not isinstance(previous, Mapping):
            return _invalid("previous_message_not_mapping")
        previous = previous if isinstance(previous, Mapping) else {}
        target_ts, target_consistent = _consistent_identifier(
            event.get("deleted_ts"), previous.get("ts")
        )
        if not target_consistent:
            return _invalid("conflicting_deleted_ts")
        if target_ts is None:
            return _invalid("deleted_ts_missing")
        actor_user_id, actor_consistent = _consistent_identifier(
            event.get("user"), previous.get("user")
        )
        if not actor_consistent:
            return _invalid("conflicting_actor_user_id")
        thread_ts = _optional_identifier(previous.get("thread_ts"))
        return MutationNormalization(
            MutationDisposition.CANONICAL,
            "message_deleted",
            CanonicalMessageMutation(
                kind=MutationKind.DELETE,
                target_ts=target_ts,
                event_ts=event_ts,
                team_id=team_id,
                channel_id=channel_id,
                thread_ts=thread_ts,
                actor_user_id=actor_user_id,
                replacement_text=None,
                event_id=event_id,
            ),
        )

    message = event.get("message")
    previous = event.get("previous_message")
    if not isinstance(message, Mapping):
        return _invalid("message_not_mapping")
    if not isinstance(previous, Mapping):
        return _invalid("previous_message_not_mapping")
    target_ts, target_consistent = _consistent_identifier(
        message.get("ts"), previous.get("ts")
    )
    if not target_consistent:
        return _invalid("conflicting_message_ts")
    if target_ts is None:
        return _invalid("message_ts_missing")
    actor_user_id, actor_consistent = _consistent_identifier(
        message.get("user"), previous.get("user"), event.get("user")
    )
    if not actor_consistent:
        return _invalid("conflicting_actor_user_id")
    thread_ts, thread_consistent = _consistent_identifier(
        message.get("thread_ts"), previous.get("thread_ts")
    )
    if not thread_consistent:
        return _invalid("conflicting_thread_ts")
    current_content = _visible_content(message)
    previous_content = _visible_content(previous)
    if current_content is None or previous_content is None:
        return _invalid("message_content_malformed")
    if current_content == previous_content:
        return MutationNormalization(
            MutationDisposition.IGNORE, "metadata_only_edit"
        )
    return MutationNormalization(
        MutationDisposition.CANONICAL,
        "message_changed",
        CanonicalMessageMutation(
            kind=MutationKind.EDIT,
            target_ts=target_ts,
            event_ts=event_ts,
            team_id=team_id,
            channel_id=channel_id,
            thread_ts=thread_ts,
            actor_user_id=actor_user_id,
            replacement_text=message.get("text", ""),
            event_id=event_id,
        ),
    )


@dataclass(frozen=True, order=True)
class SlackMethodKey:
    workspace_id: str
    method: str

    def __post_init__(self) -> None:
        if not isinstance(self.workspace_id, str) or not self.workspace_id.strip():
            raise ValueError("workspace_id must be a non-empty string")
        if self.workspace_id != self.workspace_id.strip():
            raise ValueError("workspace_id cannot have surrounding whitespace")
        if not isinstance(self.method, str) or not self.method.strip():
            raise ValueError("method must be a non-empty string")
        if self.method != self.method.strip():
            raise ValueError("method cannot have surrounding whitespace")


@dataclass(frozen=True)
class RetryAfterWindow:
    key: SlackMethodKey
    requested_delay: float
    effective_delay: float
    deadline: float
    header_valid: bool


def _retry_after_header(headers: Mapping[str, Any]) -> Any:
    for name, value in headers.items():
        if isinstance(name, str) and name.lower() == "retry-after":
            return value
    return None


def parse_retry_after(
    headers: Mapping[str, Any],
    *,
    maximum: float = 3600.0,
) -> float | None:
    """Parse Slack's delta-seconds Retry-After header.

    Invalid, negative, non-finite, ambiguous multi-value, and non-mapping
    inputs return ``None``.  Valid values are capped at ``maximum`` to keep a
    malformed upstream response from suspending a worker indefinitely.
    """

    if not isinstance(headers, Mapping):
        return None
    if isinstance(maximum, bool) or not isinstance(maximum, (int, float)):
        raise TypeError("maximum must be a finite positive number")
    maximum = float(maximum)
    if not math.isfinite(maximum) or maximum <= 0:
        raise ValueError("maximum must be a finite positive number")
    raw = _retry_after_header(headers)
    if isinstance(raw, Sequence) and not isinstance(
        raw, (str, bytes, bytearray)
    ):
        if len(raw) != 1:
            return None
        raw = raw[0]
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("ascii")
        except UnicodeDecodeError:
            return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float, Decimal)):
        return None
    try:
        parsed = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    return float(min(parsed, Decimal(str(maximum))))


class RetryAfterCoordinator:
    """Coordinate Slack 429 backoff by workspace and Web API method."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        async_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        fallback_delay: float = 60.0,
        maximum_delay: float = 3600.0,
    ) -> None:
        for name, value, allow_zero in (
            ("fallback_delay", fallback_delay, True),
            ("maximum_delay", maximum_delay, False),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} must be numeric")
            if not math.isfinite(float(value)) or (
                float(value) < 0 if allow_zero else float(value) <= 0
            ):
                raise ValueError(f"{name} is outside its valid range")
        self._clock = clock
        self._sleep = sleep
        self._async_sleep = async_sleep
        self._fallback_delay = min(float(fallback_delay), float(maximum_delay))
        self._maximum_delay = float(maximum_delay)
        self._lock = threading.Lock()
        self._deadlines: dict[SlackMethodKey, float] = {}
        self._clock_high_water = float("-inf")

    def _now_locked(self) -> float:
        observed = float(self._clock())
        if not math.isfinite(observed):
            raise RuntimeError("monotonic clock returned a non-finite value")
        self._clock_high_water = max(self._clock_high_water, observed)
        return self._clock_high_water

    def record_429(
        self,
        key: SlackMethodKey,
        headers: Mapping[str, Any],
    ) -> RetryAfterWindow:
        parsed = parse_retry_after(headers, maximum=self._maximum_delay)
        requested = self._fallback_delay if parsed is None else parsed
        with self._lock:
            now = self._now_locked()
            deadline = max(self._deadlines.get(key, now), now + requested)
            self._deadlines[key] = deadline
            effective = max(0.0, deadline - now)
        return RetryAfterWindow(
            key=key,
            requested_delay=requested,
            effective_delay=effective,
            deadline=deadline,
            header_valid=parsed is not None,
        )

    def remaining(self, key: SlackMethodKey) -> float:
        with self._lock:
            now = self._now_locked()
            deadline = self._deadlines.get(key, now)
            remaining = max(0.0, deadline - now)
            if remaining == 0:
                self._deadlines.pop(key, None)
            return remaining

    def wait(
        self,
        key: SlackMethodKey,
        *,
        stop_event: threading.Event | None = None,
    ) -> bool:
        while True:
            if stop_event is not None and stop_event.is_set():
                return False
            delay = self.remaining(key)
            if delay <= 0:
                return True
            self._sleep(
                min(delay, 0.25)
                if stop_event is not None
                else delay
            )

    async def wait_async(self, key: SlackMethodKey) -> None:
        while True:
            delay = self.remaining(key)
            if delay <= 0:
                return
            await self._async_sleep(delay)


@dataclass(frozen=True)
class CursorState:
    """Durable state for a cursor-paginated Slack method."""

    next_cursor: str | None = None
    seen_cursors: tuple[str, ...] = ()
    pages_seen: int = 0
    complete: bool = False

    def __post_init__(self) -> None:
        if self.next_cursor is not None and (
            not isinstance(self.next_cursor, str)
            or not self.next_cursor
            or self.next_cursor != self.next_cursor.strip()
        ):
            raise ValueError(
                "next_cursor must be None or a non-empty string without surrounding whitespace"
            )
        if not isinstance(self.seen_cursors, tuple):
            raise TypeError("seen_cursors must be a tuple")
        if isinstance(self.pages_seen, bool) or not isinstance(self.pages_seen, int):
            raise TypeError("pages_seen must be an integer")
        if self.pages_seen < 0:
            raise ValueError("pages_seen cannot be negative")
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be a boolean")
        if len(set(self.seen_cursors)) != len(self.seen_cursors):
            raise ValueError("seen_cursors cannot contain duplicates")
        if any(
            not isinstance(cursor, str)
            or not cursor
            or cursor != cursor.strip()
            for cursor in self.seen_cursors
        ):
            raise ValueError(
                "seen cursors must be non-empty strings without surrounding whitespace"
            )
        if self.next_cursor is not None and self.next_cursor not in self.seen_cursors:
            raise ValueError("next_cursor must be present in seen_cursors")
        if self.complete and self.next_cursor is not None:
            raise ValueError("a complete cursor state cannot have a next cursor")


@dataclass(frozen=True)
class CursorPage:
    request_cursor: str | None
    next_cursor: str | None
    items: tuple[Mapping[str, Any], ...]
    page_number: int
    complete: bool


@dataclass(frozen=True)
class CursorPageResult:
    page: CursorPage
    state: CursorState


class CursorProtocolError(ValueError):
    def __init__(self, code: str, message: str, state: CursorState) -> None:
        super().__init__(message)
        self.code = code
        self.state = state


def _cursor_error(
    code: str,
    message: str,
    state: CursorState,
) -> CursorProtocolError:
    return CursorProtocolError(code, message, state)


def validate_cursor_page(
    response: Mapping[str, Any],
    state: CursorState = CursorState(),
    *,
    items_key: str = "messages",
    max_pages: int = 100,
) -> CursorPageResult:
    """Validate one Slack cursor page and return persistable continuation state."""

    if not isinstance(max_pages, int) or isinstance(max_pages, bool) or max_pages <= 0:
        raise ValueError("max_pages must be a positive integer")
    if not isinstance(items_key, str) or not items_key:
        raise ValueError("items_key must be a non-empty string")
    if not isinstance(state, CursorState):
        raise TypeError("state must be a CursorState")
    if state.complete:
        raise _cursor_error(
            "already_complete", "cursor sequence is already complete", state
        )
    if state.pages_seen >= max_pages:
        raise _cursor_error(
            "max_pages_exhausted",
            f"Slack pagination reached the {max_pages}-page safety limit",
            state,
        )
    if not isinstance(response, Mapping):
        raise _cursor_error(
            "response_not_mapping", "Slack cursor response is not a mapping", state
        )
    if response.get("ok") is not True:
        raise _cursor_error(
            "response_not_ok", "Slack cursor response is not successful", state
        )
    items = response.get(items_key)
    if not isinstance(items, list):
        raise _cursor_error(
            "items_not_list",
            f"Slack cursor response field {items_key!r} is not a list",
            state,
        )
    if any(not isinstance(item, Mapping) for item in items):
        raise _cursor_error(
            "item_not_mapping", "Slack cursor response contains a malformed item", state
        )
    metadata = response.get("response_metadata")
    if metadata is None:
        next_cursor = None
    elif not isinstance(metadata, Mapping):
        raise _cursor_error(
            "metadata_not_mapping",
            "Slack cursor response metadata is not a mapping",
            state,
        )
    else:
        raw_cursor = metadata.get("next_cursor", "")
        if not isinstance(raw_cursor, str):
            raise _cursor_error(
                "cursor_not_string", "Slack next cursor is not a string", state
            )
        if raw_cursor != raw_cursor.strip():
            raise _cursor_error(
                "cursor_whitespace",
                "Slack next cursor contains surrounding whitespace",
                state,
            )
        next_cursor = raw_cursor or None

    seen = list(state.seen_cursors)
    if state.next_cursor is not None and state.next_cursor not in seen:
        seen.append(state.next_cursor)
    pages_seen = state.pages_seen + 1
    if next_cursor is not None and next_cursor in seen:
        failed_state = CursorState(
            next_cursor=next_cursor,
            seen_cursors=tuple(seen),
            pages_seen=pages_seen,
        )
        raise _cursor_error(
            "repeated_cursor", "Slack returned a cursor already seen", failed_state
        )
    if next_cursor is not None:
        seen.append(next_cursor)
    next_state = CursorState(
        next_cursor=next_cursor,
        seen_cursors=tuple(seen),
        pages_seen=pages_seen,
        complete=next_cursor is None,
    )
    if next_cursor is not None and pages_seen >= max_pages:
        raise _cursor_error(
            "max_pages_exhausted",
            f"Slack pagination exceeded the {max_pages}-page safety limit",
            next_state,
        )
    page = CursorPage(
        request_cursor=state.next_cursor,
        next_cursor=next_cursor,
        items=tuple(dict(item) for item in items),
        page_number=pages_seen,
        complete=next_cursor is None,
    )
    return CursorPageResult(page=page, state=next_state)
