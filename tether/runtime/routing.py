from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import FrozenSet


class ConversationKind(str, Enum):
    CHANNEL = "channel"
    DM = "dm"
    MPIM = "mpim"


class EventKind(str, Enum):
    MESSAGE = "message"
    EDIT = "edit"
    DELETE = "delete"


class BindingKind(str, Enum):
    NATIVE = "native"
    HEADLESS = "headless"
    HERMES = "hermes"


class RouteAction(str, Enum):
    SILENT = "silent"
    HERMES = "hermes"
    NATIVE = "native"


@dataclass(frozen=True)
class MessageIdentity:
    """Slack's composite message identity.

    A timestamp is unique only inside its workspace and conversation. Storage
    and deduplication must therefore use all three fields.
    """

    team_id: str
    channel_id: str
    message_ts: str

    def __post_init__(self) -> None:
        if not self.team_id or not self.channel_id or not self.message_ts:
            raise ValueError("workspace, channel, and message timestamp are required")

    @property
    def dedupe_key(self) -> tuple[str, str, str]:
        return (self.team_id, self.channel_id, self.message_ts)


@dataclass(frozen=True)
class ThreadIdentity:
    team_id: str
    channel_id: str
    thread_ts: str

    def __post_init__(self) -> None:
        if not self.team_id or not self.channel_id or not self.thread_ts:
            raise ValueError("workspace, channel, and thread timestamp are required")

    def matches(self, message: "NormalizedMessage") -> bool:
        return (
            self.team_id == message.identity.team_id
            and self.channel_id == message.identity.channel_id
            and self.thread_ts == message.thread_ts
        )


@dataclass(frozen=True)
class ActorIdentity:
    user_id: str
    is_bot: bool = False
    bot_id: str = ""

    def __post_init__(self) -> None:
        if self.is_bot:
            if not self.user_id and not self.bot_id:
                raise ValueError("bot actor requires a Slack user ID or bot ID")
        elif not self.user_id:
            raise ValueError("human actor requires a Slack user ID")


@dataclass(frozen=True)
class NormalizedMessage:
    """Slack input after identity and mention resolution.

    Every explicit ``<@U...>`` mention must appear in exactly one of the bot,
    human, or unresolved partitions. This keeps the pure router from guessing
    whether a mention names another agent.
    """

    identity: MessageIdentity
    actor: ActorIdentity
    conversation_kind: ConversationKind
    observed_at: float
    thread_ts: str | None = None
    event_kind: EventKind = EventKind.MESSAGE
    mentioned_user_ids: FrozenSet[str] = field(default_factory=frozenset)
    mentioned_bot_user_ids: FrozenSet[str] = field(default_factory=frozenset)
    mentioned_human_user_ids: FrozenSet[str] = field(default_factory=frozenset)
    unresolved_mention_user_ids: FrozenSet[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at) or self.observed_at < 0:
            raise ValueError("message observation time must be a finite epoch")
        if self.thread_ts is not None and not self.thread_ts:
            raise ValueError("thread timestamp cannot be empty")
        partitions = (
            self.mentioned_bot_user_ids,
            self.mentioned_human_user_ids,
            self.unresolved_mention_user_ids,
        )
        if any(partitions[index] & partitions[other] for index in range(3) for other in range(index + 1, 3)):
            raise ValueError("mention classifications must be disjoint")
        if frozenset().union(*partitions) != self.mentioned_user_ids:
            raise ValueError("every explicit mention must be classified exactly once")


@dataclass(frozen=True)
class ActiveBinding:
    kind: BindingKind
    bridge_id: str
    writer_id: str
    owner_user_id: str = "*"
    active: bool = True
    binding_generation: int = 1
    ambient_owned: bool = False

    def __post_init__(self) -> None:
        if not self.bridge_id or not self.writer_id:
            raise ValueError("binding requires bridge and writer identities")
        if not self.owner_user_id:
            raise ValueError("binding owner cannot be empty")
        if self.binding_generation < 1:
            raise ValueError("binding generation must be positive")

    def admits(self, actor: ActorIdentity) -> bool:
        return self.owner_user_id == "*" or self.owner_user_id == actor.user_id


@dataclass(frozen=True)
class ParticipationLease:
    """A time-bounded, unique ambient-conversation ownership claim."""

    owner_bot_user_id: str
    writer_id: str
    expires_at: float
    competing_bot_user_ids: FrozenSet[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.owner_bot_user_id or not self.writer_id:
            raise ValueError("participation lease requires owner and writer identities")
        if not math.isfinite(self.expires_at) or self.expires_at < 0:
            raise ValueError("participation expiry must be a finite epoch")

    def uniquely_owned_by(self, bot_user_id: str, observed_at: float) -> bool:
        competitors = self.competing_bot_user_ids - {bot_user_id}
        return (
            self.owner_bot_user_id == bot_user_id
            and observed_at <= self.expires_at
            and not competitors
        )


@dataclass(frozen=True)
class ThreadState:
    identity: ThreadIdentity
    binding: ActiveBinding | None = None
    participation: ParticipationLease | None = None


@dataclass(frozen=True)
class RoutingPolicy:
    self_bot_user_id: str
    hermes_writer_id: str
    self_bot_id: str = ""
    allowed_human_user_ids: FrozenSet[str] = field(default_factory=frozenset)
    trusted_peer_user_ids: FrozenSet[str] = field(default_factory=frozenset)
    trusted_peer_bot_ids: FrozenSet[str] = field(default_factory=frozenset)
    allowed_channel_ids: FrozenSet[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if not self.self_bot_user_id or not self.hermes_writer_id:
            raise ValueError("local bot and Hermes writer identities are required")


@dataclass(frozen=True)
class RoutingDecision:
    action: RouteAction
    reason: str
    message_identity: MessageIdentity
    writer_id: str | None = None
    bridge_id: str | None = None
    binding_generation: int | None = None
    targeted_bot_user_ids: FrozenSet[str] = field(default_factory=frozenset)

    @property
    def dedupe_key(self) -> tuple[str, str, str]:
        return self.message_identity.dedupe_key


def _decision(
    message: NormalizedMessage,
    action: RouteAction,
    reason: str,
    *,
    writer_id: str | None = None,
    bridge_id: str | None = None,
    binding_generation: int | None = None,
) -> RoutingDecision:
    return RoutingDecision(
        action=action,
        reason=reason,
        message_identity=message.identity,
        writer_id=writer_id,
        bridge_id=bridge_id,
        binding_generation=binding_generation,
        targeted_bot_user_ids=message.mentioned_bot_user_ids,
    )


def decide_route(
    message: NormalizedMessage,
    thread: ThreadState | None,
    policy: RoutingPolicy,
) -> RoutingDecision:
    """Return one deterministic writer decision without performing I/O.

    Exact bot mentions have precedence over DMs, bindings, and participation.
    A trusted peer bot must explicitly mention the local bot. Human ambient
    replies require either an exact binding, a one-to-one DM, or a unique,
    unexpired participation lease.
    """

    if message.event_kind is not EventKind.MESSAGE:
        return _decision(message, RouteAction.SILENT, "unsupported_event_kind")

    if thread is not None and not thread.identity.matches(message):
        return _decision(message, RouteAction.SILENT, "thread_identity_mismatch")

    self_targeted = policy.self_bot_user_id in message.mentioned_bot_user_ids
    if message.mentioned_bot_user_ids and not self_targeted:
        return _decision(message, RouteAction.SILENT, "another_bot_explicitly_targeted")
    if message.unresolved_mention_user_ids and not self_targeted:
        return _decision(message, RouteAction.SILENT, "mention_resolution_incomplete")

    actor = message.actor
    if actor.is_bot:
        if (
            actor.user_id == policy.self_bot_user_id
            or (policy.self_bot_id and actor.bot_id == policy.self_bot_id)
        ):
            return _decision(message, RouteAction.SILENT, "self_message")
        trusted = (
            bool(actor.user_id and actor.user_id in policy.trusted_peer_user_ids)
            or bool(actor.bot_id and actor.bot_id in policy.trusted_peer_bot_ids)
        )
        if not trusted:
            return _decision(message, RouteAction.SILENT, "untrusted_peer_bot")
        if not self_targeted:
            return _decision(message, RouteAction.SILENT, "peer_bot_did_not_target_self")
    elif actor.user_id not in policy.allowed_human_user_ids:
        return _decision(message, RouteAction.SILENT, "human_not_authorized")

    if (
        message.conversation_kind is not ConversationKind.DM
        and policy.allowed_channel_ids
        and message.identity.channel_id not in policy.allowed_channel_ids
    ):
        return _decision(message, RouteAction.SILENT, "conversation_not_allowed")

    if thread is not None and thread.binding is not None and thread.binding.active:
        binding = thread.binding
        binding_addressed = (
            self_targeted
            or message.conversation_kind is ConversationKind.DM
            or binding.ambient_owned
        )
        if binding_addressed and not binding.admits(actor):
            return _decision(message, RouteAction.SILENT, "binding_owner_mismatch")
        if binding_addressed and binding.kind is BindingKind.NATIVE:
            return _decision(
                message,
                RouteAction.NATIVE,
                "active_native_binding",
                writer_id=binding.writer_id,
                bridge_id=binding.bridge_id,
                binding_generation=binding.binding_generation,
            )
        if binding_addressed:
            return _decision(
                message,
                RouteAction.HERMES,
                "active_hermes_binding",
                writer_id=binding.writer_id,
                bridge_id=binding.bridge_id,
                binding_generation=binding.binding_generation,
            )
        return _decision(
            message,
            RouteAction.SILENT,
            "active_binding_not_owned",
        )

    if self_targeted:
        return _decision(
            message,
            RouteAction.HERMES,
            "self_explicitly_targeted",
            writer_id=policy.hermes_writer_id,
        )

    if message.conversation_kind is ConversationKind.DM:
        return _decision(
            message,
            RouteAction.HERMES,
            "authorized_direct_message",
            writer_id=policy.hermes_writer_id,
        )

    if (
        thread is not None
        and thread.participation is not None
        and thread.participation.uniquely_owned_by(
            policy.self_bot_user_id,
            message.observed_at,
        )
    ):
        return _decision(
            message,
            RouteAction.HERMES,
            "unique_participation_lease",
            writer_id=thread.participation.writer_id,
        )

    return _decision(message, RouteAction.SILENT, "not_confidently_addressed")
