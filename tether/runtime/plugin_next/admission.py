"""Pure admission policy for Tether's gateway hook.

Fail closed: only an exactly-provenanced Slack event from an authorized
owner in the configured workspace, landing on a thread Tether has bound,
is admitted. Everything else is either explicitly denied (recorded, and in
active mode still passed to normal Hermes dispatch — Tether never blocks
traffic it does not own) or classified as not Tether's to handle.

No I/O, no Hermes imports: the hook layer supplies event fields and a
binding index; this module only decides.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable


VERDICT_ADMIT = "admit"
VERDICT_NOT_OURS = "not_ours"          # not Slack, or not a bound thread
VERDICT_DENY = "deny"                  # provenance failed on a bound thread
VERDICT_UNCONFIGURED = "unconfigured"  # no valid security domain configured


@dataclass(frozen=True)
class AdmissionSettings:
    """What admission actually needs: a workspace and an owner set.

    Persona and policy generation belong to the schema-18 security-domain
    descriptor, not to this decision — nothing below reads them. Requiring
    them here would force config keys the deployed schema-17 broker rejects
    outright, so the shadow would demand breaking production to observe it.
    """

    workspace_id: str
    allowed_users: frozenset[str]
    # Peer agents the operator explicitly trusts (the deployed broker's
    # TETHER_ALLOWED_BOT_USERS). A bot outside this set is still denied.
    trusted_bot_users: frozenset[str] = frozenset()

    @property
    def configured(self) -> bool:
        return bool(self.workspace_id and self.allowed_users)


def event_fingerprint(fields: dict[str, Any]) -> str:
    material = json.dumps(
        {
            "platform": fields.get("platform") or "",
            "workspace": fields.get("workspace") or "",
            "channel": fields.get("channel") or "",
            "thread": fields.get("thread") or "",
            "message_id": fields.get("message_id") or "",
            "actor": fields.get("actor") or "",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode()).hexdigest()


def evaluate(
    *,
    platform: str,
    workspace: str | None,
    channel: str | None,
    thread: str | None,
    actor: str | None,
    actor_is_bot: bool,
    message_id: str | None,
    settings: AdmissionSettings,
    bound_threads: Iterable[tuple[str, str]],
) -> dict[str, Any]:
    """One decision for one normalized inbound event."""
    fields = {
        "platform": platform,
        "workspace": workspace,
        "channel": channel,
        "thread": thread,
        "message_id": message_id,
        "actor": actor,
    }
    decision: dict[str, Any] = {
        "fingerprint": event_fingerprint(fields),
        "binding_ref": None,
    }
    if platform != "slack":
        decision.update(verdict=VERDICT_NOT_OURS, reason="not_slack")
        return decision
    if not settings.configured:
        decision.update(verdict=VERDICT_UNCONFIGURED, reason="security_domain_incomplete")
        return decision

    binding_key = (channel or "", thread or "")
    bound = binding_key in {
        (bound_channel, bound_thread)
        for bound_channel, bound_thread in bound_threads
    }
    if not bound:
        decision.update(verdict=VERDICT_NOT_OURS, reason="thread_not_bound")
        return decision
    decision["binding_ref"] = hashlib.sha256(
        f"{binding_key[0]}:{binding_key[1]}".encode()
    ).hexdigest()[:24]

    # Provenance on a bound thread is judged strictly and fails closed.
    if not workspace:
        decision.update(verdict=VERDICT_DENY, reason="workspace_unknown")
        return decision
    if workspace != settings.workspace_id:
        decision.update(verdict=VERDICT_DENY, reason="wrong_workspace")
        return decision
    if actor_is_bot:
        if actor and actor in settings.trusted_bot_users and message_id:
            decision.update(verdict=VERDICT_ADMIT, reason="trusted_peer_on_bound_thread")
            return decision
        decision.update(verdict=VERDICT_DENY, reason="untrusted_bot")
        return decision
    if not actor or actor not in settings.allowed_users:
        decision.update(verdict=VERDICT_DENY, reason="unauthorized_user")
        return decision
    if not message_id:
        decision.update(verdict=VERDICT_DENY, reason="event_identity_missing")
        return decision
    decision.update(verdict=VERDICT_ADMIT, reason="authorized_owner_on_bound_thread")
    return decision
