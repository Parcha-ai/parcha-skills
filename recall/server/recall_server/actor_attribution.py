"""Stable content attribution, deliberately separate from access principals."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable


ACTOR_ID_RE = re.compile(r"actor_[0-9a-f]{32}\Z")
ACTOR_RELATIONS = frozenset({
    "author",
    "contributor",
    "owner",
    "organizer",
    "participant",
    "attendee",
})
MAX_ACTOR_LINKS = 64
MAX_EXTERNAL_SUBJECT_LENGTH = 512
EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+\Z")


def actor_id_for_principal(tenant_id: str, principal_id: str) -> str:
    """Derive the stable content identity paired with one login principal."""

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or not isinstance(principal_id, str)
        or not principal_id
    ):
        raise ValueError("invalid actor principal")
    return "actor_" + hashlib.sha256(
        f"{tenant_id}\0{principal_id}".encode()
    ).hexdigest()[:32]


def normalized_external_subject(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid actor subject")
    normalized = value.strip()
    if EMAIL_RE.fullmatch(normalized):
        normalized = normalized.casefold()
    if not normalized or len(normalized) > MAX_EXTERNAL_SUBJECT_LENGTH:
        raise ValueError("invalid actor subject")
    return normalized


class ActorIdentityIndex:
    """Purpose-bound blind indexes for provider identities stored by Recall."""

    def __init__(self, blind_index: Callable[..., str]):
        if not callable(blind_index):
            raise ValueError("actor identity index unavailable")
        self._blind_index = blind_index

    def lookup(
        self,
        tenant_id: str,
        connector_id: str,
        namespace: str,
        subject: str,
    ) -> tuple[str, str, str]:
        normalized = normalized_external_subject(subject)
        if not isinstance(tenant_id, str) or not tenant_id:
            raise ValueError("invalid actor identity tenant")
        lookup_connector = connector_id
        lookup_namespace = namespace
        if EMAIL_RE.fullmatch(normalized):
            lookup_connector = "identity"
            lookup_namespace = "email"
        if not lookup_connector or not lookup_namespace:
            raise ValueError("invalid actor identity namespace")
        digest = self._blind_index(
            normalized,
            purpose=(
                "actor-identity-v1:"
                f"{tenant_id}:{lookup_connector}:{lookup_namespace}"
            ),
        )
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid actor identity index")
        return lookup_connector, lookup_namespace, digest


@dataclass(frozen=True, order=True)
class NativeActorReference:
    namespace: str
    subject: str
    relation: str


_REFERENCE_FIELDS = (
    ("author_id", "author", False),
    ("owner_ids", "owner", True),
    ("organizer_id", "organizer", False),
    ("attendee_ids", "attendee", True),
    ("participant_ids", "participant", True),
)


def native_actor_references(event: dict[str, Any]) -> tuple[NativeActorReference, ...]:
    """Extract typed provider identities without guessing names from prose."""

    content = event.get("content") if isinstance(event, dict) else None
    if not isinstance(content, dict):
        return ()
    references: set[NativeActorReference] = set()
    for field, relation, multiple in _REFERENCE_FIELDS:
        value = content.get(field)
        values = value if multiple and isinstance(value, list) else [value]
        for subject in values:
            if not isinstance(subject, str):
                continue
            try:
                normalized = normalized_external_subject(subject)
            except ValueError:
                continue
            references.add(NativeActorReference(field, normalized, relation))
    if len(references) > MAX_ACTOR_LINKS:
        raise ValueError("too many actor attributions")
    return tuple(sorted(references))


def is_local_user_authored(event: dict[str, Any], connector_id: str) -> bool:
    """Recognize only canonical user-message records from owned coding sessions."""

    if not isinstance(event, dict) or not isinstance(connector_id, str):
        return False
    provenance = event.get("provenance")
    harness = provenance.get("harness") if isinstance(provenance, dict) else None
    content = event.get("content")
    if not isinstance(content, dict):
        return False
    if harness == "claude" or connector_id.startswith("claude"):
        if (
            content.get("type") != "user"
            or content.get("isSidechain") is True
            or content.get("isMeta") is True
        ):
            return False
        message = content.get("message")
        body = message.get("content") if isinstance(message, dict) else content.get("content")
        if isinstance(body, str):
            return bool(body.strip())
        return isinstance(body, list) and any(
            isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
            and bool(block["text"].strip())
            for block in body
        )
    if harness == "codex" or connector_id.startswith("codex"):
        payload = content.get("payload")
        return (
            content.get("type") == "event_msg"
            and isinstance(payload, dict)
            and payload.get("type") == "user_message"
            and isinstance(payload.get("message"), str)
            and bool(payload["message"].strip())
        )
    return False


def attribute_canonical_events(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    events: Iterable[tuple[str, str, dict[str, Any]]],
    identity_index: ActorIdentityIndex | None = None,
) -> int:
    """Persist exact event actors; source contributors remain source bindings."""

    prepared = list(events)
    inserted = 0
    authored_event_ids = sorted({
        event_id
        for event_id, connector_id, event in prepared
        if is_local_user_authored(event, connector_id)
    })
    if authored_event_ids:
        result = connection.execute(
            """INSERT INTO canonical_event_actors(
                   tenant_id,source_id,event_id,actor_id,relation
               )
               SELECT %s,%s,event_id,binding.actor_id,'author'
                 FROM unnest(%s::text[]) AS selected(event_id)
                 JOIN canonical_source_actor_bindings binding
                   ON binding.tenant_id=%s AND binding.source_id=%s
                  AND binding.relation IN ('contributor','owner')
               ON CONFLICT DO NOTHING""",
            (tenant_id, source_id, authored_event_ids, tenant_id, source_id),
        )
        inserted += max(0, result.rowcount)
    if identity_index is None:
        return inserted
    references: list[dict[str, str]] = []
    for event_id, connector_id, event in prepared:
        for reference in native_actor_references(event):
            lookup_connector, namespace, digest = identity_index.lookup(
                tenant_id,
                connector_id,
                reference.namespace,
                reference.subject,
            )
            references.append({
                "event_id": event_id,
                "connector_id": lookup_connector,
                "namespace": namespace,
                "subject_hmac_sha256": digest,
                "relation": reference.relation,
            })
    if references:
        import json

        result = connection.execute(
            """INSERT INTO canonical_event_actors(
                   tenant_id,source_id,event_id,actor_id,relation
               )
               SELECT %s,%s,reference.event_id,identity.actor_id,
                      reference.relation
                 FROM jsonb_to_recordset(%s::jsonb) AS reference(
                      event_id text,connector_id text,namespace text,
                      subject_hmac_sha256 char(64),relation text
                 )
                 JOIN brain_actor_external_identities identity
                   ON identity.tenant_id=%s
                  AND identity.connector_id=reference.connector_id
                  AND identity.namespace=reference.namespace
                  AND identity.subject_hmac_sha256=
                      reference.subject_hmac_sha256
               ON CONFLICT DO NOTHING""",
            (tenant_id, source_id, json.dumps(references), tenant_id),
        )
        inserted += max(0, result.rowcount)
    return inserted


@dataclass(frozen=True, order=True)
class ActorLink:
    """A stable person-to-content relationship safe to persist in evidence."""

    actor_id: str
    relation: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.actor_id, str)
            or not ACTOR_ID_RE.fullmatch(self.actor_id)
            or self.relation not in ACTOR_RELATIONS
        ):
            raise ValueError("invalid actor attribution")

    def canonical(self) -> dict[str, str]:
        return {"actor_id": self.actor_id, "relation": self.relation}


def actor_links(values: Iterable[ActorLink | dict[str, Any]]) -> tuple[ActorLink, ...]:
    """Validate, de-duplicate, and canonically order actor relationships."""

    normalized: set[ActorLink] = set()
    try:
        for value in values:
            normalized.add(
                value
                if isinstance(value, ActorLink)
                else ActorLink(
                    actor_id=value["actor_id"],
                    relation=value["relation"],
                )
            )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("invalid actor attribution") from error
    if len(normalized) > MAX_ACTOR_LINKS:
        raise ValueError("too many actor attributions")
    return tuple(sorted(normalized))


def canonical_actor_links(values: Iterable[ActorLink]) -> list[dict[str, str]]:
    return [link.canonical() for link in actor_links(values)]
