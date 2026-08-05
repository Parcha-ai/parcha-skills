"""Stable content attribution, deliberately separate from access principals."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable


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
