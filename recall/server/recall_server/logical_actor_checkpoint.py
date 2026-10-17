"""Optional event-only completeness at the native logical publication boundary.

Supported mutation owners dirty the existing queue atomically. Direct SQL actor
or event mutations must do the same; the checkpoint is not an authority proof.
"""

from __future__ import annotations

import json
from psycopg.types.json import Jsonb

from .logical_append_reader import eligible, continues_snapshot

MAX_ACTORS = 64
MAX_CHECKPOINT_BYTES = 16384
FULL_ACTOR_SQL = """INSERT INTO canonical_evidence_document_actors(
                           tenant_id,source_id,logical_document_id,revision,
                           actor_id,relation
                       )
                       SELECT %s,%s,%s,%s,link.actor_id,link.relation
                         FROM (
                               SELECT actor.actor_id,actor.relation
                                 FROM canonical_events event
                                 JOIN canonical_event_actors actor
                                   ON actor.tenant_id=event.tenant_id
                                  AND actor.source_id=event.source_id
                                  AND actor.event_id=event.event_id
                                WHERE event.tenant_id=%s
                                  AND event.source_id=%s
                                  AND coalesce(
                                      event.native_parent_id,event.native_id
                                  )=%s
                                  AND (%s::bigint IS NULL OR event.source_ordinal<=%s)
                               UNION
                               SELECT binding.actor_id,binding.relation
                                 FROM canonical_source_actor_bindings binding
                                WHERE binding.tenant_id=%s
                                  AND binding.source_id=%s
                         ) link
                       ON CONFLICT DO NOTHING"""

EVENT_ACTOR_SQL = """SELECT DISTINCT actor.actor_id,actor.relation
    FROM canonical_events event
    JOIN canonical_event_actors actor
      ON actor.tenant_id=event.tenant_id AND actor.source_id=event.source_id
     AND actor.event_id=event.event_id
    WHERE event.tenant_id=%s AND event.source_id=%s
      AND coalesce(event.native_parent_id,event.native_id)=%s
      AND (%s::bigint IS NULL OR event.source_ordinal>%s)
      AND (%s::bigint IS NULL OR event.source_ordinal<=%s)
    LIMIT 65"""


def event_actors(connection, candidate, current, queued, locators, *, retain_queue):
    """Read a proven suffix, or initialize from all event revisions once."""
    from .logical_evidence_projection import _LocatorSpool

    complete = isinstance(locators, _LocatorSpool) and locators.complete_metadata
    frontier = locators.append_source_ordinal if complete else None
    if type(frontier) is not int:
        return None
    marker = current.get("actor_checkpoint") if current else None
    previous = None
    if (
        valid_checkpoint(marker, current)
        and (
            eligible(candidate, current, queued)
            or (retain_queue and continues_snapshot(candidate, current, queued))
        )
        and marker["source_ordinal"] <= frontier
    ):
        previous = marker["source_ordinal"]
    # A retained queue contains later events outside this captured publication.
    upper = frontier if retain_queue else None
    rows = connection.execute(
        EVENT_ACTOR_SQL,
        (
            candidate.tenant_id,
            candidate.source_id,
            candidate.native_parent_id,
            previous,
            previous,
            upper,
            upper,
        ),
    ).fetchall()
    actors = {(row["actor_id"], row["relation"]) for row in rows}
    if previous is not None:
        actors.update(tuple(pair) for pair in marker["actors"])
    if len(actors) > MAX_ACTORS:
        return None
    return sorted(actors)


def valid_checkpoint(marker, current):
    if not isinstance(marker, dict) or not current:
        return False
    if set(marker) != {
        "version",
        "manifest_id",
        "revision",
        "source_ordinal",
        "actors",
    }:
        return False
    if len(json.dumps(marker, separators=(",", ":")).encode()) > MAX_CHECKPOINT_BYTES:
        return False
    if (
        type(marker["version"]) is not int
        or marker["version"] != 1
        or type(marker["revision"]) is not int
        or marker["revision"] != current["revision"]
        or marker["manifest_id"] != current["manifest_artifact_id"]
        or type(marker["source_ordinal"]) is not int
        or marker["source_ordinal"] != current.get("append_source_ordinal")
        or current.get("append_manifest_id") != current["manifest_artifact_id"]
    ):
        return False
    actors = marker["actors"]
    return (
        isinstance(actors, list)
        and len(actors) <= MAX_ACTORS
        and all(
            isinstance(pair, list)
            and len(pair) == 2
            and all(isinstance(value, str) and 0 < len(value) <= 255 for value in pair)
            for pair in actors
        )
        and len({tuple(pair) for pair in actors}) == len(actors)
    )


def publish_actors(connection, prepared, revision, upper, actors):
    key = (prepared.tenant_id, prepared.source_id, prepared.logical_document_id)
    connection.execute(
        """DELETE FROM canonical_evidence_document_actors
        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
        key,
    )
    if actors is None:
        connection.execute(
            FULL_ACTOR_SQL,
            (
                *key,
                revision,
                prepared.tenant_id,
                prepared.source_id,
                prepared.native_parent_id,
                upper,
                upper,
                prepared.tenant_id,
                prepared.source_id,
            ),
        )
        return
    connection.execute(
        """INSERT INTO canonical_evidence_document_actors(
        tenant_id,source_id,logical_document_id,revision,actor_id,relation)
        SELECT %s,%s,%s,%s,link.actor_id,link.relation FROM (
            SELECT value->>0 AS actor_id,value->>1 AS relation
            FROM jsonb_array_elements(%s::jsonb)
            UNION SELECT actor_id,relation FROM canonical_source_actor_bindings
            WHERE tenant_id=%s AND source_id=%s
        ) link ON CONFLICT DO NOTHING""",
        (*key, revision, Jsonb(actors), prepared.tenant_id, prepared.source_id),
    )


def record_checkpoint(connection, candidate, manifest_id, revision, locators, actors):
    marker = None
    if actors is not None:
        marker = dict(
            version=1,
            manifest_id=manifest_id,
            revision=revision,
            source_ordinal=locators.append_source_ordinal,
            actors=actors,
        )
        if (
            len(json.dumps(marker, separators=(",", ":")).encode())
            > MAX_CHECKPOINT_BYTES
        ):
            marker = None
    # Called after queue ACK: its trigger invalidates old-writer completeness.
    connection.execute(
        """UPDATE canonical_evidence_documents SET actor_checkpoint=%s
        WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s
          AND manifest_artifact_id=%s AND revision=%s""",
        (
            Jsonb(marker) if marker is not None else None,
            candidate.tenant_id,
            candidate.source_id,
            candidate.native_parent_id,
            manifest_id,
            revision,
        ),
    )
