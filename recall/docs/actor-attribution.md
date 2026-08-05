# Actor-aware company brains

Recall treats access identity and content attribution as different facts:

- A **principal** is an authenticated person or client that may access a brain.
- An **actor** is a human, service, or agent connected to content.
- An **actor relation** says how the actor is connected: `author`, `contributor`,
  `owner`, `organizer`, `participant`, or `attendee`.

This separation prevents a source owner or uploader from silently becoming the
author of everything they import. Actor filters always narrow an already
authorized source set; they never grant access.

## Data plane

```text
source-native identity ──> actor directory ──> event actor links
                                                │
source-to-employee binding ── contributor ──────┤
                                                v
immutable raw artifact -> canonical event -> logical document in S3
                                                │ actor IDs + relations
                                                v
                                      exact searchable passages
                                                │ actor IDs + relations
                                                v
                            actor-aware contextual embeddings
```

The actor directory holds current display names and searchable aliases. Provider
subjects are stored as HMAC-SHA-256 fingerprints scoped by tenant, connector,
and namespace. S3 logical-document records store stable actor IDs and relations,
not copied mutable profiles. An agent workspace should mount an `actors.json`
sidecar generated from
the authorized actor directory so `grep`, `jq`, and other exact inspection tools
can resolve those IDs.

Passage text remains exact and citation-safe. Actor names and relations are added
only to the fingerprinted contextual embedding representation as bounded recall
hints. Structured actor links remain the authority for person filters.

## Relation semantics

- Slack message sender or document creator: `author`
- Google document owner: `owner`; named co-editors: `contributor`
- Calendar creator: `organizer`; invitees: `attendee`
- Codex or Claude session enrolled from an employee's source: `contributor`
- Assistant/model output in that session is not represented as employee-authored

“What did Alice write/send?” should restrict to `author`. “What did Alice work
on/do?” may include `author`, `contributor`, `owner`, and `organizer`. The retrieval
agent may resolve an ambiguous human name, but the host applies the resulting
actor IDs as an authoritative filter inside the tenant and source authorization
boundary.

## Retrofitting existing data

Raw artifacts and canonical events do **not** need to be re-ingested.

1. Create an actor for each enrolled employee and link their login principal.
2. Bind their existing Codex/Claude sources as `contributor`.
3. Add exact event-level actors where a connector already exposes author IDs.
4. Queue all existing logical documents for those sources with
   `seed_backfill(include_existing=True)`.
5. Run logical-document and passage projectors. This replaces derived S3 objects
   and passage rows while retaining immutable raw artifacts.
6. Build the actor-aware contextual representation. Cut it over only after the
   person-attribution retrieval eval passes; the prior fingerprint remains a
   rollback target.

Changing an actor's searchable name or aliases must invalidate that actor's
contextual representations, but never the raw artifact, exact passage text, or
citations.
Actors with attributed content are retired with `active=false`; they are not
hard-deleted while immutable logical documents still reference their IDs.

## Employee enrollment lifecycle

1. An owner invites an exact verified email and records the employee's display
   name. OAuth acceptance creates one stable principal for access and one stable
   actor for content; the two IDs remain separate and explicitly linked.
2. Each source-local Codex or Claude route created by that employee is owned by
   their principal and bound to their actor as `contributor`. Every accepted
   company member receives read access to company sources, including sources
   that existed before they joined and sources created after they joined.
3. Canonical ingestion marks only structurally verified user messages as
   `author`. Assistant responses, tool results, sidechains, and duplicate Codex
   envelopes do not become employee-authored content.
4. Provider directory identities are registered as tenant-scoped blind indexes.
   Typed connector fields such as Slack `author_id` resolve to event actors in
   the same transaction as canonical ingestion. Human-authored provider records
   without a chat-harness role still enter actor-aware dense passages; records
   with only a source `contributor` binding remain sparse unless they already
   have an explicit visible role.
5. Removing a member revokes their MCP grant and live collector credentials,
   deletes their route configuration, and removes their source grants. It never
   deletes previously attributed company history or rewrites its actor IDs.

External identities are purpose-bound by tenant, connector, and namespace before
storage. A provider ID registered in one company cannot correlate with or resolve
inside another company even when both companies use the same Recall deployment.

## Connector contract

New connectors should emit source-native identity references already present in
the typed connector contracts (`author_id`, `owner_ids`, organizers, attendees,
and participants). Ingestion resolves those references through
`brain_actor_external_identities`, writes `canonical_event_actors`, and queues the
affected logical document in the same transaction.

The reusable ingestion resolver is connector-independent. A fresh-PostgreSQL E2E
enrolls four synthetic employees, gives each separate Codex and Claude sources,
maps a shared Slack identity, projects logical documents and actor-context
embeddings, and proves person-filter isolation, assistant exclusion, denied-user
isolation, collector revocation, and preservation of company history.
