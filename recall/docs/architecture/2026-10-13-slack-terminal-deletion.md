# Slack message deletion and delayed ingestion

Slack message IDs are immutable workspace/channel/timestamp identities. Once an
accepted tombstone exists for an identity, a previously unseen history page or
delayed edit must not make that message current again. This fence applies only to
`slack.messages` records whose native ID starts with `slack:`. Other connectors
retain their existing restore behavior. A tombstone does not prove an omitted
message was deleted; this change creates no tombstones from incomplete history.

Both scalar and batch ingestion check the tombstone under the existing native
identity transaction lock, after source-owner and forget checks and raw artifact
registration. Migration 077 links suppressed raw artifacts to their owning native
identity hashes. The relation permits shared raw captures; forget deletes a raw
artifact only after all canonical and suppressed owners have been forgotten.
The delayed raw artifact remains available for audit until an explicit forget. No canonical
message revision, searchable document, or actor link is created for its body.

The ACK keeps the SDK's existing contract: `inserted + duplicate_events` equals
the submitted count, and every native ID receives a durable receipt. An unseen
body suppressed by this fence contributes to `duplicate_events` and the optional
subset diagnostic `suppressed_deleted_events`; its receipt names the existing
tombstone revision. The audit operation `ingest.slack_suppressed_deleted` records
one content-free identity hash and raw byte count, idempotently per event hash.
An exact content-hash replay keeps its original receipt and does not increment
the suppression diagnostic. No body is represented as newly indexed.

The fence does not repair rows resurrected before deployment. Inventory those
rows separately, in bounded tenant/source pages, before proposing any repair.
Provider revocation blocks future webhook routing; it does not erase previously
authorized company evidence. Shared `slack-file:` identities and inferred
historical deletions require separate authority rules and are outside this fix.
Historical mutation coverage must remain unverified until those checks exist.

Validation uses real PostgreSQL, raw archive storage, signed loopback webhooks,
and canonical logical/passage projectors. It covers delete-before-original,
unseen late edits, both concurrent orderings through scalar and batch callers,
raw/audit retention, exact replay, retired outgoing bodies, source/tenant and
owner isolation, provider revocation, and unchanged generic restore behavior.

The migration is additive and optional for startup (mandatory schema remains 69).
Deploy compatible runtimes before applying 077. The same migration also fixes a
preexisting shared-raw lifetime race across connectors: admissions and forget
acquire sorted artifact locks after native locks, and forget rechecks all owners
while holding those locks. A compact `canonical_artifact_forget_tombstones`
record commits before external deletion and permanently rejects later reuse of
that artifact reference, including under a different native identity. It has no
payload and does not depend on audit retention. Native forget tombstones retain
their existing meaning. Committed forget intent no longer counts as a retained
owner, so concurrent forgets of shared identities cannot strand the raw file.
Failed external deletion keeps the durable fence and retries the same work. Until its ownership table exists,
normal ingestion and existing forget work; an unseen body for a deleted Slack
identity fails closed with `slack_deletion_schema_required` instead of issuing an
untracked ACK. Apply 077 before enabling historical reconciliation. Deployments
must also recognize concurrently shipped optional migrations 075 and 076 before
those migrations are applied. Rolling back the fence to an older binary after
suppression begins is unsafe: old forget code cannot see suppressed raw owners.

The artifact fence is tenant/source scoped. Old schemas retain their existing
behavior until 077 exists; starting an old-schema process is not proof of the new
artifact deletion guarantee. Tests demonstrate admission-before-forget preserves
shared raw, forget-before-admission rejects the waiting owner, a fully forgotten
reference cannot recreate a raw registry row, and two simultaneous forgets erase
the shared file. The unchanged lifecycle test still requires zero raw registry
rows and exactly one native tombstone for a single identity.

The artifact fence is prospective: it records forget operations executed by this
runtime after migration 077. It does not retroactively identify raw objects
forgotten by older releases. Claiming that coverage requires an explicit
provenance inventory; no historical artifact backfill or production repair is
performed by this change.
