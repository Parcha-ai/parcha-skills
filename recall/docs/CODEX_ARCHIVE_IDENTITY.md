# Codex session identity contract

Codex can move a completed rollout from `sessions` to `archived_sessions`.
Recall treats that as one session changing location, never as a deletion plus a
new session.

The stable session key is resolved in this order:

1. The first safe `session_meta.payload.id` in the rollout. Later
   `session_meta` records may describe nested forks or resumes and do not
   replace the outer rollout identity.
2. The UUID in the rollout filename, only when native metadata is absent.

When both values exist, the filename UUID must equal the first metadata ID.
Unsafe, missing, or conflicting identity data is quarantined. New records must
never silently fall back to an absolute or relative filesystem path.

The path remains mutable provenance. During migration, an unambiguous local
ledger mapping preserves already acknowledged native record IDs and receipts.
`canonical_rewrite` receipt redirects are reserved for declared legacy
conflicts that cannot be reconciled, and redirect chains may not exceed one
hop.

The content-free audit command is:

```bash
PYTHONPATH=recall python3 recall/scripts/audit_codex_roots.py \
  --source-id codex:example \
  --active-root ~/.codex/sessions \
  --archive-root ~/.codex/archived_sessions
```

It reports only counts, byte totals, identity classifications, duplicate
classes, and timing. It never emits paths, session IDs, filenames, or record
content.

## Multi-root ledger

Set `RECALL_ARCHIVE_ROOT` for Codex collectors only. Discovery always visits
the active root before the archive root, then resolves all locations before it
decides that any prior file is missing. Create the configured archive directory
before starting the collector. If it later becomes unavailable, scanning fails
closed and queues no deletions.

The local spool keeps one session row keyed by native session ID and separate
location rows for active, archived, duplicate, conflicting, quarantined, and
missing paths. The session owns the durable record-key prefix. On first
upgrade, an existing path-keyed prefix is adopted from the acknowledged ledger
and remains authoritative after a move. New sessions receive a path-independent
prefix.

Byte-identical active/archive copies select the active location and queue no
duplicate records. Divergent copies with one native session ID are quarantined
as an identity conflict and never generate deletion or replacement traffic.
`doctor` reports active/archive coverage, conflicts, quarantine, duplicates,
and archive backlog using counts only.
