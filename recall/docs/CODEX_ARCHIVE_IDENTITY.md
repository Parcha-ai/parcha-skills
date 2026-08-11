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
