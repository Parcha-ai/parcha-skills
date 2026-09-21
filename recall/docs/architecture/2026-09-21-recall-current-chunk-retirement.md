# Explicit current-chunk retirement and recovery

`python scripts/retire_current_chunks.py` previews or applies one explicit batch
of at most eight current documents in one tenant/source. It never discovers or
clears a whole corpus. Only `canonical_chunks.text_redacted` changes; receipts,
hashes, metadata, historical revisions and archived objects remain intact.

Before applying, verify the deployed readers use archive bodies and every writer
has restore-before-supersede support. The command requires
`RECALL_CHUNK_BODY_READS=archive` on apply, but cannot verify other deployments.
Schema 68 and positive published body locators are required for every target.
Unsupported, unlocated or not-yet-archived target revisions are refused, never accepted through a
PostgreSQL fallback.

Run from `recall/` with the existing database/archive runtime configuration.
Save the plan outside the checkout in a private directory. These are example
identifiers; use only the exact targets approved for the operation.

```sh
python scripts/retire_current_chunks.py \
  --tenant-id tenant:example --source-id source:example \
  --document-id doc_00000000000000000000000000000000 \
  --plan-file /private/retirement-plan.json
```

Review the private plan's exact document/native/revision/chunk identities and
byte counts. It is created exclusively with mode 0600; stdout contains only
counts and a proof digest. To apply, repeat the identical arguments with
`--apply`. The command repeats the archive proof and refuses a changed plan.
Do not repeatedly regenerate plans to bypass an unexplained refusal.

Every proof verifies the existing archive bytes, full text and each exact chunk
hash/receipt outside a database lease. Apply takes the writers' native locks,
locks current documents/chunks, and holds shared parent catalog locks. It then
rechecks the complete manifest/parts/locator/current-body snapshot and updates
only those verified current chunks. A busy lock, stale publication, corruption,
revision change or deadline expiry aborts the whole batch. PostgreSQL bodies are
bounded to 8 MB per document before transfer; archive results are bounded to
64 MiB. There are no new archive objects, cleanup jobs or filesystem deletions.

The default 20-second deadline is cooperative: SQL statements and archive work
share it, and it is checked before commit. Pool acquisition, DNS and COMMIT are
not forcibly deadline-cancelled. `--timeout-seconds` accepts at most 120 seconds.

## Recovery before disabling archive reads

Switching the read flag to PostgreSQL alone is **not** rollback after retirement.
Restore all still-current retired targets first, with the deployed readers and
writers kept on the compatible version. Use a **new** private plan file:

```sh
python scripts/retire_current_chunks.py \
  --tenant-id tenant:example --source-id source:example \
  --document-id doc_00000000000000000000000000000000 \
  --restore --plan-file /private/restoration-plan.json
```

Review that plan, then repeat with `--apply`. Restore uses the same exact archive
proof, publication/native/document/chunk locks, and transaction fence. It only
fills missing current chunk bodies; already-intact bytes stay untouched. The
operation is part of the proof, so a clear plan cannot authorize restoration or
vice versa. No network I/O occurs inside its write transaction.

A revision change invalidates an old plan. Restore-before-supersede already
preserves historical bodies; review the remaining current targets afresh.
After restoring every retired current target, verify PostgreSQL read parity
before disabling archive reads. Do not roll back to binaries that cannot read
locators or preserve outgoing revisions while retired bodies remain.
