# Bounded parent chunk retirement

This opt-in job removes redundant **current `canonical_chunks.text_redacted` copies** after proving their exact bodies in the existing logical archive. It creates no source objects, preserves chunk identities/hashes/receipts, and leaves historical bodies in PostgreSQL. It does not run from the projection worker or select an entire corpus.

Use this after the explicit document canary in [the recovery runbook](2026-09-21-recall-current-chunk-retirement.md). All readers must use archive mode and all revision writers must have restore-before-supersede support. Apply requires `RECALL_CHUNK_BODY_READS=archive` and schema 069. These checks do not replace verification of the deployed readers and writers. Schema 069 also updates the exact startup capability contract: apply the reviewed migration and deploy the matching binary as a controlled sequence. An older binary with capability checks enabled can refuse startup against schema 069; do not treat it as a rollback target.

## One exact parent

Run from `recall/`, with credentials supplied by the approved runtime. `TENANT`, `SOURCE`, and `PARENT` identify one reviewed parent; never use wildcard input. Keep the plan in a private directory. The CLI creates a new mode-0600 file and prints only counts, timings, and its digest.

```bash
python scripts/retire_parent_chunks.py plan --tenant-id "$TENANT" --source-id "$SOURCE" --native-parent-id "$PARENT" --plan-file "$PRIVATE_PLAN"
python scripts/retire_parent_chunks.py enable --tenant-id "$TENANT" --source-id "$SOURCE" --native-parent-id "$PARENT"
python scripts/retire_parent_chunks.py apply --tenant-id "$TENANT" --source-id "$SOURCE" --native-parent-id "$PARENT" --plan-file "$PRIVATE_PLAN" --max-batches 10 --max-clear-bytes 1048576
```

Planning changes no PostgreSQL rows. Enabling is a separate, explicit scope action. Applying reads and proves the parent again; a saved plan cannot authorize a clear by itself. A changed manifest requires a new private plan. A parent that is still pending projection can contain unchanged, fully proven eligible records. Unsupported, unlocated, and not-yet-archived target revisions retain their PostgreSQL copies or cause the attempt to refuse incomplete proof.

Default limits are 64 documents, 4,096 chunks, and 8 MiB of actual PostgreSQL body hashing per transaction. Each transaction also gets at most five seconds of the remaining operation budget. The whole attempt defaults to 300 seconds, 1,000 batches, 1 GiB cleared, 8 GiB archive input, 2 million documents, 8 million chunks, 4 million records, and a 1 GiB private metadata spool. API limits can be smaller; document batches cannot exceed 256 and hashing cannot exceed 32 MiB. A document exceeding a batch budget is refused, never split into an unproved partial clear.

The proof reads each immutable part once per attempt, validates the whole parent hash, and uses the shared exact chunk verifier. It retains one part and a bounded event body in memory. A private SQLite spool contains only identities, locators, hashes, receipt metadata, and byte counts. The spool reserves headroom and has a hard page cap. Insufficient staging space or a corrupt late part fails before the first clear. SQLite is disposable staging, not a new authority or durable body store.

## Progress and interruption

Each batch takes the same sorted native locks as writers, locks and rechecks current documents and chunks, then shares the parent publication lock and locks its progress row. It compares the immutable manifest and exact metadata, hashes the actual nonempty PostgreSQL copies, clears only matching bodies, and commits its cursor and counters in the same transaction. No archive I/O occurs while these locks are held. Publication, tombstones, revisions, changed scope epochs, or unavailable locks stop stale work. Earlier committed batches remain valid.

A cap-limited result has `complete=false` and `status=partial`; this is not corpus completion. A normal result's counters describe that call. An exception carries known committed counters, but termination immediately after a commit can prevent those counters from reaching the process. The exact parent's ledger is authoritative for durable progress. Never infer rollback from missing process output.

Resume by running `apply` again with the same reviewed plan while its manifest remains current. Do not run `enable` on every resume: enabling resets the cursor and increments the scope epoch. A restart re-proves each parent part once; it does not issue a GET per document batch. The cursor is scheduling metadata, never proof. Explicit locator backfill and same-manifest projector repair reset enabled progress to pending; they preserve disabled scopes. Earlier newly eligible nonempty records are also rechecked even if they precede the cursor.

```bash
python scripts/retire_parent_chunks.py disable --tenant-id "$TENANT" --source-id "$SOURCE" --native-parent-id "$PARENT"
```

Disabling prevents further clears and does not restore bodies. Exact document restoration from the canary API disables the parent scope in the same transaction. **Restore every retired current body before switching readers back to PostgreSQL or rolling back to a binary without archive/history support.** Use bounded exact targets and the shared verified restore operation in the recovery runbook; do not turn the flag off first.

Deadlines are cooperative SQL/S3 checks with bounded transport inactivity and precommit checks. Pool acquisition, DNS, and COMMIT are not a hard wall-clock guarantee. There are no detached archive read threads or automatic retries of partially committed jobs.

## Measurement and rollout

`cumulative_cleared_utf8_bytes` is cumulative logical work. Restores and later revisions do not subtract from it. It is not current missing bytes, compressed TOAST size, reusable relation space, or provider disk freed. Report those storage measures separately. Per-attempt `hashed_utf8_bytes`, `hash_ms`, and `sql_ms` expose hashing work and database time.

A disposable local PostgreSQL benchmark over 32 MiB of synthetic text measured 7.4 ms for size/conversion work and 26.9 ms including SHA, about 19.5 ms incremental warm hashing time. This excludes production TOAST, storage, WAL, and pool costs. Start at 8 MiB and measure actual batch timing before raising it.

The rollout remains explicit: one bounded parent, exact read/history/forget checks and measured resource cost, then a reviewed parent selection and larger byte budget. A future steady-state scheduler should use this same proof and mutation boundary, the enabled scope state, and manifest identity. Deploy this version to MCP and both logical writers (projection and managed workers) before relying on ledger completion for scheduling: older writers preserve bodies but do not reopen progress after same-manifest locator repairs. This change adds no scheduler or automatic production activation, performs no filesystem deletion or full vacuum, and makes no claim that clearing logical bytes immediately shrinks provider volumes.
