# Retiring ordinary ingest success audits

PR #782 stopped new per-record operational jobs and ordinary success audits. Historical `canonical_audit_events` rows still occupy space. The owner in this change moves only reviewed old `ingest.commit` / `success` rows to an immutable operational backup before clearing their exact PostgreSQL rows.

## Authority and scope

A reviewed run names one tenant, one source, and an exclusive UTC cutoff. All other operations, failed and rejected statuses, rows at or after the cutoff, and other tenants or sources remain in PostgreSQL. Security, suppression, and forget audits remain unchanged. No canonical event, job, receipt, native identity, raw artifact, or forget authority changes. Operational audit rows are not replay or receipt authority.

The cutoff is meaningful only after every writer runs #782 or a descendant and transactions from older writers have drained. Normal audit IDs are hashes, not timestamps. Start with a null cursor; the SQL schema also permits an empty audit ID, so an empty string is not a safe initial cursor. The owner walks a bounded primary-key page first, then filters the eligible rows in that page; an empty eligible page is not end-of-source. Late inserts before a cursor require another inventory pass, so do not declare a population complete from a cursor alone.

## Recoverable pages

Each backup contains complete rows in PostgreSQL COPY CSV with explicit columns, UTC timestamps, round-trip floating-point output, and gzip compression. It uses the existing scoped ArchiveStore and exact immutable reference. The receipt records the scope, cutoff, page bounds, full-row byte hash, and count. It is operational recovery metadata, not a new searchable corpus or permanent archive format.

The database read ends before object upload. Clear reads and verifies the saved archive, then locks the exact keys, compares every column against the archive, rechecks eligibility, and commits deletion atomically. A missing, changed, corrupt, or differently scoped page cannot authorize a partial clear. Restore inserts missing archived rows and refuses to overwrite unequal rows. Existing identical rows are harmless.

The host journal persists intent before a once-only Render POST. An unknown outcome stays unknown until the exact original job or a read-only inspection resolves it. Reopening a journal never retries a mutation. Archive references and receipts must outlive the rollback window; deleting backup objects is a separate reviewed operation. Subject hashes remain sensitive operational metadata, consistent with the original table's retention; this backup does not claim anonymity.

## Physical reclamation

Deleting rows creates dead tuples and WAL. Plain VACUUM normally makes space reusable inside the same table; it does not generally return that allocation to the filesystem. This distinction matters because #782 stops most new writes that could reuse the freed space. [PostgreSQL 17 VACUUM documentation](https://www.postgresql.org/docs/17/sql-vacuum.html)

The disposable PostgreSQL pilot measures relation bytes before deletion, after deletion, and after `VACUUM FULL`, and compares every retained row after the rewrite. This is a measured maintenance option, not production authorization. `VACUUM FULL` needs an ACCESS EXCLUSIVE lock and space for a new copy and indexes. The root operator must choose a maintenance window and fresh WAL/headroom budget before running it on the production table; exceptional audit writes and forget can block while it runs.

Even returning files to the operating system does not prove a smaller billed disk. The production provider snapshot on 2026-10-13 showed storage shrinking disabled, with 325 GiB allocated on every node and a 275 GiB configured minimum. Allocation changes are a separate provider operation after physical reclamation and replication health are confirmed. [PlanetScale disk shrinking](https://planetscale.com/changelog/postgres-disk-shrinking)


## Operator invocation

Root prepares a private JSON config containing the exact deployed commit, service ID, four owner-module SHA256 pins (`audit_retirement`, `archive`, `archive_runtime`, `db`), the existing RenderJob checkpoint and fleet fields, and `scope` with `tenant_id`, `source_id`, and a UTC ISO cutoff with `+00:00`. `cutoff_sealed: true` records root's verified writer cutoff, not a self-discovered proof.

An archive payload is `{"operation":"archive","after":null,"limit":50000}`. Clear, inspect, and restore payloads contain their operation and the complete saved `receipt`. Review the SHA256 of canonical JSON `{"config": config, "payload": payload}` before launching:

```sh
python recall/scripts/retire_ingest_audits.py PRIVATE_NEW_DIRECTORY \
  --config PRIVATE_CONFIG.json --payload PRIVATE_PAYLOAD.json \
  --expected-sha256 REVIEWED_OPERATION_SHA256
```

Each directory is create-once, with fsynced intent and immutable event files. Save a successful archive `result.json` durably before approving its clear operation. The caller advances only from the archived receipt's `next_after`; `exhausted` means the bounded snapshot had fewer keys than the requested page size. It does not prove the fleet had no late writers.

To fetch the exact original job outcome without relaunching it:

```sh
python recall/scripts/retire_ingest_audits.py EXISTING_PRIVATE_DIRECTORY \
  --reconcile-job ORIGINAL_RENDER_JOB_ID --expected-sha256 ORIGINAL_OPERATION_SHA256
```

A failed job or missing completion log can leave a committed clear unresolved. Run a separately reviewed `inspect` operation on its original archive receipt: `present` means all archived bytes still match; `absent` means all exact keys are gone; `changed_or_partial` needs investigation. Absence alone does not attribute who deleted the rows. Never repeat a clear merely because a job failed or its response was lost. Archive-only failures never authorize deletion.

## Disposable pilot evidence

The first PostgreSQL 17 pilot archived and cleared 29,700 ordinary rows out of 30,000. All 300 retained exceptional rows matched before and after. Relation size stayed at 12,066,816 bytes after deletion and became 155,648 bytes after the isolated rewrite. The complete backup was 793,122 compressed bytes for 7,246,800 COPY bytes. Archive plus clear took 1.34 seconds; the observed WAL delta was 3,336,544 bytes. Restore recovered all 30,000 rows. These local fixture values demonstrate behavior and sizing mechanics; they do not predict production throughput or lock duration.
