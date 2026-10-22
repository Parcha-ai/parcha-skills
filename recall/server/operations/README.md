# Search authority indexes

These optional access paths let the existing search authority query read receipt
and document metadata from indexes. They do not change search predicates, ranking,
schema versions, readiness requirements, or historical receipt uniqueness. They
are operator actions, outside automatic migrations and application startup.

The document index contains current, nondeleted document identities. The receipt
index covers all historical receipts and includes the metadata needed by the
existing query. After both builds are verified, an atomic cutover replaces the old
receipt uniqueness constraint and its index with the covering one. The new index
and constraint retain the same name on repeated application.

## Apply

Use a dedicated owner connection with `search_path=public`, after checking disk,
WAL and replica headroom for the temporary duplicate index and build scratch.
Record current index definitions, dependencies, fleet revisions, read latency and
provider load. Do not restart workers to apply these access paths.

Run the concurrent-build statements in order, checking each build before starting
the next. `IF NOT EXISTS` is not proof of validity. An interrupted or unknown
outcome requires inspection of the named index and active build before any retry;
never replay a build blindly or drop an unrelated index.

```sh
PGDATABASE="$RECALL_DATABASE_ADMIN_URL" \
PGOPTIONS='-c search_path=public -c statement_timeout=1800000 -c lock_timeout=1000 -c maintenance_work_mem=64MB -c max_parallel_maintenance_workers=0' \
psql -X -v ON_ERROR_STOP=1 -c 'CREATE INDEX CONCURRENTLY IF NOT EXISTS canonical_documents_live_authority_idx ON public.canonical_documents(tenant_id,source_id,document_id) WHERE is_current AND deleted_at IS NULL'
```

Verify that index's exact definition, validity, readiness and size, and recheck
provider load/headroom before the second build.

```sh
PGDATABASE="$RECALL_DATABASE_ADMIN_URL" \
PGOPTIONS='-c search_path=public -c statement_timeout=1800000 -c lock_timeout=1000 -c maintenance_work_mem=64MB -c max_parallel_maintenance_workers=0' \
psql -X -v ON_ERROR_STOP=1 -c 'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS canonical_chunks_receipt_authority_key ON public.canonical_chunks(tenant_id,receipt) INCLUDE(source_id,document_id,deleted_at)'
```

Verify both definitions and validity before cutover. The checked-in concurrent
SQL file contains these same two build statements.

```sh
PGDATABASE="$RECALL_DATABASE_ADMIN_URL" \
PGOPTIONS='-c search_path=public -c statement_timeout=10000' \
psql -X -v ON_ERROR_STOP=1 -f authority_receipt_constraint.sql
```

The cutover takes a short exclusive table lock with a one-second acquisition
limit. It refuses invalid or incompatible indexes, unexpected constraint state,
or foreign keys referencing the old receipt constraint. It uses no `CASCADE`.
A transaction abort leaves the old constraint intact; successful replay preserves
index OIDs. A lost connection or COMMIT acknowledgement has an unknown outcome:
inspect exact constraint/index ownership before retrying.
Revoke any temporary owner credential after the operation finishes.

Verify exact definitions, validity, remaining constraint ownership, unchanged
schema history, actual index bytes and provider headroom. Run the real PostgreSQL
authority regressions and unchanged public search checks. In query plans, record
heap fetches as well as index-only scan names: recently modified pages still need
heap visibility checks. A warm microbenchmark does not prove first-use latency.

## Rollback

Before cutover, both indexes are additive: after verifying their identities and
absence of constraint ownership, they can be dropped concurrently. The existing
receipt constraint remains authoritative.

Use `psql -X -v ON_ERROR_STOP=1` for every rollback step and stop on any error.
After cutover, first recreate the original full-history unique index concurrently:

```sql
CREATE UNIQUE INDEX CONCURRENTLY canonical_chunks_tenant_id_receipt_key
    ON public.canonical_chunks(tenant_id,receipt);
```

Verify that exact index is valid and ready, then replace the covering constraint
atomically under a bounded lock. Any unexpected dependency must abort; do not use
`CASCADE` or remove uniqueness first in a separate transaction.

```sql
BEGIN;
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '10s';
LOCK TABLE public.canonical_chunks IN ACCESS EXCLUSIVE MODE;
ALTER TABLE public.canonical_chunks
    DROP CONSTRAINT canonical_chunks_receipt_authority_key;
ALTER TABLE public.canonical_chunks
    ADD CONSTRAINT canonical_chunks_tenant_id_receipt_key
    UNIQUE USING INDEX canonical_chunks_tenant_id_receipt_key;
COMMIT;
```

Confirm successful commit and inspect the restored constraint/index ownership
before running this separate final step:

```sql
DROP INDEX CONCURRENTLY public.canonical_documents_live_authority_idx;
```

The covering index disappears with its constraint. Query and service code need no
rollback; recheck serving latency, authority, schema history and storage afterward.

## Publication page order

`publication_page_time_id_index_concurrent.sql` adds the optional
`canonical_passages_page_time_id_idx` on
`(tenant_id, source_id, first_occurred_at, passage_id)`. It supplies the existing
publication cursor's exact order, including equal timestamps. Query predicates,
receipt authority, page limits, checkpoints and schema versions stay unchanged.
The operation is outside automatic migrations; serving does not require it.

Retain `canonical_passages_time_idx` on
`(tenant_id, source_id, first_occurred_at, last_occurred_at)`. Passage retrieval
has temporal-overlap predicates on both endpoints, and the Parquet passage stream
also filters both endpoints. A replacement/drop requires separate workload and
plan evidence; this operation supplies none.

1. Use the existing bounded index-operation owner with a dedicated connection.
   Record the current fleet, schema history, exact old/new index definitions and
   OIDs, constraint ownership, table/index sizes, provider disk/WAL/replica
   headroom and baseline serving/publication measurements. If the new name exists,
   require its exact definition and valid/ready/live state; an incompatible or
   invalid object requires explicit reconciliation, never `IF NOT EXISTS` alone.
2. Admit one concurrent build using the existing owner limits: 900-second
   statement timeout, 30-second lock timeout, 64MiB maintenance memory and zero
   parallel maintenance workers. Run the SQL file through the existing
   `BrainStore._migrate_concurrently` owner, outside a transaction block. Do not
   start another build or redeploy/restart writers as part of this operation.
3. Monitor the identified build PID in `pg_stat_progress_create_index`, recording
   phase, blocks/tuples completed, and locker waits. Compare provider CPU/I/O,
   replica lag, free space and serving health with the recorded baseline. The
   memory setting does not bound disk, WAL or total build work. Concurrent builds
   make two table scans and may wait for existing transactions; a small local
   build time cannot predict production duration. See PostgreSQL's
   [concurrent-build contract](https://www.postgresql.org/docs/17/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY)
   and [progress fields](https://www.postgresql.org/docs/17/progress-reporting.html#CREATE-INDEX-PROGRESS-REPORTING).
4. On a lost acknowledgement, timeout or disconnect, reconcile the same operation
   and exact index OID with the active build before any further DDL. An invalid
   leftover can consume space and write work. Do not retry or drop it implicitly.
   Once the owned build is terminal, an operator may inspect its exact definition
   and dependencies, explicitly drop that index concurrently, then separately
   admit a rebuild. Never cancel unrelated sessions or use `CASCADE`.
5. After success, run `publication_page_time_id_index_readiness.sql`; it requires
   the exact definition and valid/ready/live flags. Record actual bytes, unchanged
   schema history and old-index OID, then run original serving/authority checks.
   Compare ordinary selection-phase deadlines, whole-month completion and queue
   progress over a stated interval. Index existence or a changed plan alone is
   not a throughput result.

For footprint planning, measure bytes per row with representative key lengths
and multiply by the current catalog's estimated row count, explicitly retaining
that estimate's uncertainty. Add build scratch, WAL and replica effects; do not
scale local elapsed time or assume old-index space will be reclaimed. The new
index adds ongoing insert/update/delete maintenance. The exact index size and
write impact must be measured after installation.

Rollback changes no query or checkpoint: after confirming the exact new index
identity, no active build, and no unexpected constraint/dependency ownership,
run this single statement outside a transaction through the same bounded owner:

```sql
DROP INDEX CONCURRENTLY public.canonical_passages_page_time_id_idx;
```

Reconcile an uncertain drop before retrying. Verify the new index is absent,
`canonical_passages_time_idx` and schema history remain intact, and original
serving/authority checks pass. No schema marker, reader rollout or worker restart
is required for either apply or rollback.

## Historical Claude and Codex admission

`family_typed_candidate_index_concurrent.sql` supplies the bounded historical
admission query with one shared eligible-parent index. It includes completed
legacy Claude and noncurrent Codex modes, and excludes requested upgrades and
completed typed parents in both families. The existing Codex candidate index
includes completed typed Claude indefinitely; retain it for rollback, but install
and verify this family index before enabling the family-aware worker.

Use the existing guarded concurrent-index operation owner. Record exact fleet,
schema, definitions/OIDs, estimated rows, disk/WAL/replica headroom and baseline
health. Run the DDL through `BrainStore._migrate_concurrently` outside a transaction,
with the accepted CREATE-only `lock_timeout=0`, 900-second statement timeout,
960-second process limit, 64MiB maintenance memory and no parallel maintenance
workers. Short catalog reads retain their original limits. The statement/process
limits and exact-backend cancellation remain mandatory when lock timeout is zero.

Run `family_typed_candidate_index_readiness.sql` after acknowledged completion.
It requires the exact key, covering columns, predicate and valid/ready/live flags.
An existing invalid or different object is not success. Reconcile unknown
acknowledgements against the same operation and OID before any retry or cleanup.
Retain `canonical_passage_documents_typed_candidate_idx` and
`canonical_passage_documents_upgrade_idx`; schema history and reader capabilities
do not change. Record actual index bytes and ordinary admission progress before
claiming production benefit. Build scratch, WAL and write maintenance are additional
costs; local plan improvements do not establish production latency.

Family admission uses the existing global upgrade window and owner/currentness
checks. Historical tool extraction adds passage and embedding work. Existing
content-hash retention/reuse still applies; newly exposed tool text can require
new embeddings. This operation does not establish answer coverage or reduce that
work. No source-specific repair or forced upgrade belongs in this operation.

For rollback, restore the previous worker admission code, verify no active build
or unexpected dependency, then explicitly drop only the new index through the
same bounded owner:

```sql
DROP INDEX CONCURRENTLY public.canonical_passage_documents_family_candidate_idx;
```

Reconcile an uncertain drop; verify original indexes/schema and serving authority.
No automatic schema migration installs or removes this optional index.
