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
