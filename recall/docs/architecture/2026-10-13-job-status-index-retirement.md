# Retire the unused ingestion-job status index

Prepared operator; no production DDL has run. The October 13 17:07 catalog observation measured 515,710,976 bytes, no recorded scans, no constraint owner and only the three automatic column dependencies. The statistics reset time was NULL, so the observation does not establish a known no-use window.

`canonical_jobs_status_idx` indexes `(tenant_id,status,updated_at)` on `canonical_ingest_jobs`. Ordinary ingestion used to write a committed job per event. The runtime has no scheduler or status-ordered reader for these rows. New events now reuse immutable connector lineage; old event/job associations remain intact.

The current runtime reads the job table at two boundaries: exact `(tenant_id,source_id,job_id)` lookup when registering lineage, and the same-key join that proves the Slack connector for forgetting a delete-before-original receipt. Both use the primary key. Capability checks require the table, and backup/restore preserves its rows. Neither requires this secondary index. The exact code and deployed binary still need review together with the live observation.

## Admission and operation

1. Root captures the index's exact OID, definition, bytes, owner, validity, replica-identity status, constraints and both dependency directions. Capture `idx_scan`, `last_idx_scan`, database statistics reset and observation time. A zero counter is supporting evidence, not proof of no consumer. A nonzero counter requires identifying the reader and its alternative plan before removal.
2. Verify the expected ordinary nonunique btree index, no expression or predicate, no INCLUDE column, no constraint owner or incoming dependency, and the three expected columns/collations/operator classes. Preserve its exact recreate statement. Confirm no concurrent DDL owner, current serving probes, lock/replica health, and a role that owns the index. Refuse an absent, invalid or changed index as an unexpected starting state.
3. Under the existing durable operator transport, record the reviewed catalog identity and exact SQL before issuing a single autocommit statement with a bounded lock wait and process deadline:

   ```sql
   DROP INDEX CONCURRENTLY public.canonical_jobs_status_idx;
   ```

   Do not use CASCADE, a transaction wrapper, a normal blocking DROP fallback, or an automatic retry. A timeout or lost acknowledgement requires read-only catalog reconciliation. Partial concurrent removal may leave an invalid index; that is an unresolved state, not successful removal.
4. Read back absence, original job primary-key/constraint presence and unchanged retained row witnesses. Re-run existing ingestion/replay/Slack-forget probes. Compare relation/file and node free-space metadata, replica lag and WAL after convergence. The removed index's measured relation size is the upper-bound attribution; provider allocation is a separate decision.

PostgreSQL documents that concurrent removal preserves ordinary table access while waiting for conflicting transactions, cannot run in a transaction, and cannot remove a constraint-owned or partitioned index. See [PostgreSQL 17 DROP INDEX](https://www.postgresql.org/docs/17/sql-dropindex.html).

## Explicit recovery for a partial drop

The job supports `inspect`, `drop`, `recreate`, and `finish_drop` through a reviewed `CONFIG` composed by the existing operator transport. It refuses a missing runtime/database/catalog configuration. No schema marker or serving role privilege is changed.

A real PostgreSQL test holds an older reader transaction, cancels concurrent DROP after 250ms, and observes the original index still present with `indisvalid=false`. Ordinary drop and recreate refuse that state. After the original job is terminal and no DDL backend remains, root can review one `finish_drop` with the fresh invalid catalog, original durable intent SHA, original terminal job ID and original pre-drop catalog. The original OID, definition, primary key, owning table and dependencies must match; only the three index lifecycle flags may differ. A changed, absent or still-valid index is refused by this recovery mode.

The original intent/job provenance is checked by the host operator from its durable journal and provider GET; the database job does not treat a caller-supplied ID as provider evidence. Recovery gets a new independent intent and attempts one concurrent DROP. Any uncertain result again requires inspection. A valid unchanged index after a lock timeout and an absent index after lost acknowledgement are separately tested outcomes; neither triggers an automatic retry.

## Rollback

After the original removal's outcome is known, a separately reviewed operation can recreate the preserved index:

```sql
CREATE INDEX CONCURRENTLY canonical_jobs_status_idx
    ON public.canonical_ingest_jobs USING btree (tenant_id, status, updated_at);
```

This is the repository definition; use it only if the live definition matches exactly. Rebuilding requires table scanning, scratch/WAL headroom and time. It is not an instant rollback. Do not add `IF NOT EXISTS` to accept an invalid or differently defined same-name index. An incomplete build requires separate inspection and a reviewed recovery; never silently drop or replace it.

## Versioning decision

Use an explicit, Git-versioned maintenance operation, not a new numbered schema migration, for this first removal. Index absence does not change serving capability, data identity, FK ownership or schema readiness. Do not edit migration 019 or the schema verifier.

`BrainStore.migrate()` reruns every `_concurrent.sql` companion even when its numeric marker exists. Adding an unconditional DROP companion would undo the recreate rollback on the next migrate. Adding a schema marker only to force this optimization would require fleet compatibility changes with no new serving requirement.

There is a concrete limitation: databases that still replay migration 019 can recreate the index. Production with migration 067 recorded skips those pre-067 migrations. Fresh installs retain the historical index until this explicit maintenance operation is admitted. If the project later needs automatic index-retirement policy, address that as a distinct migration-runner change with repeat-migrate and rollback tests; do not hide it in this removal.

## Proof before shipping the operator

Use disposable PostgreSQL to preserve exact old job/event rows, remove this index concurrently, then retain existing scalar/batch lineage, legacy replay and terminal Slack forget tests unchanged. Verify rollback recreates the exact index. Witness refusal for wrong definitions/dependencies and conservative handling of a lost DDL outcome. No production table scan is necessary for the discovery packet.

Deleting legacy job rows is a separate, more expensive change: canonical events reference them, and the child table has no index beginning `(tenant_id,source_id,job_id)`. Repointing millions of events and then deleting jobs would generate heap/WAL traffic and potentially repeated FK scans. This proposal changes no rows.

Run the owning and retained tests on a developer host with:

```sh
greppy-batch --memory-gib 8 -- env RECALL_TEST_PYTHON=/path/to/python bash recall/server/tests/run_job_status_index_fixture.sh
```

The isolated PostgreSQL 17 fixture uses a separate bootstrap admin so `postgres` can be a nonsuperuser durable owner, matching the hosted credential boundary. Six DDL/recovery cases pass, including temporary role impersonation without broad grants. The fixture removes the index after every migration and runs the 27 unchanged connector-lineage/Slack-deletion cases and original lifecycle verifier. The same script runs in CI. No verifier expectation is edited.
