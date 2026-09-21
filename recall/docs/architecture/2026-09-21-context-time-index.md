# Optional timestamp index for session context

Session context asks for the nearest eligible events ordered by `occurred_at,
native_id`. Migration 040 replaced the older time index with a session index
whose first ordering key after tenant/source/parent is `source_ordinal`. That
index can find a parent, but cannot directly return its timestamp neighbors.

A scoped production plan on September 21 examined 13,315 events before an anchor
and 7,095 after it to return two neighbors in each direction. Both directions
sorted the matching events. Document/chunk/tombstone eligibility checks already
stopped after two valid neighbors; the remaining excess work was the event scan.
A separate fixed-receipt observation spent 4.526 seconds in those queries during
a 5.512-second first context request, versus about 61–65 ms on warm repetitions.
These are small diagnostic samples, not a production latency distribution.

The optional access path is:

```sql
CREATE INDEX CONCURRENTLY canonical_events_context_time_idx
    ON public.canonical_events
       (tenant_id, source_id, (COALESCE(native_parent_id, native_id)),
        occurred_at, native_id);
```

It preserves timestamp/native-ID semantics and allows both forward and backward
index scans. It adds no prose copies or cache, and changes no authorization,
revision, tombstone, archive, or response logic. A disposable PostgreSQL fixture
with 1,201 events and source ordinals opposite to timestamp tie order examined
1,201 events before the index and two afterward, with identical responses.

## Why retain the existing indexes

The source-order index supports parent lookups and the logical projector's
source ordering. Replacing it would require measuring those owning paths, not
only context. The source-wide recent-search index has no parent key; it also
has a different purpose, including broad recent-event access. A new parent/time
index cannot replace its source-wide ordering. A past zero scan counter is not
proof that either index is dispensable: counters reset, and low-frequency
operations still matter. This change drops neither index. A separate measured
retirement of an unused access path can recover that space later.

The existing source-order index was about 4.1 billion allocated bytes at the
measurement. That provides scale, not an exact size forecast for the new index.
The added disk/WAL/build-I/O cost must be admitted explicitly against the storage
reduction objective. This index is not free space reclamation.

## Explicit operation; serving compatibility remains unchanged

This tool is deliberately outside `schema/*.sql` discovery. Ordinary `migrate`,
server startup, and capability checks do not install it. It adds no migration
marker and changes neither the mandatory serving floor nor accepted schema
versions. Existing runtimes on schema 69 or 70 continue serving before, during,
and after the index build. The serving role needs no DDL permission.

Inspect with the existing application DSN in `RECALL_DATABASE_URL`:

```sh
python server/scripts/build_context_time_index.py
```

Inspection uses a read-only connection and a five-second statement budget. It
returns `absent`, `ready`, `invalid`, or `incompatible`, plus numeric index
identity/size when present. It does not print connection strings, query text,
receipts, or bodies. Exact shape, Btree method, nonpartial/nonunique definition,
key count, direction, catalog collation/operator-class identities, validity, and
readiness are checked. Per-column index-definition text alone omits important
collation/operator-class details and is not treated as sufficient proof. Merely finding a
same-name index is not success.

An admitted operator may use a separate, short-lived table-owner credential:

```sh
python server/scripts/build_context_time_index.py --apply --timeout-seconds 900
```

`--apply` attempts one `CREATE INDEX CONCURRENTLY` in autocommit, then inspects
its result. A matching ready index returns `already_ready` without a second
build. Invalid or incompatible same-name objects are refused; there is no
`IF NOT EXISTS`, replacement, automatic cleanup, or retry. The build statement
budget is 1–1,800 seconds, with a one-second lock timeout, 64 MiB maintenance
memory, and no parallel maintenance workers. Connection establishment has a
five-second limit. These are statement/connection limits, not a hard total
process deadline; use the operator job's outer watchdog as well.

## Admission, monitoring, cancellation, and cleanup

1. **Admit the actual build.** Confirm the exact release/tool source, target
   cluster/database, owning role, and absence of another index/DDL operation on
   this table. Recheck current per-node free space, WAL retention, replica lag,
   and active workload. Reserve new index allocation, sort scratch, WAL growth,
   ordinary ingest growth, and the agreed free-space floor on every node. The
   tool does not measure or enforce these provider budgets. It does not copy
   the heap, but concurrently scans it; do not assume a harmless I/O load.
2. **Launch once and observe.** Record the dedicated role and backend identity
   (`application_name=recall-context-time-index`).
   Inspect `pg_stat_progress_create_index` for that exact backend/table/index,
   provider disk/WAL/replica metrics, and the unchanged reader checks. Do not log
   `pg_stat_activity.query`, credentials, or company rows. A concurrent build
   normally permits reads/writes, but may wait for older transactions and can
   compete for CPU and I/O. Pause other bulk maintenance rather than extending
   budgets automatically.
3. **Cancel only this operation if its bounds fail.** Use the existing operator
   cancellation channel or `pg_cancel_backend` only after matching the owned
   backend, role, table/index, and operation. Do not cancel unrelated sessions,
   force a checkpoint, advance slots, or change provider storage/configuration.
   Wait for actual terminal/backend disappearance evidence; a client timeout or
   lost acknowledgement alone does not establish cancellation or rollback.
4. **Inspect before deciding what happened.** A failed concurrent build can
   leave an invalid index. `inspect_required` means no automatic retry is safe;
   `ddl_acknowledged` distinguishes a returned CREATE from a failure before its
   acknowledgement. After any uncertain result, inspect again using a fresh
   connection. If the expected index is ready, preserve it and verify reads and
   plans. If it is invalid, record its exact OID/definition and prove that no
   build is still active before considering cleanup. Do not drop a ready,
   incompatible, changed-identity, or unowned index.
5. **Clean up explicitly, then decide on another attempt.** In an exclusive
   operator window, recheck that the exact known invalid index OID still has the
   expected definition and no active build. An operator can then run the fixed
   `DROP INDEX CONCURRENTLY public.canonical_events_context_time_idx` in its own
   autocommit connection with bounded statement and lock timeouts. The inspect
   and DROP are separate statements, not an atomic race fence; if concurrent
   DDL cannot be excluded, stop. Verify absence and actual temporary credential
   removal before a separately admitted retry. No worker or schema restart is
   required for a successful index to benefit readers.

A serving rollback can simply leave the optional index present. Removing a
valid index is a separate deliberate performance rollback, never failed-build
cleanup.

## Retained proof

`test_context_time_index.py` covers explicit/read-only modes, exact existing
shape refusal, one-attempt semantics, bounded configuration, and unknown CREATE
or post-CREATE acknowledgement outcomes. `e2e_context_time_index.py` exercises
real concurrent DDL and plans, refusal of same-name indexes with different text
collations or operator classes, exact context ordering, least-privilege serving
on both schema 69 and 70, refusal of runtime DDL, reads during a blocked build,
timeout leaving an invalid index, refusal to silently reuse it, and explicit
fixture cleanup/rebuild. Existing context authorization/revision/tombstone and
archive tests remain unchanged. No evaluator threshold or verifier is edited.
