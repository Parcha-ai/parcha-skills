# Parent authority generations, shadow phase

Search currently proves each candidate passage's receipts against live canonical
rows. A published manifest cannot replace this check: an edit or partial Forget
can revoke a receipt before asynchronous publication catches up.

Migration 082 records a durable generation per tenant, source and native parent.
It does not change search, authorize a passage, certify a publication, or backfill
existing parents. Missing rows carry no authority. The generation table survives
catalog deletion and source identity recreation, so an old version cannot restart
at one merely because a disposable catalog disappeared.

## Mutation boundary

Statement triggers on canonical events, documents and chunks collect affected
OLD and NEW parent keys. A transaction-local table deduplicates them. Its deferred
trigger advances generations in sorted order at commit. The trigger does not lock
the published parent or acquire the logical publisher's advisory lock.

Chunk collection first deduplicates document keys and materializes their event
ownership before joining events. This boundary prevents the planner from joining
changed chunks to every event in the source before checking document identity.
The retained native batch test checks buffer work for 256 chunks across 64 documents
and 16 parents; it also proves every parent advances exactly once.

| Table | Fields that invalidate the generation |
| --- | --- |
| `canonical_events` | tenant, source, event ID, native ID, native parent ID |
| `canonical_documents` | tenant, source, document ID, event ID, native ID, revision, currentness, deletion, singleton receipt profile |
| `canonical_chunks` | tenant, source, chunk ID, document ID, ordinal, receipt, deletion |

Inserts and deletes include the same identities. Updates compare these fields,
including nullable profiles. A no-op update produces no pending key. This covers
scalar and batch ingest, edits, tombstones, Forget revocation and hard cleanup,
profile admission/restoration, and direct DML using older application code.
Parent moves invalidate both identities. Ordered hard deletion captures the keys
before their canonical owners disappear.

Body clearing/restoration, body-location changes, locator publication, timestamps
and unchanged metadata do not change receipt membership. They retain their
existing independent archive/body proofs. Pure body updates therefore do not
advance this generation. Event payload changes are also outside this narrow
membership version; it is not a complete version of every projection input.

Source grants, brain membership and token revocation remain separate access
checks. Publication, manifest hashes, passage receipt sets, headers and policy
bindings remain separate proofs. This migration adds no reader trust in any of
them. Administrative trigger disabling, table replacement or `TRUNCATE` is not a
supported membership mutation API and must not be introduced under a future
certificate contract without its own invalidation handling.

## Concurrency and failure behavior

Known runtime writers leave constraints deferred. Their existing row, queue and
advisory locking completes before the generation flush. The publisher need only
read an input generation when a later phase adds certification; it must never
replace that captured value with the generation current at publication commit.

An arbitrary client can force `SET CONSTRAINTS ALL IMMEDIATE`, retain an authority
lock, then acquire another writer's row. PostgreSQL can detect a deadlock and
abort one transaction. The retained regression proves that the document mutation
and generation increment roll back together. Sorting each flush cannot guarantee
a global lock order across multiple early flushes. No generation update is caught
and silently skipped, and no success acknowledgment is returned for an aborted
canonical transaction.

A fail-fast advisory/NOWAIT prototype avoided these waits but added failure paths.
In a synthetic four-writer counter benchmark, three attempts exhausted 9 of 80
operations. With the actual scalar/batch ingest owners, both implementations
committed all 80 operations without an added retry. These are bounded local
observations, not a production throughput or starvation guarantee. Ordinary
deferred invalidation keeps the smaller mechanism; existing caller retry and
error behavior remains unchanged.

## Rollout and next proof

Deploy the separate optional-82 capability compatibility change before applying
this migration. Migration 082 requires optional migration 080 and its nullable
smallint profile column; both the readiness dependency and migration preflight
reject a missing prerequisite. Keep the original membership verifier and search behavior.
Observe ingest failures, commit latency and generation/table growth after a
separately reviewed deployment. Dropping these triggers stops shadow recording;
retained generation values then cannot support certification without fresh
validation. Do not enable a reader against an interrupted shadow history.

A later change must bind an exact captured generation to both logical and passage
publication, prove existing receipt sets before enrollment, and compare the new
answer independently with the unchanged verifier. Enrollment must also prove
that each receipt's actual document/event owner belongs to the certified parent.
Physical membership follows the chunk's document ID; parsing the receipt cannot
supply this parent-ownership proof. Unknown, unenrolled or changed
parents must retain native receipt verification. Partial Forget, in-flight edits,
profile anomalies, publication replacement, source recreation and grant revocation
must keep their current outcomes. This migration alone provides no search speedup
or storage reduction.
