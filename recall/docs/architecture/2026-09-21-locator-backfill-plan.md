# Existing-part locator coverage: dry run

The locator publisher can fill positions during ordinary projection, but deployment
alone does not cover existing parents. Reprojecting every parent would reread source
bodies and upload manifests unnecessarily. This planner instead reads each current
immutable logical part once, verifies the existing evidence, and proposes only NULL
positions on current eligible documents. It performs no uploads or database writes.

## Run a bounded private sample

Apply migration 068 first. Use the existing administrative environment with database
read access and evidence-object read access. The script does not retrieve credentials
or require archive write permission:

```sh
python server/scripts/plan_body_locators.py \
  --tenant TENANT --source SOURCE --limit 1 \
  --max-bytes 268435456 --seconds 60 \
  --output /private/new-locator-plan.json
```

The new report is created with mode 0600 and exclusive creation. Standard output
contains only aggregate counts. The private file contains source/parent identifiers,
current document identifiers, proposed ordinal/count pairs, exclusion counts, and a
snapshot fingerprint; it contains no source prose. `--resume PRIOR_REPORT` continues
the prior private keyset cursor in the same tenant/source scope. No `--apply` option
exists, and the module exposes no write operation.

Scope is **parents already present in the logical catalog**. Unprojected parents are
not silently counted as covered. A successful parent report proves only the current
snapshot; it does not establish historical recovery or authorize body deletion.
Failed parents are recorded and the cursor advances, so a later backfill must explicitly
retry failures rather than interpreting cursor completion as complete coverage.

## Proof and resource bounds

1. Capture parent manifest/parts, queue generation and changed-at, current document
   identities, revision hashes, chunk receipts/hashes, and existing positions in one
   read-only repeatable-read transaction. Historical documents and tombstoned events
   are excluded by the same authority conditions as the current-body reader.
2. Release the database connection. Check part tenant/source/logical identity and
   revision, ordinal topology, byte size and digest; decode the complete record stream
   and verify the aggregate parent digest and receipt count. Each part is fetched once.
3. Retain only the current event being verified, at most 8 MB. Reuse the reader's
   `_verified_body` check for exact current receipts, complete-event hash and every
   canonical chunk hash. Record only ordinal/count metadata. Existing correct positions
   produce no change; inconsistent existing positions fail the parent plan.
4. Repeat the metadata snapshot. Any append, revision, tombstone, queue-generation or
   publication change rejects the entire parent plan. No partial proposed positions
   escape a failed parent. Ingest remains active throughout the scan.

Default per-parent limits are 10,000 current documents, 100,000 chunks, 4,096 parts,
200,000 records and 256 MiB of object bytes. Every object is capped at 64 MiB. The
script shares its byte and time budgets across the parent page, stops after at least
50,000 proposed documents (one bounded parent can cross the threshold), and accepts
at most 100 parents per invocation. The API accepts larger explicit limits only up to
fixed maxima. Database queries and sequential no-retry object reads receive the same
absolute deadline; the existing transport's cooperative deadline limitations remain.

`archive_gets` and `archive_bytes` count attempted GETs and their catalog byte sizes,
including failed requests. They are work/cost estimates, not provider billing. A
budget-rejected parent reports expected GET/byte counts when its catalog was captured.
No dollar estimate is invented. Source bytes are never retained in a second store.

Oversized records, structural records, unsupported historical chunk layouts, bodies
above the canonical event limit, pending new documents, and a proven older revision
remain excluded. Missing/corrupt parts, arbitrary receipt/hash mismatches, and malformed
record sequences fail the whole parent. Unlocated historical chunk boundaries cannot
be distinguished from some chunk-metadata mismatches by hashes alone; neither yields
an eligible locator.

## Eventual apply boundary (not implemented)

An apply operation must rerun this proof or receive its in-process captured snapshot;
a saved JSON report is advisory and must never independently authorize a write. After
archive verification, acquire locks in the existing projector order: parent queue if
present, current parent catalog, then only changed current documents with `FOR UPDATE
NOWAIT`. Recheck the captured manifest/part identity, queue presence/generation and
current document/revision/chunk authority under those locks. If anything changed or a
document lock is busy, roll back the entire parent and retry from a fresh proof.
Absent-queue insertion races must also be tested; locking a missing row is not a fence.

Use the existing changed-only `_publish_body_locators` boundary rather than another
locator authority. No parent catalog change, reprojection, per-turn object, or source
body deletion is required. Historical receipt recovery, both ingest writer paths,
measured coverage and operational rollback remain separate body-retirement gates.

## Witnessed tests

`tests.central_brain.test_locator_backfill_plan` checks exact/shared proof, private
exclusive reports, absent apply support, unchanged positions, unsupported records,
corruption, deadline propagation, budgets and failed-read accounting.

`server/tests/e2e_locator_backfill_plan.py` creates a disposable PostgreSQL database.
It proves no locator/body/xmin change and no upload; reads every existing part once;
rejects append and publication races, corrupt/missing objects and current hash
mismatches; excludes historical revisions, tombstones, historical boundaries and
oversized records; and checks tenant scoping plus keyset pagination. Its archive spy
rejects any object read performed while a database connection is held.
