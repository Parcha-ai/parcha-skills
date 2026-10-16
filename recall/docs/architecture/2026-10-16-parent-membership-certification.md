# Parent membership certification

This owner records exact passage membership proofs after canonical publication.
Search still runs the unchanged native receipt verifier. Certificates do not yet
reduce query work, authorize a source, or establish global ingestion coverage.

## Write boundary

`CanonicalPassageProjector.project_pending` invokes certification only after a
successful `_commit` has released its transaction, advisory lock and commit
semaphore. It keeps passage IDs and releases prepared bodies before proof I/O.
The existing source owner is read from the current catalog; enrollment separately
checks that principal's current brain and source grants.

Enrollment captures the publication binding, parent generation, unchanged native
receipt membership, and actual document/event parent ownership in one repeatable
read snapshot. Its final short transaction takes the existing passage publisher
lock, rechecks grants and the exact binding, and stores the captured generation.
It never locks the parent authority row or replaces the native verifier.

An edit, Forget, source recreation or publication change invalidates the proof.
Body retirement and locator changes alone leave membership unchanged. A missing
version or unproved ownership leaves the passage on native verification. A lost
version is initialized only when no older certificate exists for that publication.

## Optional operation and recovery

The writer is disabled by default. `RECALL_PARENT_MEMBERSHIP_CERTIFICATION=1`
requires a nonempty `RECALL_PARENT_MEMBERSHIP_EPOCH`. Deployment also requires
schema 082 generation recording and schema 083 nullable
`canonical_passages.membership_generation` and `membership_binding_sha256`
columns. Apply schema 083 only after every database client accepts optional 83.
No image deployment installs these columns in production. Enabling before that schema exists yields failed proof work and
native fallback, while publication remains committed.

The hook has a three-second budget, including pool checkout and statements. Each
batch is at most 128 passages, 20,000 receipt references and 8 MiB of metadata.
These bound optional jobs, not captured parents. Unprocessed passages can be
explicitly enrolled later through `enroll_parent_passages` with a caller-owned
scope, deadline and resource limits. An oversized scope is refused as a whole;
there is no deletion, truncated proof, new queue or automatic ingest replay.

Counters distinguish committed certificate writes, failed documents and deferred
documents. If a later batch fails, earlier acknowledged certificate writes remain
counted. These are write outcomes, not a claim that those certificates are still
current after a concurrent mutation. Failure after publication never puts the
already committed passage queue item back on the queue.

If invalidation recording is interrupted after certificates exist, readers must
remain on native verification until old certificates are cleared or the binding
epoch is rotated. Never reuse an earlier epoch. Partial re-enrollment does not
repair old certificates in untouched parents. The current change has no reader
trust switch, so this reset contract cannot accidentally enable a shortcut.

`compare_parent_membership` always executes the original native proof and reports
content-free certificate eligibility, fallback and disagreement counts. A future
read path requires separate review, live grant fences, and measured comparison
before any certificate is trusted.


## Reader shadow comparison

`RECALL_PARENT_MEMBERSHIP_SHADOW=1` enables comparison on the singleton authority
path. It requires the same explicit `RECALL_PARENT_MEMBERSHIP_EPOCH` as enrollment.
It is off by default. The legacy path reports `requires-singleton` without querying
certificate metadata.

The original selected-passage SQL and native receipt membership verifier execute
in full. In that same repeatable-read snapshot, one extra query loads only binding
and generation metadata for the selected keys. Receipts already fetched by the
native query are reused. Certificates never change accepted rows, including when
a deliberately false certificate disagrees with the native decision.

The optional query gets at most `RECALL_PARENT_MEMBERSHIP_SHADOW_BUDGET_MS`
(default 100ms; maximum 1000ms) and must leave
`RECALL_PARENT_MEMBERSHIP_SHADOW_RESERVE_MS` (default 1000ms) before the existing
request deadline. If the reserve is reached, comparison is skipped. A savepoint
contains SQL failure or timeout so native authorization remains usable. Metadata
larger than 8MiB is refused as one optional observation; captured history is intact.

`authority_stages.parent_membership_shadow` reports status, elapsed milliseconds,
selected parents/passages, eligible certificates, fallback, disagreements and
potentially avoidable unique receipt probes. Unavailable or skipped observations
do not report zero disagreement. No source identifiers or bodies are emitted.
Native membership timing remains the completed original proof timing; total
authority latency includes this added observation. These measurements establish
agreement and potential work reduction, not a deployed speedup.

## Certification after retirement

Singleton metadata retirement changes the parent generation after removing physical
chunk rows. A certificate written by the passage publisher can therefore become
stale without another publication being queued. The retirement runner also invokes
the same native enroller after a completed parent's transaction and archive context
have closed. This includes a no-op final tail whose earlier windows did the physical
work. Partial and unknown retirement outcomes retain native fallback.

The existing certification flag and epoch control both invocation sites. Retirement
certification receives at most three seconds within the original owner deadline;
it cannot consume the maintenance intent-finalization reserve. Passage IDs stream
in bounded keyset pages. Missing grants, changed publication, concurrent Forget or
an exhausted metadata budget preserve the original authority checks. Optional
failure never changes the retirement acknowledgment or retries its SQL. The child
log exposes acknowledged certificate writes, failed/deferred parents and elapsed
milliseconds; these counters do not establish current certificate coverage.

There is one certification owner and two commit boundaries, without a refresh
queue. Keep the publisher invocation for parents that have not retired or cannot
retire; removing it would discard their coverage. Reconsider that invocation only
if measured lifecycle coverage makes it redundant. Neither invocation authorizes
removing the native receipt/revision catalogs used by the fallback verifier.
