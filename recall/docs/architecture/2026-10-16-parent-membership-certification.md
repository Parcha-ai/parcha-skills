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
schema 082 generation recording and separately installed nullable
`canonical_passages.membership_generation` and `membership_binding_sha256`
columns. This change does not install those columns in production; tests supply
them as fixtures. Enabling before that schema exists yields failed proof work and
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
