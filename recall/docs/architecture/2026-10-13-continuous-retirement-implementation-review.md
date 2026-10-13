# Continuous retirement implementation review

Reviewed against `139b562` on October 13. This is an implementation proposal, not an enabled policy or a claim that SQL growth has stopped. The existing finite enrollment and clear operators remain the production path.

The smallest durable solution has two owners: enroll approved future parents when their logical catalog commits, and run the existing clear owner separately in bounded source-rotated quanta. That handles current bodies. Historical revisions need their own exact archive read path before we can stop restoring them into SQL.

## Findings that change the implementation order

1. **Current retirement does not eliminate historical growth.** `canonical_history.prepare_history` reads cleared current bodies from the logical archive; `restore_outgoing` writes them back to `canonical_chunks` before a new revision replaces them. `db.resolve` explicitly requires historical SQL text to match its hash. Parent revision publication can delete the older logical parts. Removing restoration first would break previously issued receipts. The current `e2e_revision_history.py` intentionally proves this SQL restoration and deletion of old parts.
2. **Omitting duplicate document text alone would strand event JSON.** Both `CanonicalBodyThinner` cursor enumeration and `_thinning_statement` require `document.body_location='inline'`. New rows written as chunk-backed would never reach event compaction. The logical projector's committed-body hints also collect only inline documents. Fix these dependencies before changing admission.
3. **There is already a suitable historical archive representation.** Schema035 `canonical_evidence_objects` is keyed by the revision-specific `document_id`. `EvidenceBundle` validates whole-document text, ordered chunks and receipts. Reuse it for outgoing revisions instead of inventing another object format or retaining entire old parent manifests for a single edited event. Existing lookup helpers are current-only, so a new exact historical read owner is still required.
4. **Forget needs explicit wiring for that reuse.** `CanonicalPlane.forget` calls its configured logical projector, then deletes canonical documents/events. It does not currently delete newly introduced historical per-document objects. Their existing foreign key would also prevent document deletion. Add the per-document evidence deletion owner to that existing lifecycle before enabling archived history.
5. **Absent progress is not evidence of prior consent.** Schema069 progress cascades on catalog deletion. Retain that same ledger across catalog removal; a second holds table would duplicate authority. The existing metadata-only owner uses locks and returns early for represented rows; it is not an automatic enrollment policy or a clear proof.

## Hill A: retain intent, then authorize continuous work

The implemented prerequisite uses the existing schema 069 ledger. Migration 075 makes its catalog identity nullable and changes the catalog FK to `ON DELETE SET NULL (logical_document_id)`. One trigger disables clearing, advances the scope epoch, and records `detached` only when the removed catalog was enabled. Already disabled/restored rows remain `disabled`. A direct source FK removes metadata when its owning source is deleted.

The trigger is necessary: the existing direct clear owner checks `enabled` and epoch rather than joining on the ledger's logical document id. Merely retaining an enabled row with a null id would leave stale authority usable through that direct path. The trigger closes this path without editing `chunk_retirement.py` or `parent_chunk_proof.py`.

`retirement_runner.rebind_detached_parent` authorizes one replacement catalog. It requires the current source owner and grant, an exact freshly reviewed catalog identity including `created_at`, and the expected ledger epoch. It locks catalog before progress, refuses disabled/restored markers, resets the manifest/cursor, advances epoch, and restarts the existing grace period. Ordinary enrollment still inserts absent rows only; ordinary runs never rebind. Legacy enable on a detached null-id row fails the schema constraint; callers must use the explicit reviewed rebind owner.

The existing forget owner now deletes detached metadata only for the exact parents whose last events it removed, and only when no catalog or surviving event remains. Partial forget preserves the marker. Permanent identity tombstones continue to deny replay. This is scoped metadata cleanup inside the final forget transaction, not another background service. Source deletion also removes retained metadata through its direct FK.

Actual PostgreSQL tests cover enabled and disabled catalog replacement, stale catalog/epoch refusal, concurrent disable, rollback, deletion during full archive proof with zero clears, actual restore after clearing, owner revocation, legacy enable refusal, partial/full forget and denied replay, source cleanup, migration repeat and grace. Schema 075 is an explicitly accepted optional migration; mandatory schema69 remains supported. The rebind owner itself requires 075.

The next policy change must name an approving principal and an explicit eligible future-parent scope. It can live beside the existing runner and be called in the successful new/revised and repaired catalog commit paths, after locator publication. It must preserve disabled rows, use the explicit rebind owner for detached formerly-enabled rows, and leave stale/adopted publications alone. Publication must not wait on archive verification.

An absent row cannot prove there was no historical restore: catalog cascades before 075 erased that information. Freeze a reviewed baseline of existing parent identities and keep it under the finite process. A catalog timestamp cutoff is insufficient because recreation changes it. Events are also deleted by forget, so their timestamps are not a complete history registry. Explicit full forget removes scope metadata; a future policy must not treat a reused parent identity as newly approved solely because that row is absent.

Continuous execution stays outside ingestion. Reuse the current supervisor, durable intents/results and `run_retirement`; rotate approved sources, treat cooldown/no-ready as idle, and stop after unknown commits or launches. Do not add a queue. Activation starts with one scoped source and a small admitted set, with current provider health/headroom/lag checked before each mutation. Keep the finite transport ledger for restart and unknown-outcome reconciliation. Rollback stops new claims and disables the policy; it must not delete held rows or reverse the migration by restoring cascading deletion.

Before policy implementation, prove fresh approved enrollment, revoked/off policy refusal, exact catalog rebind, disabled/restore preservation, publication rollback, absent-row baseline exclusions and fair source rotation under real concurrent transactions. Retain the runner, full-parent proof, native-lock, history and finite campaign tests. No automatic future enrollment is enabled by this prerequisite.

### Deployment and migration order

Finish the current finite enrollment/clear invocation before changing its serving commit or owner hashes. Its pinned remote body must not span a deployment. Record the terminal result or reconcile an unknown outcome first.

Deploy code that explicitly accepts optional075 to MCP, the ingestion worker and the managed-connector worker before applying the migration. Verify each service's actual deployed commit and readiness against the still-existing schema, rather than assuming a shared deploy changed all services. The new forget cleanup is compatible with schema069: its null-catalog predicate matches no row before075. The rebind API fails closed while075 is absent. New deployments must not automatically run075 before older running services have been upgraded.

Refresh the supervisor's reviewed code pins only after those releases are healthy: `retirement_runner.py` changes for the rebind API; `chunk_retirement.py` and `parent_chunk_proof.py` retain their existing hashes. Rebuild body hashes and durable intent with the actual selected commit. Do not reuse an old launch intent with substituted code.

Then apply075 as a separate, reviewed migration with a short lock timeout and bounded statement timeout after a fresh database headroom/replica-lag check. Its progress-table FK/constraint changes take locks; a timeout rolls back the migration and is not permission to increase deadlines. Confirm marker075, nullable catalog FK, detachment trigger and source FK, then run read-only readiness and ledger checks before another bounded clear invocation. No automatic future enrollment is enabled here.

After075, rollback must retain optional075 compatibility on every service. Redeploying an older binary that rejects migration075 can make readiness fail. Stop claims/rebinds if necessary; do not reverse the migration, erase held intent, or restore cascading catalog deletion as a rollback shortcut.

### Measured drain rate

The first production quantum cleared 10 parents, 4,045 documents, 4,199 chunks and 14,277,687 UTF-8 bytes in 44.104 seconds, with zero failures or unknown outcomes. That is 0.309 MiB/s of logical body clearing and 91.7 documents/s. Twenty-two commits averaged 184 documents; the existing batch ceiling is 256. Whole-parent proof is performed once and its verified spool feeds the bounded commits; it is not repeated for each SQL batch.

The runner read 15,985,534 archive bytes in 11 GETs, 1.12 archive bytes per cleared byte. Including receipt samples, the job read 26,500,908 bytes in 17 GETs. This is a small, old-parent sample, not a sustainable-rate claim. The next authorized experiment raises only the parent cap to 100, retaining the 180-second, 64-MiB-clear and 256-MiB-read limits. Measure job startup, cooldown, large active parents, incoming SQL bytes and historical restorations in the same window before choosing cadence. Do not increase verifier limits from this single sample.

## Hill B: preserve historical receipts without restoring SQL bodies

Use existing schema035 and `EvidenceProjectionStore` for an outgoing revision bundle. Do not enable the old broad per-document projector for the whole current corpus.

1. In `prepare_history`, after the current reader has verified the outgoing bytes and chunk receipts, build and durably write the existing `EvidenceBundle` for that exact document revision outside any pooled connection. Stage its immutable reference alongside the existing bounded proof metadata.
2. Under the ingest writer's existing native/document/chunk locks, recheck revision, document hash and ordered receipt/chunk hashes. Bind the immutable reference in `canonical_evidence_objects` in the same transaction that supersedes the outgoing document. A conflicting existing reference must match, not merely be ignored.
3. Add an exact historical hydration owner for `db.resolve`. Select the authorized nondeleted revision, verify the complete bundle and requested chunk hashes/receipts, then recheck SQL authority and tombstone/forget state after I/O. Do not remove `is_current` from the current logical reader or route historical requests through the newest parent manifest.
4. Wire per-document archive deletion into the existing forget lifecycle before SQL document deletion. Tombstones deny reads even when archive deletion is retrying. An unknown ingest commit must not cause cleanup to delete a potentially committed bundle.
5. Only after those reads and deletion semantics pass, replace `restore_outgoing`'s SQL body rewrite with reference binding for proven outgoing revisions. Keep existing SQL historical fallback for rows not yet archived. Backfilling already-retained history is a separate bounded pass.

This intentionally moves one exact historical body to cheap object storage when necessary. It avoids pinning an entire old parent revision for a single changed record. It is not a new copy of every current session.

Tests before implementation:

- Both single and batch ingest: project/clear revision1, ingest revision2, delete old logical parts, resolve revision1 exactly while its SQL chunk bodies remain empty. Current code fails the empty-SQL assertion.
- Repeat through mixed duplicate batches and legacy bridge. Replay performs no unnecessary archive read/write and preserves the same receipts.
- Hash/receipt/order mismatch, missing/corrupt bundle, wrong tenant/source and ownership revocation never return an unverified body or acknowledge an unsafe revision.
- Competing revision, clear-after-preflight, tombstone and forget races preserve the existing retry behavior. No pooled database connection is held through archive reads/writes.
- Lost publication/commit replies and restart preserve referenced objects; unreferenced uploads cannot be mistaken for a committed receipt.
- Forget removes the historical evidence row/object through the existing retryable lifecycle, then removes canonical documents. The foreign key must not wedge forget.

The unchanged current clear verifier remains outside this PR. Historical backfill/clearing authority is a later explicit owner, not a widened current-parent clear.

## Hill C: remove the redundant document copy at admission

After Hill A's maintenance path is stable, make event compaction independently selectable from document compaction. Update bounded key enumeration, final guarded mutation, index support and logical publication hints together. Keep source/queue/archive requirements intact; do not introduce an unbounded OR scan.

Then change both `ingest_document` and the batch document insertion to store `text_redacted=''` with `body_location='chunks'`, preserving original document hash, full verified chunks, receipts and transaction atomicity. This should save one SQL text copy immediately. Do not omit initial chunk bodies: the first logical projection still obtains new text from them.

Tests before implementation: immediate show/receipt before projection; single/batch/empty/oversized input; successful first logical and passage projection; retry/history/tombstone/forget; and an actual PostgreSQL regression proving chunk-backed documents still have their inline event JSON compacted. Explain/measure the bounded compaction plan on many already-compacted rows before activation.

## Acceptance and remaining limits

Measure one shared time window per source: accepted SQL body bytes, bytes restored to SQL, committed cleared bytes, and forgotten bytes. The clear rate must exceed new plus restored writes with a shrinking eligible backlog while receipt correctness, ingest lag and search latency hold. Enrollment counts and configured batch limits are not throughput evidence.

Physical database allocation and billed capacity are separate measurements. Logical clearing alone does not authorize downsizing. Active parents, unsupported events, unarchived historical rows and deliberately held parents remain explicit residuals. Jev can reduce later indexing/embedding work after durable admission; it must not decide archive integrity, ownership, forget, or clear eligibility.
