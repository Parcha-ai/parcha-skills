# Future retirement policy

This change enrolls newly published parents under explicit source approval. It
preserves disabled/restored intent and freezes the pre-approval inventory in the
existing retirement ledger. It does not clear text or schedule a worker.

## State and authority

Migration 076 requires 075 and creates no enabled policy. One small row per
source stores the approving principal, epoch, capture cursor, seal timestamp and
whether detached enabled consent may be rebound. Parent membership uses the
existing indexed retirement ledger: `baseline` means excluded by inventory,
`disabled` remains an operator hold, and `detached` remains previously enabled
consent invalidated by catalog deletion. There is no second parent authority.

Capture commits at most 500 distinct parent keys and its cursor together. Indexed
successor queries cover both events without catalogs and catalogs without events.
It never overwrites existing intent. Sealing records the eligibility cutoff; an
old event committed after capture passed its key is still excluded by its event
creation timestamp. The first subsequent publication records that exclusion.
Baseline parents can be promoted only by `enroll_baseline_parent` with a fresh
exact catalog plan and expected epoch. This owner refuses disabled/restored rows.

Successful logical publication and repair call `enroll_published_parent` in the
same transaction as the catalog. With an active policy, a new post-cutoff parent
gets an enabled pending row. Detached parents rebind only when the policy
explicitly permits it, using current exact catalog identity and epoch. Existing
baseline and disabled rows remain untouched. Catalog and enrollment commit or
roll back together.

The callback uses a 150 ms savepoint and restores the caller's statement timeout.
Missing schema, disabled policy, revoked owner/grant and lock contention do not
fail evidence publication. Repair retries deferred enrollment. Policy disable
stops new enrollment/rebinding; it does not revoke separately approved finite
parents. Existing per-parent disable/restore remains the clearing kill switch.

## Controlled activation

1. Finish pinned maintenance jobs. Deploy binaries accepting optional 076 to all
   database clients, using the existing nonoverlapping projection-worker cutover.
   Apply 075, then 076, with the normal bounded migration/headroom checks. Never
   apply 076 while clients rejecting it can restart.
2. Run `retirement_policy.start_policy(store, scope=scope,
   allow_detached_rebind=False)` with a single-source `RetirementScope` and a
   current source owner. The default approves only future parents; enabling
   detached rebind is a separate explicit policy decision.
3. Call `capture_baseline(..., limit=500)` in bounded maintenance jobs until it
   reports complete. The database cursor is durable; interruption resumes from
   the last committed page. A source remains disabled throughout capture.
4. Persist the exact `policy_plan` and its hash in the operator run ledger.
   Review its source, principal, cutoff, baseline count and rebind flag. Call
   `set_policy_enabled(..., reviewed_plan=plan, enabled=True)` once. The owner
   refuses stale epochs or incomplete capture. If the transport outcome is
   unknown, read `policy_plan`; do not infer failure or repeat a mutation blindly.
5. Check one future publication and one held parent. Turn enrollment off with a
   fresh reviewed plan if needed. Baseline capture and per-parent holds remain.
   Keep optional-076-compatible binaries after migration; disabling policy is
   the rollback, not deleting its schema marker or ledger.

These are the same Python owners invoked by the real PostgreSQL tests and can
run in the existing pinned Render maintenance transport. No provider credential
or direct database access is required on the operator machine. Activation has
not been performed by this change.

## Evidence and remaining work

The real PostgreSQL suite covers resumable capture, stale approval, owner
revocation, no-policy and off states, lock refusal with publication preserved,
late commits, catalog-only parents, exact baseline promotion, detached rebind,
disabled holds, timeout restoration and transaction rollback. A 24,000-event,
12,001-parent fixture captured in 25 durable pages in 1.319 seconds. Its first
500-parent page took 9.077 ms; indexed membership took 0.039 ms. These are local
fixture measurements, not production throughput claims.

Continuous convergence still requires bounded retry and clearing rounds in the
existing projection worker. That follow-on must advance fairly across sources,
retry a deferred publication even without a later edit, respect current owner
revocation and per-parent disable, stop on the invocation byte budget, and retain
the unchanged full-parent verifier. Release acceptance requires measured cleared
bytes to exceed incoming plus restored bytes while ingest/search latency holds.
Historical revision hydration still restores SQL bodies and needs its separate
archive-backed receipt path before that write can be removed.
