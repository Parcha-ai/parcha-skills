---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
slice: L1d-schema18-runtime-and-detached-driver
status: CONSUMED (built at a6c8e19, PR 396)
target_head: c248e08c2c2337a67596f5f718b36bafd50958da
worktree: /home/ubuntu/worktrees/tether-rewrite-l1d-20260819
branch: claude/tether-rewrite-l1d-20260819
---

# L1d plan — schema-18 domain runtime and driver-owned completion

## Goal

Add the sole schema-18 `DomainRuntime` (admission, fair scheduling, fenced leases, machine-owned
attempt lifecycle) and the `detached_native` exact-turn driver, prove the August 14 blocker journey
end to end on the new domain, and rehearse packaged cutover through the L1c coordinator. The
shipped broker stays on schema 17; the schema-18 runtime is exercised only against v18 test stores
and rehearsal copies. No live schema mutation, deployment, or Slack traffic.

## Grounding (verified at c248e08)

The v18 schema already encodes the invariants the runtime must drive, not re-check in Python:
- `endpoint_one_open_lease` partial unique index — at most one unreleased lease per endpoint.
- `endpoint_leases.fence` unique per endpoint; `endpoints.next_lease_fence` is the allocator.
- `native_attempts.state` machine with terminal↔`terminal_at` CHECK and response-shape CHECKs;
  `driver_request_id`/`reply_token_hash` unique for idempotent submits and reply routing.
- `driver_receipts(attempt_id,lease_fence,sequence)` unique — a stale-fence driver cannot advance
  an attempt; `operation IN ('submit','cancel')` with allowed-state CHECKs.
- `queued_turns` idempotent by `event_key`, ready-ordered by `(binding_id,ordered_at,event_key)`;
  `native_attempt_turns` binds exact turns to an attempt and generation.
- `domain_control.blocking_snapshot` already projects native uncertainty and rebind blockers;
  operator resolution exists and is capability-gated off.

## Sub-slices (each is a commit with its own red tests; one PR)

### L1d-a — DomainRuntime scheduler core (offline)

One module `runtime/domain_runtime.py`. Every public operation is one `BEGIN IMMEDIATE`
transaction conditioned on persisted endpoint incarnation, lease fence, binding generation, and an
idempotent request/event identity. Operations: `admit_turn`, `schedule_next` (creates
`prepared` attempt + lease at `++next_lease_fence`, claiming the oldest ready turns),
`record_driver_receipt` (monotonic per-fence sequence CAS), `mark_submitting`, terminalization
(atomically releases the lease and completes claimed turns), `request_cancel` (idempotent), and
recovery classification only — never re-execution.

Fairness: among an endpoint's non-closed bindings with ready turns, pick the binding whose oldest
ready turn is globally oldest (`ordered_at,event_key`); one attempt may claim several consecutive
ready turns of that binding only. Replies route by the attempt's stored `binding_id` and
`binding_generation`, never by thread lookup.

Completion policy (driver-owned, never model-owned): `completed_with_response` requires a receipt
with durable response ref+sha; exact stripped `NO_REPLY` receipt → `no_reply`; empty output →
`failed`; lost proof after possible execution → `uncertain` terminal-visible blocker, no replay;
generic egress never closes an attempt. Herdr and Zellij `driver_kind` attempts are admitted for
bookkeeping but are ineligible for automatic scheduling/draining (fail-closed until their upstreams
prove exact-turn receipts).

### L1d-b — detached_native exact-turn driver

`runtime/native_driver.py`: spawns the harness process detached (setsid, closed stdin, private
cwd), captures `(pid, starttime)` process identity, and writes the durable `accepted`
driver receipt from the first successful spawn observation before any wait. A crash between spawn
and the accepted receipt classifies the attempt `uncertain` on recovery — possible execution is
never retried. Output contract: response file (owner-private, content-addressed blob) — absent or
empty at clean exit → `failed`; exactly `NO_REPLY` after strip → `no_reply`; otherwise
`completed_with_response` with sha256/bytes. Cancellation kills the process group and writes a
`cancel` receipt; an unobservable kill outcome is `uncertain`, not `cancelled`.

### L1d-c — August 14 journey and packaged cutover rehearsal

End-to-end regression on a v18 store: admit root+7 sibling turns, schedule, driver accepted, then
lose the completion proof — assert the accepted-without-terminal state surfaces as the existing
`native_execution_uncertain` blocker with age/turn count, doctor/status agree, no retry is offered,
and capability-gated operator resolution terminalizes exactly the claimed turns and releases the
lease. Then extend the L1c rehearsal (same coordinator, no second orchestrator): after the
disposable 17→18 transform, boot `DomainRuntime` read-write against the disposable copy, run one
synthetic admit/schedule/receipt/terminal cycle, verify `invariant_violations` is empty and the
logical v18 manifest matches the target attestation, then 18→17 rollback as today. The rehearsal
remains internal and evidence-only.

## Acceptance and falsifiers

| ID | Required observation | Falsifier |
|---|---|---|
| L1d.1 | One endpoint, N bindings: at most one open lease (index-enforced and runtime-respected), fair oldest-ready scheduling, replies land on the originating binding generation. | Cross-thread reply, starvation of a sibling binding, two open leases, or reply routed by thread lookup instead of stored attempt identity. |
| L1d.2 | Every admitted turn reaches one explicit terminal state without a model callback: reply, `NO_REPLY`, crash, cancel, lost proof, generic-post misuse, ambiguous acceptance all covered by fault injection. | Unbounded `awaiting_ack` analogue, silent ready-queue growth, or automatic replay after possible execution. |
| L1d.3 | Stale fences and duplicate requests are dead: a receipt with an old `lease_fence`, a replayed `driver_request_id`, or an out-of-order `sequence` cannot mutate attempt state. | Any stale-fence write path that advances state or double-claims a turn. |
| L1d.4 | The detached driver emits durable `accepted` on first spawn observation; kill between spawn and receipt classifies `uncertain` and is never re-executed; only exact stripped `NO_REPLY` is silence; empty output fails. | Re-spawn after possible execution, `no_reply` from empty output, or acceptance inferred without process identity. |
| L1d.5 | The August 14 journey passes on v18: uncertainty is visible with age and blocked-turn count within one status call, retry is never advertised, operator resolution terminalizes exact members and frees the lease. | A healthy component metric while the end-to-end turn lacks a terminal receipt, or copy recommending "reply to retry". |
| L1d.6 | The L1c rehearsal, unchanged in lock order, additionally boots the exact target runtime against the disposable v18 copy and runs one full synthetic cycle; live DB untouched; still no public mutation command. | A second orchestrator/journal, a live write, or rehearsal treated as migration authority. |
| L1d.7 | Full release matrix green; POST-ZEN: zero new authoritative state concepts (runtime drives the existing schema), Herdr/Zellij remain fail-closed, and the schema-17 broker behavior is unchanged. | A parallel queue/outbox/state machine in Python, a weakened v17 contract, or removed tests without replacements. |

## Required first red tests

- two open leases rejected by both index and runtime path; fence allocator never reuses;
- sibling fairness under continuous load on one binding (no starvation);
- receipt with stale fence / replayed request id / non-monotonic sequence refused;
- terminalization and lease release are one transaction;
- driver kill before accepted-receipt fsync → `uncertain`, recovery refuses re-spawn;
- `NO_REPLY ` with trailing whitespace is silence, `no reply`/empty/`NO_REPLY x` are not;
- generic egress attempt against an open attempt does not terminalize it;
- Herdr/Zellij attempts are never auto-scheduled;
- rehearsal synthetic cycle leaves `invariant_violations` empty and manifests matched.

## ZEN / POST-ZEN gate

The database schema is the state machine; `DomainRuntime` is the only writer path for v18 and adds
no shadow state. One driver receipt ledger, one lease allocator, one scheduler. The L1c coordinator
remains the only schema orchestrator. Anything that duplicates a CHECK constraint as a divergent
Python rule, or adds a second completion path, fails the gate. The shipped broker stays schema 17;
activating v18 for live traffic is explicitly out of scope for L1d and belongs to the packaged
cutover slice that closes L1.
