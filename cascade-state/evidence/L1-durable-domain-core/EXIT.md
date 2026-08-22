---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
status: COMPLETE
candidate: 9ade9d19b2f2e8802b1927ba0e25d2fb9449b41b (claude/tether-rewrite-l1e-20260819)
pull_requests: [392, 393, 395, 396, 397]
checkpoints: [CHECKPOINT-L1A.md, CHECKPOINT-L1B-CONTROL.md, CHECKPOINT-L1C.md, CHECKPOINT-L1D.md]
exit_to: L2
---

# L1 exit — durable domain core and driver-owned completion

L1 is COMPLETE at candidate `9ade9d1`. The durable domain core (endpoints, thread bindings,
queued turns, fenced leases, native attempts, driver receipts), the machine-owned completion
protocol, the detached exact-turn driver, the kill-safe schema rehearsal with production gateway
control, and the August 14 journey are implemented and proven — all without deployment, live
schema mutation, Slack traffic, or credential/host changes. The shipped broker remains schema 17;
schema 18 activates only behind the receipt-gated cutover that L2's stock-plugin work consumes.

## Acceptance

| Criterion | Verdict | Evidence | Falsifier check |
|---|---|---|---|
| **L1.1** one endpoint / N bindings, one lease, fair ordered turns, replies to originating generation | PASS | `test_domain_runtime` property tests (`one_open_lease` runtime+index, `sibling_bindings_oldest_ready_first`, `one_attempt_never_claims_across_bindings`) plus the live two-thread trace with real detached subprocesses (`test_l1_exit_traces.TwoThreadLiveTraceTest`): fences 1→2, replies routed by stored binding/generation, distinct response blobs, invariants clean. | No cross-thread reply (routing is by stored attempt identity, thread lookup does not exist in the reply path); starvation case exercised; double lease rejected by both runtime and `endpoint_one_open_lease` index. |
| **L1.2** every admitted turn reaches one explicit terminal state without a model callback | PASS | Driver/runtime fault matrix: reply, exact `NO_REPLY`, empty output (failed), nonzero exit, cancel (confirmed and unobservable→uncertain), kill before accepted receipt (uncertain, never re-executed), never-spawned (`not_started` receipt → `failed_before_start` → requeue), generic operations never closing an attempt. | The v17 `awaiting_ack` unbounded state has no v18 analogue: `uncertain` holds the lease as a visible typed blocker; automatic replay after possible execution is structurally impossible (terminal transitions require a fenced driver receipt). |
| **L1.3** operator health exposes the same blocking set with evidence-based actions | PASS | `tether schema status` + `blocking_snapshot` projection (L1b); August 14 regression on v18 (`test_august14_journey`): blocker visible with age>0 and blocked_turn_count=8 in one call, `allowed_actions=()` ungated, `abandon` only under attested capability, endpoint freed for siblings on a fresh fence. | Doctor/status never recommend "reply to retry"; retry is absent from every allowed-action set. |
| **L1.4** schema-17 fixtures migrate losslessly; backup restore and rollback exercised | PASS | L1a migration suite (PR 392); L1c receipt-bound backup with identity/digest/integrity/manifest binding; L1e populated-store rehearsal: live synthetic admissions through the public Store API, 17→18→17 with preserved-manifest digests equal and the intentional endpoint-inventory delta asserted; SIGKILL recovery at every durable phase incl. a real `SIGKILL` subprocess. | No manual DB surgery anywhere; downgrade path validated by the exact pinned predecessor artifact; endpoint identity lives once (endpoints table), never copied into sibling bindings. |
| **L1.5** current contracts stay green alongside the L0 failure corpus at the candidate digest | PASS | Full gate at `9ade9d1` on Node 22.23.2: `npm test` exit 0 — 637/637 unit tests including `test_l0_incident_corpus`, routing/authorization/outbox/file/edit-delete-cancel/process-identity/installer/release suites, plus release-install (installer SIGKILL crash recovery) and tarball lifecycle scripts. | No test was removed; 34 tests were added across the L1c–L1e slices; the one modified expectation (managed file counts 22→26/29→33) tracks the real managed surface. |
| **L1.6** POST-ZEN: fewer authoritative state concepts; legacy coupling behind one deletion gate owned by L2 | PASS | Baseline (L0 `baseline-metrics.json`): 13 authoritative v17 tables, 27 distinct state literals, unbounded `awaiting_ack`, endpoint identity duplicated per-bridge in `bridges.source_json`, four platform outbox/reconciliation tables. v18 domain: one endpoint truth with an immutable identity trigger, one queue (`queued_turns`), one receipt ledger (`driver_receipts`), one lease allocator, attempt states CHECK-bound with terminal↔timestamp equivalence, no unbounded state. One schema receipt, one lifecycle lock, one database singleton, one orchestrator (the L1c coordinator — now the live-cutover mechanism, not scaffolding to delete). | **Named deletion gate, owner L2:** the entire schema-17 transport/state layer — `bridge_runtime.py`'s v17 `Store`, the four Slack outbox/reconciliation tables, `hermes_compat.py` private coupling, and the Node CLI's direct broker paths — is the compatibility adapter. It is deleted only after L2's stock-Hermes public contracts pass crash/reconciliation tests and packaged rollback exists (chain L2.6). No second scheduler, ledger, CLI, or source of truth was added in L1. |

## Bounds

One valid PROVE attempt consumed (the L1e full gate at `9ade9d1`); review/fix rounds used: 0 of 3.
Checkpoint slices L1a–L1e each passed their own full gate before stacking.

## Witnessed defects found and fixed during L1 (not carried forward)

- Whole-manifest equality in the L1c rehearsal was wrong for populated stores (rollback retains
  archived endpoint inventory by design) — found by the populated-store red test, fixed by
  judging preservation over `PRESERVED_MANIFEST_KEYS` via `preserved_manifest_digest`.
- A time-bombed pinned clock in `test_domain_control` and a duplicated installer `add_file`
  (caught by the release integrity check) — both fixed in-slice.

## Hand-off to L2

- External gate: upstream `NousResearch/hermes-agent` public extension contracts. Upstream merge or
  release is external state — L2 may end `BLOCKED_EXTERNAL`.
- The deletion gate above is L2's exit obligation, not L1's.
- Amendment A1 stands for L3 planning: strict Slack admission, full machine capability for
  admitted agents, capability parity between Tether-spawned and user-opened sessions proven by a
  probe-script diff, bridge-credential isolation as the only machine boundary.
- Review state: PRs 392←393←395←396←397 stacked, all open; no bot reviewers fire on
  parcha-skills, human review pending. L2 work can stack further; merges land bottom-up whenever
  review happens.
