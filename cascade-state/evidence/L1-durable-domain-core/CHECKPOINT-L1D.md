---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
checkpoint: L1d-schema18-runtime-and-detached-driver
status: COMPLETE_CHECKPOINT
target_head: a6c8e19 (claude/tether-rewrite-l1d-20260819)
pull_request: https://github.com/Parcha-ai/parcha-skills/pull/396
next: L1-exit (packaged cutover wiring, exit receipt, POST-ZEN)
---

# L1d checkpoint — schema-18 domain runtime and driver-owned completion

Not an L1 exit receipt. The shipped broker remains schema 17; `DomainRuntime` and the detached
driver are exercised against v18 test stores and rehearsal copies only. No service, live database,
Slack event, credential policy, deployment, or rollback changed.

## Result

- `runtime/domain_runtime.py` (1017 lines): the sole schema-18 writer path. Single-transaction
  admission/scheduling/receipts; fenced single lease; fair oldest-ready sibling scheduling; atomic
  terminalization + turn outcomes + lease release; Herdr/Zellij fail-closed for auto-scheduling.
- `runtime/native_driver.py` (414 lines): durable spawn intent → detached spawn → pid+starttime
  identity → durable `accepted` receipt → watch/reap → exact terminal receipt. Crash before
  acceptance recovers `uncertain`, never re-executes; post-acceptance recovery is pure observation.
- August 14 journey regression end to end on v18, including capability-gated operator `abandon`
  freeing the endpoint and siblings draining on a fresh fence.
- Rehearsal coordinator extended (same lock order, still the only orchestrator): synthetic
  admit/schedule/receipt/terminal cycle on a disposable copy of the migrated store, evidence key
  `synthetic_cycle: ok` in the `runtime_verified` phase.

## Acceptance evidence (PLAN-L1D table)

| ID | Evidence |
|---|---|
| L1d.1 | `test_full_turn_lifecycle…`, `test_sibling_bindings_are_scheduled_oldest_ready_first` (starvation case), `test_one_open_lease_per_endpoint_runtime_and_index`, `test_one_attempt_never_claims_across_bindings`; replies route by stored attempt binding/generation (`attempt_status`). |
| L1d.2 | Terminal coverage: reply, exact `NO_REPLY`, empty output (failed), nonzero exit, cancel, kill-before-accept (uncertain), never-spawned (`not_started` receipt → requeue), generic ops never closing an attempt. |
| L1d.3 | `test_stale_fence_and_replay_and_sequence_gap_are_dead`: stale fence, replayed receipt (idempotent), sequence gap, forged request id all refused. |
| L1d.4 | `test_kill_before_accepted_receipt_recovers_uncertain_never_respawns` (both windows: after durable intent, after spawn), relaunch refused; `test_exact_no_reply_token_is_the_only_silence`. |
| L1d.5 | `test_august14_journey`: blocker visible with age>0 and blocked_turn_count=8, `allowed_actions=()` ungated, operator abandon under capabilities, fresh fence reschedule, invariants clean. |
| L1d.6 | Rehearsal full-run test asserts `synthetic_cycle: ok`; rollback path runs on the pristine transform copy; live DB digest asserted unchanged in the L1c suite. |
| L1d.7 | Full gate exit 0 on Node 22.23.2: 630/630 unit tests + release install (SIGKILL recovery) + tarball lifecycle. Managed surface +2 files consistent everywhere (26 codex / 33 both). |

## Notes

- The v18 schema triggers (`native_attempt_forward_state`, timestamp guard, terminal-proof guard)
  rejected three earlier design drafts during the red phase — direct prepared→accepted, response
  evidence outside the terminal statement, and a receipt-free `fail_before_start`. The shipped
  design is what the constraints admit: every terminal transition is proven by the current fenced
  driver receipt; there is no receipt-free recovery path.
- A duplicated installer `add_file` (double-applied edit) was caught by the release-install
  integrity check (`installer manifest contains an invalid record`) and removed — the managed-
  manifest verification catches real packaging drift.

## Bounds

First valid PROVE attempt for the L1d slice; zero review/fix rounds at checkpoint time. Stack:
392 ← 393 ← 395 ← 396, all open awaiting human review (no bot reviewers fire on parcha-skills).
