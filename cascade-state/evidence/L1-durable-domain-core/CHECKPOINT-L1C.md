---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
checkpoint: L1c-rehearsal-coordinator
status: COMPLETE_CHECKPOINT
target_head: c248e08c2c2337a67596f5f718b36bafd50958da
pull_request: https://github.com/Parcha-ai/parcha-skills/pull/395
next: L1d-schema18-runtime-and-detached-driver
---

# L1c checkpoint — kill-safe internal schema rehearsal

This is not an L1 exit receipt. It proves the complete process, artifact, backup, and recovery
boundary on disposable database copies. The active runtime remains schema 17; no public migrate
command exists; `migration_ready` stays false. No service, live database, Slack event, credential
policy, machine policy, deployment, or rollback changed.

## Result

- Extracted one authoritative schema-receipt module (`runtime/schema_receipt.py`): immutable
  identity fields (database device/inode, instance UID, security-domain hash, pinned artifact build
  digests, installed-manifest digest), monotonic compare-and-swap phase transitions, atomic rename
  plus file and parent-directory fsync on every write, and a redacted public view. Phase semantics
  exist only in this module; installer Bash checks flag-file existence only and Node is untouched.
- Gated broker/plugin startup in `open_locked_store`: any invalid, corrupt, or incomplete receipt
  blocks before SQLite is opened. Bootable states are exactly: absent receipt, `complete` for a
  supported schema, and `resumed` (live DB verified untouched, predecessor validated, coordinator
  restarts the gateway before `complete`). `failed_safe`/`needs_operator` retain maintenance mode.
- Added a side-effect-free `validate-store` attestation entry (read-only SQLite, exact-artifact
  build digest over the artifact's Python sources, logical-manifest digest) that never instantiates
  the migrating `Store`. Reachable only by direct invocation; the Node CLI rejects everything but
  `schema status` (`test_schema_command_rejects_missing_or_unknown_orchestrator`).
- Implemented the internal rehearsal coordinator (`runtime/schema_rehearsal.py`) with the fixed
  lock order from PLAN-L1C: lifecycle flock → receipt `planned` → maintenance gate → stop plus
  independent quiesce attestation → `bridges.db.lock` singleton (reused via
  `bridge_runtime.acquire_database_singleton`, no second lock implementation) → receipt-bound
  verified backup → singleton release → disposable 17→18→17 transforms validated by pinned target
  and predecessor artifacts via subprocess `validate-store` in a sanitized environment →
  `resumed` → attested restart → `complete` → maintenance disarmed.
- Installer refuses install/upgrade/rollback/uninstall with exit 75 while the maintenance flag
  exists, under the existing lifecycle lock.

## Acceptance evidence (PLAN-L1C table)

| ID | Observation | Evidence |
|---|---|---|
| L1c.1 | Receipt identity and phases survive restart; incomplete/corrupt/schema-conflicting receipts block both runtime paths before DB open. | `test_schema_receipt` gate tests (all phases, corrupt, unsafe mode, `schema_receipt_runtime_conflict` for a completed to_schema newer than the booting runtime); `test_startup_gate_blocks_incomplete_and_allows_resumed` exercises `open_locked_store` directly. Mutation check: disabling the gate flips the test red. |
| L1c.2 | Lifecycle lock precedes maintenance/quiesce and the DB singleton; a contender fails before any receipt or backup. | `test_lifecycle_lock_contention_fails_before_any_receipt` (no receipt created); `test_held_database_singleton_fails_before_backup` (failed_safe at quiesced, no backup directory). |
| L1c.3 | Backup binds DB identity, byte digest, integrity, schema, and logical manifest before any transform; tampering is refused. | `test_tampered_backup_is_refused` (truncation), `test_hardlinked_backup_is_refused` (nlink), identity re-verification after hashing; `owned_file_identity` enforces owner, nlink 1, and exact mode. Mutation check: removing identity re-verification flips the hardlink test red. |
| L1c.4 | Disposable 17→18→17 preserves the declared manifest and boots exact packaged validators; artifact drift after preflight is refused. | Full-run test asserts post-rollback manifest equals the backup manifest via the predecessor artifact's attestation; `test_artifact_drift_after_preflight_is_refused` mutates the pinned target between backup and transform. Predecessor and target artifacts carry distinct build digests in fixtures, so the source tree cannot substitute for either. |
| L1c.5 | Kill after every durable phase recovers to exactly one classified state with no admission and no guessed backup or artifact. | `test_kill_after_every_durable_phase_recovers_to_one_state` (8 boundaries via BaseException injection after each fsynced transition, including `backup_written` between backup fsync and receipt fsync) plus `test_sigkill_subprocess_between_backup_and_receipt_fsync` (real SIGKILL, returncode −9 asserted). Each crash: one `incomplete_*` classification, admission blocked (except `resumed`), live database digest unchanged, and a fresh coordinator refuses with `operation_already_exists`. |
| L1c.6 | Status stays redacted and `migration_ready=false`; no public migrate/rollback command exists. | `test_status_stays_redacted_during_a_rehearsal` (no paths, no phase evidence, no backup digest in the public view); `migration_capabilities` unchanged and all false beyond L1b; Node CLI dispatch rejects non-`status` subcommands. |

## Full-gate evidence

- `npm test` exit 0 on Node v22.23.2: 603/603 unit tests (13 new rehearsal tests, 9 new receipt
  tests), `py_compile` over all runtime modules including the two new files, `node --check`,
  `bash -n`, release-install lifecycle (including installer SIGKILL crash recovery), and release
  tarball lifecycle.
- Key SHA-256 at `c248e08`: `schema_receipt.py`
  `23ba42213c848ca93019b2bafcf730b3db7bb0f1a432911cade2a54ac0985813`; `schema_rehearsal.py`
  `6e134172d7f8ed36b51cecdbf1673f368fc3404b4ca151476aa391d496923ee8`; `schema_orchestrator.py`
  `a4d81f16b0c08bf3f4a22c95b889e9f6faab261868ce341910827b63755947bd`; `bridge_runtime.py`
  `6e33d2b247323b25c2d5128481a0429a4fb2819da74f084f9f68b8ac9d582f77`.
- Managed surface grew by exactly two files (24 codex / 31 both), reflected consistently in
  `install.sh`, `package.json`, `bin/tether.js`, `_expected_manifest_target_modes`, and doctor
  expectations.

## Deviations and notes

- PLAN-L1C bullet 5 asked for an "installer-owned maintenance/stop/status primitive". Implemented
  as: installer-side mechanical refusal (flag existence under the lifecycle lock, exit 75) plus a
  coordinator-injected `GatewayController` (stop/start/is_active) whose production wiring lands in
  L1d/L1e. Rationale: ZEN — Bash must not own phase or quiesce semantics; the attestation
  (supervisor probe plus broker-socket connect refusal plus singleton availability) lives in
  Python where it is testable. The lying-stop falsifier passes.
- Latent time bomb fixed: `test_domain_control` pinned `now=2026-08-18 13:00 UTC` and began
  failing on the clean L1b tree once the calendar passed it (reproduced at `43d1221` before the
  fix). Replaced with data-relative clocks.
- Baseline drift observation: the machine default Node is v20; the release gate requires Node
  22/24 (ran on nvm v22.23.2, matching the L1b receipt's environment).

## Bounds

First valid PROVE attempt for the L1c slice; zero review/fix rounds consumed at checkpoint time.
PR 395 (stacked on 393 → 392) awaits review.
