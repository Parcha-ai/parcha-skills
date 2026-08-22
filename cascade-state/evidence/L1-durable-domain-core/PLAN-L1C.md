---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
slice: L1c-rehearsal-coordinator
status: CONSUMED (built at c248e08, PR 395)
target_head: 43d12219536dfeab93571c6b300e4a4e0280f60d
worktree: /home/ubuntu/worktrees/tether-rewrite-l1c-20260818
branch: codex/tether-rewrite-l1c-20260818
---

# L1c plan — kill-safe schema rehearsal before mutation

## Goal

Prove the complete process, artifact, backup, and recovery boundary on disposable database copies
without exposing a public migrate command or changing a live schema. The rehearsal result is
evidence only; production must repeat every check under a fresh quiesced receipt.

## Lock and phase order

1. Acquire the existing installer lifecycle `flock`.
2. Create and fsync one immutable-identity schema receipt at `planned`.
3. Arm a maintenance gate that prevents automatic Hermes admission/restart.
4. Stop Hermes and independently attest that its supervisor and gateway are inactive.
5. Acquire the exact `bridges.db.lock` singleton; never unlink or force it.
6. Verify the live schema/logical manifest and create a private SQLite-native backup.
7. Bind backup identity, digest, schema, manifest, target artifact, and predecessor snapshot to the
   receipt; fsync receipt and directory before continuing.
8. Release the live database singleton. Run 17→18→17 only on disposable verified copies.
9. Validate schema 18 with the exact staged target artifact and schema 17 with the exact pinned
   predecessor artifact. Source-tree imports are not evidence.
10. Restart the unchanged predecessor, verify build/schema attestation, mark `complete`, remove the
    maintenance gate, and release the lifecycle lock.

The receipt advances monotonically through `planned`, `quiesced`, `singleton_acquired`,
`backup_verified`, `runtime_verified`, `resumed`, and `complete`. Every transition is compare-and-
swap plus atomic rename, file fsync, and parent-directory fsync. `failed_safe` and `needs_operator`
retain maintenance mode. No SQLite transaction spans a supervisor/package command.

## First build slice

- Extract one authoritative schema-receipt parser/writer used by status and the runtime startup
  gate; do not duplicate phase semantics in Node, installer Bash, and Python.
- Make normal broker/plugin startup refuse any incomplete, invalid, or schema/code-contradictory
  receipt before opening `Store` or acknowledging ingress.
- Add a side-effect-free installed runtime `validate-store` entry point. It opens SQLite read-only,
  never instantiates schema-17 `Store`, and emits only build/schema/manifest attestation hashes.
- Implement the internal rehearsal coordinator behind a test-only/internal entry point. Keep the
  public Node CLI limited to `schema status` and keep `migration_ready=false`.
- Add an installer-owned maintenance/stop/status primitive under the existing lifecycle lock. Stop
  success alone is insufficient; prove inactive state and database-lock availability.

## Acceptance and falsifiers

| ID | Required observation | Falsifier |
|---|---|---|
| L1c.1 | Receipt identity and phases survive restart; incomplete/corrupt receipt blocks both runtime versions before DB open. | Either runtime boots production or acknowledges work with an incomplete receipt. |
| L1c.2 | Lifecycle lock is acquired before maintenance/quiesce and DB singleton; a contender fails before backup or SQLite mutation. | Lock inversion, force/unlink path, or two winning coordinators. |
| L1c.3 | Verified backup has exact DB identity, byte digest, integrity result, schema, and logical manifest bound before any transform. | Replaced/linked/truncated/wrong-mode backup is accepted or selected by filename. |
| L1c.4 | Disposable 17→18→17 preserves the declared manifest and boots exact packaged target/predecessor validators. | Current source tree substitutes for either artifact or a wrong writable schema passes. |
| L1c.5 | `SIGKILL` after every durable phase recovers to exactly one classified state with no admission and no guessed backup/artifact. | Two writers, unclassified state, or automatic pre-migration backup restore after resume. |
| L1c.6 | Status stays redacted and `migration_ready=false`; no public migrate/rollback command exists. | A path, payload, session reference, partial mutation verb, or fake readiness appears. |

## Required first red tests

- startup refuses `planned`, `backup_verified`, `db_committed`, corrupt, and schema/code-conflicting receipts;
- stop returns success while gateway remains active;
- gateway is inactive while another process retains `bridges.db.lock`;
- target/predecessor manifest changes after preflight;
- backup inode swap, hard link, mode expansion, digest mismatch, stale WAL/SHM, and orphan filename;
- kill after every receipt phase, including after backup fsync and before receipt fsync;
- installed target and predecessor validators run from pinned artifacts in a sanitized environment;
- status exposes hashes/IDs and typed blockers only, never paths or receipt internals.

## Deferred to L1d

L1c does not activate schema 18 or native scheduling. L1d adds the sole schema-18 `DomainRuntime`
and a detached-native driver whose first successful `Popen` observation emits durable `accepted`;
any lost proof after possible spawn becomes `uncertain` and is never re-executed. Only exact stripped
`NO_REPLY` is silence; empty output is failure. Herdr protocol 19 and Zellij remain ineligible for
automatic draining until they provide conditional request IDs and durable exact-turn watch/
reconcile/terminal receipts.

## ZEN / POST-ZEN gate

One receipt, one lifecycle lock, one database singleton, one installed validator contract, and no
new public mutation surface. Any implementation that adds a second journal or lets Node/Bash own
domain decisions fails POST-ZEN. Temporary rehearsal code is owned by L1 and either becomes the one
live orchestrator in L1d or is deleted before L1 exits.
