---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
checkpoint: L1a-offline-expand-compat
status: COMPLETE_CHECKPOINT
target_head: a469d464a895100453f370b1ba001dea0f5ce851
pull_request: https://github.com/Parcha-ai/parcha-skills/pull/392
next: L1b-runtime-cutover
---

# L1a checkpoint — offline domain expansion and fallback compatibility

This is not an L1 exit receipt. It freezes the offline schema/migration boundary so L1b can cut the
runtime over without redesigning migration under pressure. `bridge_runtime.py` remains schema 17;
no service, live database, Slack event, credential policy, or machine policy changed.

## Result

- Added the schema-18 authority model: one `Endpoint` to many `ThreadBinding` rows, ordered
  `QueuedTurn` rows, one open fenced `EndpointLease`, immutable `NativeAttempt` history, exact driver
  receipts, and authority-backed operator resolutions.
- Added fail-closed schema-17 migration with explicit security-domain descriptors, global physical
  endpoint identity, quarantine without choosing between divergent sibling snapshots, foreign keys,
  immutable state transitions, and normalized before/after manifests.
- Added behavior-preserving schema-18→17 fallback and 17→18 re-upgrade. The bounded fallback horizon
  journals attempt/turn relationships and blocks pruning of admitted records, so multiple fallback
  retries, claimed batch order, delivered response identity, and terminal events without attempts
  remain recoverable. All rollback-only objects are removed after successful re-upgrade.
- Packaged and integrity-tracked the offline module in the installer, npm tarball, CLI managed-file
  inventory, CI, and release smoke path.
- Corrected the ADR trust model: the Tether state writer must be OS-isolated from endpoint/model
  processes; same-UID mode 0600 and `SO_PEERCRED` are not a security boundary.

## Evidence

| Claim | Evidence | Falsifier |
|---|---|---|
| Exact candidate | Commit `a469d464a895100453f370b1ba001dea0f5ce851`; domain SHA-256 `388652ab39249bc25b715b40accaa9732048c10a1d3fd69173ac8d7382f9702a`; migration-test SHA-256 `30c626364e61f9071b4d2efab326da0ab552864b24a26b58bb092d8fb99789cc`; schema-test SHA-256 `4fbfda42db9af7aa3d85a5d6a1b3ac24ac7b4cf93dc92972470aaea8bd032139`. | Dirty tree, different source hash, or evidence from another revision. |
| Domain and migration contract | `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tether/tests -p 'test_domain_*.py' -q`: 63/63 PASS. | Lost/duplicate membership, reversed batch order, cross-domain endpoint, mutable terminal, open-lease collision, blind ambiguous retry, or lossy rollback. |
| Complete compatibility and packaging | Node 22.23.2 `npm test`: exit 0; complete Python suite, byte compilation, installer upgrade/rollback crash injection, and release tarball lifecycle PASS. | Any skipped/failed legacy routing, authorization, outbox, file, installer, or release contract. |
| Hosted matrix | PR 392 at `a469d464a895100453f370b1ba001dea0f5ce851`: Python/Node 3.11/22, 3.12/24, 3.13/22, 3.14/24, native Linux arm64, repository portability, full repository test, gitleaks, and package checks PASS. | Pending/failing job, another head SHA, or a read-only public-repository check treated as write proof. |
| Static boundary | Ruff, Bandit, `py_compile`, and `git diff --check`: exit 0. | Unpackaged module, syntax error, lint/security finding, or whitespace error. |
| Independent review | Three read-only review tracks reported no remaining P0/P1 inside the intentional offline L1a boundary at the exact hashes above. | A reproducible loss, duplicate, identity confusion, authority bypass, or predecessor-operability failure. |

## L1 acceptance status

- **L1.1 partial:** schema and property tests prove Endpoint 1:N bindings and one open lease. Runtime
  scheduler cutover, sibling fairness trace, and reply-to-origin integration remain L1b.
- **L1.2 partial:** the receipt state machine is driver-owned and ambiguity is nonterminal. Exact
  Herdr/headless adapters, runtime recovery, and visible operator notice remain L1b.
- **L1.3 not passed:** one `BlockingCondition` projection and the August 14 CLI/operator journey remain
  L1b.
- **L1.4 checkpoint pass:** forward migration, fallback operation, fallback-era admissions/retries,
  pruning, fault atomicity, re-upgrade, manifests, and package inclusion are exercised offline.
  Coupled quiesce/backup/cutover orchestration remains L1b.
- **L1.5 partial:** the local complete suite and hosted candidate matrix pass. The runtime cutover
  corpus remains L1b.
- **L1.6 partial:** schema 18 has one domain authority, but schema 17 remains the only runtime by
  design. L1b must replace it rather than dual-write.

## ZEN and POST-ZEN

The core is deliberately boring: identity lives once on `Endpoint`; thread routing lives once on
`ThreadBinding`; endpoint concurrency is a unique open lease; driver proof and Slack egress are
separate facts. No model callback can assert completion. SQL constraints and one invariant validator
make invalid histories fail closed before cutover.

The large compatibility projection is temporary migration machinery, not a second runtime. L1b owns
its orchestration. L2 owns deletion after the public Hermes ingress/egress ledgers and rollback window
are proven. Removal gate: no schema-17 fallback process remains, all admitted events replay from the
Hermes ingress ledger, egress idempotency survives packaged rollback, and the stock-plugin corpus is
green. Until then, the rollback-horizon journal is retained because deleting it would lose fallback
history.

## Next

Continue L1 as L1b. Do not deploy schema 18 from this checkpoint. First build one installed CLI
orchestrator that quiesces admission, takes a receipt-bound private backup, verifies the expected
manifest, migrates, boots the packaged schema-18 runtime, and can project/boot the exact predecessor.
Then replace the legacy scheduler and callback completion path, expose one blocker/operator surface,
and prove the production August 14 wedge as visible and resolvable without retrying uncertain work.
