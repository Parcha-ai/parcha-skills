---
cascade_version: 2
episode_id: tether-rewrite-2026-08-17
loop: L1
checkpoint: L1b-read-only-control-readiness
status: COMPLETE_CHECKPOINT
target_head: 43d12219536dfeab93571c6b300e4a4e0280f60d
pull_request: https://github.com/Parcha-ai/parcha-skills/pull/393
next: L1c-rehearsal-coordinator
---

# L1b checkpoint — fail-closed schema readiness and operator control

This is not an L1 exit receipt. It freezes the read-only control and readiness boundary before any
schema mutation or native scheduler cutover. The active runtime remains schema 17. No service, live
database, Slack event, credential policy, machine policy, deployment, or rollback changed.

## Result

- Added one installed read-only `tether schema status` path that projects database/runtime
  compatibility, the domain blocker set, descriptor readiness, installed integrity, and incomplete
  receipt state into typed `BlockingCondition` records.
- Added an authority-gated operator resolution coordinator for schema 18. Capabilities default off;
  exact uncertainty may be completed or abandoned only after an external authority verifier passes
  both before lock acquisition and again inside the committing transaction. Retry is never offered.
- Disabled the legacy same-UID broker and CLI `resolve` mutation. The model/endpoint cannot be its
  own recovery authority while the OS-isolated writer/operator boundary is absent.
- Hardened status inputs with pinned no-follow reads, database inode checks, exact installed target
  inventory, installer-owned canonical mode rules, and redacted JSON failures for unsafe config.
- Added explicit persona and policy-generation configuration inputs without inventing migration
  defaults. Schema mutation remains unavailable through the stable `schema_mutation_unavailable`
  condition until quiesce, singleton, backup, predecessor boot, and isolation gates exist.
- Packaged and release-tested the new control modules and corrected all operator documentation to
  identify schema 17 as active and schema 18 as an offline target only.

## Evidence

| Claim | Evidence | Falsifier |
|---|---|---|
| Exact candidate | Commit `43d12219536dfeab93571c6b300e4a4e0280f60d`; PR 393 is stacked on PR 392 and reports mergeable. Key SHA-256 values: `domain_control.py` `b929bada5bcc4aacc154716ba2a1aa4f5ef9ae027f82ed611cb09df010f2cf8a`; `schema_orchestrator.py` `cd1444553f6043eb0d1e06660ca4be9bb66610a461570cb1db818f40ba405bb7`; `tether.js` `1efed583c1a27f576d5898dc65c3fa7a171ae69da13ddbb5ad0e8000b04b565d`. | Dirty tree, different source hash, or PR head/base mismatch. |
| Complete compatibility and packaging | Node 22.23.2 `npm test`: exit 0; 581 tests PASS, followed by byte compilation, Node/shell syntax, installer crash-recovery lifecycle, and release tarball lifecycle PASS. `npm pack --dry-run --json`: 43 files, both new runtime modules present. | A removed compatibility contract, skipped release tail, missing installed module, or evidence from pre-fix bytes. |
| Status/control falsifiers | Focused final suite after the documentation correction: 20/20 PASS. Independent adversarial review: forged mode `0666` and non-executable launchers fail closed in Node and Python; symlinked config returns exit 1 with exactly one `config_file_unsafe` JSON object, empty stderr, and no path/traceback; delayed authority replay after endpoint recapture returns `already_applied`. | Self-declared manifest mode becomes authority, symlink/rename race reaches SQLite or config parsing, replay conflicts after legal incarnation advance, or retry appears for uncertainty. |
| Static boundary | Ruff, focused Bandit on the three changed runtime security/control modules, `py_compile`, Node/shell syntax, and `git diff --check`: exit 0. A broad Bandit scan including legacy tests emitted existing test-fixture findings and was not counted as green. | New high-severity security finding, syntax failure, unmanaged artifact, or whitespace error. |
| Documentation contract | `ARCHITECTURE.md`, README, compatibility, operations, security model, and runtime all identify schema 17 as active; status says mutation unavailable and same-UID resolution disabled. | Any operator guidance claims schema 18 is active, recommends manual database surgery, or advertises unsafe resolve. |
| Independent review | Two exact-byte read-only reviews reported no remaining P0/P1 after the final schema-version correction. One reviewer ran 83 focused tests; the other ran 13 focused tests and the supported Node 22 installer lifecycle. | Reproducible authorization bypass, false readiness, unsafe path/mode acceptance, disclosure, or documentation/runtime contradiction. |

## L1 acceptance status

- **L1.1 partial:** schema authority and the shared blocker projection are present. Runtime endpoint
  scheduling and reply-to-origin traces remain L1c.
- **L1.2 partial:** terminal proof and authority resolution are schema-constrained, and unsafe legacy
  callback recovery is disabled. Exact detached-driver submit/watch/reconcile and crash proof remain
  L1c; Herdr/Zellij are not eligible for automatic production draining yet.
- **L1.3 partial:** the August 14 uncertainty is visible through one condition and never advertises
  retry; authority resolution is tested in-process. Installed operator mutation and the full
  status/doctor/resolve journey remain L1c behind OS isolation.
- **L1.4 checkpoint pass:** L1a migration/fallback remains green, and L1b now reports every missing
  cutover capability instead of mutating. Receipt-bound backup, quiesce, packaged predecessor boot,
  and recovery from interrupted orchestration remain L1c.
- **L1.5 checkpoint pass:** the complete current compatibility, installer, release, and packaging
  suite is green at the candidate commit.
- **L1.6 partial:** one read-only schema CLI and one blocker projection replace divergent recovery
  claims, but schema 17 remains the runtime authority until L1c proves cutover and rollback.

## ZEN and POST-ZEN

The control path has one job: describe the actual state and refuse unsafe mutation. One status
command delegates to one installed Python authority, one blocker type drives both schema and domain
readiness, and one capability object keeps privileged recovery off by default. The implementation
does not simulate missing quiesce, backup, isolation, or driver capabilities.

POST-ZEN keeps the system boring by deleting the externally callable same-UID recovery path rather
than wrapping it, reusing the installer manifest instead of adding a second integrity catalog, and
keeping schema mutation absent until it can be one receipt-bound operation. Temporary migration
compatibility remains owned by L1c, with L2 deletion gated on stock Hermes ingress/egress receipts
and a proven rollback horizon.

## Next

Continue L1 as L1c with an internal rehearsal coordinator only. Under the existing installer
lifecycle lock, arm a maintenance gate, quiesce and re-attest the real gateway owner, acquire the
exact database singleton, bind a verified private backup and predecessor artifact to a durable
phase receipt, exercise 17→18→17 on a disposable copy, boot-validate the exact target and
predecessor, and recover deterministically from every kill point. Keep `migration_ready=false` and
expose no public mutation. L1d can then add the schema-18 runtime and cut over only the detached
native adapter with conditional request/incarnation/fence receipts. Herdr and Zellij remain
fail-closed until their upstreams prove exact-turn lifecycle. Prove the installed August 14 blocker
journey before any deployment or L1 exit.
