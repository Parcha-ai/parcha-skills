# L0 architecture review findings

Date: 2026-08-18 UTC
Candidate: `e37126b0a9adcef1b851102878bc1ca71d90d0c5`

Three independent review tracks examined current runtime/data-model reliability,
Hermes/upstream coupling, and the target ADR/HTML. The final candidate resolves
the review findings as follows.

## Resolved P0 findings

- Native submission is conditioned atomically on endpoint incarnation, lease
  fence, and request ID; `watch` and `reconcile` carry exact-turn durable
  receipts. Herdr needs an exact-turn upstream contract. Zellij remains
  legacy/uncertain-only until it has one.
- Same UID is no longer claimed as a process-secret boundary. Privileged
  readiness requires non-dumpability, ptrace and `/proc` isolation, child
  environment/FD allowlists, and a canary that proves the boundary.
- Hermes ingress persists the normalized envelope, mutation revision, and
  unresolved disposition before transport ACK. Exclusive-owner callback
  failure stays durable and fails closed; recovery is local, not dependent on
  Slack redelivery.
- Privilege provenance is registered by supervised Tether over a non-inherited
  authority channel. Agents receive only opaque, single-use handles; grant and
  audit state remains root-owned.

## Resolved P1 findings

- Egress now specifies rejected and failed-before-I/O outcomes, immutable-key
  conflict, and get/watch/reconcile by receipt or key.
- Endpoint security domains prevent one pane from spanning workspace, persona,
  or owner trust boundaries.
- Native execution terminal state is separate from Hermes platform delivery.
- New-root, attach, rebind, close, blocker, and operator-only resolution flows
  are explicit and restart-safe.
- One cross-host Slack owner lease and generation are required; lease loss
  disconnects transport and fails readiness.
- Rollback is additive/predecessor-readable or quiesce/fence/drain/replay after
  live admissions; a pre-migration backup alone is rejected.
- `BlockingCondition` is one health/readiness/list projection, while `resolve`
  exposes only its operator-resolvable subset.
- Existing Hermes-session and headless-job sources have explicit migration
  adapters instead of disappearing.

## Evidence-harness corrections from review

- Removed the tautological disappearing-file unit test. The cross-repository
  evaluator now proves the historical ephemeral intake path and its current
  durable/readability control from exact source revisions.
- The real fleet hook is executed with a fake local Tether client to prove both
  the missing idempotency key and swallowed exit status. The real wrapper block
  is parsed to prove the missing reply key and `|| true`.
- The operator mismatch is exercised through the real Node CLI and local broker.
- Corpus, metrics, and provenance refuse dirty candidates and record source
  hashes, Git tree, runtime versions, commands, and explicit verdict classes.
- The HTML validator binds the published architecture and 505-test claim to
  the clean candidate and evidence receipts.

## Remaining gates

Runtime redesign is intentionally not implemented in L0. The ADR deletion gates
must pass before removing any compatibility path. Credential/host policy,
canary, Slack traffic, deployment, rollback, and fleet rollout remain outside
current authority.
