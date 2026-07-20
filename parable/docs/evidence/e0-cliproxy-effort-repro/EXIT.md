# E0 — CONTRACT AND FAILING REPRO · EXIT (2026-07-20)

## Status: COMPLETE

## The headline evidence

Two correctly wired Go regressions reproduce the same semantic loss at
CLIProxyAPI `v7.2.88`: a Claude-shaped request with top-level
`output_config.effort=low` and no `thinking` object becomes Codex
`reasoning.effort=medium`.

The direct translator test and the canonical cross-protocol conversion test
both expected `low`, observed `medium`, and exited non-zero for that exact
assertion. The shared failure proves the repair belongs at the canonical
thinking boundary plus its direct translator consumer, not in a Parable
model-name shim.

## What shipped

| Piece | Where |
|---|---|
| Autonomous successor chain and E0 re-plan | `docs/LOOP_CHAIN_2026-07-20-cliproxy-effort-fidelity.md` |
| Source, toolchain, red-test, and permission receipt | `docs/evidence/e0-cliproxy-effort-repro/receipt.json` |
| Focused contract merge | [PR #144](https://github.com/miguelrios/unc-skills/pull/144) |

## Bound accounting (honest)

- Correctly wired regression PROVE runs: 2, one at each architectural layer;
  both produced the intended semantic RED.
- Evidence failures: 0.
- Instrument failures: 1. The first canonical command used an incomplete Go
  subtest regex and reported “no tests to run.” Correcting the generated
  `Case...` subtest name produced the intended expected-`low`,
  observed-`medium` failure. The miswired selection did not consume an
  evidence attempt.
- Review rounds: 1. JSON invariants, credential/private-path scan,
  `git diff --check`, the full Parable suite, GitHub mergeability, and a
  Claudio-Michel-style base diff review had no findings. Review verdict: 5/5.
  The Grep-specific Python/React conventions were not applicable to this
  Markdown/JSON-only PR. GitHub reported no configured checks or reviewer.

ZEN check: E0 moved the repair to the general canonical thinking boundary
after source evidence showed the bug exists there too. It adds no model slug
condition, launcher mutation, credential path, or static effort mapping.

## Acceptance criteria → evidence

1. Clean isolated source pinned to the release commit — ✅ `receipt.json`
   records upstream commit
   `93d74a890a44802f656d7f39a573916b2611896e`, official archive SHA-256,
   matching translator SHA-256, and a clean local source import.
2. Focused semantic failure — ✅ both receipt entries expected `low`, observed
   `medium`, exited 1, and mark the semantic failure confirmed.
3. Authorized contribution route recorded — ✅ the required Claudio Michel
   GitHub App has no pull/push/triage/maintain/admin permission on the upstream
   repository. No personal-account fallback was used. E1 may create and prove
   an upstream-ready local commit but cannot fabricate an issue or PR.
4. Full Parable tests and focused merged contract PR — ✅ 81/81 tests pass in
   an isolated HOME; PR #144 is merged and verified at current `main`.

## The running delta table

| Loop | Translator regression | Canonical regression | Exact live effort | Upstream route | Kimi P2 |
|---|---|---|---:|---|---|
| G1 baseline | not pinned in source | not pinned in source | 3/15 | none | PAUSED |
| E0 | RED: low→medium | RED: low→medium | 3/15 | auth blocked | PAUSED |

MEASURE is skipped for E0 beyond the two semantic regressions: this loop pins
the mechanism and contribution boundary; the live fidelity delta belongs to
E2.

## exit → E1

Starting from local regression commit
`6db675c2e884f1cb9db97409f5c1bfc36f76a2b2`, implement and test the
cross-module canonical fix. Upstream publication remains externally blocked;
local build and proof continue autonomously.
