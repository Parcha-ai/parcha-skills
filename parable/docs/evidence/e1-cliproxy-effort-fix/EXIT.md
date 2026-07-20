# E1 — GENERAL TRANSLATOR FIX · EXIT (2026-07-20)

## Status: COMPLETE

## The headline evidence

The E0 `low → medium` regressions are green after one cross-module change:
Claude `output_config.effort` is normalized at the canonical thinking boundary
and used as a fallback only when the request has no `thinking` object. The
Claude-to-Codex translator consumes that shared normalization.

Explicit `thinking.type=disabled`, budgeted enabled thinking, enabled thinking
without a budget, and adaptive/auto thinking retain their prior precedence.
All five user-facing values (`low`, `medium`, `high`, `xhigh`, `max`) are
pinned in translator tests. The complete CLIProxyAPI Go suite and required
server build pass.

## What shipped

| Piece | Where |
|---|---|
| Upstream-ready local source commit | `ad20a87efbbd41a9d4f24c83ca8bdd934bf34ab7` |
| Sanitized mechanism/test/build receipt | `docs/evidence/e1-cliproxy-effort-fix/receipt.json` |
| Focused evidence merge | [PR #145](https://github.com/miguelrios/unc-skills/pull/145) |

## Bound accounting (honest)

- Correctly wired fix PROVE runs: 1. Focused tests, focused vet, complete Go
  tests, and the server build all passed on the first implementation.
- Evidence failures: 0.
- Instrument failures: 0.
- Review rounds: 1. The base-to-fix diff was reviewed for semantic precedence,
  invalid/blank inputs, fake-success defaults, model-specific branching,
  credential handling, formatting, and test coverage. No critical issue or
  warning remained; review verdict 5/5. Grep-specific Python/React conventions
  were not applicable to this Go change.

ZEN check: one normalized protocol field feeds the canonical extractor and
direct translator. The fix has no GPT slug check, Parable launcher mutation,
static effort table, credential path, or provider-account special case.

## Acceptance criteria → evidence

1. E0 regression passes — ✅ the direct translator and cross-protocol
   top-level-effort tests both exit zero and assert `low → low`.
2. Existing thinking modes and default are pinned — ✅ 12 canonical precedence
   subcases and 11 translator subcases cover absent/blank/non-string effort,
   all five user values, adaptive behavior, explicit disabled, explicit budget,
   enabled-without-budget, and the default `medium` fallback.
3. Relevant broader Go tests pass — ✅ focused vet passes,
   `go test -count=1 ./...` passes, and the required server build exits zero
   under official Go `1.26.5`.
4. No provider/model-specific shim — ✅ the receipt records cross-module
   `internal/thinking` plus `internal/translator/codex/claude` scope,
   `model_name_condition_added=false`, and `credential_handling_added=false`.
5. Source commit and authorized publication state — ✅ local commit
   `ad20a87efbbd41a9d4f24c83ca8bdd934bf34ab7` is clean and its two-commit patch
   SHA-256 is recorded. The required GitHub App lacks upstream permission, so
   no issue, PR, or external merge is claimed.
6. E1 EXIT merged into Parable — ✅ 81/81 Parable tests pass in an isolated
   HOME; PR #145 is merged and verified at current `main`.

## The running delta table

| Loop | Translator regression | Canonical regression | Full CLIProxy tests | Exact live effort | Upstream route | Kimi P2 |
|---|---|---|---|---:|---|---|
| G1 baseline | not pinned | not pinned | release binary | 3/15 | none | PAUSED |
| E0 | RED: low→medium | RED: low→medium | not run with red tests | 3/15 | auth blocked | PAUSED |
| E1 | GREEN: low→low | GREEN: low→low | PASS | not rerun | auth blocked | PAUSED |

MEASURE for E1 is the semantic test delta from two red layers to both green.
The subscription-backed live fidelity delta is reserved for E2.

## exit → E2

Run binary SHA-256
`139e1305a878e08026dd0fb6d85a3817801f32e40a40c1954ea1973c5bb9d697`
on a dedicated loopback port and repeat the exact 15-cell plus 3-tool G1
protocol. Runtime wire evidence, not these unit tests, decides support.
