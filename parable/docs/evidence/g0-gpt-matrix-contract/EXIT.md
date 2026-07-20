# G0 — MATRIX CONTRACT AND AUTHENTICATED CATALOG · EXIT (2026-07-20)

## Status: COMPLETE

## The headline evidence

The authenticated ChatGPT-subscription catalog exposes exactly one entry each
for `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.6-luna`. Stock Claude Code
2.1.215 advertises five effort inputs (`low`, `medium`, `high`, `xhigh`, `max`);
the G1 contract requires both inbound `output_config.effort` and translated
upstream `reasoning.effort` before calling any effort effective.

The current Parable suite passes 81/81 at baseline HEAD.

## What shipped

| Piece | Where |
|---|---|
| Successor cascade and 15-cell protocol | `docs/LOOP_CHAIN_2026-07-20-gpt-model-effort-matrix.md` |
| Sanitized runtime/catalog contract | `docs/evidence/g0-gpt-matrix-contract/contract.json` |

## Bound accounting (honest)

- Correctly wired PROVE runs: 1, passed.
- Evidence failures: 0.
- Review rounds: 1. JSON validation, content-safety scan, staged diff, baseline
  tests, and GitHub mergeability had no findings; the repository reported no
  automated checks or configured reviewer on PR #140.
- Instrument failure: the first catalog request supplied its local-token
  environment assignment and header expansion in the same shell command, so
  the header expanded before the assignment and the proxy correctly returned
  HTTP 401. Passing the directly extracted value to the header fixed the
  harness; no model request occurred and no proof attempt was consumed.

ZEN check: the matrix is declarative and general—models × advertised efforts,
with runtime wire facts deciding support. It adds no provider logic, effort
mapping, or credential handling to Parable.

## Accept criteria → evidence

1. Exactly one authenticated entry for each target model — ✅
   `contract.json` records a ten-model catalog and target counts of one for Sol,
   Terra, and Luna.
2. Five installed-CLI effort inputs plus required wire fields — ✅
   `contract.json` records `low`, `medium`, `high`, `xhigh`, `max`,
   `output_config.effort`, and `reasoning.effort`.
3. Background/title requests cannot masquerade as cells — ✅ the chain runtime
   protocol requires `--safe-mode --no-session-persistence` and a fresh
   invocation per cell.
4. Existing tests pass — ✅ 81/81 with an isolated temporary HOME at baseline
   `11249c32306ceb94ae5bc1875ec18cc58a31ff09`.
5. Focused PR merged and verified at HEAD — ✅
   [PR #140](https://github.com/miguelrios/unc-skills/pull/140) is merged, and
   its resulting `main` HEAD is checked immediately before G1 advances.

## The running delta table

| Loop | Sol catalog | Terra catalog | Luna catalog | Effort cells | Tool canaries | Kimi P2 |
|---|---:|---:|---:|---:|---:|---:|
| G0 | 1 | 1 | 1 | 0/15 run | 0/3 run | PAUSED |

MEASURE for G0 is the authenticated availability baseline; no quality, latency,
or reliability claim is made.

## exit → G1

After PR #140 is merged and verified at `main`, run all fifteen live text cells
and three representative medium-effort tool canaries. Record every clamp,
mapping, omission, or failure honestly.
