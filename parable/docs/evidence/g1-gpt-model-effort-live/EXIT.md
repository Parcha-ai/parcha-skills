# G1 — LIVE MODEL × EFFORT MATRIX · EXIT (2026-07-20)

## Status: COMPLETE

## The headline evidence

Sol, Terra, and Luna each completed all five stock Claude Code effort
invocations through CLIProxyAPI and ChatGPT subscription OAuth. All 15 text
cells exited zero, returned their exact synthetic marker, produced one
attributable model request, and carried the requested value into
`output_config.effort`.

The effort control is not end-to-end in CLIProxyAPI `v7.2.88`: all 15
translated GPT requests used `reasoning.effort=medium`. The three requested
`medium` cells are exact pass-through; the other twelve are mappings to
`medium`. Each model also completed a real medium-effort Bash tool round-trip
and produced an independently verified artifact.

## What shipped

| Piece | Where |
|---|---|
| Sanitized 15-cell and 3-canary scoreboard | `docs/evidence/g1-gpt-model-effort-live/receipt.json` |
| Version-pinned translator diagnosis | `docs/evidence/g1-gpt-model-effort-live/mechanism.md` |
| OSS-facing version caveat | `skills/parable/references/providers.md` |

## Bound accounting (honest)

- Correctly wired text attempts: 15, one per cell; all passed within the
  two-attempt bound.
- Correctly wired tool attempts: 3, one per model; all passed.
- Evidence failures: 0.
- Mechanism diagnostic: one additional Sol/low invocation with
  `CLAUDE_CODE_ALWAYS_ENABLE_EFFORT=1`; it passed but still omitted `thinking`
  and translated to `medium`.
- Review rounds: 1. Receipt invariants, JSON parsing, content/credential scan,
  `git diff --check`, 81 tests, and GitHub mergeability had no findings. PR
  #142 reported no configured checks or reviewer.
- Instrument correction: the first tool result parser expected one proxy log,
  but a successful tool round-trip correctly creates two model requests (tool
  selection and post-tool continuation). The two logs per canary were grouped
  by the bounded invocation window and parsed through the same safe-field
  filter. No rerun or evidence attempt was consumed.

ZEN check: the result is a declarative scoreboard and a source-pinned
diagnosis. Parable gains no provider-specific effort shim, credential path, or
regex routing rule. The proposed repair remains at the general protocol
translation boundary where it belongs.

## Acceptance criteria → evidence

1. All 15 cells have requested, inbound, and upstream effort plus a successful
   synthetic hash — ✅ `receipt.json` contains 15 non-null cells; every exit
   code is zero, every deterministic check is true, and every stream completed.
2. One medium-effort tool canary per model — ✅ three of three artifacts passed
   independent content and SHA-256 verification; each canary's two model
   requests returned HTTP 200.
3. OAuth routing with no direct provider key — ✅ every parsed request used the
   Codex OAuth route, while `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` were unset;
   no provider key, OAuth token, or proxy key is committed.
4. Exact pass-through is distinguished from mapping — ✅ the receipt records
   15/15 requested-to-inbound exact, 3/15 requested-to-upstream exact, and the
   explicit `low|medium|high|xhigh|max → medium` mapping.
5. Full tests and focused merged PR — ✅ 81/81 tests pass in an isolated HOME;
   [PR #142](https://github.com/miguelrios/unc-skills/pull/142) is the focused
   merge.

## The running delta table

| Loop | Sol text | Terra text | Luna text | Exact upstream effort | Tool canaries | Kimi P2 |
|---|---:|---:|---:|---:|---:|---:|
| G0 | catalog only | catalog only | catalog only | not run | 0/3 | PAUSED |
| G1 | 5/5 | 5/5 | 5/5 | 3/15 (`medium` only) | 3/3 | PAUSED |

MEASURE for G1 is transport and wire fidelity. It makes no comparative model
quality, latency, token-use, or long-session reliability claim.

## exit → G2

At the re-plan gate, present the honest product verdict and ask the user to
choose the successor: patch CLIProxyAPI and repeat this matrix, prove a GPT
parent calling a named GPT subagent, or run repeated reliability canaries.
Kimi remains explicitly paused until subscriptions reopen and the operator
resumes it.
