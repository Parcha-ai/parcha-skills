# The Loop Chain — CLIProxyAPI GPT effort fidelity (2026-07-20)

> Successor to the merged G0/G1 model matrix and its G2 human choice: patch the
> Claude-to-Codex translation boundary, then rerun the same live proof. Each
> loop has a self-contained prompt, checkable safe evidence, and a hard bound.
> Raw prompts, responses, proxy logs, credentials, and private paths never
> enter the repository.
>
> **Pacing:** autonomous, with an external-maintainer gate for any upstream
> CLIProxyAPI merge and a final human re-plan gate.

## Loop anatomy

| Field | Meaning |
|---|---|
| `goal` | One sentence describing the state change. |
| `prompt` | Self-contained execution instructions for a fresh session. |
| `accept` | Evidence-based exit criteria naming safe, checkable artifacts. |
| `bound` | Maximum evidence and review iterations before honest escalation. |
| `exit →` | The next loop triggered by a valid exit. |

## The ribbon

Every loop follows:

```text
RE-PLAN → BUILD → PIN → PROVE → MEASURE → REVIEW → MERGE → EXIT
```

The source patch is built in a fresh CLIProxyAPI checkout pinned to release
`v7.2.88` / commit `93d74a890a44802f656d7f39a573916b2611896e`.
The live proof uses stock Claude Code `2.1.215`, the merged Parable launcher,
a dedicated loopback-only proxy process, exact model ids
`gpt-5.6-{sol,terra,luna}`, and ChatGPT subscription OAuth. Direct provider API
keys remain unset. Public evidence contains only runtime pins, model and effort
fields, status codes, counts, booleans, and deterministic hashes.

**Bound + escalation:** at most two correctly wired failed PROVE runs and
three REVIEW→fix rounds per loop. At the bound, write an `AT_BOUND` EXIT naming
the unmet criteria and stop. Harness/instrument faults are diagnosed separately
and do not consume evidence attempts.

## Order rationale

Pin the fake-success regression before editing the translator. Prove the
smallest protocol-level fix with unit tests before spending subscription turns.
Only then rebuild and repeat the identical live matrix. Packaging/upstreaming
comes after live fidelity, so OSS instructions never promise an unproved patch.

## The chain

### E0 — CONTRACT AND FAILING REPRO

- **goal:** Pin the contribution route and a unit regression that reproduces
  the dropped top-level effort.
- **prompt:**
  > Create a fresh CLIProxyAPI checkout at the pinned v7.2.88 commit and read
  > its repository instructions. Determine the authorized GitHub contribution
  > route without using a personal account. Add a focused translator test whose
  > input has `output_config.effort=low` and no `thinking` object, matching
  > stock Claude Code with an exact third-party GPT model id. Prove that the
  > unmodified translator emits `reasoning.effort=medium`. Publish only a
  > content-free receipt and the source/test pins in Parable.
- **accept:**
  1. Fresh source checkout is clean and pinned to the exact release commit.
  2. The focused test fails for the intended semantic reason: expected `low`,
     observed `medium`; compilation or harness failures do not count.
  3. Receipt records the authorized contribution route and no credential
     material.
  4. Full Parable tests pass; focused contract PR is merged and verified at
     current `main`.
- **bound:** 2 PROVE runs / 3 review rounds.
- **exit →** E1.

### E1 — GENERAL TRANSLATOR FIX

- **goal:** Make a valid top-level Claude `output_config.effort` survive
  Claude-to-Codex translation even when `thinking` is absent.
- **prompt:**
  > Starting from E0's failing test, implement the smallest general change in
  > `ConvertClaudeRequestToCodex`. Preserve the existing explicit
  > `thinking.enabled`, `thinking.adaptive|auto`, and `thinking.disabled`
  > semantics. Pin precedence and normalization with table-driven tests,
  > including absent effort, effort without thinking, adaptive effort,
  > budget-derived effort, and disabled thinking. Run gofmt, the focused
  > package tests, and the repository's relevant broader test slice. Commit an
  > upstreamable change in the isolated source checkout.
- **accept:**
  1. E0 regression passes and directly asserts `low → low`.
  2. Tests pin existing thinking modes and the default `medium` fallback.
  3. Focused and broader relevant Go tests pass on the pinned toolchain.
  4. Diff contains no provider/model-specific routing rule or credential
     handling.
  5. Source change is committed and an authorized PR is opened when repository
     permissions allow; external maintainer merge is not fabricated.
  6. E1 EXIT is merged into Parable and verified at current `main`.
- **bound:** 2 PROVE runs / 3 review rounds.
- **exit →** E2.

### E2 — PATCHED LIVE MATRIX

- **goal:** Prove whether the patched proxy provides exact effort control for
  Sol, Terra, and Luna in stock Claude Code.
- **prompt:**
  > Build the pinned E1 CLIProxyAPI commit and run it on a dedicated loopback
  > port with the existing ChatGPT OAuth state. Repeat the G1 protocol exactly:
  > fifteen fresh named non-persistent text cells for three models ×
  > `low|medium|high|xhigh|max`, plus one medium Bash tool canary per model.
  > Independently verify synthetic hashes and parse only inbound
  > `output_config.effort`, translated `reasoning.effort`, statuses, completion,
  > and OAuth-route booleans. Record every provider clamp or rejection; never
  > infer fidelity from the CLI flag.
- **accept:**
  1. All 15 cells have non-null requested, inbound, and upstream effort plus a
     successful deterministic hash, or exit `AT_BOUND` naming exact failures.
  2. Requested-to-upstream exact fidelity is 15/15, unless the live provider
     returns a documented clamp/rejection that forces an honest `AT_BOUND`.
  3. Sol, Terra, and Luna each complete a deterministic tool canary.
  4. All requests use ChatGPT OAuth with direct provider API keys unset.
  5. Sanitized receipt and updated OSS caveat are merged in a focused Parable
     PR and verified at current `main`.
- **bound:** 2 correctly wired attempts per cell / 3 review rounds.
- **exit →** E3.

### E3 — OSS DELIVERY AND UPSTREAM STATUS

- **goal:** Leave OSS users a reproducible supported path while the upstream
  patch moves through maintainer review.
- **prompt:**
  > Read E0–E2 EXITs and the source PR state. If upstream merged and released,
  > pin the first good release. If the PR remains open, document the exact
  > source commit/build route without vendoring credentials or silently
  > replacing CLIProxyAPI. Ensure the five-minute setup clearly separates
  > released support from patched-source support. Re-run Parable tests and link
  > only sanitized merged evidence.
- **accept:**
  1. OSS instructions identify the exact known-good source/release state.
  2. No unmerged patch is represented as an upstream release.
  3. Upstream PR state is recorded with a public URL, or the precise
     authorization blocker is documented.
  4. Full tests pass; focused delivery PR is merged and verified at `main`.
- **bound:** 2 documentation proofs / 3 review rounds.
- **exit →** E4.

### E4 — RE-PLAN GATE

- **goal:** Present the verified effort-control verdict and choose the next
  model-routing proof.
- **prompt:**
  > Read all EXITs and state the exact released or patched setup, its model and
  > effort fidelity, test counts, and upstream status. Keep Kimi explicitly
  > paused. Recommend either GPT-parent→named-GPT-subagent proof, reliability
  > repetitions, or waiting for upstream release, then pause for the user.
- **accept:**
  1. Verdict maps every claim to merged receipts.
  2. Released and locally patched states are clearly distinguished.
  3. Kimi P2 remains `PAUSED`.
  4. User chooses the successor.
- **bound:** 2 verdict drafts; user sign-off wait is unbounded.
- **exit →** a new successor chain.

## Chain invariants

1. No translator edit precedes E0's failing semantic regression.
2. No loop advances without an `EXIT.md` mapping every criterion to evidence.
3. Deltas are cumulative; regression reopens the affected cell.
4. `AT_BOUND` is an honest stop, never completion.
5. Instrument failures are separate from evidence failures.
6. External maintainer review is a human gate; no upstream merge is assumed.
7. ZEN applies at every BUILD: one general protocol rule, no model-name shim.
8. This chain is append-forward; its final gate creates a successor document.
9. Public evidence is synthetic or content-free. Raw logs, prompts, responses,
   credentials, tokens, customer data, and private paths stay out of Git.
10. Kimi remains paused until the operator explicitly resumes it.
