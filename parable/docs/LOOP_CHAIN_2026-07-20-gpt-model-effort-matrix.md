# The Loop Chain — GPT model × effort matrix in stock Claude Code (2026-07-20)

> Kimi Code subscriptions are temporarily unavailable, so the operator paused
> the cross-provider P2 proof and asked for broader proof of the ChatGPT
> subscription route already established by L1/L2. This successor chain tests
> Sol, Terra, and Luna through stock Claude Code and distinguishes a CLI flag
> being accepted from the requested effort actually reaching the upstream GPT
> request. Raw prompts, responses, traces, OAuth state, and credentials remain
> private. Nothing advances on vibes.
>
> **Pacing:** autonomous until `AT_BOUND` or the final human re-plan gate.

## Outcome contract

The matrix under test is:

| Model | Effort levels requested through stock Claude Code |
|---|---|
| `gpt-5.6-sol` | `low`, `medium`, `high`, `xhigh`, `max` |
| `gpt-5.6-terra` | `low`, `medium`, `high`, `xhigh`, `max` |
| `gpt-5.6-luna` | `low`, `medium`, `high`, `xhigh`, `max` |

A passing cell requires all of the following:

1. the Parable launcher preflight sees the exact model in the authenticated
   CLIProxyAPI catalog;
2. stock Claude Code exits zero on a synthetic content-free canary;
3. the private inbound proxy trace records the requested model and the effective
   Claude effort value;
4. the private translated request records the upstream GPT reasoning effort;
5. the public receipt contains only model, requested/effective effort, HTTP
   outcome, counts, timestamps, and synthetic hashes.

“`--effort` did not error” is not proof that the effort reached GPT. If Claude
Code or CLIProxyAPI clamps, maps, or omits a level, the receipt records that
behavior rather than labeling the requested level as effective.

## Loop anatomy

| Field | Meaning |
|---|---|
| `goal` | One sentence describing the state change the loop produces. |
| `prompt` | A self-contained instruction block that a fresh session can execute using only this document and its named inputs. |
| `accept` | Evidence-based exit criteria naming safe, checkable artifacts. |
| `bound` | Maximum inner attempts before honest escalation. |
| `exit →` | The loop triggered after a verified exit. |

## The ribbon

```text
RE-PLAN  Read this chain, the active loop, current HEAD, and pinned runtimes.
BUILD    Add only the bounded contract/evidence or a mechanism fix exposed by proof.
PIN      Test fake-success modes: accepted flag but omitted/mapped upstream effort.
PROVE    Run the isolated live matrix; keep raw logs private.
MEASURE  Record cell pass counts and effective-effort mappings, not quality claims.
REVIEW   Open one focused PR and resolve every finding.
MERGE    Merge only after the loop's criteria are green.
EXIT     Map every criterion to merged evidence in the loop's EXIT.md.
```

Runtime protocol:

- Stock Claude Code `2.1.215`.
- CLIProxyAPI `v7.2.88`, commit
  `93d74a890a44802f656d7f39a573916b2611896e`, bound to `127.0.0.1`.
- The existing user-owned ChatGPT subscription OAuth record; no OpenAI API key.
- `parable claude` from current merged `main`, using a temporary clean repository
  and a credential-free TOML per model.
- `--safe-mode --no-session-persistence` for text cells so background title
  generation cannot masquerade as the requested cell.
- One deterministic tool-using canary per model at `medium`, separate from the
  text matrix, to preserve harness/tool coverage without multiplying tool calls
  across all fifteen effort cells.
- Raw logs live only in the local CLIProxyAPI log directory/private run
  directory. A local parser emits safe fields; it never prints message bodies.

**Bound + escalation:** maximum two correctly wired failed attempts per cell and
three review→fix rounds per loop. Instrument/setup failures do not consume a
cell attempt but are recorded. At the bound, write `EXIT.md` with
`status: AT_BOUND`, identify the exact failing cells, and stop.

## Task graph

```text
G0 CONTRACT + CATALOG
  → G1 LIVE MODEL × EFFORT MATRIX
    → G2 RE-PLAN [human sign-off]

Original L3 KIMI LIVE remains paused, not passed or failed.
```

## The chain

### G0 — MATRIX CONTRACT AND AUTHENTICATED CATALOG

- **goal:** Pin the exact runtime matrix, authenticated model availability, and
  non-spoofable effort evidence before spending model turns.
- **prompt:**
  > From a fresh worktree at current `origin/main`, pin Claude Code and
  > CLIProxyAPI versions. Start one dedicated loopback proxy and query its
  > authenticated `/v1/models` endpoint using the local client token without
  > printing or persisting the token. Confirm exact catalog entries for Sol,
  > Terra, and Luna. Inspect the installed CLI help and available source only to
  > define the effort levels and wire fields; explicitly treat source/binary
  > drift as a hazard and let G1 runtime traces decide. Write a content-free
  > contract receipt and run the existing Parable suite.
- **accept:**
  1. A sanitized catalog receipt records exactly one entry for each target model.
  2. The contract names all five effort inputs advertised by the installed CLI
     and the inbound/outbound fields G1 must observe.
  3. The protocol prevents session-title/background requests from being counted
     as matrix cells.
  4. Existing Parable tests pass at current HEAD.
  5. Focused PR merged; criteria verified at merged HEAD.
- **bound:** 2 PROVE runs / 3 review rounds.
- **exit →** G1.

### G1 — LIVE MODEL × EFFORT MATRIX

- **goal:** Produce a falsifiable live scoreboard for all fifteen model/effort
  cells plus one tool-using canary per model.
- **prompt:**
  > Use the G0 runtime and dedicated loopback process. For each model × effort
  > cell, create a fresh non-persistent stock Claude Code invocation through
  > `parable claude`, request one deterministic synthetic marker, and verify its
  > exact hash independently. Parse only safe routing fields from the private
  > proxy trace: requested model, inbound effective effort, translated upstream
  > reasoning effort, HTTP status, and stream completion. Then run one bounded
  > medium-effort tool canary per model that creates a deterministic temporary
  > artifact and verify its hash. Never infer effective effort from the CLI
  > argument alone. If a level is clamped/mapped, record the mapping; if omitted,
  > mark that cell unsupported rather than passed.
- **accept:**
  1. All 15 cells have non-null requested, inbound-effective, and
     upstream-effective effort fields plus a successful synthetic hash, or the
     loop exits `AT_BOUND` naming the exact unsupported/failed cells.
  2. Sol, Terra, and Luna each complete one medium-effort tool canary with an
     independently verified artifact.
  3. Receipts prove OpenAI Codex OAuth routing and no direct provider API key.
  4. The scoreboard distinguishes exact pass-through from clamping/mapping.
  5. Full tests pass; focused evidence/mechanism PR merged at current `main`.
- **bound:** 2 correctly wired attempts per cell / 3 review rounds.
- **exit →** G2.

### G2 — RE-PLAN GATE

- **goal:** Turn the matrix into an honest product recommendation and decide the
  next live proof while Kimi remains paused.
- **prompt:**
  > Read G0/G1 EXITs and the original L0–L2 evidence. State which GPT models and
  > effort levels are genuinely supported through Claude Code, including every
  > clamp/map. If the runtime needs `CLAUDE_CODE_ALWAYS_ENABLE_EFFORT`, a launcher
  > default, validation change, or documentation correction, draft the smallest
  > successor loop. Otherwise recommend the next proof: GPT parent→GPT named
  > subagent, repeated reliability canaries, or resuming Kimi when subscriptions
  > reopen. Pause for the user.
- **accept:**
  1. Verdict maps every model/effort claim to merged receipts.
  2. No requested effort is presented as effective without wire evidence.
  3. Original Kimi P2 remains explicitly `PAUSED`, not silently dropped.
  4. User chooses the successor.
- **bound:** 2 verdict drafts; human sign-off wait is unbounded.
- **exit →** successor chain; this document remains append-forward.

## Chain invariants

1. No live matrix turn runs before G0's contract exists.
2. No loop advances without an `EXIT.md` mapping every criterion to evidence.
3. Cell counts and effort mappings are cumulative; regression reopens the cell.
4. `AT_BOUND` is an honest stop, never a synonym for completion.
5. Harness/setup failures are separated from evidence failures.
6. Kimi OAuth remains paused until the operator explicitly resumes it.
7. Apply the ZEN check: thin adapters, general receipts, model judgment for
   semantic work, code only for structural/security invariants.
8. Public evidence is content-free or synthetic. Raw logs, prompts, responses,
   tool traces, OAuth files, credentials, and identifying local paths never enter
   the repository.
