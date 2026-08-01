---
name: parable
description: Install, onboard, and run multi-model coding orchestration or solo single-model mode. Invoke when the user says install Parable, install parable.sh, set up or onboard Parable, connect coding subscriptions, names Parable, hands over a backlog, or asks to route work across models. Solo mode (`parable --solo <model>`) runs with a single model. For first-time install intent, collect subscription choices with the harness's native structured-question tool and then run the bundled parable.sh. Not for a single isolated bugfix or standalone code review.
---

# parable — divide and conquer

## First-time install and subscription onboarding

When the user asks to install, set up, or onboard Parable, resolve `parable.sh` beside this
`SKILL.md`. Plugin caches and standalone skill installs live in different roots. Do not replace
it with `curl`, `npx`, or a repository clone: the skill carries the pinned runtime it needs.

Before running the script, state that Claude is the baseline subscription because the harness is
Claude Code, then collect the optional subscriptions with the harness's native structured-question UI:

1. Ask whether to connect a ChatGPT subscription for Sol, Terra, and Luna. Explain that Sol becomes
   an automatic fallback only when ChatGPT is connected; without it, `auto` remains on Fable.
2. Ask whether to connect an xAI subscription for Grok 4.5. Recommend yes when the user has one.
3. Ask whether to connect a Kimi Code subscription for Kimi. Recommend yes when the user has one.

In Claude Code use `AskUserQuestion`; in Codex use `request_user_input` when the active mode
permits it (normally Plan mode); in another harness use its equivalent structured-choice tool.
Ask all three questions in one tool call when possible. Use two-choice yes/no answers and explain
that the credentials stay user-owned in CLIProxyAPI. If the structured-question tool is absent or
unavailable in the active mode, ask the same three choices in one concise message and wait for the
answer. Do not attempt a known-unavailable tool and do not silently infer paid subscriptions.

Inspect `PARABLE_CLIPROXY_BIN` and `PATH` for an existing CLIProxyAPI executable. If none exists,
ask one more native yes/no question for consent to download source, install any missing build
prerequisite, and build Parable's pinned patched proxy. Prefer a user-local prerequisite install;
keep any Parable-owned data root at mode `0700`; request separate approval before any privileged
system change. Stop cleanly if consent is denied.

Build the canonical vendor list with `claude` always present and optional `chatgpt`, `xai`, and
`kimi` from those answers.

If the harness can keep a foreground process alive and write later user input to its stdin, run
exactly one foreground bootstrap:

```bash
bash /resolved/skill/directory/parable.sh \
  --non-interactive \
  --vendors claude[,chatgpt][,xai][,kimi] \
  [--proxy-bin /resolved/existing/proxy | --build-proxy]
```

Claude Code's `Bash` tool is the exception: it cannot send a pasted OAuth callback to a command's
stdin after that command starts, and its command view may clip long authorization URLs. In Claude
Code, stage setup through `Bash` with the same command plus `--no-auth`. After staging succeeds,
tell the user to open a new terminal and run exactly:

```text
parable auth login
```

Tell them to keep that command running until the selected flows complete. Do not run it through
Claude Code's `Bash` tool or `!` shell command. `auth login` walks every selected missing provider
in order, skips providers already connected, and prints the final launch command.
Do not give the user three separate `auth add` commands or ask them to run `setup finalize`. Do not
claim onboarding is complete before `auth login` succeeds. Outside this Claude Code exception, do
not pass `--no-auth` during ordinary onboarding; reserve it for explicit staged setup.

The bootstrap installs an immutable versioned runtime, creates the durable `parable` command,
updates the new-terminal PATH when necessary, and either runs authorization or prepares the one
new-terminal authorization command above.

Never print the final handoff yourself after a failed or unauthenticated script. The script prints
it only after setup and all selected authorization flows succeed:

```text
In a new terminal, open your project and run:

  parable
```

You are the **brain**: the most capable model in the room, and the most expensive. The strategy
is division of labor: carve the work into the smallest clusters that can proceed independently —
split by layer, by component, by concern, along whatever natural seams the task has — route each
cluster to the cheapest executor suited to it (the cast and its stage directions come from the
config), and run independent clusters concurrently under the shared-tree rules below. One
executor grinding through a whole feature serially is the expensive path in both wall-clock and
tokens. Planning, routing, verification, and judgment stay with you; you already know how to do
those. This file tells you what's available and the house rules; the method is yours.

## Solo Mode

**This section overrides every brain/cast, routing, delegation, reviewer, and load-balancing instruction elsewhere in this skill.** When you start with `parable --solo <alias>` or `parable --solo <exact-model-id>`, Parable runs in **solo mode**: a single model is selected as both the orchestrator and executor. No casting, no delegation, no load-balancing. The selected model plans, implements, tests, reviews, and finishes the work directly.

**Solo works only with models exposed through the configured Claude-compatible loopback proxy** (when `[claude]` is configured in `parable.toml`). Solo does not support codex, pi, or cursor executors; it is not a way to select arbitrary provider-backed models. If you need multi-model orchestration or non-Claude-proxy providers, use normal Parable mode.

Solo is **explicit only** — it never infers from pool count or vendor configuration. Start solo when you:
- Intentionally spend one Claude subscription lane.
- Skip routing overhead and multi-model complexity.
- Use one authenticated model lane without spending any other configured pool.
- Run evaluations or reproducible single-model tests.

### Solo syntax and aliases

Invoke solo with either an alias or an exact model ID. Both must resolve to models in the `[claude]` loopback proxy's authenticated catalog:

```bash
parable --solo fable             # Uses the configured alias 'fable' (exact Claude model)
parable --solo gpt-5.6-sol       # Uses exact id 'gpt-5.6-sol' (exact ChatGPT model)
parable --solo kimi              # Uses the configured alias 'kimi' (exact Kimi model)
```

Aliases come from enabled exact-model `[executors.*]` entries whose configured provider has `type = "subagent"`; the standard generated setup uses `provider = "claude"`. Parable accepts the executor id (`kimi`), the id without an `_exact` suffix (`sol` for `sol_exact`), or the exact catalog id (`kimi-k3`).

When you run `parable --solo kimi`, Parable resolves the alias to `kimi-k3`, verifies that exact id in the authenticated loopback catalog, and launches it directly as the Claude Code parent. Solo does **not** write, synchronize, or invoke project agent files.

### Solo behavior

Inside a solo session:
- The **Agent tool is unavailable**. No `Agent()`, no subagents, no agent-team orchestration.
  Solo disables delegation (the Agent tool and agent teams); it is not a sandbox — the
  session still loads your normal settings, plugins, hooks, and MCP servers, and existing
  `.claude/agents` files remain on disk (they are simply not invocable without the Agent tool).
- Startup skips `parable-config.sh`, agent synchronization, and the multi-model cast card. A **user-only** SOLO startup card is shown instead (session-scoped, no model context cost).
- The session uses only the selected model. Implement the user's work directly; do not ask to hand it to an executor.
- Verify and review your own work directly. Do not invoke `parable-review.sh` or seek another model.
- `--brain`, `--model`, and `--fallback-model` flags are rejected (they conflict with
  single-model solo selection), as are agent/tool override flags.

Solo refusals are **hard fails** — a solo session does not silently degrade to multi-model or skip Agent invocations. If you need agents or multi-model casting, use normal Parable mode (`parable` or `parable --brain`).

Every launch (solo and multi-model) also pins `CLAUDE_CODE_MAX_CONTEXT_TOKENS` to the real
context window of the proxied non-Anthropic models in play and sets
`CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=75`. Claude Code otherwise guesses 200k or 1M for models it
does not recognize, then waits until about 95% to compact; that leaves too little room for Sol
tool results before the upstream limit. Details and override knobs: `references/config.md`.

An explicit CLI resume into a smaller window (`--continue`, `--resume <name-or-id>`, or
`--from-pr <value>`) gets a pre-launch guard. Parable asks Claude Code's native `/context`
command for the resumed size using Sonnet 5's 1M context at low effort and zero model tokens. If
the existing context is already beyond the target launch's 75% safe point, the same Sonnet
session runs native `/compact` before Parable starts the selected brain. The selector is then
replaced by the resolved session id so the check, compaction, and launch cannot drift to
different "latest" sessions. A bare `--resume` picker and `--fork-session` cannot be safely
pre-compacted before Claude resolves or creates their session; the launcher reports that it
skipped the guard. SessionStart hooks cannot implement this guard because Claude Code defines
them as non-blocking context/UI hooks, not a place to invoke built-in commands before loading a
model.

If an Agent call fails after launch with `auth_unavailable` or `auth_not_found`, Parable's
`PostToolUseFailure` hook shows the user the affected subscription and exact
`parable auth add <vendor>` recovery command while giving the parent agent the same context.
`auth_unavailable` may be a short transient state, so the notice recommends one delayed retry
before reauthorization; `auth_not_found` goes directly to reauthorization. A distinct
`model_cooldown` notice says to wait or route elsewhere and explicitly does not recommend OAuth.

### Solo requires `[claude]` configuration

Solo mode **requires** `parable.toml` to have `[claude]` configured (subscription-only mode with a loopback proxy). Solo cannot run with codex, pi, cursor, or other provider-backed executors. When launching with `parable --solo <model>`:

1. Parable checks that `[claude]` is configured (proxy base_url, auth token, brain_model).
2. Startup verifies that the selected model is present in the loopback proxy's authenticated catalog.
3. If the selected model is missing, unavailable, or the proxy is unreachable, startup fails.

Solo does not fall back to multi-model mode or provider pools; it is all-or-nothing with the selected exact model.

### Configuration merging in solo

Solo mode reads `parable.toml` and focuses on the `[claude]` proxy configuration only:

**Solo does not route through:**
- `[routing]` tables.
- `[parable] default_executor` / `default_reviewer`.
- Codex, pi, cursor, or other CLI-backed executors.

**Solo uses:**
- `[claude]` base_url, auth_token_env, and binary.
- Enabled exact-model `[executors.*]` entries only for friendly alias resolution; no agent files are written.
- Check definitions (`parable-verify.sh` still runs).
- Research settings when the sole model performs in-session research directly.

The complete config is still parsed and validated. Other provider and executor blocks remain available for future normal multi-model launches, but solo never dispatches them.

## Session start (Multi-model mode)

Run `scripts/parable-config.sh` once. It prints the executor cast (credential status, costs,
`use_for`/`avoid_for` stage directions), the routing menus per task class, the configured
checks, the research provider, and `repo_notes` — repo conventions that belong in every plan.
That output plus this file covers the common case; the full selection algorithm and escalation
ladder live in `references/routing.md`. First detect whether this harness exposes a native
subagent/agent-spawn tool. If it does, no config means the Tier-0 defaults (`sonnet` implements,
`opus` reviews) can run through that tool. If it does not (notably stock pi), the built-in
subagent cast is **unavailable**: require at least one configured `codex`, `codex-native`, `pi`,
or `cursor` executor before dispatching. Installation is portable; a missing executor is not
fake runtime parity. With no configured checks, `parable-verify.sh` passes vacuously until the
user declares some.

**Note:** Solo mode skips this entire section. See **Solo Mode** above.

When the config contains `[claude]`, the session should have been entered through
bare `parable` (multi-model mode), which selects the automatic brain policy at high effort, or through
`parable --solo <model>` (solo mode). Ordinary skill-first subscription onboarding is one
`parable.sh` run followed by that fresh-terminal launch command. Setup delegates
native vendor authorization; the launcher starts or reuses the loopback proxy,
checks the selected parent against the exact catalog, synchronizes agents (multi-model mode only),
and cleans up only a proxy it owns. Missing optional exact-model agents degrade that launch:
the session card marks them unavailable and the dispatch hook blocks them without deleting their
durable agent files. Restart Parable after provider authentication recovers to refresh the
session-scoped availability snapshot.
`parable auth add`, `parable proxy start`, and `parable setup finalize` remain
headless/recovery diagnostics.

**Multi-model mode:** Arbitrary-model Claude executors are
synchronized as project-local native agents named `parable-<normalized-executor-id>`;
`parable-config.sh` prints the exact
`agent=` name beside each one. Invoke that named agent through the Agent tool and omit the
Agent call's optional `model` field. The generated agent file owns the exact model; a per-call
model silently overrides its frontmatter. Parable's session plugin removes such an override
from `parable-*` calls as a final guard. Claude Code's
built-in model aliases remain normal model-selected subagents.

**Solo mode:** Agent synchronization is skipped. The selected model runs directly as the parent; the Agent tool is unavailable.

## Load-balancing across subscriptions (Multi-model mode only)

The reason to configure more than one pool is that each subscription is a separate,
mostly-fixed budget, and the expensive failure is exhausting one while another sits idle. Solo mode uses a single model and pool, so load-balancing does not apply.
A cast can span three subscription pools — the Claude plan (subagent executors), a ChatGPT
plan (`codex-native` executors), and a Cursor plan (`cursor` executors) — plus metered
API-key providers (codex/pi) as an overflow valve. Marginal cost on the subscription pools is
zero only while included capacity remains; Claude usage credits and ChatGPT credits can turn
overflow into metered spend. Routing is therefore about
**keeping every pool's headroom above water and never starving the pool that funds the current
session**. Which pool that is depends on the harness running this skill.

`scripts/parable-usage.sh` makes this measurable instead of reactive: it reads each pool's own
usage endpoint — Claude's 5h/7d window % and current-period usage-credit meter, ChatGPT's 5h/7d
window % and credit/overage state, Cursor's dollars left this cycle — for zero model tokens and
no turn. Read it before a batch and whenever a pool feels
tight, and route the next dispatch to the pool with the most room among the executors capable of
the task. The routing menus in the config are **menus of capable peers, not priority ladders**:
the config author writes which executors can do each task class; you pick among them by live
headroom. Never dispatch an executor whose `model` is the current parent model: the parent
already owns that lane and delegating back to it only burns the same pool. During review, also
exclude the model that authored the diff. A Claude `extra=` value is the endpoint's cumulative current-period meter, not a
weekly total; `daily`/`weekly` remain explicit nulls in JSON when Anthropic does not provide
history. Don't wait for a throttle error to learn a pool is empty — that error is the failure
this tool exists to prevent. The per-pool selection detail lives in `references/routing.md`.

## The tools

**Multi-model mode:**
- `scripts/parable-usage.sh [--all] [--json]` — live subscription headroom and billing state for
  every pool the cast routes to (Claude plan window % plus usage credits, ChatGPT plan window %
  plus credits/overage, Cursor dollars-left-this-cycle),
  read from each harness's own usage endpoint for zero model tokens and no turn. Read it BEFORE
  a batch and whenever a pool feels tight: this is the measured load-balancing signal, so you
  spread work by headroom instead of discovering a spent pool through a throttle error. A pool
  at ≥80% used prints `TIGHT` and stops being a default. Fails soft — an unreadable credential
  reports `unknown` (route as if it has room), never an error. (Solo mode: not applicable.)

**Both modes:**
- `scripts/parable-run.sh <executor> <plan.md> [workdir] [--effort <level>]` — dispatch a
  codex-, pi-, or cursor-backed executor headlessly; prints status, session id, turns, tokens
  and cost, last message, and the run dir. In multi-model mode, subagent executors (`sonnet`, `opus`, …) dispatch via
  the harness's native agent-spawn tool with the plan as the prompt. For a custom model in a
  `parable` session, use the exact `agent=parable-*` name printed by
  `parable-config.sh`; for a bare Claude alias, use a general-purpose agent with the executor's
  model. If there is no native agent-spawn tool, that executor is unavailable; choose a
  CLI-backed executor. In solo mode, the selected proxy model runs directly as the parent.
- `parable [--brain auto|fable|sol|config] [--] [CLAUDE_ARGS...]` or `parable --solo <alias|exact-model> [--] [CLAUDE_ARGS...]` — safely reuse a healthy configured loopback proxy or own its
  start/readiness/cleanup lifecycle. Multi-model mode requires its selected parent and tolerates unavailable optional cast members; solo requires only its selected exact model. Multi-model mode: `auto` prefers Fable while
  its pool is below the tight threshold, then falls back to Sol by live usage; `fable` and `sol`
  pin either parent, and `config` uses `brain_model`. Idempotently synchronizes Parable-owned project agents and displays the multi-model cast card. Solo mode: `--solo <model>` launches with the single selected model only; `--brain` is rejected. With no arguments, Parable uses `auto` (multi-model) and
  forwards `--effort high`. Claude flags pass through directly, so for example
  `parable --dangerously-skip-permissions` works; the `--` separator is optional. Never enable
  permission bypass implicitly.
  `parable claude` remains a backward-compatible explicit alias. `parable setup finalize`
  performs only the catalog/sync diagnostic against an already-running proxy. These are package
  CLI commands, not skill scripts.
- `parable proxy upgrade` — build Parable's current pinned, patched CLIProxyAPI in a new
  versioned directory and atomically stage it for future launches. It does not interrupt an
  already-running proxy; the next managed start uses the new binary.
- `scripts/parable-resume.sh <run-dir> "<message>"` — continue an executor's existing session
  (caching economics: facts below). Sessions do not transfer between executors. Not used in solo mode
  (single executor, no orchestration).
- `scripts/parable-status.sh <run-dir>` — live run state in ~7 lines for zero model tokens;
  the cheap first read on any run.
- `scripts/parable-verify.sh --when <post-implement|pre-commit>` — the repo's configured
  checks, compact pass/fail with the actionable failure lines. Works in both modes.
- `scripts/parable-review.sh <reviewer> --author <executor> --paths <files> --plan <plan.md>` —
  multi-model-only cross-model review with the rubric from `references/review-prompt.md`. Findings
  print to stdout and land in the `REVIEW_FILE` path it prints. In solo, do not dispatch this reviewer; inspect and verify your own work directly.

The scripts are conveniences, not walls — drive a harness CLI directly when the flow needs
it. Provider recipes and direct-CLI gotchas: `references/providers.md`. Config schema:
`references/config.md`.

## House rules

**Multi-model mode:**
- The reviewer never shares the author's model — `--author` enforces it.
- A subagent never shares the current parent model; select another capable model from the menu.
- A feature split across concurrent plans is reviewed once, as one integrated diff against the
  whole feature intent — the union of the changed paths plus the full spec. Reviews scoped per
  plan cannot see integration seams and misread sibling work as scope violations.
- Implementing a task yourself is the user's money: ask first through the harness's user-input
  mechanism, except for fixes smaller than the handoff would cost.

**Both modes:**
- `parable-verify.sh` is green before any commit — and acceptance criteria are verified on the
  route production requests actually take. Code reached by no live caller proves nothing;
  green unit tests do not prove a route.
- Present plans for approval before implementing, unless the user pre-authorized autonomy.
- Concurrent runs in one working tree get disjoint owned paths, declared in their plans;
  overlap means serialize. Commit verified work promptly — uncommitted diffs in a shared tree
  are unprotected — and verify or commit landed runs before any tree-wide git operation of
  your own (stash, checkout, reset), which can silently drop a sibling run's uncommitted work.
- Spend and wait mechanics: your harness ships its own spend discipline, written by the model's
  creator and newer than anything here — follow it over any advice in this file. The facts below
  only add what is parable-specific.

## Facts you cannot derive mid-session

- Executor sessions share zero context with this conversation and follow plans literally —
  a plan must be self-contained: intent, scope, off-limits paths, the `repo_notes` conventions.
  (Multi-model mode: each executor gets its own plan. Solo mode: one plan, one model.)
- **Multi-model mode:** Every executor harness keeps its own sessions and rides its provider's prompt cache: codex
  stores server-side threads (`codex exec resume <id>`), pi stores local session files
  (`--session-id`), and a resumed session's replayed prefix bills at cached rates — follow-ups
  cost cents where a fresh briefing re-bills the whole context. The bar for resuming is lower
  than it feels: the session already holds the executor's codebase exploration, so even work
  needing significant improvement usually lands better as continuity feedback to that session
  than as a fresh start that re-explores from zero. Two edges: a session exists only once a
  first turn has completed (a run killed earlier records no session id, leaving nothing to
  resume), and an over-grown or misled session stops being a bargain — start fresh when its
  accumulated context misleads more than it informs, and always when changing executors.
  Claude-subagent executors have no resumable session.
- **Solo mode:** The single selected model's session is the primary Claude Code conversation itself; there is no separate executor session to resume or manage.
- Where a run's work lives, per harness: every parable run dir holds `harness.jsonl` (the live
  event stream, one JSON event per line), `plan.md`, `cmd.txt`, `meta.json`, and a
  `resume-N.jsonl` per follow-up; pi runs also keep the full session transcript under
  `<run-dir>/sessions/`; codex sessions started outside parable land in
  `~/.codex/sessions/<Y>/<m>/<d>/rollout-*.jsonl`. Transcripts grow to megabytes; the working
  tree itself is the other ground truth for what an executor has actually produced.

## Research and research-backed artifacts

grep.ai is Parcha's hosted research service (a free tier exists); parable defaults
`[research].provider` to it — set `"claude"` to keep research in-session. With the default,
route in-depth research and research-backed deliverables (reports, slidedecks, spreadsheets,
PDFs, apps) through the installed **grep-research-skills** package: invoke its skills using the
current harness's normal skill syntax (`research`, `ultra-research`, `grep-build-slidedeck`,
`grep-domain-expert`, and friends — each description says when it fits).
`npx grep-research-skills` installs them; `grep-login` authenticates. Quick lookups are ordinary
in-session web searches. These
deliverables have no git diff: close by confirming the artifact with the user.
