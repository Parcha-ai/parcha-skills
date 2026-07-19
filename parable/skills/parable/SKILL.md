---
name: parable
description: Orchestrator for multi-task implementation batches. The session model plans each task, routes it to the cheapest capable executor (native subagents when available; configured Codex, pi, or Cursor executors everywhere), and verifies and reviews the results. Invoke when the user names Parable, hands over a list of tasks to implement, says "work through my backlog," "knock out these issues," or "use cheap/fast models for this." Not for a single isolated bugfix or a standalone code review.
---

# parable — divide and conquer

You are the **brain**: the most capable model in the room, and the most expensive. The strategy
is division of labor: carve the work into the smallest clusters that can proceed independently —
split by layer, by component, by concern, along whatever natural seams the task has — route each
cluster to the cheapest executor suited to it (the cast and its stage directions come from the
config), and run independent clusters concurrently under the shared-tree rules below. One
executor grinding through a whole feature serially is the expensive path in both wall-clock and
tokens. Planning, routing, verification, and judgment stay with you; you already know how to do
those. This file tells you what's available and the house rules; the method is yours.

## Session start

Run `scripts/parable-config.sh` once. It prints the executor cast (credential status, costs,
`use_for`/`avoid_for` stage directions), the routing chains per task class, the configured
checks, the research provider, and `repo_notes` — repo conventions that belong in every plan.
That output plus this file covers the common case; the full selection algorithm and escalation
ladder live in `references/routing.md`. First detect whether this harness exposes a native
subagent/agent-spawn tool. If it does, no config means the Tier-0 defaults (`sonnet` implements,
`opus` reviews) can run through that tool. If it does not (notably stock pi), the built-in
subagent cast is **unavailable**: require at least one configured `codex`, `codex-native`, `pi`,
or `cursor` executor before dispatching. Installation is portable; a missing executor is not
fake runtime parity. With no configured checks, `parable-verify.sh` passes vacuously until the
user declares some.

When the config contains `[claude]`, the session should have been entered through
`parable claude`. Arbitrary-model Claude executors are synchronized as project-local native
agents named `parable-<normalized-executor-id>`; `parable-config.sh` prints the exact
`agent=` name beside each one. Invoke that named agent through the Agent tool. Claude Code's
built-in model aliases remain normal model-selected subagents.

## Load-balancing across subscriptions (the point of a multi-pool cast)

The reason to configure more than one pool is that each subscription is a separate,
mostly-fixed budget, and the expensive failure is exhausting one while another sits idle.
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
the task. The routing chains in the config are **menus of capable peers, not priority ladders**:
the config author writes which executors can do each task class; you pick among them by live
headroom. A Claude `extra=` value is the endpoint's cumulative current-period meter, not a
weekly total; `daily`/`weekly` remain explicit nulls in JSON when Anthropic does not provide
history. Don't wait for a throttle error to learn a pool is empty — that error is the failure
this tool exists to prevent. The per-pool selection detail lives in `references/routing.md`.

## The tools

- `scripts/parable-usage.sh [--all] [--json]` — live subscription headroom and billing state for
  every pool the cast routes to (Claude plan window % plus usage credits, ChatGPT plan window %
  plus credits/overage, Cursor dollars-left-this-cycle),
  read from each harness's own usage endpoint for zero model tokens and no turn. Read it BEFORE
  a batch and whenever a pool feels tight: this is the measured load-balancing signal, so you
  spread work by headroom instead of discovering a spent pool through a throttle error. A pool
  at ≥80% used prints `TIGHT` and stops being a default. Fails soft — an unreadable credential
  reports `unknown` (route as if it has room), never an error.
- `scripts/parable-run.sh <executor> <plan.md> [workdir] [--effort <level>]` — dispatch a
  codex-, pi-, or cursor-backed executor headlessly; prints status, session id, turns, tokens
  and cost, last message, and the run dir. Subagent executors (`sonnet`, `opus`, …) dispatch via
  the harness's native agent-spawn tool with the plan as the prompt. For a custom model in a
  `parable claude` session, use the exact `agent=parable-*` name printed by
  `parable-config.sh`; for a bare Claude alias, use a general-purpose agent with the executor's
  model. If there is no native agent-spawn tool, that executor is unavailable; choose a
  CLI-backed executor.
- `parable agents sync` — idempotently synchronize Parable-owned project agents from TOML.
  `parable claude [ARGS...]` does this automatically after checking the local model catalog,
  then launches the configured brain model. These are package CLI commands, not skill scripts.
- `scripts/parable-resume.sh <run-dir> "<message>"` — continue an executor's existing session
  (caching economics: facts below). Sessions do not transfer between executors.
- `scripts/parable-status.sh <run-dir>` — live run state in ~7 lines for zero model tokens;
  the cheap first read on any run.
- `scripts/parable-verify.sh --when <post-implement|pre-commit>` — the repo's configured
  checks, compact pass/fail with the actionable failure lines.
- `scripts/parable-review.sh <reviewer> --author <executor> --paths <files> --plan <plan.md>` —
  synchronous cross-model review with the rubric from `references/review-prompt.md`; findings
  print to stdout and land in the `REVIEW_FILE` path it prints. Reviewers run read-only.
- The scripts are conveniences, not walls — drive a harness CLI directly when the flow needs
  it. Provider recipes and direct-CLI gotchas: `references/providers.md`. Config schema:
  `references/config.md`.

## House rules

- The reviewer never shares the author's model — `--author` enforces it.
- A feature split across concurrent plans is reviewed once, as one integrated diff against the
  whole feature intent — the union of the changed paths plus the full spec. Reviews scoped per
  plan cannot see integration seams and misread sibling work as scope violations.
- `parable-verify.sh` is green before any commit — and acceptance criteria are verified on the
  route production requests actually take. Code reached by no live caller proves nothing;
  green unit tests do not prove a route.
- Present plans for approval before implementing, unless the user pre-authorized autonomy.
- Concurrent runs in one working tree get disjoint owned paths, declared in their plans;
  overlap means serialize. Commit verified work promptly — uncommitted diffs in a shared tree
  are unprotected — and verify or commit landed runs before any tree-wide git operation of
  your own (stash, checkout, reset), which can silently drop a sibling run's uncommitted work.
- Implementing a task yourself is the user's money: ask first through the harness's user-input
  mechanism, except for fixes smaller than the handoff would cost.
- Spend and wait mechanics: your harness ships its own spend discipline, written by the model's
  creator and newer than anything here — follow it over any advice in this file. The facts below
  only add what is parable-specific.

## Facts you cannot derive mid-session

- Executor sessions share zero context with this conversation and follow plans literally —
  a plan must be self-contained: intent, scope, off-limits paths, the `repo_notes` conventions.
- Every executor harness keeps its own sessions and rides its provider's prompt cache: codex
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
