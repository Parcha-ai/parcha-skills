---
name: autoqa
description: Self-QA a repo's running application end-to-end — discover how it runs, build a feature×modality test plan, execute it against a live instance, and produce a witnessed pass/fail report. Use when the user says "autoqa", "QA this repo/branch", "verify every feature works", "release readiness check", "test this like a QA engineer would", or another skill needs an automated QA pass before sign-off.
---

# autoqa — QA any repo against its own running app

You are the QA engineer for this repo. Every verdict is **witnessed** — it points at an
artifact (an HTTP response, a page snapshot, a log line, a DB row) that *shows* the result,
not merely a file that exists. The full witness contract is in Hard rules below.

## Phase 0 — RESOLVE

Establish three facts before anything else:

1. **Target repo** — path or URL the user pointed at (ask only if truly absent).
2. **Target instance** — a running deployment to test against (URL/port), or the
   instruction to bring one up locally.
3. **Repo config** — look for `AUTOQA.md` at the repo root or under `docs/`. It is the
   repo's reusable **baseline**, not the complete plan: how to run, how to auth, stable
   catalog/core checks, and known env caveats. **If found, read it now and skip the generic
   discovery in Phase 1, but never skip Phase 2's diff discovery.** Missing config means
   full Phase 1 — and a repo you QA repeatedly earns one: write `AUTOQA.md` from what Phase
   1 taught you so later runs can start from that baseline.

Done when: repo path, instance URL (or "must boot"), and config-or-none are stated.

## Phase 1 — DISCOVER

Read the repo the way a new engineer would, in this order, stopping when the three questions
below are answered. Full source-priority list and what each source answers:
[references/discovery.md](references/discovery.md).

- **How does it run?** Dockerfile / compose / Procfile / Makefile / CI workflows / README.
- **How do I authenticate?** env samples, auth middleware, dev-token conventions, CLAUDE.md / AGENTS.md.
- **What are the features?** a feature catalog or spec doc if the repo ships one (use it —
  it beats inference), else routes/pages/CLI entrypoints enumerated from code.

Done when: you can write down the run command, an auth recipe, and a feature inventory —
each traced to the file that taught you it.

## Phase 2 — PLAN

Build the plan from the union of two sources:

1. **Baseline inventory** — every feature/check required by `AUTOQA.md`, or Phase 1 when no
   config exists.
2. **Diff inventory** — cases derived from the actual change under test. Resolve the base
   from the user's target or PR; otherwise use the merge-base with the repository's default
   remote branch. Include committed, staged, unstaged, and relevant untracked changes. Read
   the diff and the acceptance/design docs it changes or cites. Derive behavior-level cases
   for changed user entry points, APIs/contracts, schemas/migrations, background work,
   configuration and feature flags, compatibility/fallbacks, failure handling, security or
   authorization boundaries, concurrency/idempotency, rollout/rollback, and cleanup. Do not
   mistake a large unit-test list for this inventory.

First write both inventories as numbered lists, preserving their source (`BASE` or `DIFF`).
Then build one matrix from their union: one row per inventory item, columns = source,
modality, entry point, check, pass criterion, witness to capture. Deduplicate overlapping
rows without dropping the stronger pass criterion. The matrix must have ≥1 row per union
item; the report states separate and total coverage arithmetic so a reader sees nothing was
silently dropped.

### Confirm execution scope with the user

After drafting the inventories and before executing, use `AskUserQuestion` with multiple
choice and `multiSelect: true` to ask what the user wants included. In Codex environments,
use the equivalent structured user-input tool when available. Populate the choices from the
actual repo and diff, rather than showing a generic checklist. Offer up to four concise
groups such as:

- **Changed behavior + seams (Recommended)** — every DIFF case and its nearest regressions.
- **Full baseline catalog** — unaffected baseline features in addition to changed seams.
- **Stateful/destructive cases** — migrations, deletes, lifecycle, imports, billing, or
  external writes; name the exact synthetic/isolated safeguards in the description.
- **Performance/soak or platform matrix** — only when the diff makes it relevant.

The stable health/auth/core-money-path baseline is always included and must be stated in the
question. Treat the selection as test scope, not authorization to mutate production or real
customer data. If the user already explicitly selected scope (for example, “smoke only” or
“go ham/full release QA”), do not ask a redundant question; record that choice in the plan.
If no structured question tool is available and scope is not explicit, ask the same concise
multi-choice question in plain text and wait.

- **Entry point is a gating column, decided here — not at execute time.** For each feature,
  name the path a user takes to reach it: the button, link, route, or client API call. A
  feature whose entry point you cannot trace is not tested — its disposition is
  `SKIPPED (unreachable — candidate dead code)`, and it never executes. Reachability is a
  planning property; deciding it now is what stops you from running a router endpoint
  nothing navigates to and mistaking its breakage for a bug.
- **Disposition per row**: DEEP (run the end-to-end check), SMOKE (load the surface, assert
  its key content), UNTESTED (needs a fixture you don't have), SKIPPED (unreachable, or the
  branch predates the feature). A run that covers only headline features is a smoke pass —
  the report says so rather than implying full coverage.
- **Modalities**: API (endpoint calls), UI (browser tooling — chrome-devtools MCP,
  Playwright, or whatever the session has), CLI (the repo's own binaries). Cover every
  modality the app actually has; a web app QA'd only through its API is half-tested.
- **Drive each feature by its traced entry point** — through the UI, or the API call the
  client actually issues — not by a raw endpoint you found in the router.
- **Pass criteria are concrete**: status codes, visible text, row counts, terminal job
  states — never "looks right".
- **Diff cases are additive.** `AUTOQA.md`, a feature tracker, or a prior report can never
  suppress a test implied by the current diff. A prior PASS is context, not a witness for
  the current run.
- Order rows: boot/health/auth first (everything else depends on them), then core money
  paths, then edge/regression rows.

Done when: baseline and diff inventories exist, the matrix has ≥1 row per union item, every
row carries all required columns, every executable row names a traced entry point, and the
user's selected execution scope is recorded.

## Phase 3 — EXECUTE

Execute only rows whose entry point was traced in PLAN. Run the matrix top to bottom
against the live instance.

- Instance not up? Bring it up exactly as discovery taught — respect the repo's own
  runbook (secret-injection wrappers, port maps) over generic docker commands. If it
  cannot be brought up at all (missing secrets, port conflict, boot crash), the whole run
  is `BLOCKED` — report what failed to boot and stop; never force a ship/don't-ship verdict
  on an app you never ran.
- Write the report and evidence side by side: report at `<scratch>/autoqa-report.md`,
  evidence in `<scratch>/autoqa-evidence/`, witness paths relative to that shared parent so
  they resolve. Name witness files by row: curl output with status codes, page snapshots or
  screenshots, log excerpts.
- A failing row gets one diagnosis pass: is it the app, the env, or your check? Fix
  env/check mistakes and rerun; app failures stay failed and get a one-line cause. (Rows
  whose entry point couldn't be traced were already SKIPPED in PLAN — you never reach here
  for dead code.)
- Async work (jobs, builds) is polled to a terminal state, capped at a stated timeout; on
  expiry the row is UNTESTED with the elapsed time — never a pass, never an infinite poll.
- Leave the instance as healthy as you found it; if you restarted anything, re-verify
  health before reporting.

Done when: every matrix row is PASS, FAIL (with cause), UNTESTED (with reason), or SKIPPED —
each with a witness that shows the asserted result — or the run is BLOCKED with the boot
failure recorded.

## Phase 4 — REPORT

Write the report from the template in
[references/report-template.md](references/report-template.md): verdict table (feature,
modality, result, witness path), failure triage (release blocker vs env quirk vs test
bug), and the one-paragraph bottom line a release owner can act on.

Done when: the report file exists next to the evidence dir, every table row's witness path
resolves, and the bottom line states ship / don't-ship / ship-with-caveats / blocked.

## Hard rules

- **No witness, no verdict.** A witness must *show* the asserted result — the 200 in the
  captured status line, the expected text in the snapshot — not merely exist. A named file
  that doesn't show the result is not a witness. Reruns beat inference.
- **Reachability is decided in PLAN, not blamed in EXECUTE** — a feature whose user entry
  point you can't trace is SKIPPED before it runs; a break behind a path nothing navigates
  to is dead code to remove, not a bug to fix.
- **Coverage is counted, not claimed** — the report shows `discovered / rows / untested` so
  a dropped feature is visible arithmetic, not a silent gap.
- **Baseline plus diff, always** — treat `AUTOQA.md` as the reusable floor. Inspect the
  current diff every run and add the cases it implies; never execute a stale static catalog
  as though it covered new behavior.
- **Scope is an explicit user choice** — use a structured multi-select question after
  planning unless the user already supplied an unambiguous scope. Record excluded groups
  as out of scope; do not silently omit them.
- **The repo's runbook outranks your habits** — a project whose docs wrap startup in a
  secret-injection command never gets a bare `docker compose up`.
- **Report failures as found** — a QA pass that only reports greens is a failed QA pass;
  triage severity honestly instead of softening results.
