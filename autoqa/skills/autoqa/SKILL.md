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
   repo's reusable **baseline**, not the complete plan: how to run, how to auth, optional
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

Build the plan from these sources:

1. **Required smoke.** Health for every run, plus auth when authenticated code changed.
2. **Diff inventory.** Cases derived from the actual change under test. Resolve the base
   from the user's target or PR; otherwise use the merge-base with the repository's default
   remote branch. Include committed, staged, unstaged, and relevant untracked changes. Read
   the diff and the acceptance/design docs it changes or cites. Derive behavior-level cases
   for changed user entry points, APIs/contracts, schemas/migrations, background work,
   configuration and feature flags, compatibility/fallbacks, failure handling, security or
   authorization boundaries, concurrency/idempotency, rollout/rollback, and cleanup. Do not
   mistake a large unit-test list for this inventory. Include changed entry points and
   directly affected downstream paths.
3. **Optional baseline inventory.** Unaffected catalog checks from `AUTOQA.md` or Phase 1
   only when the caller asks for full release or catalog QA.

Write the selected inventories as numbered lists, preserving their source (`BASE` or `DIFF`).
Then build one matrix: one row per inventory item, columns = source, modality, entry point,
check, pass criterion, witness to capture. Deduplicate overlapping rows without dropping the
stronger pass criterion. The matrix must have at least one row per item. The report states
separate and total coverage arithmetic.

Apply domain checks only when the diff affects that domain. Run UI checks when the diff
affects a UI entry point. Run research end-to-end checks when research execution changes.
Run storage or billing checks when storage or billing changes. Backend-only changes require
API checks. Run CLI checks only when the diff affects a CLI entry point or the CLI is needed
to prove the changed behavior.

State execution budgets in the plan: maximum wall time, external jobs, synthetic resources,
and UI sessions. Move-only work defaults to one preview and one pass over affected paths.
Do not launch deep research unless the diff changes research execution.

### Confirm execution scope with the user

Scope comes from the first of these that exists: an explicit instruction from the caller
("smoke only", "full release QA"), a scope the repo's `AUTOQA.md` pre-selects for
unattended runs, or a question to the user. When either of the first two applies, record it
in the plan and do not ask; a repo that runs this skill from a pipeline pre-selects its scope
in `AUTOQA.md` precisely so the run never blocks on a prompt.

Otherwise, after drafting the inventories and before executing, use `AskUserQuestion` with multiple
choice and `multiSelect: true` to ask what the user wants included. In Codex environments,
use the equivalent structured user-input tool when available. Populate the choices from the
actual repo and diff, rather than showing a generic checklist. Offer up to four concise
groups such as:

- **Changed behavior + seams (Recommended)** — every DIFF case and its nearest regressions.
- **Full baseline catalog** — unaffected baseline features in addition to changed seams.
- **Stateful/destructive cases** — migrations, deletes, lifecycle, imports, billing, or
  external writes; name the exact synthetic/isolated safeguards in the description.
- **Performance/soak or platform matrix** — only when the diff makes it relevant.

Health is always included. Include auth only when authenticated code changed. Treat the
selection as test scope, not authorization to mutate production or real customer data. If
the user already explicitly selected scope, do not ask a redundant question. Record that
choice in the plan.
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
- **Modalities**: API (endpoint calls), UI (browser tooling), CLI (the repo's own binaries).
  Match them to the diff. Do not add UI or CLI checks to a backend-only change unless they
  are needed to prove the changed behavior.
- **Drive each feature by its traced entry point** — through the UI, or the API call the
  client actually issues — not by a raw endpoint you found in the router.
- **Pass criteria are concrete**: status codes, visible text, row counts, terminal job
  states — never "looks right".
- **Diff cases are additive.** `AUTOQA.md`, a feature tracker, or a prior report can never
  suppress a test implied by the current diff. A prior PASS is context, not a witness for
  the current run.
- Mark continuation, replay, child-dispatch, and other stateful session checks as
  `session-sensitive`. Give each row the run ID and require a session created for that run. Its
  setup witness must show exactly zero turns and zero children before the first action. The
  first launch appends exactly one turn and one child,
  a continuation appends exactly one turn and one child after them, and replay appends nothing
  and returns the same IDs in the same order.
- Order rows: boot and health first, auth when required, then affected paths and regressions.

Done when: the selected inventories exist, the matrix has at least one row per item, every
row carries all required columns, every executable row names a traced entry point, and the
user's selected execution scope and budgets are recorded.

## Phase 3 — EXECUTE

Execute only rows whose entry point was traced in PLAN. Run the matrix top to bottom
against the live instance.

- Instance not up? Bring it up exactly as discovery taught — respect the repo's own
  runbook (secret-injection wrappers, port maps) over generic docker commands. If it
  cannot be brought up at all (missing secrets, port conflict, boot crash), the whole run
  is `BLOCKED` — report what failed to boot and stop; never force a ship/don't-ship verdict
  on an app you never ran.
- Before a session-sensitive row, create a fresh run-ID-scoped session through the traced entry
  point. Query it before the first action and assert exactly zero turns and zero children. After
  the first launch, assert one turn and one child. After continuation, assert two turns and two children
  with the original pair first. Replay must leave both counts and the complete ID order unchanged. A non-empty
  starting session or an approximate count fails the row. Never reuse a session from a prior
  row or run.
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
- Record each created resource's exact ID and cleanup result in the report. Run a separate
  cleanup audit only when cleanup fails, the diff changes cleanup behavior, or a created
  resource remains.
- Report shared capacity or provider failures as `BLOCKED_INFRA` for the affected rows.
  Keep completed scoped results intact. State review readiness separately from any merge
  policy that treats the infrastructure failure as blocking.

### Before / after evidence

Capture before/after evidence only when observable behavior changed. Compare that behavior
with the clearest observable evidence. UI changes use screenshots. Non-UI changes use contract
output, status and body shape, OpenAPI, logs, or an equivalent observable result.
Behavior-preserving work needs one contract-equivalence witness, not an artificial pair. Do not
require screenshots when behavior must not change.

For UI changes, pair the branch screenshot with the same view from a running base instance
when one exists:

- The base instance is one the caller names or the repo config lists (a main preview,
  staging, or production), reached read-only. Never manufacture a "before" by switching
  branches, stashing, or starting a second server; with no base instance, record
  `before: none (no base instance)` on the row and move on.
- Capture the "before" with the same browser tooling the UI rows use, at the same route,
  viewport, and element, and save it beside the "after" as `<row>-before.png` and
  `<row>-after.png` under the evidence dir.
- A pair presents the change; it does not judge it. The row's PASS or FAIL still comes from
  its own check, never from the two images looking different.
- Publishing the images somewhere a PR body can render them (an assets branch, a comment
  attachment, the repo's own upload path) is the caller's job with the repo's tooling; the
  report lists the local pair and the two instance URLs plus the "after" commit SHA.

Done when: every matrix row is PASS, FAIL (with cause), UNTESTED (with reason), SKIPPED, or
BLOCKED_INFRA. Each row has a witness that shows the asserted result. Every row with changed
observable behavior names its before/after evidence or records
`before: none (no base instance)`. Behavior-preserving rows name their equivalence witness.

## Phase 4 — REPORT

Write the report from the template in
[references/report-template.md](references/report-template.md): verdict table (feature,
modality, result, witness path), the Before / After table for changed rows, failure triage
(release blocker vs env quirk vs test bug), and the one-paragraph bottom line a release owner
can act on. The Before / After table is written so a caller can lift it into a PR body once
the images are published.

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
- **Scope follows the diff by default.** Always check health. Add auth and domain checks
  only when the diff affects them. Run the full baseline catalog only when the caller asks.
- **Scope is an explicit choice, never a guess** — from the caller, from the repo's
  `AUTOQA.md`, or from a structured multi-select question after planning, in that order.
  Record excluded groups as out of scope; do not silently omit them.
- **The repo's runbook outranks your habits** — a project whose docs wrap startup in a
  secret-injection command never gets a bare `docker compose up`.
- **Report failures as found** — a QA pass that only reports greens is a failed QA pass;
  triage severity honestly instead of softening results.
