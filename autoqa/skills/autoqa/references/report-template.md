# autoqa report template

```markdown
# autoqa report — <repo> @ <branch/commit> on <instance>

**Date:** <iso date>  **Instance:** <url>  **Evidence:** <evidence dir>

## Bottom line

<One paragraph: ship / don't-ship / ship-with-caveats / blocked, and why. Written for the
release owner. If infrastructure blocked a row, preserve completed results and state review
readiness separately from merge policy.>

## Verdict table

| # | Source | Feature | Modality | Entry point | Check | Result | Witness |
|---|--------|---------|----------|-------------|-------|--------|---------|
| 1 | BASE | Health/boot | API | GET /health | → 200 | PASS | evidence/01-health.txt |
| 2 | DIFF | <changed behavior> | UI | <button/route> | <check> | FAIL | evidence/02-<slug>.png |
| 3 | BASE | <feature> | — | <traced> | <not run> | UNTESTED | needs <fixture> |
| 4 | DIFF | <feature> | — | none found | — | SKIPPED | unreachable (dead code) |
| … | | | | | | |

Results are exactly PASS / FAIL / UNTESTED / SKIPPED / BLOCKED_INFRA. Every row has a
witness path that resolves and shows the asserted result. Record the user's selected
execution groups. State coverage arithmetic separately for baseline and diff inventories,
then total it. Say whether this is full-catalog coverage or a scoped pass.

For each session-sensitive row, record the run ID, fresh session ID, zero-state witness, and
the exact expected and observed turn/child counts and order after every action.

## Before / After

Include rows only when observable behavior changed. For behavior-preserving work, record one
contract-equivalence witness instead. Use `before: none (no base instance)` when applicable.

| Check | Before | After |
|:--|:------:|:-----:|
| <row label> | <screenshot, contract, status/body shape, OpenAPI, log, or equivalent> | <same evidence type> |

Before: <base instance url> | After: <instance url> at `<after commit sha>`

Use screenshots for UI changes. Do not require them for non-UI or behavior-preserving changes.

## Failures — triage

| # | Failure | Severity | Cause |
|---|---------|----------|-------|
| 2 | <what broke> | release blocker / env quirk / test bug / dead code | <one line, incl. the user entry point that reaches it> |

## Coverage notes

- Features in inventory but UNTESTED, and why (fixture missing, env can't reach, …).
- Modalities skipped and why (no browser tooling in session, …).

## Created resources and cleanup

| Resource | ID | Cleanup result |
|---|---|---|
| <type> | <exact ID> | removed / remains / cleanup failed |

Run another cleanup audit only when cleanup failed, cleanup behavior changed, or a resource
remains.

## Instance health after run

<health check output; anything restarted and re-verified>
```
