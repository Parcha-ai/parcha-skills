# autoqa report template

```markdown
# autoqa report — <repo> @ <branch/commit> on <instance>

**Date:** <iso date>  **Instance:** <url>  **Evidence:** <evidence dir>

## Bottom line

<One paragraph: ship / don't-ship / ship-with-caveats / blocked, and why. Written for the
release owner, no jargon invented during the run. If BLOCKED, state what failed to boot.>

## Verdict table

| # | Source | Feature | Modality | Entry point | Check | Result | Witness |
|---|--------|---------|----------|-------------|-------|--------|---------|
| 1 | BASE | Health/boot | API | GET /health | → 200 | PASS | evidence/01-health.txt |
| 2 | DIFF | <changed behavior> | UI | <button/route> | <check> | FAIL | evidence/02-<slug>.png |
| 3 | BASE | <feature> | — | <traced> | <not run> | UNTESTED | needs <fixture> |
| 4 | DIFF | <feature> | — | none found | — | SKIPPED | unreachable (dead code) |
| … | | | | | | |

Results are exactly PASS / FAIL / UNTESTED / SKIPPED. Every row has a witness path that
resolves and shows the asserted result. Record the user's selected execution groups. State
coverage arithmetic separately for baseline and diff inventories, then total it; say whether
this is full-catalog coverage or a scoped pass.

## Failures — triage

| # | Failure | Severity | Cause |
|---|---------|----------|-------|
| 2 | <what broke> | release blocker / env quirk / test bug / dead code | <one line, incl. the user entry point that reaches it> |

## Coverage notes

- Features in inventory but UNTESTED, and why (fixture missing, env can't reach, …).
- Modalities skipped and why (no browser tooling in session, …).

## Instance health after run

<health check output; anything restarted and re-verified>
```
