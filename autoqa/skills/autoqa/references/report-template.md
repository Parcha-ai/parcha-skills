# autoqa report template

```markdown
# autoqa report — <repo> @ <branch/commit> on <instance>

**Date:** <iso date>
**Target repo:** <absolute path or URL>
**Target instance:** <url>
**Target commit:** <full sha>
**Deployed commit:** <full sha>
**Target verified:** YES | NO
**Selected scope:** <groups selected by the user>
**Chrome DevTools required:** YES | NO
**Cleanup:** PASS | NOT REQUIRED
**Cleanup witness:** autoqa-evidence/cleanup.txt
**Verdict:** SHIP | SHIP WITH CAVEATS | DO NOT SHIP | BLOCKED

## Bottom line

<One paragraph for the release owner explaining the verdict.>

## Verdict table

| # | Source | Feature | Required | Disposition | Depth | Modality | Entry point | Check | Result | Witness |
|---|--------|---------|----------|-------------|-------|----------|-------------|-------|--------|---------|
| 1 | BASE | Health/boot | YES | SMOKE | SMOKE | API | GET /health | → 200 | PASS | autoqa-evidence/01-health.txt |
| 2 | DIFF | <changed behavior> | YES | DEEP | LIVE-E2E | UI | <button/route> | <check> | PASS | autoqa-evidence/02-ui.png; autoqa-evidence/02-network.json |
| 3 | BASE | <advisory feature> | NO | UNTESTED | LIVE-E2E | API | <traced> | <check> | UNTESTED | autoqa-evidence/03-fixture-attempt.txt |
| 4 | BASE | <dead feature> | NO | SKIPPED | SMOKE | UI | none found | — | SKIPPED | autoqa-evidence/04-unreachable.txt |

Results are exactly PASS / FAIL / UNTESTED / SKIPPED. Required rows are ship gates.
`Disposition=DEEP` requires `Depth=LIVE-E2E`. List multiple witness paths with semicolons.
Every witness resolves relative to this report and shows the asserted result. An UNTESTED
witness must document a real fixture attempt and makes the verdict BLOCKED.

## Coverage arithmetic

- BASE: <N discovered> / <M rows> / <K untested>
- DIFF: <N discovered> / <M rows> / <K untested>
- Total: <N discovered> / <M rows> / <K untested>

## Failures — triage

| # | Failure | Severity | Cause |
|---|---------|----------|-------|
| 2 | <what broke> | release blocker / env quirk / test bug / dead code | <cause and entry point> |

## Fixture and cleanup notes

<Run ID, content-free fixture IDs, creation attempts, reverse-order cleanup, retained
provenance required by the repo contract. Never include secrets or customer content.>

## Instance health after run

<Health output; anything restarted and re-verified.>
```
