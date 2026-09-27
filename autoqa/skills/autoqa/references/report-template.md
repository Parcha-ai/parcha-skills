# autoqa report template

```markdown
<!-- agent-sticky:autoqa-report -->
# autoqa report — <repo> @ <branch/commit> on <instance>

**Head:** `<full 40-hex commit under test>`
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

<details><summary>Earlier heads</summary>

- `<short sha>` <iso time> <verdict>

</details>
```

The first line is the marker a caller uses to find this report on a pull request and edit it
in place. `Head:` names the commit the verdicts belong to; a reader checks it against the PR
head instead of trusting the comment's timestamp. `Earlier heads` keeps one line per earlier
run of the same PR, newest first, at most ten; omit the block on the first run.

## Publishing to a pull request

Edit the existing report comment; create one only when none exists. `BOT_LOGIN` is the
authenticated `gh` identity (`gh api graphql -f query='{viewer{login}}' --jq .data.viewer.login`).
`ROSTER` is the JSON list of agent identities that may own the report, so an agent that takes
over a PR edits the report another agent posted. It defaults to this identity alone.
`HEAD_SHA` is the commit the report describes.

```bash
ROSTER="${ROSTER:-[\"$BOT_LOGIN\"]}"
MARKER="<!-- agent-sticky:autoqa-report -->"

PR_HEAD="$(gh pr view "$PR" -R "$REPO" --json headRefOid --jq .headRefOid)"
[ "$HEAD_SHA" = "$PR_HEAD" ] || { echo "stale: report is for $HEAD_SHA, PR head is $PR_HEAD"; exit 1; }

COMMENT_ID="$(gh api --paginate "repos/$REPO/issues/$PR/comments?per_page=100" \
  --jq ".[] | select((.user.login | IN($ROSTER[])) and (.body | startswith(\"$MARKER\"))) | .id" \
  | head -n1)"

if [ -n "$COMMENT_ID" ]; then
  gh api --method PATCH "repos/$REPO/issues/comments/$COMMENT_ID" \
    -F body=@autoqa-report.md --jq .html_url
else
  gh pr comment "$PR" -R "$REPO" --body-file autoqa-report.md
fi
```

- Never write a report for a head the PR has moved past; the guard above refuses it.
- Search every page right before writing; an ID read earlier may be stale. The oldest
  matching comment is the report; after writing, list again and follow the race rule in
  `review-loop`'s query reference if more than one exists.
- `-F body=@file` reads the body from the file. Never pass a multi-line body inline.
- Do not use `gh pr comment --edit-last`: it edits the identity's latest comment of any kind,
  which may be a review trigger or a thread answer.
- An edit sends no notification. When a result needs a person's attention, reach them
  through the caller's notify path, not a second PR comment.
