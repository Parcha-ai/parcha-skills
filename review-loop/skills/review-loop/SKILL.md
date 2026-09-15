---
name: review-loop
description: Drive every reviewer thread on a GitHub pull request to zero unresolved in bounded iterations. Covers bot reviewers such as Greptile and Devin and human reviewers. Fetches every inline comment, review, and issue comment, fixes what is actionable, replies, resolves bot threads through GraphQL, re-requests review once per push, and ends with a fixed-format report. Use when the user says "review loop", "address the review comments", "get this PR to zero unresolved", "greploop", or "make Greptile happy".
license: MIT
compatibility: Requires git and an authenticated gh CLI. Greptile and Devin sections apply only when those apps are installed on the repository.
metadata:
  upstream: greptileai/skills (greploop, greploop-apps)
  vendored: 2026-09-15
---

# Review loop

This skill has one job: take a GitHub PR with open review threads and drive every thread, from
every reviewer, to zero unresolved, in a bounded number of iterations, then report what it did
in a fixed format. It does not QA the running application; that is `autoqa`'s job. It does not
decide whether the diff is safe beyond what reviewers raised; that is `blast-radius`'s job. It
does not merge, and it does not mint credentials.

## Inputs

| Input | Required | Default |
|---|---|---|
| Repository (`owner/repo`) | no | The `origin` remote of the current checkout |
| PR number | no | The PR for the current branch (`gh pr view`) |
| Trigger comment text | no | `@greptile-apps review` |
| `--max-iterations N` | no | 3 |
| Gate command | no | None. The caller or the repository supplies the command that runs tests and lint (for example the repo's `make check`); the skill only says where in the loop it runs |
| Base-branch sync | no | None. The caller or repository supplies the rebase or merge policy; the skill says where it runs |

Auth precondition: the caller has authenticated `gh` before invoking this skill. The skill
never mints, reads, or stores tokens. If `gh auth status` fails, stop and report it.

## Iteration

Repeat at most `--max-iterations` times. Each iteration:

1. Sync with the base branch if the caller supplied a sync policy. Run the gate command. A
   red gate stops the iteration; fix the gate before touching review comments.
2. Fetch every open thread. See "Fetching everything". Do not filter to one reviewer.
3. Triage by content, not by author. For each thread decide: actionable (a code change is
   needed), informational (no change, reply explains why), or false positive (reply explains
   why). Record the decision.
4. Fix every actionable item in one pass. Read the file at the cited line, understand the
   comment in context, make the change.
5. Run the gate command again. Do not push a red gate.
6. Commit and push. Commit message names the iteration, for example
   `Address review feedback (review-loop iteration 2)`.
7. Reply and resolve. See "Human threads" and "Bot threads". A reply goes on the thread that
   raised the point, not in a new top-level comment.
8. Re-request review once per push. See "Greptile specifics" and "Devin specifics". Wait for
   the results with a bounded poll.
9. Check the exit criterion. Zero unresolved threads across every reviewer, and, when Greptile
   is installed, a 5/5 score in its most recently updated summary. If met, stop and report.

If the cap is reached with threads still open, stop and report them; do not start another
iteration.

## Fetching everything

Fetch all three comment sources on every iteration; a reviewer can use any of them.

```bash
# Inline review threads with resolution state (GraphQL; paginate on endCursor)
gh api graphql -f query='...' # see references/graphql-queries.md

# Reviews (approve, request changes, comment) with bodies
gh api --paginate "repos/{owner}/{repo}/pulls/<PR_NUMBER>/reviews?per_page=100"

# Issue comments on the PR conversation tab (Greptile summaries, Devin notes, humans)
gh api --paginate "repos/{owner}/{repo}/issues/<PR_NUMBER>/comments?per_page=100"
```

A thread is open when `isResolved` is false. A review body or issue comment is a thread when it
asks for a change or a reply; treat it as open until it is answered. Bot summaries that edit in
place (Greptile does this) must be read by `updated_at`, not `created_at`.

## Human threads

- Reply on the thread with what was done and why, citing the commit. When nothing was
  changed, say what was considered and why the code stays as is.
- Never resolve a thread a human opened without replying first. Prefer to leave resolution of
  human threads to the human unless the repository's policy says the author resolves.
- Never dismiss a human review. A "changes requested" review is cleared by the reviewer
  re-reviewing, and re-requesting review is the only action the loop takes on it.

## Bot threads

Resolve a bot thread after its item is fixed or answered. Use the GraphQL
`resolveReviewThread` mutation, batched with aliases, and assert the result:

```bash
gh api graphql -f query='
mutation {
  t1: resolveReviewThread(input: {threadId: "ID1"}) { thread { isResolved } }
  t2: resolveReviewThread(input: {threadId: "ID2"}) { thread { isResolved } }
}'
```

Every alias in the response must report `isResolved: true`. If one does not, re-fetch the
thread and treat it as still open; do not count it as resolved. Batch up to 20 aliases per
request. Full queries are in `references/graphql-queries.md`.

## Greptile specifics

- Before posting a trigger comment, check for a run already in progress. Look at
  `gh pr checks <PR_NUMBER> --json name,state` and at
  `repos/{owner}/{repo}/commits/<HEAD_SHA>/check-runs` for a check whose name matches
  `greptile` (case-insensitive). If its state is `PENDING` or `IN_PROGRESS`, do not post; wait
  for it.
- The trigger comment text is configurable; the default is `@greptile-apps review`. Post it at
  most once per push.
- Poll the check run at 10-second intervals for up to 10 minutes. On large PRs the tagged
  review may never create a check run for the new head; Greptile instead edits its existing
  summary comment. If no check run appears after a few attempts, poll the most recently updated
  Greptile issue comment and stop when its `updated_at` is later than the trigger comment and its
  body carries a score.
- Read the score from three places and use the most recently updated one: the most recently
  updated Greptile-authored issue comment (it edits in place, so sort by `updated_at`), the PR
  body, and the latest review from `greptile-apps[bot]` or `greptile-apps-staging[bot]`. The
  score looks like `3/5`, `5/5`, or `Confidence: 3/5`.
- Carry forward the items under "Prompt to fix all with AI" in the Greptile summary comment,
  even when the inline comment endpoint returns zero unresolved comments. They count as open
  until fixed or answered.
- Exit criterion for Greptile: `5/5` and zero unresolved Greptile threads.

## Devin specifics

- Devin reviews when the PR becomes ready for review. A draft PR never gets a Devin review.
  If the PR is a draft and the caller wants Devin's review, mark it ready (`gh pr ready`) and
  say so in the report.
- Devin posts inline threads and a summary issue comment. Triage them like any other thread;
  reply and resolve through the same GraphQL path.
- Re-requesting Devin is a push; there is no trigger comment.

## Stop on timeout

If a poll for review results times out, stop the loop and report the timeout. Never continue
with stale or missing review results, and never count a thread as resolved because the reviewer
did not answer.

## Report

End with this table. Print it even when the loop stops early.

| Field | Value |
|---|---|
| Repository / PR | `owner/repo#N` |
| Iterations | N of max M |
| Threads found (per reviewer) | Greptile N, Devin N, humans N |
| Resolved this run | N |
| Replied, left open for a human | N |
| Remaining | N, then one line each: `path:line` and a short quote |
| Final Greptile score | X/5, or "not installed" |
| Stop reason | exit criterion met, max iterations, timeout, red gate, auth failure |

Write the report and every reply through `unslop` before posting.
