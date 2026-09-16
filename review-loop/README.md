# review-loop

Drives every reviewer thread on a GitHub pull request, from bots such as Greptile and Devin and
from humans, to zero unresolved in bounded iterations. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills/review-loop)

One job: fetch every inline thread, review, and issue comment on the PR; triage by content;
fix what is actionable; reply on every thread; resolve bot threads through the GraphQL
`resolveReviewThread` mutation and assert `isResolved: true`; re-request review once per
push; stop at zero unresolved or at `--max-iterations` (default 3). It ends with a
fixed-format report. It does not QA the running app (`autoqa`), does not assess what the diff
could break beyond what reviewers raised (`blast-radius`), does not merge, and never mints
credentials: the caller has authenticated `gh` before invoking it.

Rules the loop keeps:

- Human threads get a reply saying what was done and why. A human-opened thread is never
  resolved without a reply, and a human review is never dismissed.
- Greptile: check `gh pr checks` and the commit's check runs for a run in progress before
  posting the trigger comment (default `@greptile-apps review`, configurable); read the score
  from the most recently updated Greptile comment (it edits in place), the PR body, and the
  reviews; carry forward its "Prompt to fix all with AI" items even with zero inline comments;
  exit at 5/5 with zero unresolved.
- Devin reviews on ready-for-review; a draft never gets a review.
- Gate commands (tests, lint) and base-branch sync come from the caller or the repository; the
  skill only says where in the loop they run.
- A timed-out poll stops the loop. It never continues with stale or missing results.

## Install

skills.sh:

```bash
npx skills add Parcha-ai/parcha-skills --skill review-loop
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install review-loop@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add review-loop@unc-skills
```

In pi, invoke it with `/skill:review-loop`.

## Use

```text
/review-loop                                   PR for the current branch, 3 iterations
/review-loop <PR number> --max-iterations 5    a named PR with a higher cap
```

In Codex, use `$review-loop`. Inputs: repository, PR number, trigger comment text, max
iterations, gate command. The GitHub GraphQL and REST calls are in
`skills/review-loop/references/graphql-queries.md`.

## Provenance

- Upstream: [greptileai/skills](https://github.com/greptileai/skills), skills `greploop`
  and `greploop-apps` (version 1.3), author Greptile AI, MIT. Vendored on 2026-09-15 by way of
  [michaelshimeles/skills](https://github.com/michaelshimeles/skills), where `greploop-apps`
  is a local variant of `greploop`.
- License: MIT. The upstream `LICENSE` file is included verbatim in this package directory.
- Modifications made here:
  - Merged `greploop` and `greploop-apps` into one skill named `review-loop` and added
    `license: MIT` and provenance metadata to the frontmatter.
  - GitHub only. Dropped all GitLab and Perforce material, including
    `references/gitlab-api.md`, the platform detection step, and the `glab` and `p4` commands.
  - Generalized from one reviewer to every reviewer: fetches every inline thread, review, and
    issue comment and triages by content instead of filtering to Greptile.
  - Added the human-thread rules (reply with what and why, never resolve without a reply, never
    dismiss a review) and a Devin section.
  - Kept the Greptile specifics: in-progress check before triggering, configurable trigger
    text defaulting to `@greptile-apps review` (the `greploop-apps` variant), score read by
    `updated_at`, "Prompt to fix all with AI" carry-forward, 5/5 exit criterion, and the
    huge-PR fallback that polls the edited summary comment.
  - Thread resolution asserts `isResolved: true` on every alias in the batched mutation.
  - `--max-iterations` default lowered from 10 to 3.
  - Gate commands and base-branch sync are supplied by the caller; the upstream had no gate step.
  - Report replaced with a fixed-format table (iterations, per-reviewer counts, resolved,
    remaining with `file:line`, final Greptile score, stop reason).
  - `references/graphql-queries.md` keeps the upstream GitHub queries and adds thread fields
    (`isOutdated`, `path`, `line`), the reply endpoint, the reviews endpoint, and the bounded
    poll.
