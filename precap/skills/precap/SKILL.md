---
name: precap
description: Write a precap — the recap of a task as if it were already finished — before or during long-running work, then use it to check whether the run is still on the imagined path. Use when the user says "/precap", "precap this", "write the precap", "what will this take", "imagine this is done", or "are we still on track"; at the start of any task expected to span many steps, hours, or sessions; on resume when a precap.md exists in the working directory; and whenever a long-running agent needs to verify it is doing what it set out to do. It investigates the repo, plan, and git state so the imagined path is grounded rather than guessed. Not a substitute for plan approval or for asking the user about scope.
---

# /precap — the recap written before the work

A precap is a recap from the future. You imagine the task is completely done, then write the
account of the work it took and the end result, in past tense, as concretely as a real recap.
Long-running agents lose the thread: context compacts, subtasks multiply, a detour starts to
look like the task. A precap gives your future self a fixed reference to compare against. It
does not predict the future; it records the imagined path, concretely enough that leaving it is
detectable and revising it is explicit.

The difference between a precap and an educated guess is investigation. Every step in the path
must point at something real in the repo, plan, or history. A step with no grounding is a hope.

## Modes

- **write** (default): no `precap.md` for this task yet. Investigate, imagine, write, validate.
- **check**: a precap exists and work is under way. Compare reality against it and report.
- **revise**: reality diverged. Amend the precap without erasing what was imagined.

If `precap.md` already exists in the working directory and the user did not say which mode,
read it first and default to **check**. Overwriting a live precap loses the drift history that
makes it useful.

## Phase 0 — Anchor the task

Establish what "done" means before imagining how it got done.

1. Capture the task statement verbatim: the user's words, the plan file, the issue, the PR
   description. It goes into the precap unedited so a later reader can see whether the
   interpretation drifted from the ask.
2. Read any plan that already exists: a plan-mode file, `.cascade/<project>.md`, a design or
   RDD doc, a Linear ticket. The precap sits downstream of the plan and must not contradict it
   silently. Where it does, say so.
3. Note the working directory, branch, and base. Run `git status --short` and
   `git log --oneline -10`. A dirty tree or a branch already carrying commits changes the story:
   part of the imagined path may already have happened.
4. Decide the scope questions that are the user's to make. If the task forks on something only
   they can settle (which API to keep, whether to migrate data, whether to touch a shared
   contract), use `AskUserQuestion` now with concrete options. Do not write a fork as decided
   when it is not. In Codex, use the equivalent structured user-input tool when available.
   When no user is reachable (a headless run, a cascade worker, a scheduled job), do not
   block: follow the branch that changes the least, write the fork with
   `Decided by: the user (undecided)`, and add a Drift signal that fires if the run starts
   acting on the other branch.

Done when: the verbatim task, the plan it descends from, and the git starting point are written
down.

## Phase 1 — Investigate

This phase is what makes the precap predictive. Spend effort proportional to the task: a small
change earns ten to twenty tool calls, a multi-session feature earns a real survey. Stop when
every step you intend to write can cite a file, a command, a commit, or a prior session. The
full checklist with the reason behind each item is in
[references/investigation.md](references/investigation.md). The short version:

- **Open the files the change lands in.** Names are not enough. Read the functions, the types,
  the tests that already cover them. The footprint you predict comes from what you read.
- **Learn how the repo verifies itself.** Test layout, the exact test command, lint, type
  check, CI workflows. These become the verification steps the finished run ran.
- **Measure the blast radius.** Grep the callers and importers of what you will change and
  count them. The count bounds the footprint and tells you where the second-order edits are.
- **Read the area's history.** `git log --oneline -15 -- <path>` shows prior attempts, the
  reviewers' patterns, and things that were tried and reverted.
- **Search prior sessions** with `$recall` or `/recall` when it is installed: one query built
  from the repo path and the task's key nouns, top five results, and stop there if the top hit
  is not about the same path. A precap that repeats last week's dead end is worse than none,
  but a search that returns noise is not worth a second try.
- **Convert every unverified belief into a tagged assumption.** Anything you could not confirm
  goes in the Assumptions section as `[unverified]`. Never let it pose as a fact in the Path.

Done when: you can name the files that change, the commands that verify, and the places the
path may fork, and each one is backed by something you actually read.

## Phase 2 — Imagine the finished run

Write it as a recap of work already done. Past tense throughout. The rules that keep it useful:

- **Concrete.** Name files, functions, commands, and counts. "Updated the tests" is a guess;
  "added three cases to `tests/test_registry.py` covering the empty, duplicate, and stale
  paths" is a precap.
- **Grounded.** Each step carries `Grounded in:` pointing at what you read in Phase 1.
- **Checkpointed.** Each step carries `Checkpoint:`, a fact a future reader can verify to
  confirm the step really happened. Prefer observable state (a file exists, a test passes, a
  command exits 0) over feelings of progress.
- **Forked where uncertain.** When two paths were genuinely possible, write both branches and
  the decision rule that picks one. Do not average them into a vague middle.
- **Bounded.** State what the run did not do. Scope creep is the most common way a long run
  drifts, and an explicit "not done" list is the cheapest defense.
- **Short.** Budget by footprint: about 100 words per file the run touches, plus 300 for the
  fixed sections, capped at 1,500. A four-file change lands near 700; only a multi-session
  feature earns the cap. A precap that is not re-read is decoration.

## Phase 3 — Write precap.md

Write to `precap.md` in the working directory unless the user names a path. Use the exact
section headings below; `scripts/precap.py validate` checks them so a future reader can rely on
the structure. The script lives in this skill's `scripts/` directory; resolve that to an
absolute path once and reuse it, since the working directory in a long run is rarely the
skill directory. Get a blank copy with:

```bash
python3 <skill-dir>/scripts/precap.py template > precap.md
```

Fill the `Workdir:` header with the repository root. Without it the checker falls back to the
git toplevel of the precap's own directory, which is wrong when the precap lives outside the
repo.

Sections, in order:

1. **Task**: the verbatim ask, then your one-paragraph interpretation.
2. **End state**: what exists when the work is done and how a reader confirms it. This is the
   acceptance check in prose.
3. **Path**: numbered steps in past tense. Each has `Grounded in:` and `Checkpoint:` lines.
   Cite files as `` `path:line` `` or `` `path:start-end` ``; the checker verifies the file
   exists and the range fits, so a fabricated citation fails.
4. **Footprint**: every path the run touched, tagged `(new)`, `(modified)`, or `(deleted)`,
   with a few words on why. `drift` compares this list against git.
5. **Verification**: the commands the finished run ran and what they showed.
6. **Forks**: where the path could split, the branches, and a `Decided by:` rule on every
   bullet. A fork that is the user's call says `Decided by: the user (undecided)`.
7. **Not done**: explicit out-of-scope items and the reason each stayed out.
8. **Assumptions**: one bullet per belief, each tagged `[verified]` or `[unverified]`.
9. **Drift signals**: "if you find yourself doing X, you have left the path" statements, with
   what to do about it. These are the lines a tired future self needs most.
10. **Revisions**: empty when first written. Filled by revise mode.

Then validate:

```bash
python3 <skill-dir>/scripts/precap.py validate <path-to>/precap.md
```

Validation fails closed on a missing section, a step without grounding or a checkpoint, a
grounding path that does not exist or a line range past the end of the file, a fork without
`Decided by:`, an untagged assumption, a `Base:` git cannot resolve, a footprint path tagged
`(modified)` that does not exist, or one tagged `(new)` that already does. Fix the precap; do
not weaken the validator.

Whether `precap.md` is committed is the user's call. By default leave it untracked and mention
`.git/info/exclude` if they want it invisible to git without touching `.gitignore`.

## Check mode — is the run still on the path?

Run this at every checkpoint you reach, on resume after compaction or a new session, and any
time you notice you cannot say which step you are on:

```bash
python3 <skill-dir>/scripts/precap.py drift <path-to>/precap.md --json
```

`drift` compares the Footprint against `git diff --name-only <base>` plus the working tree and
reports three sets: predicted and touched, predicted and still pending, touched but never
predicted. By default it looks only inside the top-level directories the Footprint names, so
unrelated dirty files in a monorepo do not drown the signal; the count of changes outside that
scope is reported separately. Pass `--scope <prefix>` to widen or narrow it, or `--all` for
the whole tree. It is a structural signal only. You supply the semantic judgment:

- Walk the Path and mark each step done, in progress, or not started using its Checkpoint.
- Compare the current git state with End state. Ahead, on path, or behind?
- Read the Drift signals. If one fires, say so plainly.

Report in this shape: current step, checkpoints confirmed, footprint drift (three counts),
and a verdict of `on path`, `ahead`, `drifted at step N`, or `blocked`. Drifted is not the
same as wrong. It means either reality differed from what was imagined, which calls for revise
mode, or the run wandered, which calls for returning to the path. Decide which, and say which.

## Revise mode — amend without erasing

When reality diverges, the precap must change, but the divergence itself is information.

1. Append an entry to Revisions: date, the step or section affected, what differed, and why.
2. Update the affected sections. Strike through superseded text or annotate it rather than
   deleting it, so the original imagination stays legible next to the correction.
3. Move any assumption that reality settled from `[unverified]` to `[verified]` or to a
   Revisions entry explaining it was wrong.
4. Re-run `validate`.

A precap with several honest revisions is doing its job. One that was silently rewritten to
match whatever happened is a recap with the date faked.

## Answer shape

After writing, reply with the precap path, the End state paragraph, the forks that need a
decision, and the count of `[unverified]` assumptions. After a check, reply with the verdict
and the step you are on. Keep private repo content out of the reply when the user is in a
shared channel.

## Hard rules

- Never write a precap without opening the files the Path names. A precap written from file
  names alone is the educated guess this skill exists to replace.
- No `Grounded in:`, no step. Cut it or go investigate.
- A fork that depends on the user is asked, not decided. Do not bury a scope decision in past
  tense.
- Do not overwrite an existing precap in write mode without the user saying so. Check or
  revise instead.
- The precap never records hidden reasoning or claims that predictions came true. It records
  what was imagined, what was verified, and what changed.
- Read [references/eval-contract.md](references/eval-contract.md) before judging whether a
  precap is good enough to hand to a long-running agent.
