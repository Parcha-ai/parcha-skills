# Investigation checklist

The precap is only as predictive as what you read before writing it. Each item below names the
question it answers and why a skipped item shows up later as drift.

## 1. Where does the change land?

Open the files, not the tree listing. For each entry point the task touches, read the function
bodies, the types they take and return, and the nearest tests. Note line numbers; they go in
`Grounded in:`. Why: the footprint, the step order, and the second-order edits all fall out of
what the code actually looks like. A precap written from names alone predicts the wrong files.

## 2. How does the repo verify itself?

Find the exact commands: `package.json` scripts, `Makefile`, `pyproject.toml`, `tox.ini`, CI
workflows under `.github/workflows`, a `CLAUDE.md` or `AGENTS.md` that names a test recipe.
Run the existing suite once if it is cheap so the precap can say what "green" looked like at
the start, and note whether CI runs that suite at all: "green locally" and "green in CI" are
different verification claims, and a package with no workflow only ever gets the first. Why: the Verification section must list commands that exist and their expected
result, otherwise a future check cannot tell passed from never-ran.

## 3. What is the blast radius?

Grep callers, importers, and string references of every symbol you intend to change. Record
the count. Check for generated code, schema files, fixtures, docs, and snapshots that mirror
the symbol. Why: the count bounds the Footprint and predicts the tedious middle of the run,
which is where long sessions most often lose the thread.

## 4. What has history already tried?

`git log --oneline -15 -- <path>` for each area, then read the interesting commits. Look at
open or recently merged PRs touching the same files when a remote is available. Why: prior
reverts, review comments, and abandoned branches are the cheapest source of Forks and Drift
signals.

## 5. What did prior sessions do here?

When Recall is installed, search it for the area, the branch, and the task's key nouns. Why: a
long-running agent that repeats a dead end from last week wastes the most expensive kind of
time. The precap can say "the earlier attempt at X failed because Y; this run did Z instead."

## 6. What can you not verify?

Every belief that survived the items above without confirmation becomes an `[unverified]`
assumption. Typical ones: a service is reachable, a fixture reflects production, a flag is on
in the target environment, a reviewer will accept a contract change. Why: the assumptions
section is where reality gets to disagree cleanly. An unverified belief hidden in the Path
turns into a mystery drift later.

## Budget

Match effort to the task. A one-file change earns ten to twenty tool calls and a precap under
500 words. A multi-session feature earns a real survey, possibly with subagents in parallel,
and a precap near the 1,500-word ceiling. Stop when every intended step can cite something you
read; keep going while any step cannot.
