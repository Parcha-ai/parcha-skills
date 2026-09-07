# precap

Recap, but for the future. Precap imagines the task at hand is completely done and writes the
recap of the work it took and the end result, in past tense, grounded in what the repo, the plan,
and git history actually say. Long-running agents read it back to know where they are and whether
they are still on the imagined path. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/precap)

A precap does not predict the future. It records the imagined path concretely enough that
leaving it is detectable and revising it is explicit.

1. **Anchor**: verbatim task, the plan it descends from, branch and base.
2. **Investigate**: open the files the change lands in, learn the test commands, measure the
   blast radius, read the area's git history, search prior sessions.
3. **Imagine**: write the finished run as a recap. Every step has `Grounded in:` and
   `Checkpoint:`. Forks are written as forks. Out-of-scope is written down.
4. **Validate**: `precap.py validate` fails closed on missing sections, ungrounded steps,
   untagged assumptions, and footprint paths that contradict the tree.
5. **Check** during the run: `precap.py drift` compares the predicted footprint with git, and
   the agent walks the checkpoints to report `on path`, `ahead`, `drifted at step N`, or
   `blocked`.
6. **Revise** when reality differs: amend with a dated entry in Revisions, never a silent
   rewrite.

## Install

```bash
npx skills add miguelrios/unc-skills --skill precap
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install precap@unc-skills
```

Codex:

```bash
codex plugin marketplace add miguelrios/unc-skills
codex plugin add precap@unc-skills
```

## Use

```text
/precap                      write precap.md for the task at hand
/precap check                compare the current run against precap.md
/precap revise               amend precap.md after reality diverged
```

In Codex, use `$precap`. The script commands, run from the skill directory:

```bash
python3 scripts/precap.py template > precap.md
python3 scripts/precap.py validate precap.md
python3 scripts/precap.py drift precap.md --json
```

`precap.md` stays untracked by default; whether to commit it is your call.
