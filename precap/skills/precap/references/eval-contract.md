# Eval contract for precap

Use this rubric when judging a precap, whether you wrote it or inherited it. It mirrors the
eval-contract idea from skillify: a skill is judged against its own stated purpose, not a
generic checklist.

## Goal

A precap lets a long-running agent, after compaction or a fresh session, read one file and
know what it set out to do, what the finished work looks like, which step it should be on, and
whether it has left the path. Excellent means a stranger could resume the work from the
precap alone and would notice drift within one checkpoint.

## Dimensions

Score each 1 to 10.

- **GROUNDING**: does every Path step cite a real file, command, commit, or session, and do the
  cited things exist and say what the step claims?
- **CONCRETENESS**: are files, functions, commands, and counts named, or does the precap say
  "update the tests" and "wire it up"?
- **CHECKPOINT_QUALITY**: can each Checkpoint be verified by observing state (file exists, test
  passes, command exits 0) rather than by feeling?
- **FORK_HONESTY**: are genuine uncertainties written as forks with decision rules, and are
  user-owned decisions asked rather than assumed?
- **BOUNDARY**: does Not done name the real temptations for this task, not filler?
- **DRIFT_SIGNALS**: would the signals fire on the most likely detours for this specific task?
- **RESUMABILITY**: could someone who has never seen the conversation resume from this file?

## Hard fails

Any of these zeroes the score regardless of the dimensions above.

- A Path step with no `Grounded in:` or a grounding that points at something that does not
  exist.
- A scope decision that belongs to the user written in past tense as if settled.
- A precap overwritten in place with the Revisions section still empty after reality diverged.
- Assumptions presented as facts anywhere outside the Assumptions section.
- `validate` fails on the file.

## Pass bar

Every dimension at 7 or above, no hard fail. Below that, go back to Phase 1 and read more
before rewriting.
