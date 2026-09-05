# Cascade

Cascade keeps large projects moving with one living plan: outcomes, task dependencies, exit checks,
and enough evidence to resume. Independent tasks can run in parallel; the whole project finishes
only after its integrated behavior is verified.

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/cascade)

## Install

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill cascade
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install cascade@unc-skills
```

Codex:

```bash
codex plugin marketplace add miguelrios/unc-skills
codex plugin add cascade@unc-skills
```

pi (installs the complete unc-skills collection):

```bash
pi install git:github.com/miguelrios/unc-skills
```

## Use it

```text
Use Cascade to migrate our job runner. Break it into verifiable subsystem tasks, run independent
work in parallel, and keep going locally until the integrated recovery checks pass.
```

The agent saves `.cascade/<project>.md` with the outcome, context, task graph, and resume notes.
Each task has a deliverable, dependencies, exit check, status, and evidence. Ready tasks can be
assigned to subagents with separate ownership; shared changes and the combined result have an
integration owner. Without parallel tools, the same graph works sequentially.

The plan changes as the agent learns. Failed checks remain unfinished; changed scope is recorded
explicitly. Existing session permissions and limits still apply. Pausing leaves concrete next actions;
a saved plan does not itself start a background agent or schedule another run.

The complete workflow is in one [skill file](skills/cascade/SKILL.md). Small edits do not need Cascade.

## Existing plans

Resume old chains in place. Keep their outcomes, constraints, unfinished checks, and useful evidence;
translate successor order into actual dependencies as needed. Existing receipts remain evidence.
New work uses editable Markdown without the v2 frontmatter or separate receipt requirement. The old
`validate_cascade.py chain|exit` format checker has been removed; verification is the project's actual
exit checks.

## Package checks

```bash
npm test
npm run pack:check
```
