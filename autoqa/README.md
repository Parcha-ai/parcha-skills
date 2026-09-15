# autoqa

Point it at a repo; it QAs the running app like an engineer would. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/autoqa)

1. **RESOLVE** — target repo, target instance, and the repo's `AUTOQA.md` config if it
   ships one (pre-answered discovery).
2. **DISCOVER** — read what the repo already says about itself (CLAUDE.md/AGENTS.md,
   feature-spec docs, compose files, env samples, CI) to learn run, auth, and the feature
   inventory.
3. **PLAN** — a feature×modality matrix (API / UI / CLI) with concrete pass criteria.
4. **EXECUTE** — run every row against the live instance, capturing a witness artifact per
   check. UI rows for changed behavior also get a "before" capture from a base instance when
   one exists, so the same run yields the PR's Before / After pair.
5. **REPORT** — verdict table + failure triage + a ship/don't-ship bottom line.

The invariant: **no witness, no verdict.** An `AUTOQA.md` in the repo makes repeat runs
instant — the skill writes one from its first discovery pass and skips Phase 1 forever after.

## Install

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill autoqa
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install autoqa@unc-skills
```
