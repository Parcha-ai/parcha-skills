# tdd

Red before green at pre-agreed seams, one vertical slice per cycle, regression-first bug fixes.
It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills/tdd)

One job: make the red-green loop produce tests worth keeping. Before any test is written the
seams under test are listed and confirmed with the user. Each cycle is one seam, one failing
test, one minimal implementation. For a bug, the red test is a regression test that fails for
the intended reason before the fix. It does not refactor (that belongs to review) and it does not
QA the running app (that is `autoqa`).

Reference material ships alongside the skill:

- `references/tests.md`: good and bad tests, with the implementation-coupled and tautological
  anti-patterns shown side by side.
- `references/mocking.md`: mock only at system boundaries, and how to design those boundaries
  so mocks stay simple.

## Install

skills.sh:

```bash
npx skills add Parcha-ai/parcha-skills --skill tdd
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install tdd@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add tdd@unc-skills
```

In pi, invoke it with `/skill:tdd`.

## Use

```text
/tdd                       build the current feature test-first
/tdd fix <bug>             regression test first, then the fix
```

In Codex, use `$tdd`.

## Provenance

- Upstream: [cursor/plugins, pstack/skills/tdd](https://github.com/cursor/plugins/tree/main/pstack/skills/tdd),
  author Lauren Tan, MIT. Adapted on 2026-09-15.
- License: MIT. The upstream `LICENSE` file is included verbatim in this package directory.
- Modifications made here. The upstream skill is a bug-fix-only workflow (write the failing
  regression test, fix, rerun) that runs only on explicit invocation. This package widens it
  into a general red-green reference and keeps the upstream bug-fix path as one of its rules:
  - The description triggers on any test-first request, and `disable-model-invocation` is
    dropped so the skill auto-invokes on its own scope.
  - Added the seams contract: list and confirm the public boundaries under test before writing
    any test, one seam per cycle, one vertical slice per cycle.
  - Added `references/tests.md` (good and bad tests, implementation-coupled and tautological
    anti-patterns) and `references/mocking.md` (mock only at system boundaries).
  - Added the `CONTEXT.md` and ADR vocabulary rule and the `agents/openai.yaml` Codex manifest.
  - Kept from upstream: the regression test must fail for the intended reason before the fix,
    never weaken an assertion to match a wrong implementation, and report the failing-before
    and passing-after evidence.
