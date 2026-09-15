# blast-radius

Finds what a diff breaks beyond the diff, and proves the one fact the change is safe because
of by running real code. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills/blast-radius)

One job: analysis of a change's reach, with the single load-bearing safety fact proven by a
script or test that calls the real code. It does not exercise the running application through
its entry points (that is `autoqa`) and it does not produce screenshots or presentation
artifacts for the PR body (that is `before-after`).

1. Read the change, including the part the diff does not spell out.
2. Find the one fact it is safe because of.
3. Look where grep stops: library source, pinned versions, timing, wire formats, downstream hops.
4. Rate each risk with a real chance and cost, cite `file:line`, keep cleared items separate.
5. Prove the one fact by running code, or mark it unproven.

The skill carries `disable-model-invocation: true`, so it runs only when invoked
(`/blast-radius`), never on the model's own initiative.

## Install

skills.sh:

```bash
npx skills add Parcha-ai/parcha-skills --skill blast-radius
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install blast-radius@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add blast-radius@unc-skills
```

In pi, invoke it with `/skill:blast-radius`.

## Use

```text
/blast-radius                 the current diff
/blast-radius <PR number>     a PR
/blast-radius <symbol>        one function or module
```

In Codex, use `$blast-radius`.
