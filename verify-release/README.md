# verify-release

Establishes that the intended release candidate is live in its target environment and that the
changed path works there, using live evidence. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/verify-release)

One job: map the intended change to the running artifact (commit, image digest, build ID),
follow an in-progress rollout with bounded waits, run the smallest live probe through the
changed boundary, and answer `VERIFIED`, `FAILED`, or `INCOMPLETE`. It does not deploy,
roll back, or change flags, and it is not a full QA campaign; for an agreed test matrix or
substantial browser coverage it calls `autoqa` with the candidate identity.

## Install

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill verify-release
```

Claude Code:

```bash
claude plugin marketplace add miguelrios/unc-skills
claude plugin install verify-release@unc-skills
```

Codex:

```bash
codex plugin marketplace add miguelrios/unc-skills
codex plugin add verify-release@unc-skills
```

In pi, invoke it with `/skill:verify-release`.

## Use

```text
/verify-release                         verify the release named in the current task
/verify-release <env> <sha or version>  verify a named candidate in a named environment
```

In Codex, use `$verify-release`. The answer leads with the verdict, qualified by environment,
candidate, and tested scope, and lists each required check as `PASS`, `FAIL`, `BLOCKED`, or
`NOT RUN` with an evidence pointer.
