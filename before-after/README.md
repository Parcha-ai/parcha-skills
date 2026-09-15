# before-after

Paired base-versus-branch screenshot captures for a PR body, committed to a `pr-assets` branch
in the same repository. It works in Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills/before-after)

One job: capture the same page or element from a running base deployment and from the branch,
publish the PNGs where the PR can show them, and emit a `## Before / After` section with a
two-column table (one row per pair), the two source URLs, and the commit SHA of "after". It
never judges pass or fail (that is `autoqa`) and never analyzes what could break (that is
`blast-radius`). It never fabricates a "before" by switching branches; if only one URL is
given it asks for the other.

1. Pre-flight the `@vercel/before-and-after` CLI (or fall back to `scripts/capture.sh`).
2. Check the URLs for deployment protection.
3. Capture one pair per view, named `<label>-before.png` and `<label>-after.png`.
4. `scripts/upload-and-copy.sh --markdown` publishes every pair and prints the section.
5. Put the section in the PR body once.

The default adapter, `github-branch`, commits each PNG to an orphan branch
`pr-assets/<pr-number>` (or `pr-assets/<branch-slug>` before a PR exists) using git plumbing
only: a temporary index, `git commit-tree`, and a push to `origin`. No worktree, no checkout,
and the caller's index is untouched. The emitted URL,
`https://github.com/<owner>/<repo>/blob/pr-assets/<n>/<file>.png?raw=true`, renders in PR
bodies for anyone with repository access. `gist` and `blob` are opt-in through
`IMAGE_ADAPTER`.

## Install

skills.sh:

```bash
npx skills add Parcha-ai/parcha-skills --skill before-after
```

Claude Code:

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install before-after@unc-skills
```

Codex:

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add before-after@unc-skills
```

In pi, invoke it with `/skill:before-after`.

## Use

```text
/before-after <before-url> <after-url>            page capture
/before-after <before-url> <after-url> .hero      element capture
/before-after --mobile ...                        375x812 viewport
```

In Codex, use `$before-after`. All scripts are shell only.

## Provenance

- Upstream: [vercel-labs/before-and-after](https://github.com/vercel-labs/before-and-after),
  author James Clements, PolyForm Shield License 1.0.0. Vendored on 2026-09-15 by way of
  [michaelshimeles/skills](https://github.com/michaelshimeles/skills) (`before-and-after/`).
- License: PolyForm Shield 1.0.0. The upstream `LICENSE` file is included verbatim in this
  package directory. `package.json` uses the SPDX identifier `PolyForm-Shield-1.0.0`.
- Modifications made here:
  - Renamed the skill from `before-and-after` to `before-after` (frontmatter `name` and
    directory) and added `license: PolyForm-Shield-1.0.0` to the frontmatter.
  - Removed the `0x0st` adapter and every default upload to a public host. The evidence rule
    in this repository forbids publishing evidence to public hosts.
  - Added `scripts/adapters/github-branch.sh` and made it the default adapter. It commits the
    PNGs to an orphan `pr-assets/<pr-number>` (or `pr-assets/<branch-slug>`) branch with git
    plumbing and emits `?raw=true` blob URLs.
  - Kept `gist` and `blob` as opt-in adapters. The `gist` adapter now creates a secret gist
    instead of a public one.
  - Rewrote `scripts/upload-and-copy.sh`: accepts any number of before/after pairs, per-pair
    `--label`, `--before-url`, `--after-url`, and `--after-sha`; the `--markdown` output is a
    `## Before / After` section with a two-column table, one row per pair, and a line naming
    the two source URLs and the "after" commit SHA. Uses `set -euo pipefail`.
  - Rewrote SKILL.md around the single job: the "before" URL must be a real running base
    deployment (main preview or staging); one URL given means ask for the other; no branch
    switching; no pass/fail judgement. Dropped the upstream `allowed-tools` list and the
    `--upload-url` CLI flag description, which defaulted to 0x0.st.
  - `scripts/capture.sh` is unchanged from upstream.
