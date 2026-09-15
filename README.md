# parcha-skills

Our collection of portable Agent Skills for Claude Code, Codex, and pi.

[![skills.sh](https://skills.sh/b/Parcha-ai/parcha-skills)](https://skills.sh/Parcha-ai/parcha-skills)

| Skill | What it does | Cross-harness note |
|---|---|---|
| [`hands-free`](hands-free/) | Calls your phone when the coding agent needs an answer or approval. | Same Python/Vapi contract in all three harnesses. |
| [`parable`](parable/) | Plans implementation batches, routes work to cheaper executors, verifies, and reviews. | Claude/native subagents are used only when available; stock pi needs a configured CLI-backed executor. |
| [`cascade`](cascade/) | Carries large projects through verifiable tasks and parallel work. | Keeps a living task graph; executes serially when parallel tools are unavailable. |
| [`recall`](recall/) | Indexed local search over prior Claude Code and Codex sessions. | Runs from pi, but does not index pi's own transcripts yet. |
| [`recap`](recap/) | Reconstructs everything observable that happened in one exact coding-agent session. | Uses Recall's Claude/Codex evidence; pi can run it but is not yet an indexed source. |
| [`tether`](tether/) | Keeps Slack threads attached to the exact agents that created them. | Codex and Claude Code resume natively; stock pi publishes as a headless run. End-to-end routing also installs an external Hermes plugin/runtime. |
| [`desloppify`](desloppify/) | Turns whole-codebase slop into evidence-backed cleanup with Peter O'Malley's official engine. | One canonical workflow selects honest native or prepared-packet review routes per harness. |
| [`autoqa`](autoqa/) | Points at any repo, discovers how it runs, and QAs the live app end-to-end with a witnessed report. | Uses whatever browser tooling the harness has; API/CLI checks work everywhere. |
| [`precap`](precap/) | Recaps a task before doing it: a grounded, past-tense account of the finished work that long-running agents check themselves against for drift. | Pure markdown plus a stdlib Python checker; identical in all three harnesses. |
| [`tdd`](tdd/) | Red before green at pre-agreed seams, one vertical slice per cycle, regression-first bug fixes. | Pure markdown; identical in all three harnesses. |
| [`blast-radius`](blast-radius/) | Finds what a diff breaks beyond the diff and proves the one safety fact by running real code. | Invocation only (`disable-model-invocation: true`); parallel reviewer subagents are used where the harness has them. |
| [`before-after`](before-after/) | Paired base-versus-branch captures for a PR body, committed to a `pr-assets` branch in the same repository. | Shell scripts plus `gh` and git plumbing; capture uses the `@vercel/before-and-after` CLI or `agent-browser`. |
| [`review-loop`](review-loop/) | Drives every reviewer thread on a GitHub PR (Greptile, Devin, humans) to zero unresolved in bounded iterations. | GitHub only, through the caller's authenticated `gh`; identical in all three harnesses. |
| [`verify-release`](verify-release/) | Confirms the intended candidate is live in the target environment and the changed path works there, with live evidence. | Uses whatever deployment tooling and observability the repo already has; calls `autoqa` for matrix coverage. |
| [`blast-radius`](blast-radius/) | [cursor/plugins, `pstack/skills/blast-radius`](https://github.com/cursor/plugins/tree/main/pstack/skills/blast-radius) | Lauren Tan | MIT |
| [`tdd`](tdd/) | [cursor/plugins, `pstack/skills/tdd`](https://github.com/cursor/plugins/tree/main/pstack/skills/tdd), adapted from a bug-fix-only workflow into a general red-green reference | Lauren Tan | MIT |
| [`unslop`](unslop/) | Cuts AI tells from anything a human will read. | Pure markdown; identical in all three harnesses. |

The skill payloads are canonical `skills/<name>/SKILL.md` directories. Harness-specific
manifests package those same files; there are no Claude/Codex/pi forks to drift apart.

## Skill roles in the PR lifecycle

Each shared skill has one job in the lifecycle of a pull request. A repo-specific orchestrator
calls the skill by name and never copies its body.

| Stage | Skill | Job, in one line |
|---|---|---|
| Start | `precap` | Write the finished-task recap first; detect drift against it. |
| Build | `tdd` | Red-green at pre-agreed seams. |
| Prove | `blast-radius` | Find what the diff breaks beyond the diff; prove the one safety fact by running code. |
| Prove | `autoqa` | Does the running app behave? Witnessed feature-by-modality matrix, baseline plus diff cases. |
| Describe | `before-after` | Paired base-versus-branch captures for the PR body. |
| Review | `review-loop` | Drive every reviewer thread (bots and humans) to zero, bounded. |
| Release | `verify-release` | Is the intended candidate live in the target environment and does the changed path work there? |
| Everywhere | `unslop` | Cut AI tells from anything a human reads. |

`evidence` is a rule, not a skill. It stays the always-on [`snippets/evidence`](snippets/evidence/)
block, and `autoqa` owns the witness contract (no witness, no verdict; `PASS`, `FAIL`, `UNTESTED`
with reason, `SKIPPED`, `BLOCKED`). `blast-radius` proves a single fact by running code; `autoqa`
exercises the app through its entry points; `before-after` shows a change visually without judging
it. The three do not overlap: analysis, execution, and presentation.

## Sources and credits

Some skills in this collection are vendored or adapted from other people's work. Each such
package keeps the upstream license file in its directory and a Provenance section in its README
listing every change made here.

| Skill | Source | Author | License |
|---|---|---|---|
| [`unslop`](unslop/) | [cursor/plugins, `pstack/skills/unslop`](https://github.com/cursor/plugins/tree/main/pstack/skills/unslop), by way of [michaelshimeles/skills](https://github.com/michaelshimeles/skills) | Lauren Tan | MIT |
| [`before-after`](before-after/) | [vercel-labs/before-and-after](https://github.com/vercel-labs/before-and-after), by way of [michaelshimeles/skills](https://github.com/michaelshimeles/skills) | James Clements | PolyForm Shield 1.0.0 |
| [`review-loop`](review-loop/) | [greptileai/skills](https://github.com/greptileai/skills) (`greploop`, `greploop-apps`), by way of [michaelshimeles/skills](https://github.com/michaelshimeles/skills) | Greptile AI | MIT |

`verify-release` was written for this collection and carries the repository's MIT license. `before-after` is not MIT; its PolyForm Shield license applies to that
directory only.

## Instruction snippets

[`snippets/`](snippets/) contains harness-neutral blocks for always-loaded agent
instruction files (`AGENTS.md`, `CLAUDE.md`). Skills load on demand through
discovery or invocation; a snippet applies to every session unconditionally.
Use a snippet when a rule must hold even in sessions where no skill is loaded.

| Snippet | Purpose |
|---|---|
| [`effective-comms`](snippets/effective-comms/) | Makes responses answer-first, concrete, structured, and brief by default, and bans marketing rhetoric in technical writing, while preserving safety and completeness exceptions. |
| [`evidence`](snippets/evidence/) | Ties claims to witnessed evidence and keeps sensitive evidence out of public repositories. |
| [`control`](snippets/control/) | Preserves user control over consequential actions and gives errors a bounded recovery path. |
| [`systems-thinking`](snippets/systems-thinking/) | Prevents local fixes from making the larger system worse without blocking bounded work. |

The install commands below cover skills only. To install a snippet, paste its
`AGENTS.md` block near the top of your root instruction file and keep the
`<!-- name:start/end -->` markers so tooling can update the block in place.

## Install with skills.sh

Browse all fifteen skills at [skills.sh/Parcha-ai/parcha-skills](https://skills.sh/Parcha-ai/parcha-skills),
or install interactively:

```bash
npx skills add Parcha-ai/parcha-skills
```

Install one directly with `--skill`:

```bash
npx skills add Parcha-ai/parcha-skills --skill hands-free
npx skills add Parcha-ai/parcha-skills --skill parable
npx skills add Parcha-ai/parcha-skills --skill cascade
npx skills add Parcha-ai/parcha-skills --skill recall
npx skills add Parcha-ai/parcha-skills --skill recap
npx skills add Parcha-ai/parcha-skills --skill tether
npx skills add Parcha-ai/parcha-skills --skill desloppify
npx skills add Parcha-ai/parcha-skills --skill autoqa
npx skills add Parcha-ai/parcha-skills --skill precap
npx skills add Parcha-ai/parcha-skills --skill tdd
npx skills add Parcha-ai/parcha-skills --skill blast-radius
npx skills add Parcha-ai/parcha-skills --skill before-after
npx skills add Parcha-ai/parcha-skills --skill review-loop
npx skills add Parcha-ai/parcha-skills --skill verify-release
npx skills add Parcha-ai/parcha-skills --skill unslop
```

Add `--global` for a user-level install or `--agent claude-code`, `--agent codex`, or
`--agent pi` to choose a destination explicitly. The skills.sh CLI discovers the same canonical
payloads used by the native installs below; npm publication of the individual packages is not
required.

Tether also needs its external Hermes runtime. Install the complete bridge with:

```bash
npx --yes --package=@parcha/tether@0.2.0-beta.1 \
  tether setup --harness=both
```

That command installs an immutable published package. Do not install Tether
from a moving branch. The package README documents source installs pinned to a
verified 40-character release commit.

## Install for Claude Code

```bash
claude plugin marketplace add Parcha-ai/parcha-skills
claude plugin install hands-free@unc-skills
claude plugin install parable@unc-skills
claude plugin install cascade@unc-skills
claude plugin install recall@unc-skills
claude plugin install recap@unc-skills
claude plugin install tether@unc-skills
claude plugin install desloppify@unc-skills
claude plugin install autoqa@unc-skills
claude plugin install precap@unc-skills
claude plugin install tdd@unc-skills
claude plugin install blast-radius@unc-skills
claude plugin install before-after@unc-skills
claude plugin install review-loop@unc-skills
claude plugin install verify-release@unc-skills
claude plugin install unslop@unc-skills
```

Install only the skills you want. Start a new session after installation.

## Install for Codex

```bash
codex plugin marketplace add Parcha-ai/parcha-skills
codex plugin add hands-free@unc-skills
codex plugin add parable@unc-skills
codex plugin add cascade@unc-skills
codex plugin add recall@unc-skills
codex plugin add recap@unc-skills
codex plugin add tether@unc-skills
codex plugin add desloppify@unc-skills
codex plugin add autoqa@unc-skills
codex plugin add precap@unc-skills
codex plugin add tdd@unc-skills
codex plugin add blast-radius@unc-skills
codex plugin add before-after@unc-skills
codex plugin add review-loop@unc-skills
codex plugin add verify-release@unc-skills
codex plugin add unslop@unc-skills
```

Codex uses the native `.agents/plugins/marketplace.json` and package
`.codex-plugin/plugin.json` manifests. Start a new session after installation.

## Install for pi

```bash
pi install git:github.com/Parcha-ai/parcha-skills
```

The repository is one pi package that exposes all fifteen skills. In pi, invoke one explicitly
with `/skill:<name>`, for example `/skill:hands-free`, `/skill:autoqa`, or `/skill:review-loop`.

## Compatibility checks

The local gate runs all fifteen skills across the three harnesses for native installation,
discovery, and credential-free smoke checks. Raw test output stays local and is not committed.

Run the local gate with:

```bash
npm test
for package in hands-free parable cascade recall recap tether desloppify autoqa precap tdd blast-radius before-after review-loop verify-release unslop; do (cd "$package" && npm test); done
python3 scripts/prove_portability.py --output /tmp/unc-skills-portability
```
