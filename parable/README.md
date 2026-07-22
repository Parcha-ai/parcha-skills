# parable

<p align="center">
  <img src="assets/hero.jpeg" alt="parable" width="400">
</p>


[![skills.sh](https://skills.sh/b/miguelrios/unc-skills)](https://skills.sh/miguelrios/unc-skills/parable)

**Multi-model coding orchestration for Claude Code, Codex, and pi.**

## Install Parable

```bash
npx skills add miguelrios/unc-skills --skill parable --global --agent claude-code
# Start a new Claude Code session and say: "Install Parable"
```

> **Coding agents:** A request to **Install Parable** or **Install Parable.sh** is subscription
> onboarding, not a file-copy task. Read [`skills/parable/SKILL.md`](skills/parable/SKILL.md) now
> and follow its **First-time install and subscription onboarding** section. Use the harness's
> native structured-question UI (`AskUserQuestion` in Claude Code) before running shell commands.
> Do not run `install.sh` as a generic skill copier, seed an example `parable.toml`, or stop after
> reporting that the skill exists. First-run succeeds only after the selected native OAuth flows
> complete and Parable prints the new-terminal `parable` command.

Claude is always the baseline subscription. The onboarding questions make ChatGPT
(Sol/Terra/Luna and optional Sol fallback) and xAI (Grok 4.5) opt-in. If no compatible proxy is
installed, onboarding separately asks permission to build Parable's pinned patched proxy.
Claude Code then tells the user to open a new terminal and run `parable auth login`, because its
Bash tool cannot inject later OAuth callbacks into a running process and its command view may clip
long authorization URLs. That single command connects every selected missing provider; users
should never be handed three separate provider commands.

It is Tuesday. You ask Fable to extract a helper and add a test. Three hundred lines later, the
helper has its own module and two new dependencies. The model is pleased with itself. You open
the billing console.

Parable changes the execution model: Fable plans and routes the work; specialized executors do
the implementation.

```text
Request
   |
   v
Fable: split into tasks + write one plan.md per task
   |
   v
Route each task to the best available executor:
+------------------+----------------------------------+
| Claude subagents | Sonnet / Opus                    |
+------------------+----------------------------------+
| Codex CLI        | GPT-5.5                          |
+------------------+----------------------------------+
| Cursor CLI       | Composer 2.5 / Grok 4.5          |
+------------------+----------------------------------+
| pi / API         | Kimi / MiniMax / DeepSeek        |
+------------------+----------------------------------+
   |
   v
Executor sessions (parallel when tasks are independent)
   |
   v
Shared worktree -> tests + independent review -> commit
                       |
                       +-- fail -> resume that task's executor
```

Each task gets its own `plan.md` and the best available executor for its requirements, marginal
cost, and live subscription headroom. Independent tasks can run in parallel, but they converge
on one worktree and one verification gate. Fable spends its context on decomposition, routing,
and judgment—not routine implementation.

## Unscientific stats

One real feature (a new research tier in a production monorepo), same spec and same base commit,
run twice headlessly — a plain Fable session vs parable orchestrating, Fable as the brain. Spend straight from the
LLM proxy's logs. One run per arm: an anecdote with receipts, not a benchmark.

| | Plain Fable session | With parable |
|---|---|---|
| Fable (the expensive one) | **$44.12** | **$16.08** (−64%) |
| Sonnet subagent — scouting | — | $2.08 |
| Opus subagent — review | — | $2.54 |
| Kimi K2.7-code — implementation (373 requests) | — | $6.99 |
| MiniMax M3 — mechanical edits | — | $0.05 |
| **Total** | **$44.12** | **$27.74** |

The subagent lines ride a Claude plan and the Fable line is your rate-limit budget if your
session does too — so the cash you actually send anywhere is the $7 of metered Fireworks/OpenRouter
tokens. A blind Opus judge scored the two diffs 94 and 88; parable's was smaller and carried more
tests.

## The fable

In a parable, the storyteller does not act out the scenes. Your session model, the most expensive
one you run, becomes the **brain**: it writes a fully-specified `plan.md` for each task (the
story), casts the cheapest capable **executor** model to perform it (the cast), checks the result
with your own typecheck and tests before any model spends tokens on opinions, and hands the diff
to a reviewer that is never the author.

The brain never implements — it knows what its own tokens cost.

## The cast

You configure the cast in one TOML file, with real prices and plain-prose stage directions
(`use_for`, `avoid_for`) that the brain reads verbatim when deciding who plays which scene.
A reference troupe, at their actual rates:

**The Sparrow** (Sonnet, already lives in your house)
Default implementer with zero configuration. Follows a well-written story to the letter, which
means it builds exactly what you wrote. Write the story properly.

**The Owl** (Opus, also on payroll)
Reviews and smoke-tests. Has opinions and expresses them in complete sentences. You will not
always like them. That is the point.

**The Mule** (Kimi K2.7-code, $0.95/M in, $4.00/M out)
Carries features and bugfixes without complaint. Writes code like someone who has been writing
code longer than you have been debugging it.
*Moral: strong legs cost less than strong opinions.*

**The Fox** (MiniMax M3, $0.30/M in, $1.20/M out)
Alarmingly cheap; handles boilerplate and first-pass review. Do not ask it about your
architectural decisions, because it will agree with all of them.

**The Elephant** (DeepSeek V4 Pro, $1.74/M in, $3.48/M out, 1M context)
Holds your entire repository in its head at once, for the refactors that touch everything.

**The Magpie** (GPT-5.5 via codex)
Collects shiny things from a different training run. Useful for gnarly debugging and adversarial
review, because it reads your codebase as an outsider — and it rides your ChatGPT plan.

**Cursor CLI** (Composer 2.5 or Grok 4.5)
Runs the same precise `plan.md` headlessly. Composer is fast at implementation; Grok 4.5 can
implement or provide cross-family review.

Swap any of them or add your own; the cast list is yours.

## Three acts

**Act I: no keys, works now.** Install and go. The Sparrow implements, the Owl reviews, your
session model narrates, and everything runs as Claude subagents with nothing to configure.

**Act II: enter the Magpie.** Install the [codex CLI](https://github.com/openai/codex), log in,
and add a `codex-native` provider. GPT-5.5 joins for the hard scenes.

**Act III: the full troupe.** Add any OpenAI-compatible provider with one `[providers.*]` block
plus an `[executors.*]` block per model. codex drives Responses-API providers (Fireworks,
OpenRouter, your own LiteLLM proxy); a `type = "pi"` provider runs the
[pi coding agent](https://github.com/earendil-works/pi) as a second harness and speaks plain
chat-completions to any base URL, so chat-only providers need no bridge at all. A `type = "cursor"`
provider sends the same `plan.md` through [Cursor CLI](https://cursor.com/docs/cli) to Composer
2.5 or Grok 4.5 for implementation or review. See the provider
[reference](skills/parable/references/providers.md), the complete
[Cursor example](examples/parable.cursor.toml), and the other configs in `examples/`.

There is also a single-harness, subscription-only path. `parable setup` builds or discovers a
loopback CLIProxyAPI, delegates each selected user's native ChatGPT/Claude/xAI OAuth, and writes
an exact Sol/Terra/Luna/Fable/Sonnet/Opus/Haiku/Grok cast. Bare `parable` starts or safely reuses the
configured loopback proxy, waits for its authenticated catalog, requires every exact id, creates
the project-local agents, launches stock Claude Code, and stops only the proxy process it owns.
No broker, shared deployment, provider API key, or copied OAuth credential is involved. See the
[end-to-end setup guide](docs/CLIPROXYAPI_GPT_SUBSCRIPTION.md) and the generated-config
[reference](examples/parable.claude-subscriptions.toml).

Bare `parable` uses the automatic brain policy: it prefers Fable while the Claude pool has room
and moves the parent to Sol when that pool becomes tight. `parable --brain fable` and
`parable --brain sol` pin either parent. `parable claude` remains a compatibility alias.

The generated cast gives the parent evidence-informed stage directions: Fable for ambiguous
planning and architecture; Sol for long implementation, difficult debugging, and high-recall
review; Terra for React and frontend work; Luna for data transforms and mechanical work; Sonnet
for brownfield implementation and test repair; Opus for high-blast-radius review; Haiku for fast
exploration; and Grok for bounded terminal-heavy or systems work. These are starting hypotheses,
not a universal leaderboard. Task-class arrays are capable-model menus; the parent excludes its
own model and the diff author, then chooses by task fit and live subscription headroom.

Minimal Cursor configuration:

```toml
[providers.cursor]
type = "cursor"

[executors.grok]
provider = "cursor"
model = "grok-4.5-high"
tags = ["feature", "adversarial"]
use_for = "Implementation or independent review through Cursor CLI."
```

For the non-coding half: with `[research] provider = "grep.ai"` (the default), in-depth research
and research-backed slides, sheets, and docs route through the free
[grep-research-skills](https://github.com/Parcha-ai/grep-research-skills) package — the research
runs on grep.ai's hosted service, quick lookups stay in-session, and setting `"claude"` keeps
everything local to your session instead.

## The script

```toml
[providers.fireworks]
type = "codex"
base_url = "https://api.fireworks.ai/inference/v1"
env_key = "FIREWORKS_API_KEY"
wire_api = "responses"

[executors.kimi]
provider = "fireworks"
model = "accounts/fireworks/models/kimi-k2p7-code"
effort = "high"
cost = { in = 0.95, out = 4.00, cache_in = 0.19 }
tags = ["implementer", "agentic"]
use_for = "Default implementer: fast, strong tool loop."

[routing]
feature = ["kimi", "sonnet"]
review  = ["minimax", "opus"]
```

The brain routes by reading your prose; there is no scoring function underneath. If the cast
keeps producing the wrong scene, look at the stage directions first.

## The part where it checks its own work

Your typecheck and tests run before any model spends tokens forming an opinion about the diff —
code is the cheapest witness. The result is a `PASS` or a `FAIL`, not a vibe.

Failures go back to the same executor session (context intact, cache warm) as a compact
evidence report. Models can usually fix what they broke; they just need to be told, concretely,
that they broke it. A one-line string change gets a check and a glance; a billing change gets a
frontier adversarial reviewer. And the reviewer is never the author — parable refuses to run
that configuration.

*Moral: never let the author hold the pen during the final read.*

## Installation reference

skills.sh:

```bash
npx skills add miguelrios/unc-skills --skill parable
# Then ask the harness: "Install Parable" or "Install parable.sh".
```

The skill uses Claude Code's `AskUserQuestion`, Codex's `request_user_input` when the active mode
permits it, or the current harness's equivalent structured UI to choose optional ChatGPT and xAI
subscriptions; it falls back to one concise question when structured input is unavailable.
Claude is the baseline pool because the harness is Claude Code. ChatGPT adds Sol/Terra/Luna and
enables Sol as an automatic fallback; without ChatGPT, `auto` stays on Fable. After those choices,
the skill runs its bundled installer once and delegates each selected provider's native OAuth flow.

```bash
# Claude Code plugin marketplace
claude plugin marketplace add miguelrios/unc-skills
claude plugin install parable@unc-skills
# Then, in Claude Code: `/parable:parable install`

# Codex plugin marketplace
codex plugin marketplace add miguelrios/unc-skills
codex plugin add parable@unc-skills

# pi package (installs the complete unc-skills collection)
pi install git:github.com/miguelrios/unc-skills

# optional Cursor executor (then create/export CURSOR_API_KEY from your Cursor account)
curl https://cursor.com/install -fsS | bash
export CURSOR_API_KEY="..."

# published npm CLI + standalone Claude skill
npm install -g @parcha/parable@latest
parable install

# standalone skill/runtime from source
git clone https://github.com/miguelrios/unc-skills.git
cd unc-skills/parable
./install.sh

# after setup succeeds, in a new terminal and from the project Claude should work on
cd /path/to/your/project
parable

# project-local skill copy without global subscription onboarding
./install.sh --project
```

Interactive setup offers the pinned proxy build when no executable is installed. For a headless
ChatGPT device flow, use
`parable.sh --non-interactive --vendors claude,chatgpt,xai --build-proxy --no-auth`, then run
`parable auth add chatgpt --device` plus the selected Claude/xAI auth commands. The
[subscription guide](docs/CLIPROXYAPI_GPT_SUBSCRIPTION.md) covers every command, generated file,
exact model, and failure rule.

Requirements: Claude Code, Codex, or pi as the orchestrating harness; Python 3.11+; Git and Go
for the managed CLIProxyAPI build; codex CLI
for codex-backed executors; Cursor CLI plus `CURSOR_API_KEY` for Cursor-backed executors; pi CLI
(node 22+) for pi-backed executors.

Claude Code and Codex builds with native agent spawning can use Parable's zero-config subagent
cast. Stock pi has no built-in subagents, so configure at least one Codex, pi, or Cursor executor
in a harness-neutral `parable.toml` before dispatching. Installation parity does not erase that
runtime difference.

## What's in the box

- `skills/parable/SKILL.md`: what the brain reads — the strategy, the house rules, and the
  environment facts it can't derive on its own. Deliberately small; the method is the model's.
- `skills/parable/parable.sh` and `skills/parable/runtime/`: the portable, pinned bootstrap and
  dependency-free CLI runtime. The package-level `bin/parable.js` / `lib/onboarding.js` are thin
  compatibility entrypoints. The runtime provides private subscription setup, native auth delegation,
  aggregate auth status, pinned proxy builder, diagnostic
  foreground proxy, exact catalog finalizer, and owned-or-reused stock-Claude supervisor.
- `skills/parable/scripts/parable.py`: the dispatcher, stdlib only, with `config`, `list`,
  `finalize`, `claude`, `agents sync`, `run`, `resume`, `status`, `verify`, and `review`
  subcommands. It
  launches stock Claude Code through a configured localhost proxy, runs codex and pi headlessly
  with per-invocation provider injection, drives Cursor through `cursor-agent`, and reports
  compact run summaries the brain can read for pennies. Global Claude settings,
  `~/.codex/config.toml`, and `~/.pi` are never modified.
- `skills/parable/references/`: config schema, provider recipes, routing playbook, reviewer
  rubric, and a commented example config. `examples/` holds minimal Fireworks, OpenRouter,
  LiteLLM, pi-Fireworks, [Claude subscriptions](examples/parable.claude-subscriptions.toml),
  and [Cursor](examples/parable.cursor.toml) casts.

## Credits

parable grew out of [dctanner's `cook` skill](https://gist.github.com/dctanner/54c57da4a94a24e71df6281f487f51e1),
the original plan-then-codex-then-review loop, generalized into a configurable cast with
cost-aware routing and a verification-first review ladder.

MIT © Parcha Labs

*Moral: the expensive model should tell the story, not type it.*
