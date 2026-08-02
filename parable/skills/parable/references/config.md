# parable.toml schema reference

## Multi-model mode vs. solo mode

This schema reference covers **multi-model Parable mode**. When running in solo mode (`parable --solo`), many config sections are ignored. **Solo requires `[claude]` configured and only works with loopback proxy models** (not codex, pi, cursor). See the table below.

| Section | Multi-model | Solo | Notes |
|---------|-----------|------|-------|
| `[parable]` | Used | Used | Except `default_executor`, `default_reviewer` |
| `[claude]` | Used | **Required** | Proxy and catalog checks required; solo exits if missing |
| `[providers.*]` | Used | Validated; subagent types aid aliases* | Codex/pi/cursor providers are not dispatched in solo |
| `[executors.*]` | Used | Validated; enabled exact subagent models aid aliases* | No executor is dispatched and no agent file is written |
| `[checks.*]` | Used | Used | Verification still runs |
| `[research]` | Used | Used | If in-session research tools are invoked |
| `[routing]` | **Used** | Ignored | Solo has no routing logic |

*The complete merged config is still parsed and validated. Solo reads enabled exact-model executors only to resolve friendly names, then launches the selected model directly.

## Resolution and merging

Files load lowest-precedence first; later files win:

1. `~/.config/parable/parable.toml` — personal cast, shared across repos
2. `<git-root>/parable.toml`
3. `<git-root>/.claude/parable.toml` — Claude-specific compatibility location; prefer the
   harness-neutral `<git-root>/parable.toml` for cross-harness repositories
4. `$PARABLE_CONFIG` — explicit path, wins over everything

`[executors.*]`, `[providers.*]`, `[checks.*]` merge **per id, per field** (a repo file can
override just `effort` on your personal `kimi`). `[parable]` and `[routing]` merge per key,
whole-value (a repo redefining `routing.feature` replaces that chain, not the whole table).
`[claude]` also merges per key.

Built-in Tier-0 defaults (providers.claude + executors sonnet/opus + all-subagent routing) sit
below everything. They are runnable only when the orchestrating harness exposes a native agent-
spawn tool; stock pi needs a configured CLI-backed executor. Executors that need API keys are
never defaulted — anything with an
`env_key` must be declared by a config file you wrote. `parable-config.sh` always prints
which files loaded.

Schema is versioned: `[parable] version = 1`. Unknown versions refuse to load.

## `[parable]`

| Field | Default | Meaning |
|---|---|---|
| `version` | 1 | schema version (required in written files) |
| `log_dir` | `.parable` | run/verify artifacts, relative to git root |
| `default_executor` | `sonnet` | fallback implementer |
| `default_reviewer` | `opus` | fallback reviewer |
| `repo_notes` | `""` | prose copied into every plan; repo conventions live here |

## `[claude]`

Optional stock-Claude-Code launcher configuration. It is required by bare `parable` and
`parable agents sync`; ordinary Parable dispatch remains unchanged when the table is absent.

| Field | Default | Meaning |
|---|---|---|
| `base_url` | required | Local Claude-compatible endpoint. Parable accepts only `http(s)` loopback hosts (`localhost`, `127.0.0.1`, or `::1`) so the local client token cannot be sent off-machine. |
| `auth_token_env` | required | Name of the environment variable holding the proxy's local client token. The token itself never belongs in TOML. |
| `brain_model` | required | Exact model id for the main Claude Code session, such as `gpt-5.6-sol`. |
| `binary` | `claude` | Optional Claude Code command name or path. |

`parable` checks `/v1/models` before launch and requires the selected brain model. It classifies
configured arbitrary-model Claude executors against that session snapshot, synchronizes every
project-local `.claude/agents/parable-*.md` file, and launches Claude Code with per-process
`ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN`; it does not write global Claude settings.
Missing optional executors are marked unavailable in the session card and rejected by the
`PreToolUse` hook before dispatch. Restart Parable after authentication recovers to refresh
availability; transient absence never deletes configured agent files.
The source token variable, `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, and
`CLAUDE_CODE_SUBAGENT_MODEL` are removed from the child environment. A forwarded `--model`
is rejected so parent selection stays inside Parable's declared policy.

The launcher-level `--brain` policy can be `config`, `fable`, `sol`, or `auto`. `config` requires
the configured `brain_model` to be available. `auto` prefers configured Fable while its live Claude usage is below
80%, probes ChatGPT only when that pool is tight, and then selects Sol when it has more or
unknown headroom. Explicit `fable` or `sol` fails unless that exact model is configured and
present in the authenticated catalog. Bare `parable` means `--brain auto` with high effort.
Claude flags pass through directly; use `parable --dangerously-skip-permissions` or pin the
brain with `parable --brain fable --effort high`. An optional `--` can separate Parable's brain
option from Claude arguments. Interactive sessions show this selection and the usable executor
cast in Claude Code via a user-only startup system message; the model does not receive the card.

`parable --solo <alias|exact-model>` instead requires only the selected exact catalog id, skips
agent synchronization, launches that model as the parent, removes agent-team enablement, and passes
`--disallowedTools Agent` to Claude Code. It rejects `--brain`, `--model`, `--agent`, `--agents`,
and caller-supplied allowed/disallowed-tool overrides so the single-agent contract cannot be weakened.

### Context ceiling for proxied non-Anthropic models

Claude Code does not know the real context window of models it does not recognize: it assumes
200k, or 1M when the parent carries a `[1m]`/long-context marker — and for a proxied non-Anthropic
model both guesses are wrong, so auto-compact fires far too late and the session dies with an
upstream `400 Your input exceeds the context window` error. Parable fixes this at launch by
setting both `CLAUDE_CODE_MAX_CONTEXT_TOKENS` and the independently evaluated
`CLAUDE_CODE_AUTO_COMPACT_WINDOW` to the real ceiling, plus
`CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=75` to leave room for tool results and the compaction request:

- **Solo mode** uses the selected model's exact window.
- **Multi-model mode** uses the minimum window across the brain and every enabled non-Claude
  proxy model in the cast (Claude Code applies the variable only to non-`claude-` models, so
  Anthropic parents and subagents are never handicapped). An unknown model counts as Claude
  Code's own 200k fallback rather than raising the ceiling blindly.
- Built-in windows come from the pinned proxy's own model registry (gpt-5.6-sol/terra/luna
  372k, grok-4.5 500k, kimi-k3 1M via upstream `k3` normalization, Claude 5-class 1M). Override
  or extend per executor with `context_ktok`.
- User-provided values always win independently. When you provide only
  `CLAUDE_CODE_MAX_CONTEXT_TOKENS`, Parable uses that value for the auto-compact window too;
  an explicit `CLAUDE_CODE_AUTO_COMPACT_WINDOW` or `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` is never
  overwritten. An unknown solo model leaves all three variables unset unless you explicitly
  provide a context ceiling.

The launch line reports both controls (`context ceiling 372,000 tokens; auto-compact 75%`) and
the startup card shows each model's real window (`· 372k ctx`). For Sol, 75% starts compaction
near 279k instead of waiting until roughly 353k; this leaves about 74k of headroom below the
Codex route's 353.4k effective input window.

Those environment controls protect future turns, but they cannot repair a saved conversation
that is already larger than a newly selected model's window. On explicit CLI resumes
(`--continue`, `--resume <name-or-id>`, and `--from-pr <value>`), Parable first runs Claude
Code's native `/context` under `claude-sonnet-5[1m]` at low effort. This inspection uses zero
model tokens. At or above 75% of the target ceiling, Parable invokes native `/compact` in that
same resolved session with Sonnet, then launches the requested model on the compacted history.
If inspection or compaction fails, Parable fails closed instead of opening a model that cannot
hold the session. Interactive `--resume` pickers and `--fork-session` are reported but skipped:
the exact destination does not exist early enough for a safe pre-launch mutation. Use
`--continue` or an explicit session name/id when moving a large conversation to a smaller
window.

The managed interactive launcher also has a one-shot fallback for a missed native compaction.
On the exact main-session context-window StopFailure, its private hook request tells the Node
supervisor to stop the stranded Claude child. The ordinary resume preflight then compacts that
same session under low-effort `claude-sonnet-5[1m]` and relaunches the original command with an
exact `--resume <session-id>`. Subagent and unrelated API failures do not trigger recovery, and
a second context failure remains visible for manual intervention rather than looping.

For a custom executor id such as `kimi`, `parable agents sync` creates the native Claude agent
name `parable-kimi` with the exact configured model id. Only files carrying Parable's generated
marker are updated or removed; unrelated user agents, including files that happen to begin with
`parable-`, are preserved. See `examples/parable.claude-subscriptions.toml`.

## `[providers.<id>]`

| Field | Applies to | Meaning |
|---|---|---|
| `type` | all | `codex` (custom provider via codex CLI) · `codex-native` (codex's own auth/models) · `pi` (any chat-completions/anthropic/responses endpoint via the pi coding agent CLI) · `cursor` (Cursor CLI `cursor-agent`; Composer + Grok + mirrors, subscription auth) · `subagent` (Claude Agent tool; arbitrary model ids become namespaced agents when `[claude]` is configured) |
| `base_url` | codex, pi | API root (codex: must serve `/responses`; pi: whatever `api` says). `cursor` rejects it — the CLI owns its endpoint. |
| `env_key` | codex, pi, cursor | NAME of the env var holding the API key — never the key itself. `cursor` defaults to `CURSOR_API_KEY`. |
| `wire_api` | codex | must be `"responses"` (validation enforces it) |
| `api` | pi | `openai-completions` (default) · `openai-responses` · `anthropic-messages` |
| `http_headers` | codex | optional map of static headers |
| `headers` / `compat` | pi | optional passthrough into the generated pi provider entry |
| `query_params` | codex | optional map of extra query params |

Unknown `type` values fail validation loudly (future harnesses will extend this enum).

## `[executors.<id>]`

| Field | Default | Meaning |
|---|---|---|
| `provider` | required | a `[providers.*]` id |
| `model` | required | provider-form model id |
| `effort` | `high` | `minimal`–`ultra` for Codex (`max`/`ultra` exist on GPT-5.6-class models; `ultra` flips codex into proactive multi-agent delegation). pi maps this to `--thinking`, additionally accepts `off`, and caps at `max`. Claude `subagent` accepts `low`, `medium`, `high`, `xhigh`, or `max`; Parable writes it into generated agent frontmatter. ALWAYS set it explicitly so runs never inherit a user's local harness default. `parable-run.sh --effort <level>` overrides CLI-backed executors for one dispatch. |
| `reasoning` | true | pi only: the generated model entry's reasoning flag |
| `model_overrides` | `{}` | pi only: raw fields merged into the generated model entry last (`maxTokens`, model-level `compat`, …) — pi's analog of `extra_config` |
| `cost` | — | `{ in, out, cache_in }` $/Mtok; informational + tie-breaks |
| `context_ktok` | — | context window, thousands of tokens. For Claude-proxy (`subagent`-typed) executors this also overrides Parable's built-in window table when computing the launch context ceiling (see below) |
| `tags` | `[]` | routing hints |
| `use_for` / `avoid_for` | — | prose the brain reads verbatim when routing |
| `max_minutes` | 20 | wall-clock kill for `run`/`resume` (reported TIMEOUT) |
| `extra_config` | `[]` | raw codex `-c` strings appended verbatim |
| `enabled` | true | set false to bench an executor without deleting it: `run`/`review` refuse it and `config`/`list` show it as `disabled` |

## `[checks.<id>]`

| Field | Default | Meaning |
|---|---|---|
| `run` | required | shell command; `{targets}` substituted from `--targets`. If the full suite needs services the working copy lacks, give it a hermetic shell default (`${targets:-test/unit}`) so unscoped runs fail only on real regressions, not environment |
| `cwd` | `.` | working dir relative to git root |
| `when` | — | list of `post-implement` / `pre-commit` |
| `timeout_minutes` | 15 | per-check timeout |
| `grep` | — | regex extracting actionable lines from failing output |
| `tail_lines` | 8 | failure-tail fallback when `grep` is unset/unmatched |

## `[research]`

| Field | Default | Meaning |
|---|---|---|
| `provider` | `grep.ai` | `grep.ai` or `claude`. What it governs and the scope boundary live in SKILL.md's research section. Whole-table merge, repo wins. |

## `[routing]` (Multi-model mode only)

**This section is ignored in solo mode.** It is used only when running Parable in multi-model mode (bare `parable` or `parable --brain`).

Keys are task classes such as `mechanical`, `data_transform`, `frontend`, `feature`,
`refactor_wide`, `gnarly`, `review`, `smoke_test`, and `architecture`. Their executor-id lists
are capable-peer menus selected by task fit and live subscription headroom, not priority
ladders. `escalation` is the exception: it is ordered. `notes` is prose for the brain. Lists
referencing unknown executors fail validation.

## Runtime artifacts

`<log_dir>/runs/<utc>-<slug>-<executor>/`: `plan.md`, `cmd.txt` (exact argv), `harness.jsonl`
(event stream), `resume-N.jsonl`, `last-message.txt`, `meta.json` (harness, session id,
status, timing, overrides — everything `resume`/`status` need). pi runs add `pi-agent/`
(the generated provider config — the user's `~/.pi/agent` is never read or written) and
`sessions/` (the pi session tree, a full transcript backup). `<log_dir>/verify/<utc>/`:
one log per check; `<log_dir>/reviews/<utc>-<executor>/`: pi review prompts + streams.
Add `log_dir` to `.git/info/exclude`; never commit it.
