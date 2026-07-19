# parable.toml schema reference

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

Optional stock-Claude-Code launcher configuration. It is required by `parable claude` and
`parable agents sync`; ordinary Parable dispatch remains unchanged when the table is absent.

| Field | Default | Meaning |
|---|---|---|
| `base_url` | required | Local Claude-compatible endpoint. Parable accepts only `http(s)` loopback hosts (`localhost`, `127.0.0.1`, or `::1`) so the local client token cannot be sent off-machine. |
| `auth_token_env` | required | Name of the environment variable holding the proxy's local client token. The token itself never belongs in TOML. |
| `brain_model` | required | Exact model id for the main Claude Code session, such as `gpt-5.6-sol`. |
| `binary` | `claude` | Optional Claude Code command name or path. |

`parable claude` checks `/v1/models` before launch and requires the brain model plus every
configured arbitrary-model Claude executor to be present. It then synchronizes project-local
`.claude/agents/parable-*.md` files and launches Claude Code with per-process
`ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN`; it does not write global Claude settings.
The source token variable, `ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`, and
`CLAUDE_CODE_SUBAGENT_MODEL` are removed from the child environment. A forwarded `--model`
is rejected because `brain_model` is the declarative source of truth.

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
| `effort` | `high` | `minimal`–`ultra` (`max`/`ultra` exist on GPT-5.6-class models; `ultra` flips codex into proactive multi-agent delegation — the executor spawns its own subagent threads, highest per-turn burn, so reserve it for deliberately dispatched batch clusters); ALWAYS set it explicitly — parable pins it on dispatch precisely so runs never inherit the local user's harness config, which would make cost non-reproducible across machines. pi executors map it to `--thinking`, additionally accept `off`, and cap at `max` (no `ultra`). `parable-run.sh --effort <level>` overrides it for one dispatch |
| `reasoning` | true | pi only: the generated model entry's reasoning flag |
| `model_overrides` | `{}` | pi only: raw fields merged into the generated model entry last (`maxTokens`, model-level `compat`, …) — pi's analog of `extra_config` |
| `cost` | — | `{ in, out, cache_in }` $/Mtok; informational + tie-breaks |
| `context_ktok` | — | context window, thousands of tokens |
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

## `[routing]`

Keys are task classes (`mechanical`, `feature`, `refactor_wide`, `gnarly`, `review`,
`smoke_test`, `escalation`), values are ordered executor-id lists. `notes` is prose for the
brain. Chains referencing unknown executors fail validation.

## Runtime artifacts

`<log_dir>/runs/<utc>-<slug>-<executor>/`: `plan.md`, `cmd.txt` (exact argv), `harness.jsonl`
(event stream), `resume-N.jsonl`, `last-message.txt`, `meta.json` (harness, session id,
status, timing, overrides — everything `resume`/`status` need). pi runs add `pi-agent/`
(the generated provider config — the user's `~/.pi/agent` is never read or written) and
`sessions/` (the pi session tree, a full transcript backup). `<log_dir>/verify/<utc>/`:
one log per check; `<log_dir>/reviews/<utc>-<executor>/`: pi review prompts + streams.
Add `log_dir` to `.git/info/exclude`; never commit it.
