# Provider recipes

## Why "responses" only

codex drives custom providers exclusively over the OpenAI **Responses API**
(`wire_api = "responses"`); chat-completions support was removed from codex. A provider works
with parable's codex executors if and only if it serves an OpenAI-compatible `/responses`
endpoint. parable injects providers per-invocation with `-c model_providers.*` overrides —
it never reads or writes `~/.codex/config.toml` provider entries, so your personal codex
setup is untouched.

Check credentials and reachability any time with `parable-config.sh` (shows per-executor
key status).

## Fireworks

```toml
[providers.fireworks]
type = "codex"
base_url = "https://api.fireworks.ai/inference/v1"
env_key = "FIREWORKS_API_KEY"
wire_api = "responses"
```

Model ids are the full account path, e.g. `accounts/fireworks/models/kimi-k2p7-code`.
The Responses endpoint is beta: most coding models run codex's multi-turn tool loop cleanly,
but some model chat-templates fail on multi-turn tool replay (symptom in the run log:
`jinja template rendering failed. Message has tool role, but there was no previous assistant
message with a tool call`) and some return persistent server errors. When a model misbehaves
on one provider, route it through another (OpenRouter, or a LiteLLM bridge) rather than
retrying — the failure is server-side and deterministic. A completed working tree with a
failed final stream is still verifiable: run `parable-verify.sh` and judge the evidence.

## OpenRouter

```toml
[providers.openrouter]
type = "codex"
base_url = "https://openrouter.ai/api/v1"
env_key = "OPENROUTER_API_KEY"
wire_api = "responses"
```

Model ids are OpenRouter's `vendor/model` form, e.g. `minimax/minimax-m3`,
`moonshotai/kimi-k2.7`. The Responses endpoint is beta and stateless; usage responses
include a `cost` field, which run summaries surface when present.

## pi (chat-completions without a bridge)

The [pi coding agent](https://github.com/earendil-works/pi) is a second executor harness.
Where codex only drives Responses-API providers, pi speaks **plain chat-completions** (plus
`anthropic-messages` and `openai-responses`) to any base URL — so chat-only providers work
directly, no proxy in the middle:

```toml
[providers.fireworks-pi]
type = "pi"
base_url = "https://api.fireworks.ai/inference/v1"
env_key = "FIREWORKS_API_KEY"
api = "openai-completions"

[executors.minimax-fw]
provider = "fireworks-pi"
model = "accounts/fireworks/models/minimax-m3"
effort = "medium"
cost = { in = 0.30, out = 1.20, cache_in = 0.06 }
context_ktok = 512
```

This is also the recovery path when a provider's Responses endpoint misbehaves for a model:
the same model over chat-completions often runs the full multi-turn tool loop cleanly.

Mechanics and requirements:

- **node ≥ 22.** pi currently requires node 22+ (older node hits HTTP-client
  incompatibilities); `npx @parcha/parable doctor` checks your version. Put a node 22+ bin dir
  first on PATH (e.g. `nvm use 22`), then `npm i -g @earendil-works/pi-coding-agent`.
  `doctor` checks both.
- **Hermetic per-run config.** parable generates `<run_dir>/pi-agent/models.json` and points
  pi at it via `PI_CODING_AGENT_DIR`; your `~/.pi` is never written to (the hermetic dir's
  `bin/` symlinks `~/.pi/agent/bin` read-only when present, for fd/rg reuse), the API key is
  referenced as `$ENV_VAR` (resolved at request time, never on disk or in argv), and runs get
  `PI_OFFLINE=1` plus `--no-extensions --no-skills --no-prompt-templates --no-approve` so
  nothing personal leaks in.
- **Sessions live in the run dir** (`sessions/`), so `parable-resume.sh` continues the exact
  session by id, and the session file doubles as a transcript backup.
- **Thinking:** `effort` maps to pi's `--thinking` (`off` is legal for pi executors). Models
  with baked-in reasoning may emit thinking regardless; that's the model, not a bug.

## LiteLLM proxy (bridge for chat-only providers)

Any provider that only speaks chat-completions can still back a codex executor through a
LiteLLM proxy, which serves `/v1/responses` in front of any configured model:

```toml
[providers.litellm]
type = "codex"
base_url = "http://localhost:4000/v1"   # your proxy
env_key = "LITELLM_MASTER_KEY"
wire_api = "responses"
```

Model ids are whatever aliases your proxy's `model_list` defines. If the only reason you'd
run the proxy is chat-completions translation, a `type = "pi"` provider does it without the
extra service.

## OpenAI (codex-native)

```toml
[providers.openai]
type = "codex-native"
```

Uses codex's own auth (ChatGPT login or `OPENAI_API_KEY`) and model catalog — no overrides
injected beyond `model` and effort. This is the zero-risk path: codex's home models run its
harness exactly as designed.

## Cursor (cursor-agent)

```toml
[providers.cursor]
type = "cursor"                 # cursor-agent CLI; no base_url (the CLI owns its endpoint)
# env_key = "CURSOR_API_KEY"    # optional — this is the default the CLI already reads
```

A third executor harness alongside codex and pi, driven by the
[Cursor CLI](https://cursor.com/docs/cli) (`cursor-agent`). Install:
`curl https://cursor.com/install -fsS | bash`. It reaches two model families you can't get
elsewhere: **Composer** (Cursor-exclusive — no public API, no gateway) and **Grok** (a genuine
third model family for adversarial review), plus mirrors of the Claude/GPT catalog.

- **Auth is a subscription**, not metered API keys: a user `CURSOR_API_KEY` draws on that
  account's Cursor plan. Confirm the tier with `cursor-agent about --format json`.
- **Effort rides the model slug**, not a flag: `grok-4.5-low|high|xhigh` (and a `-fast` tier);
  Composer has no effort variant. So set the full slug as `model` and treat `effort` as
  advisory metadata (parable does not gate it against the codex/pi enum for cursor executors).
  List slugs with `cursor-agent models` — route by slug, never by display name (the names are
  shifted, e.g. `grok-4.5-medium` *displays* as "Grok 4.5 Low").
- parable dispatches it as `cursor-agent -p --output-format stream-json --force --trust`, plan
  on stdin. `--force` is required or headless edits are proposed-only and the run stalls on
  approval. Sessions resume headlessly by chat id (`--resume <id>`), captured from the stream.
- Reviews run in `--plan` mode (read-only: analyze, no edits).
- **Small budget.** A Cursor plan's included budget (~$20/cycle on Pro) is orders of magnitude
  smaller than the Claude/ChatGPT plan windows and bills overage in arrears — treat cursor
  executors as a boutique lane (a fast mechanical burst, a third-family review), not bulk.
  `parable-usage.sh` shows the dollars remaining this cycle.

## Claude subagents

```toml
[providers.claude]
type = "subagent"
```

Executors on this provider are dispatched with the orchestrating harness's native agent-spawn
tool — no API keys and no CLI subprocess. `parable.py run` refuses them by design; the brain
owns subagent dispatch directly. If the current harness has no native agent-spawn tool, this
provider is unavailable.

When the session itself is launched with `parable claude --brain auto`, this same provider can carry exact
catalog model ids exposed by a localhost Claude-compatible proxy:

For a new subscription-only setup, use the CLI rather than hand-writing the
following reference config:

```bash
# Run the parable.sh bundled beside SKILL.md; it installs the CLI and enters setup.
bash /path/to/installed/parable/parable.sh
# in the working repository
parable claude --brain auto -- --effort high
```

Claude is the baseline pool for the Claude Code harness; interactive setup asks whether
to add ChatGPT and xAI and offers the pinned build when no proxy is installed.
`parable claude --brain auto` stays on Fable without ChatGPT. When ChatGPT is selected, it
prefers Fable while its pool has room and can move to Sol when it is tight. The launcher owns
proxy start, authenticated readiness, exact catalog sync,
stock-Claude launch, signal forwarding, and cleanup. It reuses but never stops
a healthy pre-existing endpoint. `parable proxy start` and
`parable setup finalize` remain explicit troubleshooting commands. Headless
operators can pass `--no-auth`, then use
`parable auth add chatgpt --device`, `parable auth add claude`, and
`parable auth add xai` for the vendors they selected. See the
[complete first-run guide](../../../docs/CLIPROXYAPI_GPT_SUBSCRIPTION.md).

Claude Code first-run uses `--no-auth` only because its Bash tool cannot write a later OAuth
callback to child stdin. It hands the user's terminal to one `! parable auth login` command, which
authorizes every selected missing provider. Do not expand that ordinary flow into per-provider
commands; those remain troubleshooting and headless-operator escape hatches.

The generated TOML has this shape:

```toml
[claude]
base_url = "http://127.0.0.1:8317"
auth_token_env = "CLIPROXY_API_KEY"
brain_model = "claude-fable-5"

[executors.terra]
provider = "claude"
model = "gpt-5.6-terra"
use_for = "Independent GPT implementation through ChatGPT subscription OAuth."

[executors.sonnet_exact]
provider = "claude"
model = "claude-sonnet-5"
use_for = "Implementation through Claude subscription OAuth."

[executors.grok]
provider = "claude"
model = "grok-4.5"
use_for = "Third-family implementation or review through xAI subscription OAuth."
```

`parable claude` checks the authenticated loopback catalog and then
materializes those executors as exact project agents; stock Claude Code sends child requests to
each full model id through the same endpoint. Diagnostic `parable setup finalize` and ordinary
launch obtain the private generated localhost client token without requiring a shell export,
and never print it. The launcher strips
`CLAUDE_CODE_SUBAGENT_MODEL`, because Claude Code gives that environment variable priority over
every agent's own `model:` field and would otherwise silently route all children to the parent.
The proxy owns provider OAuth. Parable stores no provider credential and does not implement an
OAuth flow. Kimi is currently paused and is intentionally absent from the proved setup.

### Verified GPT effort support

With stock Claude Code `2.1.215`, Sol, Terra, and Luna complete text and
tool-using requests through ChatGPT subscription OAuth. Released CLIProxyAPI
`v7.2.88` does **not** provide exact non-medium effort: Claude Code sends
`output_config.effort` but omits `thinking`, and that release translates all
five settings to `reasoning.effort=medium`. The
[released-binary receipt](../../../docs/evidence/g1-gpt-model-effort-live/receipt.json)
and [mechanism diagnosis](../../../docs/evidence/g1-gpt-model-effort-live/mechanism.md)
record the 3/15 exact baseline. Setting
`CLAUDE_CODE_ALWAYS_ENABLE_EFFORT=1` does not change the wire shape.

A source patch based on CLIProxyAPI commit
`93d74a890a44802f656d7f39a573916b2611896e` fixes the general
Claude-to-Codex translation boundary. Its independently built binary preserved
`low|medium|high|xhigh|max` exactly for all three models: 15/15 text cells and
3/3 medium tool canaries passed through ChatGPT OAuth. See the
[patched live receipt](../../../docs/evidence/e2-cliproxy-effort-live/receipt.json).

The patch is not an upstream release. Until CLIProxyAPI merges and releases
the change, released `v7.2.88` users must treat non-medium effort as
accepted-but-not-effective. Follow the
[pinned managed setup path](../../../docs/CLIPROXYAPI_GPT_SUBSCRIPTION.md)
for the exact build, OAuth, proxy, and Parable commands.

### Verified Grok 4.5 subscription support

The same patched localhost build exposes exact `grok-4.5` through per-user
xAI OAuth; no `XAI_API_KEY`, broker, or shared deployment is involved. With
stock Claude Code `2.1.215`, Grok completed 5/5 main-model text cells and 3/3
real Bash canaries. `low|medium|high` are exact. Claude's `xhigh|max` reach
CLIProxyAPI intact and clamp to Grok's supported `high`.

A generated custom executor routes exact named agent `parable-grok`:

```toml
[executors.grok]
provider = "claude"
model = "grok-4.5"
use_for = "Third-family implementation or adversarial review."
```

Sol invoked that agent successfully at all five parent effort values. Claude
Code inherited the parent effort into every child request; Grok preserved the
three supported values and clamped `xhigh|max` to `high`. The
[main-model receipt](../../../docs/evidence/x2-grok45-main-permutations/receipt.json)
and [named-child receipt](../../../docs/evidence/x3-sol-grok-named-subagent/receipt.json)
record exact model attribution, real tool use, deterministic artifacts, and
separate ChatGPT/xAI OAuth routes.

This xAI OAuth route is distinct from Parable's Cursor executor. Cursor uses a
Cursor plan and `cursor-agent`; it is not the Grok child route inside Claude
Code.

### Verified Sol subscription-subagent matrix

The exact named-child proof is complete for stock Claude Code `2.1.215`.
With `gpt-5.6-sol` as parent, every child below created a Bash artifact and
the parent consumed it with Agent and Bash at all five parent efforts:

| Exact child | OAuth route | `low` | `medium` | `high` | `xhigh` | `max` |
|---|---|---|---|---|---|---|
| `gpt-5.6-terra` | ChatGPT | exact | exact | exact | exact | exact |
| `gpt-5.6-luna` | ChatGPT | exact | exact | exact | exact | exact |
| `grok-4.5` | xAI | exact | exact | exact | → `high` | → `high` |
| `claude-sonnet-5` | Claude | adaptive exact | adaptive exact | adaptive exact | adaptive exact | adaptive exact |
| `claude-opus-4-8` | Claude | adaptive exact | adaptive exact | adaptive exact | adaptive exact | adaptive exact |
| `claude-haiku-4-5-20251001` | Claude | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 |

This is 30/30 live cells with exact model attribution and no provider
fallback. Sonnet and Opus preserve the effort label under adaptive thinking.
Haiku removes the label and consistently uses classic enabled thinking with a
31,999-token budget. See the
[GPT-child receipt](../../../docs/evidence/y1-sol-terra-luna/receipt.json),
[Claude-child receipt](../../../docs/evidence/y4-sol-claude-children/receipt.json),
and the [source-pinned setup guide](../../../docs/CLIPROXYAPI_GPT_SUBSCRIPTION.md).

Each operator must connect their own subscriptions to their own loopback
proxy. Catalog availability is the entitlement gate; do not substitute a
display alias when an exact id is missing. Plan limits and provider terms
still apply.

## Reading subscription headroom (parable-usage.sh)

Each subscription pool publishes its own remaining headroom over an authenticated endpoint the
official CLI already calls — `parable-usage.sh` reads them for zero model tokens and no turn,
using the same on-disk credential the local harness stored (it never mints or writes a token):

- **claude** — `GET https://api.anthropic.com/api/oauth/usage`, bearer
  `~/.claude/.credentials.json → .claudeAiOauth.accessToken` (needs `user:profile` scope),
  `anthropic-beta: oauth-2025-04-20`. Prefers the newer `limits[]` array (`kind` = `session`
  (5h) / `weekly_all` / `weekly_scoped` per-model, `percent` 0–100, `resets_at` ISO) — the
  per-model `weekly_scoped` bucket (e.g. the brain's own model) is often the tightest window
  and the flat `five_hour`/`seven_day` fields omit it; those flat fields are the fallback.
- **codex** — `GET https://chatgpt.com/backend-api/wham/usage`, bearer
  `~/.codex/auth.json → .tokens.access_token` + header `ChatGPT-Account-Id: .tokens.account_id`.
  Windows: `rate_limit.primary_window` (5h), `secondary_window` (weekly) (`used_percent`,
  `reset_at` unix). API-key auth (`OPENAI_API_KEY` in auth.json) has no plan usage — reports unknown.
- **cursor** — `POST https://api2.cursor.sh/auth/exchange_user_api_key` (bearer `$CURSOR_API_KEY`)
  → `accessToken`, then `POST …/aiserver.v1.DashboardService/GetCurrentPeriodUsage` → `planUsage`
  `{limit, remaining}` cents. The raw API key is NOT accepted on the RPC directly — the exchange
  step is mandatory.

All three are internal/undocumented (the same ones the CLIs call); shapes can drift across CLI
versions, so every probe fails soft to `unknown` on a missing credential, a stale-token 401, or
a shape change — the tool informs routing, it never blocks it. A 401 means the token is stale;
run any command for that CLI (or `codex login status`) to refresh it, then re-probe.

**Rate-limit note.** These endpoints throttle rapid polling — Claude's `/api/oauth/usage` in
particular trips a multi-minute HTTP 429 cooldown after a burst. `parable-usage.sh` therefore
caches each pool's read on disk for ~45s (`CACHE_TTL_SECONDS`): repeated calls within the window
reuse the last read instead of re-hitting the endpoint, and if a live probe does 429, the last
good read is served marked `(cached Ns)` rather than dropping the pool to `unknown`. So you can
call `parable usage` freely; don't build a sub-second poller around the raw endpoints yourself.

## Driving codex directly (beyond parable.py)

The dispatcher covers the common path; codex itself offers more when the brain needs it:

- `codex fork <session>` — branch an existing session to try alternative fixes in parallel
  without losing the original context.
- `codex apply` — apply the latest diff a codex session produced onto the working tree.
- `codex mcp-server` — run codex as an MCP server and call it as tools with session
  continuity; an alternative integration to subprocess dispatch for harnesses that prefer it.
- Direct `codex exec` calls: stdin is always read and concatenated with the prompt — if stdin
  is open but silent the process hangs forever with zero output. Close it (`</dev/null`) when
  no plan is piped. stderr carries the thinking stream; filter or capture it deliberately.
- Timeouts scale with effort: reasoning at `low` typically finishes in a couple of minutes;
  `xhigh` can legitimately run 20+. Set `max_minutes` per executor accordingly rather than
  one global number.

## Operational notes

- **Effort:** the mandatory-set rule and the per-dispatch `--effort` override live in
  `references/config.md`'s executor table.
- **Expect harness overhead.** A codex run carries its system prompt, skills, and hook
  context — on the order of 15–20K input tokens before the plan. Cache discounts absorb
  most of it on resumed sessions; budget for it on one-shot runs of cheap models.
- **Model metadata warnings** (`Model metadata for '…' not found`) are benign for custom
  providers; set `extra_config` overrides (e.g. `model_context_window`) if a model needs them.
- **`codex exec review`** runs in a read-only sandbox that requires working user namespaces
  (bubblewrap). parable's `review` subcommand instead pipes the diff into a normal executor
  run — identical coverage, no sandbox dependency.
