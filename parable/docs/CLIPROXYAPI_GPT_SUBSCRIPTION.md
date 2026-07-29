# GPT, Claude, Grok, and Kimi subscriptions in one Claude Code session

Parable can run stock Claude Code with exact Fable or `gpt-5.6-sol` as the parent and
exact GPT, Claude, Grok, and Kimi named agents through one user-owned, loopback-only
CLIProxyAPI process.

You can also run in **solo mode** (`parable --solo <model>`) to launch with a single exact model from the loopback proxy — no multi-model casting, no agent delegation. Solo requires `[claude]` configured and only works with exact models exposed through the loopback proxy (not external providers like codex, pi, or cursor). It disables all Agent tool calls and agent synchronization.

```text
                                      ┌→ this user's ChatGPT subscription
stock Claude Code → localhost proxy ──┼→ this user's Claude subscription
                                      ├→ this user's xAI subscription
                                      └→ this user's Kimi Code subscription
```

There is no broker, shared deployment, provider API key, or copied Claude Code
credential in this mode. CLIProxyAPI owns each native OAuth flow and record.
Parable creates local configuration, delegates to those native flows, and checks
the authenticated model catalog. Normal multi-model launches generate exact project agents;
solo launches skip agent generation and require only their selected model.

Kimi is supported as a first-class subscription through the existing loopback proxy,
distinct from metered Moonshot/Kimi API-key routes via Fireworks or OpenRouter.

## Start here

Install the Parable skill with skills.sh or the Claude Code plugin marketplace:

```bash
npx skills add miguelrios/unc-skills --skill parable

# or
claude plugin marketplace add miguelrios/unc-skills
claude plugin install parable@unc-skills
```

Then ask Claude Code to **Set up Parable**. For a plugin install, the explicit
skill invocation is `/parable:parable install`; for a standalone skill it is
`/parable install`. The skill runs its bundled `parable.sh`, which installs an
immutable versioned runtime, creates `~/.local/bin/parable`, adds that directory
to the appropriate shell startup file when necessary, and enters setup/auth.
It does not download CLI code.

Before running the shell bootstrap, the skill uses the harness's native structured-question
UI—`AskUserQuestion` in Claude Code, `request_user_input` when the active Codex mode permits it,
or the equivalent elsewhere—to ask whether to add ChatGPT, xAI, and Kimi subscriptions. It falls
back to one concise question when structured input is unavailable. Claude is the baseline pool.
ChatGPT optionally adds Sol/Terra/Luna and enables Sol as an automatic fallback; without it,
`auto` stays on Fable. xAI and Kimi are independent subscriptions. If no proxy is available, it
asks once for consent to install missing build prerequisites and build the pinned, patched proxy.
It then passes those decisions to one non-interactive `parable.sh` invocation, so the user never
answers the same question again in a terminal prompt.

Claude Code's Bash tool cannot write a later callback into a running command's stdin, and its
command view may clip long authorization URLs. In that harness only, the skill stages setup with
`--no-auth`, then tells the user to open a new terminal and run:

```text
parable auth login
```

That command authorizes every selected missing provider in order and prints the final launch
command. Keep it running until the selected flows complete. It replaces, rather than precedes,
three separate `auth add` commands. Harnesses with a controllable foreground PTY run the bundled
bootstrap and authorization as one process instead.

A published npm install can seed the same standalone skill:

```bash
npm install -g @parcha/parable@latest
parable install
```

A source checkout uses the same bundled entrypoint:

```bash
git clone https://github.com/miguelrios/unc-skills.git
cd unc-skills/parable
./install.sh
```

Setup always selects Claude because Claude Code is the harness, then starts each selected
provider's native authorization flow. Parable first discovers an existing proxy from
`--proxy-bin`, `PARABLE_CLIPROXY_BIN`, or `PATH`; when none exists, the skill passes explicit
consent through `--build-proxy`. Declining that consent performs no network or build work.

After successful setup, enter the repository where Claude should work in a new
terminal and launch it:

```bash
cd /path/to/your/project
parable
```

That is the whole ordinary path. Bare `parable` selects `auto` and high effort. `auto` prefers Fable when it is configured and
its Claude usage is below 80%. When that pool is tight it selects Sol if the
ChatGPT pool has more or unknown headroom; if both are tight, it takes the less
used pool. Unknown Claude usage keeps the preferred Fable parent. Use
`parable --brain fable` or `parable --brain sol` to pin one explicitly.

To run a single model with no cast and no delegation, use solo mode:

```bash
parable --solo kimi        # friendly alias
parable --solo kimi-k3     # exact catalog id
```

Solo launches stock Claude Code with that one exact model as the only agent. It
rejects `--model` and any agent/tool flag, disables the `Agent` tool
(`--disallowedTools Agent`), unsets the experimental agent-teams variable, and
writes no project agent files. The launch card shows a `SOLO` layout with no
cast, procession, or delegation cues, so the harness never tries to spawn a peer.
`--solo` and `--brain` are mutually exclusive.

After Claude Code mounts, an in-UI Parable launch card shows the selected brain and every routed
model with its `use_for` guidance. Parable supplies it through a session-scoped `SessionStart`
hook as a user-only system message, so it does not enter model context or create a conversation
turn. `--print`, `--bare`, help, version, and init-only launches omit the card.

`parable` authenticates a readiness
probe to the configured loopback `/v1/models`. It reuses a healthy endpoint
without owning or stopping it; otherwise it starts the configured proxy,
waits for readiness, requires the exact configured parent and every selected child,
writes or confirms only Parable-owned project agents, launches stock Claude,
and stops only the proxy process it owns when Claude exits. When it owns both
children, signals reach both and the meaningful Claude or proxy exit status is
preserved.

For a headless ChatGPT device flow, create configuration without starting auth
and connect each selected vendor explicitly:

```bash
bash /path/to/installed/parable/parable.sh \
  --non-interactive \
  --vendors claude,chatgpt,xai,kimi \
  --build-proxy \
  --no-auth
parable auth add chatgpt --device
parable auth add claude
parable auth add xai
parable auth add kimi
parable auth status
cd /path/to/your/project
parable
```

Only run the Claude/xAI/Kimi commands if you selected those vendors. Claude auth
prints an SSH-forward reminder for callback port `54545`; keep that same
command alive until its newly issued callback completes. Old authorization
URLs cannot complete a new PKCE process.

The explicit lifecycle commands remain available as diagnostic escape hatches:

```bash
parable proxy start
parable setup finalize
```

`proxy start` exposes foreground logs for troubleshooting and is never required
beside the normal launcher. `setup finalize` starts and cleans up the managed proxy when needed,
checks the exact catalog, and synchronizes agents without launching Claude.
Neither command is part of ordinary onboarding.

You do not need to source `cliproxy.env`: the CLI passes the generated local
client token only to the catalog/Claude child process, converts it to
`ANTHROPIC_AUTH_TOKEN`, and removes the source variable before Claude starts.

## Non-interactive setup

Automation must state its vendor selection and include Claude:

```bash
bash /path/to/installed/parable/parable.sh \
  --non-interactive \
  --vendors claude,chatgpt,xai,kimi \
  --build-proxy \
  --no-auth
```

To add Kimi to a complete setup without replacing the local proxy token, binary,
or existing OAuth records:

```bash
parable setup --add-vendors kimi --no-auth
parable auth login
```

The additive command validates the current generated setup before replacing only
`parable.toml` and `setup.json`. The login command skips providers that already
have private credential records.

Supported selections are:

| Selection | Parent and named children |
|---|---|
| `claude` | Fable parent plus exact Fable 5, Sonnet 5, Opus 4.8, and Haiku 4.5 agents |
| `claude,chatgpt` | Claude cast plus exact Sol, Terra, and Luna; Sol becomes an eligible fallback parent |
| `claude,xai` | Claude cast plus exact Grok 4.5 |
| `claude,kimi` | Claude cast plus exact Kimi Code through native Kimi OAuth |
| `claude,chatgpt,xai` | Claude, GPT, and Grok casts (eight models) |
| `claude,chatgpt,kimi` | Claude and GPT casts, plus Kimi (eight models) |
| `claude,chatgpt,xai,kimi` | all nine exact models with subscriptions |

Unknown vendors, a selection without Claude, missing executables, partial
state, changed generated content, unsafe modes, or symlinks all fail without
overwriting anything. There is deliberately no `--force` path.

## What setup creates

The default configuration root is `~/.config/parable` with mode `0700`:

| File | Mode | Purpose |
|---|---:|---|
| `cliproxy.yaml` | `0600` | literal loopback listener, auth directory, random local client token |
| `cliproxy.env` | `0600` | local `CLIPROXY_API_KEY` export |
| `parable.toml` | `0600` | exact selected parent, agents, and routing |
| `setup.json` | `0600` | non-secret setup manifest used for strict idempotency |

CLIProxyAPI's auth directory is `~/.cli-proxy-api`, mode `0700`. Parable never
parses, copies, or writes provider OAuth token fields. `auth status` opens only
mode-`0600` regular JSON records and emits provider presence/counts plus
mode/parse aggregates—never filenames, paths, accounts, or credential values.
Native authorization inherits a private `0077` umask. Before accepting a record,
Parable narrows broader permissions to `0600` only when it is a regular file owned
by the current user, inside that `0700` directory, with the expected provider type.
`auth status` remains read-only.

## Native authorization mapping

Parable adds no OAuth implementation. Its commands become exactly:

| Parable command | CLIProxyAPI flags |
|---|---|
| `auth login` | each selected missing vendor's native flag below, in order |
| `auth add chatgpt` | `--config … --codex-login` |
| `auth add chatgpt --device` | `--config … --codex-device-login` |
| `auth add claude` | `--config … --claude-login --no-browser` |
| `auth add xai` | `--config … --xai-login --no-browser` |
| `auth add kimi` | `--config … --kimi-login` |
| `proxy start` | `--config … --local-model` |
| `claude` when the endpoint is absent | `--config … --local-model`, supervised |

Auth subprocesses inherit the terminal. Diagnostic `proxy start` inherits
stdio and stays foreground. The ordinary `claude` command keeps owned proxy
output away from the Claude TUI, waits for its authenticated catalog, forwards
`SIGINT`, `SIGTERM`, and `SIGHUP`, preserves meaningful child exits, and leaves
no owned proxy behind. It never stops a reused listener.

## Managed proxy build

Interactive `parable setup` after consent, `parable proxy build`, and
`setup --build-proxy` create a new private managed directory. They never patch
an existing checkout. The build stops before
`git am` or Go if either source or patch pin differs:

| Item | Pin |
|---|---|
| CLIProxyAPI base | `v7.2.103` / `cade44b9cdee6b9328ea2648fd119129fdf11e2d` |
| Vendored patch SHA-256 | `c0b4c52d4b35040427e1aee0067c99da7068598803604c435ad591d577b2dc5d` |
| Verified toolchain | Go `1.26.0` |
| Verified harness | Claude Code `2.1.220` |

The builder applies the vendored effort-translation patch, runs the focused Go
test slices, and emits a mode-`0700` `parable-cliproxy-api` executable. An
existing destination is refused rather than reused or deleted.

For an existing setup, `parable proxy upgrade` builds this pin in a new
versioned directory and atomically points future launches at it. It never
interrupts a proxy that is already serving a session. Re-running the command
after the manifest points at the current pin is a no-op.

Prerequisites are Node 18+, Python 3.11+, Git, Go, and stock Claude Code.

## Exact proved cast and effort behavior

The existing live proof covered 30 parent/child cells: five Sol parent effort values
times six exact named children. Every child used Bash and the parent consumed
its result. Kimi Code now uses the same exact-model route through `kimi-k3`, but
it is not part of that earlier proof set; complete the authenticated Kimi sentinel
before claiming live effort fidelity.

| Exact child | Subscription | `low` | `medium` | `high` | `xhigh` | `max` |
|---|---|---|---|---|---|---|
| `gpt-5.6-terra` | ChatGPT | exact | exact | exact | exact | exact |
| `gpt-5.6-luna` | ChatGPT | exact | exact | exact | exact | exact |
| `grok-4.5` | xAI | exact | exact | exact | → `high` | → `high` |
| `claude-sonnet-5` | Claude | adaptive exact | adaptive exact | adaptive exact | adaptive exact | adaptive exact |
| `claude-opus-4-8` | Claude | adaptive exact | adaptive exact | adaptive exact | adaptive exact | adaptive exact |
| `claude-haiku-4-5-20251001` | Claude | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 | → enabled/31,999 |

The pinned proxy registry advertises `low`, `high`, and `max` thinking levels for
`kimi-k3`. Parable requests `high` by default and does not claim behavior for
unverified Kimi effort values.

The authenticated catalog is entitlement truth. A similarly named id, display
alias, different case, or `-latest` suffix never substitutes for a missing
exact id. Subscription plan limits and provider terms still apply.

## npm release verification

Before publishing a new version:

```bash
cd parable
npm test
npm run pack:check
npm publish --access public
```

After publication, `npm install -g @parcha/parable@latest` installs the released CLI and
`parable install` seeds its bundled skill.

## Evidence

- [Secure setup and pinned builder](evidence/o1-secure-bootstrap/EXIT.md)
- [Native auth, safe status, and proxy lifecycle](evidence/o2-auth-proxy-lifecycle/EXIT.md)
- [Exact finalize and hermetic first launch](evidence/o3-finalize-first-launch/EXIT.md)
- [Two-command contract and baseline](evidence/m0-two-command-contract/EXIT.md)
- [Owned-or-reused Claude supervisor](evidence/m1-supervised-claude/EXIT.md)
- [Unified 30/30 live subscription verdict](evidence/y5-unified-subscription-verdict/EXIT.md)
- [GPT effort patch live proof](evidence/e2-cliproxy-effort-live/EXIT.md)
- [Grok subscription proof](evidence/x5-grok45-verdict/EXIT.md)

This remains an OSS, per-user localhost recipe. It does not require or support
a shared credential broker.
