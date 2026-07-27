# Setup

Tether is an external Hermes plugin plus one canonical skill payload for Codex and Claude Code. It never modifies the Hermes source checkout and never asks for, copies, or persists Slack tokens; the running Hermes gateway retains that credential boundary.

## One-command setup

Run from any directory:

```bash
npx --yes --package=github:miguelrios/unc-skills#main tether setup --harness=both
```

Use `--harness=codex` or `--harness=claude-code` when only one is installed.

The command installs Tether, opens Hermes's own Slack setup, restarts or installs the gateway service when possible, and runs live readiness checks. Hermes generates the current Slack app manifest, including Socket Mode, Interactivity, events, scopes, and slash commands. Follow its prompts to create or update the Slack app and enter the bot/app tokens directly into Hermes.

Setup explicitly enables the `tether` Hermes plugin. When it finds the pre-release `session-bridge` plugin, it disables that legacy plugin before restart so two reply hooks never share one bridge database.

Slack's website remains the only manual boundary: create the app from Hermes's generated manifest, install it to the workspace, and create the `connections:write` app token. Keep the generated `channels:join` scope: Tether joins public destinations before posting so it can read their replies. Invite the bot to private channels explicitly.

Completion criterion: `tether doctor` reports a private live broker, at least one authorized operator, and an installed runtime/plugin. A default home channel is optional; without one, pass `--channel` when notifying.

Doctor distinguishes a copied-but-disabled plugin from the live Tether broker protocol and reports reply-ingress health. Socket Mode is the low-latency path. A bounded poll of recently active bridge threads is the recovery path for messages missed during websocket disconnects; it uses Hermes's existing Slack client, persists ingress IDs, and applies the same authorization before dispatch.

## Existing Hermes Slack setup

Tether automatically reuses Hermes's `SLACK_ALLOWED_USERS`, `GATEWAY_ALLOWED_USERS`, and `SLACK_HOME_CHANNEL` runtime settings. Do not duplicate them in Tether config.

For agent-to-agent collaboration, set `SLACK_ALLOW_BOTS=all` and set `TETHER_ALLOWED_BOT_USERS` to the comma-separated Slack member IDs of the peer agents that may participate. An integration that omits a member ID may instead be allowlisted by its Slack bot ID. The explicit Tether allowlist is required: enabling Hermes bot traffic alone does not trust every workspace bot. Disable machine-generated busy acknowledgments with `display.busy_ack_enabled=false`, and give each agent the shared-thread policy from the Tether skill: use conversation context, respond only when useful, and return exactly `NO_REPLY` otherwise. Hermes already suppresses that intentional-silence marker. Restart the gateway and rerun `tether doctor` after changing the settings.

The generated `${XDG_CONFIG_HOME:-~/.config}/tether/config.toml` is for optional overrides only:

- use `default_channel`, `default_owner`, `team_id`, or `allowed_users` for a Tether-specific policy;
- use `codex_resume_args` or `claude_resume_args` for fixed administrator-controlled sandbox flags;
- extend `zellij_agent_commands` when a Zellij-only bridge should recognize another agent executable;
- use `credential_command` plus `credential_env_allowlist` for scoped, short-lived native resume credentials.

An empty combined Hermes/Tether allowlist fails closed. Threads are shared among that explicit allowlist by default; set `default_owner` or pass `--owner` to restrict them to one member.

Owner-restricted bridges in shared `C…`/`G…` channels are rejected by default
because they can silently exclude another authorized operator. Use a DM for a
private workflow. Deployments that deliberately need per-owner threads in a
shared channel must explicitly set `allow_channel_owner_restrictions = true`.

## Native resume credentials

Resumed Codex and Claude Code processes receive a minimal environment and may use normal user-level authentication. They never inherit Hermes's Slack variables.

For an organization router, make `credential_command` read non-secret bridge metadata as JSON from stdin and return only short-lived environment values as JSON on stdout. Tether accepts only keys named in `credential_env_allowlist`, consumes the output through a pipe, and never logs it. Keep master keys and service-account credentials in the helper's protected store.

## Non-interactive and manual setup

For CI or a headless host, install first and generate the Slack manifest without prompting:

```bash
./install.sh --harness=both
tether setup --non-interactive
```

Then run `hermes gateway setup`, start the gateway, and run `tether doctor`.

## Updates

Hermes and Tether update independently. After a Hermes update, rerun the one-command setup or package installer. It replaces only Tether code, preserves config and bridge state, restarts Hermes, and checks the Slack adapter compatibility surface. Never fall back to direct Slack calls when compatibility fails.

Pin the Git reference to a release tag instead of `#main` when reproducible production installs matter.

## Reply diagnostics

Inspect a thread through the broker without exposing the Slack credential:

```bash
tether thread --channel C12345678 --thread-ts 1234567890.123456
tether doctor
```

The thread command returns a minimal message view. Doctor must identify the active implementation as Tether and show either connected Socket Mode or a healthy polling fallback. If neither ingress path is healthy, restart or fix Hermes; never bypass Tether with a direct Slack token.
