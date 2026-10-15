# Setup

Tether installs a Hermes plugin, local CLI and instruction skills for Claude
Code and Codex. Hermes keeps Slack credentials; CLI clients use the private
local broker. The portable offline demo requires no Hermes installation.

## Try the source

With Linux, Python 3.11–3.14 and Node 22 or 24, from the repository root:

```bash
node tether/bin/tether.js demo
node tether/bin/tether.js setup --help
```

Use a checkout or full source commit you have reviewed. For an immutable
source installation through the repository's package entrypoint:

```bash
TETHER_COMMIT="<reviewed-40-character-commit-sha>"
npx --yes --package="github:Parcha-ai/parcha-skills#$TETHER_COMMIT" \
  tether setup --harness=both --team-id T01234567
```

A published npm artifact is not required. The actual source package version
is `0.4.0`; the obsolete `0.3.0-beta.1` command and `--herdr` setup flag should
not be used. This package does not ship a Herdr cockpit or schema CLI.

## Configure the host

Install Hermes and its supported Slack/plugin capabilities first. Run setup
as the same non-root Unix user that runs Hermes:

```bash
node tether/bin/tether.js setup --harness=both --team-id T01234567
```

Replace `T01234567` with your actual Slack workspace ID. Setup writes
`active = true` and that workspace into the instance's Tether configuration;
an existing valid `team_id` can be reused when the flag is omitted. This is
distinct from `self` and Slack member IDs in the team manifest.

Use `--harness=codex` or `--harness=claude-code` for one harness. Setup installs
managed code and skills, links/enables the Tether plugin, disables the legacy
session bridge when present, configures mention-aware bot ingress and opens
Hermes's Slack setup. Credentials are entered directly into Hermes.

`--non-interactive` generates the Slack manifest and leaves Slack configuration
to a later `hermes gateway setup`. Without a workspace ID it does not activate
Tether; finish with `tether setup --team-id <workspace-id>` after configuring
Slack. `--no-restart` skips the gateway restart.
Help and invalid setup options leave installation/configuration untouched.

In Slack, install the app with the generated permissions and Socket Mode
configuration, set explicit allowed operators, and invite the bot into the
channels it should use. Then inspect the installed runtime:

```bash
export PATH="$HOME/.local/bin:$PATH"
tether version
tether doctor
```

Doctor checks broker and egress state. Complete the setup check with an actual
notification and an inbound thread reply from an allowed operator. A successful
Slack auth check alone does not prove the event connection works.

## Configure the colleague

Copy the package's `examples/team.toml` into `~/.config/tether/team.toml` and
edit names, roles, Slack member IDs and project references. Set
`team_config = "team.toml"` in `~/.config/tether/config.toml`, or provide the
path through `TETHER_TEAM_CONFIG` in the gateway environment. Restart the
instance so its Hermes system-prompt section picks up the selection.

`self` selects the colleague for this instance. Computer/project fields are
context only; native model selection, credentials, access and working
locations use their existing runtime configuration. A roster entry grants
no instruction authority.

## Authorization and continuation

Tether merges its `allowed_users` with Hermes's `SLACK_ALLOWED_USERS` and
`GATEWAY_ALLOWED_USERS`. Configure trusted peer Slack member identities in
`TETHER_ALLOWED_BOT_USERS` or the instance's `trusted_bot_users`. Address peer work with the configured Slack mention. Hermes supplies the
mention-aware ingress filter; Tether admits trusted peers on bound threads.
Owned thread replies from authorized humans can continue
without a mention; a direct request needs action, not an acknowledgment loop.

Run `tether notify` or `tether attach` inside the exact session that should own
the thread. Use stdin for message content. After attachment, replies use the
same session context. Use `tether rebind` only for an intentional replacement.
Never guess another pane or create a fresh session as an invisible fallback.

## Lifecycle

From the reviewed new source checkout:

```bash
node tether/bin/tether.js upgrade --harness=both --restart
tether rollback --restart
tether uninstall
```

The installer stages managed files, records plugin state, takes a lifecycle
lock and snapshots the previous payload. Rollback does not undo database or
Slack effects. Uninstall preserves runtime state/configuration and locally
modified managed files. Back up the live database with its code revision
before an important upgrade.
