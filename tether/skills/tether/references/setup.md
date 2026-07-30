# Setup

Tether is an external Hermes plugin plus one skill payload for Codex and Claude
Code. It does not modify the Hermes source checkout. Hermes retains the Slack
credential; Tether does not copy it into publishers or resumed agent processes.

## Prerequisites

- Linux on x86-64 or arm64
- Python 3.11, 3.12, 3.13, or 3.14
- maintained Node.js LTS 22 or 24
- Hermes Agent 0.19.0
- one dedicated non-root Unix account for Hermes and Tether

Run `tether doctor` after every Hermes change. Tether rejects any Hermes version
other than 0.19.0 and fails closed when the Slack adapter contract differs.

## Install

Use an immutable published package:

```bash
npx --yes --package=@parcha/tether@0.2.0-beta.1 \
  tether setup --harness=both
```

Use `--harness=codex` or `--harness=claude-code` for one harness. For a source
install, replace `<verified-release-commit-sha>` with the full 40-character
commit from the matching GitHub release:

```bash
TETHER_COMMIT="<verified-release-commit-sha>"
npx --yes \
  --package="github:Parcha-ai/parcha-skills#$TETHER_COMMIT" \
  tether setup --harness=both
```

Do not install from `main`. The Agent Plugins marketplace manifest also uses a
full source commit. Maintainers update that pin only after completing the
procedure in the package's `docs/RELEASE.md`.

`setup` installs the runtime and skills, enables the Tether Hermes plugin,
disables the legacy `session-bridge` plugin when present, configures Hermes bot
ingress for mention-aware routing, disables busy acknowledgments, opens Hermes
Slack setup, restarts the gateway, and runs readiness checks.

Slack setup remains manual: create or update the app from Hermes's generated
manifest, install it to the workspace, create the Socket Mode app token, and
enter credentials directly into Hermes. Invite the bot to private channels.

Completion criteria:

```bash
tether version
tether doctor
```

`doctor` must verify Hermes 0.19.0, the private broker, at least one explicit
human operator, and connected Socket Mode ingress. Polling cannot satisfy the
ingress health check.

## Authorization

Tether merges its `allowed_users` with Hermes's `SLACK_ALLOWED_USERS` and
`GATEWAY_ALLOWED_USERS`. An empty human allowlist fails closed. Threads are
shared by that allowlist unless `default_owner` or `--owner` restricts a DM.

Owner-restricted bridges in shared `C...` or `G...` channels are rejected by
default. Prefer a DM. Set `allow_channel_owner_restrictions = true` only when
the exclusion is deliberate.

For trusted peer agents:

```bash
TETHER_ALLOWED_BOT_USERS="U01234567,U07654321"
TETHER_ALLOWED_BOT_IDS="B01234567,B07654321"
```

`TETHER_ALLOWED_BOT_USERS` contains Slack bot member IDs.
`TETHER_ALLOWED_BOT_IDS` covers event forms that carry an app bot ID. Configure
only identities that may instruct the agent. `tether setup` sets Hermes
`slack.allow_bots=mentions`; Tether remains the final routing authority for
explicit mentions and locally owned threads.

## Configuration

Optional non-secret overrides live at:

```text
${XDG_CONFIG_HOME:-~/.config}/tether/config.toml
```

Use it for `default_channel`, `default_owner`, `team_id`, `allowed_users`,
reply style targets, retention, native resume arguments, and Zellij agent commands.
Keep one Slack app/workspace identity per Tether instance and database.

Resumed Codex and Claude Code processes receive a minimal environment. For
short-lived native credentials, configure an absolute `credential_command` and
an explicit `credential_env_allowlist`. Tether validates the helper's path,
owner, mode, link count, output size, and returned keys. Do not return Slack
credentials, master keys, or execution-control variables.

## Attachments

File upload is disabled unless the gateway starts with
`TETHER_UPLOAD_APPROVED_ROOTS`, a colon-separated list of absolute private
directories owned by the Hermes user and mode `0700`. Optional settings are
`TETHER_UPLOAD_STAGING_DIRECTORY` and `TETHER_UPLOAD_MAX_BYTES`.

Do not approve `/`, `/tmp`, a shared checkout, or a credential store. Tether
rejects symlinks, hard links, owner mismatches, non-regular files, unstable
copies, oversized files, and known secret patterns. Content classification
remains the operator's responsibility.

## Headless setup

Generate the Slack manifest without opening the interactive flow:

```bash
npx --yes --package=@parcha/tether@0.2.0-beta.1 \
  tether setup --harness=both --non-interactive
hermes gateway setup
hermes gateway start
tether doctor
```

## Diagnostics and recovery

```bash
tether thread --channel C12345678 --thread-ts 1234567890.123456
tether maintenance
tether doctor
```

Socket Mode is the authoritative ingress path. The bounded poller may recover
recent active threads where Slack permits bot-token thread reads, but channel
polling can be unavailable or heavily rate-limited. It is not an unlimited
Slack-history replay and cannot make an unhealthy Socket Mode connection
ready. Neither path weakens workspace, channel, user, bot, or binding checks.

Use the package's `docs/OPERATIONS.md` for upgrade, rollback, backup, retention,
and uninstall. Never bypass a failed broker or compatibility check with a
direct Slack token.
