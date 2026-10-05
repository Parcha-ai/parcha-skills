# Tether

Give a colleague work in Slack. Continue it in the same Claude Code or Codex
session, with the repository, tools and conversation it already knows.

Tether connects Hermes's Slack interface to exact coding sessions. Configure
your own team, attach work already in progress, and carry a thread back to its
original computer. The broader goal is an open-source AI team workspace:
colleagues that own tasks, review artifacts and follow through. The
[roadmap](docs/ROADMAP.md) describes that build; this source ships session
continuation, portable team context and an offline workflow demo.

## Try it in one minute

From a source checkout, with Python 3.11–3.14 and Node 22 or 24:

```bash
node tether/bin/tether.js demo
```

No Slack app, Hermes installation, account or model call is needed. The demo
uses the real SQLite routing/attempt core and session driver with simulated
computers and delivery. It creates a sample artifact, runs a review that
catches an even-length median bug, routes a correction back to the owner,
and handles a follow-up in the same simulated session. Temporary files are
removed afterward; the receipt includes the artifact and verification evidence.

```bash
node tether/bin/tether.js demo --json
```

This demonstrates routing and follow-through mechanics. The computers are
scripted test doubles; it does not measure a model's ability to implement or
review code.

## Make it your team

Copy [examples/team.toml](examples/team.toml) into your instance configuration:

```bash
mkdir -p ~/.config/tether
cp tether/examples/team.toml ~/.config/tether/team.toml
```

Edit the colleague names, roles, Slack member IDs and project references. Set
the path in `~/.config/tether/config.toml`:

```toml
team_config = "team.toml"
```

Or set `TETHER_TEAM_CONFIG` to the manifest path in the gateway environment.
`self` selects the colleague for this instance. The same roster and collaboration
contract reach Hermes and native continuations. With no manifest, the contract
is neutral and no private roster is injected.

The manifest supplies context. Computer names and project references do not
change accounts, model selection, permissions or the runtime of an attached
session. Configure those through the existing runtime and host settings.
See [Colleagues](docs/COLLEAGUES.md) for the complete format.

## Connect a real session

The runtime server supports Linux x86-64/arm64, Python 3.11–3.14, Node 22/24,
and a Hermes installation with the plugin and Slack capabilities described in
[Compatibility](docs/COMPATIBILITY.md). Native continuation supports Claude
Code and Codex. Hermes stores the Slack credential; local CLI clients use a
private Unix socket.

From the source checkout you reviewed:

```bash
node tether/bin/tether.js setup --help
node tether/bin/tether.js setup --harness=both --team-id T01234567
```

Replace `T01234567` with your actual Slack workspace ID. Setup writes
`active = true` and that workspace into the instance's Tether configuration;
an existing valid `team_id` can be reused when the flag is omitted. This is
distinct from `self` and Slack member IDs in the team manifest.

Setup installs the plugin, CLI and instruction skills, enables the Hermes
plugin, configures mention-aware peer ingress, and opens Hermes's Slack setup.
You provide Slack credentials directly to Hermes and choose explicit allowed
operators. Use `--harness=codex` or `--harness=claude-code` for one harness.
`--non-interactive` generates the Slack manifest for later configuration.
Without a workspace ID it leaves Tether inactive; finish with
`setup --team-id` after configuring Slack. `--no-restart` leaves the gateway
restart to you. Help and invalid options
do not install or modify configuration.

Then check the installed runtime:

```bash
export PATH="$HOME/.local/bin:$PATH"
tether version
tether doctor
```

The source package is version `0.4.0`. A published npm artifact is not required
for this quickstart; use a reviewed checkout or an immutable source commit.
[Setup](skills/tether/references/setup.md) covers the source and lifecycle paths.

To add only the portable instruction skill to a coding harness:

```bash
npx skills add miguelrios/unc-skills --skill tether
```

The [skills.sh listing](https://skills.sh/miguelrios/unc-skills/tether) describes
that skill. This command does not install the Hermes plugin or local broker;
use the setup path above for actual session continuation.

Inside the Claude Code or Codex session that should own the work:

```bash
printf '%s\n' 'I am working on the parser fix here. Reply to continue this session.' |
  tether notify --text-stdin --idempotency-key parser-fix-start
```

Reply in the resulting Slack thread. Tether routes the message to that exact
session and reports its result in the thread. An allowlisted human can continue
an owned thread without mentioning the bot. Address peer work with the
configured Slack mention so the host can route it to the right colleague.

To connect an existing thread, run inside the intended session:

```bash
tether attach --channel C01234567 --thread-ts 1234567890.123456 \
  --idempotency-key parser-fix-attach
```

An intentional replacement uses `tether rebind`; Tether does not guess another
session after an identity mismatch. An optional Herdr placement is supported
by the runtime. This package does not ship the previously advertised Herdr
cockpit or a `schema` CLI command.

## See what is happening

```bash
tether status
tether thread --channel C01234567 --thread-ts 1234567890.123456
tether unresolved
```

These inspect runtime state, thread history and unresolved attempts through
the local broker. An execution finishing, a reply being sent and the whole
task being accepted are different outcomes. Durable task/review ownership
through Hermes is the next product slice; the offline demo is its acceptance
example, not a live task-management service.

The broker checks the local Unix user and uses a mode-`0600` socket. The gateway
holds Slack credentials, and native child environments are allowlisted.
Processes sharing the same Unix user share that local authority boundary.
Slack writes may have ambiguous outcomes; this source does not promise
exactly-once delivery. Do not put secrets into a thread or team manifest.

## Build with us

Start with [Contributing](CONTRIBUTING.md), then read the current
[Architecture](docs/ARCHITECTURE.md). The offline demo and fake-computer tests
provide a development path without private infrastructure. The
[audit](docs/audit/2026-10-04-audit.md) records why the roadmap spans product,
tasks, context, judgment, adapters and measurement.

MIT licensed. Built by Parcha; configurable for your team.
