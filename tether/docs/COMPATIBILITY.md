# Tether compatibility

## Source runtime

The source package and plugin are version `0.4.0`. The runtime server supports
Linux x86-64/arm64, Python 3.11–3.14 and Node.js 22 or 24. Native continuation
supports Claude Code and Codex; optional Herdr placements use its CLI contract.
The offline demo requires Python and Node only.

Hermes is not bundled. Setup requires its plugin enable/disable commands,
configuration commands, Slack manifest/setup flow and gateway lifecycle
commands. Runtime integration uses the plugin dispatch hooks and detects
optional system-prompt, native-context and tool-registration surfaces.
These requirements describe the code's integration boundary; they are not a
claim that every stock Hermes release passed an end-to-end Slack journey.

The previous documentation pinned Hermes 0.19.0 and an exact clean checkout.
This source's plugin uses capability detection rather than enforcing that
version/commit pin. Run `tether doctor` and verify a real outbound root and
inbound reply against your chosen host. Doctor's egress authentication result
is not proof that Socket Mode ingress or task collaboration works.

macOS/Windows runtime servers, a remote client and the Grep computer adapter
remain roadmap work. A terminal or runtime being recognized does not prove
native continuation support for it.

## Persistent state

The session runtime stores endpoints, bindings, turns, attempts and thread
origins in `HERMES_HOME/plugin-data/tether/tether.db`. Store construction
creates the current tables and the runtime can import active bindings from
legacy `domain.db`. It does not implement the old documented schema-17/18
upgrade orchestrator. There is no `tether schema` CLI command in this package.

Stop the gateway before backing up runtime data. Use SQLite backup or preserve
the database and its WAL/SHM files together. Keep the code revision with the
backup. Installer rollback restores managed code and plugin state; it does
not reverse database changes or external Slack effects.

Existing bindings identify an exact session and conversation. A rebind is an
intentional replacement and advances the generation. Tether must not guess a
replacement session after a stale identity or ambiguous turn.

## CLI and package

CLI and broker use protocol 6. The package includes the Hermes plugin, CLI,
instruction skills, examples, offline demo and documentation. It does not
include the previously advertised `herdr-plugin` cockpit package.

`setup` accepts a harness selection, `--team-id`, `--non-interactive` and
`--no-restart`. A fresh interactive setup needs its actual workspace ID; an
existing valid config `team_id` can be reused. Setup activates the instance
for that workspace before restarting the gateway.
The old advertised `setup --herdr` option is unsupported. Help and invalid
setup arguments are handled before any installation or configuration change.
Use `setup --help` for the actual command contract.

The installed CLI locates `demo.py` in the managed Hermes plugin directory;
the source CLI uses its package payload. New modules are included in the
installer and managed-file manifest together so rollback can restore them.

## Configuration

The optional version-1 team manifest is prompt context. `TETHER_TEAM_CONFIG`
overrides config.toml's `team_config`. No configured path means a neutral
collaboration contract; malformed explicit configuration produces an error.
Computer preferences do not select models or change attached sessions.

Keep explicit operator and trusted-bot allowlists in the host/runtime settings.
The team roster grants no permission. Same-UID local processes share the
broker's authority boundary. Model and account authentication remain the
native computer's configuration, not a Slack credential supplied by the CLI.
