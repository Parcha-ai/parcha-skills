# Recall Brain clients

The client is a stdlib-only, consent-first boundary for explicitly selected
local sources, supported user exports, and deliberate memory writes. It sends
the same versioned envelopes as the Linux collector. It never discovers source
paths automatically: each supported database, export, or root must be selected
by the owner, and unsupported application-private stores remain out of scope.

## Build the reproducible macOS bundle

```bash
python3 scripts/build_macos_package.py \
  --source-root . \
  --output dist/recall-brain-macos.tar.gz
```

Two builds from the same tree are byte-identical. `MANIFEST.json` records every
bundle file's byte count and SHA-256, and installation stops before changing the
prefix if any entry or the package closure differs. This is corruption/tamper
detection, not publisher code signing. The bundle carries its pinned arm64
CPython runtime and installs only beneath the selected user prefix plus the
explicitly selected user LaunchAgent files.

## Inspect, install, and remove

Run a content-free inventory before enabling collection:

```bash
recall-brain dry-run --visibility private \
  --claude-root ~/.claude/projects \
  --codex-root ~/.codex/sessions
```

Put one source-scoped token per selected source into Keychain. The token is read
at runtime and never appears in the plist, process arguments, logs, manifest, or
spool:

```bash
printf '%s' "$SCOPED_CLAUDE_TOKEN" | recall-brain keychain-store \
  --service ai.parcha.recall --account claude:mac:my-mac
printf '%s' "$SCOPED_CODEX_TOKEN" | recall-brain keychain-store \
  --service ai.parcha.recall --account codex:mac:my-mac
printf '%s' "$SCOPED_COWORK_TOKEN" | recall-brain keychain-store \
  --service ai.parcha.recall --account cowork:mac:my-mac

./install.sh \
  --endpoint https://brain.example.ts.net \
  --host-id my-mac \
  --keychain-service ai.parcha.recall \
  --visibility private \
  --sources claude-code,codex,cowork \
  --privacy-mode scrub
```

For a hosted Brain, the operator enables only the authenticated canonical
archive and ingest routes described in `server/deploy/README.md`. Create the
device route in the web or Mac switchboard, store its one-time source token in
Keychain, and use the same HTTPS endpoint in the installer. The token is bound
to exactly one tenant, principal, and source; it cannot read the Brain or call
MCP. After installation, `recall-brain mac-status` must reach `ready` or report
one closed remediation code. Pause/disable retains the checkpoint, and resume
continues from the last Brain ACK without replaying committed documents.

`--sources`, `--visibility`, `--claude-root`, `--codex-root`,
`--codex-archive-root`, and `--cowork-root` are explicit consent controls.
The Codex archive root defaults to `~/.codex/archived_sessions`; active and
archived rollouts share one path-independent session identity. Cowork reads only the nested
local project JSONL surface; it does not inspect app databases, audit logs,
attachments, caches, metadata files, browser stores, Desktop, or Downloads.
On the current macOS release, `ChatGPT.app` is the Codex Desktop runtime and its
durable local work is the supported `~/.codex/sessions` rollout surface. Select
that source as either `codex` or the truthful alias `chatgpt-codex-desktop`;
both install the same single collector and neither claims consumer ChatGPT
cloud-chat history.

`recall-brain mac-claude-surface-preview` reports the separate Claude surfaces
without reading record bodies. On the probed release, Cowork project logs are
the supported desktop-work surface; Chromium IndexedDB and Local Storage remain
excluded app state, and ordinary Claude cloud-chat history is not claimed.
`--privacy-mode off|scrub|drop` defaults to `scrub`, and Cowork rejects `off`.
Re-running the installer stages and validates the new release before swapping
code, preserves private state, and automatically restores the prior code and
LaunchAgents if the upgrade fails. The last successful upgrade can be reverted
with `./install.sh --rollback`.

Inspect configured source classes without printing a path, credential, cursor,
content, or exception:

```bash
recall-brain mac-status
recall-brain mac-pause --source cowork
recall-brain mac-route-info --source cowork
recall-brain mac-route-apply --source cowork \
  --tenant-id tenant:personal --principal-id principal:owner
recall-brain mac-resume --source cowork
recall-brain mac-disable --source cowork
recall-brain mac-support
recall-brain mac-revoke --source cowork
recall-brain mac-reset-local --source cowork --confirm-source cowork
```

`mac-status` distinguishes starting, running, bounded backfill, ready, degraded,
stalled, paused, disabled, and invalid local state. Each source reports only
aggregate pending/dead-letter counts, recent scan lag, last-Brain-ACK age, and
one stable remediation class. Codex additionally reports whether archive
coverage is configured. A failed scan or Brain acknowledgement cannot be
reported as ready. Claude Code, Codex, Cowork, and the consented ChatGPT export
inbox run as separate LaunchAgents, so one source cannot hold another source's
schedule.

Per-source disable removes only that LaunchAgent and retains recoverable state.
`mac-revoke` also removes only that source's Keychain item. `mac-reset-local`
requires an exact source-name confirmation, pauses the source, and removes its
local spool/checkpoint and content-free logs. It deliberately retains—and does
not claim to delete—evidence already committed to the central Brain; central
deletion requires an authenticated server-side receipt/source operation.
`mac-support` reports only closed health and package-integrity aggregates: no
paths, credentials, content, cursors, or exception text.
`mac-route-apply` is normally driven by the native switchboard after the server
creates a source-scoped route. It atomically retains the LaunchAgent while
binding that collector to the canonical tenant writer; the next resume writes
raw objects through the configured archive and ACKs only after canonical ingest.

The bundle also carries a SwiftUI owner utility. Build it on an Apple Silicon
Mac with Xcode 16 or newer:

```bash
./macos_admin/build.sh
open "macos_admin/dist/Recall Brain.app"
```

The Mac switchboard uses the same `/admin/api/v1` contract as the web UI. It
stores the owner key and source credentials only in Keychain. Pausing retains
the exact LaunchAgent and checkpoint. Changing a destination atomically revokes
the prior device route, installs a new write-only source credential, binds the
retained LaunchAgent to that canonical tenant/principal, and sends subsequent
ingestion to the chosen personal or company brain.

Only already configured collectors are actionable. Selecting source paths,
granting Full Disk Access, and importing archives remain explicit install-time
consent steps; the app never discovers private stores.
Default uninstall removes code and agents but retains state; deleting state is
an explicit choice:

```bash
./uninstall.sh
./uninstall.sh --delete-state
```

The Recall skill can use the same central Brain without shell-specific exports.
Create a private read-only credential file, then a mode-0600
`~/.config/recall-brain/client.json` containing only schema version, the selected
HTTPS Brain endpoint, and the absolute token-file reference. Environment values still
override this profile, and `RECALL_MODE=local` remains a no-network rollback.

To continuously ingest only files deliberately copied into a flat export inbox,
add `--export-inbox "$HOME/Recall Inbox"`. The installer creates a separate
private LaunchAgent and uses Keychain account `chatgpt-export:mac:<host-id>`.
This does not inspect the ChatGPT app, Downloads, or browser storage.
Re-run with `--disable-export-inbox` to unload only that LaunchAgent while
preserving its content-free catalog/spool for recovery. Uninstall never deletes
the user-owned inbox.

## Supported exports and explicit memory

Only files supplied on the command line are considered. JSON, JSONL, and ZIP
archives containing JSON/JSONL are supported; traversal and symlink members are
rejected before any record is read.

```bash
recall-brain export --dry-run ... ~/Downloads/supported-export.zip
recall-brain export ... ~/Downloads/supported-export.zip
recall-brain put ... --text 'remember this' --provenance-uri manual://task
recall-brain delete ... 'recall://memory:mac:my-mac/memory-…?rev=1'
```

The omitted `...` is the connection tuple printed by `recall-brain --help`:
endpoint, exact source ID, principal, visibility, and a Keychain service/account
reference. Export native IDs are content/member stable, so replay is idempotent.
Delete writes a canonical tombstone; deleted evidence no longer appears in
search and its prior receipt no longer resolves.

## Pre-ingest privacy policy

Privacy policy version `recall-privacy-v2` runs on parsed content before a
collector writes version hashes or outbox rows and before export or memory code
makes an HTTP request:

- `off` preserves existing behavior when explicitly selected for supported sources.
- `scrub` replaces classified spans with category-labelled redactions while
  retaining safe context and provenance.
- `drop` omits the entire classified record. Its receipt contains only action,
  category counts, policy version, and a reason code.

Preview a value without printing it or writing a spool. Structural-only preview
makes no request; configuring the optional judge explicitly sends the value to
the approved router:

```bash
printf '%s' "$VALUE" | recall-brain privacy-preview --privacy-mode scrub
```

The local structural policy covers credential assignments, standalone common
provider credential shapes, and structured email, phone, postal, government,
financial, and medical identifiers. Contextual names and meaning require the
optional agentic judge. It accepts only an HTTPS staging
LiteLLM URL, a model alias, and a mode-0600 file containing a short-lived scoped
virtual key as `{"virtual_key":"...","scope":"recall-privacy-judge",`
`"expires_at":"...Z"}`; provider-direct URLs, wrong scopes, and expired files are
rejected. Set `RECALL_PRIVACY_JUDGE_ALLOWED_BASE_URL` from the trusted deployment
environment to the exact approved router base URL; the requested judge URL must
match it exactly. Never give it the LiteLLM master
key. Judge failure defaults to `drop`; `ignore` is an explicit availability-over-
privacy choice. The judge sees the text being classified, so enable it only with
informed consent and a router retention policy you accept.

Enabling `scrub` or `drop` compacts pre-existing pending spool rows once with
SQLite secure deletion. It cannot retroactively remove evidence already committed
to the Brain or copies retained in original source transcripts/backups. Delete
already-committed evidence by its canonical receipt before relying on the new
policy. Roll back a supported source by reinstalling it with `--privacy-mode off`;
Cowork remains `scrub`/`drop` only. Uninstall retains local state unless the user
explicitly passes `--delete-state`.

## External connectors

The bundle includes the versioned connector SDK, but installs no external source
and discovers no plugins automatically. A connector must be explicitly installed
and constructed. It owns only source fetching; the shared runner owns privacy,
durable ACK recovery, cursor commits, tombstones, and content-free health state.
See `connectors/README.md` before adding a source.

## Deliberate agent capture over MCP

The bundled stdio MCP server lets Codex, Claude Code, Claude Desktop, or another
local MCP host deliberately save one selected evidence item without giving the
model a Brain credential. Preview one source-scoped config per host; Recall does
not edit harness config:

```bash
recall-brain mcp-config-preview \
  --endpoint https://brain.example.ts.net \
  --source-id capture:mac:my-mac:codex \
  --capture-origin openai-codex \
  --visibility private \
  --keychain-service ai.parcha.recall \
  --keychain-account capture:mac:my-mac:codex \
  --privacy-mode scrub
```

Review the JSON and install it through the host's normal MCP settings. Give each
host its own source-scoped Brain credential and fixed `--capture-origin`; the
model cannot set or override origin in a tool call. The process resolves
Keychain only after launch; stdout carries newline-delimited MCP JSON-RPC and
never logs arguments. The tools are `recall_capture`, `recall_forget`, and
content-free `recall_doctor`. Capture retries are content-identity stable and
return the same receipt. `shared` visibility is available only when explicitly
selected in the process config, never in a tool call. The current stable MCP
revision is `2025-11-25`, with negotiation support for `2025-06-18` clients.

ChatGPT does not connect directly to a local MCP server. Use an approved remote
MCP deployment or OpenAI's Secure MCP Tunnel adapter instead; do not reuse a
local host's source credential for that bridge. See OpenAI's
[developer mode and MCP documentation](https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt).
